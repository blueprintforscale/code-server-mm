#!/usr/bin/env python3
"""
GHL Estimates ETL — Pull estimates from GoHighLevel for GHL-only clients.

Usage:
  python3 pull_ghl_estimates.py                       # All GHL-only clients
  python3 pull_ghl_estimates.py --client 7123434733   # Single client by customer_id

Stores estimates in ghl_estimates table with normalized phone numbers for matching.
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime

import psycopg2
import psycopg2.extras
import urllib.request
import urllib.error

# ── Database ──────────────────────────────────────────────────

DB_CONFIG = {
    "dbname": "blueprint",
    "user": "blueprint",
    "host": "localhost",
    "port": 5432,
}

GHL_BASE = "https://services.leadconnectorhq.com"
GHL_VERSION = "2021-07-28"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS ghl_estimates (
    id SERIAL PRIMARY KEY,
    ghl_estimate_id TEXT NOT NULL,
    customer_id BIGINT REFERENCES clients(customer_id),
    ghl_location_id TEXT,
    ghl_contact_id TEXT,
    contact_name TEXT,
    contact_email TEXT,
    contact_phone TEXT,
    phone_normalized TEXT,
    estimate_number TEXT,
    status TEXT,
    total_cents BIGINT,
    currency TEXT DEFAULT 'USD',
    issue_date TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    items JSONB,
    opportunity_id TEXT,
    synced_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(ghl_estimate_id)
);
CREATE INDEX IF NOT EXISTS idx_ghl_estimates_customer ON ghl_estimates(customer_id);
CREATE INDEX IF NOT EXISTS idx_ghl_estimates_phone ON ghl_estimates(phone_normalized);
CREATE INDEX IF NOT EXISTS idx_ghl_estimates_contact ON ghl_estimates(ghl_contact_id);
"""


def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute(SCHEMA_SQL)
    conn.commit()


def normalize_phone(phone):
    if not phone:
        return None
    digits = re.sub(r'\D', '', phone)
    if len(digits) >= 10:
        return digits[-10:]
    return digits if digits else None


# ── GHL API ───────────────────────────────────────────────────

def ghl_request(method, path, api_key, params=None, retries=3):
    """Make a GHL API request."""
    url = f"{GHL_BASE}{path}"
    if params:
        qs = "&".join(f"{k}={urllib.request.quote(str(v))}" for k, v in params.items())
        url = f"{url}?{qs}"

    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Version": GHL_VERSION,
            "User-Agent": "BlueprintForScale/1.0",
        },
        method=method,
    )

    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 429 and retries > 0:
            print(f"    [Rate limit] 429 — waiting 10s...")
            time.sleep(10)
            return ghl_request(method, path, api_key, params, retries - 1)
        body_text = e.read().decode() if hasattr(e, 'read') else str(e)
        raise Exception(f"GHL API error {e.code}: {body_text}")


def fetch_estimates(api_key, location_id):
    """Fetch all estimates for a location, paginating through results."""
    all_estimates = []
    offset = 0
    limit = 100

    while True:
        params = {
            "altId": location_id,
            "altType": "location",
            "limit": str(limit),
            "offset": str(offset),
        }

        data = ghl_request("GET", "/invoices/estimate/list", api_key, params)
        estimates = data.get("estimates", [])

        if len(estimates) == 0:
            break

        all_estimates.extend(estimates)
        print(f"    Fetched {len(all_estimates)} estimates so far...")

        if len(estimates) < limit:
            break

        offset += len(estimates)
        time.sleep(0.1)

    return all_estimates


# ── Upsert ────────────────────────────────────────────────────

def upsert_estimates(conn, customer_id, location_id, estimates):
    """Upsert estimates into ghl_estimates table."""
    cur = conn.cursor()
    count = 0

    for est in estimates:
        ghl_id = est.get("_id")
        if not ghl_id:
            continue

        # Extract contact details
        contact = est.get("contactDetails") or {}
        contact_name = contact.get("name")
        contact_email = contact.get("email")
        contact_phone = contact.get("phoneNo") or contact.get("phone")
        contact_id = contact.get("id") or contact.get("contactId")
        phone_norm = normalize_phone(contact_phone)

        # Total in cents
        total = est.get("total")
        total_cents = None
        if total is not None:
            try:
                total_cents = int(round(float(total) * 100))
            except (ValueError, TypeError):
                pass

        # Items as JSONB
        items = est.get("items")
        items_json = json.dumps(items) if items else None

        # Opportunity
        opp_details = est.get("opportunityDetails") or {}
        opportunity_id = opp_details.get("id") or opp_details.get("opportunityId")

        cur.execute("""
            INSERT INTO ghl_estimates (
                ghl_estimate_id, customer_id, ghl_location_id, ghl_contact_id,
                contact_name, contact_email, contact_phone, phone_normalized,
                estimate_number, status, total_cents, currency,
                issue_date, created_at, updated_at,
                items, opportunity_id, synced_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (ghl_estimate_id) DO UPDATE SET
                status = EXCLUDED.status,
                total_cents = EXCLUDED.total_cents,
                contact_name = EXCLUDED.contact_name,
                contact_email = EXCLUDED.contact_email,
                contact_phone = EXCLUDED.contact_phone,
                phone_normalized = EXCLUDED.phone_normalized,
                items = EXCLUDED.items,
                opportunity_id = EXCLUDED.opportunity_id,
                updated_at = EXCLUDED.updated_at,
                synced_at = NOW()
        """, (
            ghl_id, customer_id, location_id, contact_id,
            contact_name, contact_email, contact_phone, phone_norm,
            est.get("estimateNumber"), est.get("estimateStatus"),
            total_cents, est.get("currency", "USD"),
            est.get("issueDate"), est.get("createdAt"), est.get("updatedAt"),
            items_json, opportunity_id,
        ))
        count += 1

    conn.commit()
    return count


# ── Main ETL ─────────────────────────────────────────────────

def pull_client(conn, client_row):
    """Pull all GHL estimates for one client."""
    customer_id = client_row["customer_id"]
    name = client_row["name"]
    api_key = client_row["ghl_api_key"]
    location_id = client_row["ghl_location_id"]

    if not api_key or not location_id:
        print(f"  SKIP — missing api_key or location_id")
        return

    print(f"\n{'─' * 60}")
    print(f"  {name} (customer_id: {customer_id})")
    print(f"{'─' * 60}")

    # Fetch estimates
    print(f"  Pulling estimates...")
    estimates = fetch_estimates(api_key, location_id)
    print(f"    → {len(estimates)} estimates fetched")

    if not estimates:
        print(f"  No estimates found — done")
        return

    # Upsert
    upserted = upsert_estimates(conn, customer_id, location_id, estimates)
    print(f"    → {upserted} estimates upserted")

    # Status breakdown
    status_counts = {}
    for est in estimates:
        s = est.get("estimateStatus", "unknown")
        status_counts[s] = status_counts.get(s, 0) + 1

    breakdown = ", ".join(f"{s}: {c}" for s, c in sorted(status_counts.items()))
    print(f"    → Statuses: {breakdown}")


def main():
    parser = argparse.ArgumentParser(description="Pull GHL estimates into Postgres")
    parser.add_argument("--client", type=int, help="Single client customer_id")
    args = parser.parse_args()

    conn = get_db()

    # Ensure table exists
    ensure_schema(conn)

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    if args.client:
        cur.execute("""
            SELECT * FROM clients
            WHERE customer_id = %s
              AND ghl_api_key IS NOT NULL
              AND ghl_location_id IS NOT NULL
        """, (args.client,))
    else:
        cur.execute("""
            SELECT * FROM clients
            WHERE ghl_api_key IS NOT NULL
              AND ghl_location_id IS NOT NULL
              AND ghl_location_id != ''
              AND field_management_software IN ('none', 'ghl')
              AND status = 'active'
            ORDER BY name
        """)

    clients = cur.fetchall()
    if not clients:
        print("No GHL clients found (need ghl_api_key + ghl_location_id + field_management_software in 'none'/'ghl')")
        sys.exit(0)

    print(f"\n{'═' * 60}")
    print(f"  GHL Estimates ETL — {len(clients)} client(s)")
    print(f"{'═' * 60}")

    for client in clients:
        try:
            pull_client(conn, client)
        except Exception as e:
            print(f"  FATAL ERROR for {client['name']}: {e}")

    conn.close()
    print(f"\n{'═' * 60}")
    print(f"  GHL Estimates ETL complete")
    print(f"{'═' * 60}\n")


if __name__ == "__main__":
    main()
