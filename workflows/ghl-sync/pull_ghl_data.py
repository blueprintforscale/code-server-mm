#!/usr/bin/env python3
"""
GHL ETL — Pull contacts, opportunities, and pipeline stages from GoHighLevel.

Usage:
  python3 pull_ghl_data.py                       # All GHL clients
  python3 pull_ghl_data.py --client 7123434733   # Single client by customer_id

Enriches existing CallRail/HCP/Jobber data with pipeline stage and lost reason.
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


def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def normalize_phone(phone):
    if not phone:
        return None
    digits = re.sub(r'\D', '', phone)
    if len(digits) >= 10:
        return digits[-10:]
    return digits if digits else None


# ── GHL API ───────────────────────────────────────────────────

def ghl_request(method, path, api_key, params=None, body=None, retries=3):
    """Make a GHL API request."""
    url = f"{GHL_BASE}{path}"
    if params:
        qs = "&".join(f"{k}={urllib.request.quote(str(v))}" for k, v in params.items())
        url = f"{url}?{qs}"

    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url,
        data=data,
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
            return ghl_request(method, path, api_key, params, body, retries - 1)
        body_text = e.read().decode() if hasattr(e, 'read') else str(e)
        raise Exception(f"GHL API error {e.code}: {body_text}")


def fetch_pipelines(api_key, location_id):
    """Fetch all pipelines and stages for a location."""
    data = ghl_request("GET", "/opportunities/pipelines", api_key, {"locationId": location_id})
    return data.get("pipelines", [])


def fetch_opportunities(api_key, location_id, pipeline_id=None):
    """Fetch all opportunities for a location, deduplicated."""
    all_opps = []
    seen_ids = set()
    consecutive_dupes = 0
    params = {"location_id": location_id, "limit": "100"}
    if pipeline_id:
        params["pipeline_id"] = pipeline_id

    while True:
        data = ghl_request("GET", "/opportunities/search", api_key, params)
        opps = data.get("opportunities", [])

        if len(opps) == 0:
            break

        # Deduplicate
        new_opps = [o for o in opps if o.get("id") and o["id"] not in seen_ids]
        for o in new_opps:
            seen_ids.add(o["id"])
        all_opps.extend(new_opps)

        meta = data.get("meta", {})
        total = meta.get("total", "?")

        if len(new_opps) == 0:
            consecutive_dupes += 1
            if consecutive_dupes >= 3:
                print(f"    [STOP] 3 consecutive pages of duplicates — done")
                break
        else:
            consecutive_dupes = 0
            print(f"    Fetched {len(all_opps)}/{total} ({len(seen_ids)} unique)...")

        next_after = meta.get("startAfterId")
        if not next_after:
            break

        params["startAfterId"] = next_after
        time.sleep(0.1)

    return all_opps


def stream_contacts(api_key, location_id, conn, customer_id, field_map):
    """Fetch and upsert contacts in streaming batches. Returns total count."""
    start_after_id = None
    total_upserted = 0
    seen_contact_ids = set()
    consecutive_dupes = 0

    while True:
        params = {"locationId": location_id, "limit": "100"}
        if start_after_id:
            params["startAfterId"] = start_after_id

        data = ghl_request("GET", "/contacts/", api_key, params)
        contacts = data.get("contacts", [])

        if len(contacts) == 0:
            break

        # Filter out contacts we've already seen (GHL returns dupes)
        new_contacts = []
        for c in contacts:
            cid = c.get("id")
            if cid and cid not in seen_contact_ids:
                seen_contact_ids.add(cid)
                new_contacts.append(c)

        if len(new_contacts) == 0:
            consecutive_dupes += 1
            if consecutive_dupes >= 3:
                print(f"    [STOP] 3 consecutive pages of duplicates — done")
                break
            next_cursor = data.get("meta", {}).get("startAfterId")
            if next_cursor:
                start_after_id = next_cursor
            else:
                break
            time.sleep(0.1)
            continue

        consecutive_dupes = 0

        # Upsert this batch immediately
        batch_count = upsert_contacts(conn, customer_id, new_contacts, field_map)
        total_upserted += batch_count

        meta = data.get("meta", {})
        total = meta.get("total", "?")
        print(f"    Fetched & saved {total_upserted}/{total} ({len(seen_contact_ids)} unique)...")

        next_cursor = meta.get("startAfterId")
        if not next_cursor:
            break

        start_after_id = next_cursor
        time.sleep(0.1)

    return total_upserted


def fetch_custom_fields(api_key, location_id):
    """Fetch custom field definitions to build ID->name mapping."""
    data = ghl_request("GET", f"/locations/{location_id}/customFields", api_key)
    fields = data.get("customFields", [])
    return {f["id"]: f["name"] for f in fields}


# ── Upsert Functions ─────────────────────────────────────────

def upsert_pipelines(conn, customer_id, pipelines):
    """Cache pipeline/stage definitions."""
    cur = conn.cursor()
    count = 0
    for pipeline in pipelines:
        pid = pipeline["id"]
        pname = pipeline.get("name", "")
        for i, stage in enumerate(pipeline.get("stages", [])):
            cur.execute("""
                INSERT INTO ghl_pipelines (customer_id, ghl_pipeline_id, pipeline_name, ghl_stage_id, stage_name, stage_order)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (customer_id, ghl_pipeline_id, ghl_stage_id) DO UPDATE SET
                    pipeline_name = EXCLUDED.pipeline_name,
                    stage_name = EXCLUDED.stage_name,
                    stage_order = EXCLUDED.stage_order,
                    synced_at = NOW()
            """, (customer_id, pid, pname, stage["id"], stage.get("name", ""), i))
            count += 1
    conn.commit()
    return count


def upsert_contacts(conn, customer_id, contacts, field_map):
    """Upsert GHL contacts with custom field extraction."""
    cur = conn.cursor()
    count = 0

    # Find custom field IDs by name
    lost_reason_field_id = None
    gclid_field_id = None
    for fid, fname in field_map.items():
        fl = fname.lower()
        if "lost reason" in fl:
            lost_reason_field_id = fid
        elif "google click" in fl or "gclid" in fl or fname == "Google Click ID":
            gclid_field_id = fid

    for c in contacts:
        ghl_id = c.get("id")
        if not ghl_id:
            continue

        phone = c.get("phone", "")
        phone_norm = normalize_phone(phone)

        # Extract custom field values
        lost_reason = None
        gclid = None
        for cf in (c.get("customFields") or []):
            cf_id = cf.get("id")
            cf_val = cf.get("field_value") or cf.get("value")
            if cf_id == lost_reason_field_id:
                lost_reason = cf_val
            elif cf_id == gclid_field_id:
                gclid = cf_val if isinstance(cf_val, str) and cf_val else None

        # Also check lastAttributionSource.gclid as fallback
        if not gclid:
            attr = c.get("lastAttributionSource") or {}
            gclid = attr.get("gclid") if attr.get("gclid") else None

        tags = c.get("tags", [])

        cur.execute("""
            INSERT INTO ghl_contacts (
                ghl_contact_id, customer_id, first_name, last_name,
                email, phone, phone_normalized, tags, source,
                date_added, lost_reason, gclid
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ghl_contact_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                phone_normalized = EXCLUDED.phone_normalized,
                tags = EXCLUDED.tags,
                source = EXCLUDED.source,
                lost_reason = EXCLUDED.lost_reason,
                gclid = COALESCE(EXCLUDED.gclid, ghl_contacts.gclid),
                updated_at = NOW()
        """, (
            ghl_id, customer_id,
            c.get("firstName"), c.get("lastName"),
            c.get("email"), phone, phone_norm,
            tags if tags else None,
            c.get("source"),
            c.get("dateAdded"),
            lost_reason,
            gclid,
        ))
        count += 1

        # Commit every 200 contacts to avoid losing progress
        if count % 200 == 0:
            conn.commit()

    conn.commit()
    return count


def upsert_opportunities(conn, customer_id, opportunities, stage_map):
    """Upsert GHL opportunities with resolved stage names."""
    cur = conn.cursor()
    count = 0

    for opp in opportunities:
        ghl_id = opp.get("id")
        if not ghl_id:
            continue

        raw_contact_id = opp.get("contact", {}).get("id") if isinstance(opp.get("contact"), dict) else opp.get("contactId")
        # Only set FK if contact exists in our table (we may not have all contacts)
        contact_id = None
        if raw_contact_id:
            cur.execute("SELECT 1 FROM ghl_contacts WHERE ghl_contact_id = %s", (raw_contact_id,))
            if cur.fetchone():
                contact_id = raw_contact_id
        pipeline_id = opp.get("pipelineId", "")
        stage_id = opp.get("pipelineStageId", "")

        # Resolve stage name from cached lookup
        stage_name = stage_map.get(stage_id, "")
        pipeline_name = ""
        for key, val in stage_map.items():
            if key == f"pipeline_{pipeline_id}":
                pipeline_name = val
                break

        cur.execute("""
            INSERT INTO ghl_opportunities (
                ghl_opportunity_id, customer_id, ghl_contact_id,
                ghl_pipeline_id, ghl_stage_id, pipeline_name, stage_name,
                status, lost_reason, monetary_value,
                name, source, assigned_to, date_added
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ghl_opportunity_id) DO UPDATE SET
                ghl_stage_id = EXCLUDED.ghl_stage_id,
                stage_name = EXCLUDED.stage_name,
                status = EXCLUDED.status,
                lost_reason = EXCLUDED.lost_reason,
                monetary_value = EXCLUDED.monetary_value,
                assigned_to = EXCLUDED.assigned_to,
                updated_at = NOW()
        """, (
            ghl_id, customer_id, contact_id,
            pipeline_id, stage_id, pipeline_name, stage_name,
            opp.get("status"), opp.get("lostReasonId") or opp.get("lostReason"),
            opp.get("monetaryValue"),
            opp.get("name"), opp.get("source"),
            opp.get("assignedTo"),
            opp.get("dateAdded"),
        ))
        count += 1

    conn.commit()
    return count


def match_callrail(conn, customer_id):
    """Match GHL contacts to CallRail calls by phone."""
    cur = conn.cursor()
    cur.execute("""
        UPDATE ghl_contacts gc
        SET callrail_id = c.callrail_id,
            match_method = 'phone_normalized'
        FROM calls c
        WHERE gc.customer_id = %s
          AND gc.callrail_id IS NULL
          AND gc.phone_normalized IS NOT NULL
          AND gc.phone_normalized != ''
          AND normalize_phone(c.caller_phone) = gc.phone_normalized
          AND c.callrail_company_id = (
              SELECT callrail_company_id FROM clients WHERE customer_id = %s
          )
    """, (customer_id, customer_id))
    matched = cur.rowcount
    conn.commit()
    return matched


# ── Main ETL ─────────────────────────────────────────────────

def pull_client(conn, client_row):
    """Pull all GHL data for one client."""
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

    start_ms = int(time.time() * 1000)
    errors = []

    # Step 1: Pipelines & stages
    print(f"  [1/4] Pulling pipelines...")
    try:
        pipelines = fetch_pipelines(api_key, location_id)
        pipeline_count = upsert_pipelines(conn, customer_id, pipelines)
        print(f"    → {pipeline_count} stages cached")

        # Build stage lookup: stage_id -> stage_name
        stage_map = {}
        for p in pipelines:
            stage_map[f"pipeline_{p['id']}"] = p.get("name", "")
            for s in p.get("stages", []):
                stage_map[s["id"]] = s.get("name", "")
    except Exception as e:
        errors.append(f"Pipelines: {e}")
        print(f"    ERROR: {e}")
        stage_map = {}
        pipeline_count = 0

    # Step 2: Custom field map
    print(f"  [2/4] Pulling custom fields...")
    try:
        field_map = fetch_custom_fields(api_key, location_id)
        print(f"    → {len(field_map)} fields mapped")
    except Exception as e:
        errors.append(f"Custom fields: {e}")
        print(f"    ERROR: {e}")
        field_map = {}

    # Step 3: Contacts (streamed — fetch + upsert in batches)
    print(f"  [3/4] Pulling contacts...")
    contacts_count = 0
    try:
        contacts_count = stream_contacts(api_key, location_id, conn, customer_id, field_map)
        print(f"    → {contacts_count} contacts upserted")
    except Exception as e:
        errors.append(f"Contacts: {e}")
        print(f"    ERROR: {e}")

    # Step 4: Opportunities
    print(f"  [4/4] Pulling opportunities...")
    opps_count = 0
    try:
        opportunities = fetch_opportunities(api_key, location_id)
        opps_count = upsert_opportunities(conn, customer_id, opportunities, stage_map)
        print(f"    → {opps_count} opportunities upserted")
    except Exception as e:
        errors.append(f"Opportunities: {e}")
        print(f"    ERROR: {e}")

    # CallRail matching
    print(f"  [+] Matching to CallRail...")
    matches = match_callrail(conn, customer_id)
    print(f"    → {matches} new matches")

    # Log
    duration_ms = int(time.time() * 1000) - start_ms
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO ghl_pull_log (
            customer_id, contacts_upserted, opportunities_upserted,
            pipelines_synced, callrail_matches, errors, duration_ms
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        customer_id, contacts_count, opps_count,
        pipeline_count, matches,
        errors if errors else None, duration_ms,
    ))
    conn.commit()

    print(f"  Done in {duration_ms}ms" + (f" ({len(errors)} errors)" if errors else ""))


def main():
    parser = argparse.ArgumentParser(description="Pull GHL data into Postgres")
    parser.add_argument("--client", type=int, help="Single client customer_id")
    args = parser.parse_args()

    conn = get_db()
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
              AND status = 'active'
            ORDER BY name
        """)

    clients = cur.fetchall()
    if not clients:
        print("No GHL clients found (need ghl_api_key + ghl_location_id)")
        sys.exit(0)

    print(f"\n{'═' * 60}")
    print(f"  GHL ETL — {len(clients)} client(s)")
    print(f"{'═' * 60}")

    for client in clients:
        try:
            pull_client(conn, client)
        except Exception as e:
            print(f"  FATAL ERROR for {client['name']}: {e}")

    conn.close()
    print(f"\n{'═' * 60}")
    print(f"  GHL ETL complete")
    print(f"{'═' * 60}\n")


if __name__ == "__main__":
    main()
