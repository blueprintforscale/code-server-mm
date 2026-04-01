#!/usr/bin/env python3
"""
Generic Sheet → Database sync for clients without direct CRM integration.

Reads a standardized Google Sheet (one per client) and upserts into sheet_leads.
Any client with a sheet_id in the clients table will be synced.

Usage:
  python3 pull_sheet_data.py                       # All sheet-synced clients
  python3 pull_sheet_data.py --client 4229015839   # Single client
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime

import psycopg2
import psycopg2.extras

# ── Google Sheets API ──────────────────────────────────────────────
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_FILE = os.path.join(
    os.path.dirname(SCRIPT_DIR), "client-intelligence", "gmail_credentials.json"
)
TOKENS_DIR = os.path.join(SCRIPT_DIR, "tokens")

DB_CONFIG = {
    "dbname": "blueprint",
    "user": "blueprint",
    "host": "localhost",
    "port": 5432,
}


def normalize_phone(phone):
    if not phone:
        return None
    digits = re.sub(r"\D", "", str(phone))
    return digits[-10:] if len(digits) >= 10 else None


def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def get_sheets_service():
    """Build Google Sheets API service using OAuth tokens."""
    token_file = os.path.join(TOKENS_DIR, "sheets.json")
    creds = None

    if os.path.exists(token_file):
        with open(token_file) as f:
            token_data = json.load(f)
        creds = Credentials.from_authorized_user_info(token_data, SCOPES)

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(token_file, "w") as f:
            f.write(creds.to_json())

    if not creds or not creds.valid:
        print("ERROR: No valid Sheets token. Run sheets_auth.py first.")
        sys.exit(1)

    return build("sheets", "v4", credentials=creds)


def parse_money(val):
    """Parse dollar values like '2,597.00' or '2697' into cents (int)."""
    if not val or val.strip() == "":
        return 0
    cleaned = re.sub(r"[,$\s]", "", val)
    try:
        return int(round(float(cleaned) * 100))
    except (ValueError, TypeError):
        return 0


def parse_count(val):
    """Parse count fields like '1.00' or '1' into int."""
    if not val or val.strip() == "":
        return 0
    try:
        return int(round(float(val.replace(",", ""))))
    except (ValueError, TypeError):
        return 0


def parse_date(val):
    """Parse dates like '7/9/2025', '2025-07-09', or 'March 10, 2026'."""
    if not val or val.strip() == "":
        return None
    # Strip time portion if present (e.g., "3/9/2026 12:22")
    date_str = val.strip().split(" ")[0] if re.match(r"\d+/\d+/\d+", val.strip()) else val.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None


# ── Column mapping ─────────────────────────────────────────────────
# Maps standardized field names to possible header variations (case-insensitive)

COLUMN_MAP = {
    "contact_id": ["contact id"],
    "source": ["source locked", "source formatted", "source"],
    "date_created": ["date created"],
    "first_name": ["first name"],
    "last_name": ["last name"],
    "phone": ["phone"],
    "email": ["email"],
    "status": ["status"],
    "lost_reason": ["lost reason"],
    "scheduled_amt": ["scheduled💸"],  # Total job value scheduled (col K)
    "insp_scheduled_amt": ["insp scheduled  💸", "insp scheduled 💸"],  # Inspection amount (col W)
    "completed_amt": ["completed 💸", "completed💸"],
    "estimate_sent_amt": ["estimate sent 💸", "estimate sent💸"],
    "estimate_approved_amt": ["estimate approved 💸", "estimate approved💸"],
    "estimate_open_amt": ["estimate open 💸", "estimate open💸"],
    "job_not_completed_amt": ["job sch. not completed. 💸", "job sch not completed 💸"],
    "roas_rev_amt": ["roas rev 💸", "roas rev💸"],
    "insp_scheduled": ["insp scheduled", "insp. scheduled"],
    "insp_completed": ["insp. completed", "insp completed"],
    "estimate_sent": ["estimate sent"],
    "estimate_approved": ["estimate app.", "estimate app", "estimate approved"],
    "job_scheduled": ["job scheduled"],
    "job_completed": ["job completed"],
    "spam": ["spam (1)", "spam"],
    "lead": ["lead (1)", "lead"],
}

# Status values that indicate spam/not-quality
SPAM_STATUSES = {"abandoned", "spam", "not a lead", "wrong number", "out of area", "wrong service"}


def map_columns(headers):
    """Find column indices for each field based on header names."""
    mapping = {}
    header_lower = [h.strip().lower() for h in headers]

    for field, variants in COLUMN_MAP.items():
        for variant in variants:
            # Try exact match first
            if variant in header_lower:
                mapping[field] = header_lower.index(variant)
                break
        # Try partial match if exact didn't work
        if field not in mapping:
            for variant in variants:
                for i, h in enumerate(header_lower):
                    if variant in h and i not in mapping.values():
                        mapping[field] = i
                        break
                if field in mapping:
                    break

    return mapping


def get_val(row, mapping, field, default=""):
    """Safely get a value from a row using the column mapping."""
    idx = mapping.get(field)
    if idx is None or idx >= len(row):
        return default
    return row[idx] or default


def sync_client(service, conn, customer_id, sheet_id, sheet_tab):
    """Sync one client's sheet to database."""
    cur = conn.cursor()

    range_spec = f"{sheet_tab}!A1:AZ" if sheet_tab else "A1:AZ"
    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=sheet_id, range=range_spec)
            .execute()
        )
    except Exception as e:
        print(f"    ERROR reading sheet: {e}")
        return 0

    rows = result.get("values", [])
    if len(rows) < 2:
        print("    No data rows found")
        return 0

    headers = rows[0]
    mapping = map_columns(headers)

    # Verify we have minimum required fields
    required = ["contact_id", "phone", "date_created"]
    missing = [f for f in required if f not in mapping]
    if missing:
        print(f"    ERROR: Missing required columns: {missing}")
        print(f"    Found columns: {list(mapping.keys())}")
        return 0

    count = 0
    for row in rows[1:]:
        contact_id = get_val(row, mapping, "contact_id")
        if not contact_id:
            continue

        phone = get_val(row, mapping, "phone")
        phone_norm = normalize_phone(phone)
        date_created = parse_date(get_val(row, mapping, "date_created"))
        status = get_val(row, mapping, "status").strip()
        lost_reason = get_val(row, mapping, "lost_reason").strip()

        # Determine if spam:
        # 1. Column "Spam (1)": 1 = NOT spam, 0/empty = spam (misleading column name)
        # 2. Fallback: status or lost_reason keywords
        spam_flag = get_val(row, mapping, "spam").strip()
        if spam_flag and spam_flag != "":
            # If spam column exists and has a value, 1 = legitimate, anything else = spam
            is_spam = parse_count(spam_flag) != 1
        else:
            is_spam = (
                status.lower() in SPAM_STATUSES
                or any(kw in lost_reason.lower() for kw in SPAM_STATUSES if kw)
            )

        cur.execute("""
            INSERT INTO sheet_leads (
                customer_id, contact_id, source, date_created,
                first_name, last_name, phone, phone_normalized, email,
                status, lost_reason, is_spam,
                scheduled_amt, insp_scheduled_amt, completed_amt,
                estimate_sent_amt, estimate_approved_amt, estimate_open_amt,
                job_not_completed_amt, roas_rev_amt,
                insp_scheduled, insp_completed, estimate_sent,
                estimate_approved, job_scheduled, job_completed
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (customer_id, contact_id) DO UPDATE SET
                source = EXCLUDED.source,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                phone = EXCLUDED.phone,
                phone_normalized = EXCLUDED.phone_normalized,
                email = EXCLUDED.email,
                status = EXCLUDED.status,
                lost_reason = EXCLUDED.lost_reason,
                is_spam = EXCLUDED.is_spam,
                scheduled_amt = EXCLUDED.scheduled_amt,
                insp_scheduled_amt = EXCLUDED.insp_scheduled_amt,
                completed_amt = EXCLUDED.completed_amt,
                estimate_sent_amt = EXCLUDED.estimate_sent_amt,
                estimate_approved_amt = EXCLUDED.estimate_approved_amt,
                estimate_open_amt = EXCLUDED.estimate_open_amt,
                job_not_completed_amt = EXCLUDED.job_not_completed_amt,
                roas_rev_amt = EXCLUDED.roas_rev_amt,
                insp_scheduled = EXCLUDED.insp_scheduled,
                insp_completed = EXCLUDED.insp_completed,
                estimate_sent = EXCLUDED.estimate_sent,
                estimate_approved = EXCLUDED.estimate_approved,
                job_scheduled = EXCLUDED.job_scheduled,
                job_completed = EXCLUDED.job_completed,
                updated_at = NOW()
        """, (
            customer_id, contact_id,
            get_val(row, mapping, "source"),
            date_created,
            get_val(row, mapping, "first_name"),
            get_val(row, mapping, "last_name"),
            phone, phone_norm,
            get_val(row, mapping, "email"),
            status, lost_reason, is_spam,
            parse_money(get_val(row, mapping, "scheduled_amt")),
            parse_money(get_val(row, mapping, "insp_scheduled_amt")),
            parse_money(get_val(row, mapping, "completed_amt")),
            parse_money(get_val(row, mapping, "estimate_sent_amt")),
            parse_money(get_val(row, mapping, "estimate_approved_amt")),
            parse_money(get_val(row, mapping, "estimate_open_amt")),
            parse_money(get_val(row, mapping, "job_not_completed_amt")),
            parse_money(get_val(row, mapping, "roas_rev_amt")),
            parse_count(get_val(row, mapping, "insp_scheduled")),
            parse_count(get_val(row, mapping, "insp_completed")),
            parse_count(get_val(row, mapping, "estimate_sent")),
            parse_count(get_val(row, mapping, "estimate_approved")),
            parse_count(get_val(row, mapping, "job_scheduled")),
            parse_count(get_val(row, mapping, "job_completed")),
        ))
        count += 1

    conn.commit()
    return count


def main():
    parser = argparse.ArgumentParser(description="Sync client sheets to database")
    parser.add_argument("--client", type=int, help="Single customer_id to sync")
    args = parser.parse_args()

    print(f"{'═' * 60}")
    print(f"Sheet Sync — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'═' * 60}")

    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Get clients with sheet_id configured
    if args.client:
        cur.execute(
            "SELECT customer_id, name, sheet_id, sheet_tab FROM clients WHERE customer_id = %s AND sheet_id IS NOT NULL",
            (args.client,),
        )
    else:
        cur.execute(
            "SELECT customer_id, name, sheet_id, sheet_tab FROM clients WHERE sheet_id IS NOT NULL AND status = 'active' ORDER BY name"
        )

    clients = cur.fetchall()
    if not clients:
        print("No clients with sheet_id configured.")
        return

    print(f"Found {len(clients)} client(s) to sync\n")

    service = get_sheets_service()

    for client in clients:
        cid = client["customer_id"]
        name = client["name"]
        sheet_id = client["sheet_id"]
        sheet_tab = client["sheet_tab"]
        print(f"  {name} ({cid})")
        print(f"    Sheet: {sheet_id} tab: {sheet_tab or '(default)'}")

        count = sync_client(service, conn, cid, sheet_id, sheet_tab)
        print(f"    → {count} leads upserted")

    conn.close()
    print(f"\n{'═' * 60}")
    print(f"Done — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'═' * 60}")


if __name__ == "__main__":
    main()
