#!/usr/bin/env python3
"""
GHL Appointments ETL — Pull calendar appointments from GoHighLevel.

Iterates through ghl_contacts for each GHL-enabled client, pulls appointments
via /contacts/{id}/appointments, and classifies them as 'inspection' or 'job'
based on the calendar name.

Usage:
  python3 pull_ghl_appointments.py                       # All GHL-as-CRM clients
  python3 pull_ghl_appointments.py --client 1714816135   # Single client
  python3 pull_ghl_appointments.py --all-clients         # All clients with GHL API keys
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
GHL_VERSION = "2021-04-15"

# ── Calendar classification ──────────────────────────────────

INSPECTION_KEYWORDS = [
    'inspection', 'air quality', 'air test', 'mold test', 'assessment',
    'estimate', 'consult', 'evaluation', 'survey', 'walkthrough',
]

JOB_KEYWORDS = [
    'treatment', 'dry fog', 'dry vapor', 'remediation', 'removal',
    'abatement', 'encapsulation', 'foggin',
]


def classify_calendar(calendar_name):
    """Classify a calendar as inspection, job, or other based on name keywords."""
    name_lower = (calendar_name or '').lower()
    for kw in JOB_KEYWORDS:
        if kw in name_lower:
            return 'job'
    for kw in INSPECTION_KEYWORDS:
        if kw in name_lower:
            return 'inspection'
    return 'other'


def normalize_phone(phone):
    """Strip non-digits, take last 10."""
    if not phone:
        return None
    digits = re.sub(r'\D', '', phone)
    return digits[-10:] if len(digits) >= 10 else None


# ── GHL API ──────────────────────────────────────────────────

def ghl_get(path, api_key, params=None):
    """Make a GET request to GHL API with rate limit handling."""
    url = f"{GHL_BASE}{path}"
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{qs}"

    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {api_key}",
        "Version": GHL_VERSION,
        "User-Agent": "BlueprintForScale/1.0",
        "Accept": "application/json",
    })

    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 10 * (attempt + 1)
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            elif e.code == 401:
                print(f"    401 Unauthorized — bad API key")
                return None
            elif e.code == 403:
                print(f"    403 Forbidden — insufficient permissions")
                return None
            else:
                print(f"    HTTP {e.code}: {e.read().decode()[:200]}")
                return None
        except Exception as e:
            print(f"    Request error: {e}")
            if attempt < 2:
                time.sleep(2)
                continue
            return None

    return None


def fetch_calendars(location_id, api_key):
    """Fetch all calendars for a location, return {calendar_id: name} mapping."""
    data = ghl_get(f"/calendars/", api_key, {"locationId": location_id})
    if not data or 'calendars' not in data:
        return {}
    return {c['id']: c['name'] for c in data['calendars']}


def fetch_contact_appointments(contact_id, api_key):
    """Fetch all appointments for a single contact."""
    data = ghl_get(f"/contacts/{contact_id}/appointments", api_key)
    if not data:
        return []
    return data.get('events', [])


# ── Main sync ────────────────────────────────────────────────

def sync_client(conn, customer_id, name, location_id, api_key):
    """Sync all appointments for a single client."""
    print(f"\n{'='*60}")
    print(f"Client: {name} ({customer_id})")
    print(f"{'='*60}")

    # Step 1: Fetch calendar names
    print("  Fetching calendars...")
    calendars = fetch_calendars(location_id, api_key)
    if not calendars:
        print("  No calendars found, skipping.")
        return 0

    for cal_id, cal_name in calendars.items():
        appt_type = classify_calendar(cal_name)
        print(f"    {cal_name} → {appt_type} ({cal_id})")

    # Step 2: Get all contacts for this client
    cur = conn.cursor()
    cur.execute(
        "SELECT ghl_contact_id, first_name, last_name, phone FROM ghl_contacts WHERE customer_id = %s",
        (customer_id,)
    )
    contacts = cur.fetchall()
    print(f"  {len(contacts)} contacts to check...")

    # Step 3: Iterate through contacts, fetch appointments
    total_upserted = 0
    contacts_with_appts = 0
    type_counts = {'inspection': 0, 'job': 0, 'other': 0}

    for i, (contact_id, first_name, last_name, phone) in enumerate(contacts):
        events = fetch_contact_appointments(contact_id, api_key)

        if events:
            contacts_with_appts += 1

        for event in events:
            cal_id = event.get('calendarId', '')
            cal_name = calendars.get(cal_id, '')
            appt_type = classify_calendar(cal_name)
            contact_name = ' '.join(filter(None, [first_name, last_name])) or event.get('title', '')
            contact_phone = phone
            phone_norm = normalize_phone(phone)

            # Parse dates
            start_time = event.get('startTime')
            end_time = event.get('endTime')
            date_added = event.get('dateAdded')
            date_updated = event.get('dateUpdated')

            cur.execute("""
                INSERT INTO ghl_appointments (
                    ghl_appointment_id, customer_id, ghl_location_id, ghl_contact_id,
                    ghl_calendar_id, calendar_name, appointment_type,
                    contact_name, contact_phone, phone_normalized,
                    title, status, address,
                    start_time, end_time, date_added, date_updated,
                    deleted, synced_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (ghl_appointment_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    title = EXCLUDED.title,
                    address = EXCLUDED.address,
                    start_time = EXCLUDED.start_time,
                    end_time = EXCLUDED.end_time,
                    date_updated = EXCLUDED.date_updated,
                    deleted = EXCLUDED.deleted,
                    appointment_type = EXCLUDED.appointment_type,
                    synced_at = NOW()
            """, (
                event.get('id'),
                customer_id,
                location_id,
                contact_id,
                cal_id,
                cal_name,
                appt_type,
                contact_name,
                contact_phone,
                phone_norm,
                event.get('title', ''),
                event.get('appointmentStatus', ''),
                event.get('address', ''),
                start_time,
                end_time,
                date_added,
                date_updated,
                event.get('deleted', False),
            ))
            total_upserted += 1
            type_counts[appt_type] = type_counts.get(appt_type, 0) + 1

        # Rate limiting: ~3 requests per second
        if (i + 1) % 10 == 0:
            time.sleep(1)
            conn.commit()

        # Progress
        if (i + 1) % 25 == 0:
            print(f"    Checked {i+1}/{len(contacts)} contacts, {total_upserted} appointments so far...")

    conn.commit()
    print(f"  Done: {total_upserted} appointments from {contacts_with_appts} contacts")
    for t, c in type_counts.items():
        if c > 0:
            print(f"    {t}: {c}")

    return total_upserted


def main():
    parser = argparse.ArgumentParser(description="Pull GHL calendar appointments")
    parser.add_argument("--client", type=int, help="Single customer_id to sync")
    parser.add_argument("--all-clients", action="store_true",
                        help="Sync all clients with GHL API keys (not just GHL-as-CRM)")
    args = parser.parse_args()

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Get clients to sync
    if args.client:
        cur.execute(
            "SELECT customer_id, name, ghl_location_id, ghl_api_key FROM clients "
            "WHERE customer_id = %s AND ghl_api_key IS NOT NULL AND ghl_location_id IS NOT NULL",
            (args.client,)
        )
    elif args.all_clients:
        cur.execute(
            "SELECT customer_id, name, ghl_location_id, ghl_api_key FROM clients "
            "WHERE status = 'active' AND ghl_api_key IS NOT NULL AND ghl_location_id IS NOT NULL "
            "ORDER BY name"
        )
    else:
        # Default: only GHL-as-CRM clients (field_management_software in ('ghl', 'none'))
        cur.execute(
            "SELECT customer_id, name, ghl_location_id, ghl_api_key FROM clients "
            "WHERE status = 'active' AND field_management_software IN ('ghl', 'none') "
            "AND ghl_api_key IS NOT NULL AND ghl_location_id IS NOT NULL "
            "ORDER BY name"
        )

    clients = cur.fetchall()
    if not clients:
        print("No clients to sync.")
        return

    print(f"Syncing {len(clients)} client(s)...")
    total = 0
    for customer_id, name, location_id, api_key in clients:
        try:
            count = sync_client(conn, customer_id, name, location_id, api_key)
            total += count
        except Exception as e:
            print(f"  ERROR: {e}")
            conn.rollback()

    print(f"\n{'='*60}")
    print(f"Total: {total} appointments synced across {len(clients)} clients")
    conn.close()


if __name__ == "__main__":
    main()
