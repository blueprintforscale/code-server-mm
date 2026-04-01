#!/usr/bin/env python3
"""
GHL Client Accounts ETL — Pull client account data and associated contacts
from the Blueprint GHL sub-account custom objects.

Usage:
  python3 pull_ghl_client_accounts.py
  python3 pull_ghl_client_accounts.py --client 7123434733

Pulls from the custom_objects.client_accounts object and its associations
(owners, employees, dispatchers, GA managers) to populate client_profiles
and client_contacts tables.
"""

import argparse
import json
import logging
import re
import sys
import time

import psycopg2
import psycopg2.extras
import urllib.request
import urllib.error

DB_CONFIG = {
    "dbname": "blueprint",
    "user": "blueprint",
    "host": "localhost",
    "port": 5432,
}

GHL_BASE = "https://services.leadconnectorhq.com"
GHL_VERSION = "2021-07-28"
BLUEPRINT_API_KEY = "pit-7236ab70-f631-4e4b-8488-942c7f69d4b4"
BLUEPRINT_LOCATION_ID = "1Rq6VtK6nhZbR1KzHHqt"

RATE_LIMIT_DELAY = 0.5

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger('ghl-accounts')


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


def ghl_request(method, path, params=None, body=None, retries=3):
    url = "%s%s" % (GHL_BASE, path)
    if params:
        qs = "&".join("%s=%s" % (k, urllib.request.quote(str(v))) for k, v in params.items())
        url = "%s?%s" % (url, qs)

    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, headers={
        "Authorization": "Bearer %s" % BLUEPRINT_API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Version": GHL_VERSION,
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    }, method=method)

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 429 and retries > 0:
            log.warning("  Rate limited, waiting 10s...")
            time.sleep(10)
            return ghl_request(method, path, params, body, retries - 1)
        body_text = e.read().decode()[:300]
        log.error("  GHL API error %d: %s" % (e.code, body_text))
        return None
    except Exception as e:
        log.error("  Request error: %s" % e)
        return None


def fetch_client_accounts():
    """Fetch all client_accounts custom object records."""
    all_records = []
    page = 1
    while True:
        data = ghl_request("POST", "/objects/custom_objects.client_accounts/records/search",
                          body={"locationId": BLUEPRINT_LOCATION_ID, "page": page, "pageLimit": 50})
        if not data:
            break
        records = data.get("records", [])
        all_records.extend(records)
        if len(records) < 50:
            break
        page += 1
        time.sleep(RATE_LIMIT_DELAY)
    return all_records


def fetch_record_associations(record_id, association_key):
    """DEPRECATED — associations endpoint no longer works. Use relations from search results."""
    return []


def fetch_contact_detail(contact_id):
    """Fetch full contact details by ID."""
    data = ghl_request("GET", "/contacts/%s" % contact_id)
    if not data:
        return None
    return data.get("contact", data)


def match_account_to_client(account_title, clients):
    """Match a GHL client account to a client in our database."""
    title_lower = account_title.lower()
    for cid, name in clients:
        name_parts = name.lower().replace("|", " ").split()
        # Try matching on key name fragments
        significant_parts = [p for p in name_parts if len(p) > 3 and p not in ('pure', 'maintenance', 'mold', 'the', 'and', 'of')]
        matches = sum(1 for p in significant_parts if p in title_lower)
        if matches >= 2 or (matches == 1 and len(significant_parts) == 1):
            return cid
    return None


def main():
    parser = argparse.ArgumentParser(description="Pull GHL client accounts and contacts")
    parser.add_argument('--client', type=str, help='Filter to one customer_id')
    args = parser.parse_args()

    conn = get_db()

    try:
        # Get our client list for matching
        with conn.cursor() as cur:
            cur.execute("SELECT customer_id, name FROM clients WHERE status = 'active' ORDER BY name")
            clients = cur.fetchall()

        log.info("Fetching client accounts from GHL Blueprint sub-account...")
        accounts = fetch_client_accounts()
        log.info("Found %d client accounts" % len(accounts))

        # Association IDs map to roles (discovered from GHL custom object setup)
        # These are the 4 association types defined on the client_accounts object
        ASSOC_ROLE_MAP = {
            "68caf65e6187ca61aa829d40": "owner",       # 51 contacts — most common
            "68cc289d63f5d3fedb139768": "employee",     # 21 contacts
            "68cb65b26187ca745cbac70d": "dispatcher",   # 6 contacts
            "68caf664918925298554c5e4": "ga_manager",   # 6 contacts
        }

        updated_profiles = 0
        added_contacts = 0

        for account in accounts:
            record_id = account.get("id")
            props = account.get("properties", {})
            prefix = "custom_objects.client_accounts."

            account_title = props.get("account_title", "") or props.get(prefix + "account_title", "")
            if not account_title:
                continue

            # Match to our client
            customer_id = match_account_to_client(account_title, clients)
            if not customer_id:
                log.warning("  Could not match: %s" % account_title)
                continue

            if args.client and str(customer_id) != args.client:
                continue

            log.info("\n  %s -> customer_id %d" % (account_title, customer_id))

            # Extract profile fields
            def prop(key, default=""):
                return props.get(key, props.get(prefix + key, default))

            ads_budget = prop("ads_budget_monthly")
            phase = prop("phase")
            phone_system = prop("phone_system", [])
            if isinstance(phone_system, list):
                phone_system = ", ".join(phone_system)
            phone_answerer = prop("phone_answerer")
            call_time = prop("monthly_call_time_mdt")
            call_date = prop("date_flexible_format")
            status = prop("status")
            if isinstance(status, list):
                status = ", ".join(status)
            other_names = prop("other_titlesbusiness_names")
            event_emails = prop("event_emails")
            schedule_auto = prop("schedule_events_automatically", [])
            notes = prop("notes")

            # Build preferences string
            preferences_parts = []
            if phone_system:
                preferences_parts.append("Phone system: %s" % phone_system)
            if phone_answerer:
                preferences_parts.append("Phone answerer: %s" % phone_answerer)
            if call_time:
                preferences_parts.append("Monthly call time (MDT): %s" % call_time)
            if call_date:
                preferences_parts.append("Monthly call schedule: %s" % call_date)
            if event_emails:
                preferences_parts.append("Event emails: %s" % event_emails)

            # Build notes string
            notes_parts = []
            if other_names:
                notes_parts.append("Other names: %s" % other_names)
            if ads_budget:
                notes_parts.append("Ads budget: %s/mo" % ads_budget)
            if phase:
                notes_parts.append("Phase: %s" % phase)
            if status:
                notes_parts.append("Status flags: %s" % status)
            if notes:
                notes_parts.append(notes)

            # Update client_profiles
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE client_profiles SET
                        preferences = COALESCE(NULLIF(%s, ''), preferences),
                        notes = COALESCE(NULLIF(%s, ''), notes),
                        updated_at = NOW()
                    WHERE customer_id = %s
                """, (
                    "\n".join(preferences_parts) if preferences_parts else "",
                    "\n".join(notes_parts) if notes_parts else "",
                    customer_id,
                ))
                updated_profiles += 1

            log.info("    Updated profile (phase: %s, budget: %s)" % (phase, ads_budget))

            # Process relations (contacts linked to this account)
            relations = account.get("relations", [])
            for rel in relations:
                contact_id = rel.get("recordId")
                assoc_id = rel.get("associationId", "")
                role_name = ASSOC_ROLE_MAP.get(assoc_id, "contact")

                if not contact_id:
                    continue

                try:
                    contact = fetch_contact_detail(contact_id)
                    time.sleep(RATE_LIMIT_DELAY)

                    if not contact:
                        continue

                    first_name = contact.get("firstName", "")
                    last_name = contact.get("lastName", "")
                    name = ("%s %s" % (first_name, last_name)).strip()
                    email = contact.get("email", "")
                    phone = contact.get("phone", "")
                    phone_norm = normalize_phone(phone)

                    if not name and not email and not phone:
                        continue

                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO client_contacts (
                                customer_id, ghl_contact_id, name, role,
                                phone, phone_normalized, email, is_primary
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (ghl_contact_id) DO UPDATE SET
                                name = COALESCE(NULLIF(EXCLUDED.name, ''), client_contacts.name),
                                role = EXCLUDED.role,
                                phone = COALESCE(NULLIF(EXCLUDED.phone, ''), client_contacts.phone),
                                phone_normalized = COALESCE(NULLIF(EXCLUDED.phone_normalized, ''), client_contacts.phone_normalized),
                                email = COALESCE(NULLIF(EXCLUDED.email, ''), client_contacts.email),
                                updated_at = NOW()
                        """, (
                            customer_id, contact_id,
                            name or email or phone,
                            role_name,
                            phone, phone_norm, email,
                            role_name == "owner",
                        ))
                        added_contacts += 1

                    log.info("    Contact (%s): %s | %s | %s" % (role_name, name, email, phone))

                except Exception as e:
                    log.warning("    Error fetching contact %s: %s" % (contact_id, e))

            conn.commit()

        log.info("\n%s" % ("=" * 60))
        log.info("DONE — %d profiles updated, %d contacts added/updated" % (updated_profiles, added_contacts))
        log.info("=" * 60)

    finally:
        conn.close()


if __name__ == '__main__':
    main()
