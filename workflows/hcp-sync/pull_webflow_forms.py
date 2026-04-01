#!/usr/bin/env python3
"""
Pull form submissions from FormBridge API and sync to webflow_submissions table.
Identifies landing page forms (form_name containing 'LP') as Google Ads leads.

Usage:
    python3 pull_webflow_forms.py              # Pull last 100 submissions
    python3 pull_webflow_forms.py --limit 500  # Pull more
    python3 pull_webflow_forms.py --backfill   # Pull all (up to 5000)
"""

import sys
import re
import json
import argparse
import logging
from urllib.request import Request, urlopen
from urllib.error import HTTPError

import psycopg2

# ============================================================
# Config
# ============================================================

DSN = "host=localhost port=5432 dbname=blueprint user=blueprint"
FORMBRIDGE_URL = "https://formbridge-production-7f19.up.railway.app"
FORMBRIDGE_API_KEY = None  # Set via env or arg; None = no auth (if FormBridge has no key)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger('webflow-pull')


def normalize_phone(phone):
    if not phone:
        return None
    digits = re.sub(r'\D', '', phone)
    if len(digits) < 10:
        return None
    return digits[-10:]


def fetch_submissions(limit=100):
    """Fetch submissions from FormBridge API."""
    url = f"{FORMBRIDGE_URL}/api/submissions?limit={limit}"
    if FORMBRIDGE_API_KEY:
        url += f"&api_key={FORMBRIDGE_API_KEY}"

    req = Request(url)
    try:
        data = json.loads(urlopen(req, timeout=30).read().decode())
        return data if isinstance(data, list) else []
    except HTTPError as e:
        log.error(f"FormBridge API error: {e.code}")
        return []


def extract_contact(mapped_payload):
    """Extract contact fields from FormBridge's mapped payload."""
    if not mapped_payload or not isinstance(mapped_payload, dict):
        return {}
    return {
        'first_name': mapped_payload.get('firstName'),
        'last_name': mapped_payload.get('lastName'),
        'email': mapped_payload.get('email'),
        'phone': mapped_payload.get('phone'),
    }


def extract_ad_attribution(mapping_report):
    """Extract GCLID/UTM from FormBridge's mapping report."""
    if not mapping_report or not isinstance(mapping_report, dict):
        return {}
    aa = mapping_report.get('adAttribution') or {}
    fields = aa.get('fields') or {}
    return {
        'gclid': fields.get('gclid') or aa.get('gclid'),
        'utm_source': fields.get('utmSource') or aa.get('utmSource'),
        'utm_medium': fields.get('utmMedium') or aa.get('utmMedium'),
        'utm_campaign': fields.get('utmCampaign') or aa.get('utmCampaign'),
    }


def sync_submissions(conn, submissions):
    """Upsert FormBridge submissions into webflow_submissions."""
    cur = conn.cursor()
    upserted = 0

    # Build slug → customer_id lookup
    cur.execute("SELECT customer_id, name FROM clients WHERE status = 'active'")
    client_lookup = {}
    for row in cur.fetchall():
        # Create simple slug from name for matching
        slug = re.sub(r'[^a-z0-9]+', '-', row[1].lower()).strip('-')
        client_lookup[slug] = row[0]

    for sub in submissions:
        client_slug = sub.get('client_slug', '')
        form_name = sub.get('form_name', '')
        submitted_at = sub.get('submitted_at') or sub.get('created_at')
        status = sub.get('status', '')

        if not client_slug or not submitted_at:
            continue

        # Skip failed submissions
        if status == 'error':
            continue

        # Parse JSON strings if needed
        mapped_payload = sub.get('mapped_payload')
        if isinstance(mapped_payload, str):
            try: mapped_payload = json.loads(mapped_payload)
            except: mapped_payload = {}

        mapping_report = sub.get('mapping_report')
        if isinstance(mapping_report, str):
            try: mapping_report = json.loads(mapping_report)
            except: mapping_report = {}

        # Extract contact and attribution from mapped payload
        contact = extract_contact(mapped_payload)
        attribution = extract_ad_attribution(mapping_report)

        email = contact.get('email')
        phone = contact.get('phone')
        phone_norm = normalize_phone(phone)

        # Match client slug to customer_id
        customer_id = None
        for db_slug, cid in client_lookup.items():
            # Fuzzy match: check if the FormBridge slug words appear in the DB slug
            if client_slug.replace('-', ' ') in db_slug.replace('-', ' '):
                customer_id = cid
                break

        try:
            cur.execute("SAVEPOINT sp")
            cur.execute("""
                INSERT INTO webflow_submissions (
                    customer_id, client_slug, form_name, first_name, last_name,
                    email, phone, phone_normalized, gclid,
                    utm_source, utm_medium, utm_campaign, submitted_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (client_slug, COALESCE(email, ''), submitted_at)
                DO UPDATE SET
                    gclid = COALESCE(EXCLUDED.gclid, webflow_submissions.gclid),
                    utm_source = COALESCE(EXCLUDED.utm_source, webflow_submissions.utm_source),
                    utm_medium = COALESCE(EXCLUDED.utm_medium, webflow_submissions.utm_medium),
                    utm_campaign = COALESCE(EXCLUDED.utm_campaign, webflow_submissions.utm_campaign)
            """, [
                customer_id, client_slug, form_name or None,
                contact.get('first_name'), contact.get('last_name'),
                email, phone, phone_norm,
                attribution.get('gclid'),
                attribution.get('utm_source'), attribution.get('utm_medium'),
                attribution.get('utm_campaign'), submitted_at,
            ])
            cur.execute("RELEASE SAVEPOINT sp")
            upserted += 1
        except Exception as e:
            cur.execute("ROLLBACK TO SAVEPOINT sp")
            if 'duplicate' not in str(e).lower():
                log.warning(f"  Error: {e}")

    conn.commit()
    return upserted


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=100)
    parser.add_argument('--backfill', action='store_true')
    parser.add_argument('--api-key', type=str, default=None)
    args = parser.parse_args()

    global FORMBRIDGE_API_KEY
    if args.api_key:
        FORMBRIDGE_API_KEY = args.api_key

    limit = 5000 if args.backfill else args.limit
    log.info(f"Pulling up to {limit} submissions from FormBridge...")

    submissions = fetch_submissions(limit)
    log.info(f"Fetched {len(submissions)} submissions")

    if not submissions:
        log.info("Nothing to sync")
        return

    conn = psycopg2.connect(DSN)
    upserted = sync_submissions(conn, submissions)
    conn.close()

    log.info(f"Synced {upserted} submissions")

    # Report LP form stats
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE is_landing_page) FROM webflow_submissions")
    total, lp = cur.fetchone()
    conn.close()
    log.info(f"Total webflow submissions: {total}, Landing page forms: {lp}")


if __name__ == '__main__':
    main()
