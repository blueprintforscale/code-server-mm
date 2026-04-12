#!/usr/bin/env python3
"""
CallRail Form Submissions Pull — Fetches form submissions from CallRail API,
upserts into PostgreSQL.

Usage:
    python3 pull_callrail_forms.py                    # Pull last 7 days for all clients
    python3 pull_callrail_forms.py --client X         # Pull one client by customer_id
    python3 pull_callrail_forms.py --days 90          # Look back 90 days
    python3 pull_callrail_forms.py --backfill         # Full pull (365 days)

Cron: daily at 5:30am (before HCP sync)
"""

import sys
import re
import time
import json
import argparse
import logging
from datetime import datetime, timedelta
from urllib.request import Request, urlopen
from urllib.error import HTTPError

import psycopg2

# ============================================================
# Config
# ============================================================

DSN = "host=localhost port=5432 dbname=blueprint user=blueprint"
LOOKBACK_DAYS = 7
CALLRAIL_API_BASE = "https://api.callrail.com/v3"
RATE_LIMIT_DELAY = 0.3

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger('form-pull')


def normalize_phone(phone):
    if not phone:
        return None
    digits = re.sub(r'\D', '', phone)
    if len(digits) < 10:
        return None
    return digits[-10:]


def pull_forms_for_account(conn, account_id, api_key, client_map, start_date, end_date):
    """Pull all form submissions for a CallRail account."""
    headers = {'Authorization': f'Token token={api_key}'}
    all_forms = []
    page = 1

    while True:
        url = (
            f"{CALLRAIL_API_BASE}/a/{account_id}/form_submissions.json"
            f"?per_page=250&page={page}"
            f"&start_date={start_date}&end_date={end_date}"
            f"&fields=customer_phone_number,customer_name,customer_email,"
            f"submitted_at,utm_source,utm_medium,utm_campaign,source,medium,"
            f"landing_page_url,company_id,company_name,first_form,form_name"
        )
        req = Request(url)
        req.add_header('Authorization', f'Token token={api_key}')

        try:
            data = json.loads(urlopen(req, timeout=30).read().decode())
        except HTTPError as e:
            log.error(f"  API error on page {page}: {e.code}")
            break

        forms = data.get('form_submissions', [])
        all_forms.extend(forms)

        total_pages = data.get('total_pages', 1)
        if page >= total_pages:
            break
        page += 1
        time.sleep(RATE_LIMIT_DELAY)

    log.info(f"  Fetched {len(all_forms)} forms")

    # Upsert
    upserted = 0
    with conn.cursor() as cur:
        for f in all_forms:
            company_id = f.get('company_id')
            client = client_map.get(company_id)
            if not client:
                continue  # Not one of our clients

            customer_id = client['customer_id']

            # Extract GCLID from landing page URL
            gclid = None
            landing = f.get('landing_page_url', '') or ''
            if 'gclid=' in landing:
                m = re.search(r'gclid=([^&]+)', landing)
                if m:
                    gclid = m.group(1)

            source = f.get('source', '') or f.get('utm_source', '')
            medium = f.get('medium', '') or f.get('utm_medium', '')

            try:
                cur.execute("SAVEPOINT sp")
                cur.execute("""
                    INSERT INTO form_submissions (
                        callrail_id, customer_id, callrail_company_id,
                        customer_phone, customer_email, customer_name,
                        submitted_at, source, medium, gclid, form_name, phone_normalized
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (callrail_id) DO UPDATE SET
                        customer_phone = COALESCE(EXCLUDED.customer_phone, form_submissions.customer_phone),
                        customer_email = COALESCE(EXCLUDED.customer_email, form_submissions.customer_email),
                        customer_name = COALESCE(EXCLUDED.customer_name, form_submissions.customer_name),
                        source = COALESCE(EXCLUDED.source, form_submissions.source),
                        medium = COALESCE(EXCLUDED.medium, form_submissions.medium),
                        gclid = COALESCE(EXCLUDED.gclid, form_submissions.gclid),
                        form_name = COALESCE(EXCLUDED.form_name, form_submissions.form_name),
                        phone_normalized = COALESCE(EXCLUDED.phone_normalized, form_submissions.phone_normalized),
                        updated_at = NOW()
                """, [
                    f['id'], customer_id, company_id,
                    f.get('customer_phone_number') or None,
                    f.get('customer_email') or None,
                    f.get('customer_name') or None,
                    f.get('submitted_at'),
                    source or None, medium or None, gclid,
                    f.get('form_name') or None,
                    normalize_phone(f.get('customer_phone_number')),
                ])
                cur.execute("RELEASE SAVEPOINT sp")
                upserted += 1
            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT sp")

        conn.commit()

    log.info(f"  Upserted {upserted} forms")
    return upserted


def main():
    parser = argparse.ArgumentParser(description='Pull CallRail form submissions')
    parser.add_argument('--client', type=str, help='Pull only this customer_id')
    parser.add_argument('--days', type=int, default=LOOKBACK_DAYS, help=f'Lookback days (default {LOOKBACK_DAYS})')
    parser.add_argument('--backfill', action='store_true', help='Full historical pull (365 days)')
    args = parser.parse_args()

    if args.backfill:
        args.days = 365

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')

    conn = psycopg2.connect(DSN)
    conn.autocommit = False

    try:
        # Get CallRail accounts and build client map
        with conn.cursor() as cur:
            cur.execute("""
                SELECT customer_id, name, callrail_company_id, callrail_account_id, callrail_api_key,
                       additional_callrail_company_ids
                FROM clients WHERE status = 'active'
            """)
            clients = cur.fetchall()

        # Build company_id -> client map (includes additional_callrail_company_ids,
        # all stored under the primary customer_id)
        client_map = {}
        for cid, name, comp_id, acc_id, api_key, extras in clients:
            if comp_id:
                client_map[comp_id] = {'customer_id': cid, 'name': name}
            for extra in (extras or []):
                client_map[extra] = {'customer_id': cid, 'name': name}

        # Get unique accounts
        # Use shared account from env or per-client keys
        accounts = {}

        # Check for shared account (from CALLRAIL_ACCOUNTS env or hardcoded)
        import os
        cr_accounts = os.environ.get('CALLRAIL_ACCOUNTS', '465371377:1ebea6df680ecb1937101d46710279b8')
        for pair in cr_accounts.split(','):
            parts = pair.strip().split(':')
            if len(parts) == 2:
                accounts[parts[0]] = parts[1]

        # Also add per-client accounts
        for cid, name, comp_id, acc_id, api_key in clients:
            if acc_id and api_key and acc_id not in accounts:
                accounts[acc_id] = api_key

        if args.client:
            # Filter client_map to just the requested client
            client_map = {k: v for k, v in client_map.items() if str(v['customer_id']) == args.client}

        log.info(f"Pulling forms from {len(accounts)} account(s), {len(client_map)} client(s), {start_date} to {end_date}")

        total = 0
        for acc_id, api_key in accounts.items():
            log.info(f"--- Account {acc_id} ---")
            total += pull_forms_for_account(conn, acc_id, api_key, client_map, start_date, end_date)
            time.sleep(1)

        log.info(f"All done. {total} forms upserted.")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
