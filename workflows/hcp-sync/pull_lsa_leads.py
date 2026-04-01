#!/Users/bp/projects/workflows/call-classifier/.venv/bin/python3
"""
LSA Lead Pull — Fetches Local Service Ads leads from Google Ads API,
upserts into PostgreSQL, and matches to HCP customers + CallRail calls.

Usage:
    python3 pull_lsa_leads.py              # Pull all clients with LSA
    python3 pull_lsa_leads.py --client X   # Pull one client by customer_id
    python3 pull_lsa_leads.py --days 90    # Look back 90 days (default 60)

Cron: daily at 7am
"""

import sys
import re
import time
import argparse
import logging
from datetime import datetime, timedelta, timezone

import psycopg2
from google.ads.googleads.client import GoogleAdsClient

# ============================================================
# Config
# ============================================================

DSN = "host=localhost port=5432 dbname=blueprint user=blueprint"
GOOGLE_ADS_YAML = "/Users/bp/projects/.mcp-servers/google_ads_mcp/google-ads.yaml"
LOOKBACK_DAYS = 60

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger('lsa-pull')


def normalize_phone(phone):
    """Strip non-digits, take last 10 digits."""
    if not phone:
        return None
    digits = re.sub(r'\D', '', phone)
    if len(digits) < 10:
        return None
    return digits[-10:]


def pull_lsa_for_client(ga_client, conn, customer_id, days):
    """Pull LSA leads for a single Google Ads customer."""
    start_ms = time.time()
    stats = {'fetched': 0, 'upserted': 0, 'matches': 0, 'errors': []}

    ga_service = ga_client.get_service("GoogleAdsService")

    # Date range for the query
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    # GAQL query for LSA leads
    query = f"""
        SELECT
            local_services_lead.id,
            local_services_lead.lead_type,
            local_services_lead.creation_date_time,
            local_services_lead.lead_status,
            local_services_lead.category_id,
            local_services_lead.service_id,
            local_services_lead.lead_charged
        FROM local_services_lead
        WHERE local_services_lead.creation_date_time >= '{start_date}'
          AND local_services_lead.creation_date_time <= '{end_date}'
    """

    try:
        cid = str(customer_id).replace('-', '')
        response = ga_service.search(customer_id=cid, query=query)

        leads = []
        for row in response:
            lead = row.local_services_lead
            # Contact details may be empty without Standard API access
            cd = lead.contact_details
            phone = cd.phone_number if cd and cd.phone_number else None
            name = cd.consumer_name if cd and cd.consumer_name else None
            leads.append({
                'id': lead.id,
                'lead_type': lead.lead_type.name if hasattr(lead.lead_type, 'name') else str(lead.lead_type),
                'phone': phone,
                'name': name,
                'creation_time': lead.creation_date_time,
                'status': lead.lead_status.name if hasattr(lead.lead_status, 'name') else str(lead.lead_status),
                'category_id': lead.category_id,
                'service_id': lead.service_id,
                'charged': lead.lead_charged,
                'credit_state': None,
            })

        stats['fetched'] = len(leads)
        log.info(f"  Fetched {len(leads)} LSA leads")

        with conn.cursor() as cur:
            for lead in leads:
                try:
                    phone_norm = normalize_phone(lead['phone'])
                    lsa_id = f"lsa_{customer_id}_{lead['id']}"

                    cur.execute("""
                        INSERT INTO lsa_leads (
                            lsa_lead_id, customer_id,
                            lead_type, lead_status,
                            category_id, service_id,
                            contact_phone, contact_phone_normalized, contact_name,
                            lead_creation_time, lead_charged, credit_state
                        ) VALUES (
                            %(lsa_id)s, %(cust_id)s,
                            %(type)s, %(status)s,
                            %(cat)s, %(svc)s,
                            %(phone)s, %(phone_norm)s, %(name)s,
                            %(created)s, %(charged)s, %(credit)s
                        )
                        ON CONFLICT (lsa_lead_id) DO UPDATE SET
                            lead_status = EXCLUDED.lead_status,
                            lead_charged = EXCLUDED.lead_charged,
                            credit_state = EXCLUDED.credit_state,
                            contact_phone = COALESCE(EXCLUDED.contact_phone, lsa_leads.contact_phone),
                            contact_name = COALESCE(EXCLUDED.contact_name, lsa_leads.contact_name),
                            updated_at = NOW()
                    """, {
                        'lsa_id': lsa_id,
                        'cust_id': customer_id,
                        'type': lead.get('lead_type'),
                        'status': lead.get('status'),
                        'cat': lead.get('category_id'),
                        'svc': lead.get('service_id'),
                        'phone': lead.get('phone'),
                        'phone_norm': phone_norm,
                        'name': lead.get('name'),
                        'created': lead.get('creation_time'),
                        'charged': lead.get('charged', False),
                        'credit': lead.get('credit_state'),
                    })
                    stats['upserted'] += 1

                except Exception as e:
                    stats['errors'].append(f"lead {lead.get('id')}: {e}")
                    log.warning(f"  Error upserting lead {lead.get('id')}: {e}")

            conn.commit()

            # Match to HCP customers by phone
            cur.execute("""
                UPDATE lsa_leads l
                SET hcp_customer_id = hc.hcp_customer_id,
                    match_method = 'phone',
                    updated_at = NOW()
                FROM hcp_customers hc
                WHERE hc.customer_id = l.customer_id
                  AND hc.phone_normalized = l.contact_phone_normalized
                  AND l.customer_id = %(cid)s
                  AND l.hcp_customer_id IS NULL
                  AND l.contact_phone_normalized IS NOT NULL
            """, {'cid': customer_id})
            hcp_matches = cur.rowcount

            # Match to CallRail by timestamp (LSA calls come through CallRail with source_name='LSA')
            # Match within 5-minute window
            cur.execute("""
                UPDATE lsa_leads l
                SET callrail_id = sub.callrail_id,
                    contact_phone = COALESCE(l.contact_phone, sub.caller_phone),
                    contact_phone_normalized = COALESCE(l.contact_phone_normalized, normalize_phone(sub.caller_phone)),
                    contact_name = COALESCE(l.contact_name, sub.customer_name),
                    match_method = 'time_window',
                    updated_at = NOW()
                FROM (
                    SELECT DISTINCT ON (l2.lsa_lead_id)
                        l2.lsa_lead_id, c.callrail_id, c.caller_phone, c.customer_name
                    FROM lsa_leads l2
                    JOIN calls c ON c.customer_id = l2.customer_id
                      AND c.source_name = 'LSA'
                      AND ABS(EXTRACT(EPOCH FROM (c.start_time - l2.lead_creation_time))) < 300
                    WHERE l2.customer_id = %(cid)s
                      AND l2.callrail_id IS NULL
                      AND l2.lead_type IN ('PHONE_CALL', '3')
                    ORDER BY l2.lsa_lead_id, ABS(EXTRACT(EPOCH FROM (c.start_time - l2.lead_creation_time))) ASC
                ) sub
                WHERE l.lsa_lead_id = sub.lsa_lead_id
            """, {'cid': customer_id})
            cr_matches = cur.rowcount

            stats['matches'] = hcp_matches + cr_matches
            conn.commit()
            log.info(f"  Matched {hcp_matches} to HCP, {cr_matches} to CallRail")

    except Exception as e:
        stats['errors'].append(str(e))
        log.error(f"  Error fetching LSA leads: {e}")
        conn.rollback()

    # Log the run
    duration_ms = int((time.time() - start_ms) * 1000)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO lsa_pull_log (customer_id, leads_fetched, leads_upserted, matches_found, errors, duration_ms)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, [customer_id, stats['fetched'], stats['upserted'], stats['matches'],
              stats['errors'] or None, duration_ms])
        conn.commit()

    log.info(f"  Done: {stats['upserted']} leads, {stats['matches']} matches | {duration_ms}ms")
    if stats['errors']:
        log.warning(f"  {len(stats['errors'])} error(s)")

    return stats


def main():
    parser = argparse.ArgumentParser(description='Pull LSA leads from Google Ads API')
    parser.add_argument('--client', type=str, help='Pull only this customer_id')
    parser.add_argument('--days', type=int, default=LOOKBACK_DAYS, help=f'Lookback days (default {LOOKBACK_DAYS})')
    args = parser.parse_args()

    # Initialize Google Ads client
    ga_client = GoogleAdsClient.load_from_storage(GOOGLE_ADS_YAML)

    conn = psycopg2.connect(DSN)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            if args.client:
                cur.execute("""
                    SELECT customer_id, name FROM clients
                    WHERE customer_id = %s AND status = 'active'
                """, [args.client])
            else:
                cur.execute("""
                    SELECT customer_id, name FROM clients
                    WHERE status = 'active' AND has_lsa = true
                    ORDER BY name
                """)
            clients = cur.fetchall()

        log.info(f"Found {len(clients)} client(s), looking back {args.days} days")

        total_errors = 0
        for customer_id, name in clients:
            try:
                log.info(f"--- {name} ---")
                stats = pull_lsa_for_client(ga_client, conn, customer_id, args.days)
                total_errors += len(stats.get('errors', []))
            except Exception as e:
                # Not all clients have LSA — skip gracefully
                if 'is not enabled' in str(e).lower() or 'not found' in str(e).lower() or 'permission' in str(e).lower():
                    log.info(f"  No LSA for this account (skipped)")
                else:
                    log.error(f"  Error for client {customer_id}: {e}")
                    total_errors += 1
                conn.rollback()

            time.sleep(0.5)

        log.info(f"All done. {total_errors} total error(s).")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
