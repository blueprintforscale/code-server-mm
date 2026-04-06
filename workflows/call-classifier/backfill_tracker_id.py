#!/usr/bin/env python3
"""Backfill tracker_id on existing calls from CallRail API."""
import os
import sys
import requests
import psycopg2

DB_DSN = "dbname=blueprint user=blueprint"
DEFAULT_API_KEY = os.environ.get("CALLRAIL_API_KEY", "")
DEFAULT_ACCOUNT_ID = "465371377"

def get_headers(api_key=None):
    return {
        "Authorization": f"Token token={api_key or DEFAULT_API_KEY}",
        "Content-Type": "application/json",
    }

def main():
    db = psycopg2.connect(DB_DSN)
    cur = db.cursor()

    # Get all companies that need backfill
    cur.execute("""
        SELECT DISTINCT c.callrail_company_id,
               cl.callrail_account_id, cl.callrail_api_key,
               cl.name,
               COUNT(*) as call_count
        FROM calls c
        JOIN clients cl ON cl.customer_id = c.customer_id
        WHERE c.tracker_id IS NULL
          AND c.callrail_company_id IS NOT NULL
        GROUP BY c.callrail_company_id, cl.callrail_account_id, cl.callrail_api_key, cl.name
        ORDER BY call_count DESC
    """)
    companies = cur.fetchall()
    print(f"Found {len(companies)} companies to backfill")

    total_updated = 0
    for company_id, account_id, api_key, name, count in companies:
        acct = account_id or DEFAULT_ACCOUNT_ID
        headers = get_headers(api_key)
        print(f"\n{name} ({company_id}): {count} calls to backfill")

        page = 1
        company_updated = 0
        while True:
            url = f"https://api.callrail.com/v3/a/{acct}/calls.json"
            params = {
                "company_id": company_id,
                "start_date": "2025-01-01T00:00:00.000Z",
                "fields": "tracker_id",
                "per_page": 250,
                "page": page,
            }
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=30)
                resp.raise_for_status()
            except Exception as e:
                print(f"  ERROR fetching page {page}: {e}")
                break

            data = resp.json()
            calls = data.get("calls", [])

            for call in calls:
                cid = call.get("id")
                tid = call.get("tracker_id")
                if cid and tid:
                    cur.execute(
                        "UPDATE calls SET tracker_id = %s WHERE callrail_id = %s AND tracker_id IS NULL",
                        (tid, cid)
                    )
                    if cur.rowcount > 0:
                        company_updated += 1

            if page >= data.get("total_pages", 1):
                break
            page += 1

        db.commit()
        total_updated += company_updated
        print(f"  Updated {company_updated} calls")

    print(f"\nTotal updated: {total_updated}")
    db.close()

if __name__ == "__main__":
    main()
