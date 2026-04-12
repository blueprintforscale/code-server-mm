#!/usr/bin/env python3
"""
One-time repair script for NULL-duration calls that were incorrectly auto-spammed.

These calls were fetched mid-call (before CallRail populated duration/transcript),
then the auto-spam rule classified them as spam due to missing transcript.

This script:
1. Re-fetches affected calls from CallRail to get actual duration + transcript
2. Resets their classification so they go through proper AI classification
3. The next cron cycle will classify and upload them to Google Ads

Run: cd /Users/bp/projects/workflows/call-classifier && .venv/bin/python repair_null_duration.py
"""
import sys
import json
from datetime import timedelta, timezone
from pathlib import Path

# Add parent paths for imports
sys.path.insert(0, str(Path(__file__).parent))

from classify_calls import (
    get_db, get_active_clients, fetch_calls_for_company, store_calls, log
)
import psycopg2.extras


def main():
    db = get_db()
    cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Find all NULL-duration calls that were auto-spammed within 90-day window
    cur.execute("""
        SELECT c.customer_id, c.callrail_company_id,
               cl.callrail_account_id, cl.callrail_api_key, cl.name,
               COUNT(*) as affected,
               MIN(c.start_time) as earliest,
               MAX(c.start_time) as latest
        FROM calls c
        JOIN clients cl ON c.customer_id = cl.customer_id
        WHERE c.duration IS NULL
          AND c.classification = 'spam'
          AND c.classification_reason = 'Very short call with no transcript'
          AND c.start_time >= NOW() - INTERVAL '90 days'
        GROUP BY c.customer_id, c.callrail_company_id,
                 cl.callrail_account_id, cl.callrail_api_key, cl.name
        ORDER BY affected DESC
    """)
    clients = cur.fetchall()

    if not clients:
        print("No NULL-duration calls to repair.")
        return

    total_affected = sum(c["affected"] for c in clients)
    print(f"Found {total_affected} NULL-duration calls across {len(clients)} clients\n")

    # Step 1: Re-fetch from CallRail to get duration + transcript
    backfill_total = 0
    for client in clients:
        since = client["earliest"] - timedelta(minutes=5)
        print(f"  [{client['customer_id']}] {client['name']}: "
              f"{client['affected']} calls ({client['earliest'].date()} to {client['latest'].date()})")

        try:
            calls = fetch_calls_for_company(
                client["callrail_company_id"], since,
                account_id=client.get("callrail_account_id"),
                api_key=client.get("callrail_api_key"))
            updated = store_calls(db, calls, client["callrail_company_id"], client["customer_id"])
            print(f"    Re-fetched {len(calls)} calls, updated {updated}")
            backfill_total += updated
        except Exception as e:
            print(f"    ERROR: {e}")

    # Step 2: Check how many now have duration
    cur.execute("""
        SELECT COUNT(*) as still_null,
               COUNT(*) FILTER (WHERE duration IS NOT NULL) as now_has_duration
        FROM calls
        WHERE classification = 'spam'
          AND classification_reason = 'Very short call with no transcript'
          AND start_time >= NOW() - INTERVAL '90 days'
          AND (duration IS NULL OR duration >= 15)
    """)
    stats = cur.fetchone()
    print(f"\nAfter backfill: {stats['now_has_duration']} now have duration, "
          f"{stats['still_null']} still NULL")

    # Step 3: Reset classification on calls that were wrongly auto-spammed
    # Reset if: duration is now >= 15 (not actually short), OR duration is still
    # NULL but they have a transcript now (CallRail eventually got the recording)
    reset_cur = db.cursor()
    reset_cur.execute("""
        UPDATE calls SET
            classification = NULL,
            classification_reason = NULL,
            classification_attempts = 0,
            updated_at = NOW()
        WHERE classification = 'spam'
          AND classification_reason = 'Very short call with no transcript'
          AND start_time >= NOW() - INTERVAL '90 days'
          AND (
              (duration IS NOT NULL AND duration >= 15)
              OR (duration IS NULL AND transcript IS NOT NULL AND transcript != '')
              OR (duration IS NULL)
          )
    """)
    reset_count = reset_cur.rowcount
    db.commit()

    print(f"\nReset classification on {reset_count} calls")
    print("These will be classified on the next cron cycle and uploaded to Google Ads if legitimate.")
    db.close()


if __name__ == "__main__":
    main()
