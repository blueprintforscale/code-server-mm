#!/usr/bin/env python3
"""
Extract follow-up dates from HCP estimate option notes using Claude API.
Pre-filters notes with keyword matching, then calls Claude to extract exact dates.

Usage:
    python3 extract_followups.py              # Process unprocessed notes
    python3 extract_followups.py --reprocess  # Reprocess all notes (even already processed)
    python3 extract_followups.py --dry-run    # Show what would be processed without calling API

Cron: once daily (e.g., 6am)
"""

import os
import sys
import re
import json
import time
import argparse
import logging
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError

import psycopg2

# ============================================================
# Config
# ============================================================

DSN = "host=localhost port=5432 dbname=blueprint user=blueprint"
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
MODEL = "claude-haiku-4-5-20251001"

# Pre-filter regex — only send notes matching these to the API
FOLLOWUP_PATTERN = re.compile(
    r'follow.?up|call.?back|check.?in|next\s+week|next\s+month'
    r'|in\s+\d+\s+(week|day|month)'
    r'|after\s+(the|their)\s+'
    r'|waiting\s+(on|for)|get\s+back\s+to',
    re.IGNORECASE
)

# Rate limiting
RATE_LIMIT_DELAY = 0.5  # seconds between API calls

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger('followup-extract')

# ============================================================
# Claude API
# ============================================================

def extract_followup_date(notes_text):
    """
    Call Claude API to extract a follow-up date from notes.
    Returns (date_str, raw_snippet) or (None, None) if no follow-up found.
    """
    today = datetime.now().strftime('%m-%d-%Y')

    prompt = f"""You are analyzing a customer note from a home service business to identify follow-up instructions.

Today's date is: {today}

Rules:
- Relative dates (e.g., "in 2 weeks," "next month") should be calculated from today.
- Absolute dates (e.g., "March 2nd") default to current year. If already passed, assume next year.
- "Next week" = first business day (Monday) of the following week.
- Recognize: "follow up", "call back", "check in", "would like a follow-up", "get back to", "waiting on/for" + time reference.
- Ignore case.
- If follow-up timing is ambiguous but a follow-up is clearly requested, default to 1 week from today.

Return a JSON object with exactly two fields:
- "date": the follow-up date in MM-DD-YYYY format, or "none" if no follow-up detected
- "snippet": the specific phrase from the notes that indicates the follow-up (max 100 chars), or "none"

Return ONLY the JSON object, no other text.

Notes:
{notes_text[:2000]}"""

    body = json.dumps({
        "model": MODEL,
        "max_tokens": 150,
        "messages": [{"role": "user", "content": prompt}],
    }).encode()

    req = Request(ANTHROPIC_API_URL, data=body, method='POST')
    req.add_header('Content-Type', 'application/json')
    req.add_header('x-api-key', ANTHROPIC_API_KEY)
    req.add_header('anthropic-version', '2023-06-01')

    try:
        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
            text = data['content'][0]['text'].strip()

            # Parse JSON response
            # Handle potential markdown code blocks
            if text.startswith('```'):
                text = re.sub(r'^```\w*\n?', '', text)
                text = re.sub(r'\n?```$', '', text)
                text = text.strip()

            result = json.loads(text)
            date_str = result.get('date', 'none')
            snippet = result.get('snippet', 'none')

            if date_str and date_str.lower() != 'none':
                return date_str, snippet
            return None, None

    except (HTTPError, json.JSONDecodeError, KeyError, IndexError) as e:
        log.warning(f"  API error: {e}")
        return None, None


def parse_date(date_str):
    """Parse MM-DD-YYYY to a Python date, or return None."""
    try:
        return datetime.strptime(date_str, '%m-%d-%Y').date()
    except (ValueError, TypeError):
        return None

# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description='Extract follow-up dates from estimate notes')
    parser.add_argument('--reprocess', action='store_true', help='Reprocess already-processed notes')
    parser.add_argument('--dry-run', action='store_true', help='Show candidates without calling API')
    parser.add_argument('--limit', type=int, default=0, help='Max notes to process (0=all)')
    args = parser.parse_args()

    if not ANTHROPIC_API_KEY and not args.dry_run:
        log.error("ANTHROPIC_API_KEY not set. Export it or use --dry-run.")
        sys.exit(1)

    conn = psycopg2.connect(DSN)

    with conn.cursor() as cur:
        # Find candidate notes
        where = "WHERE eo.notes IS NOT NULL AND eo.notes != ''"
        if not args.reprocess:
            where += " AND eo.follow_up_processed_at IS NULL"

        cur.execute(f"""
            SELECT eo.id, eo.hcp_estimate_id, eo.notes, cl.name
            FROM hcp_estimate_options eo
            JOIN hcp_estimates e ON e.hcp_estimate_id = eo.hcp_estimate_id
            JOIN clients cl ON cl.customer_id = e.customer_id
            {where}
            ORDER BY eo.id
        """)
        all_notes = cur.fetchall()

    # Pre-filter with regex
    candidates = [(id, est_id, notes, client) for id, est_id, notes, client in all_notes
                  if FOLLOWUP_PATTERN.search(notes)]

    log.info(f"Found {len(all_notes)} notes, {len(candidates)} match follow-up keywords")

    if args.dry_run:
        for id, est_id, notes, client in candidates[:20]:
            # Find the matching snippet
            match = FOLLOWUP_PATTERN.search(notes)
            snippet = notes[max(0, match.start()-30):match.end()+50].strip() if match else ''
            log.info(f"  [{client[:30]}] ...{snippet}...")
        log.info(f"Dry run complete. {len(candidates)} notes would be processed.")
        return

    if args.limit:
        candidates = candidates[:args.limit]

    processed = 0
    followups_found = 0

    for id, est_id, notes, client in candidates:
        try:
            date_str, snippet = extract_followup_date(notes)
            follow_date = parse_date(date_str) if date_str else None

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE hcp_estimate_options
                    SET follow_up_date = %s,
                        follow_up_raw = %s,
                        follow_up_processed_at = NOW()
                    WHERE id = %s
                """, [follow_date, snippet, id])
            conn.commit()

            processed += 1
            if follow_date:
                followups_found += 1
                log.info(f"  [{client[:30]}] Follow-up: {follow_date} — {snippet[:60]}")

        except Exception as e:
            log.warning(f"  Error processing option {id}: {e}")
            conn.rollback()

        time.sleep(RATE_LIMIT_DELAY)

    # Mark non-candidates as processed too (no follow-up keywords)
    with conn.cursor() as cur:
        where_base = "WHERE notes IS NOT NULL AND notes != '' AND follow_up_processed_at IS NULL"
        cur.execute(f"""
            UPDATE hcp_estimate_options
            SET follow_up_processed_at = NOW()
            {where_base}
        """)
        skipped = cur.rowcount
    conn.commit()

    log.info(f"Done. Processed {processed} notes via API, found {followups_found} follow-ups. Skipped {skipped} (no keywords).")
    conn.close()


if __name__ == '__main__':
    main()
