#!/usr/bin/env python3
"""
Gmail ETL — Pull client emails from Blueprint team inboxes.

Usage:
  python3 pull_gmail_data.py                          # Recent emails (last 7 days)
  python3 pull_gmail_data.py --backfill               # Last 90 days
  python3 pull_gmail_data.py --backfill --days 180    # Last 180 days
  python3 pull_gmail_data.py --client 7123434733      # Filter to one client
  python3 pull_gmail_data.py --inbox susie@blueprintforscale.com  # One inbox only

Pulls from authorized Gmail inboxes, matches emails to clients by contact
email/domain, and stores as client interactions + personal notes.

Requires:
  - gmail_tokens/<email>.json for each inbox (run gmail_auth.py first)
  - ANTHROPIC_API_KEY env var (for AI extraction from email content)
"""

import argparse
import base64
import email
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import psycopg2
import psycopg2.extras
import urllib.request
import urllib.error

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# ── Config ──────────────────────────────────────────────────

DB_CONFIG = {
    "dbname": "blueprint",
    "user": "blueprint",
    "host": "localhost",
    "port": 5432,
}

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TOKENS_DIR = os.path.join(os.path.dirname(__file__), "gmail_tokens")

RATE_LIMIT_DELAY = 0.2  # Gmail API is generous but be polite
DEFAULT_LOOKBACK_DAYS = 7
BACKFILL_DAYS = 90

# Skip internal-only emails (newsletters, automated, etc.)
SKIP_SENDERS = {
    "noreply", "no-reply", "mailer-daemon", "notifications",
    "calendar-notification", "support@", "billing@",
}

# Blueprint team emails (to identify direction of emails)
TEAM_EMAILS = {
    "info@blueprintforscale.com",
    "susie@blueprintforscale.com",
    "martin@blueprintforscale.com",
    "josh@blueprintforscale.com",
    "kiana@blueprintforscale.com",
}

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger('gmail-sync')


# ── Database ────────────────────────────────────────────────

def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


# ── Gmail API ───────────────────────────────────────────────

def get_gmail_service(token_path):
    """Build an authenticated Gmail API service from a saved token."""
    with open(token_path) as f:
        token_data = json.load(f)

    creds = Credentials(
        token=token_data["token"],
        refresh_token=token_data["refresh_token"],
        token_uri=token_data["token_uri"],
        client_id=token_data["client_id"],
        client_secret=token_data["client_secret"],
        scopes=token_data["scopes"],
    )

    # Refresh if expired
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        # Save refreshed token
        token_data["token"] = creds.token
        with open(token_path, "w") as f:
            json.dump(token_data, f, indent=2)

    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def list_messages(service, query, max_results=500):
    """List message IDs matching a Gmail search query."""
    messages = []
    page_token = None

    while True:
        result = service.users().messages().list(
            userId="me",
            q=query,
            maxResults=min(max_results - len(messages), 100),
            pageToken=page_token,
        ).execute()

        batch = result.get("messages", [])
        messages.extend(batch)

        if len(messages) >= max_results:
            break

        page_token = result.get("nextPageToken")
        if not page_token:
            break

        time.sleep(RATE_LIMIT_DELAY)

    return messages


def get_message(service, msg_id):
    """Get a single message with full content."""
    try:
        msg = service.users().messages().get(
            userId="me",
            id=msg_id,
            format="full",
        ).execute()
        return msg
    except Exception as e:
        log.error(f"Failed to fetch message {msg_id}: {e}")
        return None


def parse_message(msg):
    """Extract useful fields from a Gmail API message."""
    headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}

    # Parse date
    date_str = headers.get("date", "")
    try:
        msg_date = parsedate_to_datetime(date_str)
    except Exception:
        # Fallback to internal date
        internal_ms = int(msg.get("internalDate", 0))
        msg_date = datetime.fromtimestamp(internal_ms / 1000, tz=timezone.utc)

    # Extract emails from From/To/Cc
    from_email = extract_email(headers.get("from", ""))
    from_name = extract_name(headers.get("from", ""))
    to_emails = extract_all_emails(headers.get("to", ""))
    cc_emails = extract_all_emails(headers.get("cc", ""))

    # Get body text
    body = extract_body(msg.get("payload", {}))

    return {
        "message_id": msg["id"],
        "thread_id": msg["threadId"],
        "date": msg_date,
        "from_email": from_email,
        "from_name": from_name,
        "to_emails": to_emails,
        "cc_emails": cc_emails,
        "subject": headers.get("subject", "(no subject)"),
        "body": body,
        "snippet": msg.get("snippet", ""),
        "label_ids": msg.get("labelIds", []),
    }


def extract_email(header_value):
    """Extract email from 'Name <email>' format."""
    match = re.search(r'<([^>]+)>', header_value)
    if match:
        return match.group(1).lower().strip()
    # Might just be a bare email
    if "@" in header_value:
        return header_value.strip().lower()
    return ""


def extract_name(header_value):
    """Extract display name from 'Name <email>' format."""
    match = re.match(r'^"?([^"<]+)"?\s*<', header_value)
    if match:
        return match.group(1).strip()
    return ""


def extract_all_emails(header_value):
    """Extract all emails from a comma-separated header."""
    if not header_value:
        return []
    return [extract_email(part) for part in header_value.split(",") if "@" in part]


def extract_body(payload):
    """Recursively extract plain text body from message payload."""
    # Direct body
    if payload.get("mimeType") == "text/plain" and payload.get("body", {}).get("data"):
        return base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="replace")

    # Multipart — look for text/plain first, then text/html
    parts = payload.get("parts", [])
    plain_text = None
    html_text = None

    for part in parts:
        mime = part.get("mimeType", "")
        if mime == "text/plain" and part.get("body", {}).get("data"):
            plain_text = base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8", errors="replace")
        elif mime == "text/html" and part.get("body", {}).get("data"):
            html_text = base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8", errors="replace")
        elif mime.startswith("multipart/"):
            # Recurse into nested multipart
            nested = extract_body(part)
            if nested:
                return nested

    if plain_text:
        return plain_text
    if html_text:
        # Strip HTML tags for a rough text version
        return re.sub(r'<[^>]+>', ' ', html_text).strip()

    return ""


# ── Client Matching ─────────────────────────────────────────

def build_client_lookup(conn):
    """Build lookup tables for matching emails to clients."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT c.customer_id, c.name, c.owner_email, c.contact_email
            FROM clients c
            WHERE c.status = 'active'
        """)
        clients = cur.fetchall()

        cur.execute("""
            SELECT customer_id, name, email
            FROM client_contacts
            WHERE email IS NOT NULL
        """)
        contacts = cur.fetchall()

    # Build email -> customer_id map
    email_map = {}
    for client in clients:
        for email_field in ['owner_email', 'contact_email']:
            addr = client.get(email_field)
            if addr:
                email_map[addr.lower().strip()] = client['customer_id']

    for contact in contacts:
        if contact['email']:
            email_map[contact['email'].lower().strip()] = contact['customer_id']

    return clients, email_map


def match_email_to_client(parsed, email_map):
    """Try to match an email to a client by sender/recipient emails."""
    # Check all non-team email addresses involved
    all_addresses = [parsed["from_email"]] + parsed["to_emails"] + parsed["cc_emails"]

    for addr in all_addresses:
        if addr and addr.lower() not in TEAM_EMAILS:
            customer_id = email_map.get(addr.lower())
            if customer_id:
                return customer_id

    return None


# ── AI Extraction ───────────────────────────────────────────

def extract_insights_from_email(parsed, client_name):
    """Use Claude to extract summary, sentiment, action items, and personal notes."""
    if not ANTHROPIC_API_KEY:
        return {
            "summary": parsed["snippet"][:300] if parsed["snippet"] else "",
            "action_items": None,
            "sentiment": "neutral",
            "personal_notes": [],
        }

    # Truncate body for API call
    body_text = (parsed["body"] or "")[:4000]

    prompt = f"""Analyze this email exchange between Blueprint for Scale (a Google Ads agency for mold remediation companies) and their client "{client_name}".

From: {parsed['from_name']} <{parsed['from_email']}>
To: {', '.join(parsed['to_emails'])}
Subject: {parsed['subject']}
Date: {parsed['date'].strftime('%Y-%m-%d')}

Body:
{body_text}

Return a JSON object with:
1. "summary" - 1-2 sentence summary of what this email is about (max 200 chars)
2. "action_items" - any action items or follow-ups mentioned (null if none)
3. "sentiment" - one of: positive, neutral, negative, at_risk
4. "personal_notes" - array of personal insights about the client (birthdays, vacations, life events, preferences, frustrations, business changes). Empty array if none.

Each personal note should have:
  - "note": the insight
  - "category": one of personal, preference, business_change, milestone

Return ONLY valid JSON, no markdown."""

    try:
        body = json.dumps({
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 500,
            "messages": [{"role": "user", "content": prompt}],
        }).encode()

        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=body,
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
        )

        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())
            text = result["content"][0]["text"]
            # Clean up potential markdown wrapping
            text = re.sub(r'^```json\s*', '', text.strip())
            text = re.sub(r'\s*```$', '', text.strip())
            return json.loads(text)

    except Exception as e:
        log.warning(f"AI extraction failed: {e}")
        return {
            "summary": parsed["snippet"][:300] if parsed["snippet"] else "",
            "action_items": None,
            "sentiment": "neutral",
            "personal_notes": [],
        }


# ── Dedup Check ─────────────────────────────────────────────

def already_ingested(conn, message_id, inbox_email):
    """Check if this message was already ingested."""
    source_id = f"gmail-{inbox_email}-{message_id}"
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM client_interactions WHERE source = 'gmail' AND source_id = %s",
            (source_id,)
        )
        return cur.fetchone() is not None


# ── Main Processing ─────────────────────────────────────────

def process_inbox(inbox_email, conn, email_map, clients, lookback_days, target_client=None):
    """Process a single inbox."""
    token_path = os.path.join(TOKENS_DIR, f"{inbox_email}.json")
    if not os.path.exists(token_path):
        log.warning(f"No token for {inbox_email} — run: python3 gmail_auth.py {inbox_email}")
        return 0

    log.info(f"Processing inbox: {inbox_email}")

    try:
        service = get_gmail_service(token_path)
    except Exception as e:
        log.error(f"Failed to auth {inbox_email}: {e}")
        return 0

    # Build search query
    after_date = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%Y/%m/%d")
    query = f"after:{after_date}"

    # Exclude sent-only and drafts — we want conversations with clients
    # Include both inbox and sent for full thread context
    messages = list_messages(service, query, max_results=500)
    log.info(f"  Found {len(messages)} messages in last {lookback_days} days")

    # Process by thread to avoid duplicating thread interactions
    threads_seen = set()
    ingested = 0
    skipped_dedup = 0
    skipped_no_match = 0
    skipped_internal = 0

    for msg_ref in messages:
        msg_id = msg_ref["id"]
        thread_id = msg_ref.get("threadId", msg_id)

        # One interaction per thread per inbox
        if thread_id in threads_seen:
            continue
        threads_seen.add(thread_id)

        # Dedup check
        source_id = f"gmail-{inbox_email}-{thread_id}"
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM client_interactions WHERE source = 'gmail' AND source_id = %s",
                (source_id,)
            )
            if cur.fetchone():
                skipped_dedup += 1
                continue

        time.sleep(RATE_LIMIT_DELAY)

        # Fetch the first message in thread for context
        msg = get_message(service, msg_id)
        if not msg:
            continue

        parsed = parse_message(msg)

        # Skip internal-only emails
        all_addresses = [parsed["from_email"]] + parsed["to_emails"] + parsed["cc_emails"]
        external = [a for a in all_addresses if a and a.lower() not in TEAM_EMAILS]
        if not external:
            skipped_internal += 1
            continue

        # Skip automated/noreply
        if any(skip in parsed["from_email"].lower() for skip in SKIP_SENDERS):
            continue

        # Match to client
        customer_id = match_email_to_client(parsed, email_map)
        if not customer_id:
            skipped_no_match += 1
            continue

        # Filter to specific client if requested
        if target_client and customer_id != target_client:
            continue

        # Determine direction
        is_outbound = parsed["from_email"].lower() in TEAM_EMAILS
        logged_by = parsed["from_name"] if is_outbound else inbox_email.split("@")[0].title()

        # Get client name for AI
        client_name = ""
        for c in clients:
            if c["customer_id"] == customer_id:
                client_name = c["name"]
                break

        # AI extraction
        insights = extract_insights_from_email(parsed, client_name)

        # Build attendees list
        attendees = []
        if parsed["from_name"]:
            attendees.append(parsed["from_name"])
        for addr in parsed["to_emails"][:5]:
            if addr not in TEAM_EMAILS:
                attendees.append(addr)

        # Save interaction
        try:
            with conn.cursor() as cur:
                cur.execute("SAVEPOINT gmail_msg")
                cur.execute("""
                    INSERT INTO client_interactions
                        (customer_id, interaction_type, interaction_date, logged_by,
                         attendees, summary, action_items, sentiment, source, source_id)
                    VALUES (%s, 'email', %s, %s, %s, %s, %s, %s, 'gmail', %s)
                    ON CONFLICT (source, source_id) WHERE source_id IS NOT NULL
                    DO NOTHING
                """, (
                    customer_id,
                    parsed["date"],
                    logged_by,
                    attendees if attendees else None,
                    f"[{parsed['subject']}] {insights.get('summary', '')}",
                    insights.get("action_items"),
                    insights.get("sentiment", "neutral"),
                    source_id,
                ))

                # Save personal notes
                for note in insights.get("personal_notes", []):
                    note_source_id = f"gmail-note-{thread_id}-{hash(note.get('note', ''))}"
                    cur.execute("""
                        INSERT INTO client_personal_notes
                            (customer_id, note, category, source, source_id,
                             captured_date, captured_by, auto_extracted)
                        VALUES (%s, %s, %s, 'gmail', %s, %s, 'gmail-etl', TRUE)
                        ON CONFLICT DO NOTHING
                    """, (
                        customer_id,
                        note.get("note", ""),
                        note.get("category", "personal"),
                        note_source_id,
                        parsed["date"].date(),
                    ))

                cur.execute("RELEASE SAVEPOINT gmail_msg")
            conn.commit()
            ingested += 1

        except Exception as e:
            with conn.cursor() as cur:
                cur.execute("ROLLBACK TO SAVEPOINT gmail_msg")
            log.error(f"  Failed to save thread {thread_id}: {e}")
            continue

    log.info(f"  {inbox_email}: {ingested} threads ingested, "
             f"{skipped_dedup} already seen, {skipped_no_match} no client match, "
             f"{skipped_internal} internal-only")
    return ingested


def main():
    parser = argparse.ArgumentParser(description="Pull Gmail data for client intelligence")
    parser.add_argument("--backfill", action="store_true", help="Pull last 90 days")
    parser.add_argument("--days", type=int, help="Custom lookback days")
    parser.add_argument("--client", type=int, help="Filter to one client (customer_id)")
    parser.add_argument("--inbox", help="Process only one inbox (email address)")
    args = parser.parse_args()

    lookback_days = DEFAULT_LOOKBACK_DAYS
    if args.backfill:
        lookback_days = args.days or BACKFILL_DAYS
    elif args.days:
        lookback_days = args.days

    conn = get_db()

    # Log the pull
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO client_intelligence_pull_log (source, status)
            VALUES ('gmail', 'running')
            RETURNING id
        """)
        pull_id = cur.fetchone()[0]
    conn.commit()

    # Build client lookup
    clients, email_map = build_client_lookup(conn)
    log.info(f"Loaded {len(email_map)} client email mappings")

    # Find all authorized inboxes
    if args.inbox:
        inboxes = [args.inbox]
    else:
        inboxes = []
        if os.path.exists(TOKENS_DIR):
            for f in sorted(os.listdir(TOKENS_DIR)):
                if f.endswith(".json"):
                    inboxes.append(f.replace(".json", ""))

    if not inboxes:
        log.error("No authorized inboxes found. Run gmail_auth.py first.")
        sys.exit(1)

    log.info(f"Processing {len(inboxes)} inbox(es) for last {lookback_days} days")

    total = 0
    errors = []
    for inbox in inboxes:
        try:
            count = process_inbox(inbox, conn, email_map, clients, lookback_days, args.client)
            total += count
        except Exception as e:
            log.error(f"Failed processing {inbox}: {e}")
            errors.append(f"{inbox}: {str(e)[:100]}")

    # Update pull log
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE client_intelligence_pull_log
            SET finished_at = NOW(), records_processed = %s,
                errors = %s, status = %s
            WHERE id = %s
        """, (total, errors or None, 'completed' if not errors else 'completed', pull_id))
    conn.commit()

    conn.close()
    log.info(f"Done. {total} total email threads ingested across {len(inboxes)} inboxes.")


if __name__ == "__main__":
    main()
