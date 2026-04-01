#!/usr/bin/env python3
"""
GHL Conversations ETL — Pull text messages and call transcripts from GoHighLevel.

Usage:
  python3 pull_ghl_conversations.py                          # All GHL clients
  python3 pull_ghl_conversations.py --client 7123434733      # Single client
  python3 pull_ghl_conversations.py --backfill --days 90     # Last 90 days

Pulls SMS/text messages and call transcripts from each client's GHL sub-account
and stores them as client interactions in the intelligence database.

Requires:
  - GHL API keys stored in clients table (ghl_api_key column)
"""

import argparse
import json
import logging
import os
import re
import sys
import time
import difflib
from datetime import datetime, timedelta, timezone

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
RATE_LIMIT_DELAY = 0.5
DEFAULT_LOOKBACK_DAYS = 7

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger('ghl-conversations')


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


# ── GHL API ─────────────────────────────────────────────────

def ghl_request(method, path, api_key, params=None, body=None, retries=3):
    url = "%s%s" % (GHL_BASE, path)
    if params:
        qs = "&".join("%s=%s" % (k, urllib.request.quote(str(v))) for k, v in params.items())
        url = "%s?%s" % (url, qs)

    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data,
        headers={
            "Authorization": "Bearer %s" % api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Version": GHL_VERSION,
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        },
        method=method,
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 429 and retries > 0:
            log.warning("  Rate limited, waiting 10s...")
            time.sleep(10)
            return ghl_request(method, path, api_key, params, body, retries - 1)
        body_text = e.read().decode()[:300]
        log.error("  GHL API error %d: %s" % (e.code, body_text))
        return None
    except Exception as e:
        log.error("  Request error: %s" % e)
        return None


def fetch_conversations(api_key, location_id, limit=50):
    """Fetch recent conversations for a location."""
    data = ghl_request("GET", "/conversations/search", api_key, {
        "locationId": location_id,
        "limit": str(limit),
        "sort": "desc",
        "sortBy": "last_message_date",
    })
    if not data:
        return []
    return data.get("conversations", [])


def fetch_messages(api_key, conversation_id, limit=50):
    """Fetch messages for a conversation."""
    data = ghl_request("GET", "/conversations/%s/messages" % conversation_id, api_key, {
        "limit": str(limit),
    })
    if not data:
        return []
    return data.get("messages", {}).get("messages", data.get("messages", []))


def fetch_transcription(api_key, location_id, message_id):
    """Fetch call transcription for a message."""
    data = ghl_request("GET",
        "/conversations/locations/%s/messages/%s/transcription" % (location_id, message_id),
        api_key)
    if not data:
        return None
    return data


# ── Client Matching ─────────────────────────────────────────

def build_contact_lookup(conn):
    """Build lookup tables for matching GHL conversations to clients."""
    phone_map = {}  # normalized_phone -> customer_id
    name_map = {}   # lowercase_name -> customer_id
    email_map = {}  # email prefix patterns -> customer_id

    with conn.cursor() as cur:
        # Phone numbers from client_contacts
        cur.execute("""
            SELECT customer_id, phone_normalized, LOWER(name) as name, LOWER(email) as email
            FROM client_contacts
            WHERE phone_normalized IS NOT NULL OR name IS NOT NULL
        """)
        for row in cur.fetchall():
            if row[1]:
                phone_map[row[1]] = row[0]
            if row[2]:
                name_map[row[2]] = row[0]
            # Map scheduling assistant emails to clients
            if row[3] and 'schedulingassistant' in row[3]:
                email_map[row[3].split('@')[0] if '@' in row[3] else ''] = row[0]

        # Also get client owner emails/phones from clients table
        cur.execute("""
            SELECT customer_id, LOWER(name) as name FROM clients WHERE status = 'active'
        """)
        for row in cur.fetchall():
            # Build name fragments for fuzzy matching
            parts = row[1].replace('|', ' ').replace('-', ' ').split()
            for part in parts:
                if len(part) > 4 and part not in ('pure', 'maintenance', 'mold', 'the', 'and', 'service'):
                    name_map[part] = row[0]

    return phone_map, name_map, email_map


def match_conversation_to_client(contact_name, contact_phone, phone_map, name_map, email_map):
    """Match a GHL conversation to a client by phone, name, or email pattern."""
    phone_norm = normalize_phone(contact_phone)

    # 1. Match by phone number (most reliable)
    if phone_norm and phone_norm in phone_map:
        return phone_map[phone_norm]

    # 2. Match by exact contact name
    name_lower = (contact_name or "").lower().strip()
    if name_lower in name_map:
        return name_map[name_lower]

    # 3. Match by email-style contact name (scheduling assistants, etc.)
    if '@' in name_lower:
        prefix = name_lower.split('@')[0]
        for pattern, cid in email_map.items():
            if pattern and pattern in prefix:
                return cid

    # 4. Match by name fragment
    for fragment, cid in name_map.items():
        if len(fragment) > 4 and fragment in name_lower:
            return cid

    return None


# ── Message Processing ──────────────────────────────────────

def process_client_conversations(conn, customer_id_unused, client_name, api_key, location_id,
                                 days_back=7, phone_map=None, name_map=None, email_map=None):
    """Pull and store conversations for a single client."""
    stats = {"sms": 0, "calls": 0, "transcripts": 0, "matched": 0, "unmatched": 0, "errors": []}

    # Fetch recent conversations
    conversations = fetch_conversations(api_key, location_id, limit=50)
    log.info("  Found %d conversations" % len(conversations))

    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

    with conn.cursor() as cur:
        for conv in conversations:
            conv_id = conv.get("id")
            contact_name = conv.get("contactName", "Unknown")
            contact_phone = conv.get("phone", "")
            last_msg_date = conv.get("lastMessageDate")

            # Match this conversation to a client
            customer_id = None
            if phone_map and name_map:
                customer_id = match_conversation_to_client(
                    contact_name, contact_phone, phone_map, name_map, email_map or {}
                )

            # Skip old conversations
            if last_msg_date:
                try:
                    if isinstance(last_msg_date, (int, float)):
                        msg_date = datetime.fromtimestamp(last_msg_date / 1000, tz=timezone.utc)
                    else:
                        msg_date = datetime.fromisoformat(str(last_msg_date).replace("Z", "+00:00"))
                    if msg_date < cutoff:
                        continue
                except (ValueError, TypeError, OSError):
                    pass

            # Fetch messages in this conversation
            messages = fetch_messages(api_key, conv_id, limit=50)
            time.sleep(RATE_LIMIT_DELAY)

            for msg in messages:
                msg_id = msg.get("id")
                msg_type = str(msg.get("type", ""))  # TYPE_SMS, TYPE_CALL, or int codes
                direction = msg.get("direction", "")  # inbound, outbound
                body = msg.get("body", "")
                date_added = msg.get("dateAdded")
                msg_source_id = "ghl-msg-%s" % msg_id

                # Skip if already stored
                cur.execute("""
                    SELECT id FROM crm_messages WHERE source_id = %s
                """, (msg_source_id,))
                if cur.fetchone():
                    continue

                # Parse date
                msg_date = None
                if date_added:
                    try:
                        if isinstance(date_added, (int, float)):
                            msg_date = datetime.fromtimestamp(date_added / 1000, tz=timezone.utc)
                        else:
                            msg_date = datetime.fromisoformat(str(date_added).replace("Z", "+00:00"))
                    except (ValueError, TypeError, OSError):
                        msg_date = datetime.now(timezone.utc)
                else:
                    msg_date = datetime.now(timezone.utc)

                # Skip messages before cutoff
                if msg_date and msg_date < cutoff:
                    continue

                # Determine channel using messageType field (more reliable than numeric type)
                # GHL messageType values: TYPE_SMS, TYPE_CALL, TYPE_IVR_CALL, TYPE_EMAIL,
                # TYPE_CAMPAIGN_VOICEMAIL, TYPE_ACTIVITY_CONTACT, TYPE_ACTIVITY_OPPORTUNITY
                # Numeric types: 1=call, 2=sms, 3=email, 24=ivr_call, 25=activity
                msg_type_str = msg.get("messageType", "")
                if "SMS" in msg_type_str.upper() or msg_type == "2":
                    channel = "sms"
                elif "CALL" in msg_type_str.upper() or msg_type in ("1", "24"):
                    channel = "call"
                elif "EMAIL" in msg_type_str.upper() or msg_type == "3":
                    channel = "email"
                elif "VOICEMAIL" in msg_type_str.upper():
                    channel = "voicemail"
                elif msg_type == "5":
                    channel = "facebook"
                elif msg_type == "7":
                    channel = "gmb"
                elif msg_type == "8":
                    channel = "instagram"
                else:
                    channel = msg_type_str.lower().replace("type_", "") or msg_type

                if channel == "sms":
                    stats["sms"] += 1
                elif channel == "call":
                    stats["calls"] += 1
                    # Get call duration from meta
                    call_duration = msg.get("meta", {}).get("call", {}).get("duration", 0)
                    # Only fetch transcription for calls > 10 seconds (skip quick hangups)
                    if call_duration > 10:
                        try:
                            transcript_data = fetch_transcription(api_key, location_id, msg_id)
                            if transcript_data and isinstance(transcript_data, list):
                                # Transcription is an array of sentence objects
                                lines = []
                                for seg in transcript_data:
                                    text = seg.get("transcript", "")
                                    if text:
                                        lines.append(text)
                                if lines:
                                    body = " ".join(lines)
                                    stats["transcripts"] += 1
                            elif transcript_data and transcript_data.get("transcription"):
                                body = transcript_data["transcription"]
                                stats["transcripts"] += 1
                            time.sleep(RATE_LIMIT_DELAY)
                        except Exception as e:
                            pass  # Transcription not always available

                phone_norm = normalize_phone(contact_phone)

                # Track matching
                if customer_id:
                    stats["matched"] += 1
                else:
                    stats["unmatched"] += 1

                # Get duration for calls
                duration = msg.get("meta", {}).get("call", {}).get("duration") if channel == "call" else None

                try:
                    cur.execute("SAVEPOINT msg_sp")
                    cur.execute("""
                        INSERT INTO crm_messages (
                            source, customer_id, contact_name, phone_number,
                            phone_normalized, direction, channel, message_body,
                            duration, message_date, source_id
                        ) VALUES ('ghl', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source_id) DO UPDATE SET
                            customer_id = COALESCE(EXCLUDED.customer_id, crm_messages.customer_id)
                    """, (
                        customer_id, contact_name, contact_phone,
                        phone_norm, direction, channel,
                        body[:10000] if body else None,
                        duration, msg_date, msg_source_id,
                    ))
                    cur.execute("RELEASE SAVEPOINT msg_sp")
                except Exception as e:
                    cur.execute("ROLLBACK TO SAVEPOINT msg_sp")
                    stats["errors"].append("msg %s: %s" % (msg_id, e))

    conn.commit()
    return stats


# ── Main ────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Pull GHL conversations for client intelligence")
    parser.add_argument('--client', type=str, help='Pull only one customer_id')
    parser.add_argument('--backfill', action='store_true', help='Pull more history')
    parser.add_argument('--days', type=int, default=DEFAULT_LOOKBACK_DAYS, help='Days to look back')
    args = parser.parse_args()

    conn = get_db()

    try:
        with conn.cursor() as cur:
            # Create crm_messages table if not exists
            cur.execute("""
                CREATE TABLE IF NOT EXISTS crm_messages (
                    id              SERIAL PRIMARY KEY,
                    source          TEXT NOT NULL,
                    customer_id     BIGINT REFERENCES clients(customer_id),
                    contact_name    TEXT,
                    phone_number    TEXT,
                    phone_normalized TEXT,
                    direction       TEXT,
                    channel         TEXT,
                    message_body    TEXT,
                    duration        INTEGER,
                    message_date    TIMESTAMPTZ,
                    source_id       TEXT UNIQUE,
                    created_at      TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_crm_messages_customer
                    ON crm_messages(customer_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_crm_messages_phone
                    ON crm_messages(phone_normalized)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_crm_messages_date
                    ON crm_messages(message_date DESC)
            """)
        conn.commit()

        # Blueprint sub-account has all agency-client communication
        blueprint_api_key = os.environ.get("GHL_BLUEPRINT_API_KEY", "")
        blueprint_location_id = os.environ.get("GHL_BLUEPRINT_LOCATION_ID", "")

        if not blueprint_api_key or not blueprint_location_id:
            log.error("GHL_BLUEPRINT_API_KEY and GHL_BLUEPRINT_LOCATION_ID required in .env")
            return

        # Get client list for matching conversations to clients
        with conn.cursor() as cur:
            query = "SELECT customer_id, name, owner_email, contact_email FROM clients WHERE status = 'active'"
            params = []
            if args.client:
                query += " AND customer_id = %s"
                params.append(int(args.client))
            query += " ORDER BY name"
            cur.execute(query, params)
            db_clients = cur.fetchall()

        days = args.days if not args.backfill else 90
        log.info("Pulling Blueprint GHL conversations (last %d days)..." % days)

        # Build contact lookup for matching conversations to clients
        phone_map, name_map, email_map = build_contact_lookup(conn)
        log.info("Contact lookup: %d phone mappings, %d name mappings" % (len(phone_map), len(name_map)))

        # Log the pull
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO client_intelligence_pull_log (source, started_at)
                VALUES ('ghl_conversations', NOW()) RETURNING id
            """)
            pull_id = cur.fetchone()[0]
        conn.commit()

        total_sms = 0
        total_calls = 0
        total_transcripts = 0
        total_matched = 0
        total_unmatched = 0

        log.info("\n%s" % ("=" * 60))
        log.info("Blueprint Agency Conversations")
        log.info("=" * 60)

        stats = process_client_conversations(
            conn, None, "Blueprint Agency", blueprint_api_key, blueprint_location_id,
            days_back=days, phone_map=phone_map, name_map=name_map, email_map=email_map
        )
        total_sms += stats["sms"]
        total_calls += stats["calls"]
        total_transcripts += stats["transcripts"]
        total_matched += stats["matched"]
        total_unmatched += stats["unmatched"]

        log.info("  SMS: %d, Calls: %d, Transcripts: %d, Matched: %d, Unmatched: %d, Errors: %d" % (
            stats["sms"], stats["calls"], stats["transcripts"],
            stats["matched"], stats["unmatched"], len(stats["errors"])))

        # Update pull log
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE client_intelligence_pull_log
                SET finished_at = NOW(),
                    records_processed = %s,
                    status = 'completed'
                WHERE id = %s
            """, (total_sms + total_calls, pull_id))
        conn.commit()

        log.info("\n%s" % ("=" * 60))
        log.info("DONE — %d SMS, %d calls, %d transcripts, %d matched, %d unmatched" % (
            total_sms, total_calls, total_transcripts, total_matched, total_unmatched))
        log.info("=" * 60)

    finally:
        conn.close()


if __name__ == '__main__':
    main()
