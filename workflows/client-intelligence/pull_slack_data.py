#!/usr/bin/env python3
"""
Slack ETL — Pull messages from client channels into the client intelligence database.

Usage:
  python3 pull_slack_data.py                          # All client channels
  python3 pull_slack_data.py --client 7123434733      # Single client
  python3 pull_slack_data.py --backfill               # Full historical pull
  python3 pull_slack_data.py --map-channels           # Interactive: map Slack channels to clients

Pulls messages from each client's Slack channel, stores raw messages,
and extracts structured interactions + personal notes via Claude.

Requires:
  - SLACK_BOT_TOKEN env var (xoxb-...) with channels:history, channels:read, users:read scopes
  - ANTHROPIC_API_KEY env var (for AI extraction)
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
import urllib.request
import urllib.error

# ── Config ──────────────────────────────────────────────────

DB_CONFIG = {
    "dbname": "blueprint",
    "user": "blueprint",
    "host": "localhost",
    "port": 5432,
}

SLACK_BASE = "https://slack.com/api"
SLACK_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

RATE_LIMIT_DELAY = 1.0  # seconds between Slack API calls
MESSAGES_PER_PAGE = 200
LOOKBACK_HOURS = 24  # default: pull last 24 hours of messages

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger('slack-sync')


# ── Database ────────────────────────────────────────────────

def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


# ── Slack API ───────────────────────────────────────────────

def slack_request(method, params=None, retries=3):
    """Make a Slack API request."""
    url = f"{SLACK_BASE}/{method}"
    if params:
        qs = "&".join(f"{k}={urllib.request.quote(str(v))}" for k, v in params.items())
        url = f"{url}?{qs}"

    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {SLACK_TOKEN}",
        "Content-Type": "application/x-www-form-urlencoded",
    })

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
            if not data.get("ok"):
                error = data.get("error", "unknown")
                if error == "ratelimited" and retries > 0:
                    retry_after = int(resp.headers.get("Retry-After", 5))
                    log.warning(f"Rate limited, waiting {retry_after}s...")
                    time.sleep(retry_after)
                    return slack_request(method, params, retries - 1)
                log.error(f"Slack API error: {error}")
                return None
            return data
    except urllib.error.HTTPError as e:
        if e.code == 429 and retries > 0:
            retry_after = int(e.headers.get("Retry-After", 5))
            log.warning(f"Rate limited (429), waiting {retry_after}s...")
            time.sleep(retry_after)
            return slack_request(method, params, retries - 1)
        log.error(f"HTTP error {e.code}: {e.read().decode()[:200]}")
        return None
    except Exception as e:
        log.error(f"Request error: {e}")
        return None


def get_channels():
    """List all public channels in the workspace."""
    channels = []
    cursor = None
    while True:
        params = {"types": "public_channel", "limit": "200"}
        if cursor:
            params["cursor"] = cursor
        data = slack_request("conversations.list", params)
        if not data:
            break
        channels.extend(data.get("channels", []))
        cursor = data.get("response_metadata", {}).get("next_cursor", "")
        if not cursor:
            break
        time.sleep(RATE_LIMIT_DELAY)
    return channels


def get_channel_history(channel_id, oldest=None, latest=None):
    """Pull messages from a channel with pagination."""
    messages = []
    cursor = None
    while True:
        params = {"channel": channel_id, "limit": str(MESSAGES_PER_PAGE)}
        if oldest:
            params["oldest"] = str(oldest)
        if latest:
            params["latest"] = str(latest)
        if cursor:
            params["cursor"] = cursor

        data = slack_request("conversations.history", params)
        if not data:
            break

        batch = data.get("messages", [])
        messages.extend(batch)

        if not data.get("has_more"):
            break
        cursor = data.get("response_metadata", {}).get("next_cursor", "")
        if not cursor:
            break
        time.sleep(RATE_LIMIT_DELAY)

    return messages


def get_thread_replies(channel_id, thread_ts):
    """Pull all replies in a thread."""
    replies = []
    cursor = None
    while True:
        params = {"channel": channel_id, "ts": thread_ts, "limit": str(MESSAGES_PER_PAGE)}
        if cursor:
            params["cursor"] = cursor

        data = slack_request("conversations.replies", params)
        if not data:
            break

        batch = data.get("messages", [])
        # First message is the parent — skip it if we already have it
        if not cursor and len(batch) > 1:
            batch = batch[1:]
        replies.extend(batch)

        if not data.get("has_more"):
            break
        cursor = data.get("response_metadata", {}).get("next_cursor", "")
        if not cursor:
            break
        time.sleep(RATE_LIMIT_DELAY)

    return replies


def get_users():
    """Build a user_id -> display_name mapping."""
    users = {}
    cursor = None
    while True:
        params = {"limit": "200"}
        if cursor:
            params["cursor"] = cursor
        data = slack_request("users.list", params)
        if not data:
            break
        for member in data.get("members", []):
            uid = member["id"]
            profile = member.get("profile", {})
            name = (profile.get("display_name")
                    or profile.get("real_name")
                    or member.get("name", uid))
            users[uid] = name
        cursor = data.get("response_metadata", {}).get("next_cursor", "")
        if not cursor:
            break
        time.sleep(RATE_LIMIT_DELAY)
    return users


# ── Channel Mapping ─────────────────────────────────────────

def map_channels_interactive(conn):
    """Interactive tool to map Slack channels to clients."""
    channels = get_channels()
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id, name FROM clients WHERE status = 'active' ORDER BY name")
        clients = cur.fetchall()

    client_map = {str(c[0]): c[1] for c in clients}
    log.info(f"Found {len(channels)} Slack channels and {len(clients)} active clients")

    # Auto-match by name similarity
    matched = 0
    for ch in channels:
        ch_name = ch["name"].lower()
        for cust_id, client_name in clients:
            # Try matching on client name keywords
            name_parts = client_name.lower().replace("|", " ").split()
            # Skip common prefixes like "0 -"
            name_parts = [p for p in name_parts if len(p) > 2 and p not in ('the', 'and', 'of')]
            if any(part in ch_name for part in name_parts):
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO client_profiles (customer_id, slack_channel_id, slack_channel_name)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (customer_id) DO UPDATE SET
                            slack_channel_id = EXCLUDED.slack_channel_id,
                            slack_channel_name = EXCLUDED.slack_channel_name,
                            updated_at = NOW()
                    """, (cust_id, ch["id"], ch["name"]))
                log.info(f"  Mapped: #{ch['name']} -> {client_name}")
                matched += 1
                break

    conn.commit()
    log.info(f"Auto-mapped {matched} channels. Review client_profiles table for accuracy.")
    log.info("For unmatched channels, manually UPDATE client_profiles SET slack_channel_id = '...'")


# ── Message Processing ──────────────────────────────────────

def ts_to_datetime(ts):
    """Convert Slack timestamp to datetime."""
    return datetime.fromtimestamp(float(ts), tz=timezone.utc)


def upsert_message(cur, customer_id, channel_id, msg, user_map):
    """Insert or update a single Slack message."""
    user_id = msg.get("user", "")
    user_name = user_map.get(user_id, user_id)
    text = msg.get("text", "")
    ts = msg.get("ts", "")
    thread_ts = msg.get("thread_ts") if msg.get("thread_ts") != ts else None
    has_files = bool(msg.get("files"))
    reactions = json.dumps(msg.get("reactions")) if msg.get("reactions") else None
    posted_at = ts_to_datetime(ts)

    cur.execute("""
        INSERT INTO slack_messages (
            customer_id, channel_id, message_ts, thread_ts,
            user_id, user_name, message_text, has_files,
            reactions, posted_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (channel_id, message_ts) DO UPDATE SET
            message_text = EXCLUDED.message_text,
            reactions = EXCLUDED.reactions,
            user_name = EXCLUDED.user_name
    """, (customer_id, channel_id, ts, thread_ts,
          user_id, user_name, text, has_files,
          reactions, posted_at))


def pull_client_messages(conn, customer_id, channel_id, channel_name, user_map, backfill=False):
    """Pull messages for a single client channel."""
    stats = {"messages": 0, "threads": 0, "errors": []}

    # Determine oldest timestamp to fetch
    if backfill:
        oldest = None  # Get everything
    else:
        # Get the most recent message we have, or default to LOOKBACK_HOURS
        with conn.cursor() as cur:
            cur.execute("""
                SELECT MAX(message_ts) FROM slack_messages
                WHERE channel_id = %s
            """, (channel_id,))
            row = cur.fetchone()
            if row[0]:
                oldest = row[0]
            else:
                # First run — pull last 7 days
                oldest = str((datetime.now(timezone.utc) - timedelta(days=7)).timestamp())

    log.info(f"  Pulling #{channel_name} (oldest={oldest or 'all'})")
    messages = get_channel_history(channel_id, oldest=oldest)
    log.info(f"  Got {len(messages)} messages")

    with conn.cursor() as cur:
        for msg in messages:
            try:
                cur.execute("SAVEPOINT msg_sp")
                upsert_message(cur, customer_id, channel_id, msg, user_map)
                stats["messages"] += 1

                # Pull thread replies if this is a thread parent
                if msg.get("reply_count", 0) > 0:
                    replies = get_thread_replies(channel_id, msg["ts"])
                    for reply in replies:
                        upsert_message(cur, customer_id, channel_id, reply, user_map)
                        stats["messages"] += 1
                    stats["threads"] += 1
                    time.sleep(RATE_LIMIT_DELAY)

                cur.execute("RELEASE SAVEPOINT msg_sp")
            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT msg_sp")
                stats["errors"].append(f"msg {msg.get('ts')}: {e}")
                log.warning(f"  Error processing message: {e}")

    conn.commit()
    return stats


# ── AI Extraction ───────────────────────────────────────────

def extract_insights_from_messages(conn, customer_id, since_hours=24):
    """Use Claude to extract interactions and personal notes from recent Slack messages."""
    if not ANTHROPIC_API_KEY:
        log.info("  Skipping AI extraction (no ANTHROPIC_API_KEY)")
        return

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Get recent messages not yet processed
        since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        cur.execute("""
            SELECT user_name, message_text, posted_at
            FROM slack_messages
            WHERE customer_id = %s
              AND posted_at >= %s
              AND message_text IS NOT NULL
              AND message_text != ''
            ORDER BY posted_at
            LIMIT 200
        """, (customer_id, since))
        messages = cur.fetchall()

    if not messages:
        return

    # Get client name for context
    with conn.cursor() as cur:
        cur.execute("SELECT name FROM clients WHERE customer_id = %s", (customer_id,))
        client_name = cur.fetchone()[0]

    # Build message transcript
    transcript = "\n".join([
        f"[{m['posted_at'].strftime('%Y-%m-%d %H:%M')}] {m['user_name']}: {m['message_text']}"
        for m in messages
    ])

    prompt = f"""Analyze these Slack messages from a channel about client "{client_name}" (a mold remediation company).
Extract any of the following if present. Return JSON only.

{{
  "interactions": [
    {{
      "type": "call|meeting|email|note",
      "date": "YYYY-MM-DD",
      "summary": "brief summary of what happened",
      "action_items": "any action items mentioned",
      "sentiment": "positive|neutral|negative|at_risk",
      "attendees": ["names"]
    }}
  ],
  "personal_notes": [
    {{
      "note": "the personal detail",
      "category": "personal|preference|business_change|milestone"
    }}
  ],
  "tasks": [
    {{
      "title": "task description",
      "assigned_to": "person name or null",
      "due_date": "YYYY-MM-DD or null",
      "type": "routine|custom|website_edit|one_off"
    }}
  ]
}}

Only include items you are confident about. If nothing relevant found, return empty arrays.
Do NOT fabricate information. Only extract what is clearly stated in the messages.

Messages:
{transcript}"""

    try:
        body = json.dumps({
            "model": "claude-sonnet-4-6",
            "max_tokens": 2000,
            "messages": [{"role": "user", "content": prompt}]
        }).encode()

        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=body,
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
            method="POST"
        )

        with urllib.request.urlopen(req, timeout=60) as resp:
            result = json.loads(resp.read())
            content = result["content"][0]["text"]
            # Extract JSON from response (handle markdown code blocks)
            if "```" in content:
                content = content.split("```json")[-1].split("```")[0].strip()
                if not content:
                    content = result["content"][0]["text"].split("```")[-2].strip()
            insights = json.loads(content)

    except Exception as e:
        log.warning(f"  AI extraction failed: {e}")
        return

    # Store extracted interactions
    with conn.cursor() as cur:
        for interaction in insights.get("interactions", []):
            try:
                cur.execute("SAVEPOINT ai_sp")
                cur.execute("""
                    INSERT INTO client_interactions (
                        customer_id, interaction_type, interaction_date,
                        summary, action_items, sentiment, attendees,
                        source, source_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'slack', %s)
                    ON CONFLICT (source, source_id) WHERE source_id IS NOT NULL
                    DO NOTHING
                """, (
                    customer_id,
                    interaction.get("type", "note"),
                    interaction.get("date", datetime.now().date().isoformat()),
                    interaction.get("summary"),
                    interaction.get("action_items"),
                    interaction.get("sentiment"),
                    interaction.get("attendees"),
                    f"slack-extract-{customer_id}-{interaction.get('date', 'unknown')}"
                ))
                cur.execute("RELEASE SAVEPOINT ai_sp")
            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT ai_sp")
                log.warning(f"  Error storing interaction: {e}")

        for note in insights.get("personal_notes", []):
            try:
                cur.execute("SAVEPOINT ai_sp")
                cur.execute("""
                    INSERT INTO client_personal_notes (
                        customer_id, note, category, source, auto_extracted
                    ) VALUES (%s, %s, %s, 'slack', TRUE)
                """, (
                    customer_id,
                    note.get("note"),
                    note.get("category", "personal"),
                ))
                cur.execute("RELEASE SAVEPOINT ai_sp")
            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT ai_sp")
                log.warning(f"  Error storing personal note: {e}")

    conn.commit()
    log.info(f"  AI extracted: {len(insights.get('interactions', []))} interactions, "
             f"{len(insights.get('personal_notes', []))} personal notes")


# ── Main ────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Pull Slack messages for client intelligence")
    parser.add_argument('--client', type=str, help='Pull only one customer_id')
    parser.add_argument('--backfill', action='store_true', help='Full historical pull')
    parser.add_argument('--map-channels', action='store_true', help='Map Slack channels to clients')
    parser.add_argument('--no-ai', action='store_true', help='Skip AI extraction')
    args = parser.parse_args()

    if not SLACK_TOKEN:
        log.error("SLACK_BOT_TOKEN environment variable required")
        sys.exit(1)

    conn = get_db()

    try:
        if args.map_channels:
            map_channels_interactive(conn)
            return

        # Get user map for display names
        log.info("Fetching Slack user directory...")
        user_map = get_users()
        log.info(f"Found {len(user_map)} users")

        # Get clients with mapped Slack channels
        with conn.cursor() as cur:
            query = """
                SELECT c.customer_id, c.name, cp.slack_channel_id, cp.slack_channel_name
                FROM clients c
                JOIN client_profiles cp ON cp.customer_id = c.customer_id
                WHERE c.status = 'active'
                  AND cp.slack_channel_id IS NOT NULL
            """
            params = []
            if args.client:
                query += " AND c.customer_id = %s"
                params.append(int(args.client))
            query += " ORDER BY c.name"
            cur.execute(query, params)
            clients = cur.fetchall()

        if not clients:
            log.warning("No clients with mapped Slack channels found.")
            log.info("Run with --map-channels first to map Slack channels to clients.")
            return

        log.info(f"Pulling Slack data for {len(clients)} clients...")

        # Log the pull
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO client_intelligence_pull_log (source, started_at)
                VALUES ('slack', NOW()) RETURNING id
            """)
            pull_id = cur.fetchone()[0]
        conn.commit()

        total_messages = 0
        total_errors = 0

        for customer_id, name, channel_id, channel_name in clients:
            log.info(f"\n{'='*60}")
            log.info(f"Client: {name}")
            log.info(f"{'='*60}")

            stats = pull_client_messages(
                conn, customer_id, channel_id, channel_name,
                user_map, backfill=args.backfill
            )
            total_messages += stats["messages"]
            total_errors += len(stats["errors"])

            log.info(f"  Messages: {stats['messages']}, Threads: {stats['threads']}, "
                     f"Errors: {len(stats['errors'])}")

            # Run AI extraction on new messages
            if not args.no_ai and stats["messages"] > 0:
                extract_insights_from_messages(conn, customer_id)

            time.sleep(RATE_LIMIT_DELAY)

        # Update pull log
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE client_intelligence_pull_log
                SET finished_at = NOW(),
                    records_processed = %s,
                    status = %s
                WHERE id = %s
            """, (total_messages, 'completed' if total_errors == 0 else 'completed', pull_id))
        conn.commit()

        log.info(f"\n{'='*60}")
        log.info(f"DONE — {total_messages} messages, {total_errors} errors")
        log.info(f"{'='*60}")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
