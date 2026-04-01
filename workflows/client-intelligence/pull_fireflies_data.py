#!/usr/bin/env python3
"""
Fireflies ETL — Pull meeting transcripts, summaries, and action items.

Usage:
  python3 pull_fireflies_data.py                          # Recent meetings (last 7 days)
  python3 pull_fireflies_data.py --backfill               # All historical meetings
  python3 pull_fireflies_data.py --backfill --days 90     # Last 90 days
  python3 pull_fireflies_data.py --client 7123434733      # Filter to one client

Pulls from Fireflies GraphQL API, matches meetings to clients by participant
email/name, and stores transcripts + summaries as client interactions.

Requires:
  - FIREFLIES_API_KEY env var
  - ANTHROPIC_API_KEY env var (for client matching from transcript content)
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

FIREFLIES_ENDPOINT = "https://api.fireflies.ai/graphql"
FIREFLIES_API_KEY = os.environ.get("FIREFLIES_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

RATE_LIMIT_DELAY = 1.0
DEFAULT_LOOKBACK_DAYS = 7

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger('fireflies-sync')


# ── Database ────────────────────────────────────────────────

def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


# ── Fireflies GraphQL API ──────────────────────────────────

def fireflies_query(query, variables=None, retries=3):
    """Execute a Fireflies GraphQL query."""
    body = json.dumps({
        "query": query,
        "variables": variables or {}
    }).encode()

    req = urllib.request.Request(
        FIREFLIES_ENDPOINT,
        data=body,
        headers={
            "Authorization": f"Bearer {FIREFLIES_API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read())
            if "errors" in data:
                log.error(f"GraphQL errors: {data['errors']}")
                return None
            return data.get("data")
    except urllib.error.HTTPError as e:
        if e.code == 429 and retries > 0:
            log.warning("Rate limited, waiting 10s...")
            time.sleep(10)
            return fireflies_query(query, variables, retries - 1)
        body_text = e.read().decode()[:200]
        log.error(f"HTTP {e.code}: {body_text}")
        return None
    except Exception as e:
        log.error(f"Request error: {e}")
        return None


def list_transcripts(from_date_ms=None, to_date_ms=None, limit=50, skip=0):
    """List transcripts with optional date filtering."""
    query = """
    query ListTranscripts($limit: Int, $skip: Int) {
      transcripts(limit: $limit, skip: $skip) {
        id
        title
        date
        duration
        host_email
        organizer_email
        participants
        summary {
          overview
          action_items
          keywords
          short_summary
        }
      }
    }
    """
    variables = {"limit": limit, "skip": skip}

    return fireflies_query(query, variables)


def get_transcript_detail(transcript_id):
    """Get full transcript with speaker labels and sentences."""
    query = """
    query GetTranscript($transcriptId: String!) {
      transcript(id: $transcriptId) {
        id
        title
        date
        duration
        host_email
        organizer_email
        participants {
          name
        }
        speakers {
          id
          name
        }
        sentences {
          index
          text
          speaker_name
          start_time
          end_time
        }
        summary {
          overview
          action_items
          keywords
          short_summary
          outline
        }
      }
    }
    """
    return fireflies_query(query, {"transcriptId": transcript_id})


# ── Client Matching ─────────────────────────────────────────

def build_client_lookup(conn):
    """Build lookup tables for matching meetings to clients."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Get client names, emails, and contact emails
        cur.execute("""
            SELECT c.customer_id, c.name, c.owner_email, c.contact_email,
                   cp.slack_channel_name
            FROM clients c
            LEFT JOIN client_profiles cp ON cp.customer_id = c.customer_id
            WHERE c.status = 'active'
        """)
        clients = cur.fetchall()

        # Get client contacts
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
            email = client.get(email_field)
            if email:
                email_map[email.lower().strip()] = client['customer_id']

    for contact in contacts:
        if contact['email']:
            email_map[contact['email'].lower().strip()] = contact['customer_id']

    # Build name fragment -> customer_id map (for title matching)
    # Use multi-word fragments to avoid ambiguity (e.g., "david" matches multiple clients)
    name_map = {}
    common_words = {'pure', 'maintenance', 'mold', 'the', 'and', 'of', 'llc', 'inc'}
    for client in clients:
        name = client['name']
        # Split on pipe to get owner name and business name
        parts = name.split('|') if '|' in name else name.split(' - ')

        if len(parts) > 1:
            owner = parts[0].strip().lower()
            business = parts[1].strip().lower()

            # Full business name (most specific, highest priority)
            if len(business) > 5:
                name_map[business] = client['customer_id']

            # Business name without common words (e.g., "pure maintenance ohio" -> "ohio")
            biz_words = [w for w in business.split() if w not in common_words and len(w) > 3]
            if biz_words:
                # Use the most unique word (longest, least common)
                unique_word = max(biz_words, key=len)
                if len(unique_word) > 4:
                    name_map[unique_word] = client['customer_id']

            # Owner last name (more unique than first name)
            owner_parts = owner.replace(',', '').split()
            if len(owner_parts) >= 2:
                last_name = owner_parts[-1]
                if len(last_name) > 3 and last_name not in common_words:
                    name_map[last_name] = client['customer_id']

            # Full owner name as a phrase
            if len(owner) > 5:
                name_map[owner] = client['customer_id']
        else:
            # Single name (no pipe), use the whole thing
            if len(name.strip()) > 5:
                name_map[name.strip().lower()] = client['customer_id']

    # Add email-based matching from attendee domains
    # (e.g., pmoforegon@gmail.com -> match "pmoforegon" isn't useful,
    #  but we can add known client email prefixes)

    return clients, email_map, name_map


def match_meeting_to_client(transcript, email_map, name_map):
    """Try to match a Fireflies meeting to a client."""
    # 0. Skip known internal meeting titles
    title = (transcript.get("title") or "").lower()
    internal_titles = [
        "team sync", "partner sync", "projects sync", "investments sync",
        "team meeting", "standup", "stand-up", "internal", "1:1", "one on one",
        "all hands", "retrospective", "sprint", "planning"
    ]
    if any(t in title for t in internal_titles):
        return None

    # 1. Match by participant email — only non-Blueprint emails
    blueprint_domains = {"blueprintforscale.com", "mypurecompanies.com"}
    for participant in transcript.get("participants", []):
        email = str(participant).lower().strip()
        if email and email in email_map:
            domain = email.split("@")[1] if "@" in email else ""
            if domain not in blueprint_domains:
                return email_map[email]

    # 2. Match by meeting title (only if has non-Blueprint attendees)
    has_external = any(
        "@" in str(p) and str(p).lower().split("@")[1] not in blueprint_domains
        for p in transcript.get("participants", [])
    )
    if has_external:
        for name_fragment, customer_id in name_map.items():
            if name_fragment in title:
                return customer_id

    return None


# ── AI Client Matching (fallback) ──────────────────────────

def ai_match_client(transcript, clients):
    """Use Claude to match a meeting to a client based on content."""
    if not ANTHROPIC_API_KEY:
        return None

    title = transcript.get("title", "")
    participants = ", ".join([str(p) for p in transcript.get("participants", [])])
    summary = ""
    if transcript.get("summary"):
        summary = transcript["summary"].get("overview", "") or transcript["summary"].get("short_summary", "")

    client_list = "\n".join([f"- {c['name']} (ID: {c['customer_id']})" for c in clients])

    prompt = f"""Match this meeting to one of our clients. Return ONLY the customer_id number, or "unknown" if no match.

Meeting title: {title}
Participants: {participants}
Summary: {summary[:500]}

Our clients:
{client_list}

Return only the customer_id number or "unknown". Nothing else."""

    try:
        body = json.dumps({
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 50,
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

        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())
            answer = result["content"][0]["text"].strip()
            if answer != "unknown" and answer.isdigit():
                return int(answer)
    except Exception as e:
        log.warning(f"  AI matching failed: {e}")

    return None


# ── Store Meeting Data ──────────────────────────────────────

def store_meeting(conn, customer_id, transcript):
    """Store a Fireflies meeting as a client interaction."""
    ff_id = transcript.get("id")
    title = transcript.get("title", "")
    date_val = transcript.get("date")
    duration = transcript.get("duration")

    # Parse date (Fireflies returns epoch ms or ISO string)
    if isinstance(date_val, (int, float)):
        meeting_date = datetime.fromtimestamp(date_val / 1000, tz=timezone.utc)
    elif isinstance(date_val, str):
        try:
            meeting_date = datetime.fromisoformat(date_val.replace("Z", "+00:00"))
        except ValueError:
            meeting_date = datetime.now(timezone.utc)
    else:
        meeting_date = datetime.now(timezone.utc)

    # Build attendee list (participants are email strings)
    attendees = [str(p) for p in transcript.get("participants", [])]

    # Summary and action items
    summary_data = transcript.get("summary") or {}
    summary = summary_data.get("overview") or summary_data.get("short_summary") or ""
    action_items = summary_data.get("action_items") or ""
    if isinstance(action_items, list):
        action_items = "\n".join(f"- {item}" for item in action_items)

    # Build transcript text from sentences if available
    transcript_text = None
    sentences = transcript.get("sentences")
    if sentences:
        transcript_text = "\n".join([
            f"{s.get('speaker_name', 'Unknown')}: {s.get('text', '')}"
            for s in sentences
        ])

    # Duration in minutes
    duration_min = None
    if duration:
        duration_min = round(duration / 60) if duration > 60 else duration

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO client_interactions (
                customer_id, interaction_type, interaction_date,
                logged_by, attendees, summary, action_items,
                source, source_id, transcript
            ) VALUES (%s, 'meeting', %s, %s, %s, %s, %s, 'fireflies', %s, %s)
            ON CONFLICT (source, source_id) WHERE source_id IS NOT NULL
            DO UPDATE SET
                summary = EXCLUDED.summary,
                action_items = EXCLUDED.action_items,
                transcript = EXCLUDED.transcript,
                attendees = EXCLUDED.attendees,
                updated_at = NOW()
        """, (
            customer_id,
            meeting_date,
            transcript.get("host_email"),
            attendees if attendees else None,
            summary[:5000] if summary else None,
            action_items[:5000] if action_items else None,
            f"ff-{ff_id}",
            transcript_text[:50000] if transcript_text else None,
        ))

    conn.commit()


# ── AI Extraction from Transcript ───────────────────────────

def extract_insights_from_transcript(conn, customer_id, transcript):
    """Use Claude to extract personal notes and sentiment from a meeting transcript."""
    if not ANTHROPIC_API_KEY:
        return

    summary_data = transcript.get("summary") or {}
    summary = summary_data.get("overview") or summary_data.get("short_summary") or ""
    if not summary:
        return

    # Get client name
    with conn.cursor() as cur:
        cur.execute("SELECT name FROM clients WHERE customer_id = %s", (customer_id,))
        row = cur.fetchone()
        client_name = row[0] if row else "Unknown"

    prompt = f"""Analyze this meeting summary for client "{client_name}" (mold remediation company).
Extract personal notes and sentiment. Return JSON only.

{{
  "sentiment": "positive|neutral|negative|at_risk",
  "personal_notes": [
    {{
      "note": "the personal/business detail",
      "category": "personal|preference|business_change|milestone"
    }}
  ]
}}

Only include items clearly stated. Return empty array if nothing found.

Meeting summary:
{summary[:3000]}"""

    try:
        body = json.dumps({
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 1000,
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

        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())
            content = result["content"][0]["text"]
            if "```" in content:
                content = content.split("```json")[-1].split("```")[0].strip()
            insights = json.loads(content)

    except Exception as e:
        log.warning(f"  AI extraction failed: {e}")
        return

    with conn.cursor() as cur:
        # Update sentiment on the interaction
        if insights.get("sentiment"):
            ff_id = transcript.get("id")
            cur.execute("""
                UPDATE client_interactions
                SET sentiment = %s
                WHERE source = 'fireflies' AND source_id = %s
            """, (insights["sentiment"], f"ff-{ff_id}"))

        # Store personal notes
        for note in insights.get("personal_notes", []):
            if note.get("note"):
                cur.execute("""
                    INSERT INTO client_personal_notes (
                        customer_id, note, category, source, auto_extracted
                    ) VALUES (%s, %s, %s, 'fireflies', TRUE)
                """, (customer_id, note["note"], note.get("category", "personal")))

    conn.commit()

    notes_count = len(insights.get("personal_notes", []))
    if notes_count > 0:
        log.info(f"    Extracted {notes_count} personal notes, sentiment: {insights.get('sentiment')}")


# ── Main ────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Pull Fireflies meeting data")
    parser.add_argument('--client', type=str, help='Filter to one customer_id')
    parser.add_argument('--backfill', action='store_true', help='Pull all historical meetings')
    parser.add_argument('--days', type=int, default=DEFAULT_LOOKBACK_DAYS, help='Days to look back')
    parser.add_argument('--no-ai', action='store_true', help='Skip AI extraction')
    parser.add_argument('--no-transcript', action='store_true', help='Skip full transcript pull (faster)')
    args = parser.parse_args()

    if not FIREFLIES_API_KEY:
        log.error("FIREFLIES_API_KEY environment variable required")
        sys.exit(1)

    conn = get_db()

    try:
        # Build client lookup
        clients, email_map, name_map = build_client_lookup(conn)
        log.info(f"Loaded {len(clients)} clients, {len(email_map)} email mappings")

        # Determine date range
        if args.backfill:
            from_date_ms = None
            to_date_ms = None
            log.info("Backfilling all historical meetings...")
        else:
            now = datetime.now(timezone.utc)
            from_date = now - timedelta(days=args.days)
            from_date_ms = int(from_date.timestamp() * 1000)
            to_date_ms = int(now.timestamp() * 1000)
            log.info(f"Pulling meetings from last {args.days} days...")

        # Log the pull
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO client_intelligence_pull_log (source, started_at)
                VALUES ('fireflies', NOW()) RETURNING id
            """)
            pull_id = cur.fetchone()[0]
        conn.commit()

        # Fetch all transcripts with pagination
        all_transcripts = []
        skip = 0
        while True:
            data = list_transcripts(from_date_ms, to_date_ms, limit=50, skip=skip)
            if not data:
                break
            batch = data.get("transcripts", [])
            if not batch:
                break
            all_transcripts.extend(batch)
            log.info(f"  Fetched {len(all_transcripts)} meetings so far...")
            if len(batch) < 50:
                break
            skip += 50
            time.sleep(RATE_LIMIT_DELAY)

        log.info(f"Total meetings found: {len(all_transcripts)}")

        # Process each transcript
        matched = 0
        unmatched = 0
        stored = 0

        for transcript in all_transcripts:
            title = transcript.get("title", "Untitled")
            ff_id = transcript.get("id")

            # Check if already processed
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id FROM client_interactions
                    WHERE source = 'fireflies' AND source_id = %s
                """, (f"ff-{ff_id}",))
                if cur.fetchone():
                    continue

            # Match to client
            customer_id = match_meeting_to_client(transcript, email_map, name_map)

            # AI fallback matching
            if not customer_id and not args.no_ai:
                customer_id = ai_match_client(transcript, clients)
                time.sleep(0.5)

            if not customer_id:
                unmatched += 1
                participants = [str(p) for p in transcript.get("participants", [])]
                log.info(f"  UNMATCHED: {title} | Participants: {', '.join(participants[:3])}")
                continue

            # Filter to specific client if requested
            if args.client and str(customer_id) != args.client:
                continue

            matched += 1
            log.info(f"  Matched: {title} -> customer_id {customer_id}")

            # Get full transcript detail if requested
            if not args.no_transcript:
                detail = get_transcript_detail(ff_id)
                if detail and detail.get("transcript"):
                    transcript = detail["transcript"]
                time.sleep(RATE_LIMIT_DELAY)

            # Store the meeting
            try:
                store_meeting(conn, customer_id, transcript)
                stored += 1

                # Extract insights
                if not args.no_ai:
                    extract_insights_from_transcript(conn, customer_id, transcript)
                    time.sleep(0.5)

            except Exception as e:
                log.error(f"  Error storing meeting: {e}")
                conn.rollback()

        # Update pull log
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE client_intelligence_pull_log
                SET finished_at = NOW(),
                    records_processed = %s,
                    status = 'completed'
                WHERE id = %s
            """, (stored, pull_id))
        conn.commit()

        log.info(f"\n{'='*60}")
        log.info(f"DONE — {matched} matched, {unmatched} unmatched, {stored} stored")
        log.info(f"{'='*60}")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
