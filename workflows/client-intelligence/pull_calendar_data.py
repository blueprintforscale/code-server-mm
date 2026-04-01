#!/usr/bin/env python3
"""
Google Calendar ETL — Pull client meetings from Blueprint team calendars.

Usage:
  python3 pull_calendar_data.py                          # Recent events (last 7 days + next 7 days)
  python3 pull_calendar_data.py --backfill               # Last 90 days
  python3 pull_calendar_data.py --backfill --days 180    # Last 180 days
  python3 pull_calendar_data.py --client 7123434733      # Filter to one client
  python3 pull_calendar_data.py --inbox susie@blueprintforscale.com  # One calendar only

Pulls from authorized Google Calendars, matches events to clients by attendee
email/name or event title, and stores as client interactions.

Requires:
  - calendar_tokens/<email>.json for each calendar (run calendar_auth.py first)
  - ANTHROPIC_API_KEY env var (for client matching from event details)
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
TOKENS_DIR = os.path.join(os.path.dirname(__file__), "calendar_tokens")

RATE_LIMIT_DELAY = 0.1
DEFAULT_LOOKBACK_DAYS = 7
DEFAULT_LOOKAHEAD_DAYS = 7
BACKFILL_DAYS = 90

# Blueprint team emails
TEAM_EMAILS = {
    "info@blueprintforscale.com",
    "susie@blueprintforscale.com",
    "martin@blueprintforscale.com",
    "josh@blueprintforscale.com",
    "kiana@blueprintforscale.com",
}

# Internal meeting titles to skip (not client meetings)
SKIP_TITLES = {
    'team sync', 'team meeting', 'standup', 'stand-up', 'all hands',
    'internal', '1:1', 'one on one', 'staff meeting', 'huddle',
    'blueprint team', 'weekly sync', 'daily sync',
}

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger('calendar-sync')


# ── Database ────────────────────────────────────────────────

def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


# ── Google Calendar API ─────────────────────────────────────

def get_calendar_service(token_path):
    """Build an authenticated Calendar API service from a saved token."""
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

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        token_data["token"] = creds.token
        with open(token_path, "w") as f:
            json.dump(token_data, f, indent=2)

    return build("calendar", "v3", credentials=creds, cache_discovery=False)


def list_all_calendars(service):
    """List all calendars visible to this account."""
    result = service.calendarList().list().execute()
    return result.get("items", [])


def list_events(service, time_min, time_max, calendar_id="primary", max_results=500):
    """List calendar events in a date range."""
    events = []
    page_token = None

    while True:
        result = service.events().list(
            calendarId=calendar_id,
            timeMin=time_min.isoformat(),
            timeMax=time_max.isoformat(),
            maxResults=min(max_results - len(events), 250),
            singleEvents=True,
            orderBy="startTime",
            pageToken=page_token,
        ).execute()

        batch = result.get("items", [])
        events.extend(batch)

        if len(events) >= max_results:
            break

        page_token = result.get("nextPageToken")
        if not page_token:
            break

        time.sleep(RATE_LIMIT_DELAY)

    return events


# ── Client Matching ─────────────────────────────────────────

def build_client_lookup(conn):
    """Build lookup tables for matching events to clients."""
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

    # Email -> customer_id
    email_map = {}
    for client in clients:
        for email_field in ['owner_email', 'contact_email']:
            addr = client.get(email_field)
            if addr:
                email_map[addr.lower().strip()] = client['customer_id']

    for contact in contacts:
        if contact['email']:
            email_map[contact['email'].lower().strip()] = contact['customer_id']

    # Name fragments -> customer_id (for title matching)
    # Use longer fragments (5+ chars) to avoid false matches on common names
    # like "greg", "scott", "david", "mark", "mike"
    name_map = {}
    for client in clients:
        name = client['name']
        parts = name.replace("|", " ").replace("-", " ").split()
        for part in parts:
            if len(part) > 4 and part.lower() not in (
                'pure', 'maintenance', 'mold', 'the', 'and', 'inc', 'llc', 'usa',
                'pro', 'service', 'services', 'solutions', 'state', 'county',
                'south', 'north', 'east', 'west', 'central', 'mountain',
            ):
                name_map[part.lower()] = client['customer_id']

    return clients, email_map, name_map


def match_event_to_client(event, email_map, name_map):
    """Try to match a calendar event to a client."""
    # 1. Match by attendee email
    attendees = event.get("attendees", [])
    for attendee in attendees:
        email = attendee.get("email", "").lower()
        if email and email not in TEAM_EMAILS:
            customer_id = email_map.get(email)
            if customer_id:
                return customer_id

    # 2. Match by event title
    title = (event.get("summary") or "").lower()
    for name_fragment, customer_id in name_map.items():
        if name_fragment in title:
            return customer_id

    # 3. Match by description
    description = (event.get("description") or "").lower()
    for name_fragment, customer_id in name_map.items():
        if name_fragment in description:
            return customer_id

    return None


# ── Event Parsing ───────────────────────────────────────────

def parse_dt(dt_str):
    """Parse a datetime string, handling Z suffix for Python 3.9."""
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))


def parse_event(event):
    """Extract useful fields from a calendar event."""
    # Handle all-day events vs timed events
    start = event.get("start", {})
    if "dateTime" in start:
        event_start = parse_dt(start["dateTime"])
    elif "date" in start:
        event_start = datetime.fromisoformat(start["date"] + "T00:00:00").replace(tzinfo=timezone.utc)
    else:
        event_start = datetime.now(timezone.utc)

    end = event.get("end", {})
    if "dateTime" in end:
        event_end = parse_dt(end["dateTime"])
    elif "date" in end:
        event_end = datetime.fromisoformat(end["date"] + "T00:00:00").replace(tzinfo=timezone.utc)
    else:
        event_end = event_start

    # Duration in minutes
    duration_min = int((event_end - event_start).total_seconds() / 60)

    # Attendees
    attendee_names = []
    for att in event.get("attendees", []):
        name = att.get("displayName") or att.get("email", "")
        status = att.get("responseStatus", "needsAction")
        if name:
            attendee_names.append(f"{name} ({status})")

    # Organizer
    organizer = event.get("organizer", {})
    organizer_name = organizer.get("displayName") or organizer.get("email", "")

    # Conference link (Zoom, Meet, etc.)
    conference_url = None
    entry_points = event.get("conferenceData", {}).get("entryPoints", [])
    for ep in entry_points:
        if ep.get("entryPointType") == "video":
            conference_url = ep.get("uri")
            break

    # Hangout link fallback
    if not conference_url:
        conference_url = event.get("hangoutLink")

    return {
        "event_id": event["id"],
        "title": event.get("summary", "(no title)"),
        "description": event.get("description", ""),
        "start": event_start,
        "end": event_end,
        "duration_min": duration_min,
        "location": event.get("location", ""),
        "organizer": organizer_name,
        "attendees": attendee_names,
        "conference_url": conference_url,
        "status": event.get("status", "confirmed"),
        "recurring_event_id": event.get("recurringEventId"),
        "is_all_day": "date" in start and "dateTime" not in start,
    }


# ── Main Processing ─────────────────────────────────────────

def process_calendar(inbox_email, conn, email_map, name_map, clients,
                     lookback_days, lookahead_days, target_client=None):
    """Process all team calendars visible from this account."""
    token_path = os.path.join(TOKENS_DIR, f"{inbox_email}.json")
    if not os.path.exists(token_path):
        log.warning(f"No token for {inbox_email} — run: python3 calendar_auth.py {inbox_email}")
        return 0

    log.info(f"Processing calendars via: {inbox_email}")

    try:
        service = get_calendar_service(token_path)
    except Exception as e:
        log.error(f"Failed to auth {inbox_email}: {e}")
        return 0

    # Find all team calendars visible from this account
    all_cals = list_all_calendars(service)
    team_cals = []
    for cal in all_cals:
        cal_id = cal.get("id", "")
        cal_name = cal.get("summary", "")
        # Include @blueprintforscale.com calendars only
        if "blueprintforscale.com" in cal_id:
            team_cals.append({"id": cal_id, "name": cal_name})

    if not team_cals:
        team_cals = [{"id": "primary", "name": inbox_email}]

    log.info(f"  Found {len(team_cals)} team calendars: {', '.join(c['name'] or c['id'] for c in team_cals)}")

    now = datetime.now(timezone.utc)
    time_min = now - timedelta(days=lookback_days)
    time_max = now + timedelta(days=lookahead_days)

    all_events = []
    for cal in team_cals:
        try:
            cal_events = list_events(service, time_min, time_max, calendar_id=cal["id"])
            # Tag each event with which calendar it came from
            for ev in cal_events:
                ev["_calendar_owner"] = cal["id"]
            all_events.extend(cal_events)
            log.info(f"    {cal['name'] or cal['id']}: {len(cal_events)} events")
        except Exception as e:
            log.warning(f"    Failed to read {cal['name'] or cal['id']}: {e}")

    events = all_events
    log.info(f"  Total: {len(events)} events across {len(team_cals)} calendars")

    ingested = 0
    skipped_dedup = 0
    skipped_no_match = 0
    skipped_allday = 0
    skipped_cancelled = 0

    for event in events:
        # Skip cancelled events
        if event.get("status") == "cancelled":
            skipped_cancelled += 1
            continue

        parsed = parse_event(event)

        # Skip all-day events (usually holidays, OOO, etc.)
        if parsed["is_all_day"]:
            skipped_allday += 1
            continue

        # Skip very short events (< 10 min, likely reminders)
        if parsed["duration_min"] < 10:
            continue

        # Skip internal team meetings
        title_lower = parsed["title"].lower()
        if any(skip in title_lower for skip in SKIP_TITLES):
            continue

        # Dedup by event ID + calendar owner
        cal_owner = event.get("_calendar_owner", inbox_email)
        source_id = f"cal-{cal_owner}-{parsed['event_id']}"
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM client_interactions WHERE source = 'calendar' AND source_id = %s",
                (source_id,)
            )
            if cur.fetchone():
                skipped_dedup += 1
                continue

        # Match to client
        customer_id = match_event_to_client(event, email_map, name_map)
        if not customer_id:
            skipped_no_match += 1
            continue

        if target_client and customer_id != target_client:
            continue

        # Determine if this is past or upcoming
        is_upcoming = parsed["start"] > now

        # Build summary
        attendee_list = ", ".join(parsed["attendees"][:5]) if parsed["attendees"] else ""
        duration_str = f"{parsed['duration_min']}min"

        summary_parts = [f"[{parsed['title']}]"]
        summary_parts.append(f"({duration_str})")
        if attendee_list:
            summary_parts.append(f"with {attendee_list}")
        if is_upcoming:
            summary_parts.append("[UPCOMING]")
        if parsed["conference_url"]:
            summary_parts.append(f"[{parsed['conference_url']}]")

        summary = " ".join(summary_parts)

        # Determine interaction type
        interaction_type = "meeting"

        # Logged by = the calendar owner
        logged_by = cal_owner.split("@")[0].title()

        # Save
        try:
            with conn.cursor() as cur:
                cur.execute("SAVEPOINT cal_event")
                cur.execute("""
                    INSERT INTO client_interactions
                        (customer_id, interaction_type, interaction_date, logged_by,
                         attendees, summary, sentiment, source, source_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'calendar', %s)
                    ON CONFLICT (source, source_id) WHERE source_id IS NOT NULL
                    DO NOTHING
                """, (
                    customer_id,
                    interaction_type,
                    parsed["start"],
                    logged_by,
                    parsed["attendees"] if parsed["attendees"] else None,
                    summary[:500],
                    None,  # No sentiment for calendar events
                    source_id,
                ))
                cur.execute("RELEASE SAVEPOINT cal_event")
            conn.commit()
            ingested += 1

        except Exception as e:
            with conn.cursor() as cur:
                cur.execute("ROLLBACK TO SAVEPOINT cal_event")
            log.error(f"  Failed to save event {parsed['event_id']}: {e}")
            continue

    log.info(f"  {inbox_email}: {ingested} events ingested, "
             f"{skipped_dedup} already seen, {skipped_no_match} no client match, "
             f"{skipped_allday} all-day, {skipped_cancelled} cancelled")
    return ingested


def main():
    parser = argparse.ArgumentParser(description="Pull Google Calendar data for client intelligence")
    parser.add_argument("--backfill", action="store_true", help="Pull last 90 days")
    parser.add_argument("--days", type=int, help="Custom lookback days")
    parser.add_argument("--ahead", type=int, default=DEFAULT_LOOKAHEAD_DAYS, help="Days to look ahead (default 7)")
    parser.add_argument("--client", type=int, help="Filter to one client (customer_id)")
    parser.add_argument("--inbox", help="Process only one calendar (email address)")
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
            VALUES ('calendar', 'running')
            RETURNING id
        """)
        pull_id = cur.fetchone()[0]
    conn.commit()

    # Build client lookup
    clients, email_map, name_map = build_client_lookup(conn)
    log.info(f"Loaded {len(email_map)} email mappings + {len(name_map)} name fragments")

    # Find all authorized calendars
    if args.inbox:
        inboxes = [args.inbox]
    else:
        inboxes = []
        if os.path.exists(TOKENS_DIR):
            for f in sorted(os.listdir(TOKENS_DIR)):
                if f.endswith(".json"):
                    inboxes.append(f.replace(".json", ""))

    if not inboxes:
        log.error("No authorized calendars found. Run calendar_auth.py first.")
        sys.exit(1)

    log.info(f"Processing {len(inboxes)} calendar(s): {lookback_days} days back, {args.ahead} days ahead")

    total = 0
    errors = []
    for inbox in inboxes:
        try:
            count = process_calendar(inbox, conn, email_map, name_map, clients,
                                     lookback_days, args.ahead, args.client)
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
    log.info(f"Done. {total} total calendar events ingested across {len(inboxes)} calendars.")


if __name__ == "__main__":
    main()
