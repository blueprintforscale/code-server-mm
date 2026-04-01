#!/usr/bin/env python3
"""
Meeting Transcript Ingestion — Parse exported ClickUp/Motion meeting notes
and store as client interactions.

Usage:
  python3 ingest_meeting_exports.py --folder /path/to/markdown/files
  python3 ingest_meeting_exports.py --folder /path/to/files --dry-run

Reads markdown files exported from ClickUp AI Notetaker, extracts:
- Meeting date, title, attendees
- Full transcript
- Summary, action items, key takeaways

Uses Claude to match meetings to clients and extract personal notes.

Requires:
  - ANTHROPIC_API_KEY env var
"""

import argparse
import glob
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone

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

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger('meeting-ingest')


def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def html_to_text(html_content):
    """Convert HTML to plain text, preserving structure."""
    import html.parser
    class HTMLTextExtractor(html.parser.HTMLParser):
        def __init__(self):
            super().__init__()
            self.result = []
            self.in_tag = None
        def handle_starttag(self, tag, attrs):
            if tag in ('h1', 'h2', 'h3'):
                self.result.append('\n## ')
            elif tag in ('p', 'div', 'br', 'li'):
                self.result.append('\n')
            elif tag == 'strong' or tag == 'b':
                self.result.append('**')
        def handle_endtag(self, tag):
            if tag in ('h1', 'h2', 'h3'):
                self.result.append('\n')
            elif tag == 'strong' or tag == 'b':
                self.result.append('**')
        def handle_data(self, data):
            self.result.append(data)

    extractor = HTMLTextExtractor()
    extractor.feed(html_content)
    return ''.join(extractor.result)


def parse_markdown_meeting(filepath):
    """Parse a ClickUp meeting notes markdown or HTML export."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Convert HTML to markdown-like text if needed
    if filepath.endswith('.html') or content.strip().startswith('<!') or content.strip().startswith('<'):
        content = html_to_text(content)

    result = {
        "filename": os.path.basename(filepath),
        "raw_content": content,
        "title": None,
        "date": None,
        "attendees": [],
        "summary": None,
        "action_items": None,
        "key_takeaways": None,
        "transcript": None,
        "topics": None,
    }

    # Try to extract title from first heading
    title_match = re.search(r'^#\s+(.+)', content, re.MULTILINE)
    if title_match:
        result["title"] = title_match.group(1).strip()

    # Extract attendees from **Attendees:** line (ClickUp format)
    attendee_match = re.search(r'\*\*Attendees:\*\*\s*(.+)', content)
    if attendee_match:
        attendee_str = attendee_match.group(1).strip()
        result["attendees"] = [a.strip() for a in attendee_str.split(',') if a.strip()]

    # Try to extract date from content or filename
    # ClickUp format: "03/10/2026" in title, or "03-10-2026" in filename
    date_patterns = [
        (r'(\d{4}-\d{2}-\d{2})', '%Y-%m-%d'),
        (r'(\d{2}-\d{2}-\d{4})', '%m-%d-%Y'),
        (r'(\d{1,2}/\d{1,2}/\d{4})', '%m/%d/%Y'),
        (r'((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4})', '%B %d, %Y'),
        (r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s+\d{4})', '%b %d, %Y'),
    ]

    # Try filename first (more reliable for ClickUp exports)
    fname = os.path.basename(filepath)
    for pattern, fmt in date_patterns:
        date_match = re.search(pattern, fname)
        if date_match:
            try:
                result["date"] = datetime.strptime(date_match.group(1), fmt)
                break
            except ValueError:
                continue

    # Then try content
    if not result["date"]:
        for pattern, fmt in date_patterns:
            date_match = re.search(pattern, content)
            if date_match:
                try:
                    date_str = date_match.group(1).strip()
                    parsed = datetime.strptime(date_str, fmt)
                    # Sanity check: year should be 2024-2027
                    if 2024 <= parsed.year <= 2027:
                        result["date"] = parsed
                        break
                except ValueError:
                    continue

    # Extract sections by heading (## or ###)
    sections = re.split(r'^#{2,3}\s+', content, flags=re.MULTILINE)
    for section in sections:
        section_lower = section.lower()
        section_body = '\n'.join(section.split('\n')[1:]).strip()

        if section_lower.startswith('attendee') or section_lower.startswith('participant'):
            if not result["attendees"]:
                attendees = re.findall(r'[-*]\s*(.+)', section_body)
                if not attendees:
                    attendees = [a.strip() for a in section_body.split('\n') if a.strip()]
                result["attendees"] = [a.strip() for a in attendees]

        elif section_lower.startswith('summary') or section_lower.startswith('overview'):
            result["summary"] = section_body

        elif section_lower.startswith('action') or section_lower.startswith('next step'):
            result["action_items"] = section_body

        elif section_lower.startswith('key takeaway') or section_lower.startswith('takeaway'):
            result["key_takeaways"] = section_body

        elif section_lower.startswith('transcript'):
            result["transcript"] = section_body

        elif section_lower.startswith('key topic') or section_lower.startswith('topic'):
            result["topics"] = section_body

    # If no structured sections found, the whole content is the transcript
    if not result["summary"] and not result["transcript"]:
        result["transcript"] = content

    return result


def match_meeting_to_client_ai(meeting, clients):
    """Use Claude to match a meeting to a client."""
    if not ANTHROPIC_API_KEY:
        return None

    title = meeting.get("title", "")
    attendees = ", ".join(meeting.get("attendees", []))
    summary = meeting.get("summary", "") or ""
    transcript_preview = (meeting.get("transcript", "") or "")[:1000]

    client_list = "\n".join([f"- {c['name']} (ID: {c['customer_id']})" for c in clients])

    prompt = f"""Match this meeting to one of our clients. Return ONLY the customer_id number, or "unknown" if no match.

Meeting title: {title}
Attendees: {attendees}
Summary: {summary[:500]}
Transcript preview: {transcript_preview[:500]}

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


def extract_insights(meeting, customer_id, client_name):
    """Use Claude to extract sentiment and personal notes from meeting content."""
    if not ANTHROPIC_API_KEY:
        return None

    content = meeting.get("summary", "") or ""
    if meeting.get("key_takeaways"):
        content += "\n" + meeting["key_takeaways"]
    if meeting.get("transcript"):
        content += "\n" + meeting["transcript"][:3000]

    if not content.strip():
        return None

    prompt = f"""Analyze this meeting for client "{client_name}" (mold remediation company).
Return JSON only.

{{
  "sentiment": "positive|neutral|negative|at_risk",
  "personal_notes": [
    {{
      "note": "the detail",
      "category": "personal|preference|business_change|milestone"
    }}
  ],
  "better_summary": "A concise 2-3 sentence summary if the existing one is poor or missing"
}}

Only include items clearly stated. Empty arrays if nothing found.

Meeting content:
{content[:4000]}"""

    try:
        body = json.dumps({
            "model": "claude-sonnet-4-6",
            "max_tokens": 1500,
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
            text = result["content"][0]["text"]
            if "```" in text:
                text = text.split("```json")[-1].split("```")[0].strip()
            return json.loads(text)
    except Exception as e:
        log.warning(f"  AI extraction failed: {e}")
        return None


def extract_multi_client_insights(conn, meeting, clients):
    """For team meetings that discuss multiple clients, extract per-client insights."""
    if not ANTHROPIC_API_KEY:
        return

    content = meeting.get("summary", "") or ""
    if meeting.get("key_takeaways"):
        content += "\n" + meeting["key_takeaways"]
    if meeting.get("topics"):
        content += "\n" + meeting["topics"]
    if meeting.get("transcript"):
        content += "\n" + meeting["transcript"][:6000]

    client_list = "\n".join(["%s (ID: %s)" % (c['name'], c['customer_id']) for c in clients])

    prompt = """Analyze this team meeting transcript. Multiple clients are discussed.
For EACH client mentioned, extract relevant information. Return JSON only.

{
  "clients_mentioned": [
    {
      "customer_id": 1234567890,
      "summary": "what was discussed about this client",
      "action_items": "any action items for this client",
      "personal_notes": [{"note": "detail", "category": "personal|preference|business_change|milestone"}],
      "sentiment": "positive|neutral|negative|at_risk"
    }
  ]
}

Only include clients that are clearly discussed. Use exact customer_id from the list.

Our clients:
%s

Meeting content:
%s""" % (client_list, content[:6000])

    try:
        body = json.dumps({
            "model": "claude-sonnet-4-6",
            "max_tokens": 4000,
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

        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read())
            text = result["content"][0]["text"]
            if "```" in text:
                text = text.split("```json")[-1].split("```")[0].strip()
            data = json.loads(text)

    except Exception as e:
        log.warning("  AI multi-client extraction failed: %s" % e)
        return

    meeting_date = meeting.get("date") or datetime.now(timezone.utc)
    if not hasattr(meeting_date, 'tzinfo') or not meeting_date.tzinfo:
        meeting_date = meeting_date.replace(tzinfo=timezone.utc)
    title = meeting.get("title", meeting.get("filename", "Team Meeting"))

    with conn.cursor() as cur:
        for cm in data.get("clients_mentioned", []):
            cid = cm.get("customer_id")
            if not cid:
                continue

            # Store interaction for this client
            source_id = "clickup-export-%s-%s" % (meeting.get("filename", "unknown"), cid)
            try:
                cur.execute("""
                    INSERT INTO client_interactions (
                        customer_id, interaction_type, interaction_date,
                        attendees, summary, action_items, sentiment,
                        source, source_id
                    ) VALUES (%s, 'meeting', %s, %s, %s, %s, %s, 'clickup_export', %s)
                    ON CONFLICT (source, source_id) WHERE source_id IS NOT NULL DO NOTHING
                """, (
                    cid, meeting_date,
                    meeting.get("attendees") or None,
                    cm.get("summary", "")[:5000] or None,
                    cm.get("action_items", "")[:5000] or None,
                    cm.get("sentiment"),
                    source_id,
                ))
            except Exception as e:
                log.warning("  Error storing for client %s: %s" % (cid, e))
                conn.rollback()
                continue

            # Store personal notes
            for note in cm.get("personal_notes", []):
                if note.get("note"):
                    cur.execute("""
                        INSERT INTO client_personal_notes (
                            customer_id, note, category, source, auto_extracted
                        ) VALUES (%s, %s, %s, 'clickup_export', TRUE)
                    """, (cid, note["note"], note.get("category", "personal")))

    conn.commit()
    clients_found = len(data.get("clients_mentioned", []))
    log.info("  Extracted insights for %d clients from team meeting" % clients_found)


def store_meeting(conn, customer_id, meeting, insights=None):
    """Store meeting as a client interaction."""
    title = meeting.get("title", meeting.get("filename", "Unknown Meeting"))
    meeting_date = meeting.get("date") or datetime.now(timezone.utc)
    if not meeting_date.tzinfo:
        meeting_date = meeting_date.replace(tzinfo=timezone.utc)

    summary = meeting.get("summary", "")
    if insights and insights.get("better_summary") and not summary:
        summary = insights["better_summary"]

    action_items = meeting.get("action_items", "")
    if meeting.get("key_takeaways"):
        if action_items:
            action_items += "\n\nKey Takeaways:\n" + meeting["key_takeaways"]
        else:
            action_items = "Key Takeaways:\n" + meeting["key_takeaways"]

    sentiment = insights.get("sentiment") if insights else None
    source_id = f"clickup-export-{meeting.get('filename', 'unknown')}"

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO client_interactions (
                customer_id, interaction_type, interaction_date,
                attendees, summary, action_items, sentiment,
                source, source_id, transcript
            ) VALUES (%s, 'meeting', %s, %s, %s, %s, %s, 'clickup_export', %s, %s)
            ON CONFLICT (source, source_id) WHERE source_id IS NOT NULL
            DO UPDATE SET
                summary = EXCLUDED.summary,
                action_items = EXCLUDED.action_items,
                sentiment = EXCLUDED.sentiment,
                transcript = EXCLUDED.transcript,
                updated_at = NOW()
        """, (
            customer_id,
            meeting_date,
            meeting.get("attendees") or None,
            summary[:5000] if summary else None,
            action_items[:5000] if action_items else None,
            sentiment,
            source_id,
            (meeting.get("transcript") or "")[:50000] or None,
        ))

        # Store personal notes
        if insights:
            for note in insights.get("personal_notes", []):
                if note.get("note"):
                    cur.execute("""
                        INSERT INTO client_personal_notes (
                            customer_id, note, category, source, auto_extracted
                        ) VALUES (%s, %s, %s, 'clickup_export', TRUE)
                    """, (customer_id, note["note"], note.get("category", "personal")))

    conn.commit()


def main():
    parser = argparse.ArgumentParser(description="Ingest exported meeting transcripts")
    parser.add_argument('--folder', required=True, help='Path to folder with markdown files')
    parser.add_argument('--dry-run', action='store_true', help='Parse and match only, do not store')
    parser.add_argument('--no-ai', action='store_true', help='Skip AI extraction')
    args = parser.parse_args()

    if not os.path.isdir(args.folder):
        log.error(f"Folder not found: {args.folder}")
        sys.exit(1)

    conn = get_db()

    try:
        # Load clients for matching
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT customer_id, name FROM clients WHERE status = 'active' ORDER BY name")
            clients = cur.fetchall()

        # Find all markdown and HTML files
        md_files = sorted(glob.glob(os.path.join(args.folder, '*.md')))
        md_files += sorted(glob.glob(os.path.join(args.folder, '*.html')))
        if not md_files:
            md_files = sorted(glob.glob(os.path.join(args.folder, '*.txt')))
        if not md_files:
            md_files = sorted(glob.glob(os.path.join(args.folder, '**/*.md'), recursive=True))
            md_files += sorted(glob.glob(os.path.join(args.folder, '**/*.html'), recursive=True))

        log.info(f"Found {len(md_files)} files in {args.folder}")

        stored = 0
        unmatched = 0

        for filepath in md_files:
            filename = os.path.basename(filepath)
            log.info(f"\nProcessing: {filename}")

            # Parse the markdown
            meeting = parse_markdown_meeting(filepath)
            log.info(f"  Title: {meeting.get('title', 'N/A')}")
            log.info(f"  Date: {meeting.get('date', 'N/A')}")
            log.info(f"  Attendees: {len(meeting.get('attendees', []))}")
            log.info(f"  Has summary: {bool(meeting.get('summary'))}")
            log.info(f"  Has transcript: {bool(meeting.get('transcript'))}")

            # Match to client
            customer_id = match_meeting_to_client_ai(meeting, clients)
            time.sleep(0.5)

            if customer_id:
                client_name = next((c['name'] for c in clients if c['customer_id'] == customer_id), 'Unknown')
                log.info(f"  Matched to: {client_name} ({customer_id})")
            else:
                log.info(f"  No specific client match — extracting multi-client insights")
                if not args.dry_run and not args.no_ai:
                    extract_multi_client_insights(conn, meeting, clients)
                unmatched += 1
                continue

            if args.dry_run:
                log.info(f"  [DRY RUN] Would store this meeting")
                stored += 1
                continue

            # Extract insights
            insights = None
            if not args.no_ai:
                insights = extract_insights(meeting, customer_id, client_name)
                if insights:
                    notes_count = len(insights.get("personal_notes", []))
                    log.info(f"  Sentiment: {insights.get('sentiment')}, Notes: {notes_count}")
                time.sleep(1)

            # Store it
            store_meeting(conn, customer_id, meeting, insights)
            stored += 1
            log.info(f"  Stored successfully")

        log.info(f"\n{'='*60}")
        log.info(f"DONE — {stored} stored, {unmatched} unmatched out of {len(md_files)} files")
        log.info(f"{'='*60}")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
