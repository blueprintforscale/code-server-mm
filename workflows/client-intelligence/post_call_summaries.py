#!/usr/bin/env python3
"""
Post Fireflies call summaries to client Slack channels + draft follow-up emails.

Reads meetings from client_interactions (source='fireflies'),
uses Claude to generate summaries, posts to Slack, and creates
a Gmail draft in info@ ready to edit and send to the client.

Usage:
  python3 post_call_summaries.py                # Post all unposted summaries
  python3 post_call_summaries.py --backfill     # Re-post all (marks as posted)
  python3 post_call_summaries.py --dry-run      # Preview without posting
  python3 post_call_summaries.py --no-email     # Slack only, skip email drafts
"""

import argparse
import json
import logging
import os
import re
import sys
import time
import urllib.request
import urllib.error

import psycopg2
import psycopg2.extras

DB_CONFIG = {
    "dbname": "blueprint",
    "user": "blueprint",
    "host": "localhost",
    "port": 5432,
}

SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger('call-summaries')


def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def generate_slack_summary(meeting):
    """Use Claude to generate a scannable Slack call summary."""
    if not ANTHROPIC_API_KEY:
        log.warning("No ANTHROPIC_API_KEY — using raw summary")
        return format_fallback(meeting)

    # Build context for Claude
    client_name = meeting["client_name"]
    parts = client_name.split(" | ")
    biz_name = parts[-1].strip() if len(parts) > 1 else client_name

    # Parse attendees
    attendees = meeting.get("attendees") or []
    attendee_names = []
    for a in attendees:
        # Extract name from email
        if "@blueprintforscale.com" in str(a):
            name = str(a).split("@")[0].title()
            attendee_names.append(name)
        else:
            attendee_names.append(str(a).split("@")[0].title())

    date_str = meeting["interaction_date"].strftime("%b %d, %Y") if meeting.get("interaction_date") else ""

    prompt = f"""You are formatting a call summary for a Slack message. The call was with a Google Ads client named {biz_name}.

Here is the raw summary from the call:
{meeting.get('summary', 'No summary available')}

Here are the action items:
{meeting.get('action_items', 'No action items')}

The sentiment was marked as: {meeting.get('sentiment', 'unknown')}

Format this into a scannable Slack message with these exact sections. Use Slack markdown (bold with *, not **). Keep it concise — each bullet should be one line. Focus on what matters to the team:

1. A sentiment emoji and one-line sentiment description
2. "What's on their mind" — the client's concerns, questions, or requests (2-4 bullets)
3. "Key takeaways" — major decisions or discussion points (2-4 bullets)
4. "Personal notes" — any biographical or personal details mentioned (family, travel, background). If none, skip this section.
5. "Our action items" — what Blueprint team needs to do (from the action items above)
6. "Client action items" — what the client needs to do (from the action items above)

Rules:
- Don't include stats/metrics unless they were a major talking point
- Use plain language, not marketing speak
- Keep the whole message under 15 lines
- Use Slack emoji where appropriate
- NEVER use em dashes (—). Use commas or periods instead.
- NEVER use "thrilled", "excited", "fantastic", "wonderful", "incredibly", "invaluable", "positioned perfectly"
- NEVER use "dive into", "leverage", "streamline", "spearhead"
- Write like a human taking notes, not an AI summarizing
- Return ONLY the formatted message, no preamble"""

    data = json.dumps({
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 1000,
        "messages": [{"role": "user", "content": prompt}]
    }).encode()

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=data,
        method="POST"
    )
    req.add_header("Content-Type", "application/json")
    req.add_header("x-api-key", ANTHROPIC_API_KEY)
    req.add_header("anthropic-version", "2023-06-01")

    try:
        resp = urllib.request.urlopen(req, timeout=30)
        result = json.loads(resp.read())
        return result["content"][0]["text"]
    except Exception as e:
        log.error(f"Claude API error: {e}")
        return format_fallback(meeting)


def format_fallback(meeting):
    """Simple fallback format if Claude is unavailable."""
    sentiment_emoji = {"positive": "😊", "neutral": "😐", "at_risk": "⚠️", "negative": "😟"}.get(
        meeting.get("sentiment", ""), "📞"
    )
    lines = [f"{sentiment_emoji} *Sentiment:* {meeting.get('sentiment', 'unknown').replace('_', ' ').title()}"]
    if meeting.get("summary"):
        lines.append("")
        lines.append("*Summary:*")
        lines.append(meeting["summary"][:500])
    if meeting.get("action_items"):
        lines.append("")
        lines.append("*Action Items:*")
        lines.append(meeting["action_items"][:500])
    return "\n".join(lines)


def post_to_slack(channel_id, header, body):
    """Post a formatted message to a Slack channel."""
    if not SLACK_BOT_TOKEN:
        log.error("No SLACK_BOT_TOKEN")
        return False

    message = {
        "channel": channel_id,
        "text": header,
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": header, "emoji": True}
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": body}
            }
        ]
    }

    data = json.dumps(message).encode()
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=data,
        method="POST"
    )
    req.add_header("Content-Type", "application/json; charset=utf-8")
    req.add_header("Authorization", f"Bearer {SLACK_BOT_TOKEN}")

    try:
        resp = urllib.request.urlopen(req, timeout=15)
        result = json.loads(resp.read())
        if result.get("ok"):
            return True
        else:
            log.error(f"Slack error: {result.get('error')}")
            return False
    except Exception as e:
        log.error(f"Slack post error: {e}")
        return False


# ── Gmail Draft ─────────────────────────────────────────────

GMAIL_TOKENS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gmail_tokens")
GMAIL_CREDENTIALS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gmail_credentials.json")
GMAIL_SENDER = "info@blueprintforscale.com"
BLUEPRINT_DOMAINS = {"blueprintforscale.com", "mypurecompanies.com"}


def get_gmail_service():
    """Build Gmail API service."""
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build

        token_file = os.path.join(GMAIL_TOKENS_DIR, f"{GMAIL_SENDER}.json")
        if not os.path.exists(token_file):
            log.warning(f"No Gmail token for {GMAIL_SENDER} — run gmail_draft_auth.py first")
            return None

        with open(token_file) as f:
            token_data = json.load(f)

        scopes = token_data.get("scopes", [])
        if "https://www.googleapis.com/auth/gmail.compose" not in scopes:
            log.warning("Gmail token missing compose scope — run gmail_draft_auth.py to re-authorize")
            return None

        creds = Credentials.from_authorized_user_info(token_data)
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(token_file, "w") as f:
                f.write(creds.to_json())

        return build("gmail", "v1", credentials=creds)
    except Exception as e:
        log.error(f"Gmail service error: {e}")
        return None


def get_client_emails(meeting, conn=None):
    """Extract non-Blueprint attendee emails + fallback to owner_email from DB."""
    attendees = meeting.get("attendees") or []
    client_emails = set()

    for a in attendees:
        a_str = str(a).strip().lower()
        if "@" in a_str:
            domain = a_str.split("@")[1]
            if domain not in BLUEPRINT_DOMAINS:
                client_emails.add(a_str)

    # Fallback: if no client emails found in attendees, use owner_email from clients table
    if not client_emails and meeting.get("customer_id") and conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT owner_email FROM clients WHERE customer_id = %s", (meeting["customer_id"],))
                row = cur.fetchone()
                if row and row[0]:
                    client_emails.add(row[0].strip().lower())
        except Exception:
            pass

    return list(client_emails)


def generate_email_body(meeting):
    """Use Claude to generate a client-friendly email recap."""
    if not ANTHROPIC_API_KEY:
        return format_email_fallback(meeting)

    client_name = meeting["client_name"]
    parts = client_name.split(" | ")
    biz_name = parts[-1].strip() if len(parts) > 1 else client_name
    owner_name = parts[0].strip() if len(parts) > 1 else ""

    # Use owner name from client record for greeting (more reliable than email parsing)
    if owner_name:
        # Handle multi-person names: "Jim & Karen Blagg" -> "Jim & Karen"
        # "Ethan, Tapan, Nate" -> "Ethan, Tapan & Nate"
        greeting_name = owner_name.split("|")[0].strip() if "|" in owner_name else owner_name
        # Remove last names if it's "First Last" format
        name_parts = greeting_name.replace("&", ",").split(",")
        first_names = [p.strip().split()[0] for p in name_parts if p.strip()]
        if len(first_names) > 1:
            greeting_name = ", ".join(first_names[:-1]) + " & " + first_names[-1]
        else:
            greeting_name = first_names[0] if first_names else "there"
    else:
        greeting_name = "there"

    prompt = f"""Write a short, warm follow-up email after a client call. The client is {biz_name} ({owner_name}).

Here is the call summary:
{meeting.get('summary', '')}

Here are the action items:
{meeting.get('action_items', '')}

Write the email body only (no subject line) in HTML format. Guidelines:
- Start with "Hi {greeting_name},"
- Keep it warm and professional but casual. Write like a real person texting a colleague, not a corporate template.
- 2-3 sentences of opening. Reference something specific and personal from the call. Be genuine.
- A "<b>Key takeaways</b>" section with 3-4 short bullets
- An "<b>Our next steps</b>" section with team member names
- A "<b>Your next steps</b>" section
- Close with a short, natural sign-off and "Best,<br>Susie"
- Keep the whole email under 200 words
- Use HTML tags: <b> for bold headers, <br> for line breaks, <ul><li> for bullets

CRITICAL STYLE RULES — the email must sound human, not AI-generated:
- NEVER use em dashes (—). Use commas or periods instead.
- NEVER use "I'm thrilled", "I'm excited", "fantastic", "wonderful", "incredibly", "invaluable"
- NEVER use "don't hesitate to reach out" or "please don't hesitate"
- NEVER use "dive into", "deep dive", "leverage", "streamline", "spearhead"
- NEVER use "positioned perfectly" or "well-positioned"
- Use short, punchy sentences. Mix in sentence fragments. Like this.
- Use contractions naturally (we'll, you're, that's)
- Use casual transitions ("Also", "Oh and", "One more thing")
- Sound like you actually remember the conversation, not like you're summarizing a doc
- It's ok to be brief. Shorter is better.

Return ONLY the HTML email body, no preamble."""

    data = json.dumps({
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 800,
        "messages": [{"role": "user", "content": prompt}]
    }).encode()

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=data,
        method="POST"
    )
    req.add_header("Content-Type", "application/json")
    req.add_header("x-api-key", ANTHROPIC_API_KEY)
    req.add_header("anthropic-version", "2023-06-01")

    try:
        resp = urllib.request.urlopen(req, timeout=30)
        result = json.loads(resp.read())
        return result["content"][0]["text"]
    except Exception as e:
        log.error(f"Claude API error (email): {e}")
        return format_email_fallback(meeting)


def format_email_fallback(meeting):
    """Simple fallback if Claude is unavailable."""
    return f"""Hi,

Thanks for taking the time to chat today! Here's a quick recap:

{meeting.get('summary', 'No summary available.')[:500]}

Action items:
{meeting.get('action_items', 'None noted.')[:500]}

Let me know if I missed anything.

Best,
Susie"""


def create_gmail_draft(gmail_service, to_emails, subject, body):
    """Create a draft email in Gmail (HTML format)."""
    import base64
    from email.mime.text import MIMEText

    msg = MIMEText(body, "html")
    msg["to"] = ", ".join(to_emails)
    msg["from"] = GMAIL_SENDER
    msg["subject"] = subject

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

    try:
        draft = gmail_service.users().drafts().create(
            userId="me",
            body={"message": {"raw": raw}}
        ).execute()
        return draft.get("id")
    except Exception as e:
        log.error(f"Gmail draft error: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Post call summaries to Slack + draft emails")
    parser.add_argument("--backfill", action="store_true", help="Re-post all meetings")
    parser.add_argument("--dry-run", action="store_true", help="Preview without posting")
    parser.add_argument("--no-email", action="store_true", help="Skip email drafts")
    parser.add_argument("--limit", type=int, default=10, help="Max meetings to process")
    parser.add_argument("--days", type=int, default=0, help="Only process meetings from last N days")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info(f"Call Summary Bot — {'DRY RUN' if args.dry_run else 'LIVE'}")
    log.info("=" * 60)

    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Add columns if they don't exist
    cur.execute("""
        DO $$ BEGIN
            ALTER TABLE client_interactions ADD COLUMN slack_posted_at TIMESTAMPTZ;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
    """)
    cur.execute("""
        DO $$ BEGIN
            ALTER TABLE client_interactions ADD COLUMN slack_summary TEXT;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
    """)
    cur.execute("""
        DO $$ BEGIN
            ALTER TABLE client_interactions ADD COLUMN email_draft TEXT;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
    """)
    conn.commit()

    # Build date filter
    date_filter = ""
    query_params = []
    if args.days > 0:
        date_filter = "AND ci.interaction_date >= NOW() - INTERVAL '%s days'"
        query_params.append(args.days)

    # Find meetings to post
    if args.backfill:
        cur.execute(f"""
            SELECT ci.*, c.name AS client_name, c.slack_channel_id
            FROM client_interactions ci
            JOIN clients c ON c.customer_id = ci.customer_id
            WHERE ci.source = 'fireflies'
              AND c.slack_channel_id IS NOT NULL AND c.slack_channel_id != ''
              AND ci.summary IS NOT NULL AND ci.summary != ''
              {date_filter}
            ORDER BY ci.interaction_date DESC
            LIMIT %s
        """, query_params + [args.limit])
    else:
        cur.execute(f"""
            SELECT ci.*, c.name AS client_name, c.slack_channel_id
            FROM client_interactions ci
            JOIN clients c ON c.customer_id = ci.customer_id
            WHERE ci.source = 'fireflies'
              AND c.slack_channel_id IS NOT NULL AND c.slack_channel_id != ''
              AND ci.summary IS NOT NULL AND ci.summary != ''
              AND ci.slack_posted_at IS NULL
              {date_filter}
            ORDER BY ci.interaction_date DESC
            LIMIT %s
        """, query_params + [args.limit])

    meetings = cur.fetchall()
    log.info(f"Found {len(meetings)} meeting(s) to process")

    gmail_service = None  # Lazy-loaded on first email
    posted = 0
    for meeting in meetings:
        client_name = meeting["client_name"]
        parts = client_name.split(" | ")
        biz_name = parts[-1].strip() if len(parts) > 1 else client_name
        date_str = meeting["interaction_date"].strftime("%b %d") if meeting.get("interaction_date") else ""

        # Parse attendees for header
        attendees = meeting.get("attendees") or []
        team_names = []
        for a in attendees:
            a_str = str(a)
            if "@blueprintforscale.com" in a_str:
                name = a_str.split("@")[0].replace(".", " ").title()
                if name.lower() not in ("info", "jake"):
                    team_names.append(name.split()[0])  # First name only

        # Skip internal meetings (no client attendees)
        attendees_list = meeting.get("attendees") or []
        has_client_attendee = any(
            "@" in str(a) and str(a).split("@")[1].lower() not in BLUEPRINT_DOMAINS
            for a in attendees_list
        )
        if not has_client_attendee:
            log.info(f"\n  Skipping internal meeting: {biz_name} — {date_str} (no client attendees)")
            # Mark as posted so we don't retry
            if not args.dry_run:
                cur.execute("UPDATE client_interactions SET slack_posted_at = NOW() WHERE id = %s", (meeting["id"],))
                conn.commit()
            continue

        header = f"📞 Call with {biz_name} — {date_str}"
        if team_names:
            header += f" · {', '.join(team_names[:3])}"

        log.info(f"\n  {header}")
        log.info(f"  Channel: {meeting['slack_channel_id']}")

        # Generate summary with Claude
        body = generate_slack_summary(dict(meeting))
        log.info(f"  Summary generated ({len(body)} chars)")

        # Email draft
        client_emails = get_client_emails(dict(meeting), conn)
        email_subject = f"Today's Call — {meeting['interaction_date'].strftime('%B %d, %Y')}" if meeting.get("interaction_date") else "Today's Call"

        # Always store the slack summary in DB
        cur.execute("UPDATE client_interactions SET slack_summary = %s WHERE id = %s", (body, meeting["id"]))
        conn.commit()

        if args.dry_run:
            log.info(f"\n--- SLACK PREVIEW ---\n{header}\n\n{body}\n--- END ---\n")
            if not args.no_email and client_emails:
                email_body = generate_email_body(dict(meeting))
                cur.execute("UPDATE client_interactions SET email_draft = %s WHERE id = %s", (email_body, meeting["id"]))
                conn.commit()
                log.info(f"--- EMAIL PREVIEW ---")
                log.info(f"To: {', '.join(client_emails)}")
                log.info(f"Subject: {email_subject}")
                log.info(f"\n{email_body}\n--- END ---\n")
            elif not client_emails:
                log.info("  No client emails found — skipping draft")
        else:
            # Post to Slack
            success = post_to_slack(meeting["slack_channel_id"], header, body)
            if success:
                cur.execute("""
                    UPDATE client_interactions SET slack_posted_at = NOW()
                    WHERE id = %s
                """, (meeting["id"],))
                conn.commit()
                posted += 1
                log.info(f"  ✓ Posted to Slack")
            else:
                log.error(f"  ✗ Failed to post to Slack")

            # Create email draft
            if not args.no_email and client_emails:
                if gmail_service is None:
                    gmail_service = get_gmail_service()
                if gmail_service:
                    email_body = generate_email_body(dict(meeting))
                    cur.execute("UPDATE client_interactions SET email_draft = %s WHERE id = %s", (email_body, meeting["id"]))
                    conn.commit()
                    draft_id = create_gmail_draft(gmail_service, client_emails, email_subject, email_body)
                    if draft_id:
                        log.info(f"  ✓ Email draft created → {', '.join(client_emails)}")
                    else:
                        log.error(f"  ✗ Failed to create email draft")
                else:
                    log.warning("  Gmail not available — skipping draft")
            elif not client_emails:
                log.info("  No client emails — skipping draft")

            time.sleep(1)

    log.info(f"\nDone — {posted} posted to Slack")
    conn.close()


if __name__ == "__main__":
    main()
