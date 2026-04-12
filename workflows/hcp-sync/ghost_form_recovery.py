#!/usr/bin/env python3
"""
Ghost Form Recovery

CallRail's JavaScript snippet captures form submissions via field-change listeners,
which means it sees partial/abandoned fills too. When a user starts filling a form
but never successfully submits (captcha timeout, JS error, navigated away, typo'd
email), CallRail still fires its form_captured_webhook and pushes the partial data
into GHL via the direct GHL webhook — with the wrong tag, so the client's form
automation never fires.

This script scans form_submissions for orphans (CallRail captured, FormBridge did
not), filters out bot/spam rows, and re-posts legitimate orphans to FormBridge's
per-client webhook URL. FormBridge then upserts the GHL contact (finding the
existing one by phone/email) and fires its normal remove/re-add of the "websiteform"
tag — which triggers the client's GHL form workflow just like a real submission.

Idempotent: every processed row is logged in ghost_form_recoveries. Subsequent runs
skip rows already in that table.

Usage:
    python3 ghost_form_recovery.py                  # Last 60 min
    python3 ghost_form_recovery.py --since-hours 24 # Last 24 hours
    python3 ghost_form_recovery.py --backfill       # Last 60 days (one-shot)
    python3 ghost_form_recovery.py --dry-run        # Show what would happen, no side effects
"""
import argparse
import json
import logging
import re
import sys
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

import psycopg2
import psycopg2.extras

DSN = "dbname=blueprint user=blueprint"
FORMBRIDGE_BASE = "https://formbridge-production-7f19.up.railway.app"

# Wait this long before considering a CallRail capture an orphan — gives the
# FormBridge → webflow_submissions ETL time to pull the matching real submission.
SETTLE_MINUTES = 30

logging.basicConfig(
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("ghost_recovery")


# ---------------------------------------------------------------------------
# Spam detection — mirrors callrail-client.js detectFormSpam (score >= 3 = spam)
# ---------------------------------------------------------------------------
_NAME_KEYS_FIRST = ["first_name", "firstname", "fname", "First name", "First name *"]
_NAME_KEYS_LAST = ["last_name", "lastname", "lname", "Last name", "Last name *"]
_NAME_KEYS_FULL = ["name", "full_name", "fullname", "your_name"]
_EMAIL_KEYS = ["email", "e-mail", "Email", "Email *", "email_address"]
_ZIP_KEYS = ["zip", "zipcode", "zip_code", "postal", "Zip", "Zip *"]
_MSG_KEYS = ["message", "comments", "additional_information", "Additional information", "Additional Information"]
_PHONE_KEYS = ["phone", "phone_number", "Phone", "Phone *", "Phone number", "Phone Number"]


def _find(form_data, keys):
    if not isinstance(form_data, dict):
        return None
    for k in keys:
        if k in form_data and form_data[k] not in (None, ""):
            return form_data[k]
    lower = {k.lower(): k for k in form_data}
    for k in keys:
        lk = lower.get(k.lower())
        if lk and form_data[lk] not in (None, ""):
            return form_data[lk]
    return None


def is_gibberish(s):
    if not s or not isinstance(s, str) or len(s) < 3:
        return False
    s = s.strip()
    if len(s) > 25 and " " not in s:
        return True
    vowels = len(re.findall(r"[aeiou]", s, re.IGNORECASE))
    if len(s) > 5 and vowels / len(s) < 0.15:
        return True
    case_changes = len(re.findall(r"[a-z][A-Z]|[A-Z][a-z]", s))
    if case_changes > 4 and len(s) > 10:
        return True
    if re.search(r"[bcdfghjklmnpqrstvwxyz]{5,}", s, re.IGNORECASE):
        return True
    return False


def is_dotted_gmail(email):
    if not email or not isinstance(email, str):
        return False
    lower = email.lower().strip()
    if not lower.endswith("@gmail.com"):
        return False
    local = lower.split("@")[0]
    dots = local.count(".")
    segments = local.split(".")
    if dots >= 4 and any(len(s) <= 2 for s in segments):
        return True
    return False


def is_gibberish_zip(zip_val):
    if not zip_val:
        return False
    return bool(re.search(r"[a-zA-Z]{3,}", str(zip_val)))


def detect_form_spam(form_data):
    """Returns (is_spam: bool, score: int, signals: list[str])."""
    signals = []
    score = 0
    if not isinstance(form_data, dict):
        return (False, 0, [])

    first = _find(form_data, _NAME_KEYS_FIRST)
    last = _find(form_data, _NAME_KEYS_LAST)
    full = _find(form_data, _NAME_KEYS_FULL)
    if is_gibberish(first):
        signals.append(f"gibberish_first_name:{first!r}")
        score += 2
    if is_gibberish(last):
        signals.append(f"gibberish_last_name:{last!r}")
        score += 2
    if is_gibberish(full):
        signals.append(f"gibberish_full_name:{full!r}")
        score += 2

    email = _find(form_data, _EMAIL_KEYS)
    if is_dotted_gmail(email):
        signals.append(f"dotted_gmail:{email!r}")
        score += 2

    zip_val = _find(form_data, _ZIP_KEYS)
    if is_gibberish_zip(zip_val):
        signals.append(f"gibberish_zip:{zip_val!r}")
        score += 1

    for field in ("utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content"):
        v = form_data.get(field)
        if is_gibberish(v):
            signals.append(f"gibberish_{field}")
            score += 1
            break

    msg = _find(form_data, _MSG_KEYS)
    if is_gibberish(msg):
        signals.append("gibberish_message")
        score += 1

    return (score >= 3, score, signals)


# ---------------------------------------------------------------------------
# Payload builder — turn CallRail form_data into a FormBridge-shaped POST body
# ---------------------------------------------------------------------------
def build_fb_payload(fs_row):
    fd = fs_row["form_data"] or {}
    first = _find(fd, _NAME_KEYS_FIRST) or ""
    last = _find(fd, _NAME_KEYS_LAST) or ""
    full = _find(fd, _NAME_KEYS_FULL) or ""
    if (not first) and full:
        parts = full.strip().split(None, 1)
        first = parts[0] if parts else ""
        last = parts[1] if len(parts) > 1 else ""

    phone = _find(fd, _PHONE_KEYS) or fs_row.get("customer_phone") or ""
    email = _find(fd, _EMAIL_KEYS) or fs_row.get("customer_email") or ""
    zip_val = _find(fd, _ZIP_KEYS) or ""
    msg = _find(fd, _MSG_KEYS) or ""

    payload = {
        "first_name": first,
        "last_name": last,
        "phone": str(phone),
        "email": email,
        "zip": str(zip_val),
        "source": "ghost-recovery",
        "_ghost_recovery": True,
        "_original_callrail_form_id": fs_row.get("callrail_id"),
    }
    if msg:
        payload["additional_information"] = msg
    for k in ("gclid", "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content"):
        v = fd.get(k)
        if v:
            payload[k] = v
    form_url = fs_row.get("form_url")
    if form_url:
        # Derive a page hint from the URL so the ETL's form_name fallback works
        m = re.search(r"/([^/?#]+)(?:[?#]|$)", form_url)
        if m:
            payload["page"] = m.group(1)
    return payload


# ---------------------------------------------------------------------------
# Find orphans
# ---------------------------------------------------------------------------
FIND_ORPHANS_SQL = """
SELECT
    fs.id                      AS form_submission_id,
    fs.callrail_id,
    fs.customer_id,
    fs.customer_phone,
    fs.customer_email,
    fs.customer_name,
    fs.source,
    fs.form_url,
    fs.submitted_at,
    fs.form_data,
    c.slug                     AS formbridge_slug,
    c.name                     AS client_name
FROM form_submissions fs
JOIN clients c USING (customer_id)
WHERE c.ghost_recovery_mode = 'on'
  AND (fs.submitted_at >= NOW() - (%(since_minutes)s || ' minutes')::interval OR %(only_id)s IS NOT NULL)
  AND fs.submitted_at <  NOW() - (%(settle_minutes)s || ' minutes')::interval
  AND fs.customer_phone IS NOT NULL
  AND normalize_phone(fs.customer_phone) ~ '^[2-9][0-9]{9}$'
  AND NOT EXISTS (
    SELECT 1 FROM webflow_submissions ws
    WHERE ws.client_slug = c.slug
      AND ws.phone_normalized = normalize_phone(fs.customer_phone)
      AND ws.submitted_at BETWEEN fs.submitted_at - INTERVAL '30 minutes'
                              AND fs.submitted_at + INTERVAL '30 minutes'
  )
  AND NOT EXISTS (
    SELECT 1 FROM ghost_form_recoveries gfr
    WHERE gfr.form_submission_id = fs.id
  )
  AND (%(only_id)s IS NULL OR fs.id = %(only_id)s)
ORDER BY fs.submitted_at DESC
"""


# ---------------------------------------------------------------------------
# POST to FormBridge
# ---------------------------------------------------------------------------
def post_to_formbridge(slug, payload, dry_run=False):
    url = f"{FORMBRIDGE_BASE}/webhook/{slug}"
    body = json.dumps(payload).encode()
    if dry_run:
        return (0, "(dry-run, not sent)")
    req = Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(req, timeout=20) as r:
            return (r.getcode(), r.read().decode()[:500])
    except HTTPError as e:
        return (e.code, e.read().decode()[:500] if e.fp else str(e))
    except URLError as e:
        return (0, f"URLError: {e.reason}")


# ---------------------------------------------------------------------------
# Log to ghost_form_recoveries
# ---------------------------------------------------------------------------
def log_result(conn, row, action, spam_score=None, spam_signals=None, http_status=None, response=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ghost_form_recoveries
              (form_submission_id, callrail_form_id, customer_id, formbridge_slug,
               action, spam_score, spam_signals, fb_http_status, fb_response)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (form_submission_id) DO NOTHING
            """,
            [
                row["form_submission_id"],
                row["callrail_id"],
                row["customer_id"],
                row["formbridge_slug"],
                action,
                spam_score,
                spam_signals,
                http_status,
                response,
            ],
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since-minutes", type=int, default=60, help="Window of form_submissions to scan (default 60)")
    ap.add_argument("--since-hours", type=int, help="Shortcut: --since-minutes = N*60")
    ap.add_argument("--backfill", action="store_true", help="Scan last 60 days (one-shot cleanup)")
    ap.add_argument("--dry-run", action="store_true", help="Don't POST, don't log")
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--only-form-id", type=int, help="Process a single form_submissions.id (testing)")
    args = ap.parse_args()

    if args.backfill:
        since_minutes = 60 * 24 * 60  # 60 days
    elif args.since_hours:
        since_minutes = args.since_hours * 60
    else:
        since_minutes = args.since_minutes

    log.info(f"Scanning last {since_minutes} min (settle={SETTLE_MINUTES} min)")

    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(FIND_ORPHANS_SQL, {"since_minutes": since_minutes, "settle_minutes": SETTLE_MINUTES, "only_id": args.only_form_id})
        orphans = cur.fetchall()

    log.info(f"Found {len(orphans)} orphan form submissions")

    stats = {"recovered": 0, "spam": 0, "missing_config": 0, "error": 0}
    for i, row in enumerate(orphans):
        if i >= args.limit:
            log.info(f"Stopping at --limit {args.limit}")
            break

        name = row.get("customer_name") or "(no name)"
        phone = row.get("customer_phone") or ""
        slug = row.get("formbridge_slug")
        client = row.get("client_name") or "?"

        if not slug:
            log.info(f"  [MISSING_CONFIG] {client} | {name} {phone} — no clients.slug set")
            if not args.dry_run:
                log_result(conn, row, "missing_config")
            stats["missing_config"] += 1
            continue

        # Spam check
        is_spam, score, signals = detect_form_spam(row["form_data"])
        if is_spam:
            log.info(f"  [SPAM score={score}] {client} | {name} — signals={signals}")
            if not args.dry_run:
                log_result(conn, row, "spam", spam_score=score, spam_signals=signals)
            stats["spam"] += 1
            continue

        # Recover
        payload = build_fb_payload(row)
        http_status, resp = post_to_formbridge(slug, payload, dry_run=args.dry_run)

        if args.dry_run:
            log.info(f"  [DRY-RUN] {client} | {name} {phone} → would POST to /webhook/{slug}")
        elif 200 <= http_status < 300:
            log.info(f"  [RECOVERED HTTP {http_status}] {client} | {name} {phone} → /webhook/{slug}")
            log_result(conn, row, "recovered", http_status=http_status, response=resp)
            stats["recovered"] += 1
        else:
            log.warning(f"  [ERROR HTTP {http_status}] {client} | {name} {phone} → {resp[:200]}")
            log_result(conn, row, "error", http_status=http_status, response=resp)
            stats["error"] += 1

    if not args.dry_run:
        conn.commit()
    conn.close()

    log.info(f"Done. {stats}")


if __name__ == "__main__":
    main()
