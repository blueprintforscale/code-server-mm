#!/usr/bin/env python3
"""
Lead Qualifier Pipeline
Fetches ALL calls from CallRail (every source) and Google Ads form submissions.
Classifies only Google Ads calls/forms, and uploads legitimate leads to Google Ads
as Enhanced Conversions (via GCLID, hashed phone number, or hashed email).

Classification is done by Claude Code itself (via system cron), not via API calls.
This keeps costs at $0 by leveraging the existing Anthropic subscription.

Subcommands — Calls:
  fetch              — Pull all calls from CallRail, store in DB
  pending            — Print unclassified Google Ads calls as JSON
  classify-batch     — Store multiple call classifications from JSON

Subcommands — Forms:
  fetch-forms        — Pull Google Ads form submissions from CallRail, store in DB
  pending-forms      — Print unclassified forms as JSON
  classify-forms     — Store multiple form classifications from JSON

Shared:
  upload             — Upload all legitimate leads (calls + forms) to Google Ads
  classify           — Store a single classification (calls only)
  log-run            — Start/finish a pipeline run log entry
  summary            — Print classification stats (calls + forms)

Schedule: Every 30 minutes via system cron (launchd).
"""
import hashlib
import sys
import json
import logging
import argparse
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

# Load .env from project root
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

import os

# Config
DB_DSN = "host=localhost port=5432 dbname=blueprint user=blueprint password=Blueprint2025!"
CALLRAIL_API_KEY = os.environ.get("CALLRAIL_API_KEY", "")
CALLRAIL_ACCOUNT_ID = "465371377"
GOOGLE_ADS_YAML = Path("/Users/bp/projects/.mcp-servers/google_ads_mcp/google-ads.yaml")
MCC_ID = "2985235474"
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "classify_calls.log"),
    ],
)
log = logging.getLogger(__name__)


def get_db():
    return psycopg2.connect(DB_DSN)


def get_callrail_headers():
    return {
        "Authorization": f"Token token={CALLRAIL_API_KEY}",
        "Content-Type": "application/json",
    }


def get_active_clients(db, client_filter=None):
    """Get clients with CallRail company IDs configured."""
    cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if client_filter:
        cur.execute("""
            SELECT customer_id, name, callrail_company_id, conversion_value
            FROM clients
            WHERE callrail_company_id IS NOT NULL AND status = 'active'
              AND customer_id = %s
        """, (int(client_filter),))
    else:
        cur.execute("""
            SELECT customer_id, name, callrail_company_id, conversion_value
            FROM clients
            WHERE callrail_company_id IS NOT NULL AND status = 'active'
        """)
    return cur.fetchall()


def normalize_phone(phone):
    """Normalize phone to E.164 format (+1XXXXXXXXXX) for hashing."""
    if not phone:
        return None
    digits = re.sub(r'\D', '', phone)
    if len(digits) == 10:
        digits = '1' + digits
    if len(digits) == 11 and digits.startswith('1'):
        return '+' + digits
    return None


def hash_phone(phone):
    """SHA-256 hash a normalized phone number for Enhanced Conversions."""
    normalized = normalize_phone(phone)
    if not normalized:
        return None
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()


def hash_email(email):
    """SHA-256 hash a normalized email for Enhanced Conversions."""
    if not email:
        return None
    normalized = email.strip().lower()
    if '@' not in normalized:
        return None
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()


def format_conversion_time(ts):
    """Format a timestamp for Google Ads.

    Google Ads requires: "yyyy-mm-dd hh:mm:ss+|-hh:mm"
    """
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    dt_str = ts.strftime("%Y-%m-%d %H:%M:%S%z")
    # Python strftime gives +0000, Google Ads wants +00:00
    if len(dt_str) >= 5 and dt_str[-3] != ":":
        dt_str = dt_str[:-2] + ":" + dt_str[-2:]
    return dt_str


# ===========================================================================
# CallRail API — Calls
# ===========================================================================
def fetch_calls_for_company(company_id, since):
    """Fetch all calls from CallRail (all sources, all statuses)."""
    calls = []
    page = 1
    while True:
        url = f"https://api.callrail.com/v3/a/{CALLRAIL_ACCOUNT_ID}/calls.json"
        params = {
            "company_id": company_id,
            "start_date": since.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "fields": "gclid,transcription,milestones,source,medium,customer_phone_number,first_call,call_type,answered",
            "per_page": 250,
            "page": page,
        }
        resp = requests.get(url, headers=get_callrail_headers(), params=params)
        resp.raise_for_status()
        data = resp.json()

        calls.extend(data.get("calls", []))

        if page >= data.get("total_pages", 1):
            break
        page += 1

    return calls


def store_calls(db, calls, company_id, customer_id):
    """Insert new calls into the database, skipping duplicates."""
    cur = db.cursor()
    inserted = 0
    for call in calls:
        callrail_id = call.get("id")
        if not callrail_id:
            continue

        transcript_text = None
        transcription = call.get("transcription")
        if transcription:
            if isinstance(transcription, dict):
                transcript_text = transcription.get("content", "")
            else:
                transcript_text = str(transcription)

        # Derive callrail_status from answered flag and call_type
        answered = call.get("answered")
        call_type = call.get("call_type")
        if call_type == "abandoned":
            callrail_status = "abandoned"
        elif answered:
            callrail_status = "answered"
        else:
            callrail_status = "missed"

        cur.execute("""
            INSERT INTO calls (callrail_id, callrail_company_id, customer_id,
                             caller_phone, gclid, start_time, duration, transcript,
                             source, medium, first_call, callrail_status, call_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (callrail_id) DO NOTHING
        """, (
            callrail_id, company_id, customer_id,
            call.get("customer_phone_number") or call.get("caller_number"),
            call.get("gclid"), call.get("start_time"), call.get("duration"),
            transcript_text, call.get("source"), call.get("medium"),
            call.get("first_call"), callrail_status, call_type,
        ))
        if cur.rowcount > 0:
            inserted += 1

    db.commit()
    return inserted


# ===========================================================================
# CallRail API — Forms
# ===========================================================================
def fetch_forms_for_company(company_id, since):
    """Fetch Google Ads form submissions from CallRail."""
    forms = []
    page = 1
    while True:
        url = f"https://api.callrail.com/v3/a/{CALLRAIL_ACCOUNT_ID}/form_submissions.json"
        params = {
            "company_id": company_id,
            "start_date": since.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "per_page": 250,
            "page": page,
        }
        resp = requests.get(url, headers=get_callrail_headers(), params=params)
        resp.raise_for_status()
        data = resp.json()

        for form in data.get("form_submissions", []):
            source = (form.get("source") or "").lower()
            medium = (form.get("medium") or "").lower()
            if "google" in source or medium in ("cpc", "CPC"):
                forms.append(form)

        if page >= data.get("total_pages", 1):
            break
        page += 1

    return forms


def store_forms(db, forms, company_id, customer_id):
    """Insert new form submissions into the database, skipping duplicates."""
    cur = db.cursor()
    inserted = 0
    for form in forms:
        callrail_id = form.get("id")
        if not callrail_id:
            continue

        form_data = form.get("form_data", {})
        gclid = form_data.get("gclid")

        cur.execute("""
            INSERT INTO form_submissions (callrail_id, callrail_company_id, customer_id,
                customer_name, customer_email, customer_phone, gclid,
                form_data, form_url, submitted_at, source, medium, campaign)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (callrail_id) DO NOTHING
        """, (
            callrail_id, company_id, customer_id,
            form.get("customer_name"),
            form.get("customer_email"),
            form.get("customer_phone_number"),
            gclid,
            json.dumps(form_data),
            form.get("form_url"),
            form.get("submitted_at"),
            form.get("source"),
            form.get("medium"),
            form.get("campaign"),
        ))
        if cur.rowcount > 0:
            inserted += 1

    db.commit()
    return inserted


# ===========================================================================
# Shared: Google Ads upload
# ===========================================================================
def build_click_conversion(gads_client, conversion_action, gclid, phone, email, ts, value):
    """Build a ClickConversion proto with GCLID or hashed user identifiers."""
    click_conversion = gads_client.get_type("ClickConversion")
    click_conversion.conversion_action = conversion_action
    click_conversion.conversion_value = float(value or 1.00)
    click_conversion.currency_code = "USD"
    click_conversion.conversion_date_time = format_conversion_time(ts)

    if gclid:
        click_conversion.gclid = gclid
    else:
        # Use hashed user identifiers (phone and/or email)
        added = False
        hashed_phone = hash_phone(phone)
        if hashed_phone:
            uid = gads_client.get_type("UserIdentifier")
            uid.hashed_phone_number = hashed_phone
            uid.user_identifier_source = gads_client.enums.UserIdentifierSourceEnum.FIRST_PARTY
            click_conversion.user_identifiers.append(uid)
            added = True
        hashed_email = hash_email(email)
        if hashed_email:
            uid = gads_client.get_type("UserIdentifier")
            uid.hashed_email = hashed_email
            uid.user_identifier_source = gads_client.enums.UserIdentifierSourceEnum.FIRST_PARTY
            click_conversion.user_identifiers.append(uid)
            added = True
        if not added:
            return None

    return click_conversion


def upload_conversions_batch(gads_client, customer_id, items, update_cur, db):
    """Upload a batch of conversions for one customer. Returns (uploaded, skipped, errors)."""
    service = gads_client.get_service("GoogleAdsService")
    query = """
        SELECT conversion_action.resource_name, conversion_action.name
        FROM conversion_action
        WHERE conversion_action.name = 'Qualified Lead [AI]'
          AND conversion_action.status = 'ENABLED'
    """
    conversion_action = None
    try:
        results = service.search(customer_id=str(customer_id), query=query)
        for row in results:
            conversion_action = row.conversion_action.resource_name
            break
    except Exception as e:
        log.warning(f"  Could not find conversion action for {customer_id}: {e}")

    if not conversion_action:
        err = f"No 'Qualified Lead [AI]' conversion action for {customer_id}"
        log.warning(f"  {err}, skipping {len(items)} items")
        for item in items:
            update_cur.execute(f"""
                UPDATE {item['_table']} SET upload_error = %s, updated_at = NOW()
                WHERE id = %s
            """, ("No conversion action found", item["id"]))
        db.commit()
        return 0, 0, [err]

    click_conversions = []
    id_map = {}
    skipped = 0

    for item in items:
        conv = build_click_conversion(
            gads_client, conversion_action,
            item.get("gclid"), item.get("phone"), item.get("email"),
            item["ts"], item["value"],
        )
        if conv is None:
            log.info(f"  {item['_table']} {item['id']}: no GCLID/phone/email, skipping")
            update_cur.execute(f"""
                UPDATE {item['_table']} SET upload_error = 'No match key (GCLID/phone/email)', updated_at = NOW()
                WHERE id = %s
            """, (item["id"],))
            skipped += 1
            continue

        idx = len(click_conversions)
        click_conversions.append(conv)
        id_map[idx] = item

    if not click_conversions:
        db.commit()
        return 0, skipped, []

    uploaded = 0
    errors = []

    try:
        conversion_upload_service = gads_client.get_service("ConversionUploadService")
        request = gads_client.get_type("UploadClickConversionsRequest")
        request.customer_id = str(customer_id)
        request.conversions = click_conversions
        request.partial_failure = True

        response = conversion_upload_service.upload_click_conversions(request=request)

        if response.partial_failure_error:
            log.warning(f"  Partial failures for {customer_id}: {response.partial_failure_error.message}")

        for i, result in enumerate(response.results):
            item = id_map.get(i)
            if not item:
                continue
            if result.gclid or result.user_identifiers:
                update_cur.execute(f"""
                    UPDATE {item['_table']} SET
                        uploaded_to_gads = TRUE,
                        conversion_value = %s,
                        updated_at = NOW()
                    WHERE id = %s
                """, (float(click_conversions[i].conversion_value), item["id"]))
                uploaded += 1
            else:
                update_cur.execute(f"""
                    UPDATE {item['_table']} SET
                        upload_error = 'Partial failure (see logs)',
                        updated_at = NOW()
                    WHERE id = %s
                """, (item["id"],))

        db.commit()

    except Exception as e:
        err = f"Upload failed for {customer_id}: {str(e)[:200]}"
        errors.append(err)
        log.error(f"  {err}")
        for item in items:
            update_cur.execute(f"""
                UPDATE {item['_table']} SET upload_error = %s, updated_at = NOW()
                WHERE id = %s
            """, (str(e)[:500], item["id"]))
        db.commit()

    return uploaded, skipped, errors


# ===========================================================================
# Subcommand: fetch (calls)
# ===========================================================================
def cmd_fetch(args):
    """Fetch Google Ads calls from CallRail and store in DB."""
    if not CALLRAIL_API_KEY or CALLRAIL_API_KEY == "REPLACE_ME":
        log.error("CALLRAIL_API_KEY not set in .env")
        sys.exit(1)

    db = get_db()
    cur = db.cursor()
    clients = get_active_clients(db, args.client)

    if not clients:
        log.error("No matching clients found")
        sys.exit(1)

    if args.backfill_hours:
        since = datetime.now(timezone.utc) - timedelta(hours=args.backfill_hours)
    else:
        cur.execute("""
            SELECT MAX(started_at) FROM call_pipeline_log
            WHERE status IN ('completed', 'completed_with_errors')
        """)
        last_run = cur.fetchone()[0]
        if last_run:
            since = last_run - timedelta(minutes=5)
        else:
            since = datetime.now(timezone.utc) - timedelta(hours=24)

    log.info(f"Fetching all calls since {since.isoformat()} for {len(clients)} clients")
    total = 0
    errors = []

    for client in clients:
        company_id = client["callrail_company_id"]
        customer_id = client["customer_id"]
        log.info(f"  [{customer_id}] {client['name']}")

        try:
            calls = fetch_calls_for_company(company_id, since)
            inserted = store_calls(db, calls, company_id, customer_id)
            log.info(f"    Fetched {len(calls)} calls, stored {inserted} new")
            total += inserted
        except Exception as e:
            err = f"Fetch error {customer_id}: {str(e)[:200]}"
            errors.append(err)
            log.error(f"    ERROR: {e}")

    # Auto-classify obvious spam (short/no transcript) — Google Ads calls only
    update_cur = db.cursor()
    update_cur.execute("""
        UPDATE calls SET
            classification = 'spam',
            classification_reason = 'Very short call with no transcript',
            classification_attempts = 1,
            updated_at = NOW()
        WHERE classification IS NULL
          AND (duration < 15 OR transcript IS NULL OR transcript = '')
          AND (LOWER(source) LIKE '%%google%%' OR LOWER(medium) = 'cpc')
    """)
    auto_spam = update_cur.rowcount
    db.commit()

    if auto_spam > 0:
        log.info(f"  Auto-classified {auto_spam} short/empty calls as spam")

    print(json.dumps({
        "status": "ok" if not errors else "errors",
        "fetched": total,
        "auto_spam": auto_spam,
        "errors": errors,
    }))
    db.close()


# ===========================================================================
# Subcommand: fetch-forms
# ===========================================================================
def cmd_fetch_forms(args):
    """Fetch Google Ads form submissions from CallRail and store in DB."""
    if not CALLRAIL_API_KEY or CALLRAIL_API_KEY == "REPLACE_ME":
        log.error("CALLRAIL_API_KEY not set in .env")
        sys.exit(1)

    db = get_db()
    cur = db.cursor()
    clients = get_active_clients(db, args.client)

    if not clients:
        log.error("No matching clients found")
        sys.exit(1)

    if args.backfill_hours:
        since = datetime.now(timezone.utc) - timedelta(hours=args.backfill_hours)
    else:
        cur.execute("""
            SELECT MAX(started_at) FROM call_pipeline_log
            WHERE status IN ('completed', 'completed_with_errors')
        """)
        last_run = cur.fetchone()[0]
        if last_run:
            since = last_run - timedelta(minutes=5)
        else:
            since = datetime.now(timezone.utc) - timedelta(hours=24)

    log.info(f"Fetching Google Ads forms since {since.isoformat()} for {len(clients)} clients")
    total = 0
    errors = []

    for client in clients:
        company_id = client["callrail_company_id"]
        customer_id = client["customer_id"]
        log.info(f"  [{customer_id}] {client['name']}")

        try:
            forms = fetch_forms_for_company(company_id, since)
            inserted = store_forms(db, forms, company_id, customer_id)
            log.info(f"    Fetched {len(forms)} Google Ads forms, stored {inserted} new")
            total += inserted
        except Exception as e:
            err = f"Fetch forms error {customer_id}: {str(e)[:200]}"
            errors.append(err)
            log.error(f"    ERROR: {e}")

    db.commit()
    print(json.dumps({
        "status": "ok" if not errors else "errors",
        "fetched": total,
        "errors": errors,
    }))
    db.close()


# ===========================================================================
# Subcommand: pending (calls)
# ===========================================================================
def cmd_pending(args):
    """Print unclassified calls as JSON for Claude Code to classify."""
    db = get_db()
    cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT id, caller_phone, duration, transcript, classification_attempts
        FROM calls
        WHERE (classification IS NULL
           OR (classification = 'error' AND classification_attempts < 3))
          AND (LOWER(source) LIKE '%%google%%' OR LOWER(medium) = 'cpc')
        ORDER BY start_time ASC
    """)
    pending = cur.fetchall()
    db.close()

    results = []
    for call in pending:
        results.append({
            "id": call["id"],
            "caller_phone": call["caller_phone"],
            "duration": call["duration"],
            "transcript": (call["transcript"] or "")[:4000],
        })

    print(json.dumps(results, indent=2))


# ===========================================================================
# Subcommand: pending-forms
# ===========================================================================
def cmd_pending_forms(args):
    """Print unclassified form submissions as JSON for Claude Code to classify."""
    db = get_db()
    cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT id, customer_name, customer_email, customer_phone, form_data
        FROM form_submissions
        WHERE classification IS NULL
           OR (classification = 'error' AND classification_attempts < 3)
        ORDER BY submitted_at ASC
    """)
    pending = cur.fetchall()
    db.close()

    results = []
    for form in pending:
        form_data = form["form_data"] if isinstance(form["form_data"], dict) else json.loads(form["form_data"] or "{}")
        # Build a readable summary of form fields for classification
        display_fields = {}
        for key, val in form_data.items():
            # Skip internal/tracking fields
            if any(skip in key.lower() for skip in ['cf-turnstile', 'utm_', 'gclid', 'ft_', 'landing_page', 'referrer', 'visit_count']):
                continue
            display_fields[key] = val

        results.append({
            "id": form["id"],
            "customer_name": form["customer_name"],
            "customer_email": form["customer_email"],
            "form_fields": display_fields,
        })

    print(json.dumps(results, indent=2))


# ===========================================================================
# Subcommand: classify (single call)
# ===========================================================================
def cmd_classify(args):
    """Store a classification result for a call."""
    db = get_db()
    cur = db.cursor()

    cur.execute("""
        UPDATE calls SET
            classification = %s,
            classification_reason = %s,
            classification_attempts = classification_attempts + 1,
            updated_at = NOW()
        WHERE id = %s
    """, (args.classification, args.reason, args.call_id))

    if cur.rowcount == 0:
        print(json.dumps({"status": "error", "message": f"Call {args.call_id} not found"}))
    else:
        print(json.dumps({"status": "ok", "call_id": args.call_id, "classification": args.classification}))

    db.commit()
    db.close()


# ===========================================================================
# Subcommand: classify-batch (calls)
# ===========================================================================
def cmd_classify_batch(args):
    """Store multiple call classifications from JSON."""
    if args.data:
        data = json.loads(args.data)
    else:
        data = json.loads(sys.stdin.read())

    db = get_db()
    cur = db.cursor()
    updated = 0

    for item in data:
        cur.execute("""
            UPDATE calls SET
                classification = %s,
                classification_reason = %s,
                classification_attempts = classification_attempts + 1,
                updated_at = NOW()
            WHERE id = %s
        """, (item["classification"], item.get("reason", ""), item["id"]))
        updated += cur.rowcount

    db.commit()
    db.close()
    print(json.dumps({"status": "ok", "updated": updated}))


# ===========================================================================
# Subcommand: classify-forms (batch)
# ===========================================================================
def cmd_classify_forms(args):
    """Store multiple form classifications from JSON."""
    if args.data:
        data = json.loads(args.data)
    else:
        data = json.loads(sys.stdin.read())

    db = get_db()
    cur = db.cursor()
    updated = 0

    for item in data:
        cur.execute("""
            UPDATE form_submissions SET
                classification = %s,
                classification_reason = %s,
                classification_attempts = classification_attempts + 1,
                updated_at = NOW()
            WHERE id = %s
        """, (item["classification"], item.get("reason", ""), item["id"]))
        updated += cur.rowcount

    db.commit()
    db.close()
    print(json.dumps({"status": "ok", "updated": updated}))


# ===========================================================================
# Subcommand: upload (calls + forms)
# ===========================================================================
def cmd_upload(args):
    """Upload legitimate calls and forms to Google Ads as Enhanced Conversions."""
    if args.dry_run:
        db = get_db()
        cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("""
            SELECT gclid, caller_phone FROM calls
            WHERE classification = 'legitimate' AND uploaded_to_gads = FALSE
              AND (gclid IS NOT NULL OR caller_phone IS NOT NULL)
        """)
        call_rows = cur.fetchall()

        cur.execute("""
            SELECT gclid, customer_phone, customer_email FROM form_submissions
            WHERE classification = 'legitimate' AND uploaded_to_gads = FALSE
              AND (gclid IS NOT NULL OR customer_phone IS NOT NULL OR customer_email IS NOT NULL)
        """)
        form_rows = cur.fetchall()

        calls_gclid = sum(1 for r in call_rows if r["gclid"])
        calls_phone = len(call_rows) - calls_gclid
        forms_gclid = sum(1 for r in form_rows if r["gclid"])
        forms_user = len(form_rows) - forms_gclid

        log.info(f"DRY RUN: Would upload {len(call_rows)} calls ({calls_gclid} GCLID, {calls_phone} phone) + {len(form_rows)} forms ({forms_gclid} GCLID, {forms_user} user ID)")
        print(json.dumps({
            "status": "dry_run",
            "calls": len(call_rows), "calls_gclid": calls_gclid, "calls_phone": calls_phone,
            "forms": len(form_rows), "forms_gclid": forms_gclid, "forms_user_id": forms_user,
        }))
        db.close()
        return

    from google.ads.googleads.client import GoogleAdsClient

    gads_client = GoogleAdsClient.load_from_storage(str(GOOGLE_ADS_YAML))
    db = get_db()
    cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    update_cur = db.cursor()

    # Gather uploadable calls
    cur.execute("""
        SELECT c.id, c.gclid, c.caller_phone, c.start_time, c.customer_id,
               cl.conversion_value AS client_conversion_value
        FROM calls c
        JOIN clients cl ON c.customer_id = cl.customer_id
        WHERE c.classification = 'legitimate'
          AND c.uploaded_to_gads = FALSE
          AND (c.gclid IS NOT NULL OR c.caller_phone IS NOT NULL)
        ORDER BY c.customer_id, c.start_time
    """)
    call_rows = cur.fetchall()

    # Gather uploadable forms
    cur.execute("""
        SELECT f.id, f.gclid, f.customer_phone, f.customer_email, f.submitted_at, f.customer_id,
               cl.conversion_value AS client_conversion_value
        FROM form_submissions f
        JOIN clients cl ON f.customer_id = cl.customer_id
        WHERE f.classification = 'legitimate'
          AND f.uploaded_to_gads = FALSE
          AND (f.gclid IS NOT NULL OR f.customer_phone IS NOT NULL OR f.customer_email IS NOT NULL)
        ORDER BY f.customer_id, f.submitted_at
    """)
    form_rows = cur.fetchall()

    # Normalize into unified upload items grouped by customer
    by_customer = {}

    for row in call_rows:
        cid = row["customer_id"]
        if cid not in by_customer:
            by_customer[cid] = []
        by_customer[cid].append({
            "id": row["id"],
            "_table": "calls",
            "gclid": row["gclid"],
            "phone": row["caller_phone"],
            "email": None,
            "ts": row["start_time"],
            "value": row["client_conversion_value"],
        })

    for row in form_rows:
        cid = row["customer_id"]
        if cid not in by_customer:
            by_customer[cid] = []
        by_customer[cid].append({
            "id": row["id"],
            "_table": "form_submissions",
            "gclid": row["gclid"],
            "phone": row["customer_phone"],
            "email": row["customer_email"],
            "ts": row["submitted_at"],
            "value": row["client_conversion_value"],
        })

    if not by_customer:
        log.info("No leads to upload")
        print(json.dumps({"status": "ok", "uploaded": 0}))
        db.close()
        return

    total_uploaded = 0
    total_skipped = 0
    all_errors = []

    for customer_id, items in by_customer.items():
        uploaded, skipped, errors = upload_conversions_batch(
            gads_client, customer_id, items, update_cur, db
        )
        total_uploaded += uploaded
        total_skipped += skipped
        all_errors.extend(errors)
        log.info(f"  Customer {customer_id}: uploaded {uploaded}, skipped {skipped}")

    print(json.dumps({
        "status": "ok" if not all_errors else "errors",
        "uploaded": total_uploaded,
        "skipped": total_skipped,
        "errors": all_errors,
    }))
    db.close()


# ===========================================================================
# Subcommand: log-run
# ===========================================================================
def cmd_log_run(args):
    """Log a pipeline run result."""
    db = get_db()
    cur = db.cursor()

    if args.action == "start":
        cur.execute("INSERT INTO call_pipeline_log DEFAULT VALUES RETURNING id")
        run_id = cur.fetchone()[0]
        db.commit()
        print(json.dumps({"run_id": run_id}))
    elif args.action == "finish":
        data = json.loads(args.data) if args.data else {}
        cur.execute("""
            UPDATE call_pipeline_log SET
                finished_at = NOW(),
                status = %s,
                calls_fetched = %s,
                calls_classified = %s,
                calls_spam = %s,
                calls_legitimate = %s,
                calls_uploaded = %s,
                errors = %s,
                clients_processed = %s
            WHERE id = %s
        """, (
            data.get("status", "completed"),
            data.get("calls_fetched", 0),
            data.get("calls_classified", 0),
            data.get("calls_spam", 0),
            data.get("calls_legitimate", 0),
            data.get("calls_uploaded", 0),
            data.get("errors") or None,
            data.get("clients_processed") or None,
            args.run_id,
        ))
        db.commit()
        print(json.dumps({"status": "ok"}))

    db.close()


# ===========================================================================
# Subcommand: summary
# ===========================================================================
def cmd_summary(args):
    """Print classification summary stats for calls and forms."""
    db = get_db()
    cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT
            classification,
            COUNT(*) as count,
            SUM(CASE WHEN uploaded_to_gads THEN 1 ELSE 0 END) as uploaded,
            SUM(CASE WHEN gclid IS NOT NULL THEN 1 ELSE 0 END) as with_gclid
        FROM calls
        GROUP BY classification
        ORDER BY classification
    """)
    call_rows = cur.fetchall()

    cur.execute("""
        SELECT
            classification,
            COUNT(*) as count,
            SUM(CASE WHEN uploaded_to_gads THEN 1 ELSE 0 END) as uploaded,
            SUM(CASE WHEN gclid IS NOT NULL THEN 1 ELSE 0 END) as with_gclid
        FROM form_submissions
        GROUP BY classification
        ORDER BY classification
    """)
    form_rows = cur.fetchall()
    db.close()

    result = {"calls": {}, "forms": {}}
    for row in call_rows:
        key = row["classification"] or "pending"
        result["calls"][key] = {"count": row["count"], "uploaded": row["uploaded"], "with_gclid": row["with_gclid"]}
    for row in form_rows:
        key = row["classification"] or "pending"
        result["forms"][key] = {"count": row["count"], "uploaded": row["uploaded"], "with_gclid": row["with_gclid"]}

    print(json.dumps(result, indent=2))


# ===========================================================================
# CLI
# ===========================================================================
def main():
    parser = argparse.ArgumentParser(description="Lead Qualifier Pipeline")
    subparsers = parser.add_subparsers(dest="command", help="Subcommand")

    # Calls
    p = subparsers.add_parser("fetch", help="Fetch Google Ads calls from CallRail")
    p.add_argument("--backfill-hours", type=int, help="Override lookback window (hours)")
    p.add_argument("--client", type=str, help="Single client customer_id")

    subparsers.add_parser("pending", help="Print unclassified calls as JSON")

    p = subparsers.add_parser("classify", help="Store a classification for one call")
    p.add_argument("--call-id", type=int, required=True)
    p.add_argument("--classification", type=str, required=True, choices=["spam", "legitimate"])
    p.add_argument("--reason", type=str, required=True)

    p = subparsers.add_parser("classify-batch", help="Store call classifications from JSON")
    p.add_argument("--data", type=str, help="JSON array of {id, classification, reason}")

    # Forms
    p = subparsers.add_parser("fetch-forms", help="Fetch Google Ads forms from CallRail")
    p.add_argument("--backfill-hours", type=int, help="Override lookback window (hours)")
    p.add_argument("--client", type=str, help="Single client customer_id")

    subparsers.add_parser("pending-forms", help="Print unclassified forms as JSON")

    p = subparsers.add_parser("classify-forms", help="Store form classifications from JSON")
    p.add_argument("--data", type=str, help="JSON array of {id, classification, reason}")

    # Shared
    p = subparsers.add_parser("upload", help="Upload legitimate leads to Google Ads")
    p.add_argument("--dry-run", action="store_true", help="Skip actual upload")

    p = subparsers.add_parser("log-run", help="Log a pipeline run")
    p.add_argument("--action", required=True, choices=["start", "finish"])
    p.add_argument("--run-id", type=int)
    p.add_argument("--data", type=str)

    subparsers.add_parser("summary", help="Print classification stats")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands = {
        "fetch": cmd_fetch,
        "pending": cmd_pending,
        "classify": cmd_classify,
        "classify-batch": cmd_classify_batch,
        "fetch-forms": cmd_fetch_forms,
        "pending-forms": cmd_pending_forms,
        "classify-forms": cmd_classify_forms,
        "upload": cmd_upload,
        "log-run": cmd_log_run,
        "summary": cmd_summary,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
