#!/usr/bin/env python3
"""
HCP Data Pull — Fetches customers, estimates, jobs, and invoices from Housecall Pro API,
upserts into PostgreSQL, and matches to CallRail leads by phone number.

Usage:
    python3 pull_hcp_data.py              # Pull all HCP clients
    python3 pull_hcp_data.py --client X   # Pull one client by customer_id
    python3 pull_hcp_data.py --backfill   # Full historical pull (no date filter)

Cron: every 30 min at :05/:35
"""

import sys
import re
import time
import json
import argparse
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode

import psycopg2
from psycopg2.extras import execute_values

from classifier import classify_job, classify_invoice, classify_estimate

# ============================================================
# Config
# ============================================================

DSN = "host=localhost port=5432 dbname=blueprint user=blueprint"
HCP_API_BASE = "https://api.housecallpro.com"

# How far back to look for changes on incremental pulls (minutes)
LOOKBACK_MINUTES = 60

# Rate limiting: seconds between API requests
RATE_LIMIT_DELAY = 0.5

# Accounts with more than this many customers get date-filtered on jobs/estimates/invoices
LARGE_ACCOUNT_THRESHOLD = 1000

# How far back to pull for large accounts (days)
LARGE_ACCOUNT_LOOKBACK_DAYS = 365

# Classification keywords
JOB_KEYWORDS = re.compile(
    r'remediation|dry\s*fog|treatment|removal|abatement|encapsulation',
    re.IGNORECASE
)
INSPECTION_KEYWORDS = re.compile(
    r'assessment|inspection|test|evaluat|consult|survey|sample'
    r'|estimate|sampling|walk.?through|instascope|scan'
    r'|moisture.?check|mold.?report|clearance|ermi',
    re.IGNORECASE
)

INSPECTION_PRIORITY_PHRASES = re.compile(
    r'pre.?treatment|air\s*quality\s*test|air\s*test|mold\s*test'
    r'|testing\s+and\s+estimate|visual\s+assessment|complimentary\s+estimate',
    re.IGNORECASE
)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger('hcp-sync')

# ============================================================
# Phone normalization (mirrors SQL normalize_phone function)
# ============================================================

def normalize_phone(phone):
    """Strip non-digits, take last 10 digits."""
    if not phone:
        return None
    digits = re.sub(r'\D', '', phone)
    if len(digits) < 10:
        return None
    return digits[-10:]


def derive_phone_primary(mobile, home, work):
    """Priority: mobile > home > work."""
    for p in [mobile, home, work]:
        if p and p.strip():
            return p.strip()
    return None


def extract_address(addr):
    """Build a one-line address string from HCP address dict."""
    if not addr or not isinstance(addr, dict):
        return None
    parts = [addr.get('street', ''), addr.get('city', ''), addr.get('state', ''), addr.get('zip', '')]
    result = ', '.join(p.strip() for p in parts if p and p.strip())
    return result or None


def extract_tags(tags_raw):
    """Extract tags list from HCP tags field (may be strings or dicts)."""
    if not tags_raw or not isinstance(tags_raw, list):
        return None
    tags = [t.get('name', t) if isinstance(t, dict) else str(t) for t in tags_raw if t]
    return tags or None


def concat_notes(notes_list):
    """Concatenate HCP notes array [{id, content}, ...] into single text."""
    if not notes_list or not isinstance(notes_list, list):
        return None
    parts = [n.get('content', '').strip() for n in notes_list if isinstance(n, dict) and n.get('content')]
    return '\n---\n'.join(parts) if parts else None

# ============================================================
# HCP API client
# ============================================================

def hcp_request(api_key, endpoint, params=None):
    """Make a GET request to the HCP API. Returns parsed JSON."""
    url = f"{HCP_API_BASE}{endpoint}"
    if params:
        url += '?' + urlencode(params)

    req = Request(url)
    req.add_header('Authorization', f'Bearer {api_key}')
    req.add_header('Content-Type', 'application/json')

    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except HTTPError as e:
        body = e.read().decode() if e.fp else ''
        log.error(f"HCP API {e.code} on {endpoint}: {body[:200]}")
        raise
    except URLError as e:
        log.error(f"HCP API connection error on {endpoint}: {e.reason}")
        raise


def hcp_paginate(api_key, endpoint, params=None, key=None):
    """Paginate through HCP API results. Returns all items."""
    params = dict(params or {})
    params.setdefault('page_size', 200)
    page = 1
    all_items = []

    while True:
        params['page'] = page
        data = hcp_request(api_key, endpoint, params)

        # HCP returns items under various keys
        items = data
        if key and key in data:
            items = data[key]
        elif isinstance(data, dict):
            # Try common keys
            for k in ['customers', 'estimates', 'jobs', 'invoices']:
                if k in data:
                    items = data[k]
                    break

        if not isinstance(items, list):
            items = [items] if items else []

        all_items.extend(items)

        # Check for more pages
        total_pages = data.get('total_pages', 1) if isinstance(data, dict) else 1
        if page >= total_pages:
            break

        page += 1
        time.sleep(RATE_LIMIT_DELAY)

    return all_items

# ============================================================
# Classification logic
# ============================================================

def classify_job_or_inspection(description, original_estimate_id, total_amount_cents):
    """
    Classify a job event as either 'job' or 'inspection'.

    Rules (in priority order):
    0. Description contains inspection-priority phrases (e.g. pre treatment, air quality test) → INSPECTION
    1. Description contains job keywords → JOB
    2. Description contains inspection keywords (and NOT job keywords) → INSPECTION
    3. original_estimate_id is filled → JOB (created from an estimate = actual work)
    4. Fallback: total_amount_cents < 10000 ($100) → INSPECTION, else → JOB
    """
    desc = (description or '').strip()

    # Rule 0: Inspection-priority phrases (checked before job keywords to avoid false positives)
    if INSPECTION_PRIORITY_PHRASES.search(desc):
        return 'inspection'

    if JOB_KEYWORDS.search(desc):
        return 'job'

    if INSPECTION_KEYWORDS.search(desc) and not JOB_KEYWORDS.search(desc):
        return 'inspection'

    if original_estimate_id:
        return 'job'

    if (total_amount_cents or 0) < 10000:
        return 'inspection'

    return 'job'

# ============================================================
# Database operations
# ============================================================

def validate_hcp_customer_id(cur, hcp_customer_id):
    """Check if an hcp_customer_id exists in hcp_customers. Returns it if valid, None otherwise."""
    if not hcp_customer_id:
        return None
    cur.execute("SELECT 1 FROM hcp_customers WHERE hcp_customer_id = %s", [hcp_customer_id])
    return hcp_customer_id if cur.fetchone() else None


def upsert_customer(cur, customer_id, c):
    """Upsert an HCP customer record."""
    mobile = (c.get('mobile_number') or '').strip() or None
    home = (c.get('home_number') or '').strip() or None
    work = (c.get('work_number') or '').strip() or None
    phone_primary = derive_phone_primary(mobile, home, work)
    phone_norm = normalize_phone(phone_primary)

    tags = extract_tags(c.get('tags'))

    cur.execute("""
        INSERT INTO hcp_customers (
            hcp_customer_id, customer_id,
            first_name, last_name, email,
            mobile_number, home_number, work_number,
            phone_primary, phone_normalized,
            street, city, state, zip,
            notes, lead_source, tags,
            hcp_created_at, hcp_updated_at
        ) VALUES (
            %(hcp_id)s, %(cust_id)s,
            %(first)s, %(last)s, %(email)s,
            %(mobile)s, %(home)s, %(work)s,
            %(primary)s, %(norm)s,
            %(street)s, %(city)s, %(state)s, %(zip)s,
            %(notes)s, %(lead_source)s, %(tags)s,
            %(created)s, %(updated)s
        )
        ON CONFLICT (hcp_customer_id) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            email = EXCLUDED.email,
            mobile_number = EXCLUDED.mobile_number,
            home_number = EXCLUDED.home_number,
            work_number = EXCLUDED.work_number,
            phone_primary = EXCLUDED.phone_primary,
            phone_normalized = EXCLUDED.phone_normalized,
            street = EXCLUDED.street,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            zip = EXCLUDED.zip,
            notes = EXCLUDED.notes,
            lead_source = EXCLUDED.lead_source,
            tags = EXCLUDED.tags,
            hcp_updated_at = EXCLUDED.hcp_updated_at,
            updated_at = NOW()
    """, {
        'hcp_id': c.get('id'),
        'cust_id': customer_id,
        'first': c.get('first_name'),
        'last': c.get('last_name'),
        'email': (c.get('email') or '').strip() or None,
        'mobile': mobile,
        'home': home,
        'work': work,
        'primary': phone_primary,
        'norm': phone_norm,
        'street': (c.get('address', {}) or {}).get('street'),
        'city': (c.get('address', {}) or {}).get('city'),
        'state': (c.get('address', {}) or {}).get('state'),
        'zip': (c.get('address', {}) or {}).get('zip'),
        'notes': (c.get('notes') or '').strip() or None,
        'lead_source': c.get('lead_source') or None,
        'tags': tags or None,
        'created': c.get('created_at'),
        'updated': c.get('updated_at'),
    })


def upsert_inspection(cur, customer_id, hcp_customer_id, insp):
    """Upsert an HCP inspection record."""
    cur.execute("""
        INSERT INTO hcp_inspections (
            hcp_id, customer_id, hcp_customer_id,
            source_event, status,
            scheduled_at, completed_at, hcp_created_at,
            total_amount_cents, employee_name, employee_id,
            description, service_address
        ) VALUES (
            %(hcp_id)s, %(cust_id)s, %(hcp_cust_id)s,
            %(source)s, %(status)s,
            %(scheduled)s, %(completed)s, %(created)s,
            %(amount)s, %(emp_name)s, %(emp_id)s,
            %(desc)s, %(addr)s
        )
        ON CONFLICT (hcp_id) DO UPDATE SET
            status = EXCLUDED.status,
            completed_at = COALESCE(EXCLUDED.completed_at, hcp_inspections.completed_at),
            hcp_created_at = COALESCE(EXCLUDED.hcp_created_at, hcp_inspections.hcp_created_at),
            total_amount_cents = EXCLUDED.total_amount_cents,
            employee_name = EXCLUDED.employee_name,
            employee_id = EXCLUDED.employee_id,
            description = EXCLUDED.description,
            service_address = EXCLUDED.service_address,
            updated_at = NOW()
    """, {
        'hcp_id': insp['hcp_id'],
        'cust_id': customer_id,
        'hcp_cust_id': hcp_customer_id,
        'source': insp.get('source_event'),
        'status': insp.get('status', 'scheduled'),
        'scheduled': insp.get('scheduled_at'),
        'completed': insp.get('completed_at'),
        'created': insp.get('created_at'),
        'amount': insp.get('total_amount_cents', 0),
        'emp_name': insp.get('employee_name'),
        'emp_id': insp.get('employee_id'),
        'desc': insp.get('description'),
        'addr': insp.get('service_address'),
    })


def upsert_estimate(cur, customer_id, hcp_customer_id, est):
    """Upsert an HCP estimate document and its options."""
    estimate_id = est.get('id') or est.get('hcp_estimate_id')
    options = est.get('options', [])

    # Calculate derived amounts from options
    highest_option_cents = 0
    approved_total_cents = 0
    any_approved = False

    any_declined = False
    all_declined = True

    for opt in options:
        amt = opt.get('total_amount', 0) or opt.get('total_amount_cents', 0) or opt.get('amount', 0) or 0
        # HCP may send amounts in dollars — normalize
        if isinstance(amt, float) and amt > 0 and amt < 100000:
            amt = int(amt * 100)  # Convert dollars to cents if looks like dollars
        highest_option_cents = max(highest_option_cents, amt)
        # Check approval_status (not status — status is work status)
        approval = (opt.get('approval_status') or '').lower()
        if approval in ('approved', 'pro approved'):
            approved_total_cents += amt
            any_approved = True
            all_declined = False
        elif approval in ('declined', 'pro declined'):
            any_declined = True
        else:
            all_declined = False

    if not options:
        all_declined = False

    est_status = 'approved' if any_approved else 'declined' if (any_declined and all_declined) else 'sent'

    # Extract employee from assigned_employees
    emp_name = None
    emp_id = None
    assigned = est.get('assigned_employees', [])
    if assigned and isinstance(assigned, list) and len(assigned) > 0:
        emp = assigned[0]
        emp_name = emp.get('name') or f"{emp.get('first_name', '')} {emp.get('last_name', '')}".strip()
        emp_id = emp.get('id')

    # Build option dicts for classify_estimate
    _est_option_dicts = []
    for _opt in options:
        _amt = _opt.get('total_amount', 0) or _opt.get('total_amount_cents', 0) or _opt.get('amount', 0) or 0
        if isinstance(_amt, float) and _amt > 0 and _amt < 100000:
            _amt = int(_amt * 100)
        _est_option_dicts.append({
            'name': _opt.get('name') or _opt.get('label'),
            'total_cents': _amt,
            'tags': extract_tags(_opt.get('tags')),
            'message_from_pro': (_opt.get('message_from_pro') or '').strip() or None,
            'status': _opt.get('status'),
            'approval_status': _opt.get('approval_status'),
        })
    _est_class = classify_estimate(
        options=_est_option_dicts,
        status=est_status,
        linked_job_category=None,  # jobs pulled after estimates; backfill re-applies
        fallback_highest_option_cents=highest_option_cents,
    )
    # Write to legacy estimate_type for v_estimate_groups; fall back to 'treatment'
    # for canceled/unknown so existing consumers don't break.
    estimate_type = (
        _est_class['category']
        if _est_class['category'] in ('treatment', 'inspection')
        else 'treatment'
    )

    cur.execute("""
        INSERT INTO hcp_estimates (
            hcp_estimate_id, customer_id, hcp_customer_id,
            status, sent_at, approved_at, hcp_created_at,
            highest_option_cents, approved_total_cents,
            employee_name, employee_id, estimate_type, service_address,
            work_category, review_needed, review_reason, classifier_signal, classified_at
        ) VALUES (
            %(est_id)s, %(cust_id)s, %(hcp_cust_id)s,
            %(status)s, %(sent_at)s, %(approved_at)s, %(created_at)s,
            %(highest)s, %(approved)s,
            %(emp_name)s, %(emp_id)s, %(est_type)s, %(addr)s,
            %(work_category)s, %(review_needed)s, %(review_reason)s, %(classifier_signal)s, NOW()
        )
        ON CONFLICT (hcp_estimate_id) DO UPDATE SET
            status = EXCLUDED.status,
            approved_at = COALESCE(EXCLUDED.approved_at, hcp_estimates.approved_at),
            hcp_created_at = COALESCE(EXCLUDED.hcp_created_at, hcp_estimates.hcp_created_at),
            highest_option_cents = EXCLUDED.highest_option_cents,
            approved_total_cents = EXCLUDED.approved_total_cents,
            employee_name = EXCLUDED.employee_name,
            employee_id = EXCLUDED.employee_id,
            estimate_type = EXCLUDED.estimate_type,
            service_address = EXCLUDED.service_address,
            work_category = EXCLUDED.work_category,
            review_needed = EXCLUDED.review_needed,
            review_reason = EXCLUDED.review_reason,
            classifier_signal = EXCLUDED.classifier_signal,
            classified_at = NOW(),
            updated_at = NOW()
    """, {
        'est_id': estimate_id,
        'cust_id': customer_id,
        'hcp_cust_id': hcp_customer_id,
        'status': est_status,
        'sent_at': est.get('sent_at') or est.get('created_at'),
        'approved_at': est.get('approved_at'),
        'created_at': est.get('created_at'),
        'highest': highest_option_cents,
        'approved': approved_total_cents,
        'emp_name': emp_name or None,
        'emp_id': emp_id,
        'est_type': estimate_type,
        'work_category': _est_class['category'],
        'review_needed': _est_class['review_needed'],
        'review_reason': _est_class['review_reason'],
        'classifier_signal': _est_class['signal'],
        'addr': extract_address(est.get('address')),
    })

    # Upsert options
    for i, opt in enumerate(options):
        amt = opt.get('total_amount', 0) or opt.get('total_amount_cents', 0) or opt.get('amount', 0) or 0
        if isinstance(amt, float) and amt > 0 and amt < 100000:
            amt = int(amt * 100)
        opt_status = (opt.get('status') or 'sent').lower()

        cur.execute("""
            INSERT INTO hcp_estimate_options (
                hcp_estimate_id, option_number, name,
                total_amount_cents, status, approval_status,
                notes, message_from_pro,
                hcp_option_id, tags
            ) VALUES (
                %(est_id)s, %(num)s, %(name)s, %(amt)s, %(status)s,
                %(approval)s, %(notes)s, %(msg)s,
                %(hcp_option_id)s, %(tags)s
            )
            ON CONFLICT (hcp_estimate_id, option_number) DO UPDATE SET
                name = EXCLUDED.name,
                total_amount_cents = EXCLUDED.total_amount_cents,
                status = EXCLUDED.status,
                approval_status = EXCLUDED.approval_status,
                notes = EXCLUDED.notes,
                message_from_pro = EXCLUDED.message_from_pro,
                hcp_option_id = EXCLUDED.hcp_option_id,
                tags = EXCLUDED.tags,
                updated_at = NOW()
        """, {
            'est_id': estimate_id,
            'num': i + 1,
            'name': opt.get('name') or opt.get('label'),
            'amt': amt,
            'status': opt_status,
            'approval': opt.get('approval_status'),
            'notes': concat_notes(opt.get('notes')),
            'msg': (opt.get('message_from_pro') or '').strip() or None,
            'hcp_option_id': opt.get('id'),  # the est_xxx ID that jobs reference
            'tags': extract_tags(opt.get('tags')),
        })


def upsert_job(cur, customer_id, hcp_customer_id, job):
    """Upsert an HCP job record (parent jobs only — segments go to hcp_job_segments)."""
    invoice_number = job.get('invoice_number') or ''

    # Classify this job (treatment/inspection/canceled/unknown) using the
    # classifier module so it gets the right work_category at ingest time.
    _job_desc = job.get('description')
    _job_tags = extract_tags(job.get('tags'))
    _job_custom_type = ((job.get('job_fields') or {}).get('job_type') or {}).get('name')
    _job_amount = job.get('total_amount_cents', 0) or job.get('total_amount', 0) or 0
    _job_status = job.get('work_status', 'scheduled')
    # Look up linked estimate option (if job was created from an estimate).
    # Estimates are pulled BEFORE jobs so the option row exists by now.
    _linked_option = None
    _orig_est_id = job.get('original_estimate_id')
    if _orig_est_id:
        cur.execute(
            """SELECT name, total_amount_cents, tags, message_from_pro, status
               FROM hcp_estimate_options WHERE hcp_option_id = %s LIMIT 1""",
            [_orig_est_id],
        )
        _opt_row = cur.fetchone()
        if _opt_row:
            _linked_option = {
                'name': _opt_row[0],
                'total_cents': _opt_row[1] or 0,
                'tags': _opt_row[2] or [],
                'message_from_pro': _opt_row[3],
                'status': _opt_row[4],
            }
    _classification = classify_job(
        description=_job_desc,
        tags=_job_tags,
        hcp_job_type=_job_custom_type,
        line_items=None,  # line item pull comes later
        total_cents=_job_amount,
        status=_job_status,
        parent_job_classification=None,  # parent inheritance handled post-grouping
        linked_estimate=None,
        linked_option=_linked_option,
    )

    cur.execute("""
        INSERT INTO hcp_jobs (
            hcp_job_id, customer_id, hcp_customer_id,
            description, invoice_number, total_amount_cents,
            original_estimate_id,
            status, scheduled_at, completed_at, hcp_created_at,
            employee_name, employee_id, notes, tags, service_address,
            job_type,
            work_category, review_needed, review_reason, classifier_signal, classified_at
        ) VALUES (
            %(job_id)s, %(cust_id)s, %(hcp_cust_id)s,
            %(desc)s, %(invoice)s, %(amount)s,
            %(est_id)s,
            %(status)s, %(scheduled)s, %(completed)s, %(created)s,
            %(emp_name)s, %(emp_id)s, %(notes)s, %(tags)s, %(addr)s,
            %(job_type)s,
            %(work_category)s, %(review_needed)s, %(review_reason)s, %(classifier_signal)s, NOW()
        )
        ON CONFLICT (hcp_job_id) DO UPDATE SET
            description = EXCLUDED.description,
            invoice_number = EXCLUDED.invoice_number,
            total_amount_cents = EXCLUDED.total_amount_cents,
            original_estimate_id = EXCLUDED.original_estimate_id,
            status = EXCLUDED.status,
            completed_at = COALESCE(EXCLUDED.completed_at, hcp_jobs.completed_at),
            hcp_created_at = COALESCE(EXCLUDED.hcp_created_at, hcp_jobs.hcp_created_at),
            employee_name = EXCLUDED.employee_name,
            employee_id = EXCLUDED.employee_id,
            notes = EXCLUDED.notes,
            tags = EXCLUDED.tags,
            service_address = EXCLUDED.service_address,
            job_type = EXCLUDED.job_type,
            work_category = EXCLUDED.work_category,
            review_needed = EXCLUDED.review_needed,
            review_reason = EXCLUDED.review_reason,
            classifier_signal = EXCLUDED.classifier_signal,
            classified_at = NOW(),
            updated_at = NOW()
    """, {
        'job_id': job.get('id'),
        'cust_id': customer_id,
        'hcp_cust_id': hcp_customer_id,
        'desc': _job_desc,
        'invoice': invoice_number or None,
        'amount': _job_amount,
        'est_id': job.get('original_estimate_id'),
        'status': _job_status,
        'scheduled': job.get('schedule', {}).get('scheduled_start') if isinstance(job.get('schedule'), dict) else job.get('scheduled_start'),
        'completed': (job.get('work_timestamps') or {}).get('completed_at') or job.get('completed_at'),
        'created': job.get('created_at'),
        'emp_name': None,
        'emp_id': None,
        'notes': concat_notes(job.get('notes')),
        'tags': _job_tags,
        'addr': extract_address(job.get('address')),
        'job_type': _job_custom_type,
        'work_category': _classification['category'],
        'review_needed': _classification['review_needed'],
        'review_reason': _classification['review_reason'],
        'classifier_signal': _classification['signal'],
    })

    # Try to set employee from assigned_employee
    assigned = job.get('assigned_employee') or job.get('dispatched_employee')
    if assigned:
        emp_name = assigned.get('name') or f"{assigned.get('first_name', '')} {assigned.get('last_name', '')}".strip()
        emp_id = assigned.get('id')
        if emp_name or emp_id:
            cur.execute("""
                UPDATE hcp_jobs SET employee_name = %s, employee_id = %s
                WHERE hcp_job_id = %s
            """, [emp_name or None, emp_id, job.get('id')])


def upsert_job_segment(cur, customer_id, hcp_customer_id, job, parent_hcp_job_id):
    """Upsert an HCP job segment record."""
    invoice_number = job.get('invoice_number') or ''
    # Parse segment number from "123-2" → 2
    segment_number = 1
    if '-' in invoice_number:
        try:
            segment_number = int(invoice_number.split('-')[1])
        except (ValueError, IndexError):
            segment_number = 1

    emp_name = None
    emp_id = None
    assigned = job.get('assigned_employee') or job.get('dispatched_employee')
    if not assigned:
        employees = job.get('assigned_employees', [])
        if employees and isinstance(employees, list):
            assigned = employees[0]
    if assigned:
        emp_name = assigned.get('name') or f"{assigned.get('first_name', '')} {assigned.get('last_name', '')}".strip()
        emp_id = assigned.get('id')

    cur.execute("""
        INSERT INTO hcp_job_segments (
            hcp_job_id, parent_hcp_job_id, customer_id, hcp_customer_id,
            segment_number, invoice_number,
            description, total_amount_cents,
            status, scheduled_at, completed_at,
            employee_name, employee_id, notes, tags, service_address
        ) VALUES (
            %(job_id)s, %(parent_id)s, %(cust_id)s, %(hcp_cust_id)s,
            %(seg_num)s, %(invoice)s,
            %(desc)s, %(amount)s,
            %(status)s, %(scheduled)s, %(completed)s,
            %(emp_name)s, %(emp_id)s, %(notes)s, %(tags)s, %(addr)s
        )
        ON CONFLICT (hcp_job_id) DO UPDATE SET
            parent_hcp_job_id = EXCLUDED.parent_hcp_job_id,
            description = EXCLUDED.description,
            total_amount_cents = EXCLUDED.total_amount_cents,
            status = EXCLUDED.status,
            completed_at = COALESCE(EXCLUDED.completed_at, hcp_job_segments.completed_at),
            employee_name = EXCLUDED.employee_name,
            employee_id = EXCLUDED.employee_id,
            notes = EXCLUDED.notes,
            tags = EXCLUDED.tags,
            service_address = EXCLUDED.service_address,
            updated_at = NOW()
    """, {
        'job_id': job.get('id'),
        'parent_id': parent_hcp_job_id,
        'cust_id': customer_id,
        'hcp_cust_id': hcp_customer_id,
        'seg_num': segment_number,
        'invoice': invoice_number or None,
        'desc': job.get('description'),
        'amount': job.get('total_amount_cents', 0) or job.get('total_amount', 0) or 0,
        'status': job.get('work_status', 'scheduled'),
        'scheduled': job.get('schedule', {}).get('scheduled_start') if isinstance(job.get('schedule'), dict) else job.get('scheduled_start'),
        'completed': (job.get('work_timestamps') or {}).get('completed_at') or job.get('completed_at'),
        'emp_name': emp_name or None,
        'emp_id': emp_id,
        'notes': concat_notes(job.get('notes')),
        'tags': extract_tags(job.get('tags')),
        'addr': extract_address(job.get('address')),
    })


def upsert_invoice(cur, customer_id, inv):
    """Upsert an HCP invoice and its line items."""
    hcp_job_id = inv.get('job_id')

    # Resolve hcp_customer_id from the job if we have it
    hcp_customer_id = None
    if hcp_job_id:
        cur.execute(
            "SELECT hcp_customer_id FROM hcp_jobs WHERE hcp_job_id = %s",
            [hcp_job_id]
        )
        row = cur.fetchone()
        if row:
            hcp_customer_id = row[0]

    # Calculate discount and tax totals
    discounts = inv.get('discounts', []) or []
    taxes = inv.get('taxes', []) or []
    discount_cents = sum(abs(d.get('amount', 0)) for d in discounts)
    tax_cents = sum(t.get('amount', 0) for t in taxes)

    # Extract payment info from first payment
    payments = inv.get('payments', []) or []
    payment_method = None
    payment_note = None
    if payments:
        payment_method = payments[0].get('category') or payments[0].get('payment_method')
        payment_note = payments[0].get('note')

    # Build line items for classifier — HCP returns them on the invoice dict.
    # Shape matches classify_invoice's expected format (name, amount_cents).
    _inv_line_items = [
        {'name': it.get('name'), 'amount_cents': it.get('amount', 0) or 0}
        for it in (inv.get('items', []) or [])
    ]
    # Look up linked job's work_category for the fallback signal
    _linked_job_cat = None
    if hcp_job_id:
        cur.execute(
            "SELECT work_category FROM hcp_jobs WHERE hcp_job_id = %s",
            [hcp_job_id]
        )
        _row = cur.fetchone()
        if _row:
            _linked_job_cat = _row[0]

    _inv_class = classify_invoice(
        line_items=_inv_line_items,
        total_cents=inv.get('amount', 0) or 0,
        status=inv.get('status'),
        linked_job_category=_linked_job_cat,
    )
    # Also write to legacy invoice_type for existing consumers (mv_funnel_leads,
    # review app, etc). 'canceled' and 'unknown' leave invoice_type alone.
    _legacy_invoice_type = (
        _inv_class['category']
        if _inv_class['category'] in ('treatment', 'inspection')
        else None
    )

    cur.execute("""
        INSERT INTO hcp_invoices (
            hcp_invoice_id, customer_id, hcp_customer_id, hcp_job_id,
            invoice_number, status, invoice_sequence,
            amount_cents, subtotal_cents, due_amount_cents,
            discount_cents, tax_cents,
            invoice_date, sent_at, paid_at, due_at, service_date,
            payment_method, payment_note,
            work_category, review_needed, review_reason, classifier_signal, classified_at,
            invoice_type
        ) VALUES (
            %(inv_id)s, %(cust_id)s, %(hcp_cust_id)s, %(job_id)s,
            %(inv_num)s, %(status)s, %(seq)s,
            %(amount)s, %(subtotal)s, %(due)s,
            %(discount)s, %(tax)s,
            %(inv_date)s, %(sent)s, %(paid)s, %(due_at)s, %(svc_date)s,
            %(pay_method)s, %(pay_note)s,
            %(work_category)s, %(review_needed)s, %(review_reason)s, %(classifier_signal)s, NOW(),
            COALESCE(%(legacy_type)s, 'treatment')
        )
        ON CONFLICT (hcp_invoice_id) DO UPDATE SET
            status = EXCLUDED.status,
            invoice_sequence = EXCLUDED.invoice_sequence,
            amount_cents = EXCLUDED.amount_cents,
            subtotal_cents = EXCLUDED.subtotal_cents,
            due_amount_cents = EXCLUDED.due_amount_cents,
            discount_cents = EXCLUDED.discount_cents,
            tax_cents = EXCLUDED.tax_cents,
            paid_at = COALESCE(EXCLUDED.paid_at, hcp_invoices.paid_at),
            sent_at = COALESCE(EXCLUDED.sent_at, hcp_invoices.sent_at),
            payment_method = COALESCE(EXCLUDED.payment_method, hcp_invoices.payment_method),
            payment_note = COALESCE(EXCLUDED.payment_note, hcp_invoices.payment_note),
            work_category = EXCLUDED.work_category,
            review_needed = EXCLUDED.review_needed,
            review_reason = EXCLUDED.review_reason,
            classifier_signal = EXCLUDED.classifier_signal,
            classified_at = NOW(),
            invoice_type = COALESCE(EXCLUDED.invoice_type, hcp_invoices.invoice_type),
            updated_at = NOW()
    """, {
        'inv_id': inv.get('id'),
        'cust_id': customer_id,
        'hcp_cust_id': hcp_customer_id,
        'job_id': hcp_job_id,
        'inv_num': inv.get('invoice_number'),
        'status': inv.get('status', 'open'),
        'seq': inv.get('_sequence', 1),
        'amount': inv.get('amount', 0) or 0,
        'subtotal': inv.get('subtotal', 0) or 0,
        'due': inv.get('due_amount', 0) or 0,
        'discount': discount_cents,
        'tax': tax_cents,
        'inv_date': inv.get('invoice_date'),
        'sent': inv.get('sent_at'),
        'paid': inv.get('paid_at'),
        'due_at': inv.get('due_at'),
        'svc_date': inv.get('service_date'),
        'pay_method': payment_method,
        'pay_note': payment_note,
        'work_category': _inv_class['category'],
        'review_needed': _inv_class['review_needed'],
        'review_reason': _inv_class['review_reason'],
        'classifier_signal': _inv_class['signal'],
        'legacy_type': _legacy_invoice_type,
    })

    # Upsert line items — delete existing then re-insert (items don't have stable IDs across pulls)
    cur.execute(
        "DELETE FROM hcp_invoice_items WHERE hcp_invoice_id = %s",
        [inv.get('id')]
    )
    for item in (inv.get('items', []) or []):
        cur.execute("""
            INSERT INTO hcp_invoice_items (
                hcp_invoice_id, hcp_item_id, name, type,
                unit_cost_cents, unit_price_cents,
                qty_in_hundredths, amount_cents
            ) VALUES (
                %(inv_id)s, %(item_id)s, %(name)s, %(type)s,
                %(cost)s, %(price)s, %(qty)s, %(amount)s
            )
        """, {
            'inv_id': inv.get('id'),
            'item_id': item.get('id'),
            'name': item.get('name'),
            'type': item.get('type'),
            'cost': item.get('unit_cost', 0) or 0,
            'price': item.get('unit_price', 0) or 0,
            'qty': item.get('qty_in_hundredths', 0) or 0,
            'amount': item.get('amount', 0) or 0,
        })


def match_callrail(cur, customer_id):
    """
    Match HCP customers to CallRail leads.
    Step 1: Phone match via calls (highest confidence)
    Step 2: Email match via form_submissions (fallback for unmatched)
    Step 3: Phone match via form_submissions (catches form-only leads)
    Step 4: Phone match via webflow_submissions (Webflow forms with GCLIDs)
    Step 5: Name match via GHL bridge (different phone, same person within 3 days)
    Returns total match count.
    """
    # Step 1: Match by phone — find the earliest CallRail call for each HCP customer's phone
    # Also matches calls from child customer accounts (for clients with multiple CallRail accounts)
    cur.execute("""
        UPDATE hcp_customers hc
        SET
            callrail_id = sub.callrail_id,
            match_method = 'phone',
            updated_at = NOW()
        FROM (
            SELECT DISTINCT ON (hc2.hcp_customer_id)
                hc2.hcp_customer_id,
                c.callrail_id
            FROM hcp_customers hc2
            JOIN calls c ON normalize_phone(c.caller_phone) = hc2.phone_normalized
                AND (c.customer_id = hc2.customer_id
                     OR c.customer_id IN (SELECT customer_id FROM clients WHERE parent_customer_id = hc2.customer_id))
            WHERE hc2.customer_id = %(cust_id)s
              AND hc2.phone_normalized IS NOT NULL
              AND c.caller_phone IS NOT NULL
            ORDER BY hc2.hcp_customer_id, c.start_time ASC
        ) sub
        WHERE hc.hcp_customer_id = sub.hcp_customer_id
          AND hc.customer_id = %(cust_id)s
          AND (hc.callrail_id IS NULL OR hc.callrail_id != sub.callrail_id)
    """, {'cust_id': customer_id})
    phone_matches = cur.rowcount

    # Step 2: Email match — for customers still unmatched, try matching email to form_submissions
    cur.execute("""
        UPDATE hcp_customers hc
        SET
            callrail_id = sub.callrail_id,
            match_method = 'email',
            updated_at = NOW()
        FROM (
            SELECT DISTINCT ON (hc2.hcp_customer_id)
                hc2.hcp_customer_id,
                f.callrail_id
            FROM hcp_customers hc2
            JOIN form_submissions f
                ON LOWER(TRIM(hc2.email)) = LOWER(TRIM(f.customer_email))
                AND f.customer_id = hc2.customer_id
            WHERE hc2.customer_id = %(cust_id)s
              AND hc2.callrail_id IS NULL
              AND hc2.email IS NOT NULL AND hc2.email != ''
              AND f.customer_email IS NOT NULL AND f.customer_email != ''
            ORDER BY hc2.hcp_customer_id, f.submitted_at ASC
        ) sub
        WHERE hc.hcp_customer_id = sub.hcp_customer_id
          AND hc.customer_id = %(cust_id)s
          AND hc.callrail_id IS NULL
    """, {'cust_id': customer_id})
    email_matches = cur.rowcount

    # Step 3: Phone match via form_submissions — for customers still unmatched,
    # try matching phone_normalized to form submission phone numbers
    cur.execute("""
        UPDATE hcp_customers hc
        SET
            callrail_id = sub.callrail_id,
            match_method = 'phone',
            updated_at = NOW()
        FROM (
            SELECT DISTINCT ON (hc2.hcp_customer_id)
                hc2.hcp_customer_id,
                f.callrail_id
            FROM hcp_customers hc2
            JOIN form_submissions f
                ON hc2.phone_normalized = normalize_phone(f.customer_phone)
                AND (f.customer_id = hc2.customer_id
                     OR f.customer_id IN (SELECT customer_id FROM clients WHERE parent_customer_id = hc2.customer_id))
            WHERE hc2.customer_id = %(cust_id)s
              AND hc2.callrail_id IS NULL
              AND hc2.phone_normalized IS NOT NULL
              AND f.customer_phone IS NOT NULL AND f.customer_phone != ''
            ORDER BY hc2.hcp_customer_id, f.submitted_at ASC
        ) sub
        WHERE hc.hcp_customer_id = sub.hcp_customer_id
          AND hc.customer_id = %(cust_id)s
          AND hc.callrail_id IS NULL
    """, {'cust_id': customer_id})
    form_phone_matches = cur.rowcount

    # Step 4: Phone match via webflow_submissions — for customers still unmatched,
    # catches leads who submitted through Webflow forms with GCLIDs (bypassed CallRail)
    cur.execute("""
        UPDATE hcp_customers hc
        SET
            callrail_id = sub.callrail_id,
            match_method = 'webflow',
            updated_at = NOW()
        FROM (
            SELECT DISTINCT ON (hc2.hcp_customer_id)
                hc2.hcp_customer_id,
                'WF_' || ws.id AS callrail_id
            FROM hcp_customers hc2
            JOIN webflow_submissions ws
                ON hc2.phone_normalized = ws.phone_normalized
                AND (ws.customer_id = hc2.customer_id
                     OR ws.customer_id IN (SELECT customer_id FROM clients WHERE parent_customer_id = hc2.customer_id))
            WHERE hc2.customer_id = %(cust_id)s
              AND hc2.callrail_id IS NULL
              AND hc2.phone_normalized IS NOT NULL
              AND ws.phone_normalized IS NOT NULL
              AND ws.gclid IS NOT NULL
            ORDER BY hc2.hcp_customer_id, ws.submitted_at ASC
        ) sub
        WHERE hc.hcp_customer_id = sub.hcp_customer_id
          AND hc.customer_id = %(cust_id)s
          AND hc.callrail_id IS NULL
    """, {'cust_id': customer_id})
    webflow_matches = cur.rowcount
    if webflow_matches:
        log.info(f'    Step 4: Matched {webflow_matches} via webflow_submissions')

    # Step 5: Name match via GHL bridge — for customers still unmatched,
    # find GHL contacts with matching first+last name but different phone,
    # then link to CallRail call on the GHL phone, within 3 days of HCP creation.
    # Only matches when the name is unique in the customer account for the window.
    cur.execute("""
        UPDATE hcp_customers hc
        SET
            callrail_id = sub.callrail_id,
            match_method = 'name',
            updated_at = NOW()
        FROM (
            SELECT DISTINCT ON (hc2.hcp_customer_id)
                hc2.hcp_customer_id,
                c.callrail_id
            FROM hcp_customers hc2
            JOIN ghl_contacts gc
                ON gc.customer_id = hc2.customer_id
                AND LOWER(TRIM(gc.first_name)) = LOWER(TRIM(hc2.first_name))
                AND LOWER(TRIM(gc.last_name)) = LOWER(TRIM(hc2.last_name))
                AND gc.phone_normalized IS NOT NULL
                AND gc.phone_normalized != hc2.phone_normalized
            JOIN calls c
                ON normalize_phone(c.caller_phone) = gc.phone_normalized
                AND c.customer_id = hc2.customer_id
            WHERE hc2.customer_id = %(cust_id)s
              AND hc2.callrail_id IS NULL
              AND hc2.phone_normalized IS NOT NULL
              AND hc2.first_name IS NOT NULL AND TRIM(hc2.first_name) != ''
              AND hc2.last_name IS NOT NULL AND TRIM(hc2.last_name) != ''
              AND ABS(EXTRACT(EPOCH FROM hc2.hcp_created_at - c.start_time)) <= 3 * 86400
              -- Uniqueness: no other HCP customer with same name within 3 days
              AND NOT EXISTS (
                  SELECT 1 FROM hcp_customers hc3
                  WHERE hc3.customer_id = hc2.customer_id
                    AND hc3.hcp_customer_id != hc2.hcp_customer_id
                    AND LOWER(TRIM(hc3.first_name)) = LOWER(TRIM(hc2.first_name))
                    AND LOWER(TRIM(hc3.last_name)) = LOWER(TRIM(hc2.last_name))
                    AND ABS(EXTRACT(EPOCH FROM hc3.hcp_created_at - hc2.hcp_created_at)) <= 3 * 86400
              )
            ORDER BY hc2.hcp_customer_id, c.start_time ASC
        ) sub
        WHERE hc.hcp_customer_id = sub.hcp_customer_id
          AND hc.customer_id = %(cust_id)s
          AND hc.callrail_id IS NULL
    """, {'cust_id': customer_id})
    name_matches = cur.rowcount
    if name_matches:
        log.info(f'    Step 5: Matched {name_matches} via GHL name bridge')

    return phone_matches + email_matches + form_phone_matches + webflow_matches + name_matches


def detect_exceptions(cur, customer_id):
    """
    Flag exception cases on hcp_customers for review.
    Only sets flags — never clears manually-resolved flags.
    Returns count of customers with new exceptions.
    """
    # 1. no_phone — HCP customer has no phone number
    cur.execute("""
        UPDATE hcp_customers
        SET exception_flags = array_append(
            COALESCE(exception_flags, '{}'), 'no_phone'
        ), updated_at = NOW()
        WHERE customer_id = %(cid)s
          AND phone_normalized IS NULL
          AND (exception_flags IS NULL OR NOT 'no_phone' = ANY(exception_flags))
    """, {'cid': customer_id})

    # 2. no_phone_match — has phone but no CallRail match
    cur.execute("""
        UPDATE hcp_customers
        SET exception_flags = array_append(
            COALESCE(exception_flags, '{}'), 'no_phone_match'
        ), updated_at = NOW()
        WHERE customer_id = %(cid)s
          AND phone_normalized IS NOT NULL
          AND callrail_id IS NULL
          AND (exception_flags IS NULL OR NOT 'no_phone_match' = ANY(exception_flags))
    """, {'cid': customer_id})

    # 3. (removed — name_mismatch was based on caller ID which is unreliable)

    # 4. classification_fallback — job/inspection classified by amount (no keyword match)
    # Skip if items have been grouped (grouping resolves classification)
    cur.execute("""
        UPDATE hcp_customers hc
        SET exception_flags = array_append(
            COALESCE(hc.exception_flags, '{}'), 'classification_fallback'
        ), updated_at = NOW()
        WHERE hc.customer_id = %(cid)s
          AND EXISTS (
              SELECT 1 FROM hcp_inspections i
              WHERE i.hcp_customer_id = hc.hcp_customer_id
                AND i.source_event = 'job.scheduled'
                AND i.record_status = 'active'
                AND (i.description IS NULL OR i.description = ''
                     OR (i.description !~* 'assessment|inspection|test|evaluat|consult|survey|sample'
                         AND i.description !~* 'remediation|dry.?fog|treatment|removal|abatement|encapsulation'))
          )
          AND NOT EXISTS (
              SELECT 1 FROM hcp_inspections ins
              WHERE ins.hcp_customer_id = hc.hcp_customer_id AND ins.record_status = 'segment'
          )
          AND NOT EXISTS (
              SELECT 1 FROM hcp_jobs j
              WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'segment'
          )
          AND (hc.exception_flags IS NULL OR NOT 'classification_fallback' = ANY(hc.exception_flags))
    """, {'cid': customer_id})

    # 5. missing_funnel_step — has job >K but no estimate (skipped estimate step)
    cur.execute("""
        UPDATE hcp_customers hc
        SET exception_flags = array_append(
            COALESCE(hc.exception_flags, '{}'), 'missing_funnel_step'
        ), updated_at = NOW()
        WHERE hc.customer_id = %(cid)s
          AND hc.callrail_id IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM hcp_jobs j
              WHERE j.hcp_customer_id = hc.hcp_customer_id
                AND j.record_status = 'active'
                AND j.status NOT IN ('user canceled', 'pro canceled')
                AND j.total_amount_cents > 200000
          )
          AND NOT EXISTS (
              SELECT 1 FROM hcp_estimates e
              WHERE e.hcp_customer_id = hc.hcp_customer_id
                AND e.record_status IN ('active', 'option')
          )
          AND (hc.exception_flags IS NULL OR NOT 'missing_funnel_step' = ANY(hc.exception_flags))
    """, {'cid': customer_id})

    # 6. job_no_estimate — job >K scheduled but no estimate sent (possible missing data)
    cur.execute("""
        UPDATE hcp_customers hc
        SET exception_flags = array_append(
            COALESCE(hc.exception_flags, '{}'), 'job_no_estimate'
        ), updated_at = NOW()
        WHERE hc.customer_id = %(cid)s
          AND hc.callrail_id IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM hcp_jobs j
              WHERE j.hcp_customer_id = hc.hcp_customer_id
                AND j.record_status = 'active'
                AND j.status NOT IN ('user canceled', 'pro canceled')
                AND j.total_amount_cents > 200000
          )
          AND NOT EXISTS (
              SELECT 1 FROM hcp_estimates e
              WHERE e.hcp_customer_id = hc.hcp_customer_id
                AND e.record_status IN ('active', 'option')
          )
          AND (hc.exception_flags IS NULL OR NOT 'job_no_estimate' = ANY(hc.exception_flags))
    """, {'cid': customer_id})

    # 7. pre_lead — HCP customer existed 7+ days before first CallRail contact
    #    Indicates a pre-existing customer who later clicked a Google Ad.
    #    Revenue attribution may be incorrect (lead existed before the ad drove them).
    cur.execute("""
        UPDATE hcp_customers hc
        SET exception_flags = array_append(
            COALESCE(hc.exception_flags, '{}'), 'pre_lead'
        ), updated_at = NOW()
        WHERE hc.customer_id = %(cid)s
          AND hc.callrail_id IS NOT NULL
          AND hc.hcp_created_at IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM calls c
              WHERE c.callrail_id = hc.callrail_id
                AND c.start_time::date - hc.hcp_created_at::date > 7
          )
          AND (hc.exception_flags IS NULL OR NOT 'pre_lead' = ANY(hc.exception_flags))
    """, {'cid': customer_id})

    # 8. multiple_estimates — REMOVED (auto-grouping handles this)

    # 9. multiple_inspections — REMOVED (auto-grouping handles this)

    # 10a. Auto-fix: set count_revenue = false on canceled jobs
    cur.execute("""
        UPDATE hcp_jobs
        SET count_revenue = false, updated_at = NOW()
        WHERE customer_id = %(cid)s
          AND status IN ('user canceled', 'pro canceled')
          AND count_revenue = true
    """, {'cid': customer_id})

    # 10b. Auto-fix: set count_revenue = false on canceled inspections
    cur.execute("""
        UPDATE hcp_inspections
        SET count_revenue = false, updated_at = NOW()
        WHERE customer_id = %(cid)s
          AND status IN ('user canceled', 'pro canceled')
          AND count_revenue = true
    """, {'cid': customer_id})
    # 10c. Auto-fix: set count_revenue = false on $0 unknown estimates
    #      These are inspection confirmations/placeholders, not real treatment estimates
    cur.execute("""
        UPDATE hcp_estimates
        SET count_revenue = false, updated_at = NOW()
        WHERE customer_id = %(cid)s
          AND estimate_type = 'unknown'
          AND highest_option_cents = 0
          AND record_status IN ('active', 'option')
          AND count_revenue = true
    """, {'cid': customer_id})

    # 10d. Auto-fix: restore count_revenue on APPROVED treatment estimates
    #      The classifier handles treatment/inspection separation; if it says
    #      treatment AND it's approved, it should count regardless of dollar amount
    #      (catches the Work Authorization $0 pattern where pricing is on the invoice).
    cur.execute("""
        UPDATE hcp_estimates
        SET count_revenue = true, updated_at = NOW()
        WHERE customer_id = %(cid)s
          AND estimate_type = 'treatment'
          AND status = 'approved'
          AND record_status IN ('active', 'option')
          AND count_revenue = false
    """, {'cid': customer_id})

    # 10d-2. Auto-fix: restore count_revenue on SENT treatment estimates with $1000+
    #        Sent estimates are pipeline value and should count even before approval.
    #        Some get false=count_revenue from prior placeholder state — this restores them.
    cur.execute("""
        UPDATE hcp_estimates
        SET count_revenue = true, updated_at = NOW()
        WHERE customer_id = %(cid)s
          AND estimate_type = 'treatment'
          AND status = 'sent'
          AND highest_option_cents >= 100000
          AND record_status IN ('active', 'option')
          AND count_revenue = false
    """, {'cid': customer_id})
    # 10. Auto-fix: set count_revenue = false on canceled segments
    cur.execute("""
        UPDATE hcp_job_segments
        SET count_revenue = false, updated_at = NOW()
        WHERE customer_id = %(cid)s
          AND status IN ('user canceled', 'pro canceled')
          AND count_revenue = true
    """, {'cid': customer_id})

    # 11. Auto-fix: set count_revenue = false on segments within 10% of parent amount
    cur.execute("""
        UPDATE hcp_job_segments s
        SET count_revenue = false, updated_at = NOW()
        FROM hcp_jobs j
        WHERE j.hcp_job_id = s.parent_hcp_job_id
          AND s.customer_id = %(cid)s
          AND s.count_revenue = true
          AND s.total_amount_cents > 0
          AND j.total_amount_cents > 0
          AND ABS(s.total_amount_cents - j.total_amount_cents) <= j.total_amount_cents * 0.1
    """, {'cid': customer_id})

    # 12. revenue_exceeds_estimate — REMOVED (upsells are normal, already in ROAS waterfall)

    # 13. segment_revenue_suspicious — segments with revenue > parent job
    cur.execute("""
        UPDATE hcp_customers hc
        SET exception_flags = array_append(
            COALESCE(hc.exception_flags, '{}'), 'segment_revenue_suspicious'
        ), updated_at = NOW()
        WHERE hc.customer_id = %(cid)s
          AND hc.callrail_id IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM hcp_jobs j
              WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active'
                AND COALESCE((SELECT SUM(s.total_amount_cents) FROM hcp_job_segments s
                    WHERE s.parent_hcp_job_id = j.hcp_job_id AND s.count_revenue = true
                      AND s.status NOT IN ('user canceled', 'pro canceled')), 0) > j.total_amount_cents
          )
          AND (hc.exception_flags IS NULL OR NOT 'segment_revenue_suspicious' = ANY(hc.exception_flags))
    """, {'cid': customer_id})

    # 14. approved_estimate_stale — REMOVED (business insight metric, not a data quality flag)

    # 15. invoice_below_estimate — treatment invoice < 50% of approved estimate
    cur.execute("""
        UPDATE hcp_customers hc
        SET exception_flags = array_append(
            COALESCE(hc.exception_flags, '{}'), 'invoice_below_estimate'
        ), updated_at = NOW()
        WHERE hc.customer_id = %(cid)s
          AND COALESCE((
              SELECT SUM(i.amount_cents) FROM hcp_invoices i
              WHERE i.hcp_customer_id = hc.hcp_customer_id
                AND i.invoice_type = 'treatment' AND i.status <> 'canceled'
          ), 0) > 0
          AND COALESCE((
              SELECT SUM(e.approved_total_cents) FROM hcp_estimates e
              WHERE e.hcp_customer_id = hc.hcp_customer_id
                AND e.status = 'approved' AND e.record_status = 'active'
          ), 0) > 0
          AND COALESCE((
              SELECT SUM(i.amount_cents) FROM hcp_invoices i
              WHERE i.hcp_customer_id = hc.hcp_customer_id
                AND i.invoice_type = 'treatment' AND i.status <> 'canceled'
          ), 0) < COALESCE((
              SELECT SUM(e.approved_total_cents) FROM hcp_estimates e
              WHERE e.hcp_customer_id = hc.hcp_customer_id
                AND e.status = 'approved' AND e.record_status = 'active'
          ), 0) * 0.5
          AND (hc.exception_flags IS NULL OR NOT 'invoice_below_estimate' = ANY(hc.exception_flags))
    """, {'cid': customer_id})

    # Count how many customers have any flags
    cur.execute("""
        SELECT COUNT(*) FROM hcp_customers
        WHERE customer_id = %(cid)s
          AND exception_flags IS NOT NULL
          AND array_length(exception_flags, 1) > 0
    """, {'cid': customer_id})
    return cur.fetchone()[0]

# ============================================================
# Main pull logic
# ============================================================


def upsert_job_line_items(cur, hcp_job_id, items):
    """Delete + re-insert line items for one job (matches hcp_invoice_items pattern)."""
    cur.execute(
        "DELETE FROM hcp_job_line_items WHERE hcp_job_id = %s",
        [hcp_job_id],
    )
    for item in items:
        cur.execute(
            """
            INSERT INTO hcp_job_line_items (
                hcp_job_id, hcp_item_id, name, description, kind,
                order_index, quantity_hundredths, unit_price_cents,
                unit_cost_cents, amount_cents, taxable
            ) VALUES (
                %(job_id)s, %(item_id)s, %(name)s, %(desc)s, %(kind)s,
                %(order)s, %(qty)s, %(price)s,
                %(cost)s, %(amount)s, %(taxable)s
            )
            """,
            {
                'job_id': hcp_job_id,
                'item_id': item.get('id'),
                'name': item.get('name'),
                'desc': item.get('description'),
                'kind': item.get('kind'),
                'order': item.get('order_index'),
                'qty': int((item.get('quantity') or 0) * 100),
                'price': item.get('unit_price') or 0,
                'cost': item.get('unit_cost') or 0,
                'amount': item.get('amount') or 0,
                'taxable': item.get('taxable'),
            },
        )


def resolve_unknown_jobs(cur, customer_id, api_key):
    """
    For every hcp_jobs row in this client with work_category='unknown',
    fetch line items from HCP API, store them, and re-classify the job.

    Returns the number of jobs that got a new (non-unknown) classification.
    """
    cur.execute(
        """
        SELECT j.hcp_job_id, j.description, j.tags, j.job_type,
               j.total_amount_cents, j.status, j.original_estimate_id,
               eo.name AS opt_name, eo.total_amount_cents AS opt_amount_cents,
               eo.tags AS opt_tags, eo.message_from_pro AS opt_message,
               eo.status AS opt_status
        FROM hcp_jobs j
        LEFT JOIN hcp_estimate_options eo ON eo.hcp_option_id = j.original_estimate_id
        WHERE j.customer_id = %s
          AND j.record_status = 'active'
          AND j.work_category = 'unknown'
        """,
        [customer_id],
    )
    rows = cur.fetchall()
    if not rows:
        return 0

    resolved = 0
    fetched = 0
    for row in rows:
        (hcp_job_id, desc, tags, hcp_job_type_field, amt, status,
         orig_est_id, opt_name, opt_amt, opt_tags, opt_msg, opt_status) = row

        # Fetch line items from HCP
        try:
            data = hcp_request(api_key, f'/jobs/{hcp_job_id}/line_items')
        except Exception as e:
            log.warning(f"  line items fetch failed for {hcp_job_id}: {e}")
            continue
        items = data.get('data') if isinstance(data, dict) else (data or [])
        upsert_job_line_items(cur, hcp_job_id, items or [])
        fetched += 1

        line_items_for_classifier = [
            {'name': it.get('name'), 'description': it.get('description'),
             'amount_cents': it.get('amount') or 0}
            for it in (items or [])
        ]

        linked_option = None
        if orig_est_id and opt_name is not None:
            linked_option = {
                'name': opt_name,
                'total_cents': opt_amt or 0,
                'tags': opt_tags or [],
                'message_from_pro': opt_msg,
                'status': opt_status,
            }

        result = classify_job(
            description=desc,
            tags=tags,
            hcp_job_type=hcp_job_type_field,
            line_items=line_items_for_classifier,
            total_cents=amt or 0,
            status=status,
            parent_job_classification=None,
            linked_estimate=None,
            linked_option=linked_option,
        )

        if result['category'] != 'unknown':
            resolved += 1

        cur.execute(
            """
            UPDATE hcp_jobs SET
              work_category     = %(category)s,
              review_needed     = %(review_needed)s,
              review_reason     = %(review_reason)s,
              classifier_signal = %(signal)s,
              classified_at     = NOW()
            WHERE hcp_job_id = %(job_id)s
            """,
            {
                'job_id': hcp_job_id,
                'category': result['category'],
                'review_needed': result['review_needed'],
                'review_reason': result['review_reason'],
                'signal': result['signal'],
            },
        )

    if fetched:
        log.info(f"  Fetched line items for {fetched} unknown jobs, resolved {resolved}")
    return resolved


def pull_client(conn, customer_id, api_key, backfill=False):
    """Pull all HCP data for a single client."""
    start_ms = time.time()
    stats = {
        'customers_fetched': 0, 'customers_upserted': 0,
        'inspections_upserted': 0, 'estimates_upserted': 0,
        'jobs_upserted': 0, 'segments_upserted': 0,
        'invoices_upserted': 0, 'matches_found': 0,
        'errors': [],
    }

    log.info(f"Pulling HCP data for client {customer_id}")

    with conn.cursor() as cur:
        # ── 1. Pull customers (always full — no date filter available) ──
        try:
            customers = hcp_paginate(api_key, '/customers')
            stats['customers_fetched'] = len(customers)
            log.info(f"  Fetched {len(customers)} customers")

            # Determine if this is a large account
            is_large = len(customers) >= LARGE_ACCOUNT_THRESHOLD and not backfill
            date_params = {}
            if is_large:
                cutoff = (datetime.now(timezone.utc) - timedelta(days=LARGE_ACCOUNT_LOOKBACK_DAYS)).strftime('%Y-%m-%d')
                date_params = {'scheduled_start_min': cutoff}
                log.info(f"  Large account ({len(customers)} customers) — filtering jobs/estimates/invoices to {cutoff}+")

            for c in customers:
                try:
                    cur.execute("SAVEPOINT cust_sp")
                    upsert_customer(cur, customer_id, c)
                    stats['customers_upserted'] += 1
                    cur.execute("RELEASE SAVEPOINT cust_sp")
                except Exception as e:
                    cur.execute("ROLLBACK TO SAVEPOINT cust_sp")
                    stats['errors'].append(f"customer {c.get('id')}: {e}")
                    log.warning(f"  Error upserting customer {c.get('id')}: {e}")

            conn.commit()
        except Exception as e:
            conn.rollback()
            stats['errors'].append(f"customers fetch: {e}")
            log.error(f"  Error fetching customers: {e}")

        # ── 2. Pull estimates → classify into inspections vs estimate documents ──
        try:
            # Always pull ALL estimates — no date filter. Estimates are critical for ROAS
            # and scheduled_start_min filters by inspection date, not estimate creation date
            estimates = hcp_paginate(api_key, '/estimates')
            log.info(f"  Fetched {len(estimates)} estimates")

            for est in estimates:
                try:
                    hcp_customer_id = None
                    cust_ref = est.get('customer')
                    if isinstance(cust_ref, dict):
                        hcp_customer_id = cust_ref.get('id')
                    elif isinstance(cust_ref, str):
                        hcp_customer_id = cust_ref

                    # Validate customer exists in our DB (may be deleted in HCP)
                    hcp_customer_id = validate_hcp_customer_id(cur, hcp_customer_id)

                    cur.execute("SAVEPOINT est_sp")
                    est_data = {
                        'id': est.get('id'),
                        'hcp_estimate_id': est.get('id'),
                        'sent_at': est.get('sent_at') or est.get('created_at'),
                        'approved_at': est.get('approved_at'),
                        'created_at': est.get('created_at'),
                        'options': est.get('options', []),
                        'assigned_employees': est.get('assigned_employees', []),
                    }
                    upsert_estimate(cur, customer_id, hcp_customer_id, est_data)
                    stats['estimates_upserted'] += 1

                    # If estimate has a scheduled appointment, it's also an inspection
                    schedule = est.get('schedule') or {}
                    if isinstance(schedule, dict) and schedule.get('scheduled_start'):
                        insp_data = {
                            'hcp_id': est.get('id'),
                            'source_event': 'estimate.scheduled',
                            'status': est.get('work_status', 'scheduled'),
                            'scheduled_at': schedule.get('scheduled_start'),
                            'completed_at': (est.get('work_timestamps') or {}).get('completed_at') or est.get('completed_at'),
                            'created_at': est.get('created_at'),
                            'total_amount_cents': est.get('total_amount', 0) or 0,
                            'description': est.get('description') or est.get('name'),
                            'service_address': extract_address(est.get('address')),
                        }
                        assigned = est.get('assigned_employee') or est.get('dispatched_employee')
                        if not assigned:
                            employees = est.get('assigned_employees', [])
                            if employees and isinstance(employees, list):
                                assigned = employees[0]
                        if assigned:
                            insp_data['employee_name'] = assigned.get('name') or f"{assigned.get('first_name', '')} {assigned.get('last_name', '')}".strip()
                            insp_data['employee_id'] = assigned.get('id')

                        upsert_inspection(cur, customer_id, hcp_customer_id, insp_data)
                        stats['inspections_upserted'] += 1

                    cur.execute("RELEASE SAVEPOINT est_sp")
                except Exception as e:
                    cur.execute("ROLLBACK TO SAVEPOINT est_sp")
                    stats['errors'].append(f"estimate {est.get('id')}: {e}")
                    log.warning(f"  Error processing estimate {est.get('id')}: {e}")

            conn.commit()
        except Exception as e:
            conn.rollback()
            stats['errors'].append(f"estimates fetch: {e}")
            log.error(f"  Error fetching estimates: {e}")

        # ── 3. Pull jobs → classify as job or inspection, separate segments ──
        try:
            jobs = hcp_paginate(api_key, '/jobs', params=date_params if date_params else None)
            # Large accounts: also fetch recently modified jobs to catch status changes
            # (e.g., "pro canceled" → "Completed") that the scheduled_start_min filter misses
            if is_large:
                mod_cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%d')
                recent_jobs = hcp_paginate(api_key, '/jobs', params={'work_date_min': mod_cutoff})
                # Merge: add any jobs not already in the list
                existing_ids = {j.get('id') for j in jobs}
                new_jobs = [j for j in recent_jobs if j.get('id') not in existing_ids]
                jobs.extend(new_jobs)
                if new_jobs:
                    log.info(f"  Large account: fetched {len(new_jobs)} additional recently-modified jobs")
            log.info(f"  Fetched {len(jobs)} jobs total")

            # Separate parent jobs from segments
            parent_jobs = []
            segment_jobs = []
            for job in jobs:
                inv = job.get('invoice_number') or ''
                if '-' in inv:
                    segment_jobs.append(job)
                else:
                    parent_jobs.append(job)

            # Build invoice_number → job_id lookup for parent resolution
            inv_to_job_id = {}

            # Pass 1: Process parent jobs (non-segments)
            for job in parent_jobs:
                try:
                    hcp_customer_id = None
                    cust_ref = job.get('customer')
                    if isinstance(cust_ref, dict):
                        hcp_customer_id = cust_ref.get('id')
                    elif isinstance(cust_ref, str):
                        hcp_customer_id = cust_ref

                    # Track invoice → job_id for segment parent lookup
                    inv = job.get('invoice_number') or ''
                    if inv:
                        inv_to_job_id[inv] = job.get('id')

                    # Validate customer exists in our DB
                    hcp_customer_id = validate_hcp_customer_id(cur, hcp_customer_id)

                    cur.execute("SAVEPOINT job_sp")

                    description = job.get('description') or job.get('name') or ''
                    original_estimate_id = job.get('original_estimate_id')
                    total_amount = job.get('total_amount_cents', 0) or job.get('total_amount', 0) or 0

                    # Append job_type to description for keyword classification
                    job_type_name = ((job.get('job_fields') or {}).get('job_type') or {}).get('name') or ''
                    classify_text = (description + ' ' + job_type_name).strip()
                    classification = classify_job_or_inspection(
                        classify_text, original_estimate_id, total_amount
                    )

                    if classification == 'inspection':
                        schedule = job.get('schedule') or {}
                        scheduled_start = None
                        if isinstance(schedule, dict):
                            scheduled_start = schedule.get('scheduled_start')
                        elif isinstance(job.get('scheduled_start'), str):
                            scheduled_start = job.get('scheduled_start')

                        insp_data = {
                            'hcp_id': job.get('id'),
                            'source_event': 'job.scheduled',
                            'status': job.get('work_status', 'scheduled'),
                            'scheduled_at': scheduled_start,
                            'completed_at': job.get('completed_at'),
                            'created_at': job.get('created_at'),
                            'total_amount_cents': total_amount,
                            'description': description,
                            'service_address': extract_address(job.get('address')),
                        }
                        assigned = job.get('assigned_employee') or job.get('dispatched_employee')
                        if not assigned:
                            employees = job.get('assigned_employees', [])
                            if employees and isinstance(employees, list):
                                assigned = employees[0]
                        if assigned:
                            insp_data['employee_name'] = assigned.get('name') or f"{assigned.get('first_name', '')} {assigned.get('last_name', '')}".strip()
                            insp_data['employee_id'] = assigned.get('id')

                        upsert_inspection(cur, customer_id, hcp_customer_id, insp_data)
                        stats['inspections_upserted'] += 1
                    else:
                        upsert_job(cur, customer_id, hcp_customer_id, job)
                        stats['jobs_upserted'] += 1

                    cur.execute("RELEASE SAVEPOINT job_sp")
                except Exception as e:
                    cur.execute("ROLLBACK TO SAVEPOINT job_sp")
                    stats['errors'].append(f"job {job.get('id')}: {e}")
                    log.warning(f"  Error processing job {job.get('id')}: {e}")

            conn.commit()

            # Pass 2: Process segments — link to parent by base invoice number
            for job in segment_jobs:
                try:
                    hcp_customer_id = None
                    cust_ref = job.get('customer')
                    if isinstance(cust_ref, dict):
                        hcp_customer_id = cust_ref.get('id')
                    elif isinstance(cust_ref, str):
                        hcp_customer_id = cust_ref

                    hcp_customer_id = validate_hcp_customer_id(cur, hcp_customer_id)

                    inv = job.get('invoice_number') or ''
                    base_inv = inv.split('-')[0]

                    # Resolve parent job ID
                    parent_job_id = inv_to_job_id.get(base_inv)
                    if not parent_job_id:
                        cur.execute(
                            "SELECT hcp_job_id FROM hcp_jobs WHERE customer_id = %s AND invoice_number = %s",
                            [customer_id, base_inv]
                        )
                        row = cur.fetchone()
                        parent_job_id = row[0] if row else f"unknown_{base_inv}"

                    cur.execute("SAVEPOINT seg_sp")
                    upsert_job_segment(cur, customer_id, hcp_customer_id, job, parent_job_id)
                    stats['segments_upserted'] += 1
                    cur.execute("RELEASE SAVEPOINT seg_sp")
                except Exception as e:
                    cur.execute("ROLLBACK TO SAVEPOINT seg_sp")
                    stats['errors'].append(f"segment {job.get('id')}: {e}")
                    log.warning(f"  Error processing segment {job.get('id')}: {e}")

            conn.commit()
            log.info(f"  Processed {stats['jobs_upserted']} jobs, {stats['inspections_upserted']} inspections, {stats['segments_upserted']} segments")

        except Exception as e:
            conn.rollback()
            stats['errors'].append(f"jobs fetch: {e}")
            log.error(f"  Error fetching jobs: {e}")

        # ── 4. Pull invoices ──
        try:
            inv_params = dict(date_params) if date_params else {}
            invoices = hcp_paginate(api_key, '/invoices', params=inv_params if inv_params else None, key='invoices')
            log.info(f"  Fetched {len(invoices)} invoices")

            # Group by job_id and assign sequence numbers (by invoice_date)
            inv_by_job = defaultdict(list)
            for inv in invoices:
                jid = inv.get('job_id')
                if jid:
                    inv_by_job[jid].append(inv)

            for jid, job_invoices in inv_by_job.items():
                job_invoices.sort(key=lambda x: x.get('invoice_date') or x.get('sent_at') or '')
                for seq, inv in enumerate(job_invoices, 1):
                    inv['_sequence'] = seq

            # Also handle invoices with no job_id
            for inv in invoices:
                if not inv.get('job_id'):
                    inv['_sequence'] = 1

            for inv in invoices:
                try:
                    cur.execute("SAVEPOINT inv_sp")
                    upsert_invoice(cur, customer_id, inv)
                    stats['invoices_upserted'] += 1
                    cur.execute("RELEASE SAVEPOINT inv_sp")
                except Exception as e:
                    cur.execute("ROLLBACK TO SAVEPOINT inv_sp")
                    stats['errors'].append(f"invoice {inv.get('id')}: {e}")
                    log.warning(f"  Error upserting invoice {inv.get('id')}: {e}")

            conn.commit()
        except Exception as e:
            conn.rollback()
            stats['errors'].append(f"invoices fetch: {e}")
            log.error(f"  Error fetching invoices: {e}")

        # ── 5. Match to CallRail by phone ──
        try:
            matches = match_callrail(cur, customer_id)
            stats['matches_found'] = matches
            conn.commit()
            log.info(f"  Matched {matches} customers to CallRail leads")
        except Exception as e:
            conn.rollback()
            stats['errors'].append(f"callrail matching: {e}")
            log.error(f"  Error matching CallRail: {e}")

        # ── 6. Detect exceptions for review queue ──
        try:
            exceptions = detect_exceptions(cur, customer_id)
            conn.commit()
            if exceptions:
                log.info(f"  {exceptions} customers with exception flags")
        except Exception as e:
            conn.rollback()
            stats['errors'].append(f"exception detection: {e}")
            log.error(f"  Error detecting exceptions: {e}")

        # ── 7. Resolve unknown-classified jobs via line item pull ──
        try:
            resolve_unknown_jobs(cur, customer_id, api_key)
            conn.commit()
        except Exception as e:
            conn.rollback()
            stats['errors'].append(f"resolve unknown jobs: {e}")
            log.error(f"  Error resolving unknowns: {e}")

    # Log the run
    duration_ms = int((time.time() - start_ms) * 1000)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO hcp_pull_log (
                customer_id, customers_fetched, customers_upserted,
                inspections_upserted, estimates_upserted, jobs_upserted,
                invoices_upserted, matches_found, errors, duration_ms
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, [
            customer_id,
            stats['customers_fetched'], stats['customers_upserted'],
            stats['inspections_upserted'], stats['estimates_upserted'],
            stats['jobs_upserted'], stats['invoices_upserted'],
            stats['matches_found'],
            stats['errors'] or None, duration_ms,
        ])
        conn.commit()

    log.info(
        f"  Done: {stats['customers_upserted']} customers, "
        f"{stats['inspections_upserted']} inspections, "
        f"{stats['estimates_upserted']} estimates, "
        f"{stats['jobs_upserted']} jobs, "
        f"{stats['segments_upserted']} segments, "
        f"{stats['invoices_upserted']} invoices, "
        f"{stats['matches_found']} matches | "
        f"{duration_ms}ms"
    )

    if stats['errors']:
        log.warning(f"  {len(stats['errors'])} error(s)")

    return stats

# ============================================================
# Entry point
# ============================================================

def main():
    parser = argparse.ArgumentParser(description='Pull HCP data into PostgreSQL')
    parser.add_argument('--client', type=str, help='Pull only this customer_id')
    parser.add_argument('--backfill', action='store_true', help='Full historical pull (ignore large account limits)')
    parser.add_argument('--verify', action='store_true', help='Verify API keys match expected clients, then exit')
    args = parser.parse_args()

    conn = psycopg2.connect(DSN)
    conn.autocommit = False

    try:
        # Get HCP clients
        with conn.cursor() as cur:
            if args.client:
                cur.execute("""
                    SELECT customer_id, hcp_api_key, name FROM clients
                    WHERE customer_id = %s
                      AND hcp_api_key IS NOT NULL
                      AND field_management_software = 'housecall_pro'
                """, [args.client])
            else:
                cur.execute("""
                    SELECT customer_id, hcp_api_key, name FROM clients
                    WHERE hcp_api_key IS NOT NULL
                      AND field_management_software = 'housecall_pro'
                      AND status = 'active'
                    ORDER BY (SELECT MAX(synced_at) FROM hcp_estimates e WHERE e.customer_id = clients.customer_id) NULLS FIRST
                """)
            clients = cur.fetchall()

        if not clients:
            log.warning("No HCP clients found. Check clients.hcp_api_key and field_management_software.")
            return

        log.info(f"Found {len(clients)} HCP client(s)")

        # Verify mode: check API keys match expected clients
        if args.verify:
            log.info("Verifying API keys...")
            mismatches = 0
            for customer_id, api_key, name in clients:
                try:
                    data = hcp_request(api_key, '/customers', {'page_size': 1})
                    total = data.get('total_items', '?')
                    company = ''
                    custs = data.get('customers', [])
                    if custs:
                        company = custs[0].get('company_name', '')
                    large = ' ** LARGE **' if isinstance(total, int) and total >= LARGE_ACCOUNT_THRESHOLD else ''
                    log.info(f"  OK  {customer_id} | DB: {name[:40]} | HCP: {company[:40]} | {total} customers{large}")
                except Exception as e:
                    log.error(f"  FAIL {customer_id} | DB: {name[:40]} | {e}")
                    mismatches += 1
                time.sleep(RATE_LIMIT_DELAY)
            log.info(f"Verification complete. {mismatches} failures.")
            return

        total_errors = 0
        for customer_id, api_key, name in clients:
            try:
                log.info(f"--- {name} ---")
                stats = pull_client(conn, customer_id, api_key, backfill=args.backfill)
                total_errors += len(stats.get('errors', []))
            except Exception as e:
                log.error(f"Fatal error for client {customer_id}: {e}")
                total_errors += 1
                conn.rollback()

            # Rate limit between clients
            time.sleep(1)

        # Auto-group same-address items within 90-day windows
        if not args.verify:
            log.info('Running auto-grouping...')
            try:
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute('SELECT COUNT(*), COALESCE(SUM(out_nested), 0) FROM auto_group_inspections()')
                    ig, in_ = cur.fetchone()
                    cur.execute('SELECT COUNT(*), COALESCE(SUM(out_nested), 0) FROM auto_group_jobs()')
                    jg, jn = cur.fetchone()
                    cur.execute('SELECT COUNT(*), COALESCE(SUM(out_nested), 0) FROM auto_group_estimates()')
                    eg, en = cur.fetchone()
                    log.info(f'  Auto-grouped: {in_} inspections ({ig} customers), {jn} jobs ({jg} customers), {en} estimates ({eg} customers)')
                conn.autocommit = False
            except Exception as e:
                log.error(f'  Auto-grouping error: {e}')
                try:
                    conn.autocommit = False
                except:
                    pass

        log.info(f"All done. {total_errors} total error(s).")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
