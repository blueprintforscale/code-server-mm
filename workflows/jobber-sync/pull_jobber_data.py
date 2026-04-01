#!/usr/bin/env python3
"""
Jobber ETL — Pull clients, quotes, jobs, invoices from Jobber GraphQL API.

Usage:
  python3 pull_jobber_data.py                    # All Jobber clients
  python3 pull_jobber_data.py --client 7123434733  # Single client by customer_id

Mirrors the HCP ETL structure. Runs on Mac Mini, stores in Postgres.
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import urllib.request
import urllib.error

# ── Database ──────────────────────────────────────────────────

DB_CONFIG = {
    "dbname": "blueprint",
    "user": "blueprint",
    "host": "localhost",
    "port": 5432,
}

JOBBER_API_URL = "https://api.getjobber.com/api/graphql"
JOBBER_TOKEN_URL = "https://api.getjobber.com/api/oauth/token"
JOBBER_GRAPHQL_VERSION = "2025-01-20"

# Read OAuth client creds from env or hardcode (set during deploy)
import os
JOBBER_CLIENT_ID = os.environ.get("JOBBER_CLIENT_ID", "e46bcd1e-04a1-4770-bd86-88cf4abd9f35")
JOBBER_CLIENT_SECRET = os.environ.get("JOBBER_CLIENT_SECRET", "204ba95f7ec0f79a236112e4e475c1d22e8910689cd085573eade9d9009c2f62")


def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def normalize_phone(phone):
    """Strip non-digits, take last 10 digits."""
    if not phone:
        return None
    digits = re.sub(r'\D', '', phone)
    if len(digits) >= 10:
        return digits[-10:]
    return digits if digits else None


def cents_from_amount(amount_str):
    """Convert Jobber amount string (dollars) to cents integer."""
    if amount_str is None:
        return 0
    try:
        return int(round(float(amount_str) * 100))
    except (ValueError, TypeError):
        return 0


# ── Jobber API ────────────────────────────────────────────────

def refresh_token(conn, client_row):
    """Refresh an expired Jobber access token using the refresh token."""
    refresh = client_row["jobber_refresh_token"]
    if not refresh:
        raise Exception(f"No refresh token for {client_row['name']}")

    payload = json.dumps({
        "client_id": JOBBER_CLIENT_ID,
        "client_secret": JOBBER_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": refresh,
    }).encode()

    req = urllib.request.Request(
        JOBBER_TOKEN_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req) as resp:
        tokens = json.loads(resp.read())

    # Update tokens in DB
    cur = conn.cursor()
    cur.execute("""
        UPDATE clients SET
            jobber_access_token = %s,
            jobber_refresh_token = %s,
            jobber_token_expires_at = NOW() + INTERVAL '60 minutes',
            updated_at = NOW()
        WHERE customer_id = %s
    """, (tokens["access_token"], tokens["refresh_token"], client_row["customer_id"]))
    conn.commit()

    print(f"  [Token] Refreshed for {client_row['name']}")
    return tokens["access_token"]


def graphql_query(access_token, query, variables=None, retries=5):
    """Execute a Jobber GraphQL query. Auto-handles rate limiting."""
    payload = json.dumps({"query": query, "variables": variables or {}}).encode()

    req = urllib.request.Request(
        JOBBER_API_URL,
        data=payload,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "X-JOBBER-GRAPHQL-VERSION": JOBBER_GRAPHQL_VERSION,
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 429 and retries > 0:
            print(f"    [Rate limit] 429 — waiting 15s...")
            time.sleep(15)
            return graphql_query(access_token, query, variables, retries - 1)
        if e.code == 401 and retries > 0:
            raise Exception("TOKEN_EXPIRED")
        body = e.read().decode() if hasattr(e, 'read') else str(e)
        raise Exception(f"Jobber API error {e.code}: {body}")

    if "errors" in data:
        err_msg = data["errors"][0].get("message", str(data["errors"]))
        if "unauthorized" in err_msg.lower() or "authentication" in err_msg.lower():
            raise Exception("TOKEN_EXPIRED")
        if "throttle" in err_msg.lower() and retries > 0:
            print(f"    [Rate limit] Throttled — waiting 15s...")
            time.sleep(15)
            return graphql_query(access_token, query, variables, retries - 1)
        raise Exception(f"GraphQL error: {err_msg}")

    # Check rate limit budget
    cost = data.get("extensions", {}).get("cost", {})
    available = cost.get("currentlyAvailable", 10000)
    if available < 500:
        wait = max(2, int((500 - available) / cost.get("restoreRate", 500)) + 1)
        print(f"    [Rate limit] Low budget ({available}), waiting {wait}s...")
        time.sleep(wait)

    return data.get("data", {})


def fetch_all_pages(access_token, query, root_field, page_size=25):
    """Paginate through all results for a Jobber GraphQL query."""
    all_nodes = []
    cursor = None
    has_next = True

    while has_next:
        variables = {"first": page_size, "after": cursor}
        data = graphql_query(access_token, query, variables)

        connection = data.get(root_field, {})
        nodes = connection.get("nodes", [])
        all_nodes.extend(nodes)

        page_info = connection.get("pageInfo", {})
        has_next = page_info.get("hasNextPage", False)
        cursor = page_info.get("endCursor")

        if nodes:
            total = connection.get("totalCount", "?")
            print(f"    Fetched {len(all_nodes)}/{total}...")

        time.sleep(0.2)  # Be gentle

    return all_nodes


# ── GraphQL Queries ───────────────────────────────────────────

CLIENTS_QUERY = """
query GetClients($first: Int!, $after: String) {
  clients(first: $first, after: $after) {
    nodes {
      id
      firstName
      lastName
      companyName
      isCompany
      isLead
      isArchived
      phones { number description primary }
      emails { address description primary }
      billingAddress {
        street1
        street2
        city
        province
        postalCode
        country
      }
      balance
      createdAt
      updatedAt
    }
    pageInfo { endCursor hasNextPage }
    totalCount
  }
}
"""

QUOTES_QUERY = """
query GetQuotes($first: Int!, $after: String) {
  quotes(first: $first, after: $after) {
    nodes {
      id
      quoteNumber
      quoteStatus
      title
      amounts { subtotal total discountAmount depositAmount }
      client { id }
      createdAt
      updatedAt
    }
    pageInfo { endCursor hasNextPage }
    totalCount
  }
}
"""

JOBS_QUERY = """
query GetJobs($first: Int!, $after: String) {
  jobs(first: $first, after: $after) {
    nodes {
      id
      jobNumber
      jobStatus
      title
      instructions
      client { id }
      quote { id }
      createdAt
      updatedAt
      visits(first: 50) {
        nodes {
          startAt
          endAt
          title
        }
      }
    }
    pageInfo { endCursor hasNextPage }
    totalCount
  }
}
"""

INVOICES_QUERY = """
query GetInvoices($first: Int!, $after: String) {
  invoices(first: $first, after: $after) {
    nodes {
      id
      invoiceNumber
      subject
      invoiceStatus
      amounts { subtotal total discountAmount depositAmount }
      client { id }
      createdAt
      updatedAt
      dueDate
    }
    pageInfo { endCursor hasNextPage }
    totalCount
  }
}
"""


REQUESTS_QUERY = """
query GetRequests($first: Int!, $after: String) {
  requests(first: $first, after: $after) {
    nodes {
      id
      title
      createdAt
      client { id name }
      assessment {
        id
        startAt
        endAt
        completedAt
      }
    }
    pageInfo { endCursor hasNextPage }
    totalCount
  }
}
"""


# ── Upsert Functions ─────────────────────────────────────────

def upsert_requests(conn, customer_id, requests):
    """Upsert Jobber requests into jobber_requests."""
    cur = conn.cursor()
    count = 0

    for r in requests:
        jobber_id = r["id"]
        client_node = r.get("client") or {}
        assessment = r.get("assessment")

        try:
            cur.execute("SAVEPOINT sp")
            cur.execute("""
                INSERT INTO jobber_requests (
                    jobber_request_id, customer_id, jobber_customer_id,
                    title, created_at, has_assessment,
                    assessment_id, assessment_start_at, assessment_end_at, assessment_completed_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (jobber_request_id, customer_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    jobber_customer_id = EXCLUDED.jobber_customer_id,
                    has_assessment = EXCLUDED.has_assessment,
                    assessment_id = EXCLUDED.assessment_id,
                    assessment_start_at = EXCLUDED.assessment_start_at,
                    assessment_end_at = EXCLUDED.assessment_end_at,
                    assessment_completed_at = EXCLUDED.assessment_completed_at,
                    updated_at = NOW()
            """, [
                jobber_id, customer_id, client_node.get("id"),
                r.get("title"), r.get("createdAt"),
                assessment is not None,
                assessment.get("id") if assessment else None,
                assessment.get("startAt") if assessment else None,
                assessment.get("endAt") if assessment else None,
                assessment.get("completedAt") if assessment else None,
            ])
            cur.execute("RELEASE SAVEPOINT sp")
            count += 1
        except Exception as e:
            cur.execute("ROLLBACK TO SAVEPOINT sp")

    conn.commit()
    return count


def upsert_customers(conn, customer_id, customers):
    """Upsert Jobber customers into jobber_customers."""
    cur = conn.cursor()
    count = 0

    for c in customers:
        jobber_id = c["id"]

        # Extract primary phone
        phones = c.get("phones") or []
        primary_phone = None
        for p in phones:
            if p.get("primary"):
                primary_phone = p.get("number")
                break
        if not primary_phone and phones:
            primary_phone = phones[0].get("number")

        phone_norm = normalize_phone(primary_phone)

        # Extract primary email
        emails = c.get("emails") or []
        primary_email = None
        for e in emails:
            if e.get("primary"):
                primary_email = e.get("address")
                break
        if not primary_email and emails:
            primary_email = emails[0].get("address")

        addr = c.get("billingAddress") or {}

        cur.execute("""
            INSERT INTO jobber_customers (
                jobber_customer_id, customer_id, first_name, last_name,
                company_name, email, phone_primary, phone_normalized, phones,
                street, city, province, postal_code, country,
                balance, is_company, is_lead, is_archived,
                jobber_created_at, jobber_updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (jobber_customer_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                company_name = EXCLUDED.company_name,
                email = EXCLUDED.email,
                phone_primary = EXCLUDED.phone_primary,
                phone_normalized = EXCLUDED.phone_normalized,
                phones = EXCLUDED.phones,
                street = EXCLUDED.street,
                city = EXCLUDED.city,
                province = EXCLUDED.province,
                postal_code = EXCLUDED.postal_code,
                balance = EXCLUDED.balance,
                is_company = EXCLUDED.is_company,
                is_lead = EXCLUDED.is_lead,
                is_archived = EXCLUDED.is_archived,
                jobber_updated_at = EXCLUDED.jobber_updated_at,
                updated_at = NOW()
        """, (
            jobber_id, customer_id,
            c.get("firstName"), c.get("lastName"),
            c.get("companyName"),
            primary_email, primary_phone, phone_norm,
            json.dumps(phones) if phones else None,
            addr.get("street1"), addr.get("city"),
            addr.get("province"), addr.get("postalCode"),
            addr.get("country"),
            c.get("balance"), c.get("isCompany", False),
            c.get("isLead", False), c.get("isArchived", False),
            c.get("createdAt"), c.get("updatedAt"),
        ))
        count += 1

    conn.commit()
    return count


def upsert_quotes(conn, customer_id, quotes, customer_map):
    """Upsert Jobber quotes into jobber_quotes + jobber_quote_items."""
    cur = conn.cursor()
    count = 0

    for q in quotes:
        jobber_id = q["id"]
        client_jobber_id = q.get("client", {}).get("id") if q.get("client") else None
        amounts = q.get("amounts") or {}

        # Determine approved_at: if status is APPROVED, use updatedAt
        status = q.get("quoteStatus")
        approved_at = q.get("updatedAt") if status == "APPROVED" else None

        cur.execute("""
            INSERT INTO jobber_quotes (
                jobber_quote_id, customer_id, jobber_customer_id,
                quote_number, status, title,
                total_cents, subtotal_cents, discount_cents, deposit_cents,
                jobber_created_at, jobber_updated_at, approved_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (jobber_quote_id) DO UPDATE SET
                status = EXCLUDED.status,
                title = EXCLUDED.title,
                total_cents = EXCLUDED.total_cents,
                subtotal_cents = EXCLUDED.subtotal_cents,
                discount_cents = EXCLUDED.discount_cents,
                deposit_cents = EXCLUDED.deposit_cents,
                jobber_updated_at = EXCLUDED.jobber_updated_at,
                approved_at = COALESCE(EXCLUDED.approved_at, jobber_quotes.approved_at),
                updated_at = NOW()
        """, (
            jobber_id, customer_id, client_jobber_id,
            q.get("quoteNumber"), status, q.get("title"),
            cents_from_amount(amounts.get("total")),
            cents_from_amount(amounts.get("subtotal")),
            cents_from_amount(amounts.get("discountAmount")),
            cents_from_amount(amounts.get("depositAmount")),
            q.get("createdAt"), q.get("updatedAt"), approved_at,
        ))

        # Upsert line items (delete + re-insert)
        cur.execute("DELETE FROM jobber_quote_items WHERE jobber_quote_id = %s", (jobber_id,))
        for item in (q.get("lineItems", {}).get("nodes") or []):
            cur.execute("""
                INSERT INTO jobber_quote_items (
                    jobber_quote_id, name, description, quantity,
                    unit_price_cents, total_price_cents
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                jobber_id, item.get("name"), item.get("description"),
                item.get("quantity"),
                cents_from_amount(item.get("unitPrice")),
                cents_from_amount(item.get("totalPrice")),
            ))

        count += 1

    conn.commit()
    return count


def upsert_jobs(conn, customer_id, jobs, customer_map):
    """Upsert Jobber jobs into jobber_jobs + jobber_visits."""
    cur = conn.cursor()
    count = 0

    for j in jobs:
        jobber_id = j["id"]
        client_jobber_id = j.get("client", {}).get("id") if j.get("client") else None
        quote_id = j.get("quote", {}).get("id") if j.get("quote") else None

        status = j.get("jobStatus")
        completed_at = j.get("updatedAt") if status == "COMPLETE" else None

        # Calculate total from line items if available
        line_items = j.get("lineItems", {}).get("nodes") or []
        total_cents = sum(cents_from_amount(li.get("totalPrice")) for li in line_items)
        subtotal_cents = total_cents

        cur.execute("""
            INSERT INTO jobber_jobs (
                jobber_job_id, customer_id, jobber_customer_id, jobber_quote_id,
                job_number, status, title, instructions,
                total_cents, subtotal_cents,
                jobber_created_at, jobber_updated_at, completed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (jobber_job_id) DO UPDATE SET
                status = EXCLUDED.status,
                title = EXCLUDED.title,
                instructions = EXCLUDED.instructions,
                total_cents = EXCLUDED.total_cents,
                subtotal_cents = EXCLUDED.subtotal_cents,
                jobber_updated_at = EXCLUDED.jobber_updated_at,
                completed_at = COALESCE(EXCLUDED.completed_at, jobber_jobs.completed_at),
                updated_at = NOW()
        """, (
            jobber_id, customer_id, client_jobber_id, quote_id,
            j.get("jobNumber"), status, j.get("title"), j.get("instructions"),
            total_cents, subtotal_cents,
            j.get("createdAt"), j.get("updatedAt"), completed_at,
        ))

        # Upsert visits (delete + re-insert)
        cur.execute("DELETE FROM jobber_visits WHERE jobber_job_id = %s", (jobber_id,))
        for v in (j.get("visits", {}).get("nodes") or []):
            cur.execute("""
                INSERT INTO jobber_visits (
                    jobber_job_id, start_at, end_at, title
                ) VALUES (%s, %s, %s, %s)
            """, (
                jobber_id, v.get("startAt"), v.get("endAt"),
                v.get("title"),
            ))

        count += 1

    conn.commit()
    return count


def upsert_invoices(conn, customer_id, invoices, customer_map):
    """Upsert Jobber invoices into jobber_invoices + jobber_invoice_items."""
    cur = conn.cursor()
    count = 0

    for inv in invoices:
        jobber_id = inv["id"]
        client_jobber_id = inv.get("client", {}).get("id") if inv.get("client") else None
        amounts = inv.get("amounts") or {}

        cur.execute("""
            INSERT INTO jobber_invoices (
                jobber_invoice_id, customer_id, jobber_customer_id,
                invoice_number, subject, status,
                total_cents, subtotal_cents, due_cents,
                discount_cents, deposit_cents, tax_cents, payments_total_cents,
                invoice_date, due_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (jobber_invoice_id) DO UPDATE SET
                status = EXCLUDED.status,
                total_cents = EXCLUDED.total_cents,
                subtotal_cents = EXCLUDED.subtotal_cents,
                due_cents = EXCLUDED.due_cents,
                discount_cents = EXCLUDED.discount_cents,
                payments_total_cents = EXCLUDED.payments_total_cents,
                due_date = EXCLUDED.due_date,
                updated_at = NOW()
        """, (
            jobber_id, customer_id, client_jobber_id,
            inv.get("invoiceNumber"), inv.get("subject"), inv.get("invoiceStatus"),
            cents_from_amount(amounts.get("total")),
            cents_from_amount(amounts.get("subtotal")),
            0,  # due_cents — not available in this API version
            cents_from_amount(amounts.get("discountAmount")),
            cents_from_amount(amounts.get("depositAmount")),
            0,  # tax_cents
            0,  # payments_total_cents
            inv.get("createdAt"), inv.get("dueDate"),
        ))

        # Upsert line items (delete + re-insert)
        cur.execute("DELETE FROM jobber_invoice_items WHERE jobber_invoice_id = %s", (jobber_id,))
        for item in (inv.get("lineItems", {}).get("nodes") or []):
            cur.execute("""
                INSERT INTO jobber_invoice_items (
                    jobber_invoice_id, name, description, quantity,
                    unit_price_cents, total_price_cents
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                jobber_id, item.get("name"), item.get("description"),
                item.get("quantity"),
                cents_from_amount(item.get("unitPrice")),
                cents_from_amount(item.get("totalPrice")),
            ))

        count += 1

    conn.commit()
    return count


def match_callrail(conn, customer_id):
    """Match Jobber customers to CallRail leads by phone/email.
    Step 1: Phone match via calls
    Step 2: Email match via form_submissions
    Step 3: Phone match via form_submissions
    Step 4: Phone match via webflow_submissions (GCLID required)
    """
    cur = conn.cursor()

    # Step 1: Phone match via calls
    cur.execute("""
        UPDATE jobber_customers jc
        SET callrail_id = c.callrail_id,
            match_method = 'phone_normalized'
        FROM calls c
        WHERE jc.customer_id = %s
          AND jc.callrail_id IS NULL
          AND jc.phone_normalized IS NOT NULL
          AND jc.phone_normalized != ''
          AND normalize_phone(c.caller_phone) = jc.phone_normalized
          AND c.callrail_company_id = (
              SELECT callrail_company_id FROM clients WHERE customer_id = %s
          )
    """, (customer_id, customer_id))
    phone_matches = cur.rowcount

    # Step 2: Email match via form_submissions
    cur.execute("""
        UPDATE jobber_customers jc
        SET callrail_id = fs.callrail_id,
            match_method = 'email'
        FROM form_submissions fs
        WHERE jc.customer_id = %s
          AND jc.callrail_id IS NULL
          AND jc.email IS NOT NULL AND jc.email != ''
          AND LOWER(jc.email) = LOWER(fs.customer_email)
          AND fs.customer_id = %s
    """, (customer_id, customer_id))
    email_matches = cur.rowcount

    # Step 3: Phone match via form_submissions
    cur.execute("""
        UPDATE jobber_customers jc
        SET callrail_id = fs.callrail_id,
            match_method = 'phone'
        FROM form_submissions fs
        WHERE jc.customer_id = %s
          AND jc.callrail_id IS NULL
          AND jc.phone_normalized IS NOT NULL AND jc.phone_normalized != ''
          AND normalize_phone(fs.customer_phone) = jc.phone_normalized
          AND fs.customer_id = %s
    """, (customer_id, customer_id))
    form_phone_matches = cur.rowcount

    # Step 4: Phone match via webflow_submissions (GCLID required)
    cur.execute("""
        UPDATE jobber_customers jc
        SET callrail_id = 'WF_' || ws.id,
            match_method = 'webflow'
        FROM webflow_submissions ws
        WHERE jc.customer_id = %s
          AND jc.callrail_id IS NULL
          AND jc.phone_normalized IS NOT NULL AND jc.phone_normalized != ''
          AND ws.phone_normalized = jc.phone_normalized
          AND ws.gclid IS NOT NULL
    """, (customer_id,))
    webflow_matches = cur.rowcount

    conn.commit()
    total = phone_matches + email_matches + form_phone_matches + webflow_matches
    if email_matches > 0 or form_phone_matches > 0 or webflow_matches > 0:
        print(f"    Matching: {phone_matches} phone + {email_matches} email + {form_phone_matches} form-phone + {webflow_matches} webflow")
    return total


# ── Main ETL ─────────────────────────────────────────────────

def pull_client(conn, client_row):
    """Pull all Jobber data for one client."""
    customer_id = client_row["customer_id"]
    name = client_row["name"]
    access_token = client_row["jobber_access_token"]

    if not access_token:
        print(f"  SKIP — no access token")
        return

    print(f"\n{'─' * 60}")
    print(f"  {name} (customer_id: {customer_id})")
    print(f"{'─' * 60}")

    start_ms = int(time.time() * 1000)
    errors = []

    # Check if token needs refresh
    expires_at = client_row.get("jobber_token_expires_at")
    if expires_at and expires_at < datetime.now(timezone.utc):
        try:
            access_token = refresh_token(conn, client_row)
        except Exception as e:
            print(f"  ERROR refreshing token: {e}")
            errors.append(f"Token refresh: {e}")
            return

    def safe_query(query_fn, *args):
        nonlocal access_token
        try:
            return query_fn(*args)
        except Exception as e:
            if "TOKEN_EXPIRED" in str(e):
                try:
                    access_token = refresh_token(conn, client_row)
                    return query_fn(*args)
                except Exception as e2:
                    errors.append(str(e2))
                    return None
            errors.append(str(e))
            return None

    skip_customers = getattr(pull_client, '_skip_customers', False)

    # Step 1: Customers
    customers_count = 0
    customer_map = {}
    if skip_customers:
        print(f"  [1/5] Skipping customers (--skip-customers)")
    else:
        print(f"  [1/5] Pulling customers...")
        customers = safe_query(fetch_all_pages, access_token, CLIENTS_QUERY, "clients")
        if customers:
            customers_count = upsert_customers(conn, customer_id, customers)
            customer_map = {c["id"]: c for c in customers}
            print(f"    → {customers_count} customers upserted")

    # Cooldown between resource types to let rate limit budget restore
    if customers_count > 100:
        print(f"    [Cooldown] Waiting 30s for rate limit to restore...")
        time.sleep(30)

    # Step 2: Quotes
    print(f"  [2/5] Pulling quotes...")
    quotes = safe_query(fetch_all_pages, access_token, QUOTES_QUERY, "quotes")
    quotes_count = 0
    if quotes:
        quotes_count = upsert_quotes(conn, customer_id, quotes, customer_map)
        print(f"    → {quotes_count} quotes upserted")

    if quotes_count > 100:
        print(f"    [Cooldown] Waiting 20s...")
        time.sleep(20)

    # Step 3: Jobs
    print(f"  [3/5] Pulling jobs...")
    jobs = safe_query(fetch_all_pages, access_token, JOBS_QUERY, "jobs")
    jobs_count = 0
    if jobs:
        jobs_count = upsert_jobs(conn, customer_id, jobs, customer_map)
        print(f"    → {jobs_count} jobs upserted")

    if jobs_count > 100:
        print(f"    [Cooldown] Waiting 20s...")
        time.sleep(20)

    # Step 4: Invoices
    print(f"  [4/5] Pulling invoices...")
    invoices = safe_query(fetch_all_pages, access_token, INVOICES_QUERY, "invoices")
    invoices_count = 0
    if invoices:
        invoices_count = upsert_invoices(conn, customer_id, invoices, customer_map)
        print(f"    → {invoices_count} invoices upserted")

    # Step 5: Requests (assessments/inspections)
    print(f"  [5/6] Pulling requests...")
    requests = safe_query(fetch_all_pages, access_token, REQUESTS_QUERY, "requests")
    requests_count = 0
    if requests:
        requests_count = upsert_requests(conn, customer_id, requests)
        print(f"    → {requests_count} requests upserted ({sum(1 for r in requests if r.get('assessment'))} with assessments)")

    # Step 6: CallRail matching
    print(f"  [6/6] Matching to CallRail...")
    matches = match_callrail(conn, customer_id)
    print(f"    → {matches} new matches")

    # Step 6b: Detect pre_lead exceptions (HCP created 7+ days before first CallRail contact)
    cur = conn.cursor()
    cur.execute("""
        UPDATE jobber_customers jc
        SET exception_flags = array_append(
            COALESCE(jc.exception_flags, '{}'), 'pre_lead'
        ), updated_at = NOW()
        WHERE jc.customer_id = %s
          AND jc.callrail_id IS NOT NULL
          AND jc.jobber_created_at IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM calls c
              WHERE c.callrail_id = jc.callrail_id
                AND c.start_time::date - jc.jobber_created_at::date > 7
          )
          AND (jc.exception_flags IS NULL OR NOT 'pre_lead' = ANY(jc.exception_flags))
    """, (customer_id,))
    pre_lead_count = cur.rowcount
    if pre_lead_count:
        print(f"    → {pre_lead_count} pre_lead flags set")
    conn.commit()

    # Log the run
    duration_ms = int(time.time() * 1000) - start_ms
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO jobber_pull_log (
            customer_id, customers_upserted, quotes_upserted,
            jobs_upserted, invoices_upserted, callrail_matches,
            errors, duration_ms
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        customer_id, customers_count, quotes_count,
        jobs_count, invoices_count, matches,
        errors if errors else None, duration_ms,
    ))
    conn.commit()

    print(f"  Done in {duration_ms}ms" + (f" ({len(errors)} errors)" if errors else ""))


def main():
    parser = argparse.ArgumentParser(description="Pull Jobber data into Postgres")
    parser.add_argument("--client", type=int, help="Single client customer_id")
    parser.add_argument("--skip-customers", action="store_true", help="Skip customer pull (already synced)")
    args = parser.parse_args()

    # Pass skip flag to pull_client via function attribute
    pull_client._skip_customers = args.skip_customers

    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    if args.client:
        cur.execute("""
            SELECT * FROM clients
            WHERE customer_id = %s AND jobber_access_token IS NOT NULL
        """, (args.client,))
    else:
        cur.execute("""
            SELECT * FROM clients
            WHERE field_management_software = 'jobber'
              AND jobber_access_token IS NOT NULL
              AND status = 'active'
            ORDER BY name
        """)

    clients = cur.fetchall()
    if not clients:
        print("No Jobber clients found (need jobber_access_token set)")
        sys.exit(0)

    print(f"\n{'═' * 60}")
    print(f"  Jobber ETL — {len(clients)} client(s)")
    print(f"{'═' * 60}")

    for client in clients:
        try:
            pull_client(conn, client)
        except Exception as e:
            print(f"  FATAL ERROR for {client['name']}: {e}")

    conn.close()
    print(f"\n{'═' * 60}")
    print(f"  Jobber ETL complete")
    print(f"{'═' * 60}\n")


if __name__ == "__main__":
    main()
