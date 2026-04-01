#!/usr/bin/env python3
"""
Google Drive Onboarding Doc ETL — Pull client onboarding docs and extract profile data.

Usage:
  python3 pull_onboarding_docs.py                          # All clients
  python3 pull_onboarding_docs.py --client 7123434733      # Single client

Searches Google Drive for "Blueprint Onboarding - [Client Name]" docs,
reads them via the gdrive MCP read_doc tool pattern, and uses Claude
to extract structured client profile data.

Requires:
  - ANTHROPIC_API_KEY env var
  - Google Drive access (via service account or OAuth)
"""

import argparse
import json
import logging
import os
import re
import sys
import time

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

# Known onboarding doc IDs mapped to client names (from Drive search)
# Format: partial client name match -> Google Doc ID
ONBOARDING_DOCS = {
    "Dave Hinckley": "1lijulHuvUaVXTMeYBlV_q0k4Re5bVFbqsVT9U5mJbLw",
    "Pete Pratt": "1DkHwHL4X9-xtFzROmVKmSeQ9zhbNtmh4acwbukLC9yU",
    "Mark Edwards": "121ZDT8Q7Zwp494w7GiVXglyOYRvpkdQQAqXuc3WHy_I",
    "Greg Sazdanoff": "1E54ZKZj8W-r8SJ94VLbQc-1Ol4o8_aTmx58TIafPfSM",
    "Pete": "1mGcbMFeuNksEgb_fuc44kf38XGavjy_i7GGaLG8mQ1Y",
    "John & Josh": "1eAtatqBJEMjDAszSFD_ZYxFXCSNOKjFJFUYqXxiP8Hc",
    "Isaac": "1oPiGhX5M2Bj_Wu_H1wXcMIyiv7rEGs1_l_Mh_xiyPSY",
    "Michelle": "1q6RZsgWJtl7tT28KNDJRhWqXDccvrAVFX1LvMcfvxls",
    "Rob Brown San Diego": "1qf7C56CbQ_m9MPeVgspuNQ2V4ibQBaHbAuqa1Aon-HQ",
    "Elise": "1zqT8JcsJ8GmOSialn2NlCfT8gs2n5K72L8Vmy8QdgTc",
}

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger('onboarding-docs')


def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def read_google_doc(doc_id):
    """Read a Google Doc using the export API.

    Uses the Google Docs export URL which works for publicly shared docs
    or docs accessible via service account.
    """
    url = f"https://docs.google.com/document/d/{doc_id}/export?format=txt"
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read().decode('utf-8')
    except Exception as e:
        log.warning(f"  Could not read doc {doc_id}: {e}")
        return None


def extract_client_profile(doc_text, client_name):
    """Use Claude to extract structured profile data from an onboarding doc."""
    if not ANTHROPIC_API_KEY:
        log.warning("No ANTHROPIC_API_KEY — skipping extraction")
        return None

    prompt = f"""Analyze this onboarding document for client "{client_name}" (a mold remediation company working with Blueprint for Scale, a Google Ads agency).

Extract the following information if present. Return JSON only.

{{
  "owner_name": "full name of business owner(s)",
  "business_name": "official business name",
  "location": "city, state or service area",
  "phone": "business phone number",
  "email": "business email",
  "website": "business website URL",
  "employees": [
    {{
      "name": "employee name",
      "role": "their role",
      "phone": "phone if available",
      "email": "email if available"
    }}
  ],
  "service_area": "geographic areas they serve",
  "inspection_type": "free or paid inspections",
  "business_details": "how long in business, certifications, specialties",
  "goals": "what they want from the program",
  "personal_notes": [
    "any personal details about the owner (family, hobbies, background)"
  ],
  "preferences": "communication preferences, availability, notes",
  "competitors": "any competitors mentioned",
  "crm_system": "what CRM or scheduling system they use",
  "marketing_history": "any past marketing they have done",
  "notes": "any other relevant information"
}}

Only include fields you find in the document. Return empty string or empty array for missing fields.

Document:
{doc_text[:8000]}"""

    try:
        body = json.dumps({
            "model": "claude-sonnet-4-6",
            "max_tokens": 3000,
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
            if "```" in content:
                content = content.split("```json")[-1].split("```")[0].strip()
                if not content:
                    content = result["content"][0]["text"].split("```")[-2].strip()
            return json.loads(content)

    except Exception as e:
        log.warning(f"  AI extraction failed: {e}")
        return None


def store_profile_data(conn, customer_id, profile_data):
    """Store extracted profile data into client_profiles and client_contacts."""
    with conn.cursor() as cur:
        # Update client_profiles with goals, preferences, notes
        updates = []
        params = []

        if profile_data.get("goals"):
            updates.append("client_goals = %s")
            params.append(profile_data["goals"])

        if profile_data.get("preferences"):
            updates.append("preferences = %s")
            params.append(profile_data["preferences"])

        notes_parts = []
        if profile_data.get("business_details"):
            notes_parts.append(f"Business: {profile_data['business_details']}")
        if profile_data.get("marketing_history"):
            notes_parts.append(f"Marketing history: {profile_data['marketing_history']}")
        if profile_data.get("competitors"):
            notes_parts.append(f"Competitors: {profile_data['competitors']}")
        if profile_data.get("notes"):
            notes_parts.append(profile_data["notes"])

        if notes_parts:
            updates.append("notes = %s")
            params.append("\n".join(notes_parts))

        if updates:
            updates.append("updated_at = NOW()")
            params.append(customer_id)
            cur.execute(f"""
                UPDATE client_profiles
                SET {', '.join(updates)}
                WHERE customer_id = %s
            """, params)

        # Store employee contacts
        for emp in profile_data.get("employees", []):
            if not emp.get("name"):
                continue
            phone_norm = None
            if emp.get("phone"):
                digits = re.sub(r'\D', '', emp["phone"])
                if len(digits) >= 10:
                    phone_norm = digits[-10:]

            cur.execute("""
                INSERT INTO client_contacts (
                    customer_id, name, role, phone, phone_normalized, email
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (customer_id, emp["name"], emp.get("role"),
                  emp.get("phone"), phone_norm, emp.get("email")))

        # Store personal notes
        for note in profile_data.get("personal_notes", []):
            if note and note.strip():
                cur.execute("""
                    INSERT INTO client_personal_notes (
                        customer_id, note, category, source, auto_extracted
                    ) VALUES (%s, %s, 'personal', 'onboarding_doc', TRUE)
                """, (customer_id, note.strip()))

    conn.commit()


def match_doc_to_client(doc_name_hint, clients):
    """Try to match an onboarding doc to a client by name."""
    hint_lower = doc_name_hint.lower()
    for cid, name in clients:
        name_parts = name.lower().replace("|", " ").split()
        name_parts = [p for p in name_parts if len(p) > 2 and p not in ('the', 'and', 'of', 'pure', 'maintenance')]
        if any(part in hint_lower for part in name_parts):
            return cid, name
    return None, None


def main():
    parser = argparse.ArgumentParser(description="Pull onboarding docs from Google Drive")
    parser.add_argument('--client', type=str, help='Pull only one customer_id')
    args = parser.parse_args()

    if not ANTHROPIC_API_KEY:
        log.error("ANTHROPIC_API_KEY environment variable required")
        sys.exit(1)

    conn = get_db()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT customer_id, name FROM clients
                WHERE status = 'active' ORDER BY name
            """)
            clients = cur.fetchall()

        log.info(f"Processing {len(ONBOARDING_DOCS)} onboarding docs...")

        for doc_name, doc_id in ONBOARDING_DOCS.items():
            customer_id, client_name = match_doc_to_client(doc_name, clients)

            if not customer_id:
                log.warning(f"  Could not match '{doc_name}' to any client — skipping")
                continue

            if args.client and str(customer_id) != args.client:
                continue

            log.info(f"\n  Processing: {client_name}")
            log.info(f"  Doc: {doc_name} ({doc_id})")

            # Read the document
            doc_text = read_google_doc(doc_id)
            if not doc_text:
                log.warning(f"  Could not read doc — skipping")
                continue

            log.info(f"  Doc length: {len(doc_text)} chars")

            # Extract profile data
            profile_data = extract_client_profile(doc_text, client_name)
            if not profile_data:
                continue

            # Store it
            store_profile_data(conn, customer_id, profile_data)

            employees = len(profile_data.get("employees", []))
            notes = len(profile_data.get("personal_notes", []))
            log.info(f"  Extracted: {employees} contacts, {notes} personal notes")

            time.sleep(2)  # Rate limit Claude API

        log.info("\nDone!")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
