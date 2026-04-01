#!/usr/bin/env python3
"""
GHL Transactions ETL — Pull payment transactions from GoHighLevel.

Usage:
  python3 pull_ghl_transactions.py                       # All GHL clients
  python3 pull_ghl_transactions.py --client 7123434733   # Single client

Stores succeeded transactions in ghl_transactions table with normalized phone for matching.
"""

import argparse
import json
import re
import sys
import time

import psycopg2
import psycopg2.extras

sys.path.insert(0, ".")
import pull_ghl_data as p


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS ghl_transactions (
    id SERIAL PRIMARY KEY,
    ghl_transaction_id TEXT NOT NULL,
    customer_id BIGINT REFERENCES clients(customer_id),
    ghl_location_id TEXT,
    ghl_contact_id TEXT,
    contact_name TEXT,
    contact_email TEXT,
    phone_normalized TEXT,
    amount_cents BIGINT,
    currency TEXT DEFAULT 'USD',
    status TEXT,
    entity_type TEXT,
    entity_source_type TEXT,
    entity_source_sub_type TEXT,
    entity_source_name TEXT,
    entity_source_id TEXT,
    invoice_number TEXT,
    payment_provider TEXT,
    created_at TIMESTAMPTZ,
    fulfilled_at TIMESTAMPTZ,
    synced_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(ghl_transaction_id)
);
CREATE INDEX IF NOT EXISTS idx_ghl_txn_customer ON ghl_transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_ghl_txn_contact ON ghl_transactions(ghl_contact_id);
CREATE INDEX IF NOT EXISTS idx_ghl_txn_phone ON ghl_transactions(phone_normalized);
"""


def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute(SCHEMA_SQL)
    conn.commit()


def fetch_transactions(api_key, location_id):
    """Fetch all transactions for a location, paginating."""
    all_txns = []
    offset = 0
    limit = 100

    while True:
        data = p.ghl_request("GET", "/payments/transactions", api_key, {
            "altId": location_id,
            "altType": "location",
            "limit": str(limit),
            "offset": str(offset),
        })
        txns = data.get("data", [])
        if not txns:
            break

        all_txns.extend(txns)
        print(f"    Fetched {len(all_txns)} transactions so far...")

        if len(txns) < limit:
            break

        offset += len(txns)
        time.sleep(0.1)

    return all_txns


def upsert_transactions(conn, customer_id, location_id, transactions, phone_map):
    """Upsert transactions into ghl_transactions table."""
    cur = conn.cursor()
    count = 0

    for t in transactions:
        txn_id = t.get("_id")
        if not txn_id:
            continue

        # Only store succeeded transactions
        status = t.get("status", "")
        if status != "succeeded":
            continue

        contact_id = t.get("contactId")
        phone_norm = phone_map.get(contact_id)

        # Amount in dollars from API -> cents
        amount = t.get("amount", 0)
        amount_cents = int(round(float(amount) * 100)) if amount else 0

        # Entity source info
        src_meta = t.get("entitySourceMeta") or {}
        inv_prefix = src_meta.get("invoiceNumberPrefix", "")
        inv_number = src_meta.get("invoiceNumber", "")
        invoice_number = f"{inv_prefix}{inv_number}" if inv_number else None

        cur.execute("""
            INSERT INTO ghl_transactions (
                ghl_transaction_id, customer_id, ghl_location_id,
                ghl_contact_id, contact_name, contact_email, phone_normalized,
                amount_cents, currency, status,
                entity_type, entity_source_type, entity_source_sub_type,
                entity_source_name, entity_source_id, invoice_number,
                payment_provider, created_at, fulfilled_at, synced_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (ghl_transaction_id) DO UPDATE SET
                status = EXCLUDED.status,
                amount_cents = EXCLUDED.amount_cents,
                phone_normalized = COALESCE(EXCLUDED.phone_normalized, ghl_transactions.phone_normalized),
                contact_name = EXCLUDED.contact_name,
                fulfilled_at = EXCLUDED.fulfilled_at,
                synced_at = NOW()
        """, (
            txn_id, customer_id, location_id,
            contact_id,
            t.get("contactName"),
            t.get("contactEmail"),
            phone_norm,
            amount_cents,
            t.get("currency", "USD"),
            status,
            t.get("entityType"),
            t.get("entitySourceType"),
            t.get("entitySourceSubType"),
            t.get("entitySourceName"),
            t.get("entitySourceId"),
            invoice_number,
            t.get("paymentProviderType"),
            t.get("createdAt"),
            t.get("fulfilledAt"),
        ))
        count += 1

    conn.commit()
    return count


def pull_client(conn, client_row):
    """Pull all transactions for one client."""
    customer_id = client_row["customer_id"]
    name = client_row["name"]
    api_key = client_row["ghl_api_key"]
    location_id = client_row["ghl_location_id"]

    if not api_key or not location_id:
        print(f"  SKIP — missing api_key or location_id")
        return

    print(f"\n{'─' * 60}")
    print(f"  {name} (customer_id: {customer_id})")
    print(f"{'─' * 60}")

    # Build contactId -> phone map from ghl_contacts
    cur = conn.cursor()
    cur.execute(
        "SELECT ghl_contact_id, phone_normalized FROM ghl_contacts "
        "WHERE customer_id = %s AND phone_normalized IS NOT NULL",
        (customer_id,)
    )
    phone_map = {row[0]: row[1] for row in cur.fetchall()}
    print(f"  {len(phone_map)} contacts with phones for matching")

    # Fetch transactions
    print(f"  Pulling transactions...")
    try:
        transactions = fetch_transactions(api_key, location_id)
    except Exception as e:
        print(f"  ERROR fetching transactions: {e}")
        return

    print(f"    → {len(transactions)} transactions fetched")

    if not transactions:
        print(f"  No transactions found — done")
        return

    # Upsert
    upserted = upsert_transactions(conn, customer_id, location_id, transactions, phone_map)
    print(f"    → {upserted} succeeded transactions upserted")

    # Summary
    succeeded = [t for t in transactions if t.get("status") == "succeeded"]
    total_amount = sum(t.get("amount", 0) for t in succeeded)
    estimate_linked = sum(1 for t in succeeded if t.get("entitySourceSubType") == "estimate")
    standalone = sum(1 for t in succeeded if not t.get("entitySourceSubType"))
    print(f"    → Total revenue: ${total_amount:,.2f}")
    print(f"    → {estimate_linked} from estimates, {standalone} standalone invoices")


def main():
    parser = argparse.ArgumentParser(description="Pull GHL transactions into Postgres")
    parser.add_argument("--client", type=int, help="Single client customer_id")
    args = parser.parse_args()

    conn = p.get_db()
    ensure_schema(conn)

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    if args.client:
        cur.execute("""
            SELECT * FROM clients
            WHERE customer_id = %s
              AND ghl_api_key IS NOT NULL
              AND ghl_location_id IS NOT NULL
        """, (args.client,))
    else:
        cur.execute("""
            SELECT * FROM clients
            WHERE ghl_api_key IS NOT NULL
              AND ghl_location_id IS NOT NULL
              AND ghl_location_id != ''
              AND status = 'active'
            ORDER BY name
        """)

    clients = cur.fetchall()
    if not clients:
        print("No GHL clients found")
        sys.exit(0)

    print(f"\n{'═' * 60}")
    print(f"  GHL Transactions ETL — {len(clients)} client(s)")
    print(f"{'═' * 60}")

    for client in clients:
        try:
            pull_client(conn, client)
        except Exception as e:
            print(f"  FATAL ERROR for {client['name']}: {e}")

    conn.close()
    print(f"\n{'═' * 60}")
    print(f"  GHL Transactions ETL complete")
    print(f"{'═' * 60}\n")


if __name__ == "__main__":
    main()
