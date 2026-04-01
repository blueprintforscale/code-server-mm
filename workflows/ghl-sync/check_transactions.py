#!/usr/bin/env python3
"""Check GHL transactions for inspection-fee contacts."""
import json
import psycopg2
import pull_ghl_data as p

api_key = "pit-ab6ecd6e-7acc-4759-9334-911c0cf48ad7"
loc = "CirscxC4HNpRqMMBzOhI"
cust_id = 1714816135

# Paginate all transactions
all_txns = []
offset = 0
while True:
    data = p.ghl_request("GET", "/payments/transactions", api_key, {
        "altId": loc, "altType": "location", "limit": "100", "offset": str(offset)
    })
    txns = data.get("data", [])
    if not txns:
        break
    all_txns.extend(txns)
    offset += len(txns)
    if len(txns) < 100:
        break

succeeded = [t for t in all_txns if t.get("status") == "succeeded"]
print(f"Total transactions: {len(all_txns)}, Succeeded: {len(succeeded)}")

# Inspection-fee contacts from the spreadsheet
targets = {
    "9037460733","9032413791","9032408448","9362757769","9039467445",
    "9035151966","4697555355","9036904384","9035204294","9454002269",
    "2149296039","9035044755","4699713740","9032378884","5127605870","9032533739"
}

# Build contactId -> phone map
conn = psycopg2.connect(dbname="blueprint", user="blueprint", host="localhost")
cur = conn.cursor()
cur.execute(
    "SELECT ghl_contact_id, phone_normalized FROM ghl_contacts "
    "WHERE customer_id = %s AND phone_normalized IN %s",
    (cust_id, tuple(targets))
)
contact_map = {row[0]: row[1] for row in cur.fetchall()}

print(f"\nTransactions for the 16 inspection-fee contacts:")
found = set()
for t in succeeded:
    cid = t.get("contactId")
    if cid in contact_map:
        phone = contact_map[cid]
        found.add(phone)
        src = t.get("entitySourceSubType") or t.get("entitySourceName") or "?"
        print(f"  {t.get('contactName','?'):35s} {phone}  ${t.get('amount',0):>10,.2f}  source: {src}")

missing = targets - found
if missing:
    print(f"\nNo transactions found for: {missing}")

conn.close()
