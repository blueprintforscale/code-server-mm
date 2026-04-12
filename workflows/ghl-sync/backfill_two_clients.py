#!/usr/bin/env python3
"""Backfill GCLIDs for Nathan Brown and Alemania only."""

import sys
import time
sys.path.insert(0, "ghl-sync")
import pull_ghl_data as p
import psycopg2
import psycopg2.extras

conn = psycopg2.connect("dbname=blueprint user=blueprint")
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

for cid in [1916645644, 3703996852]:
    cur.execute("SELECT name, ghl_api_key, ghl_location_id FROM clients WHERE customer_id = %s", (cid,))
    row = cur.fetchone()
    name, api_key, location_id = row["name"], row["ghl_api_key"], row["ghl_location_id"]
    print(f"\n=== {name} ===")

    field_map = p.fetch_custom_fields(api_key, location_id)
    gclid_field_id = None
    for fid, fname in field_map.items():
        fl = fname.lower()
        if "google click" in fl or "gclid" in fl:
            gclid_field_id = fid
            print(f"GCLID field: {fid} = {fname}")
            break

    if not gclid_field_id:
        print("No GCLID field found")
        continue

    cur.execute(
        "SELECT ghl_contact_id, phone_normalized, first_name, last_name "
        "FROM ghl_contacts WHERE customer_id = %s AND (gclid IS NULL OR gclid = '') "
        "ORDER BY date_added DESC",
        (cid,)
    )
    contacts = cur.fetchall()
    print(f"{len(contacts)} contacts to check...")

    updated = 0
    errors = 0
    for i, c in enumerate(contacts):
        try:
            data = p.ghl_request("GET", f"/contacts/{c['ghl_contact_id']}", api_key)
            contact = data.get("contact", {})

            gclid = None
            for cf in (contact.get("customFields") or []):
                if cf.get("id") == gclid_field_id:
                    val = cf.get("field_value") or cf.get("value")
                    if isinstance(val, str) and val:
                        gclid = val
                        break

            if not gclid:
                attr = contact.get("lastAttributionSource") or {}
                if attr.get("gclid"):
                    gclid = attr["gclid"]

            if gclid:
                cur.execute(
                    "UPDATE ghl_contacts SET gclid = %s WHERE ghl_contact_id = %s",
                    (gclid, c["ghl_contact_id"])
                )
                updated += 1
                fn = c["first_name"] or ""
                ln = c["last_name"] or ""
                print(f"  + {fn} {ln} ({c['phone_normalized']}): {gclid[:40]}...")

            if (i + 1) % 100 == 0:
                conn.commit()
                print(f"  ...checked {i+1}/{len(contacts)}, {updated} found")

            time.sleep(0.12)
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  ERR {c['ghl_contact_id']}: {e}")
            time.sleep(1)

    conn.commit()
    print(f"Done: {updated} GCLIDs added from {len(contacts)} checked ({errors} errors)")

conn.close()
print("\nAll done.")
