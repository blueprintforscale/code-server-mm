#!/usr/bin/env python3
"""One-time backfill: fetch GCLID from GHL custom fields for all contacts missing it."""

import sys
import time
import psycopg2
import psycopg2.extras
import pull_ghl_data as p


def main():
    customer_id = int(sys.argv[1]) if len(sys.argv) > 1 else None

    conn = p.get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    if customer_id:
        cur.execute("SELECT * FROM clients WHERE customer_id = %s AND ghl_api_key IS NOT NULL", (customer_id,))
    else:
        cur.execute("""
            SELECT * FROM clients
            WHERE ghl_api_key IS NOT NULL AND ghl_location_id IS NOT NULL
              AND ghl_location_id != '' AND status = 'active'
            ORDER BY name
        """)

    clients = cur.fetchall()
    print(f"Backfilling GCLIDs for {len(clients)} client(s)\n")

    for client in clients:
        cid = client["customer_id"]
        name = client["name"]
        api_key = client["ghl_api_key"]
        location_id = client["ghl_location_id"]

        print(f"{'─' * 60}")
        print(f"  {name} ({cid})")
        print(f"{'─' * 60}")

        # Get custom field map
        try:
            field_map = p.fetch_custom_fields(api_key, location_id)
        except Exception as e:
            print(f"  ERROR fetching fields: {e}")
            continue

        # Find GCLID field ID
        gclid_field_id = None
        for fid, fname in field_map.items():
            fl = fname.lower()
            if "google click" in fl or "gclid" in fl:
                gclid_field_id = fid
                break

        if not gclid_field_id:
            print(f"  No GCLID custom field found — skipping")
            continue

        print(f"  GCLID field: {gclid_field_id}")

        # Get contacts missing GCLID
        cur.execute(
            "SELECT ghl_contact_id, phone_normalized, first_name, last_name "
            "FROM ghl_contacts WHERE customer_id = %s AND gclid IS NULL ORDER BY date_added",
            (cid,)
        )
        contacts = cur.fetchall()
        print(f"  {len(contacts)} contacts to check...")

        updated = 0
        errors = 0
        for i, row in enumerate(contacts):
            try:
                data = p.ghl_request("GET", f"/contacts/{row['ghl_contact_id']}", api_key)
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
                        (gclid, row["ghl_contact_id"])
                    )
                    updated += 1
                    fn = row["first_name"] or ""
                    ln = row["last_name"] or ""
                    print(f"    + {fn} {ln} ({row['phone_normalized']}): {gclid[:40]}...")

                if (i + 1) % 50 == 0:
                    conn.commit()
                    print(f"    ...checked {i+1}/{len(contacts)}, {updated} GCLIDs found")

                time.sleep(0.1)

            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"    ERR {row['ghl_contact_id']}: {e}")
                time.sleep(1)

        conn.commit()
        print(f"  → {updated} GCLIDs added ({errors} errors)\n")

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
