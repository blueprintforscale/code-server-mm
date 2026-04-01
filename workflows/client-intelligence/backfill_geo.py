#!/usr/bin/env python3
"""
One-time backfill: populate latitude/longitude in ads_location_targets
using Nominatim (free OpenStreetMap geocoder).

Run on Mac Mini where DB is local:
  python3 backfill_geo.py          # All missing
  python3 backfill_geo.py --dry    # Preview without writing

Rate-limited to 1 req/sec per Nominatim policy.
"""

import argparse
import json
import time
import urllib.request
import urllib.parse
import psycopg2

DB_CONFIG = {
    "dbname": "blueprint",
    "user": "blueprint",
    "host": "localhost",
    "port": 5432,
}

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
HEADERS = {"User-Agent": "BlueprintForScale-Backfill/1.0 (susie@blueprintforscale.com)"}


def geocode(name, target_type):
    """Geocode a location name via Nominatim. Returns (lat, lng) or None."""
    # Build search query
    query = name
    if "United States" not in query and "USA" not in query:
        query += ", United States"

    params = urllib.parse.urlencode({
        "q": query,
        "format": "json",
        "limit": 1,
        "countrycodes": "us",
    })
    url = f"{NOMINATIM_URL}?{params}"

    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        print(f"  ERROR geocoding '{name}': {e}")
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry", action="store_true", help="Preview only")
    args = parser.parse_args()

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Get distinct locations that need geocoding (active clients only, US only)
    cur.execute("""
        SELECT DISTINCT a.location_id, a.location_name, a.canonical_name, a.target_type
        FROM ads_location_targets a
        JOIN clients c ON c.customer_id = a.customer_id
        WHERE a.is_negative = false
          AND (a.latitude IS NULL OR a.longitude IS NULL)
          AND c.status = 'active'
          AND a.country_code = 'US'
        ORDER BY a.target_type, a.location_name
    """)
    rows = cur.fetchall()
    print(f"Found {len(rows)} locations needing geocoding\n")

    if not rows:
        print("Nothing to do!")
        return

    success = 0
    failed = []

    for i, (loc_id, name, canonical, ttype) in enumerate(rows):
        search = canonical or (name + ", United States")
        print(f"[{i+1}/{len(rows)}] {name} ({ttype}) -> ", end="", flush=True)

        if args.dry:
            print("(dry run)")
            continue

        result = geocode(search, ttype)
        if result:
            lat, lng = result
            cur.execute("""
                UPDATE ads_location_targets
                SET latitude = %s, longitude = %s
                WHERE location_id = %s AND (latitude IS NULL OR longitude IS NULL)
            """, (lat, lng, loc_id))
            conn.commit()
            print(f"{lat:.4f}, {lng:.4f}")
            success += 1
        else:
            print("FAILED")
            failed.append(name)

        # Respect Nominatim rate limit
        time.sleep(1.1)

    print(f"\nDone: {success} geocoded, {len(failed)} failed")
    if failed:
        print("Failed locations:")
        for f in failed:
            print(f"  - {f}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
