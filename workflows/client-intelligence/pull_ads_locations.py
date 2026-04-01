#!/usr/bin/env python3
"""
Google Ads Location Targeting ETL — Pull geo-targeting for each client's campaigns.

Usage:
  python3 pull_ads_locations.py                    # All active US clients with campaigns
  python3 pull_ads_locations.py --client 7123434733  # Single client

Pulls campaign location criteria from Google Ads API and stores in
ads_location_targets table. Used to render targeting maps in the dashboard.

Skips:
  - UK/non-US accounts (country_code != 'US')
  - Clients with no enabled campaigns (no location criteria returned)

Auto-geocodes new locations via Nominatim (1 req/sec).

Requires:
  - google-ads.yaml config
  - Python 3.12 venv with google-ads SDK
"""

import argparse
import json
import logging
import os
import sys
import time
import urllib.request
import urllib.parse

import psycopg2
import psycopg2.extras

# Google Ads SDK needs the venv
GOOGLE_ADS_YAML = os.path.expanduser("~/projects/.mcp-servers/google_ads_mcp/google-ads.yaml")

DB_CONFIG = {
    "dbname": "blueprint",
    "user": "blueprint",
    "host": "localhost",
    "port": 5432,
}

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": "BlueprintForScale-ETL/1.0 (susie@blueprintforscale.com)"}

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger('ads-locations')


def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ads_location_targets (
                id              SERIAL PRIMARY KEY,
                customer_id     BIGINT NOT NULL REFERENCES clients(customer_id),
                campaign_id     BIGINT NOT NULL,
                campaign_name   TEXT,
                location_id     BIGINT NOT NULL,
                location_name   TEXT,
                location_type   TEXT,
                is_negative     BOOLEAN DEFAULT FALSE,
                canonical_name  TEXT,
                country_code    TEXT,
                target_type     TEXT,
                latitude        NUMERIC(10,6),
                longitude       NUMERIC(10,6),
                reach           BIGINT,
                pulled_at       TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(customer_id, campaign_id, location_id, is_negative)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_ads_locations_customer
                ON ads_location_targets(customer_id)
        """)
    conn.commit()


def pull_location_targets(client, customer_id):
    """Pull location targeting criteria for a customer's ENABLED campaigns."""
    query = """
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign_criterion.location.geo_target_constant,
            campaign_criterion.negative
        FROM campaign_criterion
        WHERE campaign.status = 'ENABLED'
            AND campaign_criterion.type = 'LOCATION'
    """

    try:
        response = client.get_service("GoogleAdsService").search(
            customer_id=str(customer_id),
            query=query,
        )

        results = []
        for row in response:
            results.append({
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "geo_target": row.campaign_criterion.location.geo_target_constant,
                "is_negative": row.campaign_criterion.negative,
            })
        return results
    except Exception as e:
        log.error("  Failed to pull locations for %s: %s" % (customer_id, e))
        return []


def resolve_geo_target(client, resource_name):
    """Resolve a geo_target_constant resource name to location details."""
    try:
        response = client.get_service("GoogleAdsService").search(
            customer_id="2985235474",  # Use MCC for geo lookups
            query="""
                SELECT
                    geo_target_constant.id,
                    geo_target_constant.name,
                    geo_target_constant.canonical_name,
                    geo_target_constant.country_code,
                    geo_target_constant.target_type,
                    geo_target_constant.status
                FROM geo_target_constant
                WHERE geo_target_constant.resource_name = '%s'
            """ % resource_name,
        )
        for row in response:
            gtc = row.geo_target_constant
            return {
                "location_id": gtc.id,
                "location_name": gtc.name,
                "canonical_name": gtc.canonical_name,
                "country_code": gtc.country_code,
                "target_type": gtc.target_type.name if hasattr(gtc.target_type, 'name') else str(gtc.target_type),
            }
    except Exception as e:
        log.warning("  Could not resolve %s: %s" % (resource_name, e))
    return None


def geocode(name):
    """Geocode a US location name via Nominatim. Returns (lat, lng) or None."""
    query = name
    if "United States" not in query and "USA" not in query:
        query += ", United States"

    params = urllib.parse.urlencode({
        "q": query,
        "format": "json",
        "limit": 1,
        "countrycodes": "us",
    })
    url = "%s?%s" % (NOMINATIM_URL, params)
    req = urllib.request.Request(url, headers=NOMINATIM_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        log.warning("    Geocode failed for '%s': %s" % (name, e))
    return None


def main():
    parser = argparse.ArgumentParser(description="Pull Google Ads location targeting")
    parser.add_argument('--client', type=str, help='Single customer_id')
    args = parser.parse_args()

    # Import Google Ads client
    from google.ads.googleads.client import GoogleAdsClient
    gads_client = GoogleAdsClient.load_from_storage(GOOGLE_ADS_YAML)

    conn = get_db()
    ensure_table(conn)

    # Get active clients only
    with conn.cursor() as cur:
        query = "SELECT customer_id, name FROM clients WHERE status = 'active'"
        params = []
        if args.client:
            query += " AND customer_id = %s"
            params.append(int(args.client))
        query += " ORDER BY name"
        cur.execute(query, params)
        clients = cur.fetchall()

    log.info("Pulling location targets for %d active clients" % len(clients))

    # Cache resolved geo targets
    geo_cache = {}
    total_locations = 0
    skipped_uk = 0
    skipped_no_campaigns = 0
    new_geocoded = 0

    for customer_id, client_name in clients:
        log.info("\n  %s (%s)" % (client_name, customer_id))

        targets = pull_location_targets(gads_client, customer_id)
        if not targets:
            log.info("    No enabled campaigns — skipping")
            skipped_no_campaigns += 1
            continue

        log.info("    Found %d location criteria" % len(targets))

        for target in targets:
            geo_resource = target["geo_target"]
            if not geo_resource:
                continue

            # Resolve geo target details (cached)
            if geo_resource not in geo_cache:
                geo_cache[geo_resource] = resolve_geo_target(gads_client, geo_resource)
                time.sleep(0.1)

            geo = geo_cache.get(geo_resource)
            if not geo:
                continue

            # Skip non-US locations (UK accounts etc.)
            if geo.get("country_code") and geo["country_code"] != "US":
                skipped_uk += 1
                continue

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ads_location_targets (
                        customer_id, campaign_id, campaign_name,
                        location_id, location_name, location_type,
                        is_negative, canonical_name, country_code, target_type,
                        pulled_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (customer_id, campaign_id, location_id, is_negative)
                    DO UPDATE SET
                        campaign_name = EXCLUDED.campaign_name,
                        location_name = EXCLUDED.location_name,
                        canonical_name = EXCLUDED.canonical_name,
                        target_type = EXCLUDED.target_type,
                        pulled_at = NOW()
                """, (
                    customer_id,
                    target["campaign_id"],
                    target["campaign_name"],
                    geo["location_id"],
                    geo["location_name"],
                    geo.get("location_type"),
                    target["is_negative"],
                    geo.get("canonical_name"),
                    geo.get("country_code"),
                    geo.get("target_type"),
                ))
                total_locations += 1

            log.info("    %s: %s (%s)%s" % (
                target["campaign_name"][:30],
                geo["location_name"],
                geo.get("target_type", "?"),
                " [EXCLUDED]" if target["is_negative"] else ""
            ))

        conn.commit()

    # Auto-geocode any new locations missing lat/lng (US only, active clients)
    log.info("\nChecking for locations needing geocoding...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT a.location_id, a.location_name, a.canonical_name, a.target_type
            FROM ads_location_targets a
            JOIN clients c ON c.customer_id = a.customer_id
            WHERE a.is_negative = false
              AND (a.latitude IS NULL OR a.longitude IS NULL)
              AND a.country_code = 'US'
              AND c.status = 'active'
            ORDER BY a.location_name
        """)
        need_geo = cur.fetchall()

    if need_geo:
        log.info("Geocoding %d new locations..." % len(need_geo))
        for loc_id, name, canonical, ttype in need_geo:
            search = canonical or (name + ", United States")
            result = geocode(search)
            if result:
                lat, lng = result
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE ads_location_targets
                        SET latitude = %s, longitude = %s
                        WHERE location_id = %s AND (latitude IS NULL OR longitude IS NULL)
                    """, (lat, lng, loc_id))
                conn.commit()
                new_geocoded += 1
                log.info("    %s → %.4f, %.4f" % (name, lat, lng))
            else:
                log.warning("    %s → FAILED" % name)
            time.sleep(1.1)  # Nominatim rate limit
    else:
        log.info("All locations already geocoded.")

    conn.close()
    log.info("\nDone. %d locations stored, %d geocoded, %d UK skipped, %d no-campaign skipped." % (
        total_locations, new_geocoded, skipped_uk, skipped_no_campaigns))
    log.info("Geo cache: %d unique locations resolved." % len(geo_cache))


if __name__ == "__main__":
    main()
