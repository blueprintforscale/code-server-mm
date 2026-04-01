-- ============================================================
-- GCLID → Campaign Map — Migration
-- ============================================================
-- Maps Google Ads click IDs (GCLIDs) from CallRail calls/forms
-- back to the campaign they originated from.
--
-- Populated by: pull_gclid_campaigns.py (click_view API)
-- Used by: client dashboard (campaign badges, trends)
--
-- Run via SSH:
--   ssh mac-mini '/opt/homebrew/opt/postgresql@17/bin/psql -U blueprint -d blueprint -f /path/to/gclid-campaign-map-migration.sql'

-- ═══════════════════════════════════════════════
-- 1. Create the gclid_campaign_map table
-- ═══════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS gclid_campaign_map (
    id              SERIAL PRIMARY KEY,
    customer_id     BIGINT NOT NULL REFERENCES clients(customer_id),
    gclid           TEXT NOT NULL,
    campaign_id     BIGINT NOT NULL,
    campaign_name   TEXT,
    ad_group_id     BIGINT,
    ad_group_name   TEXT,
    keyword_text    TEXT,
    click_date      DATE,
    pulled_at       TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(customer_id, gclid)
);

-- Index for joining with calls/forms by gclid
CREATE INDEX IF NOT EXISTS idx_gclid_map_gclid ON gclid_campaign_map(gclid);

-- Index for campaign-level queries
CREATE INDEX IF NOT EXISTS idx_gclid_map_customer_campaign ON gclid_campaign_map(customer_id, campaign_id);

-- Index for date-range lookups
CREATE INDEX IF NOT EXISTS idx_gclid_map_click_date ON gclid_campaign_map(click_date);
