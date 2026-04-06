-- Migration: Call Extension Tracker Attribution + Post-Touch ROAS
-- Date: 2026-04-06
-- 
-- 1. Add tracker_id to calls table
-- 2. Create callrail_trackers lookup table
-- 3. Add kpi_date_created to ghl_contacts
-- 4. Rebuild mv_funnel_leads with:
--    - Call extension tracker attribution
--    - first_ga_touch_time (LEAST of CallRail call, form, GHL GCLID)
--    - Post-touch ROAS: only revenue/stages after first GA touch for google_ads leads

-- Step 1: tracker_id on calls
ALTER TABLE calls ADD COLUMN IF NOT EXISTS tracker_id TEXT;

-- Step 2: callrail_trackers table
CREATE TABLE IF NOT EXISTS callrail_trackers (
    tracker_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    source_type TEXT,
    company_id TEXT,
    company_name TEXT,
    tracking_number TEXT,
    status TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Step 3: kpi_date_created on ghl_contacts
ALTER TABLE ghl_contacts ADD COLUMN IF NOT EXISTS kpi_date_created TIMESTAMPTZ;

-- Step 4: Populate callrail_trackers via backfill_tracker_id.py and CallRail API
-- Step 5: Backfill tracker_id on calls via backfill_tracker_id.py
-- Step 6: Rebuild mv_funnel_leads (see view definition in classify_calls.py comments or run REFRESH)
