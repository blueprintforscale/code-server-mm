-- Sheet Sync: Generic spreadsheet-as-CRM integration
-- Deploy: psql -U blueprint blueprint -f migration.sql

-- ── Add sheet config columns to clients ──────────────────────────
ALTER TABLE clients ADD COLUMN IF NOT EXISTS sheet_id TEXT;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS sheet_tab TEXT;

-- ── Main table: one row per lead per client ──────────────────────
CREATE TABLE IF NOT EXISTS sheet_leads (
    id                    SERIAL PRIMARY KEY,
    customer_id           BIGINT NOT NULL REFERENCES clients(customer_id),
    contact_id            TEXT NOT NULL,
    source                TEXT,
    date_created          DATE,
    first_name            TEXT,
    last_name             TEXT,
    phone                 TEXT,
    phone_normalized      TEXT,
    email                 TEXT,
    status                TEXT,
    lost_reason           TEXT,
    is_spam               BOOLEAN DEFAULT FALSE,
    -- Dollar amounts in cents
    scheduled_amt         INT DEFAULT 0,  -- Total job value scheduled (col K)
    insp_scheduled_amt    INT DEFAULT 0,  -- Inspection amount only (col W)
    completed_amt         INT DEFAULT 0,
    estimate_sent_amt     INT DEFAULT 0,
    estimate_approved_amt INT DEFAULT 0,
    estimate_open_amt     INT DEFAULT 0,
    job_not_completed_amt INT DEFAULT 0,  -- Job scheduled but not completed (col V)
    roas_rev_amt          INT DEFAULT 0,
    -- Stage counts (1/0)
    insp_scheduled        INT DEFAULT 0,
    insp_completed        INT DEFAULT 0,
    estimate_sent         INT DEFAULT 0,
    estimate_approved     INT DEFAULT 0,
    job_scheduled         INT DEFAULT 0,
    job_completed         INT DEFAULT 0,
    synced_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (customer_id, contact_id)
);

CREATE INDEX IF NOT EXISTS idx_sheet_leads_customer_id ON sheet_leads(customer_id);
CREATE INDEX IF NOT EXISTS idx_sheet_leads_date_created ON sheet_leads(date_created);
CREATE INDEX IF NOT EXISTS idx_sheet_leads_phone_normalized ON sheet_leads(phone_normalized);
CREATE INDEX IF NOT EXISTS idx_sheet_leads_is_spam ON sheet_leads(is_spam);

-- ── View: matches the pattern used by HCP/Jobber revenue CTEs ────
-- This view provides the same interface the risk dashboard expects
CREATE OR REPLACE VIEW v_sheet_lead_revenue AS
SELECT
    sl.customer_id,
    sl.contact_id,
    sl.phone_normalized,
    sl.date_created AS lead_date,
    sl.source,
    sl.status,
    sl.is_spam,
    -- Revenue in dollars (stored as cents)
    sl.roas_rev_amt / 100.0 AS roas_revenue,
    sl.completed_amt / 100.0 AS completed_revenue,
    sl.scheduled_amt / 100.0 AS scheduled_value,
    sl.estimate_sent_amt / 100.0 AS estimate_sent_value,
    sl.estimate_approved_amt / 100.0 AS estimate_approved_value,
    sl.estimate_open_amt / 100.0 AS estimate_open_value,
    sl.insp_scheduled_amt / 100.0 AS insp_scheduled_value,
    sl.job_not_completed_amt / 100.0 AS job_not_completed_value,
    -- Stage flags
    sl.insp_scheduled AS has_insp_scheduled,
    sl.insp_completed AS has_insp_completed,
    sl.estimate_sent AS has_estimate_sent,
    sl.estimate_approved AS has_estimate_approved,
    sl.job_scheduled AS has_job_scheduled,
    sl.job_completed AS has_job_completed
FROM sheet_leads sl;

-- ── Configure Luke & Gabi (Fresno Mold Busters) ─────────────────
UPDATE clients
SET sheet_id = '1BMLIjxEyWh722bjWadNdgoIcXET7DTsDES7jM6mYnsg',
    sheet_tab = 'LeadGHL'
WHERE customer_id = 4229015839;
