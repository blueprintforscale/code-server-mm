-- ============================================================
-- HCP Dashboard Views — Phase 3
-- ============================================================
-- Joins CallRail (Google Ads attribution) → HCP tables via phone matching
-- Run after migration and initial data pull.
--
-- Matching chain:
--   calls.caller_phone (normalized) = hcp_customers.phone_normalized
--   → hcp_inspections, hcp_estimates, hcp_jobs (via hcp_customer_id)
--
-- Google Ads filter:
--   calls.gclid IS NOT NULL OR source IN ('Google Ads', 'Google Ads 2')
-- ============================================================

BEGIN;

-- ============================================================
-- Helper: Google Ads lead identification
-- ============================================================

DROP VIEW IF EXISTS v_google_ads_leads CASCADE;
CREATE OR REPLACE VIEW v_google_ads_leads AS
SELECT
    c.customer_id,
    normalize_phone(c.caller_phone) AS phone_normalized,
    c.callrail_id,
    c.start_time,
    c.gclid,
    c.source,
    c.source AS call_source,
    c.lead_score,
    c.callrail_company_id,
    -- Lead quality: is_good_lead from AI classification
    CASE
        WHEN c.lead_score IS NOT NULL AND (c.lead_score->>'is_good_lead')::boolean = true
        THEN true
        ELSE false
    END AS is_quality_lead
FROM calls c
WHERE (
    is_google_ads_call(c.source, c.source_name, c.gclid)
)
AND normalize_phone(c.caller_phone) IS NOT NULL;

-- ============================================================
-- Helper: Google Ads form leads
-- ============================================================

CREATE OR REPLACE VIEW v_google_ads_form_leads AS
SELECT
    f.customer_id,
    f.callrail_company_id,
    normalize_phone(f.customer_phone) AS phone_normalized,
    f.callrail_id,
    f.submitted_at,
    f.source,
    f.lead_score,
    CASE
        WHEN f.lead_score IS NOT NULL AND (f.lead_score->>'is_good_lead')::boolean = true
        THEN true
        ELSE false
    END AS is_quality_lead
FROM form_submissions f
WHERE (
    f.source ILIKE '%google%'
    OR f.gclid IS NOT NULL
)
AND normalize_phone(f.customer_phone) IS NOT NULL;

-- ============================================================
-- Core: HCP customers matched to Google Ads leads
-- ============================================================

CREATE OR REPLACE VIEW v_hcp_google_ads_matched AS
SELECT
    hc.hcp_customer_id,
    hc.customer_id,
    hc.first_name,
    hc.last_name,
    hc.phone_normalized,
    hc.hcp_created_at,
    -- Earliest CallRail interaction for this phone
    gal.callrail_id AS matched_call_id,
    gal.start_time AS callrail_first_contact,
    gal.gclid,
    gal.is_quality_lead,
    -- Lead date = earlier of CallRail first contact or HCP customer creation
    LEAST(gal.start_time, hc.hcp_created_at) AS lead_date
FROM hcp_customers hc
JOIN LATERAL (
    SELECT
        g.callrail_id,
        g.start_time,
        g.gclid,
        g.is_quality_lead
    FROM v_google_ads_leads g
    WHERE g.phone_normalized = hc.phone_normalized
    ORDER BY g.start_time ASC
    LIMIT 1
) gal ON true;

-- ============================================================
-- Dashboard: Funnel metrics per client per date range
-- ============================================================
-- Usage: SELECT * FROM v_hcp_funnel WHERE customer_id = 'XXX'
--        AND lead_date >= '2026-01-01' AND lead_date < '2026-02-01';

CREATE OR REPLACE VIEW v_hcp_funnel AS
SELECT
    m.customer_id,
    m.hcp_customer_id,
    m.lead_date,
    m.is_quality_lead,
    m.gclid,

    -- Inspection data
    insp.inspection_scheduled,
    insp.inspection_completed,
    insp.inspection_revenue_cents,

    -- Estimate data
    est.estimate_sent,
    est.estimate_approved,
    est.highest_option_cents,
    est.approved_total_cents,

    -- Job data
    j.job_scheduled,
    j.job_completed,
    j.job_revenue_cents

FROM v_hcp_google_ads_matched m

-- Inspections
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) FILTER (WHERE i.status IN ('scheduled', 'completed')) AS inspection_scheduled,
        COUNT(*) FILTER (WHERE i.status = 'completed') AS inspection_completed,
        COALESCE(SUM(i.total_amount_cents) FILTER (WHERE i.status = 'completed'), 0) AS inspection_revenue_cents
    FROM hcp_inspections i
    WHERE i.hcp_customer_id = m.hcp_customer_id
) insp ON true

-- Estimates
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) AS estimate_sent,
        COUNT(*) FILTER (WHERE e.status = 'approved') AS estimate_approved,
        COALESCE(SUM(e.highest_option_cents), 0) AS highest_option_cents,
        COALESCE(SUM(e.approved_total_cents) FILTER (WHERE e.status = 'approved'), 0) AS approved_total_cents
    FROM hcp_estimates e
    WHERE e.hcp_customer_id = m.hcp_customer_id
) est ON true

-- Jobs (distinct by hcp_job_id to handle segments)
LEFT JOIN LATERAL (
    SELECT
        COUNT(DISTINCT j2.hcp_job_id) FILTER (WHERE j2.status IN ('scheduled', 'completed')) AS job_scheduled,
        COUNT(DISTINCT j2.hcp_job_id) FILTER (WHERE j2.status = 'completed') AS job_completed,
        COALESCE(SUM(j2.total_amount_cents) FILTER (WHERE j2.status = 'completed' AND NOT j2.is_segment), 0) AS job_revenue_cents
    FROM hcp_jobs j2
    WHERE j2.hcp_customer_id = m.hcp_customer_id
) j ON true;

-- ============================================================
-- Dashboard: Aggregated metrics per client
-- ============================================================
-- Usage: SELECT * FROM v_hcp_client_summary
--        WHERE customer_id = 'XXX' AND period_start >= '2026-01-01';

CREATE OR REPLACE VIEW v_hcp_client_summary AS
SELECT
    f.customer_id,
    DATE_TRUNC('month', f.lead_date) AS period_start,

    -- Contacts: distinct phones from Google Ads leads
    COUNT(DISTINCT f.hcp_customer_id) AS contacts,

    -- Quality leads
    COUNT(DISTINCT f.hcp_customer_id) FILTER (WHERE f.is_quality_lead) AS quality_leads,

    -- Inspections
    SUM(f.inspection_scheduled) AS inspections_scheduled,
    SUM(f.inspection_completed) AS inspections_completed,

    -- Estimates
    SUM(f.estimate_sent) AS estimates_sent,
    SUM(f.estimate_approved) AS estimates_approved,

    -- Jobs
    SUM(f.job_scheduled) AS jobs_scheduled,
    SUM(f.job_completed) AS jobs_completed,

    -- Revenue (cents → dollars in dashboard layer, but provide both)
    SUM(f.inspection_revenue_cents) AS inspection_revenue_cents,
    SUM(f.approved_total_cents) AS estimate_approved_revenue_cents,
    SUM(f.job_revenue_cents) AS job_revenue_cents,

    -- Closed revenue = inspection revenue (< $1000 items) + approved estimate revenue
    SUM(
        CASE WHEN f.inspection_revenue_cents < 100000 THEN f.inspection_revenue_cents ELSE 0 END
    ) + SUM(f.approved_total_cents) AS closed_revenue_cents,

    -- Estimate open = sent-but-not-approved highest option amounts
    SUM(f.highest_option_cents) FILTER (WHERE f.estimate_sent > 0 AND f.estimate_approved = 0) AS estimate_open_cents

FROM v_hcp_funnel f
GROUP BY f.customer_id, DATE_TRUNC('month', f.lead_date);

-- ============================================================
-- Dashboard: ROAS view (joins with Google Ads cost data)
-- ============================================================

DROP VIEW IF EXISTS v_hcp_roas CASCADE;
CREATE OR REPLACE VIEW v_hcp_roas AS
SELECT
    cs.customer_id,
    cs.period_start,
    cs.contacts,
    cs.quality_leads,
    cs.inspections_scheduled,
    cs.inspections_completed,
    cs.estimates_sent,
    cs.estimates_approved,
    cs.jobs_scheduled,
    cs.jobs_completed,
    cs.closed_revenue_cents,
    cs.estimate_open_cents,

    -- Google Ads cost for the period (stored as dollars in account_daily_metrics.cost)
    adm.total_cost,
    COALESCE(adm.total_cost, 0) AS ad_spend_dollars,

    -- Cost per lead
    CASE
        WHEN cs.quality_leads > 0
        THEN adm.total_cost / cs.quality_leads
        ELSE NULL
    END AS cost_per_lead,

    -- ROAS
    CASE
        WHEN adm.total_cost > 0
        THEN (cs.closed_revenue_cents / 100.0) / adm.total_cost
        ELSE NULL
    END AS roas

FROM v_hcp_client_summary cs
LEFT JOIN LATERAL (
    SELECT SUM(a.cost) AS total_cost
    FROM account_daily_metrics a
    WHERE a.customer_id = cs.customer_id
      AND a.date >= cs.period_start
      AND a.date < cs.period_start + INTERVAL '1 month'
) adm ON true;

COMMIT;
