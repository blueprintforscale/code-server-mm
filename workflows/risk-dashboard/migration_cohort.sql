-- Cohort Performance Metrics
-- Returns quality leads, ad spend, revenue, and CPL per client per program month
-- Used by the /api/cohort endpoint

CREATE OR REPLACE FUNCTION get_cohort_metrics()
RETURNS TABLE (
  customer_id BIGINT,
  client_name TEXT,
  start_date DATE,
  program_month INT,
  month_start DATE,
  month_end DATE,
  cal_month INT,
  contacts INT,
  spam_count INT,
  abandoned_count INT,
  excluded_abandoned INT,
  quality_leads INT,
  ad_spend NUMERIC,
  revenue_cents BIGINT,
  rev_per_lead NUMERIC,
  cpl NUMERIC
)
LANGUAGE sql STABLE
AS $function$

WITH cohort_clients AS (
  SELECT
    c.customer_id,
    c.name AS client_name,
    c.start_date,
    c.extra_spam_keywords
  FROM clients c
  WHERE c.start_date IS NOT NULL
    AND c.callrail_company_id IS NOT NULL
    AND c.status = 'active'
),

-- Generate program months 1-12 for each client
client_months AS (
  SELECT
    cc.customer_id, cc.client_name, cc.start_date, cc.extra_spam_keywords,
    gs + 1 AS program_month,
    (cc.start_date + (gs || ' months')::interval)::date AS month_start,
    (cc.start_date + ((gs + 1) || ' months')::interval)::date AS month_end,
    EXTRACT(MONTH FROM cc.start_date + (gs || ' months')::interval)::int AS cal_month
  FROM cohort_clients cc
  CROSS JOIN generate_series(0, 11) gs
  -- Only complete months (month_end must be in the past)
  WHERE (cc.start_date + ((gs + 1) || ' months')::interval)::date < DATE_TRUNC('month', CURRENT_DATE)::date
),

-- Google Ads call contacts
ga_calls AS (
  SELECT ca.customer_id, normalize_phone(ca.caller_phone) AS dedup_phone, ca.start_time::date AS lead_date
  FROM calls ca
  WHERE (ca.source IN ('Google Ads', 'Google Ads 2') OR (ca.gclid IS NOT NULL AND ca.gclid != ''))
    AND ca.customer_id IN (SELECT customer_id FROM cohort_clients)
),

-- Google Ads form contacts (deduped against calls)
ga_forms AS (
  SELECT fs.customer_id,
    COALESCE(normalize_phone(fs.customer_phone), 'form_' || fs.callrail_id) AS dedup_phone,
    fs.submitted_at::date AS lead_date
  FROM form_submissions fs
  WHERE fs.gclid IS NOT NULL AND fs.gclid != ''
    AND fs.customer_id IN (SELECT customer_id FROM cohort_clients)
    AND NOT EXISTS (
      SELECT 1 FROM ga_calls gc
      WHERE gc.customer_id = fs.customer_id
        AND gc.dedup_phone = COALESCE(normalize_phone(fs.customer_phone), 'form_' || fs.callrail_id)
    )
),

-- All contacts with first lead date
all_contacts AS (
  SELECT customer_id, dedup_phone, MIN(lead_date) AS first_lead_date
  FROM ga_calls GROUP BY 1, 2
  UNION ALL
  SELECT customer_id, dedup_phone, MIN(lead_date) AS first_lead_date
  FROM ga_forms GROUP BY 1, 2
),

-- Core spam phones (always excluded)
spam_phones AS (
  SELECT DISTINCT gc.customer_id, gc.phone_normalized AS dedup_phone
  FROM ghl_contacts gc
  WHERE LOWER(COALESCE(gc.lost_reason, '')) SIMILAR TO '%(spam|not a lead|wrong number|out of area|wrong service)%'
    AND gc.phone_normalized IS NOT NULL
  UNION
  SELECT DISTINCT o.customer_id, gc.phone_normalized AS dedup_phone
  FROM ghl_opportunities o
  JOIN ghl_contacts gc ON gc.ghl_contact_id = o.ghl_contact_id
  WHERE LOWER(o.stage_name) SIMILAR TO '%(spam|not a lead|out of area|wrong service)%'
    AND gc.phone_normalized IS NOT NULL
),

-- Abandoned phones (conditionally excluded)
abandoned_phones AS (
  SELECT DISTINCT gc.customer_id, gc.phone_normalized AS dedup_phone
  FROM ghl_contacts gc
  WHERE LOWER(COALESCE(gc.lost_reason, '')) LIKE '%abandoned%'
    AND gc.phone_normalized IS NOT NULL
  UNION
  SELECT DISTINCT o.customer_id, gc.phone_normalized AS dedup_phone
  FROM ghl_opportunities o
  JOIN ghl_contacts gc ON gc.ghl_contact_id = o.ghl_contact_id
  WHERE o.status = 'abandoned'
    AND gc.phone_normalized IS NOT NULL
),

-- Revenue per phone (from HCP via v_lead_revenue)
phone_revenue AS (
  SELECT
    lr.customer_id,
    normalize_phone(ca.caller_phone) AS dedup_phone,
    SUM(COALESCE(lr.roas_revenue_cents, 0)) AS revenue_cents
  FROM v_lead_revenue lr
  LEFT JOIN calls ca ON ca.callrail_id = lr.callrail_id
  WHERE lr.lead_source_type = 'google_ads'
    AND lr.customer_id IN (SELECT customer_id FROM cohort_clients)
    AND normalize_phone(ca.caller_phone) IS NOT NULL
  GROUP BY lr.customer_id, normalize_phone(ca.caller_phone)
),

-- Ad spend per client per program month
monthly_spend AS (
  SELECT
    cm.customer_id,
    cm.program_month,
    COALESCE(SUM(cdm.cost), 0) AS ad_spend
  FROM client_months cm
  LEFT JOIN campaign_daily_metrics cdm ON cdm.customer_id = cm.customer_id
    AND cdm.date >= cm.month_start AND cdm.date < cm.month_end
  GROUP BY cm.customer_id, cm.program_month
),

-- Aggregate contacts, spam, abandoned, revenue per client-month
month_data AS (
  SELECT
    cm.customer_id, cm.client_name, cm.start_date, cm.extra_spam_keywords,
    cm.program_month, cm.month_start, cm.month_end, cm.cal_month,
    COUNT(DISTINCT ac.dedup_phone)::int AS contacts,
    COUNT(DISTINCT ac.dedup_phone) FILTER (
      WHERE EXISTS (SELECT 1 FROM spam_phones sp WHERE sp.customer_id = ac.customer_id AND sp.dedup_phone = ac.dedup_phone)
    )::int AS spam_count,
    COUNT(DISTINCT ac.dedup_phone) FILTER (
      WHERE EXISTS (SELECT 1 FROM abandoned_phones ap WHERE ap.customer_id = ac.customer_id AND ap.dedup_phone = ac.dedup_phone)
        AND NOT EXISTS (SELECT 1 FROM spam_phones sp WHERE sp.customer_id = ac.customer_id AND sp.dedup_phone = ac.dedup_phone)
    )::int AS abandoned_count,
    COALESCE(SUM(pr.revenue_cents) FILTER (
      WHERE NOT EXISTS (SELECT 1 FROM spam_phones sp WHERE sp.customer_id = ac.customer_id AND sp.dedup_phone = ac.dedup_phone)
    ), 0)::bigint AS revenue_cents
  FROM client_months cm
  LEFT JOIN all_contacts ac ON ac.customer_id = cm.customer_id
    AND ac.first_lead_date >= cm.month_start AND ac.first_lead_date < cm.month_end
  LEFT JOIN phone_revenue pr ON pr.customer_id = ac.customer_id AND pr.dedup_phone = ac.dedup_phone
  GROUP BY cm.customer_id, cm.client_name, cm.start_date, cm.extra_spam_keywords,
    cm.program_month, cm.month_start, cm.month_end, cm.cal_month
)

SELECT
  md.customer_id,
  md.client_name,
  md.start_date,
  md.program_month,
  md.month_start,
  md.month_end,
  md.cal_month,
  md.contacts,
  md.spam_count,
  md.abandoned_count,
  -- Excluded abandoned: apply 20% rule or extra_spam_keywords
  CASE
    WHEN 'abandoned' = ANY(md.extra_spam_keywords) THEN md.abandoned_count
    WHEN md.contacts > 0 AND (md.abandoned_count::numeric / NULLIF(md.contacts - md.spam_count, 0)) > 0.20 THEN md.abandoned_count
    ELSE 0
  END::int AS excluded_abandoned,
  -- Quality leads
  (md.contacts - md.spam_count -
    CASE
      WHEN 'abandoned' = ANY(md.extra_spam_keywords) THEN md.abandoned_count
      WHEN md.contacts > 0 AND (md.abandoned_count::numeric / NULLIF(md.contacts - md.spam_count, 0)) > 0.20 THEN md.abandoned_count
      ELSE 0
    END
  )::int AS quality_leads,
  ROUND(ms.ad_spend, 2) AS ad_spend,
  md.revenue_cents,
  -- Revenue per lead
  CASE WHEN (md.contacts - md.spam_count -
    CASE WHEN 'abandoned' = ANY(md.extra_spam_keywords) THEN md.abandoned_count
         WHEN md.contacts > 0 AND (md.abandoned_count::numeric / NULLIF(md.contacts - md.spam_count, 0)) > 0.20 THEN md.abandoned_count
         ELSE 0 END) > 0
    THEN ROUND(md.revenue_cents / 100.0 / (md.contacts - md.spam_count -
      CASE WHEN 'abandoned' = ANY(md.extra_spam_keywords) THEN md.abandoned_count
           WHEN md.contacts > 0 AND (md.abandoned_count::numeric / NULLIF(md.contacts - md.spam_count, 0)) > 0.20 THEN md.abandoned_count
           ELSE 0 END), 2)
    ELSE 0
  END AS rev_per_lead,
  -- CPL
  CASE WHEN (md.contacts - md.spam_count -
    CASE WHEN 'abandoned' = ANY(md.extra_spam_keywords) THEN md.abandoned_count
         WHEN md.contacts > 0 AND (md.abandoned_count::numeric / NULLIF(md.contacts - md.spam_count, 0)) > 0.20 THEN md.abandoned_count
         ELSE 0 END) > 0 AND ms.ad_spend > 0
    THEN ROUND(ms.ad_spend / (md.contacts - md.spam_count -
      CASE WHEN 'abandoned' = ANY(md.extra_spam_keywords) THEN md.abandoned_count
           WHEN md.contacts > 0 AND (md.abandoned_count::numeric / NULLIF(md.contacts - md.spam_count, 0)) > 0.20 THEN md.abandoned_count
           ELSE 0 END), 2)
    ELSE 0
  END AS cpl
FROM month_data md
LEFT JOIN monthly_spend ms ON ms.customer_id = md.customer_id AND ms.program_month = md.program_month
ORDER BY md.client_name, md.program_month;

$function$;
