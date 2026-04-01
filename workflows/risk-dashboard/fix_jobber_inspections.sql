-- Fix v_jobber_lead_revenue: count inspection-titled jobs in addition to request assessments
-- Mold Cure and Jake & Mel track inspections as jobs ("Free Instascope Assessment", etc.)

CREATE OR REPLACE VIEW v_jobber_lead_revenue AS
SELECT
  jc.jobber_customer_id,
  jc.customer_id,
  jc.first_name,
  jc.last_name,
  jc.callrail_id,
  jc.match_method,
  jc.attribution_override,
  CASE WHEN jc.callrail_id IS NOT NULL THEN 'in_funnel' ELSE 'lead_only' END AS lead_status,
  COALESCE(jc.attribution_override,
    CASE
      WHEN EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = jc.callrail_id AND c.source_name = 'LSA') THEN 'lsa'
      WHEN EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = jc.callrail_id
        AND (c.source_name ~~* '%gmb%' OR c.source_name ~~* '%gbp%' OR c.source_name = 'Main Business Line'))
        AND NOT EXISTS (SELECT 1 FROM calls c2 WHERE c2.customer_id = jc.customer_id
          AND c2.caller_phone = (SELECT c3.caller_phone FROM calls c3 WHERE c3.callrail_id = jc.callrail_id LIMIT 1)
          AND c2.gclid IS NOT NULL
          AND c2.source_name NOT IN ('GBP','GMB Call Extension','Main Business Line')
          AND c2.source_name NOT ILIKE '%gmb%' AND c2.source_name NOT ILIKE '%gbp%')
        THEN 'google_business_profile'
      WHEN EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = jc.callrail_id
        AND is_google_ads_call(c.source, c.source_name, c.gclid))
        THEN 'google_ads'
      WHEN EXISTS (SELECT 1 FROM form_submissions f WHERE f.callrail_id = jc.callrail_id
        AND (f.gclid IS NOT NULL OR f.source = 'Google Ads'))
        THEN 'google_ads'
      WHEN jc.callrail_id LIKE 'WF_%' THEN 'google_ads'
      ELSE 'unknown'
    END
  ) AS lead_source_type,
  -- Inspection invoices (< $1000)
  COALESCE((SELECT SUM(i.total_cents) FROM jobber_invoices i
    WHERE i.jobber_customer_id = jc.jobber_customer_id AND i.customer_id = jc.customer_id
    AND i.total_cents < 100000), 0)::bigint AS inspection_invoice_cents,
  -- Treatment invoices (>= $1000)
  COALESCE((SELECT SUM(i.total_cents) FROM jobber_invoices i
    WHERE i.jobber_customer_id = jc.jobber_customer_id AND i.customer_id = jc.customer_id
    AND i.total_cents >= 100000), 0)::bigint AS treatment_invoice_cents,
  -- All invoices
  COALESCE((SELECT SUM(i.total_cents) FROM jobber_invoices i
    WHERE i.jobber_customer_id = jc.jobber_customer_id AND i.customer_id = jc.customer_id), 0)::bigint AS invoice_total_cents,
  -- Approved quotes
  COALESCE((SELECT SUM(q.total_cents) FROM jobber_quotes q
    WHERE q.jobber_customer_id = jc.jobber_customer_id AND q.customer_id = jc.customer_id
    AND q.status IN ('approved','converted')), 0)::bigint AS approved_quote_cents,
  -- Pipeline quotes (sent, not approved)
  COALESCE((SELECT SUM(q.total_cents) FROM jobber_quotes q
    WHERE q.jobber_customer_id = jc.jobber_customer_id AND q.customer_id = jc.customer_id
    AND q.status IN ('awaiting_response','draft','sent','changes_requested')), 0)::bigint AS pipeline_quote_cents,
  -- Job count
  COALESCE((SELECT COUNT(*) FROM jobber_jobs j
    WHERE j.jobber_customer_id = jc.jobber_customer_id AND j.customer_id = jc.customer_id), 0)::int AS job_count,
  -- Inspection scheduled: requests with assessments OR inspection-titled jobs
  GREATEST(
    COALESCE((SELECT COUNT(*) FROM jobber_requests jr
      WHERE jr.jobber_customer_id = jc.jobber_customer_id AND jr.has_assessment = true AND jr.assessment_start_at IS NOT NULL), 0),
    COALESCE((SELECT COUNT(*) FROM jobber_jobs jj
      WHERE jj.jobber_customer_id = jc.jobber_customer_id AND jj.customer_id = jc.customer_id
      AND (LOWER(jj.title) LIKE '%assessment%' OR LOWER(jj.title) LIKE '%instascope%'
        OR LOWER(jj.title) LIKE '%inspection%' OR LOWER(jj.title) LIKE '%mold test%'
        OR LOWER(jj.title) LIKE '%air quality%' OR LOWER(jj.title) LIKE '%air test%')), 0)
  )::int AS inspection_scheduled,
  -- Inspection completed
  GREATEST(
    COALESCE((SELECT COUNT(*) FROM jobber_requests jr
      WHERE jr.jobber_customer_id = jc.jobber_customer_id AND jr.assessment_completed_at IS NOT NULL), 0),
    COALESCE((SELECT COUNT(*) FROM jobber_jobs jj
      WHERE jj.jobber_customer_id = jc.jobber_customer_id AND jj.customer_id = jc.customer_id
      AND (LOWER(jj.title) LIKE '%assessment%' OR LOWER(jj.title) LIKE '%instascope%'
        OR LOWER(jj.title) LIKE '%inspection%' OR LOWER(jj.title) LIKE '%mold test%'
        OR LOWER(jj.title) LIKE '%air quality%' OR LOWER(jj.title) LIKE '%air test%')
      AND jj.completed_at IS NOT NULL), 0)
  )::int AS inspection_completed,
  -- ROAS revenue: inspection_inv + GREATEST(treatment_inv, approved_quote), fallback to job total
  COALESCE((SELECT SUM(i.total_cents) FROM jobber_invoices i
    WHERE i.jobber_customer_id = jc.jobber_customer_id AND i.customer_id = jc.customer_id
    AND i.total_cents < 100000), 0)::numeric +
  CASE
    WHEN COALESCE((SELECT SUM(i.total_cents) FROM jobber_invoices i
      WHERE i.jobber_customer_id = jc.jobber_customer_id AND i.customer_id = jc.customer_id
      AND i.total_cents >= 100000), 0) > 0
    OR COALESCE((SELECT SUM(q.total_cents) FROM jobber_quotes q
      WHERE q.jobber_customer_id = jc.jobber_customer_id AND q.customer_id = jc.customer_id
      AND q.status IN ('approved','converted')), 0) > 0
    THEN GREATEST(
      COALESCE((SELECT SUM(i.total_cents) FROM jobber_invoices i
        WHERE i.jobber_customer_id = jc.jobber_customer_id AND i.customer_id = jc.customer_id
        AND i.total_cents >= 100000), 0),
      COALESCE((SELECT SUM(q.total_cents) FROM jobber_quotes q
        WHERE q.jobber_customer_id = jc.jobber_customer_id AND q.customer_id = jc.customer_id
        AND q.status IN ('approved','converted')), 0)
    )::numeric
    ELSE 0::numeric
  END AS roas_revenue_cents,
  CASE
    WHEN COALESCE((SELECT SUM(i.total_cents) FROM jobber_invoices i
      WHERE i.jobber_customer_id = jc.jobber_customer_id AND i.customer_id = jc.customer_id
      AND i.total_cents >= 100000), 0) >= COALESCE((SELECT SUM(q.total_cents) FROM jobber_quotes q
      WHERE q.jobber_customer_id = jc.jobber_customer_id AND q.customer_id = jc.customer_id
      AND q.status IN ('approved','converted')), 0)
    AND COALESCE((SELECT SUM(i.total_cents) FROM jobber_invoices i
      WHERE i.jobber_customer_id = jc.jobber_customer_id AND i.customer_id = jc.customer_id
      AND i.total_cents >= 100000), 0) > 0
      THEN 'invoice'
    WHEN COALESCE((SELECT SUM(q.total_cents) FROM jobber_quotes q
      WHERE q.jobber_customer_id = jc.jobber_customer_id AND q.customer_id = jc.customer_id
      AND q.status IN ('approved','converted')), 0) > 0
      THEN 'approved_estimate'
    WHEN COALESCE((SELECT SUM(i.total_cents) FROM jobber_invoices i
      WHERE i.jobber_customer_id = jc.jobber_customer_id AND i.customer_id = jc.customer_id
      AND i.total_cents < 100000), 0) > 0
      THEN 'inspection_only'
    ELSE 'none'
  END AS revenue_source
FROM jobber_customers jc
WHERE jc.callrail_id IS NOT NULL;
