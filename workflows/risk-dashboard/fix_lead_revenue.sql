-- Fix v_lead_revenue: exclude GBP from google_ads attribution
-- Two changes:
-- 1. Call-side: add GBP source_name exclusion to google_ads tagging
-- 2. Form-side: change source ~~* '%google%' to exact 'Google Ads' match

CREATE OR REPLACE VIEW v_lead_revenue AS
-- Part 1: HCP customers matched to CallRail (in_funnel leads)
SELECT hc.hcp_customer_id,
   hc.customer_id,
   hc.first_name,
   hc.last_name,
   hc.callrail_id,
   hc.match_method,
   hc.attribution_override,
   'in_funnel'::text AS lead_status,
   COALESCE(hc.attribution_override,
       CASE
           WHEN (EXISTS ( SELECT 1
              FROM calls c
             WHERE c.callrail_id = hc.callrail_id AND c.source_name = 'LSA')) THEN 'lsa'
           -- GBP/GMB calls: check for multi-touch (prior GCLID interaction = google_ads)
           WHEN (EXISTS ( SELECT 1
              FROM calls c
             WHERE c.callrail_id = hc.callrail_id
               AND (c.source_name ~~* '%gmb%' OR c.source_name ~~* '%gbp%' OR c.source_name = 'Main Business Line')
           )) THEN
             CASE WHEN EXISTS (
               SELECT 1 FROM calls c2
               WHERE c2.customer_id = hc.customer_id
                 AND normalize_phone(c2.caller_phone) = hc.phone_normalized
                 AND c2.gclid IS NOT NULL
                 AND c2.source_name NOT IN ('GBP', 'GMB Call Extension', 'Main Business Line')
                 AND c2.source_name NOT ILIKE '%gmb%' AND c2.source_name NOT ILIKE '%gbp%'
             ) THEN 'google_ads'
             WHEN EXISTS (
               SELECT 1 FROM form_submissions f
               WHERE f.customer_id = hc.customer_id
                 AND (normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email)))
                 AND (f.gclid IS NOT NULL OR f.source = 'Google Ads')
             ) THEN 'google_ads'
             ELSE 'google_business_profile'
             END
           WHEN (EXISTS ( SELECT 1
              FROM calls c
             WHERE c.callrail_id = hc.callrail_id
               AND is_google_ads_call(c.source, c.source_name, c.gclid)
           )) THEN 'google_ads'
           WHEN (EXISTS ( SELECT 1
              FROM form_submissions f
             WHERE f.customer_id = hc.customer_id
               AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email)))
               AND (f.gclid IS NOT NULL OR f.source = 'Google Ads')
           )) THEN 'google_ads'
           WHEN hc.callrail_id LIKE 'WF_%' THEN 'google_ads'
           -- GHL GCLID fallback: lead bypassed CallRail but GHL captured Google Click ID
           WHEN EXISTS (
             SELECT 1 FROM ghl_contacts gc
             WHERE gc.customer_id = hc.customer_id
               AND (gc.phone_normalized = hc.phone_normalized
                    OR (hc.email IS NOT NULL AND hc.email != '' AND LOWER(gc.email) = LOWER(hc.email)))
               AND gc.gclid IS NOT NULL AND gc.gclid != ''
           ) THEN 'google_ads'
           ELSE 'unknown'
       END) AS lead_source_type,
   -- Inspection invoices
   COALESCE(( SELECT sum(i.amount_cents)
          FROM hcp_invoices i
         WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'inspection'), 0::bigint) AS inspection_invoice_cents,
   -- Treatment invoices
   COALESCE(( SELECT sum(i.amount_cents)
          FROM hcp_invoices i
         WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'treatment'), 0::bigint) AS treatment_invoice_cents,
   -- All invoices
   COALESCE(( SELECT sum(i.amount_cents)
          FROM hcp_invoices i
         WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled'), 0::bigint) AS invoice_total_cents,
   -- Job totals
   COALESCE(( SELECT sum(sub.total)
          FROM ( SELECT j.total_amount_cents + COALESCE(( SELECT sum(s.total_amount_cents)
                          FROM hcp_job_segments s
                         WHERE s.parent_hcp_job_id = j.hcp_job_id AND s.count_revenue = true AND (s.status <> ALL (ARRAY['user canceled', 'pro canceled']))), 0::bigint) AS total
                  FROM hcp_jobs j
                 WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND (j.status <> ALL (ARRAY['user canceled', 'pro canceled'])) AND j.count_revenue = true) sub), 0::numeric)::bigint AS job_total_cents,
   -- Inspection totals
   COALESCE(( SELECT sum(i.total_amount_cents)
          FROM hcp_inspections i
         WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.record_status = 'active' AND i.count_revenue = true AND (i.status <> ALL (ARRAY['user canceled', 'pro canceled']))), 0::bigint) AS inspection_total_cents,
   -- Inferred inspection fee: inspections with fee where an estimate has been sent (proof inspection happened)
   -- Guards: fee < $1500, fee < lowest estimate amount (to avoid misclassified jobs)
   CASE WHEN COALESCE(( SELECT sum(i.amount_cents) FROM hcp_invoices i
         WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'inspection'), 0) = 0
     AND EXISTS (SELECT 1 FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.record_status = 'active' AND e.status IN ('sent','approved'))
     THEN COALESCE(( SELECT sum(i.total_amount_cents) FROM hcp_inspections i
         WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.record_status = 'active'
           AND i.count_revenue = true AND (i.status <> ALL (ARRAY['user canceled', 'pro canceled']))
           AND i.total_amount_cents > 0 AND i.total_amount_cents < 150000
           AND i.total_amount_cents < COALESCE(NULLIF(( SELECT MIN(GREATEST(e2.approved_total_cents, e2.highest_option_cents)) FROM hcp_estimates e2
               WHERE e2.hcp_customer_id = hc.hcp_customer_id AND e2.record_status = 'active' AND e2.status IN ('sent','approved') AND e2.count_revenue = true), 0), 999999999)
     ), 0) ELSE 0 END::bigint AS inspection_fee_inferred_cents,
   CASE WHEN COALESCE(( SELECT sum(i.amount_cents) FROM hcp_invoices i
         WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'inspection'), 0) = 0
     AND EXISTS (SELECT 1 FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.record_status = 'active' AND e.status IN ('sent','approved'))
     AND COALESCE(( SELECT sum(i.total_amount_cents) FROM hcp_inspections i
         WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.record_status = 'active'
           AND i.count_revenue = true AND (i.status <> ALL (ARRAY['user canceled', 'pro canceled']))
           AND i.total_amount_cents > 0 AND i.total_amount_cents < 150000
           AND i.total_amount_cents < COALESCE(NULLIF(( SELECT MIN(GREATEST(e2.approved_total_cents, e2.highest_option_cents)) FROM hcp_estimates e2
               WHERE e2.hcp_customer_id = hc.hcp_customer_id AND e2.record_status = 'active' AND e2.status IN ('sent','approved') AND e2.count_revenue = true), 0), 999999999)
     ), 0) > 0
     THEN true ELSE false END AS inspection_revenue_inferred,
   -- Approved estimates
   COALESCE(( SELECT sum(e.approved_total_cents)
          FROM hcp_estimates e
         WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'approved' AND e.record_status = 'active' AND e.count_revenue = true), 0::bigint) AS approved_estimate_cents,
   -- Pipeline estimates (sent, not approved)
   COALESCE(( SELECT sum(e.highest_option_cents)
          FROM hcp_estimates e
         WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'sent' AND e.record_status = 'active' AND e.estimate_type = 'treatment' AND e.count_revenue = true), 0::bigint) AS pipeline_estimate_cents,
   -- ROAS revenue: (inspection_inv OR inferred_inspection_fee) + GREATEST(treatment_inv, approved_est), fallback to job_total
   (COALESCE(( SELECT sum(i.amount_cents)
          FROM hcp_invoices i
         WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'inspection'), 0::bigint)
   + CASE WHEN COALESCE(( SELECT sum(i.amount_cents) FROM hcp_invoices i
         WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'inspection'), 0) = 0
     AND EXISTS (SELECT 1 FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.record_status = 'active' AND e.status IN ('sent','approved'))
     THEN COALESCE(( SELECT sum(i2.total_amount_cents) FROM hcp_inspections i2
         WHERE i2.hcp_customer_id = hc.hcp_customer_id AND i2.record_status = 'active'
           AND i2.count_revenue = true AND (i2.status <> ALL (ARRAY['user canceled', 'pro canceled']))
           AND i2.total_amount_cents > 0 AND i2.total_amount_cents < 150000
           AND i2.total_amount_cents < COALESCE(NULLIF(( SELECT MIN(GREATEST(e2.approved_total_cents, e2.highest_option_cents)) FROM hcp_estimates e2
               WHERE e2.hcp_customer_id = hc.hcp_customer_id AND e2.record_status = 'active' AND e2.status IN ('sent','approved') AND e2.count_revenue = true), 0), 999999999)
     ), 0) ELSE 0 END)::numeric +
       CASE
           WHEN COALESCE(( SELECT sum(i.amount_cents)
              FROM hcp_invoices i
             WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'treatment'), 0::bigint) > 0 OR COALESCE(( SELECT sum(e.approved_total_cents)
              FROM hcp_estimates e
             WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'approved' AND e.record_status = 'active' AND e.count_revenue = true), 0::bigint) > 0 THEN GREATEST(COALESCE(( SELECT sum(i.amount_cents)
              FROM hcp_invoices i
             WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'treatment'), 0::bigint), COALESCE(( SELECT sum(e.approved_total_cents)
              FROM hcp_estimates e
             WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'approved' AND e.record_status = 'active' AND e.count_revenue = true), 0::bigint))::numeric
           ELSE COALESCE(( SELECT sum(sub2.total)
              FROM ( SELECT j.total_amount_cents + COALESCE(( SELECT sum(s.total_amount_cents)
                              FROM hcp_job_segments s
                             WHERE s.parent_hcp_job_id = j.hcp_job_id AND s.count_revenue = true AND (s.status <> ALL (ARRAY['user canceled', 'pro canceled']))), 0::bigint) AS total
                      FROM hcp_jobs j
                     WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND (j.status <> ALL (ARRAY['user canceled', 'pro canceled'])) AND j.count_revenue = true) sub2), 0::numeric) + COALESCE(( SELECT sum(ins.total_amount_cents)
              FROM hcp_inspections ins
             WHERE ins.hcp_customer_id = hc.hcp_customer_id AND ins.record_status = 'active' AND ins.count_revenue = true AND (ins.status <> ALL (ARRAY['user canceled', 'pro canceled']))), 0::bigint)::numeric
       END AS roas_revenue_cents,
       CASE
           WHEN COALESCE(( SELECT sum(i.amount_cents)
              FROM hcp_invoices i
             WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'treatment'), 0::bigint) >= COALESCE(( SELECT sum(e.approved_total_cents)
              FROM hcp_estimates e
             WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'approved' AND e.record_status = 'active' AND e.count_revenue = true), 0::bigint) AND COALESCE(( SELECT sum(i.amount_cents)
              FROM hcp_invoices i
             WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'treatment'), 0::bigint) > 0 THEN 'invoice'
           WHEN COALESCE(( SELECT sum(e.approved_total_cents)
              FROM hcp_estimates e
             WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'approved' AND e.record_status = 'active' AND e.count_revenue = true), 0::bigint) > 0 THEN 'approved_estimate'
           WHEN COALESCE(( SELECT sum(j.total_amount_cents)
              FROM hcp_jobs j
             WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND (j.status <> ALL (ARRAY['user canceled', 'pro canceled'])) AND j.count_revenue = true), 0::bigint) > 0 THEN 'job'
           WHEN COALESCE(( SELECT sum(i.amount_cents)
              FROM hcp_invoices i
             WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'inspection'), 0::bigint) > 0 THEN 'inspection_only'
           ELSE 'none'
       END AS revenue_source
  FROM hcp_customers hc
 WHERE hc.attribution_override IS NOT NULL
    OR (EXISTS ( SELECT 1
          FROM calls c
         WHERE c.callrail_id = hc.callrail_id AND (is_google_ads_call(c.source, c.source_name, c.gclid) OR c.source_name = 'LSA' OR c.source_name ~~* '%gmb%' OR c.source_name ~~* '%gbp%' OR c.source_name = 'Main Business Line')))
    OR (EXISTS ( SELECT 1
          FROM form_submissions f
         WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND (f.gclid IS NOT NULL OR f.source = 'Google Ads')))
    OR (hc.callrail_id LIKE 'WF_%')
    -- GHL GCLID fallback: include leads that bypassed CallRail but GHL captured Google Click ID
    OR (EXISTS ( SELECT 1
          FROM ghl_contacts gc
         WHERE gc.customer_id = hc.customer_id
           AND (gc.phone_normalized = hc.phone_normalized
                OR (hc.email IS NOT NULL AND hc.email != '' AND LOWER(gc.email) = LOWER(hc.email)))
           AND gc.gclid IS NOT NULL AND gc.gclid != ''))
UNION ALL
-- Part 2: CallRail calls with no HCP match (lead_only)
SELECT NULL::text AS hcp_customer_id,
   c.customer_id,
   NULL::text AS first_name,
   c.customer_name AS last_name,
   c.callrail_id,
   'call'::text AS match_method,
   NULL::text AS attribution_override,
   'lead_only'::text AS lead_status,
   CASE
     WHEN c.source_name = 'LSA' THEN 'lsa'
     -- GBP/GMB calls: multi-touch check
     WHEN c.source_name ~~* '%gmb%' OR c.source_name ~~* '%gbp%' OR c.source_name = 'Main Business Line' THEN
       CASE WHEN EXISTS (
         SELECT 1 FROM calls c3
         WHERE c3.customer_id = c.customer_id
           AND c3.caller_phone = c.caller_phone
           AND c3.gclid IS NOT NULL
           AND c3.source_name NOT IN ('GBP', 'GMB Call Extension', 'Main Business Line')
           AND c3.source_name NOT ILIKE '%gmb%' AND c3.source_name NOT ILIKE '%gbp%'
       ) THEN 'google_ads'
       ELSE 'google_business_profile'
       END
     WHEN is_google_ads_call(c.source, c.source_name, c.gclid)
       THEN 'google_ads'
     ELSE 'unknown'
   END AS lead_source_type,
   0::bigint AS inspection_invoice_cents,
   0::bigint AS treatment_invoice_cents,
   0::bigint AS invoice_total_cents,
   0::bigint AS job_total_cents,
   0::bigint AS inspection_total_cents,
   0::bigint AS inspection_fee_inferred_cents,
   false AS inspection_revenue_inferred,
   0::bigint AS approved_estimate_cents,
   0::bigint AS pipeline_estimate_cents,
   0::numeric AS roas_revenue_cents,
   'none'::text AS revenue_source
FROM calls c
WHERE c.classification = 'legitimate'
  AND c.customer_id IS NOT NULL
  AND (is_google_ads_call(c.source, c.source_name, c.gclid) OR c.source_name ~~* '%gmb%' OR c.source_name ~~* '%gbp%' OR c.source_name = 'Main Business Line')
  AND NOT EXISTS (
    SELECT 1 FROM hcp_customers hc
    WHERE hc.callrail_id = c.callrail_id
      AND hc.customer_id = c.customer_id
  )
UNION ALL
-- Part 3: Form submissions with no HCP match (lead_only)
SELECT NULL::text AS hcp_customer_id,
   f.customer_id,
   NULL::text AS first_name,
   f.customer_name AS last_name,
   f.callrail_id,
   'form'::text AS match_method,
   NULL::text AS attribution_override,
   'lead_only'::text AS lead_status,
   CASE
     WHEN f.source = 'Google Ads' OR f.gclid IS NOT NULL THEN 'google_ads'
     WHEN f.source = 'Google My Business' THEN 'google_business_profile'
     WHEN f.source ILIKE '%google%' THEN 'google_organic'
     ELSE 'unknown'
   END AS lead_source_type,
   0::bigint AS inspection_invoice_cents,
   0::bigint AS treatment_invoice_cents,
   0::bigint AS invoice_total_cents,
   0::bigint AS job_total_cents,
   0::bigint AS inspection_total_cents,
   0::bigint AS inspection_fee_inferred_cents,
   false AS inspection_revenue_inferred,
   0::bigint AS approved_estimate_cents,
   0::bigint AS pipeline_estimate_cents,
   0::numeric AS roas_revenue_cents,
   'none'::text AS revenue_source
FROM form_submissions f
WHERE f.classification = 'legitimate'
  AND f.customer_id IS NOT NULL
  AND (f.gclid IS NOT NULL OR f.source = 'Google Ads')
  AND NOT EXISTS (
    SELECT 1 FROM hcp_customers hc
    WHERE hc.callrail_id = f.callrail_id
      AND hc.customer_id = f.customer_id
  )
  AND NOT EXISTS (
    SELECT 1 FROM calls c
    WHERE normalize_phone(c.caller_phone) = normalize_phone(f.customer_phone)
      AND c.customer_id = f.customer_id
      AND is_google_ads_call(c.source, c.source_name, c.gclid)
  );
