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
              WHERE c.callrail_id = hc.callrail_id AND c.source_name = 'LSA'::text)) THEN 'lsa'::text
            WHEN (EXISTS ( SELECT 1
               FROM calls c
              WHERE c.callrail_id = hc.callrail_id AND (c.source_name ~~* '%gmb%'::text OR c.source_name ~~* '%gbp%'::text OR c.source_name = 'Main Business Line'::text))) THEN
            CASE
                WHEN (EXISTS ( SELECT 1
                   FROM calls c2
                  WHERE c2.customer_id = hc.customer_id AND normalize_phone(c2.caller_phone) = hc.phone_normalized AND c2.gclid IS NOT NULL AND (c2.source_name <> ALL (ARRAY['GBP'::text, 'GMB Call Extension'::text, 'Main Business Line'::text])) AND c2.source_name !~~* '%gmb%'::text AND c2.source_name !~~* '%gbp%'::text)) THEN 'google_ads'::text
                WHEN (EXISTS ( SELECT 1
                   FROM form_submissions f
                  WHERE f.customer_id = hc.customer_id AND (normalize_phone(f.customer_phone) = hc.phone_normalized OR hc.email IS NOT NULL AND lower(f.customer_email) = lower(hc.email)) AND (f.gclid IS NOT NULL OR f.source = 'Google Ads'::text))) THEN 'google_ads'::text
                ELSE 'google_business_profile'::text
            END
            WHEN (EXISTS ( SELECT 1
               FROM calls c
              WHERE c.callrail_id = hc.callrail_id AND is_google_ads_call(c.source, c.source_name, c.gclid))) THEN 'google_ads'::text
            WHEN (EXISTS ( SELECT 1
               FROM form_submissions f
              WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR hc.email IS NOT NULL AND lower(f.customer_email) = lower(hc.email)) AND (f.gclid IS NOT NULL OR f.source = 'Google Ads'::text))) THEN 'google_ads'::text
            WHEN hc.callrail_id ~~ 'WF_%'::text THEN 'google_ads'::text
            WHEN (EXISTS ( SELECT 1
               FROM ghl_contacts gc
              WHERE gc.customer_id = hc.customer_id AND (gc.phone_normalized = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(gc.email) = lower(hc.email)) AND gc.gclid IS NOT NULL AND gc.gclid <> ''::text)) THEN 'google_ads'::text
            ELSE 'unknown'::text
        END) AS lead_source_type,
    COALESCE(( SELECT sum(i.amount_cents) AS sum
           FROM hcp_invoices i
          WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled'::text AND i.invoice_type = 'inspection'::text), 0::bigint) AS inspection_invoice_cents,
    COALESCE(( SELECT sum(i.amount_cents) AS sum
           FROM hcp_invoices i
          WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled'::text AND i.invoice_type = 'treatment'::text), 0::bigint) AS treatment_invoice_cents,
    COALESCE(( SELECT sum(i.amount_cents) AS sum
           FROM hcp_invoices i
          WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled'::text), 0::bigint) AS invoice_total_cents,
    COALESCE(( SELECT sum(sub.total) AS sum
           FROM ( SELECT j.total_amount_cents + COALESCE(( SELECT sum(s.total_amount_cents) AS sum
                           FROM hcp_job_segments s
                          WHERE s.parent_hcp_job_id = j.hcp_job_id AND s.count_revenue = true AND (s.status <> ALL (ARRAY['user canceled'::text, 'pro canceled'::text]))), 0::bigint) AS total
                   FROM hcp_jobs j
                  WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active'::text AND (j.status <> ALL (ARRAY['user canceled'::text, 'pro canceled'::text])) AND j.count_revenue = true) sub), 0::numeric)::bigint AS job_total_cents,
    COALESCE(( SELECT sum(i.total_amount_cents) AS sum
           FROM hcp_inspections i
          WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.record_status = 'active'::text AND i.count_revenue = true AND (i.status <> ALL (ARRAY['user canceled'::text, 'pro canceled'::text]))), 0::bigint) AS inspection_total_cents,
        CASE
            WHEN COALESCE(( SELECT sum(i.amount_cents) AS sum
               FROM hcp_invoices i
              WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled'::text AND i.invoice_type = 'inspection'::text), 0::bigint) = 0 AND (EXISTS ( SELECT 1
               FROM hcp_estimates e
              WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.record_status = 'active'::text AND (e.status = ANY (ARRAY['sent'::text, 'approved'::text])))) THEN COALESCE(( SELECT sum(i.total_amount_cents) AS sum
               FROM hcp_inspections i
              WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.record_status = 'active'::text AND i.count_revenue = true AND (i.status <> ALL (ARRAY['user canceled'::text, 'pro canceled'::text])) AND i.total_amount_cents > 0 AND i.total_amount_cents < 150000 AND i.total_amount_cents < COALESCE(NULLIF(( SELECT min(GREATEST(e2.approved_total_cents, e2.highest_option_cents)) AS min
                       FROM hcp_estimates e2
                      WHERE e2.hcp_customer_id = hc.hcp_customer_id AND e2.record_status = 'active'::text AND (e2.status = ANY (ARRAY['sent'::text, 'approved'::text])) AND e2.count_revenue = true), 0), 999999999)), 0::bigint)
            ELSE 0::bigint
        END AS inspection_fee_inferred_cents,
        CASE
            WHEN COALESCE(( SELECT sum(i.amount_cents) AS sum
               FROM hcp_invoices i
              WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled'::text AND i.invoice_type = 'inspection'::text), 0::bigint) = 0 AND (EXISTS ( SELECT 1
               FROM hcp_estimates e
              WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.record_status = 'active'::text AND (e.status = ANY (ARRAY['sent'::text, 'approved'::text])))) AND COALESCE(( SELECT sum(i.total_amount_cents) AS sum
               FROM hcp_inspections i
              WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.record_status = 'active'::text AND i.count_revenue = true AND (i.status <> ALL (ARRAY['user canceled'::text, 'pro canceled'::text])) AND i.total_amount_cents > 0 AND i.total_amount_cents < 150000 AND i.total_amount_cents < COALESCE(NULLIF(( SELECT min(GREATEST(e2.approved_total_cents, e2.highest_option_cents)) AS min
                       FROM hcp_estimates e2
                      WHERE e2.hcp_customer_id = hc.hcp_customer_id AND e2.record_status = 'active'::text AND (e2.status = ANY (ARRAY['sent'::text, 'approved'::text])) AND e2.count_revenue = true), 0), 999999999)), 0::bigint) > 0 THEN true
            ELSE false
        END AS inspection_revenue_inferred,
    COALESCE(( SELECT sum(e.approved_total_cents) AS sum
           FROM hcp_estimates e
          WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'approved'::text AND e.record_status = 'active'::text AND e.count_revenue = true), 0::bigint) AS approved_estimate_cents,
    COALESCE(( SELECT sum(e.highest_option_cents) AS sum
           FROM hcp_estimates e
          WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'sent'::text AND e.record_status = 'active'::text AND e.estimate_type = 'treatment'::text AND e.count_revenue = true), 0::bigint) AS pipeline_estimate_cents,
    (COALESCE(( SELECT sum(i.amount_cents) AS sum
           FROM hcp_invoices i
          WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled'::text AND i.invoice_type = 'inspection'::text), 0::bigint) +
        CASE
            WHEN COALESCE(( SELECT sum(i.amount_cents) AS sum
               FROM hcp_invoices i
              WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled'::text AND i.invoice_type = 'inspection'::text), 0::bigint) = 0 AND (EXISTS ( SELECT 1
               FROM hcp_estimates e
              WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.record_status = 'active'::text AND (e.status = ANY (ARRAY['sent'::text, 'approved'::text])))) THEN COALESCE(( SELECT sum(i2.total_amount_cents) AS sum
               FROM hcp_inspections i2
              WHERE i2.hcp_customer_id = hc.hcp_customer_id AND i2.record_status = 'active'::text AND i2.count_revenue = true AND (i2.status <> ALL (ARRAY['user canceled'::text, 'pro canceled'::text])) AND i2.total_amount_cents > 0 AND i2.total_amount_cents < 150000 AND i2.total_amount_cents < COALESCE(NULLIF(( SELECT min(GREATEST(e2.approved_total_cents, e2.highest_option_cents)) AS min
                       FROM hcp_estimates e2
                      WHERE e2.hcp_customer_id = hc.hcp_customer_id AND e2.record_status = 'active'::text AND (e2.status = ANY (ARRAY['sent'::text, 'approved'::text])) AND e2.count_revenue = true), 0), 999999999)), 0::bigint)
            ELSE 0::bigint
        END)::numeric +
        CASE
            WHEN COALESCE(( SELECT sum(i.amount_cents) AS sum
               FROM hcp_invoices i
              WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled'::text AND i.invoice_type = 'treatment'::text), 0::bigint) > 0 OR COALESCE(( SELECT sum(e.approved_total_cents) AS sum
               FROM hcp_estimates e
              WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'approved'::text AND e.record_status = 'active'::text AND e.count_revenue = true), 0::bigint) > 0 THEN GREATEST(COALESCE(( SELECT sum(i.amount_cents) AS sum
               FROM hcp_invoices i
              WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled'::text AND i.invoice_type = 'treatment'::text), 0::bigint), COALESCE(( SELECT sum(e.approved_total_cents) AS sum
               FROM hcp_estimates e
              WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'approved'::text AND e.record_status = 'active'::text AND e.count_revenue = true), 0::bigint))::numeric
            ELSE COALESCE(( SELECT sum(sub2.total) AS sum
               FROM ( SELECT j.total_amount_cents + COALESCE(( SELECT sum(s.total_amount_cents) AS sum
                               FROM hcp_job_segments s
                              WHERE s.parent_hcp_job_id = j.hcp_job_id AND s.count_revenue = true AND (s.status <> ALL (ARRAY['user canceled'::text, 'pro canceled'::text]))), 0::bigint) AS total
                       FROM hcp_jobs j
                      WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active'::text AND (j.status <> ALL (ARRAY['user canceled'::text, 'pro canceled'::text])) AND j.count_revenue = true) sub2), 0::numeric) + COALESCE(( SELECT sum(ins.total_amount_cents) AS sum
               FROM hcp_inspections ins
              WHERE ins.hcp_customer_id = hc.hcp_customer_id AND ins.record_status = 'active'::text AND ins.count_revenue = true AND (ins.status <> ALL (ARRAY['user canceled'::text, 'pro canceled'::text]))), 0::bigint)::numeric
        END AS roas_revenue_cents,
        CASE
            WHEN COALESCE(( SELECT sum(i.amount_cents) AS sum
               FROM hcp_invoices i
              WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled'::text AND i.invoice_type = 'treatment'::text), 0::bigint) >= COALESCE(( SELECT sum(e.approved_total_cents) AS sum
               FROM hcp_estimates e
              WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'approved'::text AND e.record_status = 'active'::text AND e.count_revenue = true), 0::bigint) AND COALESCE(( SELECT sum(i.amount_cents) AS sum
               FROM hcp_invoices i
              WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled'::text AND i.invoice_type = 'treatment'::text), 0::bigint) > 0 THEN 'invoice'::text
            WHEN COALESCE(( SELECT sum(e.approved_total_cents) AS sum
               FROM hcp_estimates e
              WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'approved'::text AND e.record_status = 'active'::text AND e.count_revenue = true), 0::bigint) > 0 THEN 'approved_estimate'::text
            WHEN COALESCE(( SELECT sum(j.total_amount_cents) AS sum
               FROM hcp_jobs j
              WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active'::text AND (j.status <> ALL (ARRAY['user canceled'::text, 'pro canceled'::text])) AND j.count_revenue = true), 0::bigint) > 0 THEN 'job'::text
            WHEN COALESCE(( SELECT sum(i.amount_cents) AS sum
               FROM hcp_invoices i
              WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled'::text AND i.invoice_type = 'inspection'::text), 0::bigint) > 0 THEN 'inspection_only'::text
            ELSE 'none'::text
        END AS revenue_source
   FROM hcp_customers hc
  WHERE hc.attribution_override IS NOT NULL OR (EXISTS ( SELECT 1
           FROM calls c
          WHERE c.callrail_id = hc.callrail_id AND (is_google_ads_call(c.source, c.source_name, c.gclid) OR c.source_name = 'LSA'::text OR c.source_name ~~* '%gmb%'::text OR c.source_name ~~* '%gbp%'::text OR c.source_name = 'Main Business Line'::text))) OR (EXISTS ( SELECT 1
           FROM form_submissions f
          WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR hc.email IS NOT NULL AND lower(f.customer_email) = lower(hc.email)) AND (f.gclid IS NOT NULL OR f.source = 'Google Ads'::text))) OR hc.callrail_id ~~ 'WF_%'::text OR (EXISTS ( SELECT 1
           FROM ghl_contacts gc
          WHERE gc.customer_id = hc.customer_id AND (gc.phone_normalized = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(gc.email) = lower(hc.email)) AND gc.gclid IS NOT NULL AND gc.gclid <> ''::text))
UNION ALL
 SELECT NULL::text AS hcp_customer_id,
    c.customer_id,
    NULL::text AS first_name,
    c.customer_name AS last_name,
    c.callrail_id,
    'call'::text AS match_method,
    NULL::text AS attribution_override,
    'lead_only'::text AS lead_status,
        CASE
            WHEN c.source_name = 'LSA'::text THEN 'lsa'::text
            WHEN c.source_name ~~* '%gmb%'::text OR c.source_name ~~* '%gbp%'::text OR c.source_name = 'Main Business Line'::text THEN
            CASE
                WHEN (EXISTS ( SELECT 1
                   FROM calls c3
                  WHERE c3.customer_id = c.customer_id AND c3.caller_phone = c.caller_phone AND c3.gclid IS NOT NULL AND (c3.source_name <> ALL (ARRAY['GBP'::text, 'GMB Call Extension'::text, 'Main Business Line'::text])) AND c3.source_name !~~* '%gmb%'::text AND c3.source_name !~~* '%gbp%'::text)) THEN 'google_ads'::text
                ELSE 'google_business_profile'::text
            END
            WHEN is_google_ads_call(c.source, c.source_name, c.gclid) THEN 'google_ads'::text
            ELSE 'unknown'::text
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
  WHERE c.classification = 'legitimate'::text AND c.customer_id IS NOT NULL AND (is_google_ads_call(c.source, c.source_name, c.gclid) OR c.source_name ~~* '%gmb%'::text OR c.source_name ~~* '%gbp%'::text OR c.source_name = 'Main Business Line'::text) AND NOT (EXISTS ( SELECT 1
           FROM hcp_customers hc
          WHERE hc.callrail_id = c.callrail_id AND hc.customer_id = c.customer_id))
UNION ALL
 SELECT NULL::text AS hcp_customer_id,
    f.customer_id,
    NULL::text AS first_name,
    f.customer_name AS last_name,
    f.callrail_id,
    'form'::text AS match_method,
    NULL::text AS attribution_override,
    'lead_only'::text AS lead_status,
        CASE
            WHEN f.source = 'Google Ads'::text OR f.gclid IS NOT NULL THEN 'google_ads'::text
            WHEN f.source = 'Google My Business'::text THEN 'google_business_profile'::text
            WHEN f.source ~~* '%google%'::text THEN 'google_organic'::text
            ELSE 'unknown'::text
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
  WHERE f.classification = 'legitimate'::text AND f.customer_id IS NOT NULL AND (f.gclid IS NOT NULL OR f.source = 'Google Ads'::text) AND NOT (EXISTS ( SELECT 1
           FROM hcp_customers hc
          WHERE hc.callrail_id = f.callrail_id AND hc.customer_id = f.customer_id)) AND NOT (EXISTS ( SELECT 1
           FROM calls c
          WHERE normalize_phone(c.caller_phone) = normalize_phone(f.customer_phone) AND c.customer_id = f.customer_id AND is_google_ads_call(c.source, c.source_name, c.gclid)));
