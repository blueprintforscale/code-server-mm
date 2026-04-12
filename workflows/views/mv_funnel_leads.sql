-- mv_funnel_leads materialized view definition
-- Exported 2026-04-08 from PostgreSQL
-- This file is a reference snapshot. The canonical definition lives in PostgreSQL.
-- To recreate: DROP MATERIALIZED VIEW mv_funnel_leads; CREATE MATERIALIZED VIEW mv_funnel_leads AS <this query>;


 WITH phone_groups AS (
         SELECT hcp_customers.customer_id,
            hcp_customers.phone_normalized,
            array_agg(hcp_customers.hcp_customer_id) AS all_ids
           FROM hcp_customers
          WHERE hcp_customers.phone_normalized IS NOT NULL
          GROUP BY hcp_customers.customer_id, hcp_customers.phone_normalized
        ), lead_base AS (
         SELECT hc.hcp_customer_id,
            hc.customer_id,
            hc.first_name,
            hc.last_name,
            hc.phone_normalized,
            hc.email,
            hc.callrail_id,
            hc.hcp_created_at,
            hc.attribution_override,
            hc.client_flag_reason,
            pg.all_ids,
                CASE
                    WHEN hc.attribution_override = 'google_ads'::text THEN 'google_ads'::text
                    WHEN hc.callrail_id ~~ 'WF_%'::text THEN 'google_ads'::text
                    WHEN (EXISTS ( SELECT 1
                       FROM calls c
                         JOIN callrail_trackers ct ON ct.tracker_id = c.tracker_id
                      WHERE c.callrail_id = hc.callrail_id AND ct.source_type = 'google_ad_extension'::text)) THEN 'google_ads'::text
                    WHEN (EXISTS ( SELECT 1
                       FROM calls c
                         JOIN callrail_trackers ct ON ct.tracker_id = c.tracker_id
                      WHERE c.customer_id = hc.customer_id AND normalize_phone(c.caller_phone) = hc.phone_normalized AND ct.source_type = 'google_ad_extension'::text)) THEN 'google_ads'::text
                    WHEN (EXISTS ( SELECT 1
                       FROM calls c
                      WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads'::text) AND COALESCE(c.source_name, ''::text) <> 'LSA'::text)) THEN 'google_ads'::text
                    WHEN (EXISTS ( SELECT 1
                       FROM form_submissions f
                      WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(f.customer_email) = lower(hc.email)) AND (f.gclid IS NOT NULL OR f.source = 'Google Ads'::text))) THEN 'google_ads'::text
                    WHEN (EXISTS ( SELECT 1
                       FROM ghl_contacts gc
                      WHERE gc.customer_id = hc.customer_id AND (gc.phone_normalized = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(gc.email) = lower(hc.email)) AND gc.gclid IS NOT NULL AND gc.gclid <> ''::text)) THEN 'google_ads'::text
                    WHEN hc.attribution_override = 'lsa'::text THEN 'lsa'::text
                    WHEN (EXISTS ( SELECT 1
                       FROM calls c
                      WHERE c.callrail_id = hc.callrail_id AND c.source_name = 'LSA'::text)) THEN 'lsa'::text
                    WHEN hc.attribution_override = 'gbp'::text THEN 'gbp'::text
                    WHEN (EXISTS ( SELECT 1
                       FROM calls c
                      WHERE c.callrail_id = hc.callrail_id AND c.source = 'Google My Business'::text)) THEN 'gbp'::text
                    ELSE 'other'::text
                END AS lead_source,
            LEAST(( SELECT min(c.start_time) AS min
                   FROM calls c
                  WHERE (c.callrail_id = hc.callrail_id OR c.customer_id = hc.customer_id AND normalize_phone(c.caller_phone) = hc.phone_normalized) AND ((c.source = ANY (ARRAY['Google Ads'::text, 'Google Ads 2'::text])) OR c.gclid IS NOT NULL OR c.classified_source = 'google_ads'::text OR (EXISTS ( SELECT 1
                           FROM callrail_trackers ct
                          WHERE ct.tracker_id = c.tracker_id AND ct.source_type = 'google_ad_extension'::text)))), ( SELECT min(f.submitted_at) AS min
                   FROM form_submissions f
                  WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(f.customer_email) = lower(hc.email)) AND (f.gclid IS NOT NULL OR f.source = 'Google Ads'::text)), ( SELECT min(COALESCE(gc.kpi_date_created, gc.date_added)) AS min
                   FROM ghl_contacts gc
                  WHERE gc.customer_id = hc.customer_id AND (gc.phone_normalized = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(gc.email) = lower(hc.email)) AND gc.gclid IS NOT NULL AND gc.gclid <> ''::text)) AS first_ga_touch_time,
            GREATEST(hc.hcp_created_at, COALESCE(( SELECT max(COALESCE(j.scheduled_at, j.hcp_created_at)) AS max
                   FROM hcp_jobs j
                  WHERE (j.hcp_customer_id = ANY (pg.all_ids)) AND COALESCE(j.scheduled_at, j.hcp_created_at) < LEAST(( SELECT min(c2.start_time) AS min
                           FROM calls c2
                          WHERE (c2.callrail_id = hc.callrail_id OR c2.customer_id = hc.customer_id AND normalize_phone(c2.caller_phone) = hc.phone_normalized) AND ((c2.source = ANY (ARRAY['Google Ads'::text, 'Google Ads 2'::text])) OR c2.gclid IS NOT NULL OR c2.classified_source = 'google_ads'::text OR (EXISTS ( SELECT 1
                                   FROM callrail_trackers ct
                                  WHERE ct.tracker_id = c2.tracker_id AND ct.source_type = 'google_ad_extension'::text)))), ( SELECT min(f2.submitted_at) AS min
                           FROM form_submissions f2
                          WHERE f2.customer_id = hc.customer_id AND (f2.callrail_id = hc.callrail_id OR normalize_phone(f2.customer_phone) = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(f2.customer_email) = lower(hc.email)) AND (f2.gclid IS NOT NULL OR f2.source = 'Google Ads'::text)), ( SELECT min(COALESCE(gc2.kpi_date_created, gc2.date_added)) AS min
                           FROM ghl_contacts gc2
                          WHERE gc2.customer_id = hc.customer_id AND (gc2.phone_normalized = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(gc2.email) = lower(hc.email)) AND gc2.gclid IS NOT NULL AND gc2.gclid <> ''::text))), hc.hcp_created_at), COALESCE(( SELECT max(COALESCE(i.scheduled_at, i.hcp_created_at)) AS max
                   FROM hcp_inspections i
                  WHERE (i.hcp_customer_id = ANY (pg.all_ids)) AND i.record_status = 'active'::text AND COALESCE(i.scheduled_at, i.hcp_created_at) < LEAST(( SELECT min(c2.start_time) AS min
                           FROM calls c2
                          WHERE (c2.callrail_id = hc.callrail_id OR c2.customer_id = hc.customer_id AND normalize_phone(c2.caller_phone) = hc.phone_normalized) AND ((c2.source = ANY (ARRAY['Google Ads'::text, 'Google Ads 2'::text])) OR c2.gclid IS NOT NULL OR c2.classified_source = 'google_ads'::text OR (EXISTS ( SELECT 1
                                   FROM callrail_trackers ct
                                  WHERE ct.tracker_id = c2.tracker_id AND ct.source_type = 'google_ad_extension'::text)))), ( SELECT min(f2.submitted_at) AS min
                           FROM form_submissions f2
                          WHERE f2.customer_id = hc.customer_id AND (f2.callrail_id = hc.callrail_id OR normalize_phone(f2.customer_phone) = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(f2.customer_email) = lower(hc.email)) AND (f2.gclid IS NOT NULL OR f2.source = 'Google Ads'::text)), ( SELECT min(COALESCE(gc2.kpi_date_created, gc2.date_added)) AS min
                           FROM ghl_contacts gc2
                          WHERE gc2.customer_id = hc.customer_id AND (gc2.phone_normalized = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(gc2.email) = lower(hc.email)) AND gc2.gclid IS NOT NULL AND gc2.gclid <> ''::text))), hc.hcp_created_at), COALESCE(( SELECT max(eg.sent_at) AS max
                   FROM v_estimate_groups eg
                  WHERE (eg.hcp_customer_id = ANY (pg.all_ids)) AND eg.sent_at < LEAST(( SELECT min(c2.start_time) AS min
                           FROM calls c2
                          WHERE (c2.callrail_id = hc.callrail_id OR c2.customer_id = hc.customer_id AND normalize_phone(c2.caller_phone) = hc.phone_normalized) AND ((c2.source = ANY (ARRAY['Google Ads'::text, 'Google Ads 2'::text])) OR c2.gclid IS NOT NULL OR c2.classified_source = 'google_ads'::text OR (EXISTS ( SELECT 1
                                   FROM callrail_trackers ct
                                  WHERE ct.tracker_id = c2.tracker_id AND ct.source_type = 'google_ad_extension'::text)))), ( SELECT min(f2.submitted_at) AS min
                           FROM form_submissions f2
                          WHERE f2.customer_id = hc.customer_id AND (f2.callrail_id = hc.callrail_id OR normalize_phone(f2.customer_phone) = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(f2.customer_email) = lower(hc.email)) AND (f2.gclid IS NOT NULL OR f2.source = 'Google Ads'::text)), ( SELECT min(COALESCE(gc2.kpi_date_created, gc2.date_added)) AS min
                           FROM ghl_contacts gc2
                          WHERE gc2.customer_id = hc.customer_id AND (gc2.phone_normalized = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(gc2.email) = lower(hc.email)) AND gc2.gclid IS NOT NULL AND gc2.gclid <> ''::text))), hc.hcp_created_at), COALESCE(( SELECT max(c.start_time) AS max
                   FROM calls c
                  WHERE c.customer_id = hc.customer_id AND normalize_phone(c.caller_phone) = hc.phone_normalized AND c.start_time < LEAST(( SELECT min(c2.start_time) AS min
                           FROM calls c2
                          WHERE (c2.callrail_id = hc.callrail_id OR c2.customer_id = hc.customer_id AND normalize_phone(c2.caller_phone) = hc.phone_normalized) AND ((c2.source = ANY (ARRAY['Google Ads'::text, 'Google Ads 2'::text])) OR c2.gclid IS NOT NULL OR c2.classified_source = 'google_ads'::text OR (EXISTS ( SELECT 1
                                   FROM callrail_trackers ct
                                  WHERE ct.tracker_id = c2.tracker_id AND ct.source_type = 'google_ad_extension'::text)))), ( SELECT min(f2.submitted_at) AS min
                           FROM form_submissions f2
                          WHERE f2.customer_id = hc.customer_id AND (f2.callrail_id = hc.callrail_id OR normalize_phone(f2.customer_phone) = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(f2.customer_email) = lower(hc.email)) AND (f2.gclid IS NOT NULL OR f2.source = 'Google Ads'::text)), ( SELECT min(COALESCE(gc2.kpi_date_created, gc2.date_added)) AS min
                           FROM ghl_contacts gc2
                          WHERE gc2.customer_id = hc.customer_id AND (gc2.phone_normalized = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(gc2.email) = lower(hc.email)) AND gc2.gclid IS NOT NULL AND gc2.gclid <> ''::text))), hc.hcp_created_at), COALESCE(( SELECT max(f.submitted_at) AS max
                   FROM form_submissions f
                  WHERE f.customer_id = hc.customer_id AND (normalize_phone(f.customer_phone) = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(f.customer_email) = lower(hc.email)) AND f.submitted_at < LEAST(( SELECT min(c2.start_time) AS min
                           FROM calls c2
                          WHERE (c2.callrail_id = hc.callrail_id OR c2.customer_id = hc.customer_id AND normalize_phone(c2.caller_phone) = hc.phone_normalized) AND ((c2.source = ANY (ARRAY['Google Ads'::text, 'Google Ads 2'::text])) OR c2.gclid IS NOT NULL OR c2.classified_source = 'google_ads'::text OR (EXISTS ( SELECT 1
                                   FROM callrail_trackers ct
                                  WHERE ct.tracker_id = c2.tracker_id AND ct.source_type = 'google_ad_extension'::text)))), ( SELECT min(f2.submitted_at) AS min
                           FROM form_submissions f2
                          WHERE f2.customer_id = hc.customer_id AND (f2.callrail_id = hc.callrail_id OR normalize_phone(f2.customer_phone) = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(f2.customer_email) = lower(hc.email)) AND (f2.gclid IS NOT NULL OR f2.source = 'Google Ads'::text)), ( SELECT min(COALESCE(gc2.kpi_date_created, gc2.date_added)) AS min
                           FROM ghl_contacts gc2
                          WHERE gc2.customer_id = hc.customer_id AND (gc2.phone_normalized = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(gc2.email) = lower(hc.email)) AND gc2.gclid IS NOT NULL AND gc2.gclid <> ''::text))), hc.hcp_created_at)) AS last_prior_interaction,
            (EXISTS ( SELECT 1
                   FROM hcp_jobs j
                  WHERE (j.hcp_customer_id = ANY (pg.all_ids)) AND j.record_status = 'active'::text AND (j.status = ANY (ARRAY['complete rated'::text, 'complete unrated'::text])) AND j.total_amount_cents >= 100000 AND j.scheduled_at < LEAST(( SELECT min(c2.start_time) AS min
                           FROM calls c2
                          WHERE (c2.callrail_id = hc.callrail_id OR c2.customer_id = hc.customer_id AND normalize_phone(c2.caller_phone) = hc.phone_normalized) AND ((c2.source = ANY (ARRAY['Google Ads'::text, 'Google Ads 2'::text])) OR c2.gclid IS NOT NULL OR c2.classified_source = 'google_ads'::text OR (EXISTS ( SELECT 1
                                   FROM callrail_trackers ct
                                  WHERE ct.tracker_id = c2.tracker_id AND ct.source_type = 'google_ad_extension'::text)))), ( SELECT min(f2.submitted_at) AS min
                           FROM form_submissions f2
                          WHERE f2.customer_id = hc.customer_id AND (f2.callrail_id = hc.callrail_id OR normalize_phone(f2.customer_phone) = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(f2.customer_email) = lower(hc.email)) AND (f2.gclid IS NOT NULL OR f2.source = 'Google Ads'::text)), ( SELECT min(COALESCE(gc2.kpi_date_created, gc2.date_added)) AS min
                           FROM ghl_contacts gc2
                          WHERE gc2.customer_id = hc.customer_id AND (gc2.phone_normalized = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(gc2.email) = lower(hc.email)) AND gc2.gclid IS NOT NULL AND gc2.gclid <> ''::text)))) OR (EXISTS ( SELECT 1
                   FROM hcp_invoices inv
                  WHERE (inv.hcp_customer_id = ANY (pg.all_ids)) AND (inv.status <> ALL (ARRAY['canceled'::text, 'voided'::text])) AND inv.invoice_type = 'treatment'::text AND inv.amount_cents > 0 AND inv.created_at < LEAST(( SELECT min(c2.start_time) AS min
                           FROM calls c2
                          WHERE (c2.callrail_id = hc.callrail_id OR c2.customer_id = hc.customer_id AND normalize_phone(c2.caller_phone) = hc.phone_normalized) AND ((c2.source = ANY (ARRAY['Google Ads'::text, 'Google Ads 2'::text])) OR c2.gclid IS NOT NULL OR c2.classified_source = 'google_ads'::text OR (EXISTS ( SELECT 1
                                   FROM callrail_trackers ct
                                  WHERE ct.tracker_id = c2.tracker_id AND ct.source_type = 'google_ad_extension'::text)))), ( SELECT min(f2.submitted_at) AS min
                           FROM form_submissions f2
                          WHERE f2.customer_id = hc.customer_id AND (f2.callrail_id = hc.callrail_id OR normalize_phone(f2.customer_phone) = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(f2.customer_email) = lower(hc.email)) AND (f2.gclid IS NOT NULL OR f2.source = 'Google Ads'::text)), ( SELECT min(COALESCE(gc2.kpi_date_created, gc2.date_added)) AS min
                           FROM ghl_contacts gc2
                          WHERE gc2.customer_id = hc.customer_id AND (gc2.phone_normalized = hc.phone_normalized OR hc.email IS NOT NULL AND hc.email <> ''::text AND lower(gc2.email) = lower(hc.email)) AND gc2.gclid IS NOT NULL AND gc2.gclid <> ''::text)))) AS has_prior_treatment
           FROM hcp_customers hc
             JOIN phone_groups pg ON pg.phone_normalized = hc.phone_normalized AND pg.customer_id = hc.customer_id
          WHERE hc.phone_normalized IS NOT NULL
        )
 SELECT hcp_customer_id,
    customer_id,
    first_name,
    last_name,
    phone_normalized,
    email,
    callrail_id,
    hcp_created_at,
    attribution_override,
    client_flag_reason,
    lead_source,
    first_ga_touch_time,
    last_prior_interaction,
    has_prior_treatment,
        CASE
            WHEN lead_source = 'google_ads'::text AND first_ga_touch_time IS NOT NULL AND hcp_created_at < (first_ga_touch_time - '7 days'::interval) AND ((EXTRACT(epoch FROM first_ga_touch_time - last_prior_interaction) / 86400::numeric) <= 60::numeric OR has_prior_treatment) THEN true
            ELSE false
        END AS exclude_from_ga_roas,
    (EXISTS ( SELECT 1
           FROM hcp_inspections i
          WHERE (i.hcp_customer_id = ANY (lb.all_ids)) AND i.record_status = 'active'::text AND ((i.status = ANY (ARRAY['scheduled'::text, 'complete rated'::text, 'complete unrated'::text, 'in progress'::text])) OR i.scheduled_at IS NOT NULL OR i.inferred_complete = true) AND (lb.lead_source <> 'google_ads'::text OR lb.first_ga_touch_time IS NULL OR COALESCE(i.scheduled_at, i.hcp_created_at) >= lb.first_ga_touch_time))) AS has_inspection_scheduled,
    (EXISTS ( SELECT 1
           FROM hcp_inspections i
          WHERE (i.hcp_customer_id = ANY (lb.all_ids)) AND i.record_status = 'active'::text AND ((i.status = ANY (ARRAY['complete rated'::text, 'complete unrated'::text])) OR i.inferred_complete = true) AND (lb.lead_source <> 'google_ads'::text OR lb.first_ga_touch_time IS NULL OR COALESCE(i.scheduled_at, i.hcp_created_at) >= lb.first_ga_touch_time))) AS has_inspection_completed,
    (EXISTS ( SELECT 1
           FROM v_estimate_groups eg
          WHERE (eg.hcp_customer_id = ANY (lb.all_ids)) AND (eg.status = ANY (ARRAY['sent'::text, 'approved'::text, 'declined'::text])) AND eg.count_revenue AND eg.estimate_type = 'treatment'::text AND (lb.lead_source <> 'google_ads'::text OR lb.first_ga_touch_time IS NULL OR eg.sent_at >= lb.first_ga_touch_time))) AS has_estimate_sent,
    (EXISTS ( SELECT 1
           FROM v_estimate_groups eg
          WHERE (eg.hcp_customer_id = ANY (lb.all_ids)) AND eg.status = 'approved'::text AND eg.count_revenue AND eg.estimate_type = 'treatment'::text AND eg.approved_total_cents >= 100000 AND (lb.lead_source <> 'google_ads'::text OR lb.first_ga_touch_time IS NULL OR eg.sent_at >= lb.first_ga_touch_time))) AS has_estimate_approved,
    (EXISTS ( SELECT 1
           FROM hcp_jobs j
          WHERE (j.hcp_customer_id = ANY (lb.all_ids)) AND j.record_status = 'active'::text AND j.work_category = 'treatment'::text AND (j.status = ANY (ARRAY['scheduled'::text, 'complete rated'::text, 'complete unrated'::text, 'in progress'::text])) AND j.total_amount_cents >= 100000 AND (lb.lead_source <> 'google_ads'::text OR lb.first_ga_touch_time IS NULL OR COALESCE(j.scheduled_at, j.hcp_created_at) >= lb.first_ga_touch_time))
     OR EXISTS ( SELECT 1
           FROM v_estimate_groups eg
          WHERE (eg.hcp_customer_id = ANY (lb.all_ids)) AND eg.status = 'approved'::text AND eg.count_revenue AND eg.estimate_type = 'treatment'::text AND eg.approved_total_cents >= 100000 AND (lb.lead_source <> 'google_ads'::text OR lb.first_ga_touch_time IS NULL OR eg.sent_at >= lb.first_ga_touch_time))) AS has_job_scheduled,
    (EXISTS ( SELECT 1
           FROM hcp_jobs j
          WHERE (j.hcp_customer_id = ANY (lb.all_ids)) AND j.record_status = 'active'::text AND j.work_category = 'treatment'::text AND (j.status = ANY (ARRAY['complete rated'::text, 'complete unrated'::text])) AND j.total_amount_cents >= 100000 AND (lb.lead_source <> 'google_ads'::text OR lb.first_ga_touch_time IS NULL OR COALESCE(j.scheduled_at, j.hcp_created_at) >= lb.first_ga_touch_time))
     OR EXISTS ( SELECT 1
           FROM hcp_invoices inv
          WHERE (inv.hcp_customer_id = ANY (lb.all_ids)) AND (inv.status <> ALL (ARRAY['canceled'::text, 'voided'::text])) AND inv.amount_cents > 0 AND inv.invoice_type = 'treatment'::text AND (lb.lead_source <> 'google_ads'::text OR lb.first_ga_touch_time IS NULL OR inv.created_at >= lb.first_ga_touch_time))) AS has_job_completed,
    (EXISTS ( SELECT 1
           FROM hcp_invoices inv
          WHERE (inv.hcp_customer_id = ANY (lb.all_ids)) AND (inv.status <> ALL (ARRAY['canceled'::text, 'voided'::text])) AND inv.amount_cents > 0 AND (lb.lead_source <> 'google_ads'::text OR lb.first_ga_touch_time IS NULL OR inv.created_at >= lb.first_ga_touch_time))) OR (EXISTS ( SELECT 1
           FROM v_estimate_groups eg
          WHERE (eg.hcp_customer_id = ANY (lb.all_ids)) AND eg.status = 'approved'::text AND eg.count_revenue AND eg.estimate_type = 'treatment'::text AND eg.approved_total_cents >= 100000 AND (lb.lead_source <> 'google_ads'::text OR lb.first_ga_touch_time IS NULL OR eg.sent_at >= lb.first_ga_touch_time))) AS has_invoice,
    COALESCE(( SELECT sum(eg.highest_option_cents) AS sum
           FROM v_estimate_groups eg
          WHERE (eg.hcp_customer_id = ANY (lb.all_ids)) AND (eg.status = ANY (ARRAY['sent'::text, 'approved'::text, 'declined'::text])) AND eg.count_revenue AND eg.estimate_type = 'treatment'::text AND (lb.lead_source <> 'google_ads'::text OR lb.first_ga_touch_time IS NULL OR eg.sent_at >= lb.first_ga_touch_time)), 0::bigint) AS est_sent_cents,
    COALESCE(( SELECT sum(eg.approved_total_cents) AS sum
           FROM v_estimate_groups eg
          WHERE (eg.hcp_customer_id = ANY (lb.all_ids)) AND eg.status = 'approved'::text AND eg.count_revenue AND eg.estimate_type = 'treatment'::text AND eg.approved_total_cents >= 100000 AND (lb.lead_source <> 'google_ads'::text OR lb.first_ga_touch_time IS NULL OR eg.sent_at >= lb.first_ga_touch_time)), 0::numeric) AS est_approved_cents,
    COALESCE(( SELECT sum(j.total_amount_cents) AS sum
           FROM hcp_jobs j
          WHERE (j.hcp_customer_id = ANY (lb.all_ids)) AND j.record_status = 'active'::text AND (j.status <> ALL (ARRAY['user canceled'::text, 'pro canceled'::text])) AND j.count_revenue = true AND (lb.lead_source <> 'google_ads'::text OR lb.first_ga_touch_time IS NULL OR COALESCE(j.scheduled_at, j.hcp_created_at) >= lb.first_ga_touch_time)), 0::bigint) AS job_cents,
    COALESCE(( SELECT sum(inv.amount_cents) AS sum
           FROM hcp_invoices inv
          WHERE (inv.hcp_customer_id = ANY (lb.all_ids)) AND (inv.status <> ALL (ARRAY['canceled'::text, 'voided'::text])) AND inv.amount_cents > 0 AND (lb.lead_source <> 'google_ads'::text OR lb.first_ga_touch_time IS NULL OR inv.created_at >= lb.first_ga_touch_time)), 0::bigint) AS invoice_cents,
    COALESCE(( SELECT sum(inv.amount_cents) AS sum
           FROM hcp_invoices inv
          WHERE (inv.hcp_customer_id = ANY (lb.all_ids)) AND (inv.status <> ALL (ARRAY['canceled'::text, 'voided'::text])) AND inv.invoice_type = 'inspection'::text AND (lb.lead_source <> 'google_ads'::text OR lb.first_ga_touch_time IS NULL OR inv.created_at >= lb.first_ga_touch_time)), 0::bigint) AS insp_invoice_cents,
    COALESCE(( SELECT sum(inv.amount_cents) AS sum
           FROM hcp_invoices inv
          WHERE (inv.hcp_customer_id = ANY (lb.all_ids)) AND (inv.status <> ALL (ARRAY['canceled'::text, 'voided'::text])) AND inv.invoice_type = 'treatment'::text AND (lb.lead_source <> 'google_ads'::text OR lb.first_ga_touch_time IS NULL OR inv.created_at >= lb.first_ga_touch_time)), 0::bigint) AS treat_invoice_cents,
    (EXISTS ( SELECT 1
           FROM ghl_contacts gc
          WHERE gc.phone_normalized = lb.phone_normalized AND gc.customer_id = lb.customer_id AND (lower(gc.lost_reason) ~ similar_to_escape('%(spam|not a lead|wrong number|out of area|wrong service)%'::text) OR (EXISTS ( SELECT 1
                   FROM ghl_opportunities o
                  WHERE o.ghl_contact_id = gc.ghl_contact_id AND lower(o.stage_name) ~ similar_to_escape('%(spam|not a lead|out of area|wrong service)%'::text)))))) AS ghl_spam,
    (EXISTS ( SELECT 1
           FROM ghl_contacts gc
          WHERE gc.phone_normalized = lb.phone_normalized AND gc.customer_id = lb.customer_id AND (lower(COALESCE(gc.lost_reason, ''::text)) ~~ '%abandoned%'::text OR (EXISTS ( SELECT 1
                   FROM ghl_opportunities o
                  WHERE o.ghl_contact_id = gc.ghl_contact_id AND o.status = 'abandoned'::text))))) AS ghl_abandoned
   FROM lead_base lb;
