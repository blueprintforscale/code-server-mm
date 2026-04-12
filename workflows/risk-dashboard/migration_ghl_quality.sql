CREATE OR REPLACE FUNCTION public.get_dashboard_metrics(p_start date DEFAULT (CURRENT_DATE - '30 days'::interval), p_end date DEFAULT CURRENT_DATE)
 RETURNS TABLE(customer_id bigint, client_name text, ads_manager text, inspection_type text, budget numeric, months_in_program integer, start_date date, field_mgmt text, timezone text, quality_leads integer, prior_quality_leads integer, lead_volume_change numeric, ad_spend numeric, all_time_spend numeric, cpl numeric, total_calls integer, spam_calls integer, abandoned_calls integer, spam_rate numeric, abandoned_rate numeric, days_since_lead integer, biz_hour_calls integer, biz_hour_answered integer, call_answer_rate numeric, hcp_insp_booked integer, hcp_closed_rev numeric, hcp_open_est_rev numeric, hcp_approved_no_inv numeric, jobber_insp_booked integer, jobber_closed_rev numeric, jobber_open_est_rev numeric, total_insp_booked integer, total_closed_rev numeric, total_open_est_rev numeric, approved_no_inv numeric, on_cal_14d integer, on_cal_total integer, lsa_spend numeric, lsa_leads integer, insp_booked_pct numeric, roas numeric, guarantee numeric, all_time_rev numeric, prior_cpl numeric)
 LANGUAGE sql
 STABLE
AS $function$

WITH client_base AS (
  SELECT
    c.customer_id,
    c.name AS client_name,
    c.ads_manager,
    COALESCE(c.inspection_type, 'free') AS inspection_type,
    COALESCE(c.budget, 0) AS budget,
    c.start_date,
    c.field_management_software AS field_mgmt,
    c.timezone,
    c.biz_hours_start,
    c.biz_hours_end,
    c.biz_days,
    c.callrail_company_id,
    GREATEST(1, (EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM c.start_date))::INT * 12
      + (EXTRACT(MONTH FROM CURRENT_DATE) - EXTRACT(MONTH FROM c.start_date))::INT + 1) AS months_in_program
  FROM clients c
  WHERE c.status = 'active'
    AND c.start_date IS NOT NULL
    AND c.budget IS NOT NULL
    AND c.budget > 0
),

-- ALL contacts from calls (GA, any classification — for Contacts count)
all_call_contacts AS (
  SELECT
    ca.customer_id,
    normalize_phone(ca.caller_phone) AS dedup_phone,
    ca.start_time::date AS lead_date
  FROM calls ca
  INNER JOIN client_base cb ON cb.customer_id = ca.customer_id
  WHERE (ca.source IN ('Google Ads', 'Google Ads 2') OR (ca.gclid IS NOT NULL AND ca.gclid != ''))
    AND ca.start_time::date BETWEEN (p_start - (p_end - p_start)) AND p_end
),

-- ALL contacts from forms (GA, any classification)
all_form_contacts AS (
  SELECT
    fs.customer_id,
    COALESCE(normalize_phone(fs.customer_phone), 'form_' || fs.callrail_id) AS dedup_phone,
    fs.submitted_at::date AS lead_date
  FROM form_submissions fs
  INNER JOIN client_base cb ON cb.customer_id = fs.customer_id
  WHERE fs.gclid IS NOT NULL AND fs.gclid != ''
    AND fs.submitted_at::date BETWEEN (p_start - (p_end - p_start)) AND p_end
    AND NOT EXISTS (
      SELECT 1 FROM all_call_contacts ac
      WHERE ac.customer_id = fs.customer_id
        AND ac.dedup_phone = normalize_phone(fs.customer_phone)
    )
),

-- Combine and count contacts (all) + prior period + calendar months
lead_counts AS (
  SELECT
    customer_id,
    COUNT(DISTINCT dedup_phone) FILTER (WHERE lead_date BETWEEN p_start AND p_end)::INT AS quality_leads,
    COUNT(DISTINCT dedup_phone) FILTER (WHERE lead_date BETWEEN (p_start - (p_end - p_start)) AND (p_start - 1))::INT AS prior_quality_leads,
    -- Calendar month counts for pro-rated monthly pace
    COUNT(DISTINCT dedup_phone) FILTER (WHERE lead_date >= DATE_TRUNC('month', CURRENT_DATE))::INT AS this_month_leads,
    COUNT(DISTINCT dedup_phone) FILTER (WHERE lead_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND lead_date < DATE_TRUNC('month', CURRENT_DATE))::INT AS last_month_leads
  FROM (
    SELECT customer_id, dedup_phone, lead_date FROM all_call_contacts
    UNION ALL
    SELECT customer_id, dedup_phone, lead_date FROM all_form_contacts
  ) all_leads
  GROUP BY customer_id
),

-- Ad spend (excluding LSA / Local Services campaigns)
ad_spend AS (
  SELECT
    cdm.customer_id,
    COALESCE(SUM(cdm.cost) FILTER (WHERE cdm.date BETWEEN p_start AND p_end), 0) AS ad_spend,
    COALESCE(SUM(cdm.cost) FILTER (WHERE cdm.date BETWEEN (p_start - (p_end - p_start)) AND (p_start - 1)), 0) AS prior_ad_spend,
    COALESCE(SUM(cdm.cost), 0) AS all_time_spend
  FROM campaign_daily_metrics cdm
  INNER JOIN client_base cb ON cb.customer_id = cdm.customer_id
  WHERE cdm.campaign_type != 'LOCAL_SERVICES'
  GROUP BY cdm.customer_id
),

-- LSA spend + leads (tracked separately)
-- Lead count: prefer CallRail LSA-tagged calls, fall back to lsa_leads API table
lsa_metrics AS (
  SELECT
    cdm.customer_id,
    COALESCE(SUM(cdm.cost) FILTER (WHERE cdm.date BETWEEN p_start AND p_end), 0) AS lsa_spend,
    GREATEST(
      (SELECT COUNT(DISTINCT normalize_phone(ca.caller_phone))
       FROM calls ca
       WHERE ca.customer_id = cdm.customer_id
         AND ca.source_name = 'LSA'
         AND ca.classification = 'legitimate'
         AND ca.start_time::date BETWEEN p_start AND p_end
      ),
      (SELECT COUNT(DISTINCT ll.lsa_lead_id)
       FROM lsa_leads ll
       WHERE ll.customer_id = cdm.customer_id
         AND ll.lead_creation_time::date BETWEEN p_start AND p_end
         AND ll.lead_charged = true
      )
    )::INT AS lsa_leads
  FROM campaign_daily_metrics cdm
  INNER JOIN client_base cb ON cb.customer_id = cdm.customer_id
  WHERE cdm.campaign_type = 'LOCAL_SERVICES'
  GROUP BY cdm.customer_id
),

-- Call metrics (days since lead + answer rate from CallRail)
call_metrics AS (
  SELECT
    cb.customer_id,
    COUNT(*) FILTER (WHERE ca.start_time::date BETWEEN p_start AND p_end
                     AND (ca.source IN ('Google Ads', 'Google Ads 2') OR (ca.gclid IS NOT NULL AND ca.gclid != '')))::INT AS total_calls,
    -- Days since last lead: most recent of calls OR forms
    (CURRENT_DATE - GREATEST(
      MAX(ca.start_time::date) FILTER (WHERE (ca.source IN ('Google Ads', 'Google Ads 2') OR (ca.gclid IS NOT NULL AND ca.gclid != ''))
                                        AND ca.classification = 'legitimate'),
      (SELECT MAX(fs.submitted_at::date) FROM form_submissions fs
       WHERE fs.customer_id = cb.customer_id
         AND (fs.gclid IS NOT NULL OR fs.source = 'Google Ads')
         AND fs.classification = 'legitimate')
    ))::INT AS days_since_lead,
    -- Answer rate: first-time GA callers during business hours only
    COUNT(*) FILTER (
      WHERE ca.start_time::date BETWEEN p_start AND p_end
        AND (ca.source IN ('Google Ads', 'Google Ads 2') OR (ca.gclid IS NOT NULL AND ca.gclid != ''))
        AND ca.first_call = true
        AND EXTRACT(DOW FROM (ca.start_time AT TIME ZONE COALESCE(cb.timezone, 'America/New_York')))::INT
            = ANY(COALESCE(cb.biz_days, ARRAY[1,2,3,4,5]))
        AND (ca.start_time AT TIME ZONE COALESCE(cb.timezone, 'America/New_York'))::time
            BETWEEN COALESCE(cb.biz_hours_start::time, '08:00'::time)
                AND COALESCE(cb.biz_hours_end::time, '18:00'::time)
    )::INT AS biz_hour_calls,
    COUNT(*) FILTER (
      WHERE ca.start_time::date BETWEEN p_start AND p_end
        AND (ca.source IN ('Google Ads', 'Google Ads 2') OR (ca.gclid IS NOT NULL AND ca.gclid != ''))
        AND ca.first_call = true
        AND COALESCE(ca.classified_status, CASE WHEN ca.answered THEN 'answered' ELSE 'missed' END) = 'answered'
        AND EXTRACT(DOW FROM (ca.start_time AT TIME ZONE COALESCE(cb.timezone, 'America/New_York')))::INT
            = ANY(COALESCE(cb.biz_days, ARRAY[1,2,3,4,5]))
        AND (ca.start_time AT TIME ZONE COALESCE(cb.timezone, 'America/New_York'))::time
            BETWEEN COALESCE(cb.biz_hours_start::time, '08:00'::time)
                AND COALESCE(cb.biz_hours_end::time, '18:00'::time)
    )::INT AS biz_hour_answered
  FROM client_base cb
  LEFT JOIN calls ca ON ca.customer_id = cb.customer_id
  GROUP BY cb.customer_id, cb.timezone, cb.biz_days, cb.biz_hours_start, cb.biz_hours_end
),

-- GHL contact quality: match GHL contacts to period leads by phone number
-- "Not quality" = spam, not a lead, wrong number, invalid number, out of area, wrong service
ghl_metrics AS (
  SELECT
    al.customer_id,
    COUNT(DISTINCT al.dedup_phone)::INT AS total_period_contacts,
    -- Spam/not-quality: direct count of period leads flagged in GHL
    COUNT(DISTINCT al.dedup_phone) FILTER (
      WHERE (
        o.status = 'lost' AND (
          LOWER(COALESCE(gc.lost_reason, '')) LIKE '%spam%'
          OR LOWER(COALESCE(gc.lost_reason, '')) LIKE '%not a lead%'
          OR LOWER(COALESCE(gc.lost_reason, '')) LIKE '%wrong number%'
          OR LOWER(COALESCE(gc.lost_reason, '')) LIKE '%invalid%'
          OR LOWER(COALESCE(gc.lost_reason, '')) LIKE '%out of area%'
          OR LOWER(COALESCE(gc.lost_reason, '')) LIKE '%wrong service%'
        )
      ) OR (
        LOWER(o.stage_name) LIKE '%spam%'
        OR LOWER(o.stage_name) LIKE '%not a lead%'
        OR LOWER(o.stage_name) LIKE '%out of area%'
      )
    )::INT AS spam_count,
    COUNT(DISTINCT al.dedup_phone) FILTER (WHERE o.status = 'abandoned')::INT AS abandoned_count
  FROM (
    SELECT customer_id, dedup_phone FROM all_call_contacts WHERE lead_date BETWEEN p_start AND p_end
    UNION
    SELECT customer_id, dedup_phone FROM all_form_contacts WHERE lead_date BETWEEN p_start AND p_end
  ) al
  LEFT JOIN ghl_contacts gc2 ON gc2.customer_id = al.customer_id
    AND normalize_phone(gc2.phone) = al.dedup_phone
  LEFT JOIN ghl_opportunities o ON o.ghl_contact_id = gc2.ghl_contact_id
    AND o.customer_id = gc2.customer_id
  LEFT JOIN ghl_contacts gc ON gc.ghl_contact_id = o.ghl_contact_id
    AND gc.customer_id = o.customer_id
  GROUP BY al.customer_id
),

-- HCP revenue: period-scoped for ROAS, all-time for guarantee
-- lead_date: from call start_time OR form submitted_at (covers phone + email matches)
revenue_hcp AS (
  SELECT
    lr.customer_id,
    COUNT(DISTINCT CASE
      WHEN lr.lead_source_type = 'google_ads'
        AND lp.inspection_scheduled_at IS NOT NULL
        AND COALESCE(ca.start_time::date, fs.submitted_at::date, ws.submitted_at::date) BETWEEN p_start AND p_end
      THEN lr.hcp_customer_id
    END)::INT AS insp_booked,
    COALESCE(ROUND(SUM(lr.roas_revenue_cents) FILTER (
      WHERE lr.lead_source_type = 'google_ads'
        AND COALESCE(ca.start_time::date, fs.submitted_at::date, ws.submitted_at::date) BETWEEN p_start AND p_end
    ) / 100.0, 2), 0) AS period_rev,
    COALESCE(ROUND(SUM(lr.roas_revenue_cents) FILTER (
      WHERE lr.lead_source_type = 'google_ads'
    ) / 100.0, 2), 0) AS all_time_rev,
    -- Flag: revenue from approved estimates with no invoice (at risk of being overstated)
    COALESCE(ROUND(SUM(lr.approved_estimate_cents) FILTER (
      WHERE lr.lead_source_type = 'google_ads'
        AND lr.revenue_source = 'approved_estimate'
        AND lr.treatment_invoice_cents = 0
    ) / 100.0, 2), 0) AS approved_no_invoice_rev
  FROM v_lead_revenue lr
  INNER JOIN client_base cb ON cb.customer_id = lr.customer_id
  LEFT JOIN v_lead_pipeline lp ON lp.hcp_customer_id = lr.hcp_customer_id
    AND lp.customer_id = lr.customer_id
  LEFT JOIN calls ca ON ca.callrail_id = lr.callrail_id
  LEFT JOIN form_submissions fs ON fs.callrail_id = lr.callrail_id
  LEFT JOIN LATERAL (
    SELECT ws2.submitted_at
    FROM webflow_submissions ws2
    WHERE lr.callrail_id LIKE 'WF_%'
      AND ws2.id = CAST(SUBSTRING(lr.callrail_id FROM 4) AS INTEGER)
    LIMIT 1
  ) ws ON TRUE
  GROUP BY lr.customer_id
),

-- HCP open estimates
open_est_hcp AS (
  SELECT
    oe.customer_id,
    COALESCE(ROUND(SUM(oe.amount), 2), 0) AS open_est_rev
  FROM v_open_estimates oe
  INNER JOIN client_base cb ON cb.customer_id = oe.customer_id
  WHERE oe.platform = 'hcp' AND oe.is_google_ads = true
  GROUP BY oe.customer_id
),

-- Jobber revenue: period-scoped for ROAS, all-time for guarantee
revenue_jobber AS (
  SELECT
    jlr.customer_id,
    COUNT(DISTINCT jlr.jobber_customer_id) FILTER (
      WHERE jlr.lead_source_type = 'google_ads'
        AND jlr.inspection_scheduled > 0
        AND COALESCE(ca.start_time::date, fs.submitted_at::date) BETWEEN p_start AND p_end
    )::INT AS insp_booked,
    COALESCE(ROUND(SUM(jlr.roas_revenue_cents) FILTER (
      WHERE jlr.lead_source_type = 'google_ads'
        AND COALESCE(ca.start_time::date, fs.submitted_at::date) BETWEEN p_start AND p_end
    ) / 100.0, 2), 0) AS period_rev,
    COALESCE(ROUND(SUM(jlr.roas_revenue_cents) FILTER (
      WHERE jlr.lead_source_type = 'google_ads'
    ) / 100.0, 2), 0) AS all_time_rev
  FROM v_jobber_lead_revenue jlr
  INNER JOIN client_base cb ON cb.customer_id = jlr.customer_id
  LEFT JOIN calls ca ON ca.callrail_id = jlr.callrail_id
  LEFT JOIN form_submissions fs ON fs.callrail_id = jlr.callrail_id
  GROUP BY jlr.customer_id
),

-- Jobber open estimates
open_est_jobber AS (
  SELECT
    oe.customer_id,
    COALESCE(ROUND(SUM(oe.amount), 2), 0) AS open_est_rev
  FROM v_open_estimates oe
  INNER JOIN client_base cb ON cb.customer_id = oe.customer_id
  WHERE oe.platform = 'jobber' AND oe.is_google_ads = true
  GROUP BY oe.customer_id
),

-- On-calendar: future HCP inspections (GA + all)
on_cal_hcp AS (
  SELECT
    hi.customer_id,
    -- GA-attributed: call match OR form match OR webflow match
    COUNT(*) FILTER (WHERE hi.scheduled_at::date BETWEEN CURRENT_DATE AND CURRENT_DATE + 13
      AND EXISTS (
        SELECT 1 FROM hcp_customers hc
        WHERE hc.hcp_customer_id = hi.hcp_customer_id AND hc.customer_id = hi.customer_id
          AND (
            -- Call with GA attribution
            EXISTS (SELECT 1 FROM calls ca WHERE normalize_phone(ca.caller_phone) = hc.phone_normalized
              AND ca.customer_id = hi.customer_id AND (ca.source IN ('Google Ads', 'Google Ads 2') OR (ca.gclid IS NOT NULL AND ca.gclid != '')))
            -- Form with GCLID
            OR EXISTS (SELECT 1 FROM form_submissions fs WHERE normalize_phone(fs.customer_phone) = hc.phone_normalized
              AND fs.customer_id = hi.customer_id AND (fs.gclid IS NOT NULL OR fs.source = 'Google Ads'))
            -- Webflow with GCLID
            OR (hc.callrail_id LIKE 'WF_%')
          )
      )
    )::INT AS on_cal_14d,
    -- All inspections (any source)
    COUNT(*) FILTER (WHERE hi.scheduled_at::date BETWEEN CURRENT_DATE AND CURRENT_DATE + 13)::INT AS on_cal_14d_all
  FROM hcp_inspections hi
  INNER JOIN client_base cb ON cb.customer_id = hi.customer_id
  WHERE hi.status IN ('scheduled', 'in_progress', 'needs scheduling')
    AND hi.record_status = 'active'
    AND hi.scheduled_at >= CURRENT_DATE
  GROUP BY hi.customer_id
),

-- On-calendar: future Jobber assessments + upcoming inspection jobs (GA + all)
on_cal_jobber AS (
  SELECT
    customer_id,
    SUM(ga_count)::INT AS on_cal_14d,
    SUM(total_count)::INT AS on_cal_14d_all
  FROM (
    -- Requests with assessments
    SELECT
      jr.customer_id,
      COUNT(*) FILTER (WHERE jr.assessment_start_at::date BETWEEN CURRENT_DATE AND CURRENT_DATE + 13
        AND EXISTS (
          SELECT 1 FROM jobber_customers jc
          WHERE jc.jobber_customer_id = jr.jobber_customer_id AND jc.customer_id = jr.customer_id
            AND (
              EXISTS (SELECT 1 FROM calls ca WHERE normalize_phone(ca.caller_phone) = jc.phone_normalized
                AND ca.customer_id = jr.customer_id AND (ca.source IN ('Google Ads', 'Google Ads 2') OR (ca.gclid IS NOT NULL AND ca.gclid != '')))
              OR EXISTS (SELECT 1 FROM form_submissions fs WHERE normalize_phone(fs.customer_phone) = jc.phone_normalized
                AND fs.customer_id = jr.customer_id AND (fs.gclid IS NOT NULL OR fs.source = 'Google Ads'))
            )
        )
      ) AS ga_count,
      COUNT(*) FILTER (WHERE jr.assessment_start_at::date BETWEEN CURRENT_DATE AND CURRENT_DATE + 13) AS total_count
    FROM jobber_requests jr
    INNER JOIN client_base cb ON cb.customer_id = jr.customer_id
    WHERE jr.has_assessment = true
      AND jr.assessment_start_at >= CURRENT_DATE
      AND jr.assessment_completed_at IS NULL
    GROUP BY jr.customer_id
    UNION ALL
    -- Upcoming inspection-titled jobs
    SELECT
      j.customer_id,
      COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jobber_customers jc
        WHERE jc.jobber_customer_id = j.jobber_customer_id AND jc.customer_id = j.customer_id
          AND (
            EXISTS (SELECT 1 FROM calls ca WHERE normalize_phone(ca.caller_phone) = jc.phone_normalized
              AND ca.customer_id = j.customer_id AND (ca.source IN ('Google Ads', 'Google Ads 2') OR (ca.gclid IS NOT NULL AND ca.gclid != '')))
            OR EXISTS (SELECT 1 FROM form_submissions fs WHERE normalize_phone(fs.customer_phone) = jc.phone_normalized
              AND fs.customer_id = j.customer_id AND (fs.gclid IS NOT NULL OR fs.source = 'Google Ads'))
          )
      )) AS ga_count,
      COUNT(*) AS total_count
    FROM jobber_jobs j
    INNER JOIN client_base cb ON cb.customer_id = j.customer_id
    WHERE j.status = 'upcoming'
      AND (LOWER(j.title) LIKE '%assessment%' OR LOWER(j.title) LIKE '%instascope%'
        OR LOWER(j.title) LIKE '%inspection%' OR LOWER(j.title) LIKE '%mold test%'
        OR LOWER(j.title) LIKE '%air quality%' OR LOWER(j.title) LIKE '%air test%')
    GROUP BY j.customer_id
  ) combined
  GROUP BY customer_id
)

-- Final SELECT
SELECT
  cb.customer_id,
  cb.client_name,
  cb.ads_manager,
  cb.inspection_type,
  cb.budget,
  cb.months_in_program,
  cb.start_date,
  cb.field_mgmt,
  cb.timezone,
  COALESCE(lc.quality_leads, 0)::INT,
  COALESCE(lc.prior_quality_leads, 0)::INT,
  -- Lead volume change: raw 30-day rolling (current vs prior period)
  CASE WHEN COALESCE(lc.prior_quality_leads, 0) > 0
    THEN ROUND((COALESCE(lc.quality_leads, 0) - lc.prior_quality_leads)::numeric / lc.prior_quality_leads, 3)
    ELSE NULL
  END AS lead_volume_change,
  COALESCE(asp.ad_spend, 0) AS ad_spend,
  COALESCE(asp.all_time_spend, 0) AS all_time_spend,
  -- CPL: use actual quality leads (contacts - period spam count)
  CASE WHEN GREATEST(COALESCE(lc.quality_leads, 0) - COALESCE(gm.spam_count, 0), 0) > 0
    THEN ROUND(COALESCE(asp.ad_spend, 0) / GREATEST(COALESCE(lc.quality_leads, 0) - COALESCE(gm.spam_count, 0), 1), 2)
    WHEN COALESCE(lc.quality_leads, 0) > 0
    THEN ROUND(COALESCE(asp.ad_spend, 0) / lc.quality_leads, 2)
    ELSE 0
  END AS cpl,
  COALESCE(cm.total_calls, 0)::INT,
  COALESCE(gm.spam_count, 0)::INT,
  COALESCE(gm.abandoned_count, 0)::INT,
  -- Spam/abandoned rates: based on period contacts, not all-time
  CASE WHEN COALESCE(lc.quality_leads, 0) > 0
    THEN ROUND(COALESCE(gm.spam_count, 0)::numeric / lc.quality_leads, 3)
    ELSE 0
  END AS spam_rate,
  CASE WHEN COALESCE(lc.quality_leads, 0) > 0
    THEN ROUND(COALESCE(gm.abandoned_count, 0)::numeric / lc.quality_leads, 3)
    ELSE 0
  END AS abandoned_rate,
  cm.days_since_lead,
  COALESCE(cm.biz_hour_calls, 0)::INT,
  COALESCE(cm.biz_hour_answered, 0)::INT,
  CASE WHEN COALESCE(cm.biz_hour_calls, 0) > 0
    THEN ROUND(cm.biz_hour_answered::numeric / cm.biz_hour_calls, 3)
    ELSE NULL
  END AS call_answer_rate,
  COALESCE(rh.insp_booked, 0)::INT AS hcp_insp_booked,
  COALESCE(rh.period_rev, 0) AS hcp_closed_rev,
  COALESCE(oeh.open_est_rev, 0) AS hcp_open_est_rev,
  COALESCE(rh.approved_no_invoice_rev, 0) AS hcp_approved_no_inv,
  COALESCE(rj.insp_booked, 0)::INT AS jobber_insp_booked,
  COALESCE(rj.period_rev, 0) AS jobber_closed_rev,
  COALESCE(oej.open_est_rev, 0) AS jobber_open_est_rev,
  (COALESCE(rh.insp_booked, 0) + COALESCE(rj.insp_booked, 0))::INT AS total_insp_booked,
  -- Closed Rev = period revenue (for the table display)
  (COALESCE(rh.period_rev, 0) + COALESCE(rj.period_rev, 0)) AS total_closed_rev,
  (COALESCE(oeh.open_est_rev, 0) + COALESCE(oej.open_est_rev, 0)) AS total_open_est_rev,
  COALESCE(rh.approved_no_invoice_rev, 0) AS approved_no_inv,
  (COALESCE(och.on_cal_14d, 0) + COALESCE(ocj.on_cal_14d, 0))::INT AS on_cal_14d,
  (COALESCE(och.on_cal_14d_all, 0) + COALESCE(ocj.on_cal_14d_all, 0))::INT AS on_cal_total,
  COALESCE(lsa.lsa_spend, 0) AS lsa_spend,
  COALESCE(lsa.lsa_leads, 0)::INT AS lsa_leads,
  -- Derived: book rate = inspections / actual quality leads (contacts - spam)
  CASE
    WHEN GREATEST(COALESCE(lc.quality_leads, 0) - COALESCE(gm.spam_count, 0), 0) > 0
    THEN ROUND(
      (COALESCE(rh.insp_booked, 0) + COALESCE(rj.insp_booked, 0))::numeric
      / GREATEST(COALESCE(lc.quality_leads, 0) - COALESCE(gm.spam_count, 0), 1)
    , 3)
    WHEN COALESCE(lc.quality_leads, 0) > 0
    THEN ROUND((COALESCE(rh.insp_booked, 0) + COALESCE(rj.insp_booked, 0))::numeric / lc.quality_leads, 3)
    ELSE 0
  END AS insp_booked_pct,
  -- ROAS = period revenue / period spend
  CASE WHEN COALESCE(asp.ad_spend, 0) > 0
    THEN ROUND((COALESCE(rh.period_rev, 0) + COALESCE(rj.period_rev, 0)) / asp.ad_spend, 3)
    ELSE 0
  END AS roas,
  -- Guarantee = all-time revenue / all-time spend
  CASE WHEN COALESCE(asp.all_time_spend, 0) > 0
    THEN ROUND((COALESCE(rh.all_time_rev, 0) + COALESCE(rj.all_time_rev, 0)) / asp.all_time_spend, 3)
    ELSE 0
  END AS guarantee,
  (COALESCE(rh.all_time_rev, 0) + COALESCE(rj.all_time_rev, 0)) AS all_time_rev,
  -- Prior CPL: prior spend / prior leads (simplified — no spam adjustment for prior period)
  CASE WHEN COALESCE(lc.prior_quality_leads, 0) > 0
    THEN ROUND(COALESCE(asp.prior_ad_spend, 0) / lc.prior_quality_leads, 2)
    ELSE 0
  END AS prior_cpl

FROM client_base cb
LEFT JOIN lead_counts lc ON lc.customer_id = cb.customer_id
LEFT JOIN ad_spend asp ON asp.customer_id = cb.customer_id
LEFT JOIN lsa_metrics lsa ON lsa.customer_id = cb.customer_id
LEFT JOIN call_metrics cm ON cm.customer_id = cb.customer_id
LEFT JOIN ghl_metrics gm ON gm.customer_id = cb.customer_id
LEFT JOIN revenue_hcp rh ON rh.customer_id = cb.customer_id
LEFT JOIN open_est_hcp oeh ON oeh.customer_id = cb.customer_id
LEFT JOIN revenue_jobber rj ON rj.customer_id = cb.customer_id
LEFT JOIN open_est_jobber oej ON oej.customer_id = cb.customer_id
LEFT JOIN on_cal_hcp och ON och.customer_id = cb.customer_id
LEFT JOIN on_cal_jobber ocj ON ocj.customer_id = cb.customer_id;

$function$

