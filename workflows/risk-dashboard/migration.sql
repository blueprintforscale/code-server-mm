-- Risk Dashboard SQL Functions
-- Deploy: scp to Mac Mini, run via psql -f

-- ============================================================
-- Helper: Google Ads call attribution check
-- Returns TRUE if a call is from Google Ads (Rule #22 + #35)
-- Uses raw CallRail source field + gclid fallback, excludes
-- mislabeled GBP/GMB tracking numbers (e.g. Chad Adams)
-- ============================================================
CREATE OR REPLACE FUNCTION is_google_ads_call(
  p_source TEXT,
  p_source_name TEXT,
  p_gclid TEXT
) RETURNS BOOLEAN
LANGUAGE sql IMMUTABLE AS $$
  SELECT (
    -- Source-based: GA tracking number, but NOT mislabeled GBP/GMB numbers
    (p_source IN ('Google Ads', 'Google Ads 2')
     AND COALESCE(p_source_name, '') NOT IN ('GBP', 'GMB Call Extension', 'Main Business Line')
     AND COALESCE(p_source_name, '') NOT ILIKE '%gmb%'
     AND COALESCE(p_source_name, '') NOT ILIKE '%gbp%')
    -- GCLID fallback: multi-touch (clicked ad, called via different number)
    OR (p_gclid IS NOT NULL AND p_gclid != '')
  )
$$;

-- ============================================================
-- 1A. get_dashboard_metrics(p_start, p_end)
-- Returns one row per active client with all metrics
-- ============================================================

CREATE OR REPLACE FUNCTION get_dashboard_metrics(
  p_start DATE DEFAULT CURRENT_DATE - INTERVAL '30 days',
  p_end   DATE DEFAULT CURRENT_DATE
)
RETURNS TABLE (
  customer_id        BIGINT,
  client_name        TEXT,
  ads_manager        TEXT,
  inspection_type    TEXT,
  budget             NUMERIC,
  months_in_program  INT,
  start_date         DATE,
  field_mgmt         TEXT,
  timezone           TEXT,
  quality_leads      INT,
  prior_quality_leads INT,
  actual_quality_leads INT,
  prior_actual_quality_leads INT,
  spam_contacts      INT,
  lead_volume_change NUMERIC,
  ad_spend           NUMERIC,
  all_time_spend     NUMERIC,
  cpl                NUMERIC,
  total_calls        INT,
  spam_rate          NUMERIC,
  abandoned_rate     NUMERIC,
  days_since_lead    INT,
  biz_hour_calls     INT,
  biz_hour_answered  INT,
  call_answer_rate   NUMERIC,
  hcp_insp_booked    INT,
  hcp_closed_rev     NUMERIC,
  hcp_open_est_rev   NUMERIC,
  hcp_approved_no_inv NUMERIC,
  jobber_insp_booked INT,
  jobber_closed_rev  NUMERIC,
  jobber_open_est_rev NUMERIC,
  total_insp_booked  INT,
  total_closed_rev   NUMERIC,
  total_open_est_rev NUMERIC,
  approved_no_inv    NUMERIC,
  on_cal_14d         INT,
  on_cal_total       INT,
  lsa_spend          NUMERIC,
  lsa_leads          INT,
  insp_booked_pct    NUMERIC,
  roas               NUMERIC,
  period_potential_roas NUMERIC,
  guarantee          NUMERIC,
  all_time_rev       NUMERIC,
  trailing_6mo_roas  NUMERIC,
  trailing_6mo_potential_roas NUMERIC,
  trailing_3mo_roas  NUMERIC,
  trailing_3mo_potential_roas NUMERIC,
  prior_cpl          NUMERIC,
  risk_override      TEXT
) LANGUAGE SQL STABLE AS $$

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
    c.risk_override,
    COALESCE(c.program_price, 0) AS program_price,
    GREATEST(1, (EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM c.start_date))::INT * 12
      + (EXTRACT(MONTH FROM CURRENT_DATE) - EXTRACT(MONTH FROM c.start_date))::INT + 1) AS months_in_program
  FROM clients c
  WHERE c.status = 'active'
    AND c.start_date IS NOT NULL
    AND c.budget IS NOT NULL
    AND c.budget > 0
    AND c.parent_customer_id IS NULL  -- exclude child accounts
),

-- Map child account IDs to their parent for data rollup
client_ids AS (
  SELECT cb.customer_id AS parent_id, cb.customer_id AS data_id FROM client_base cb
  UNION ALL
  SELECT ch.parent_customer_id AS parent_id, ch.customer_id AS data_id
  FROM clients ch
  WHERE ch.parent_customer_id IS NOT NULL
    AND ch.status = 'active'
    AND EXISTS (SELECT 1 FROM client_base cb WHERE cb.customer_id = ch.parent_customer_id)
),

-- ALL contacts from calls (GA, any classification — for Contacts count)
all_call_contacts AS (
  SELECT
    ci.parent_id AS customer_id,
    normalize_phone(ca.caller_phone) AS dedup_phone,
    ca.start_time::date AS lead_date
  FROM calls ca
  INNER JOIN client_ids ci ON ci.data_id = ca.customer_id
  WHERE is_google_ads_call(ca.source, ca.source_name, ca.gclid)
    AND ca.start_time::date BETWEEN (p_start - (p_end - p_start)) AND p_end
),

-- ALL contacts from forms (GA, any classification)
all_form_contacts AS (
  SELECT
    ci.parent_id AS customer_id,
    COALESCE(normalize_phone(fs.customer_phone), 'form_' || fs.callrail_id) AS dedup_phone,
    LOWER(NULLIF(TRIM(fs.customer_email), '')) AS lead_email,
    fs.submitted_at::date AS lead_date
  FROM form_submissions fs
  INNER JOIN client_ids ci ON ci.data_id = fs.customer_id
  WHERE fs.gclid IS NOT NULL AND fs.gclid != ''
    AND fs.submitted_at::date BETWEEN (p_start - (p_end - p_start)) AND p_end
    AND NOT EXISTS (
      SELECT 1 FROM all_call_contacts ac
      WHERE ac.customer_id = ci.parent_id
        AND ac.dedup_phone = normalize_phone(fs.customer_phone)
    )
),

-- GHL-identified spam: by phone (all-time, permanent)
ghl_spam_phones AS (
  SELECT DISTINCT ci.parent_id AS customer_id, gc.phone_normalized
  FROM ghl_contacts gc
  INNER JOIN client_ids ci ON ci.data_id = gc.customer_id
  LEFT JOIN clients cl ON cl.customer_id = ci.parent_id
  WHERE gc.phone_normalized IS NOT NULL AND gc.phone_normalized != ''
    AND (
      LOWER(COALESCE(gc.lost_reason, '')) SIMILAR TO '%(spam|not a lead|wrong number|out of area|wrong service)%'
      OR EXISTS (
        SELECT 1 FROM ghl_opportunities o
        WHERE o.ghl_contact_id = gc.ghl_contact_id
          AND LOWER(o.stage_name) SIMILAR TO '%(spam|not a lead|out of area|wrong service)%'
      )
      OR (cl.extra_spam_keywords IS NOT NULL
          AND LOWER(COALESCE(gc.lost_reason, '')) = ANY(SELECT LOWER(k) FROM unnest(cl.extra_spam_keywords) k))
    )
),

-- GHL-identified spam: by email (all-time, for form leads without phone)
ghl_spam_emails AS (
  SELECT DISTINCT ci.parent_id AS customer_id, LOWER(gc.email) AS email
  FROM ghl_contacts gc
  INNER JOIN client_ids ci ON ci.data_id = gc.customer_id
  LEFT JOIN clients cl ON cl.customer_id = ci.parent_id
  WHERE gc.email IS NOT NULL AND gc.email != ''
    AND (
      LOWER(COALESCE(gc.lost_reason, '')) SIMILAR TO '%(spam|not a lead|wrong number|out of area|wrong service)%'
      OR EXISTS (
        SELECT 1 FROM ghl_opportunities o
        WHERE o.ghl_contact_id = gc.ghl_contact_id
          AND LOWER(o.stage_name) SIMILAR TO '%(spam|not a lead|out of area|wrong service)%'
      )
      OR (cl.extra_spam_keywords IS NOT NULL
          AND LOWER(COALESCE(gc.lost_reason, '')) = ANY(SELECT LOWER(k) FROM unnest(cl.extra_spam_keywords) k))
    )
),

-- GA contacts matched to GHL abandoned (period-scoped by lead date)
-- Matches by phone against GHL opps with status='abandoned' OR lost_reason containing 'abandoned'
ga_abandoned_contacts AS (
  SELECT DISTINCT al.customer_id, al.dedup_phone
  FROM (
    SELECT customer_id, dedup_phone, lead_date FROM all_call_contacts
    UNION ALL
    SELECT customer_id, dedup_phone, lead_date FROM all_form_contacts
  ) al
  WHERE al.lead_date BETWEEN p_start AND p_end
    AND EXISTS (
      SELECT 1 FROM ghl_contacts gc
      INNER JOIN client_ids ci ON ci.data_id = gc.customer_id
      WHERE ci.parent_id = al.customer_id
        AND gc.phone_normalized = al.dedup_phone
        AND (
          -- Opportunity status = abandoned
          EXISTS (
            SELECT 1 FROM ghl_opportunities o
            WHERE o.ghl_contact_id = gc.ghl_contact_id
              AND o.status = 'abandoned'
          )
          -- OR lost_reason contains abandoned
          OR LOWER(COALESCE(gc.lost_reason, '')) LIKE '%abandoned%'
        )
    )
),

-- Per-client abandoned rate for the period (used to trigger abandoned→spam reclassification)
abandoned_rates AS (
  SELECT
    al.customer_id,
    COUNT(DISTINCT al.dedup_phone)::INT AS total_ga_contacts,
    COUNT(DISTINCT ab.dedup_phone)::INT AS abandoned_count,
    CASE WHEN COUNT(DISTINCT al.dedup_phone) > 0
      THEN COUNT(DISTINCT ab.dedup_phone)::numeric / COUNT(DISTINCT al.dedup_phone)
      ELSE 0
    END AS rate
  FROM (
    SELECT customer_id, dedup_phone, lead_date FROM all_call_contacts
    UNION ALL
    SELECT customer_id, dedup_phone, lead_date FROM all_form_contacts
  ) al
  LEFT JOIN ga_abandoned_contacts ab
    ON ab.customer_id = al.customer_id AND ab.dedup_phone = al.dedup_phone
  WHERE al.lead_date BETWEEN p_start AND p_end
  GROUP BY al.customer_id
),

-- GHL abandoned phones that become spam when client's abandoned rate > 20%
ghl_abandoned_as_spam AS (
  SELECT DISTINCT gc.phone_normalized, ci.parent_id AS customer_id
  FROM ghl_contacts gc
  INNER JOIN client_ids ci ON ci.data_id = gc.customer_id
  INNER JOIN abandoned_rates ar ON ar.customer_id = ci.parent_id
  WHERE ar.rate > 0.20
    AND gc.phone_normalized IS NOT NULL AND gc.phone_normalized != ''
    AND LOWER(COALESCE(gc.lost_reason, '')) LIKE '%abandoned%'
),

-- Combined spam check: phone in ghl_spam_phones OR ghl_abandoned_as_spam, OR email in ghl_spam_emails
-- Used as a single predicate in lead_counts
-- Combine and count contacts (all) + quality (non-spam) + prior period
lead_counts AS (
  SELECT
    customer_id,
    COUNT(DISTINCT dedup_phone) FILTER (WHERE lead_date BETWEEN p_start AND p_end)::INT AS quality_leads,
    COUNT(DISTINCT dedup_phone) FILTER (WHERE lead_date BETWEEN (p_start - (p_end - p_start)) AND (p_start - 1))::INT AS prior_quality_leads,
    -- Actual quality: contacts NOT flagged as spam in GHL (by phone OR email)
    COUNT(DISTINCT dedup_phone) FILTER (
      WHERE lead_date BETWEEN p_start AND p_end
        AND NOT EXISTS (SELECT 1 FROM ghl_spam_phones sp WHERE sp.customer_id = all_leads.customer_id AND sp.phone_normalized = all_leads.dedup_phone)
        AND NOT EXISTS (SELECT 1 FROM ghl_abandoned_as_spam aas WHERE aas.customer_id = all_leads.customer_id AND aas.phone_normalized = all_leads.dedup_phone)
        AND NOT EXISTS (SELECT 1 FROM ghl_spam_emails se WHERE se.customer_id = all_leads.customer_id AND se.email = all_leads.lead_email)
    )::INT AS actual_quality_leads,
    COUNT(DISTINCT dedup_phone) FILTER (
      WHERE lead_date BETWEEN (p_start - (p_end - p_start)) AND (p_start - 1)
        AND NOT EXISTS (SELECT 1 FROM ghl_spam_phones sp WHERE sp.customer_id = all_leads.customer_id AND sp.phone_normalized = all_leads.dedup_phone)
        AND NOT EXISTS (SELECT 1 FROM ghl_abandoned_as_spam aas WHERE aas.customer_id = all_leads.customer_id AND aas.phone_normalized = all_leads.dedup_phone)
        AND NOT EXISTS (SELECT 1 FROM ghl_spam_emails se WHERE se.customer_id = all_leads.customer_id AND se.email = all_leads.lead_email)
    )::INT AS prior_actual_quality_leads,
    -- Spam contacts in current period (phone OR email match)
    COUNT(DISTINCT dedup_phone) FILTER (
      WHERE lead_date BETWEEN p_start AND p_end
        AND (
          EXISTS (SELECT 1 FROM ghl_spam_phones sp WHERE sp.customer_id = all_leads.customer_id AND sp.phone_normalized = all_leads.dedup_phone)
          OR EXISTS (SELECT 1 FROM ghl_abandoned_as_spam aas WHERE aas.customer_id = all_leads.customer_id AND aas.phone_normalized = all_leads.dedup_phone)
          OR EXISTS (SELECT 1 FROM ghl_spam_emails se WHERE se.customer_id = all_leads.customer_id AND se.email = all_leads.lead_email)
        )
    )::INT AS spam_contacts,
    -- Calendar month counts for pro-rated monthly pace
    COUNT(DISTINCT dedup_phone) FILTER (WHERE lead_date >= DATE_TRUNC('month', CURRENT_DATE))::INT AS this_month_leads,
    COUNT(DISTINCT dedup_phone) FILTER (WHERE lead_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND lead_date < DATE_TRUNC('month', CURRENT_DATE))::INT AS last_month_leads
  FROM (
    SELECT customer_id, dedup_phone, NULL::text AS lead_email, lead_date FROM all_call_contacts
    UNION ALL
    SELECT customer_id, dedup_phone, lead_email, lead_date FROM all_form_contacts
  ) all_leads
  GROUP BY customer_id
),

-- Ad spend (excluding LSA / Local Services campaigns)
ad_spend AS (
  SELECT
    ci.parent_id AS customer_id,
    COALESCE(SUM(cdm.cost) FILTER (WHERE cdm.date BETWEEN p_start AND p_end), 0) AS ad_spend,
    COALESCE(SUM(cdm.cost) FILTER (WHERE cdm.date BETWEEN (p_start - (p_end - p_start)) AND (p_start - 1)), 0) AS prior_ad_spend,
    COALESCE(SUM(cdm.cost), 0) AS all_time_spend,
    COALESCE(SUM(cdm.cost) FILTER (WHERE cdm.date >= p_end - 180), 0) AS trailing_6mo_spend,
    COALESCE(SUM(cdm.cost) FILTER (WHERE cdm.date >= p_end - 90), 0) AS trailing_3mo_spend
  FROM campaign_daily_metrics cdm
  INNER JOIN client_ids ci ON ci.data_id = cdm.customer_id
  WHERE cdm.campaign_type != 'LOCAL_SERVICES'
  GROUP BY ci.parent_id
),

-- LSA spend + leads (tracked separately)
lsa_metrics AS (
  SELECT
    ci.parent_id AS customer_id,
    COALESCE(SUM(cdm.cost) FILTER (WHERE cdm.date BETWEEN p_start AND p_end), 0) AS lsa_spend,
    (SELECT COUNT(DISTINCT normalize_phone(ca.caller_phone))
     FROM calls ca
     INNER JOIN client_ids ci2 ON ci2.data_id = ca.customer_id AND ci2.parent_id = ci.parent_id
     WHERE ca.source_name = 'LSA'
       AND ca.classification = 'legitimate'
       AND ca.start_time::date BETWEEN p_start AND p_end
    )::INT AS lsa_leads
  FROM campaign_daily_metrics cdm
  INNER JOIN client_ids ci ON ci.data_id = cdm.customer_id
  WHERE cdm.campaign_type = 'LOCAL_SERVICES'
  GROUP BY ci.parent_id
),

-- Call metrics (days since lead + answer rate from CallRail)
call_metrics AS (
  SELECT
    cb.customer_id,
    COUNT(*) FILTER (WHERE ca.start_time::date BETWEEN p_start AND p_end
                     AND is_google_ads_call(ca.source, ca.source_name, ca.gclid))::INT AS total_calls,
    -- Days since last lead: most recent contact (call OR form) from Google Ads, regardless of classification
    (CURRENT_DATE - GREATEST(
      MAX(ca.start_time::date) FILTER (WHERE is_google_ads_call(ca.source, ca.source_name, ca.gclid)),
      (SELECT MAX(fs.submitted_at::date) FROM form_submissions fs
       INNER JOIN client_ids ci2 ON ci2.data_id = fs.customer_id AND ci2.parent_id = cb.customer_id
       WHERE (fs.gclid IS NOT NULL OR fs.source = 'Google Ads'))
    ))::INT AS days_since_lead,
    -- Answer rate: first-time GA callers during business hours only
    -- Excludes abandoned calls (hung up <10s before anyone could answer) from denominator
    COUNT(*) FILTER (
      WHERE ca.start_time::date BETWEEN p_start AND p_end
        AND is_google_ads_call(ca.source, ca.source_name, ca.gclid)
        AND ca.first_call = true
        AND COALESCE(ca.classified_status, CASE WHEN ca.answered THEN 'answered' ELSE 'missed' END) != 'abandoned'
        AND EXTRACT(DOW FROM (ca.start_time AT TIME ZONE COALESCE(cb.timezone, 'America/New_York')))::INT
            = ANY(COALESCE(cb.biz_days, ARRAY[1,2,3,4,5]))
        AND (ca.start_time AT TIME ZONE COALESCE(cb.timezone, 'America/New_York'))::time
            BETWEEN COALESCE(cb.biz_hours_start::time, '08:00'::time)
                AND COALESCE(cb.biz_hours_end::time, '18:00'::time)
    )::INT AS biz_hour_calls,
    COUNT(*) FILTER (
      WHERE ca.start_time::date BETWEEN p_start AND p_end
        AND is_google_ads_call(ca.source, ca.source_name, ca.gclid)
        AND ca.first_call = true
        AND COALESCE(ca.classified_status, CASE WHEN ca.answered THEN 'answered' ELSE 'missed' END) = 'answered'
        AND EXTRACT(DOW FROM (ca.start_time AT TIME ZONE COALESCE(cb.timezone, 'America/New_York')))::INT
            = ANY(COALESCE(cb.biz_days, ARRAY[1,2,3,4,5]))
        AND (ca.start_time AT TIME ZONE COALESCE(cb.timezone, 'America/New_York'))::time
            BETWEEN COALESCE(cb.biz_hours_start::time, '08:00'::time)
                AND COALESCE(cb.biz_hours_end::time, '18:00'::time)
    )::INT AS biz_hour_answered
  FROM client_base cb
  LEFT JOIN client_ids ci ON ci.parent_id = cb.customer_id
  LEFT JOIN calls ca ON ca.customer_id = ci.data_id
  GROUP BY cb.customer_id, cb.timezone, cb.biz_days, cb.biz_hours_start, cb.biz_hours_end
),

-- GHL contact quality from opportunities (cumulative)
-- "Not quality" = spam, not a lead, wrong number, invalid number, out of area, wrong service
ghl_metrics AS (
  SELECT
    ci.parent_id AS customer_id,
    COUNT(*)::INT AS total_opps,
    -- Spam/not-quality: broad definition
    COUNT(*) FILTER (
      WHERE (
        -- Lost with bad reason
        o.status = 'lost' AND (
          LOWER(COALESCE(c.lost_reason, '')) LIKE '%spam%'
          OR LOWER(COALESCE(c.lost_reason, '')) LIKE '%not a lead%'
          OR LOWER(COALESCE(c.lost_reason, '')) LIKE '%wrong number%'
          OR LOWER(COALESCE(c.lost_reason, '')) LIKE '%invalid%'
          OR LOWER(COALESCE(c.lost_reason, '')) LIKE '%out of area%'
          OR LOWER(COALESCE(c.lost_reason, '')) LIKE '%wrong service%'
        )
      ) OR (
        -- Stage indicates bad lead
        LOWER(o.stage_name) LIKE '%spam%'
        OR LOWER(o.stage_name) LIKE '%not a lead%'
        OR LOWER(o.stage_name) LIKE '%out of area%'
      )
    )::INT AS spam_count,
    COUNT(*) FILTER (WHERE o.status = 'abandoned')::INT AS abandoned_count_legacy
  FROM ghl_opportunities o
  INNER JOIN client_ids ci ON ci.data_id = o.customer_id
  LEFT JOIN ghl_contacts c ON c.ghl_contact_id = o.ghl_contact_id
    AND c.customer_id = o.customer_id
  GROUP BY ci.parent_id
),

-- Pre-compute GA attribution for HCP customers
-- Uses hcp_created_at as lead_date (matches portal dashboard scoping)
hcp_ga_attribution AS (
  SELECT hc.hcp_customer_id, hc.customer_id, hc.hcp_created_at::date AS lead_date
  FROM hcp_customers hc
  INNER JOIN client_ids ci ON ci.data_id = hc.customer_id
  WHERE hc.attribution_override = 'google_ads'
     OR hc.callrail_id LIKE 'WF_%'
     OR EXISTS (SELECT 1 FROM calls ca WHERE ca.callrail_id = hc.callrail_id
         AND is_google_ads_call(ca.source, ca.source_name, ca.gclid)
         AND COALESCE(ca.source_name, '') <> 'LSA')
     OR EXISTS (SELECT 1 FROM form_submissions fs WHERE fs.customer_id = hc.customer_id
         AND (fs.callrail_id = hc.callrail_id
              OR fs.phone_normalized = hc.phone_normalized
              OR (hc.email IS NOT NULL AND LOWER(fs.customer_email) = LOWER(hc.email)))
         AND (fs.gclid IS NOT NULL OR fs.source = 'Google Ads'))
),

-- HCP revenue: period-scoped for ROAS, all-time for guarantee
-- Uses callrail_id + form phone/email GA attribution (matches portal dashboard logic)
revenue_hcp AS (
  SELECT
    ci.parent_id AS customer_id,
    COUNT(DISTINCT CASE
      WHEN hc.hcp_created_at::date BETWEEN p_start AND p_end
        AND lp.inspection_scheduled_at IS NOT NULL
      THEN hc.hcp_customer_id
    END)::INT AS insp_booked,
    COALESCE(ROUND(SUM(roas_rev.cents) FILTER (
      WHERE hc.hcp_created_at::date BETWEEN p_start AND p_end
    ) / 100.0, 2), 0) AS period_rev,
    COALESCE(ROUND(SUM(roas_rev.cents) / 100.0, 2), 0) AS all_time_rev,
    COALESCE(ROUND(SUM(roas_rev.cents) FILTER (
      WHERE hc.hcp_created_at::date >= p_end - 180
    ) / 100.0, 2), 0) AS trailing_6mo_rev,
    COALESCE(ROUND(SUM(pipeline_rev.cents) FILTER (
      WHERE hc.hcp_created_at::date >= p_end - 180
    ) / 100.0, 2), 0) AS trailing_6mo_open_est,
    COALESCE(ROUND(SUM(roas_rev.cents) FILTER (
      WHERE hc.hcp_created_at::date >= p_end - 90
    ) / 100.0, 2), 0) AS trailing_3mo_rev,
    COALESCE(ROUND(SUM(pipeline_rev.cents) FILTER (
      WHERE hc.hcp_created_at::date >= p_end - 90
    ) / 100.0, 2), 0) AS trailing_3mo_open_est,
    COALESCE(ROUND(SUM(CASE
      WHEN approved_est.cents > 0 AND treat_inv.cents = 0 THEN approved_est.cents ELSE 0
    END) / 100.0, 2), 0) AS approved_no_invoice_rev
  FROM hcp_customers hc
  INNER JOIN client_ids ci ON ci.data_id = hc.customer_id
  INNER JOIN hcp_ga_attribution ga_lead ON ga_lead.hcp_customer_id = hc.hcp_customer_id AND ga_lead.customer_id = hc.customer_id
  LEFT JOIN v_lead_pipeline lp ON lp.hcp_customer_id = hc.hcp_customer_id AND lp.customer_id = hc.customer_id
  -- ROAS revenue waterfall: insp_invoice + GREATEST(treat_invoice, approved_est)
  LEFT JOIN LATERAL (
    SELECT COALESCE(SUM(i.amount_cents) FILTER (WHERE i.invoice_type = 'inspection'), 0) AS insp_inv,
           COALESCE(SUM(i.amount_cents) FILTER (WHERE i.invoice_type = 'treatment'), 0) AS treat_inv,
           COALESCE((SELECT SUM(eg.approved_total_cents) FROM v_estimate_groups eg
             WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status = 'approved' AND eg.count_revenue), 0) AS approved_est
    FROM hcp_invoices i
    WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status NOT IN ('canceled','voided')
  ) rev_parts ON TRUE
  LEFT JOIN LATERAL (
    SELECT rev_parts.insp_inv + GREATEST(rev_parts.treat_inv, rev_parts.approved_est) AS cents
  ) roas_rev ON TRUE
  LEFT JOIN LATERAL (
    SELECT rev_parts.treat_inv AS cents
  ) treat_inv ON TRUE
  LEFT JOIN LATERAL (
    SELECT rev_parts.approved_est AS cents
  ) approved_est ON TRUE
  LEFT JOIN LATERAL (
    SELECT CASE
      WHEN rev_parts.treat_inv = 0 AND rev_parts.approved_est = 0
        THEN COALESCE((SELECT SUM(eg.highest_option_cents) FROM v_estimate_groups eg
          WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status = 'sent' AND eg.count_revenue), 0)
      ELSE 0
    END AS cents
  ) pipeline_rev ON TRUE
  GROUP BY ci.parent_id
),

-- HCP open estimates (parameterized by p_start/p_end, GA-only)
-- Scoped by LEAD DATE (first CallRail contact), not estimate sent date
open_est_hcp AS (
  SELECT
    ci.parent_id AS customer_id,
    COALESCE(ROUND(SUM(e.highest_option_cents::numeric / 100.0), 2), 0) AS open_est_rev
  FROM hcp_estimates e
  INNER JOIN client_ids ci ON ci.data_id = e.customer_id
  LEFT JOIN hcp_customers hc ON hc.hcp_customer_id = e.hcp_customer_id
  WHERE e.status <> 'approved'
    AND e.sent_at IS NOT NULL
    AND e.highest_option_cents > 0
    AND EXISTS (
      SELECT 1 FROM calls c
      WHERE normalize_phone(c.caller_phone) = hc.phone_normalized
        AND c.customer_id = e.customer_id
        AND is_google_ads_call(c.source, c.source_name, c.gclid)
        AND c.start_time::date BETWEEN p_start AND p_end
    )
  GROUP BY ci.parent_id
),

-- Jobber revenue: period-scoped for ROAS, all-time for guarantee
revenue_jobber AS (
  SELECT
    ci.parent_id AS customer_id,
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
    ) / 100.0, 2), 0) AS all_time_rev,
    -- Trailing 6-month revenue
    COALESCE(ROUND(SUM(jlr.roas_revenue_cents) FILTER (
      WHERE jlr.lead_source_type = 'google_ads'
        AND COALESCE(ca.start_time::date, fs.submitted_at::date) >= p_end - 180
    ) / 100.0, 2), 0) AS trailing_6mo_rev,
    -- Trailing 6-month pipeline quotes (open estimates)
    COALESCE(ROUND(SUM(jlr.pipeline_quote_cents) FILTER (
      WHERE jlr.lead_source_type = 'google_ads'
        AND COALESCE(ca.start_time::date, fs.submitted_at::date) >= p_end - 180
    ) / 100.0, 2), 0) AS trailing_6mo_open_est,
    -- Trailing 3-month revenue
    COALESCE(ROUND(SUM(jlr.roas_revenue_cents) FILTER (
      WHERE jlr.lead_source_type = 'google_ads'
        AND COALESCE(ca.start_time::date, fs.submitted_at::date) >= p_end - 90
    ) / 100.0, 2), 0) AS trailing_3mo_rev,
    -- Trailing 3-month pipeline quotes
    COALESCE(ROUND(SUM(jlr.pipeline_quote_cents) FILTER (
      WHERE jlr.lead_source_type = 'google_ads'
        AND COALESCE(ca.start_time::date, fs.submitted_at::date) >= p_end - 90
    ) / 100.0, 2), 0) AS trailing_3mo_open_est
  FROM v_jobber_lead_revenue jlr
  INNER JOIN client_ids ci ON ci.data_id = jlr.customer_id
  LEFT JOIN calls ca ON ca.callrail_id = jlr.callrail_id
  LEFT JOIN form_submissions fs ON fs.callrail_id = jlr.callrail_id
  GROUP BY ci.parent_id
),

-- Jobber open estimates (parameterized by p_start/p_end, GA-only)
-- Scoped by LEAD DATE (first CallRail contact), not quote creation date
open_est_jobber AS (
  SELECT
    ci.parent_id AS customer_id,
    COALESCE(ROUND(SUM(q.total_cents::numeric / 100.0), 2), 0) AS open_est_rev
  FROM jobber_quotes q
  INNER JOIN client_ids ci ON ci.data_id = q.customer_id
  LEFT JOIN jobber_customers jc ON jc.jobber_customer_id = q.jobber_customer_id
  WHERE LOWER(q.status) IN ('awaiting_response', 'changes_requested', 'draft', 'sent')
    AND EXISTS (
      SELECT 1 FROM calls c
      WHERE normalize_phone(c.caller_phone) = jc.phone_normalized
        AND c.customer_id = q.customer_id
        AND is_google_ads_call(c.source, c.source_name, c.gclid)
        AND c.start_time::date BETWEEN p_start AND p_end
    )
  GROUP BY ci.parent_id
),

-- On-calendar: future HCP inspections (GA + all)
on_cal_hcp AS (
  SELECT
    ci.parent_id AS customer_id,
    -- GA-attributed: call match OR form match OR webflow match
    COUNT(*) FILTER (WHERE hi.scheduled_at::date BETWEEN CURRENT_DATE AND CURRENT_DATE + 13
      AND EXISTS (
        SELECT 1 FROM hcp_customers hc
        WHERE hc.hcp_customer_id = hi.hcp_customer_id AND hc.customer_id = hi.customer_id
          AND (
            -- Call with GA attribution
            EXISTS (SELECT 1 FROM calls ca WHERE normalize_phone(ca.caller_phone) = hc.phone_normalized
              AND ca.customer_id = hi.customer_id AND is_google_ads_call(ca.source, ca.source_name, ca.gclid))
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
  INNER JOIN client_ids ci ON ci.data_id = hi.customer_id
  WHERE hi.status IN ('scheduled', 'in_progress', 'needs scheduling')
    AND hi.record_status = 'active'
    AND hi.scheduled_at >= CURRENT_DATE
  GROUP BY ci.parent_id
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
      ci.parent_id AS customer_id,
      COUNT(*) FILTER (WHERE jr.assessment_start_at::date BETWEEN CURRENT_DATE AND CURRENT_DATE + 13
        AND EXISTS (
          SELECT 1 FROM jobber_customers jc
          WHERE jc.jobber_customer_id = jr.jobber_customer_id AND jc.customer_id = jr.customer_id
            AND (
              EXISTS (SELECT 1 FROM calls ca WHERE normalize_phone(ca.caller_phone) = jc.phone_normalized
                AND ca.customer_id = jr.customer_id AND is_google_ads_call(ca.source, ca.source_name, ca.gclid))
              OR EXISTS (SELECT 1 FROM form_submissions fs WHERE normalize_phone(fs.customer_phone) = jc.phone_normalized
                AND fs.customer_id = jr.customer_id AND (fs.gclid IS NOT NULL OR fs.source = 'Google Ads'))
            )
        )
      ) AS ga_count,
      COUNT(*) FILTER (WHERE jr.assessment_start_at::date BETWEEN CURRENT_DATE AND CURRENT_DATE + 13) AS total_count
    FROM jobber_requests jr
    INNER JOIN client_ids ci ON ci.data_id = jr.customer_id
    WHERE jr.has_assessment = true
      AND jr.assessment_start_at >= CURRENT_DATE
      AND jr.assessment_completed_at IS NULL
    GROUP BY ci.parent_id
    UNION ALL
    -- Upcoming inspection-titled jobs
    SELECT
      ci.parent_id AS customer_id,
      COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM jobber_customers jc
        WHERE jc.jobber_customer_id = j.jobber_customer_id AND jc.customer_id = j.customer_id
          AND (
            EXISTS (SELECT 1 FROM calls ca WHERE normalize_phone(ca.caller_phone) = jc.phone_normalized
              AND ca.customer_id = j.customer_id AND is_google_ads_call(ca.source, ca.source_name, ca.gclid))
            OR EXISTS (SELECT 1 FROM form_submissions fs WHERE normalize_phone(fs.customer_phone) = jc.phone_normalized
              AND fs.customer_id = j.customer_id AND (fs.gclid IS NOT NULL OR fs.source = 'Google Ads'))
          )
      )) AS ga_count,
      COUNT(*) AS total_count
    FROM jobber_jobs j
    INNER JOIN client_ids ci ON ci.data_id = j.customer_id
    WHERE j.status = 'upcoming'
      AND (LOWER(j.title) LIKE '%assessment%' OR LOWER(j.title) LIKE '%instascope%'
        OR LOWER(j.title) LIKE '%inspection%' OR LOWER(j.title) LIKE '%mold test%'
        OR LOWER(j.title) LIKE '%air quality%' OR LOWER(j.title) LIKE '%air test%')
    GROUP BY ci.parent_id
  ) combined
  GROUP BY customer_id
),

-- GHL revenue: per-contact GREATEST(invoiced, accepted) — same ROAS waterfall as HCP
-- Step 1: aggregate per contact, then sum across contacts
revenue_ghl AS (
  SELECT
    customer_id,
    SUM(insp_booked)::INT AS insp_booked,
    COALESCE(ROUND(SUM(period_rev) / 100.0, 2), 0) AS period_rev,
    COALESCE(ROUND(SUM(all_time_rev) / 100.0, 2), 0) AS all_time_rev,
    COALESCE(ROUND(SUM(trailing_6mo_rev) / 100.0, 2), 0) AS trailing_6mo_rev,
    COALESCE(ROUND(SUM(trailing_6mo_open_est) / 100.0, 2), 0) AS trailing_6mo_open_est,
    COALESCE(ROUND(SUM(trailing_3mo_rev) / 100.0, 2), 0) AS trailing_3mo_rev,
    COALESCE(ROUND(SUM(trailing_3mo_open_est) / 100.0, 2), 0) AS trailing_3mo_open_est,
    0 AS approved_no_invoice_rev
  FROM (
    SELECT
      ci.parent_id AS customer_id,
      ge.phone_normalized,
      -- Get the earliest GA call date for this contact (lead date)
      ga_lead.lead_date,
      -- Insp booked: count distinct inspection appointments for this contact in period
      (SELECT COUNT(*) FROM ghl_appointments ga
        WHERE ga.ghl_contact_id = ge.ghl_contact_id AND ga.customer_id = ge.customer_id
          AND ga.appointment_type = 'inspection' AND ga.deleted = false
          AND ga.status NOT IN ('cancelled')
          AND ga.start_time::date BETWEEN p_start AND p_end
          AND ga_lead.lead_date IS NOT NULL
      ) AS insp_booked,
      -- All-time rev: GREATEST(invoiced, accepted) for GA-matched contacts
      CASE WHEN ga_lead.lead_date IS NOT NULL
        THEN GREATEST(
          COALESCE(SUM(ge.total_cents) FILTER (WHERE ge.status = 'invoiced'), 0),
          COALESCE(SUM(ge.total_cents) FILTER (WHERE ge.status = 'accepted'), 0)
        ) ELSE 0 END AS all_time_rev,
      -- Period rev: revenue from leads created within p_start..p_end
      CASE WHEN ga_lead.lead_date BETWEEN p_start AND p_end
        THEN GREATEST(
          COALESCE(SUM(ge.total_cents) FILTER (WHERE ge.status = 'invoiced'), 0),
          COALESCE(SUM(ge.total_cents) FILTER (WHERE ge.status = 'accepted'), 0)
        ) ELSE 0 END AS period_rev,
      -- Trailing 6-month rev: leads created in last 180 days
      CASE WHEN ga_lead.lead_date >= p_end - 180
        THEN GREATEST(
          COALESCE(SUM(ge.total_cents) FILTER (WHERE ge.status = 'invoiced'), 0),
          COALESCE(SUM(ge.total_cents) FILTER (WHERE ge.status = 'accepted'), 0)
        ) ELSE 0 END AS trailing_6mo_rev,
      -- Trailing 6-month open est (sent only — accepted is counted as revenue)
      CASE WHEN ga_lead.lead_date >= p_end - 180
        THEN COALESCE(SUM(ge.total_cents) FILTER (WHERE ge.status = 'sent'), 0)
        ELSE 0 END AS trailing_6mo_open_est,
      -- Trailing 3-month rev: leads created in last 90 days
      CASE WHEN ga_lead.lead_date >= p_end - 90
        THEN GREATEST(
          COALESCE(SUM(ge.total_cents) FILTER (WHERE ge.status = 'invoiced'), 0),
          COALESCE(SUM(ge.total_cents) FILTER (WHERE ge.status = 'accepted'), 0)
        ) ELSE 0 END AS trailing_3mo_rev,
      -- Trailing 3-month open est
      CASE WHEN ga_lead.lead_date >= p_end - 90
        THEN COALESCE(SUM(ge.total_cents) FILTER (WHERE ge.status = 'sent'), 0)
        ELSE 0 END AS trailing_3mo_open_est
    FROM ghl_estimates ge
    INNER JOIN client_ids ci ON ci.data_id = ge.customer_id
    LEFT JOIN LATERAL (
      SELECT MIN(lead_date) AS lead_date FROM (
        SELECT MIN(ca.start_time::date) AS lead_date
        FROM calls ca
        WHERE normalize_phone(ca.caller_phone) = ge.phone_normalized
          AND ca.customer_id = ge.customer_id
          AND is_google_ads_call(ca.source, ca.source_name, ca.gclid)
        UNION ALL
        SELECT MIN(fs.submitted_at::date) AS lead_date
        FROM form_submissions fs
        WHERE normalize_phone(fs.customer_phone) = ge.phone_normalized
          AND fs.customer_id = ge.customer_id
          AND fs.gclid IS NOT NULL AND fs.gclid != ''
      ) sources
    ) ga_lead ON TRUE
    WHERE ge.status != 'draft'
    GROUP BY ci.parent_id, ge.phone_normalized, ge.ghl_contact_id, ge.customer_id, ga_lead.lead_date
  ) per_contact
  GROUP BY customer_id
),

-- GHL open estimates (sent only — accepted counted as closed rev in revenue_ghl)
-- GA-matched only (calls or forms), scoped to leads created within the date window
open_est_ghl AS (
  SELECT
    ci.parent_id AS customer_id,
    COALESCE(ROUND(SUM(ge.total_cents) / 100.0, 2), 0) AS open_est_rev
  FROM ghl_estimates ge
  INNER JOIN client_ids ci ON ci.data_id = ge.customer_id
  WHERE ge.status = 'sent'
    AND (
      EXISTS (
        SELECT 1 FROM calls ca
        WHERE normalize_phone(ca.caller_phone) = ge.phone_normalized
          AND ca.customer_id = ge.customer_id
          AND is_google_ads_call(ca.source, ca.source_name, ca.gclid)
          AND ca.start_time::date BETWEEN p_start AND p_end
      )
      OR EXISTS (
        SELECT 1 FROM form_submissions fs
        WHERE normalize_phone(fs.customer_phone) = ge.phone_normalized
          AND fs.customer_id = ge.customer_id
          AND fs.gclid IS NOT NULL AND fs.gclid != ''
          AND fs.submitted_at::date BETWEEN p_start AND p_end
      )
    )
  GROUP BY ci.parent_id
),

-- GHL on-calendar: future inspection appointments (GA + all)
on_cal_ghl AS (
  SELECT
    ci.parent_id AS customer_id,
    COUNT(*) FILTER (WHERE ga.start_time::date BETWEEN CURRENT_DATE AND CURRENT_DATE + 13
      AND EXISTS (
        SELECT 1 FROM calls ca
        WHERE normalize_phone(ca.caller_phone) = ga.phone_normalized
          AND ca.customer_id = ga.customer_id AND is_google_ads_call(ca.source, ca.source_name, ca.gclid)
      )
    )::INT AS on_cal_14d,
    COUNT(*) FILTER (WHERE ga.start_time::date BETWEEN CURRENT_DATE AND CURRENT_DATE + 13)::INT AS on_cal_14d_all
  FROM ghl_appointments ga
  INNER JOIN client_ids ci ON ci.data_id = ga.customer_id
  WHERE ga.appointment_type = 'inspection'
    AND ga.deleted = false
    AND ga.status NOT IN ('cancelled')
    AND ga.start_time >= CURRENT_DATE
  GROUP BY ci.parent_id
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
  COALESCE(lc.actual_quality_leads, 0)::INT,
  COALESCE(lc.prior_actual_quality_leads, 0)::INT,
  COALESCE(lc.spam_contacts, 0)::INT,
  -- Lead volume change: 30-day rolling using actual quality leads
  CASE WHEN COALESCE(lc.prior_actual_quality_leads, 0) > 0
    THEN ROUND((COALESCE(lc.actual_quality_leads, 0) - lc.prior_actual_quality_leads)::numeric / lc.prior_actual_quality_leads, 3)
    ELSE NULL
  END AS lead_volume_change,
  COALESCE(asp.ad_spend, 0) AS ad_spend,
  COALESCE(asp.all_time_spend, 0) AS all_time_spend,
  -- CPL: ad spend / actual quality leads
  CASE WHEN COALESCE(lc.actual_quality_leads, 0) > 0
    THEN ROUND(COALESCE(asp.ad_spend, 0) / lc.actual_quality_leads, 2)
    ELSE 0
  END AS cpl,
  COALESCE(cm.total_calls, 0)::INT,
  -- Spam rate: actual spam contacts / total contacts
  CASE WHEN COALESCE(lc.quality_leads, 0) > 0
    THEN ROUND(COALESCE(lc.spam_contacts, 0)::numeric / lc.quality_leads, 3)
    ELSE 0
  END AS spam_rate,
  -- Abandoned rate: GA abandoned contacts / total GA contacts (period-scoped)
  CASE WHEN COALESCE(abr.total_ga_contacts, 0) > 0
    THEN ROUND(abr.abandoned_count::numeric / abr.total_ga_contacts, 3)
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
  (COALESCE(rh.insp_booked, 0) + COALESCE(rj.insp_booked, 0) + COALESCE(rg.insp_booked, 0))::INT AS total_insp_booked,
  -- Closed Rev = period revenue (for the table display)
  (COALESCE(rh.period_rev, 0) + COALESCE(rj.period_rev, 0) + COALESCE(rg.period_rev, 0)) AS total_closed_rev,
  (COALESCE(oeh.open_est_rev, 0) + COALESCE(oej.open_est_rev, 0) + COALESCE(oeg.open_est_rev, 0)) AS total_open_est_rev,
  COALESCE(rh.approved_no_invoice_rev, 0) AS approved_no_inv,
  (COALESCE(och.on_cal_14d, 0) + COALESCE(ocj.on_cal_14d, 0) + COALESCE(ocg.on_cal_14d, 0))::INT AS on_cal_14d,
  (COALESCE(och.on_cal_14d_all, 0) + COALESCE(ocj.on_cal_14d_all, 0) + COALESCE(ocg.on_cal_14d_all, 0))::INT AS on_cal_total,
  COALESCE(lsa.lsa_spend, 0) AS lsa_spend,
  COALESCE(lsa.lsa_leads, 0)::INT AS lsa_leads,
  -- Derived: book rate = inspections / actual quality leads
  CASE WHEN COALESCE(lc.actual_quality_leads, 0) > 0
    THEN ROUND(
      (COALESCE(rh.insp_booked, 0) + COALESCE(rj.insp_booked, 0) + COALESCE(rg.insp_booked, 0))::numeric
      / lc.actual_quality_leads
    , 3)
    ELSE 0
  END AS insp_booked_pct,
  -- ROAS = period revenue / period spend
  CASE WHEN COALESCE(asp.ad_spend, 0) > 0
    THEN ROUND((COALESCE(rh.period_rev, 0) + COALESCE(rj.period_rev, 0) + COALESCE(rg.period_rev, 0)) / asp.ad_spend, 3)
    ELSE 0
  END AS roas,
  -- Period potential ROAS = (closed rev + open estimates) / spend
  CASE WHEN COALESCE(asp.ad_spend, 0) > 0
    THEN ROUND((COALESCE(rh.period_rev, 0) + COALESCE(rj.period_rev, 0) + COALESCE(rg.period_rev, 0)
      + COALESCE(oeh.open_est_rev, 0) + COALESCE(oej.open_est_rev, 0) + COALESCE(oeg.open_est_rev, 0)) / asp.ad_spend, 3)
    ELSE 0
  END AS period_potential_roas,
  -- Guarantee = all-time revenue / program price (how much of agency fees covered)
  CASE WHEN cb.program_price > 0
    THEN ROUND((COALESCE(rh.all_time_rev, 0) + COALESCE(rj.all_time_rev, 0) + COALESCE(rg.all_time_rev, 0)) / cb.program_price, 3)
    ELSE 0
  END AS guarantee,
  (COALESCE(rh.all_time_rev, 0) + COALESCE(rj.all_time_rev, 0) + COALESCE(rg.all_time_rev, 0)) AS all_time_rev,
  -- Trailing 6-month ROAS (primary ramp-up metric for dashboard)
  CASE WHEN COALESCE(asp.trailing_6mo_spend, 0) > 0
    THEN ROUND((COALESCE(rh.trailing_6mo_rev, 0) + COALESCE(rj.trailing_6mo_rev, 0) + COALESCE(rg.trailing_6mo_rev, 0)) / asp.trailing_6mo_spend, 3)
    ELSE 0
  END AS trailing_6mo_roas,
  -- Trailing 6-month potential ROAS (closed + open estimates)
  CASE WHEN COALESCE(asp.trailing_6mo_spend, 0) > 0
    THEN ROUND((COALESCE(rh.trailing_6mo_rev, 0) + COALESCE(rj.trailing_6mo_rev, 0) + COALESCE(rg.trailing_6mo_rev, 0) + COALESCE(rh.trailing_6mo_open_est, 0) + COALESCE(rj.trailing_6mo_open_est, 0) + COALESCE(rg.trailing_6mo_open_est, 0)) / asp.trailing_6mo_spend, 3)
    ELSE 0
  END AS trailing_6mo_potential_roas,
  -- Trailing 3-month ROAS (for drilldown trend arrows)
  CASE WHEN COALESCE(asp.trailing_3mo_spend, 0) > 0
    THEN ROUND((COALESCE(rh.trailing_3mo_rev, 0) + COALESCE(rj.trailing_3mo_rev, 0) + COALESCE(rg.trailing_3mo_rev, 0)) / asp.trailing_3mo_spend, 3)
    ELSE 0
  END AS trailing_3mo_roas,
  -- Trailing 3-month potential ROAS
  CASE WHEN COALESCE(asp.trailing_3mo_spend, 0) > 0
    THEN ROUND((COALESCE(rh.trailing_3mo_rev, 0) + COALESCE(rj.trailing_3mo_rev, 0) + COALESCE(rg.trailing_3mo_rev, 0) + COALESCE(rh.trailing_3mo_open_est, 0) + COALESCE(rj.trailing_3mo_open_est, 0) + COALESCE(rg.trailing_3mo_open_est, 0)) / asp.trailing_3mo_spend, 3)
    ELSE 0
  END AS trailing_3mo_potential_roas,
  -- Prior CPL: prior spend / prior actual quality leads
  CASE WHEN COALESCE(lc.prior_actual_quality_leads, 0) > 0
    THEN ROUND(COALESCE(asp.prior_ad_spend, 0) / lc.prior_actual_quality_leads, 2)
    ELSE 0
  END AS prior_cpl,
  cb.risk_override

FROM client_base cb
LEFT JOIN lead_counts lc ON lc.customer_id = cb.customer_id
LEFT JOIN ad_spend asp ON asp.customer_id = cb.customer_id
LEFT JOIN lsa_metrics lsa ON lsa.customer_id = cb.customer_id
LEFT JOIN call_metrics cm ON cm.customer_id = cb.customer_id
LEFT JOIN ghl_metrics gm ON gm.customer_id = cb.customer_id
LEFT JOIN abandoned_rates abr ON abr.customer_id = cb.customer_id
LEFT JOIN revenue_hcp rh ON rh.customer_id = cb.customer_id
LEFT JOIN open_est_hcp oeh ON oeh.customer_id = cb.customer_id
LEFT JOIN revenue_jobber rj ON rj.customer_id = cb.customer_id
LEFT JOIN open_est_jobber oej ON oej.customer_id = cb.customer_id
LEFT JOIN on_cal_hcp och ON och.customer_id = cb.customer_id
LEFT JOIN on_cal_jobber ocj ON ocj.customer_id = cb.customer_id
LEFT JOIN revenue_ghl rg ON rg.customer_id = cb.customer_id
LEFT JOIN open_est_ghl oeg ON oeg.customer_id = cb.customer_id
LEFT JOIN on_cal_ghl ocg ON ocg.customer_id = cb.customer_id;

$$;


-- ============================================================
-- 1B. compute_risk_status(...)
-- PL/pgSQL function implementing Dashboard_Criteria_Log.md
-- ============================================================

CREATE OR REPLACE FUNCTION compute_risk_status(
  p_months_in_program  INT,
  p_quality_leads      INT,
  p_cpl                NUMERIC,
  p_lead_volume_change NUMERIC,
  p_days_since_lead    INT,
  p_insp_booked_pct    NUMERIC,
  p_roas               NUMERIC,
  p_guarantee          NUMERIC,
  p_spam_rate          NUMERIC,
  p_abandoned_rate     NUMERIC,
  p_budget             NUMERIC,
  p_ad_spend           NUMERIC,
  p_inspection_type    TEXT,
  p_on_cal_14d         INT,
  p_call_answer_rate   NUMERIC,
  p_field_mgmt         TEXT,
  p_prior_cpl          NUMERIC DEFAULT 0,
  p_trailing_6mo_roas  NUMERIC DEFAULT 0,
  p_trailing_6mo_potential_roas NUMERIC DEFAULT 0,
  p_trailing_3mo_roas  NUMERIC DEFAULT 0,
  p_trailing_3mo_potential_roas NUMERIC DEFAULT 0,
  p_current_confirmed_status TEXT DEFAULT NULL
)
RETURNS TABLE (
  status        TEXT,
  risk_type     TEXT,
  risk_triggers TEXT[],
  flag_triggers TEXT[],
  flag_count    INT,
  sort_priority INT
) LANGUAGE plpgsql STABLE AS $$
DECLARE
  v_ads_risks   TEXT[] := '{}';
  v_funnel_risks TEXT[] := '{}';
  v_flags       TEXT[] := '{}';
  v_has_ads     BOOLEAN;
  v_has_funnel  BOOLEAN;
  v_status      TEXT;
  v_risk_type   TEXT;
  v_sort        INT;
  v_program     TEXT;
BEGIN
  v_program := COALESCE(p_inspection_type, 'free');

  -- ============================================================
  -- MONTHS 0-2: Only $0 spend with budget set triggers risk
  -- ============================================================
  IF p_months_in_program <= 2 THEN
    IF p_budget > 0 AND (p_ad_spend IS NULL OR p_ad_spend = 0) THEN
      v_ads_risks := array_append(v_ads_risks,
        format('$0 spend with $%s budget (month %s)', ROUND(p_budget), p_months_in_program));
    END IF;

    -- (Budget over/underspend flags removed)

    -- Flag #1: Lead count
    IF p_months_in_program = 1 AND p_quality_leads = 0 THEN
      v_flags := array_append(v_flags, 'Zero leads in month 1');
    ELSIF p_months_in_program = 2 AND p_quality_leads <= 10 THEN
      v_flags := array_append(v_flags, format('%s leads in month 2 (<=10)', p_quality_leads));
    END IF;

    -- Flag #2: CPL
    IF p_months_in_program = 2 AND p_cpl > 400 THEN
      v_flags := array_append(v_flags, format('CPL $%s in month 2 (>$400)', ROUND(p_cpl)));
    END IF;

    -- Flag #4: Book rate
    IF p_months_in_program = 2 AND p_insp_booked_pct < 0.2 THEN
      v_flags := array_append(v_flags,
        format('Book rate %s%% in month 2 (<20%%)', ROUND(p_insp_booked_pct * 100)));
    END IF;

    -- Flag #12: Days since lead
    IF p_months_in_program = 2 AND p_days_since_lead IS NOT NULL AND p_days_since_lead >= 14 THEN
      v_flags := array_append(v_flags,
        format('%s days since last lead in month 2 (>=14)', p_days_since_lead));
    END IF;

    -- Spam/abandoned flags
    IF p_spam_rate > 0.11 THEN
      v_flags := array_append(v_flags, format('Spam rate %s%% (>11%%)', ROUND(p_spam_rate * 100)));
    END IF;
    IF p_abandoned_rate > 0.11 THEN
      v_flags := array_append(v_flags, format('Abandoned rate %s%% (>11%%)', ROUND(p_abandoned_rate * 100)));
    END IF;

    IF array_length(v_ads_risks, 1) > 0 THEN
      RETURN QUERY SELECT 'Risk'::TEXT, 'Ads Risk'::TEXT, v_ads_risks, v_flags,
        COALESCE(array_length(v_flags, 1), 0)::INT, 2;
      RETURN;
    END IF;

    IF COALESCE(array_length(v_flags, 1), 0) >= 3 THEN
      RETURN QUERY SELECT 'Flag'::TEXT, ''::TEXT, '{}'::TEXT[], v_flags,
        COALESCE(array_length(v_flags, 1), 0)::INT, 4;
    ELSE
      RETURN QUERY SELECT 'Healthy'::TEXT, ''::TEXT, '{}'::TEXT[], v_flags,
        COALESCE(array_length(v_flags, 1), 0)::INT, 5;
    END IF;
    RETURN;
  END IF;

  -- ============================================================
  -- MONTHS 3+: Full risk evaluation
  -- ============================================================

  -- ---- ADS RISK TRIGGERS ----

  IF p_budget > 0 AND (p_ad_spend IS NULL OR p_ad_spend = 0) THEN
    v_ads_risks := array_append(v_ads_risks,
      format('$0 spend with $%s budget', ROUND(p_budget)));
  END IF;

  -- Lead count: hysteresis — entry <=10, exit requires >=15 (months 3-5)
  IF p_months_in_program BETWEEN 3 AND 5
    AND p_quality_leads <= (CASE WHEN p_current_confirmed_status = 'Risk' THEN 14 ELSE 10 END) THEN
    v_ads_risks := array_append(v_ads_risks,
      format('%s leads (<=%s, months 3-5)', p_quality_leads,
        CASE WHEN p_current_confirmed_status = 'Risk' THEN 14 ELSE 10 END));
  END IF;

  -- Lead count: hysteresis — entry <20, exit requires >=25 (months 6+)
  IF p_months_in_program >= 6
    AND p_quality_leads < (CASE WHEN p_current_confirmed_status = 'Risk' THEN 25 ELSE 20 END)
    AND p_budget >= 3000 THEN
    v_ads_risks := array_append(v_ads_risks,
      format('%s leads (<%s, budget $%s)', p_quality_leads,
        CASE WHEN p_current_confirmed_status = 'Risk' THEN 25 ELSE 20 END, ROUND(p_budget)));
  END IF;

  -- High CPL: risk at >$170, exit requires <=$150 (hysteresis buffer)
  -- Downgraded to flag if ROAS > 3x (profitable despite high CPL)
  DECLARE
    v_cpl_thresh NUMERIC := CASE WHEN p_current_confirmed_status = 'Risk' THEN 150 ELSE 170 END;
  BEGIN
    IF p_cpl > v_cpl_thresh AND (p_roas <= 3 OR p_field_mgmt NOT IN ('housecall_pro', 'jobber', 'ghl')) THEN
      v_ads_risks := array_append(v_ads_risks, format('CPL $%s (>$%s)', ROUND(p_cpl), v_cpl_thresh));
    ELSIF p_cpl > v_cpl_thresh AND p_roas > 3 THEN
      v_flags := array_append(v_flags, format('CPL $%s (>$%s but ROAS %sx overrides)', ROUND(p_cpl), v_cpl_thresh, ROUND(p_roas, 2)));
    END IF;
  END;

  -- Lead volume drop: hysteresis — entry >30% drop, exit requires recovery to <=20% drop
  -- Risk only if CPL is also unhealthy (>$150). Healthy CPL → flag instead.
  DECLARE
    v_vol_thresh NUMERIC := CASE WHEN p_current_confirmed_status = 'Risk' THEN -0.2 ELSE -0.3 END;
  BEGIN
    IF p_lead_volume_change IS NOT NULL AND p_lead_volume_change < v_vol_thresh THEN
      IF p_cpl > 150 THEN
        v_ads_risks := array_append(v_ads_risks,
          format('Lead volume %s%% (>%s%% drop)', ROUND(p_lead_volume_change * 100), ROUND(ABS(v_vol_thresh) * 100)));
      ELSE
        v_flags := array_append(v_flags,
          format('Lead volume %s%% (>%s%% drop, CPL $%s healthy)', ROUND(p_lead_volume_change * 100), ROUND(ABS(v_vol_thresh) * 100), ROUND(p_cpl)));
      END IF;
    END IF;
  END;

  -- Days since lead: hysteresis — entry >=7 (months 4+), exit requires <=4
  -- Early program: entry >10, exit requires <=7
  IF p_days_since_lead IS NOT NULL THEN
    IF p_months_in_program >= 4
      AND p_days_since_lead >= (CASE WHEN p_current_confirmed_status = 'Risk' THEN 5 ELSE 7 END) THEN
      v_ads_risks := array_append(v_ads_risks, format('%s days since last lead (>=%s)',
        p_days_since_lead, CASE WHEN p_current_confirmed_status = 'Risk' THEN 5 ELSE 7 END));
    ELSIF p_months_in_program <= 3
      AND p_days_since_lead > (CASE WHEN p_current_confirmed_status = 'Risk' THEN 7 ELSE 10 END) THEN
      v_ads_risks := array_append(v_ads_risks, format('%s days since last lead (>%s, early program)',
        p_days_since_lead, CASE WHEN p_current_confirmed_status = 'Risk' THEN 7 ELSE 10 END));
    END IF;
  END IF;

  -- (Budget over/underspend risks removed — budget discrepancies are admin issues, not ad performance)

  -- ---- FUNNEL RISK TRIGGERS ----
  -- Three stories can save a client from funnel risk:
  --   Story 1 (Lifetime): all-time ROAS is strong
  --   Story 2 (Ramp-up): trailing 3-month ROAS shows momentum
  --   Story 3 (Potential): trailing 3-month potential ROAS (closed + open estimates) shows pipeline
  -- Funnel risk = no story works at their stage in the program

  -- No field management connected = can't evaluate funnel
  IF p_field_mgmt NOT IN ('housecall_pro', 'jobber', 'ghl') AND p_months_in_program >= 3 THEN
    v_funnel_risks := array_append(v_funnel_risks, 'No funnel data available — no field management connected');

  ELSIF p_field_mgmt IN ('housecall_pro', 'jobber', 'ghl') AND p_months_in_program >= 5 THEN

    -- Guarantee risk (non-negotiable, separate from presentation)
    IF p_months_in_program >= 7 AND p_guarantee < 0.5 THEN
      v_funnel_risks := array_append(v_funnel_risks,
        format('Guarantee %sx (<0.5x at month %s)', ROUND(p_guarantee, 2), p_months_in_program));
    END IF;

    -- Presentation risk: can we tell a good ROAS story?
    -- Check the three stories
    DECLARE
      v_lifetime_ok BOOLEAN;
      v_rampup_ok BOOLEAN;
      v_potential_ok BOOLEAN;
      v_lifetime_threshold NUMERIC;
      v_rampup_threshold NUMERIC;
    BEGIN
      -- Thresholds scale with program maturity
      -- Hysteresis: exit thresholds 0.5x lower than entry
      IF p_months_in_program BETWEEN 5 AND 6 THEN
        v_lifetime_threshold := CASE WHEN p_current_confirmed_status = 'Risk' THEN 1.0 ELSE 1.5 END;
        v_rampup_threshold := CASE WHEN p_current_confirmed_status = 'Risk' THEN 1.5 ELSE 2.0 END;
      ELSIF p_months_in_program BETWEEN 7 AND 9 THEN
        v_lifetime_threshold := CASE WHEN p_current_confirmed_status = 'Risk' THEN 1.5 ELSE 2.0 END;
        v_rampup_threshold := CASE WHEN p_current_confirmed_status = 'Risk' THEN 2.0 ELSE 2.5 END;
      ELSE -- months 10-12+
        v_lifetime_threshold := CASE WHEN p_current_confirmed_status = 'Risk' THEN 2.0 ELSE 2.5 END;
        v_rampup_threshold := CASE WHEN p_current_confirmed_status = 'Risk' THEN 2.5 ELSE 3.0 END;
      END IF;

      v_lifetime_ok := p_guarantee >= v_lifetime_threshold;
      -- Ramp-up: 6-month is primary, 3-month also works
      v_rampup_ok := p_trailing_6mo_roas >= v_rampup_threshold
                  OR p_trailing_3mo_roas >= v_rampup_threshold;
      -- Potential: 6-month or 3-month including open estimates
      v_potential_ok := p_trailing_6mo_potential_roas >= v_rampup_threshold
                     OR p_trailing_3mo_potential_roas >= v_rampup_threshold;

      -- Funnel risk only if NO story works
      IF NOT v_lifetime_ok AND NOT v_rampup_ok AND NOT v_potential_ok THEN
        v_funnel_risks := array_append(v_funnel_risks,
          format('Presentation risk: guarantee %sx (need %sx), trailing 6mo ROAS %sx, 3mo ROAS %sx (need %sx), potential 6mo %sx',
            ROUND(p_guarantee, 2), v_lifetime_threshold,
            ROUND(p_trailing_6mo_roas, 1), ROUND(p_trailing_3mo_roas, 1), v_rampup_threshold,
            ROUND(p_trailing_6mo_potential_roas, 1)));
      END IF;
    END;

  END IF;

  -- ---- FLAG TRIGGERS ----

  -- ---- FLAGS (all metrics — affects main dashboard flag status) ----

  -- #1: Lead count
  IF p_months_in_program BETWEEN 3 AND 5 AND p_quality_leads BETWEEN 11 AND 19 THEN
    v_flags := array_append(v_flags, format('%s leads (11-19, months 3-5)', p_quality_leads));
  ELSIF p_months_in_program >= 6 AND p_quality_leads BETWEEN 20 AND 29 THEN
    v_flags := array_append(v_flags, format('%s leads (20-29)', p_quality_leads));
  ELSIF p_months_in_program >= 6 AND p_quality_leads < 20 AND p_budget < 3000 THEN
    v_flags := array_append(v_flags,
      format('%s leads (<20, smaller account)', p_quality_leads));
  END IF;

  -- #2: CPL
  IF p_cpl > 140 AND p_cpl <= 170 THEN
    v_flags := array_append(v_flags, format('CPL $%s (140-170 range)', ROUND(p_cpl)));
  ELSIF p_cpl > 0 AND p_cpl < 40 THEN
    v_flags := array_append(v_flags, format('CPL $%s (<$40, suspiciously low)', ROUND(p_cpl)));
  END IF;

  -- #3: Contact volume
  IF p_lead_volume_change IS NOT NULL AND p_lead_volume_change >= -0.3 AND p_lead_volume_change < -0.1 THEN
    v_flags := array_append(v_flags,
      format('Lead volume %s%% (moderate decrease)', ROUND(p_lead_volume_change * 100)));
  END IF;

  -- #4: Book rate
  IF p_field_mgmt IN ('housecall_pro', 'jobber') THEN
    IF v_program = 'free' AND p_insp_booked_pct >= 0.15 AND p_insp_booked_pct < 0.28 THEN
      v_flags := array_append(v_flags,
        format('Book rate %s%% (15-28%%, Free)', ROUND(p_insp_booked_pct * 100)));
    ELSIF v_program = 'paid' AND p_insp_booked_pct >= 0.11 AND p_insp_booked_pct <= 0.19 THEN
      v_flags := array_append(v_flags,
        format('Book rate %s%% (11-19%%, Paid)', ROUND(p_insp_booked_pct * 100)));
    ELSIF v_program = 'free' AND p_insp_booked_pct < 0.15 AND p_guarantee > 3 THEN
      v_flags := array_append(v_flags,
        format('Book rate %s%% (<15%% but guarantee %sx saves)', ROUND(p_insp_booked_pct * 100), ROUND(p_guarantee, 1)));
    ELSIF v_program = 'paid' AND p_insp_booked_pct <= 0.1 AND p_guarantee > 3 THEN
      v_flags := array_append(v_flags,
        format('Book rate %s%% (<=10%% but guarantee %sx saves)', ROUND(p_insp_booked_pct * 100), ROUND(p_guarantee, 1)));
    END IF;
  END IF;

  -- #5: Guarantee
  IF p_months_in_program BETWEEN 5 AND 6 AND p_guarantee < 0.5 THEN
    v_flags := array_append(v_flags,
      format('Guarantee %sx at month %s (<0.5x)', ROUND(p_guarantee, 2), p_months_in_program));
  END IF;

  -- #6: ROAS
  IF p_field_mgmt IN ('housecall_pro', 'jobber', 'ghl') AND p_roas >= 0.6 AND p_roas < 1.0 THEN
    v_flags := array_append(v_flags, format('ROAS %s%% (60-100%%)', ROUND(p_roas * 100)));
  END IF;

  -- #7: Spam
  IF p_spam_rate > 0.11 THEN
    v_flags := array_append(v_flags, format('Spam rate %s%% (>11%%)', ROUND(p_spam_rate * 100)));
  END IF;

  -- #8: Abandoned
  IF p_abandoned_rate > 0.11 THEN
    v_flags := array_append(v_flags, format('Abandoned rate %s%% (>11%%)', ROUND(p_abandoned_rate * 100)));
  END IF;

  -- #12: Days since lead (flag range)
  IF p_days_since_lead IS NOT NULL THEN
    IF p_months_in_program >= 4 AND p_days_since_lead BETWEEN 4 AND 6 THEN
      v_flags := array_append(v_flags, format('%s days since last lead (4-6)', p_days_since_lead));
    ELSIF p_months_in_program <= 3 AND p_days_since_lead BETWEEN 7 AND 10 THEN
      v_flags := array_append(v_flags, format('%s days since last lead (7-10, early program)', p_days_since_lead));
    END IF;
  END IF;

  -- #13: On-calendar (0 or 1 GA inspections = flag)
  IF p_on_cal_14d = 0 AND p_months_in_program >= 3 THEN
    v_flags := array_append(v_flags, '0 GA inspections on calendar in next 14 days');
  ELSIF p_on_cal_14d = 1 AND p_months_in_program >= 3 THEN
    v_flags := array_append(v_flags, 'Only 1 GA inspection on calendar in next 14 days');
  END IF;

  -- ============================================================
  -- Determine final status
  -- ============================================================
  v_has_ads := COALESCE(array_length(v_ads_risks, 1), 0) > 0;
  v_has_funnel := COALESCE(array_length(v_funnel_risks, 1), 0) > 0;

  IF v_has_ads AND v_has_funnel THEN
    v_status := 'Risk'; v_risk_type := 'Both Risk'; v_sort := 1;
  ELSIF v_has_ads THEN
    v_status := 'Risk'; v_risk_type := 'Ads Risk'; v_sort := 2;
  ELSIF v_has_funnel THEN
    v_status := 'Risk'; v_risk_type := 'Funnel Risk'; v_sort := 3;
  ELSIF COALESCE(array_length(v_flags, 1), 0) >= 3 THEN
    v_status := 'Flag'; v_risk_type := ''; v_sort := 4;
  ELSE
    v_status := 'Healthy'; v_risk_type := ''; v_sort := 5;
  END IF;

  -- Floor: CPL >$170 or 3+ flags always forces at least Flag status
  IF v_status = 'Healthy' AND (p_cpl > 170 OR COALESCE(array_length(v_flags, 1), 0) >= 3) THEN
    v_status := 'Flag'; v_risk_type := ''; v_sort := 4;
  END IF;

  RETURN QUERY SELECT v_status, v_risk_type,
    v_ads_risks || v_funnel_risks, v_flags,
    COALESCE(array_length(v_flags, 1), 0)::INT, v_sort;
END;
$$;


-- ============================================================
-- Sticky Status table (must exist before get_dashboard_with_risk references it)
-- ============================================================
CREATE TABLE IF NOT EXISTS client_confirmed_status (
  customer_id         BIGINT PRIMARY KEY REFERENCES clients(customer_id),
  confirmed_status    TEXT NOT NULL DEFAULT 'Healthy',
  confirmed_risk_type TEXT DEFAULT '',
  pending_status      TEXT,
  pending_streak      INT DEFAULT 0,
  last_computed       TEXT,
  confirmed_at        DATE,
  updated_at          TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- 1C. get_dashboard_with_risk(p_start, p_end)
-- ============================================================

CREATE OR REPLACE FUNCTION get_dashboard_with_risk(
  p_start DATE DEFAULT CURRENT_DATE - INTERVAL '30 days',
  p_end   DATE DEFAULT CURRENT_DATE
)
RETURNS TABLE (
  customer_id        BIGINT,
  client_name        TEXT,
  ads_manager        TEXT,
  inspection_type    TEXT,
  budget             NUMERIC,
  months_in_program  INT,
  start_date         DATE,
  field_mgmt         TEXT,
  quality_leads      INT,
  prior_quality_leads INT,
  actual_quality_leads INT,
  prior_actual_quality_leads INT,
  spam_contacts      INT,
  lead_volume_change NUMERIC,
  ad_spend           NUMERIC,
  all_time_spend     NUMERIC,
  cpl                NUMERIC,
  total_calls        INT,
  spam_rate          NUMERIC,
  abandoned_rate     NUMERIC,
  days_since_lead    INT,
  call_answer_rate   NUMERIC,
  total_insp_booked  INT,
  insp_booked_pct    NUMERIC,
  total_closed_rev   NUMERIC,
  total_open_est_rev NUMERIC,
  approved_no_inv    NUMERIC,
  all_time_rev       NUMERIC,
  roas               NUMERIC,
  period_potential_roas NUMERIC,
  guarantee          NUMERIC,
  trailing_6mo_roas  NUMERIC,
  trailing_6mo_potential_roas NUMERIC,
  trailing_3mo_roas  NUMERIC,
  trailing_3mo_potential_roas NUMERIC,
  on_cal_14d         INT,
  on_cal_total       INT,
  lsa_spend          NUMERIC,
  lsa_leads          INT,
  prior_cpl          NUMERIC,
  status             TEXT,
  risk_type          TEXT,
  risk_triggers      TEXT[],
  flag_triggers      TEXT[],
  flag_count         INT,
  sort_priority      INT
) LANGUAGE SQL STABLE AS $$

SELECT
  m.customer_id, m.client_name, m.ads_manager, m.inspection_type,
  m.budget, m.months_in_program, m.start_date, m.field_mgmt,
  m.quality_leads, m.prior_quality_leads,
  m.actual_quality_leads, m.prior_actual_quality_leads, m.spam_contacts,
  m.lead_volume_change,
  m.ad_spend, m.all_time_spend, m.cpl,
  m.total_calls, m.spam_rate, m.abandoned_rate, m.days_since_lead,
  m.call_answer_rate,
  m.total_insp_booked, m.insp_booked_pct,
  m.total_closed_rev, m.total_open_est_rev, m.approved_no_inv, m.all_time_rev,
  m.roas, m.period_potential_roas, m.guarantee, m.trailing_6mo_roas, m.trailing_6mo_potential_roas,
  m.trailing_3mo_roas, m.trailing_3mo_potential_roas,
  m.on_cal_14d, m.on_cal_total, m.lsa_spend, m.lsa_leads, m.prior_cpl,
  -- Apply risk_override: if set, force that status
  CASE WHEN m.risk_override IS NOT NULL THEN INITCAP(m.risk_override) ELSE r.status END AS status,
  CASE WHEN m.risk_override IS NOT NULL THEN '' ELSE r.risk_type END AS risk_type,
  CASE WHEN m.risk_override IS NOT NULL THEN '{}' ELSE r.risk_triggers END AS risk_triggers,
  CASE WHEN m.risk_override IS NOT NULL THEN '{}' ELSE r.flag_triggers END AS flag_triggers,
  CASE WHEN m.risk_override IS NOT NULL THEN 0 ELSE r.flag_count END AS flag_count,
  CASE WHEN m.risk_override = 'healthy' THEN 5
       WHEN m.risk_override = 'flag' THEN 4
       WHEN m.risk_override = 'risk' THEN 1
       ELSE r.sort_priority END AS sort_priority
FROM get_dashboard_metrics(p_start, p_end) m
LEFT JOIN client_confirmed_status ccs ON ccs.customer_id = m.customer_id
CROSS JOIN LATERAL compute_risk_status(
  m.months_in_program, m.actual_quality_leads, m.cpl, m.lead_volume_change,
  m.days_since_lead, m.insp_booked_pct, m.roas, m.guarantee,
  m.spam_rate, m.abandoned_rate, m.budget, m.ad_spend,
  m.inspection_type, m.on_cal_14d, m.call_answer_rate, m.field_mgmt,
  m.prior_cpl, m.trailing_6mo_roas, m.trailing_6mo_potential_roas,
  m.trailing_3mo_roas, m.trailing_3mo_potential_roas,
  ccs.confirmed_status
) r
ORDER BY
  CASE WHEN m.risk_override = 'healthy' THEN 5
       WHEN m.risk_override = 'flag' THEN 4
       WHEN m.risk_override = 'risk' THEN 1
       ELSE r.sort_priority END,
  m.client_name;

$$;

-- ============================================================
-- 1D. get_dashboard_with_risk_raw(p_start, p_end)
-- "Ads View" — same as get_dashboard_with_risk but WITHOUT CRM spam/abandoned adjustments
-- Uses quality_leads (all GA contacts) instead of actual_quality_leads
-- Used for the "CRM Spam Adjusted" toggle on the trend chart
-- ============================================================

CREATE OR REPLACE FUNCTION get_dashboard_with_risk_raw(
  p_start DATE DEFAULT CURRENT_DATE - INTERVAL '30 days',
  p_end   DATE DEFAULT CURRENT_DATE
)
RETURNS TABLE (
  customer_id        BIGINT,
  client_name        TEXT,
  status             TEXT,
  risk_type          TEXT,
  risk_triggers      TEXT[],
  flag_triggers      TEXT[],
  flag_count         INT
) LANGUAGE plpgsql STABLE AS $$
DECLARE
  rec RECORD;
  risk_result RECORD;
BEGIN
  FOR rec IN SELECT * FROM get_dashboard_metrics(p_start, p_end) LOOP
    -- Override: use quality_leads (contacts) as the lead count instead of actual_quality_leads
    -- Recompute CPL and other derived metrics without GHL spam
    SELECT INTO risk_result *
    FROM compute_risk_status(
      rec.months_in_program,
      rec.quality_leads,           -- all GA contacts, not actual_quality_leads
      CASE WHEN rec.quality_leads > 0 THEN ROUND(COALESCE(rec.ad_spend, 0) / rec.quality_leads, 2) ELSE 0 END,  -- CPL on raw contacts
      CASE WHEN rec.prior_quality_leads > 0
        THEN ROUND((rec.quality_leads - rec.prior_quality_leads)::numeric / rec.prior_quality_leads, 3)
        ELSE NULL END,             -- lead volume change on raw contacts
      rec.days_since_lead,
      CASE WHEN rec.quality_leads > 0 THEN ROUND(rec.total_insp_booked::numeric / rec.quality_leads, 3) ELSE 0 END,  -- book rate on raw contacts
      rec.roas,
      rec.guarantee,
      0,                           -- spam_rate = 0 (no CRM spam in raw view)
      0,                           -- abandoned_rate = 0 (no CRM abandoned in raw view)
      rec.budget,
      rec.ad_spend,
      rec.inspection_type,
      rec.on_cal_14d,
      rec.call_answer_rate,
      rec.field_mgmt,
      rec.prior_cpl,
      rec.trailing_6mo_roas,
      rec.trailing_6mo_potential_roas,
      rec.trailing_3mo_roas,
      rec.trailing_3mo_potential_roas
    );

    -- Apply risk_override
    customer_id := rec.customer_id;
    client_name := rec.client_name;
    IF rec.risk_override IS NOT NULL THEN
      status := INITCAP(rec.risk_override);
      risk_type := '';
      risk_triggers := '{}';
      flag_triggers := '{}';
      flag_count := 0;
    ELSE
      status := risk_result.status;
      risk_type := risk_result.risk_type;
      risk_triggers := risk_result.risk_triggers;
      flag_triggers := risk_result.flag_triggers;
      flag_count := risk_result.flag_count;
    END IF;

    RETURN NEXT;
  END LOOP;
END;
$$;


-- ============================================================
-- 2. Sticky Status: Anti-Flapping System
-- ============================================================

-- Add audit columns to snapshots
ALTER TABLE risk_status_snapshots
  ADD COLUMN IF NOT EXISTS confirmed_status TEXT,
  ADD COLUMN IF NOT EXISTS pending_status TEXT;

-- Backfill: seed confirmed status from latest snapshot for each client
INSERT INTO client_confirmed_status (customer_id, confirmed_status, confirmed_risk_type, last_computed, confirmed_at)
SELECT DISTINCT ON (customer_id)
  customer_id, status, COALESCE(risk_type, ''), status, snapshot_date
FROM risk_status_snapshots
ORDER BY customer_id, snapshot_date DESC
ON CONFLICT (customer_id) DO NOTHING;


-- ============================================================
-- 2A. update_confirmed_statuses()
-- Called daily after snapshot. Implements 3-day confirmation period.
-- ============================================================

CREATE OR REPLACE FUNCTION update_confirmed_statuses()
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  rec RECORD;
  v_computed_status TEXT;
  v_computed_risk_type TEXT;
  v_confirmation_days INT;
  v_current_confirmed TEXT;
BEGIN
  FOR rec IN SELECT * FROM get_dashboard_with_risk() LOOP
    v_computed_status := rec.status;
    v_computed_risk_type := COALESCE(rec.risk_type, '');

    -- Asymmetric confirmation: worsening is fast, improving is slow
    -- Look up current confirmed status for this client
    SELECT confirmed_status INTO v_current_confirmed
    FROM client_confirmed_status WHERE customer_id = rec.customer_id;

    v_confirmation_days := CASE
      -- Into Risk: instant (1 day) — never miss a risk signal
      WHEN v_computed_status = 'Risk' THEN 1
      -- Into Flag from Healthy: 2 days — slight buffer for flags
      WHEN v_computed_status = 'Flag' AND COALESCE(v_current_confirmed, 'Healthy') = 'Healthy' THEN 2
      -- Risk → Flag (partial improvement): 3 days
      WHEN v_computed_status = 'Flag' AND v_current_confirmed = 'Risk' THEN 3
      -- Flag → Healthy: 3 days
      WHEN v_computed_status = 'Healthy' AND v_current_confirmed = 'Flag' THEN 3
      -- Risk → Healthy: 5 days — must really prove recovery
      WHEN v_computed_status = 'Healthy' AND v_current_confirmed = 'Risk' THEN 5
      -- Default
      ELSE 3
    END;

    -- Skip clients with manual risk_override (override always wins)
    IF EXISTS (SELECT 1 FROM clients c WHERE c.customer_id = rec.customer_id AND c.risk_override IS NOT NULL) THEN
      INSERT INTO client_confirmed_status (customer_id, confirmed_status, confirmed_risk_type, last_computed, confirmed_at, updated_at)
      VALUES (rec.customer_id, v_computed_status, v_computed_risk_type, v_computed_status, CURRENT_DATE, NOW())
      ON CONFLICT (customer_id) DO UPDATE SET
        confirmed_status = v_computed_status,
        confirmed_risk_type = v_computed_risk_type,
        last_computed = v_computed_status,
        pending_status = NULL,
        pending_streak = 0,
        updated_at = NOW();
      CONTINUE;
    END IF;

    INSERT INTO client_confirmed_status (customer_id, confirmed_status, confirmed_risk_type, last_computed, confirmed_at, updated_at)
    VALUES (rec.customer_id, v_computed_status, v_computed_risk_type, v_computed_status, CURRENT_DATE, NOW())
    ON CONFLICT (customer_id) DO UPDATE SET
      last_computed = v_computed_status,

      -- Pending streak logic
      pending_status = CASE
        -- Computed matches confirmed: no change pending
        WHEN v_computed_status = client_confirmed_status.confirmed_status THEN NULL
        -- Computed matches existing pending: keep pending (streak incremented below)
        WHEN v_computed_status = client_confirmed_status.pending_status THEN client_confirmed_status.pending_status
        -- New direction: start fresh pending
        ELSE v_computed_status
      END,

      pending_streak = CASE
        WHEN v_computed_status = client_confirmed_status.confirmed_status THEN 0
        WHEN v_computed_status = client_confirmed_status.pending_status
          THEN client_confirmed_status.pending_streak + 1
        ELSE 1
      END,

      -- Promote to confirmed when streak reaches threshold
      confirmed_status = CASE
        WHEN v_computed_status = client_confirmed_status.pending_status
          AND client_confirmed_status.pending_streak + 1 >= v_confirmation_days
          THEN v_computed_status
        ELSE client_confirmed_status.confirmed_status
      END,

      confirmed_risk_type = CASE
        WHEN v_computed_status = client_confirmed_status.pending_status
          AND client_confirmed_status.pending_streak + 1 >= v_confirmation_days
          THEN v_computed_risk_type
        ELSE client_confirmed_status.confirmed_risk_type
      END,

      confirmed_at = CASE
        WHEN v_computed_status = client_confirmed_status.pending_status
          AND client_confirmed_status.pending_streak + 1 >= v_confirmation_days
          THEN CURRENT_DATE
        ELSE client_confirmed_status.confirmed_at
      END,

      updated_at = NOW();

    -- After promotion, clear pending fields
    UPDATE client_confirmed_status
    SET pending_status = NULL, pending_streak = 0
    WHERE customer_id = rec.customer_id
      AND confirmed_status = pending_status;
  END LOOP;
END;
$$;


-- ============================================================
-- 3. Location Groups: Per-location campaign breakdown
-- ============================================================

CREATE TABLE IF NOT EXISTS client_location_groups (
    id              SERIAL PRIMARY KEY,
    customer_id     BIGINT NOT NULL REFERENCES clients(customer_id),
    location_name   TEXT NOT NULL,
    campaign_ids    BIGINT[] NOT NULL,
    UNIQUE(customer_id, location_name)
);
CREATE INDEX IF NOT EXISTS idx_clg_customer ON client_location_groups(customer_id);

-- Seed: Nez Iskandrani (9159518133)
INSERT INTO client_location_groups (customer_id, location_name, campaign_ids) VALUES
  (9159518133, 'Kansas', ARRAY[22519823667, 22196078601, 22813387593, 21843386860, 21785289528, 22206386584]),
  (9159518133, 'Wichita', ARRAY[23582949346])
ON CONFLICT (customer_id, location_name) DO UPDATE SET campaign_ids = EXCLUDED.campaign_ids;

-- Seed: Aaron Meadows (9699974772)
INSERT INTO client_location_groups (customer_id, location_name, campaign_ids) VALUES
  (9699974772, 'NorCal', ARRAY[22713487088, 22886232871, 23217100260, 21587855410]),
  (9699974772, 'Marin County', ARRAY[22933413981]),
  (9699974772, 'Santa Clara County', ARRAY[23628998285])
ON CONFLICT (customer_id, location_name) DO UPDATE SET campaign_ids = EXCLUDED.campaign_ids;
