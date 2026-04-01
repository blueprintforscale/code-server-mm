-- Client Data Health Check View
-- Flags clients with potential data issues

CREATE OR REPLACE VIEW v_client_data_health AS
WITH client_calls AS (
  SELECT
    c.customer_id,
    c.name,
    c.status,
    c.start_date,
    c.callrail_company_id,
    c.field_management_software,
    c.hcp_api_key,
    COUNT(ca.id) AS total_calls,
    COUNT(ca.id) FILTER (WHERE ca.start_time > CURRENT_DATE - 7) AS calls_last_7d,
    COUNT(ca.id) FILTER (WHERE ca.start_time > CURRENT_DATE - 30) AS calls_last_30d,
    MAX(ca.start_time)::date AS last_call_date,
    COUNT(ca.id) FILTER (WHERE (ca.source IN ('Google Ads', 'Google Ads 2') OR (ca.gclid IS NOT NULL AND ca.gclid != ''))) AS ga_legit_calls
  FROM clients c
  LEFT JOIN calls ca ON ca.customer_id = c.customer_id
  WHERE c.status = 'active'
    AND c.start_date IS NOT NULL
  GROUP BY c.customer_id, c.name, c.status, c.start_date, c.callrail_company_id,
           c.field_management_software, c.hcp_api_key
),
orphaned AS (
  SELECT
    callrail_company_id,
    COUNT(*) AS orphan_count,
    MAX(start_time)::date AS latest_orphan
  FROM calls
  WHERE customer_id IS NULL
  GROUP BY callrail_company_id
),
hcp_match AS (
  SELECT
    customer_id,
    COUNT(*) AS total_hcp_customers,
    COUNT(callrail_id) AS matched_customers
  FROM hcp_customers
  GROUP BY customer_id
)
SELECT
  cc.customer_id,
  cc.name,
  cc.start_date,
  cc.callrail_company_id,
  cc.field_management_software,
  cc.total_calls,
  cc.calls_last_7d,
  cc.calls_last_30d,
  cc.last_call_date,
  cc.ga_legit_calls,
  COALESCE(hm.total_hcp_customers, 0) AS hcp_customers,
  COALESCE(hm.matched_customers, 0) AS hcp_matched,
  COALESCE(orph.orphan_count, 0) AS orphaned_calls,
  orph.latest_orphan AS orphan_latest_date,
  -- Health flags
  ARRAY_REMOVE(ARRAY[
    CASE WHEN cc.total_calls = 0 AND cc.start_date < CURRENT_DATE - 30
      THEN 'NO_CALLS: Zero calls ever — check callrail_company_id' END,
    CASE WHEN cc.calls_last_7d = 0 AND cc.total_calls > 0 AND cc.start_date < CURRENT_DATE - 30
      THEN 'NO_RECENT_CALLS: No calls in 7 days — may be stale or paused' END,
    CASE WHEN cc.calls_last_30d = 0 AND cc.total_calls > 0 AND cc.start_date < CURRENT_DATE - 60
      THEN 'STALE: No calls in 30 days — check if client is still active' END,
    CASE WHEN COALESCE(orph.orphan_count, 0) > 10
      THEN format('ORPHANED_CALLS: %s calls with NULL customer_id for company %s',
        orph.orphan_count, cc.callrail_company_id) END,
    CASE WHEN cc.field_management_software IN ('housecall_pro') AND COALESCE(hm.total_hcp_customers, 0) = 0
      THEN 'NO_HCP_DATA: Has HCP configured but zero customers synced' END,
    CASE WHEN cc.field_management_software IN ('housecall_pro')
      AND COALESCE(hm.total_hcp_customers, 0) > 0
      AND COALESCE(hm.matched_customers, 0) = 0
      AND cc.total_calls > 0
      THEN 'NO_HCP_MATCH: HCP customers exist + calls exist but zero phone matches' END,
    CASE WHEN cc.field_management_software IN ('housecall_pro')
      AND COALESCE(hm.total_hcp_customers, 0) > 50
      AND COALESCE(hm.matched_customers, 0)::numeric / hm.total_hcp_customers < 0.05
      AND cc.total_calls > 50
      THEN format('LOW_HCP_MATCH: Only %s/%s HCP customers matched (<5%%)',
        hm.matched_customers, hm.total_hcp_customers) END,
    CASE WHEN cc.ga_legit_calls = 0 AND cc.total_calls > 20
      THEN 'NO_GA_LEADS: Calls exist but zero classified as google_ads + legitimate' END
  ], NULL) AS health_flags,
  -- Overall status
  CASE
    WHEN cc.total_calls = 0 AND cc.start_date < CURRENT_DATE - 30 THEN 'CRITICAL'
    WHEN cc.calls_last_30d = 0 AND cc.start_date < CURRENT_DATE - 60 THEN 'WARNING'
    WHEN COALESCE(orph.orphan_count, 0) > 10 THEN 'WARNING'
    WHEN cc.field_management_software IN ('housecall_pro')
      AND COALESCE(hm.matched_customers, 0) = 0
      AND cc.total_calls > 0 THEN 'WARNING'
    ELSE 'OK'
  END AS health_status
FROM client_calls cc
LEFT JOIN orphaned orph ON orph.callrail_company_id = cc.callrail_company_id
LEFT JOIN hcp_match hm ON hm.customer_id = cc.customer_id
ORDER BY
  CASE
    WHEN cc.total_calls = 0 THEN 1
    WHEN cc.calls_last_30d = 0 THEN 2
    ELSE 3
  END,
  cc.name;
