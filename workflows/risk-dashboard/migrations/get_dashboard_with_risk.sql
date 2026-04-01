CREATE OR REPLACE FUNCTION public.get_dashboard_with_risk(p_start date DEFAULT (CURRENT_DATE - '30 days'::interval), p_end date DEFAULT CURRENT_DATE)
 RETURNS TABLE(customer_id bigint, client_name text, ads_manager text, inspection_type text, budget numeric, months_in_program integer, start_date date, field_mgmt text, quality_leads integer, prior_quality_leads integer, lead_volume_change numeric, ad_spend numeric, all_time_spend numeric, cpl numeric, total_calls integer, spam_rate numeric, abandoned_rate numeric, days_since_lead integer, call_answer_rate numeric, total_insp_booked integer, insp_booked_pct numeric, total_closed_rev numeric, total_open_est_rev numeric, approved_no_inv numeric, all_time_rev numeric, roas numeric, guarantee numeric, on_cal_14d integer, on_cal_total integer, lsa_spend numeric, lsa_leads integer, status text, risk_type text, risk_triggers text[], flag_triggers text[], flag_count integer, sort_priority integer)
 LANGUAGE sql
 STABLE
AS $function$

SELECT
  m.customer_id, m.client_name, m.ads_manager, m.inspection_type,
  m.budget, m.months_in_program, m.start_date, m.field_mgmt,
  m.quality_leads, m.prior_quality_leads, m.lead_volume_change,
  m.ad_spend, m.all_time_spend, m.cpl,
  m.total_calls, m.spam_rate, m.abandoned_rate, m.days_since_lead,
  m.call_answer_rate,
  m.total_insp_booked, m.insp_booked_pct,
  m.total_closed_rev, m.total_open_est_rev, m.approved_no_inv, m.all_time_rev,
  m.roas, m.guarantee,
  m.on_cal_14d, m.on_cal_total, m.lsa_spend, m.lsa_leads,
  r.status, r.risk_type, r.risk_triggers, r.flag_triggers, r.flag_count, r.sort_priority
FROM get_dashboard_metrics(p_start, p_end) m
CROSS JOIN LATERAL compute_risk_status(
  m.months_in_program, m.quality_leads, m.cpl, m.lead_volume_change,
  m.days_since_lead, m.insp_booked_pct, m.roas, m.guarantee,
  m.spam_rate, m.abandoned_rate, m.budget, m.ad_spend,
  m.inspection_type, m.on_cal_14d, m.call_answer_rate, m.field_mgmt
) r
ORDER BY r.sort_priority, m.client_name;

$function$

