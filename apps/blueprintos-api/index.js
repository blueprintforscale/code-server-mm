require('dotenv').config();
const fastify = require('fastify')({ logger: true });
const cors = require('@fastify/cors');
const { Pool } = require('pg');
const bcrypt = require('bcrypt');

const pool = new Pool({
  user: 'blueprint',
  database: 'blueprint',
  host: 'localhost',
  port: 5432,
});

const ALLOWED_ORIGINS = [
  'https://blueprintos.vercel.app',
  /\.vercel\.app$/,
  'http://localhost:3000',
];

const API_KEY = process.env.BLUEPRINTOS_API_KEY;
const SALT_ROUNDS = 12;

fastify.register(cors, {
  origin: ALLOWED_ORIGINS,
  methods: ['GET', 'POST', 'PUT', 'OPTIONS'],
});

// API key auth
fastify.addHook('onRequest', async (request, reply) => {
  if (request.url === '/health') return;
  // Allow GHL iframe embed tokens without API key
  if (request.url.startsWith('/embed/')) return;

  const key = request.headers['x-api-key'];
  if (!API_KEY || key !== API_KEY) {
    reply.code(401).send({ error: 'Unauthorized' });
  }
});

// ============================================================
// Health
// ============================================================
fastify.get('/health', async () => ({ status: 'ok' }));

// ============================================================
// Clients
// ============================================================

// List all active clients
fastify.get('/clients', async () => {
  const { rows } = await pool.query(`
    SELECT customer_id, name, status, ads_manager, budget, start_date,
           field_management_software, inspection_type,
           EXTRACT(MONTH FROM age(CURRENT_DATE, start_date))::int + EXTRACT(YEAR FROM age(CURRENT_DATE, start_date))::int * 12 AS months_in_program,
           parent_customer_id, dashboard_token, dashboard_config
    FROM clients
    WHERE status = 'active' AND parent_customer_id IS NULL
    ORDER BY name
  `);
  return rows;
});

// Client detail
fastify.get('/clients/:customerId', async (request) => {
  const { customerId } = request.params;
  const { rows } = await pool.query(
    `SELECT customer_id, name, status, ads_manager, budget, start_date,
            field_management_software, inspection_type,
            EXTRACT(MONTH FROM age(CURRENT_DATE, start_date))::int + EXTRACT(YEAR FROM age(CURRENT_DATE, start_date))::int * 12 AS months_in_program,
            parent_customer_id, ghl_location_id, risk_override,
            callrail_company_id, dashboard_config
     FROM clients WHERE customer_id = $1`,
    [customerId]
  );
  if (rows.length === 0) return { error: 'Not found' };
  return rows[0];
});

// Client's child accounts
fastify.get('/clients/:customerId/children', async (request) => {
  const { customerId } = request.params;
  const { rows } = await pool.query(
    `SELECT customer_id, name FROM clients WHERE parent_customer_id = $1`,
    [customerId]
  );
  return rows;
});

// ============================================================
// Analytics — Dashboard Metrics (risk dashboard data)
// ============================================================

// Full dashboard metrics for one client
fastify.get('/clients/:customerId/metrics', async (request) => {
  const { customerId } = request.params;
  const { days = 30 } = request.query;
  const endDate = new Date().toISOString().split('T')[0];
  const startDate = new Date(Date.now() - days * 86400000).toISOString().split('T')[0];

  const { rows } = await pool.query(
    `SELECT * FROM get_dashboard_metrics($1::date, $2::date) WHERE customer_id = $3`,
    [startDate, endDate, customerId]
  );
  if (rows.length === 0) return { error: 'No data' };

  const r = rows[0];
  // Compute derived metrics
  r.cpl = r.quality_leads > 0 ? +(r.ad_spend / r.quality_leads).toFixed(2) : 0;
  r.roas = r.ad_spend > 0 ? +(r.total_closed_rev / r.ad_spend).toFixed(2) : 0;
  r.insp_booked_pct = r.quality_leads > 0 ? +(r.total_insp_booked / r.quality_leads).toFixed(4) : 0;
  r.call_answer_rate = r.biz_hour_calls > 0 ? +(r.biz_hour_answered / r.biz_hour_calls).toFixed(4) : 0;
  r.guarantee = r.all_time_spend > 0 ? +(r.all_time_rev / r.all_time_spend).toFixed(2) : 0;
  return r;
});

// Full dashboard with risk status
fastify.get('/clients/:customerId/risk', async (request) => {
  const { customerId } = request.params;
  const { days = 30 } = request.query;
  const endDate = new Date().toISOString().split('T')[0];
  const startDate = new Date(Date.now() - days * 86400000).toISOString().split('T')[0];

  const { rows } = await pool.query(
    `SELECT * FROM get_dashboard_with_risk($1::date, $2::date) WHERE customer_id = $3`,
    [startDate, endDate, customerId]
  );
  if (rows.length === 0) return { error: 'No data' };
  return rows[0];
});

// All clients risk overview
fastify.get('/dashboard/risk', async (request) => {
  const { days = 30 } = request.query;
  const endDate = new Date().toISOString().split('T')[0];
  const startDate = new Date(Date.now() - days * 86400000).toISOString().split('T')[0];

  const { rows } = await pool.query(
    `SELECT * FROM get_dashboard_with_risk($1::date, $2::date) ORDER BY sort_priority, client_name`,
    [startDate, endDate]
  );
  return rows;
});

// ============================================================
// Analytics — Funnel Data (for client-facing dashboard)
// ============================================================

// Funnel counts by source
fastify.get('/clients/:customerId/funnel', async (request) => {
  const { customerId } = request.params;
  const { source = 'all', date_from, date_to } = request.query;

  // Get client info
  const clientResult = await pool.query(
    `SELECT field_management_software, dashboard_config, spreadsheet_id, extra_spam_keywords FROM clients WHERE customer_id = $1`,
    [customerId]
  );
  if (clientResult.rows.length === 0) return { error: 'Client not found' };
  const client = clientResult.rows[0];

  // Build client_ids CTE for parent+child
  const cidCTE = `WITH client_ids AS (
    SELECT customer_id FROM clients WHERE customer_id = $1 OR parent_customer_id = $1
  )`;

  // Date params
  const params = [customerId];
  let paramIdx = 2;
  let dateWhere = '';
  if (date_from) {
    dateWhere += ` AND lead_date >= $${paramIdx}::date`;
    params.push(date_from);
    paramIdx++;
  }
  if (date_to) {
    dateWhere += ` AND lead_date <= $${paramIdx}::date`;
    params.push(date_to);
    paramIdx++;
  }

  // Source filter
  let sourceWhere = '';
  if (source === 'google_ads') {
    sourceWhere = `AND is_google_ads_call(c.source, c.source_name, c.gclid)`;
  } else if (source === 'gbp') {
    sourceWhere = `AND c.source = 'Google My Business' AND NOT is_google_ads_call(c.source, c.source_name, c.gclid)`;
  } else if (source === 'lsa') {
    sourceWhere = `AND c.source_name = 'LSA'`;
  }

  // LSA source: use lsa_leads table directly for lead counts + HCP for funnel progress
  let result;
  if (source === 'lsa') {
    result = await getLsaFunnel(pool, customerId, params, dateWhere, cidCTE);
  } else if (client.spreadsheet_id) {
    result = await getSpreadsheetFunnel(pool, customerId, params, dateWhere, sourceWhere, cidCTE);
  } else if (client.field_management_software === 'housecall_pro') {
    result = await getHcpFunnel(pool, customerId, params, dateWhere, sourceWhere, cidCTE, client.extra_spam_keywords);
  } else if (client.field_management_software === 'jobber') {
    result = await getJobberFunnel(pool, customerId, params, dateWhere, sourceWhere, cidCTE);
  } else if (client.field_management_software === 'ghl') {
    result = await getGhlFunnel(pool, customerId, params, dateWhere, sourceWhere, cidCTE);
  } else {
    return { error: 'Unknown field management software' };
  }
  if (result.error) return result;

  // ====================================================================
  // CRITICAL: DO NOT RE-ENABLE THIS BLOCK. (Disabled 2026-04-08)
  // getHcpFunnel is now the single source of truth for lead counts.
  // get_dashboard_metrics() does NOT handle: bot detection, repeat caller
  // filtering, attribution overrides, or reactivation protocol.
  // Re-enabling this will cause funnel/drawer count mismatches.
  // See: RULES.md Section 21 (bot detection), Section 22 (reactivation)
  // ====================================================================
  if (false && source === 'google_ads' && date_from && date_to) {
    const { rows: metricsRows } = await pool.query(
      `SELECT quality_leads, actual_quality_leads, ad_spend, cpl, total_closed_rev, total_open_est_rev, roas, all_time_rev, all_time_spend, guarantee, total_insp_booked FROM get_dashboard_metrics($1::date, $2::date) WHERE customer_id = $3`,
      [date_from, date_to, customerId]
    );
    if (metricsRows.length > 0) {
      const m = metricsRows[0];
      const qualityLeads = parseInt(m.actual_quality_leads) || 0;
      const allContacts = parseInt(m.quality_leads) || 0;  // pre-spam-filter count
      // Override lead counts with risk dashboard's CallRail-deduped counts
      result.leads = String(qualityLeads);
      result.quality_leads = String(qualityLeads);
      result.total_contacts = String(Math.max(allContacts, qualityLeads));
      result.spam_count = String(Math.max(allContacts - qualityLeads, 0));
      // Override financial metrics
      result.ad_spend = String(parseFloat(m.ad_spend) || 0);
      result.cpl = qualityLeads > 0
        ? (parseFloat(m.ad_spend) / qualityLeads).toFixed(2)
        : '0';
      result.closed_rev = String(parseFloat(m.total_closed_rev) || 0);
      result.open_est_rev = String(parseFloat(m.total_open_est_rev) || 0);
      result.all_time_rev = String(parseFloat(m.all_time_rev) || 0);
      result.all_time_spend = String(parseFloat(m.all_time_spend) || 0);
      // Override inspection count
      result.inspection_scheduled = String(parseInt(m.total_insp_booked) || 0);
      // Override guarantee (all_time_rev / program_price)
      result.guarantee = String(parseFloat(m.guarantee) || 0);
    }
  }

  return result;
});

// HCP funnel helper — matches portal dashboard logic exactly
// Lead count = HCP-matched leads + unmatched calls + unmatched forms (minus GHL spam)
// Spreadsheet funnel helper for BlueprintOS API
// Returns same shape as getHcpFunnel so the frontend works unchanged

async function getSpreadsheetFunnel(pool, customerId, params, dateWhere, sourceWhere, cidCTE) {
  // Map source filter to spreadsheet source column
  let slSourceWhere = '';
  if (sourceWhere.includes('is_google_ads_call')) {
    slSourceWhere = `AND sl.source ILIKE '%google ads%'`;
  } else if (sourceWhere.includes('Google My Business')) {
    slSourceWhere = `AND (sl.source ILIKE '%google my business%' OR sl.source ILIKE '%gbp%')`;
  }

  // Map date filter
  const slDateWhere = dateWhere.replace(/lead_date/g, 'sl.date_created');

  const { rows } = await pool.query(`
    ${cidCTE},
    -- Spreadsheet leads (primary source)
    sheet_counts AS (
      SELECT
        COUNT(*) FILTER (WHERE sl.is_quality_lead) as matched_leads,
        COUNT(*) as total_contacts,
        COUNT(*) FILTER (WHERE NOT sl.is_quality_lead) as spam_count,
        COUNT(*) FILTER (WHERE sl.inspection_scheduled AND sl.is_quality_lead) as inspection_scheduled,
        COUNT(*) FILTER (WHERE sl.inspection_completed AND sl.is_quality_lead) as inspection_completed,
        COUNT(*) FILTER (WHERE sl.estimate_sent AND sl.is_quality_lead) as estimate_sent,
        COUNT(*) FILTER (WHERE sl.estimate_approved AND sl.is_quality_lead) as estimate_approved,
        COUNT(*) FILTER (WHERE sl.job_scheduled AND sl.is_quality_lead) as job_scheduled,
        COUNT(*) FILTER (WHERE sl.job_completed AND sl.is_quality_lead) as job_completed,
        0 as revenue_closed,
        COUNT(*) FILTER (WHERE sl.estimate_sent AND NOT sl.estimate_approved AND sl.is_quality_lead) as open_estimate_count,
        COALESCE(SUM(sl.estimate_sent_cents) FILTER (WHERE sl.estimate_sent AND sl.is_quality_lead), 0) / 100.0 as estimate_sent_value,
        COALESCE(SUM(sl.estimate_approved_cents) FILTER (WHERE sl.estimate_approved AND sl.is_quality_lead), 0) / 100.0 as estimate_approved_value,
        COALESCE(SUM(sl.job_scheduled_cents) FILTER (WHERE sl.job_scheduled AND sl.is_quality_lead), 0) / 100.0 as job_value,
        COALESCE(SUM(sl.roas_revenue_cents) FILTER (WHERE sl.is_quality_lead), 0) / 100.0 as closed_rev,
        COALESCE(SUM(sl.estimate_open_cents) FILTER (WHERE sl.estimate_sent AND NOT sl.estimate_approved AND sl.is_quality_lead), 0) / 100.0 as open_est_rev
      FROM spreadsheet_leads sl
      WHERE sl.customer_id = $1 ${slDateWhere} ${slSourceWhere}
    ),
    -- All-time revenue from spreadsheet (for guarantee - always Google Ads, no date filter)
    all_time_rev AS (
      SELECT COALESCE(SUM(sl.roas_revenue_cents), 0) / 100.0 as total
      FROM spreadsheet_leads sl
      WHERE sl.customer_id = $1 AND sl.is_quality_lead AND sl.source ILIKE '%google ads%'
    ),
    -- All-time ad spend (exclude LSA)
    all_time_spend AS (
      SELECT COALESCE(SUM(cost), 0) as total
      FROM campaign_daily_metrics
      WHERE customer_id IN (SELECT customer_id FROM client_ids)
        AND campaign_type != 'LOCAL_SERVICES'
    ),
    -- Period ad spend (exclude LSA)
    period_spend AS (
      SELECT COALESCE(SUM(cost), 0) as ad_spend
      FROM campaign_daily_metrics adm
      WHERE adm.customer_id IN (SELECT customer_id FROM client_ids)
        AND adm.campaign_type != 'LOCAL_SERVICES'
        ${dateWhere.replace(/lead_date/g, 'adm.date')}
    ),
    -- Program fee
    program_fee AS (
      SELECT COALESCE(SUM(program_price), 0) as total
      FROM clients WHERE customer_id IN (SELECT customer_id FROM client_ids)
    ),
    -- Unmatched CallRail leads
    unmatched_count AS (
      SELECT COUNT(DISTINCT normalize_phone(c.caller_phone)) as count
      FROM calls c
      WHERE c.customer_id IN (SELECT customer_id FROM client_ids)
        ${dateWhere.replace(/lead_date/g, 'c.start_time::date')}
        ${sourceWhere}
        AND normalize_phone(c.caller_phone) IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM spreadsheet_leads sl WHERE sl.customer_id = $1 AND sl.phone_normalized = normalize_phone(c.caller_phone))
        AND NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.customer_id = $1 AND hc.phone_normalized = normalize_phone(c.caller_phone))
    )
    SELECT
      sc.matched_leads + (SELECT count FROM unmatched_count) as leads,
      sc.total_contacts + (SELECT count FROM unmatched_count) as total_contacts,
      sc.matched_leads + (SELECT count FROM unmatched_count) as quality_leads,
      sc.spam_count,
      sc.inspection_scheduled,
      sc.inspection_completed,
      sc.estimate_sent,
      sc.estimate_approved,
      sc.job_scheduled,
      sc.job_completed,
      sc.revenue_closed,
      sc.open_estimate_count,
      sc.estimate_sent_value,
      sc.estimate_approved_value,
      sc.job_value,
      (SELECT ad_spend FROM period_spend) as ad_spend,
      sc.closed_rev,
      sc.open_est_rev,
      (SELECT total FROM all_time_spend) as all_time_spend,
      (SELECT total FROM all_time_rev) as all_time_rev,
      (SELECT total FROM program_fee) as program_price
    FROM sheet_counts sc
  `, params);

  const result = rows[0] || {};

  // Add projected close total
  const { rows: projRows } = await pool.query(
    `SELECT COALESCE(SUM(projected_revenue_cents), 0) / 100.0 as total FROM projected_closes WHERE customer_id = $1`,
    [customerId]
  );
  result.projected_close_total = parseFloat(projRows[0]?.total) || 0;

  return result;
}

// LSA funnel: uses lsa_leads table for lead counts, HCP data for funnel progress
async function getLsaFunnel(pool, customerId, params, dateWhere, cidCTE) {
  const lsaDateWhere = dateWhere.replace(/lead_date/g, 'l.lead_creation_time::date');
  const hcpDateWhere = dateWhere.replace(/lead_date/g, 'hc.hcp_created_at::date');

  const { rows } = await pool.query(`
    ${cidCTE},
    -- All LSA leads for the period
    lsa_all AS (
      SELECT l.id, l.contact_phone_normalized as phone, l.lead_type, l.lead_charged,
        l.hcp_customer_id, l.lead_creation_time
      FROM lsa_leads l
      WHERE l.customer_id = $1
        ${lsaDateWhere}
    ),
    -- LSA leads with HCP match (for funnel progress)
    lsa_matched AS (
      SELECT DISTINCT ON (la.phone) la.phone, la.hcp_customer_id,
        pg.all_ids
      FROM lsa_all la
      JOIN hcp_customers hc ON hc.hcp_customer_id = la.hcp_customer_id
      JOIN (SELECT customer_id, phone_normalized, array_agg(hcp_customer_id) as all_ids
            FROM hcp_customers WHERE customer_id = $1 GROUP BY customer_id, phone_normalized) pg
        ON pg.phone_normalized = hc.phone_normalized AND pg.customer_id = hc.customer_id
      WHERE la.hcp_customer_id IS NOT NULL AND la.phone IS NOT NULL
      ORDER BY la.phone, la.lead_creation_time ASC
    ),
    -- Funnel counts from HCP data for matched LSA leads
    funnel AS (
      SELECT
        COUNT(*) as matched_leads,
        COUNT(*) FILTER (WHERE EXISTS (
          SELECT 1 FROM hcp_inspections i WHERE i.hcp_customer_id = ANY(lm.all_ids)
            AND i.record_status = 'active'
            AND (i.status IN ('scheduled','complete rated','complete unrated','in progress') OR i.scheduled_at IS NOT NULL OR i.inferred_complete = true)
        )) as inspection_scheduled,
        COUNT(*) FILTER (WHERE EXISTS (
          SELECT 1 FROM hcp_inspections i WHERE i.hcp_customer_id = ANY(lm.all_ids)
            AND i.record_status = 'active'
            AND (i.status IN ('complete rated','complete unrated') OR i.inferred_complete = true)
        )) as inspection_completed,
        COUNT(*) FILTER (WHERE EXISTS (
          SELECT 1 FROM v_estimate_groups eg WHERE eg.hcp_customer_id = ANY(lm.all_ids)
            AND eg.status IN ('sent','approved','declined') AND eg.count_revenue AND eg.estimate_type = 'treatment'
        )) as estimate_sent,
        COUNT(*) FILTER (WHERE EXISTS (
          SELECT 1 FROM v_estimate_groups eg WHERE eg.hcp_customer_id = ANY(lm.all_ids)
            AND eg.status = 'approved' AND eg.count_revenue AND eg.estimate_type = 'treatment' AND eg.approved_total_cents >= 100000
        )) as estimate_approved,
        COUNT(*) FILTER (WHERE EXISTS (
          SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = ANY(lm.all_ids)
            AND j.record_status = 'active' AND j.status IN ('scheduled','complete rated','complete unrated','in progress') AND j.total_amount_cents >= 100000
        )) as job_scheduled,
        COUNT(*) FILTER (WHERE EXISTS (
          SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = ANY(lm.all_ids)
            AND j.record_status = 'active' AND j.status IN ('complete rated','complete unrated') AND j.total_amount_cents >= 100000
        )) as job_completed,
        COUNT(*) FILTER (WHERE EXISTS (
          SELECT 1 FROM hcp_invoices inv WHERE inv.hcp_customer_id = ANY(lm.all_ids)
            AND inv.status NOT IN ('canceled','voided') AND inv.amount_cents > 0
        ) OR EXISTS (
          SELECT 1 FROM v_estimate_groups eg WHERE eg.hcp_customer_id = ANY(lm.all_ids)
            AND eg.status = 'approved' AND eg.count_revenue AND eg.estimate_type = 'treatment' AND eg.approved_total_cents >= 100000
        )) as revenue_closed,
        COALESCE(SUM(
          COALESCE((SELECT SUM(inv.amount_cents) FROM hcp_invoices inv WHERE inv.hcp_customer_id = ANY(lm.all_ids)
            AND inv.status NOT IN ('canceled','voided') AND inv.invoice_type = 'treatment'), 0)
          + COALESCE((SELECT SUM(inv.amount_cents) FROM hcp_invoices inv WHERE inv.hcp_customer_id = ANY(lm.all_ids)
            AND inv.status NOT IN ('canceled','voided') AND inv.invoice_type = 'inspection'), 0)
        ), 0) / 100.0 as closed_rev,
        COALESCE(SUM(
          COALESCE((SELECT SUM(eg.highest_option_cents) FROM v_estimate_groups eg WHERE eg.hcp_customer_id = ANY(lm.all_ids)
            AND eg.status IN ('sent','approved','declined') AND eg.count_revenue AND eg.estimate_type = 'treatment'), 0)
        ), 0) / 100.0 as estimate_sent_value,
        COALESCE(SUM(
          COALESCE((SELECT SUM(eg.approved_total_cents) FROM v_estimate_groups eg WHERE eg.hcp_customer_id = ANY(lm.all_ids)
            AND eg.status = 'approved' AND eg.count_revenue AND eg.estimate_type = 'treatment' AND eg.approved_total_cents >= 100000), 0)
        ), 0) / 100.0 as estimate_approved_value
      FROM lsa_matched lm
    ),
    -- LSA ad spend
    lsa_spend AS (
      SELECT COALESCE(SUM(cost), 0) as ad_spend
      FROM campaign_daily_metrics adm
      WHERE adm.customer_id IN (SELECT customer_id FROM client_ids)
        AND adm.campaign_type = 'LOCAL_SERVICES'
        ${dateWhere.replace(/lead_date/g, 'adm.date')}
    )
    SELECT
      (SELECT COUNT(DISTINCT COALESCE(phone, 'nophone_' || id::text)) FROM lsa_all) as quality_leads,
      (SELECT COUNT(DISTINCT COALESCE(phone, 'nophone_' || id::text)) FROM lsa_all) as total_contacts,
      (SELECT COUNT(DISTINCT COALESCE(phone, 'nophone_' || id::text)) FROM lsa_all) as leads,
      0 as spam_count,
      f.matched_leads,
      f.inspection_scheduled,
      f.inspection_completed,
      f.estimate_sent,
      f.estimate_approved,
      f.job_scheduled,
      f.job_completed,
      f.revenue_closed,
      0 as open_estimate_count,
      f.estimate_sent_value,
      f.estimate_approved_value,
      0 as job_value,
      (SELECT ad_spend FROM lsa_spend) as ad_spend,
      f.closed_rev,
      0 as open_est_rev,
      0 as all_time_spend,
      0 as all_time_rev,
      0 as program_price,
      0 as months_in_program,
      0 as projected_close_total
    FROM funnel f
  `, params);

  return rows[0] || {};
}

// Fast HCP funnel using mv_funnel_leads materialized view
// Quality lead logic aligned with risk dashboard: 20% abandoned rule, CRM activity rescue, unmatched spam filtering
async function getHcpFunnel(pool, customerId, params, dateWhere, sourceWhere, cidCTE, extraSpamKeywords = null) {
  // Map source filter to mv_funnel_leads.lead_source
  // NOTE: GBP check must come before GA because GBP filter also contains 'is_google_ads_call' (in NOT)
  let mvSourceWhere = '';
  if (sourceWhere.includes('Google My Business')) {
    mvSourceWhere = `AND fl.lead_source = 'gbp'`;
  } else if (sourceWhere.includes("source_name = 'LSA'")) {
    mvSourceWhere = `AND fl.lead_source = 'lsa'`;
  } else if (sourceWhere.includes('is_google_ads_call')) {
    mvSourceWhere = `AND fl.lead_source = 'google_ads'`;
  }

  const mvDateWhere = dateWhere.replace(/lead_date/g, 'fl.hcp_created_at::date');
  const crDateWhere = dateWhere.replace(/lead_date/g, 'c2.start_time::date');
  const fmDateWhere = dateWhere.replace(/lead_date/g, 'f2.submitted_at::date');

  // Source filters for unmatched calls/forms
  // NOTE: GBP check must come before GA because GBP filter also contains 'is_google_ads_call' (in NOT)
  let crSourceWhere = '';
  let fmSourceWhere = '';
  if (sourceWhere.includes('Google My Business')) {
    crSourceWhere = `AND c2.source = 'Google My Business' AND NOT is_google_ads_call(c2.source, c2.source_name, c2.gclid)`;
    fmSourceWhere = `AND f2.source = 'Google My Business' AND f2.gclid IS NULL`;
  } else if (sourceWhere.includes("source_name = 'LSA'")) {
    crSourceWhere = `AND c2.source_name = 'LSA'`;
    fmSourceWhere = `AND 1=0`; // LSA doesn't have form submissions
  } else if (sourceWhere.includes('is_google_ads_call')) {
    crSourceWhere = `AND is_google_ads_call(c2.source, c2.source_name, c2.gclid)`;
    fmSourceWhere = `AND (f2.gclid IS NOT NULL OR f2.source = 'Google Ads')`;
  }

  // Should abandoned always be excluded? (extra_spam_keywords includes 'abandoned')
  const abandonedAlwaysExclude = extraSpamKeywords && extraSpamKeywords.includes('abandoned');

  // Ad spend campaign type filter: LSA shows LSA spend, GBP shows $0 (organic), others exclude LSA
  const spendTypeWhere = sourceWhere.includes("source_name = 'LSA'")
    ? `AND adm.campaign_type = 'LOCAL_SERVICES'`
    : sourceWhere.includes("Google My Business")
    ? `AND 1=0`
    : `AND adm.campaign_type != 'LOCAL_SERVICES'`;

  // CRM activity rescue expression (reused in multiple places)
  const crmRescue = `(fl.has_inspection_scheduled OR fl.has_estimate_sent OR fl.has_job_scheduled OR fl.has_invoice)`;

  const { rows } = await pool.query(`
    ${cidCTE},
    -- Period-scoped abandoned rate (for 20% threshold rule)
    abandoned_rate AS (
      SELECT
        CASE WHEN COUNT(DISTINCT fl.phone_normalized) > 0
          THEN COUNT(DISTINCT fl.phone_normalized) FILTER (WHERE fl.ghl_abandoned)::numeric
               / COUNT(DISTINCT fl.phone_normalized)
          ELSE 0
        END AS rate
      FROM mv_funnel_leads fl
      WHERE fl.customer_id = $1
        ${mvSourceWhere}
        ${mvDateWhere}
    ),
    -- Main funnel from materialized view
    matched AS (
      SELECT fl.*
      FROM mv_funnel_leads fl
      WHERE fl.customer_id = $1
        ${mvSourceWhere}
        ${mvDateWhere}
        -- Core spam exclusion (with CRM activity rescue)
        AND NOT (fl.ghl_spam AND NOT ${crmRescue})
        -- Abandoned-as-spam: only when rate > 20% AND no CRM activity
        AND NOT (fl.ghl_abandoned AND NOT ${crmRescue}
                 AND ${abandonedAlwaysExclude ? 'true' : '(SELECT rate FROM abandoned_rate) > 0.20'})
        AND COALESCE(fl.client_flag_reason, '') NOT IN ('spam', 'out_of_area', 'wrong_service')
    ),
    matched_agg AS (
      SELECT
        COUNT(*) as matched_leads,
        COUNT(*) FILTER (WHERE has_inspection_scheduled) as inspection_scheduled,
        COUNT(*) FILTER (WHERE has_inspection_completed) as inspection_completed,
        COUNT(*) FILTER (WHERE has_estimate_sent) as estimate_sent,
        COUNT(*) FILTER (WHERE has_estimate_approved) as estimate_approved,
        COUNT(*) FILTER (WHERE has_job_scheduled) as job_scheduled,
        COUNT(*) FILTER (WHERE has_job_completed) as job_completed,
        COUNT(*) FILTER (WHERE has_invoice) as revenue_closed,
        COUNT(*) FILTER (WHERE has_estimate_sent AND NOT has_estimate_approved AND NOT has_invoice) as open_estimate_count,
        COALESCE(SUM(est_sent_cents) FILTER (WHERE has_estimate_sent), 0) / 100.0 as estimate_sent_value,
        COALESCE(SUM(est_approved_cents) FILTER (WHERE has_estimate_approved), 0) / 100.0 as estimate_approved_value,
        COALESCE(SUM(job_cents) FILTER (WHERE has_job_scheduled), 0) / 100.0 as job_value,
        COALESCE(SUM(
          CASE WHEN NOT COALESCE(exclude_from_ga_roas, false) THEN
            CASE
              WHEN treat_invoice_cents > 0 OR est_approved_cents > 0
                THEN insp_invoice_cents + GREATEST(treat_invoice_cents, est_approved_cents)
              ELSE job_cents + insp_invoice_cents
            END
          ELSE 0 END
        ), 0) / 100.0 as closed_rev,
        COALESCE(SUM(CASE WHEN has_estimate_sent AND NOT has_estimate_approved AND NOT has_invoice
            AND NOT COALESCE(exclude_from_ga_roas, false)
          THEN est_sent_cents ELSE 0 END), 0) / 100.0 as open_est_rev
      FROM matched
    ),
    -- GHL spam phones for unmatched lead filtering (core spam only, no abandoned)
    ghl_spam_phones AS (
      SELECT DISTINCT gc.phone_normalized as phone
      FROM ghl_contacts gc
      WHERE gc.customer_id IN (SELECT customer_id FROM client_ids)
        AND (
          lower(gc.lost_reason) SIMILAR TO '%(spam|not a lead|wrong number|out of area|wrong service)%'
          OR EXISTS (SELECT 1 FROM ghl_opportunities o
            WHERE o.ghl_contact_id = gc.ghl_contact_id
              AND lower(o.stage_name) SIMILAR TO '%(spam|not a lead|out of area|wrong service)%')
        )
    ),
    -- GHL abandoned phones for unmatched lead filtering (only used when rate > 20%)
    ghl_abandoned_phones AS (
      SELECT DISTINCT gc.phone_normalized as phone
      FROM ghl_contacts gc
      WHERE gc.customer_id IN (SELECT customer_id FROM client_ids)
        AND ${abandonedAlwaysExclude ? 'true' : '(SELECT rate FROM abandoned_rate) > 0.20'}
        AND (
          lower(COALESCE(gc.lost_reason, '')) LIKE '%abandoned%'
          OR EXISTS (SELECT 1 FROM ghl_opportunities o
            WHERE o.ghl_contact_id = gc.ghl_contact_id AND o.status = 'abandoned')
        )
    ),
    -- Unmatched calls (with spam filtering)
    unmatched_calls AS (
      SELECT DISTINCT normalize_phone(c2.caller_phone) as phone
      FROM calls c2
      WHERE c2.customer_id IN (SELECT customer_id FROM client_ids)
        ${crDateWhere} ${crSourceWhere}
        AND normalize_phone(c2.caller_phone) IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM mv_funnel_leads fl WHERE fl.customer_id = $1 AND fl.phone_normalized = normalize_phone(c2.caller_phone))
        -- Exclude core spam phones
        AND NOT EXISTS (SELECT 1 FROM ghl_spam_phones sp WHERE sp.phone = normalize_phone(c2.caller_phone))
        -- Exclude abandoned-as-spam phones (only populated when rate > 20%)
        AND NOT EXISTS (SELECT 1 FROM ghl_abandoned_phones ap WHERE ap.phone = normalize_phone(c2.caller_phone))
        -- 60-day repeat caller filter: exclude returning callers who aren't reactivated
        AND (
          c2.first_call = true
          OR NOT EXISTS (SELECT 1 FROM calls c_prior
            WHERE c_prior.customer_id = c2.customer_id
              AND normalize_phone(c_prior.caller_phone) = normalize_phone(c2.caller_phone)
              AND c_prior.start_time < c2.start_time)
          OR (
            -- Has prior call: apply 60-day combo rule
            -- Keep if: gap >= 60 days AND no prior treatment
            EXTRACT(EPOCH FROM (c2.start_time - (
              SELECT MAX(c_prior2.start_time) FROM calls c_prior2
              WHERE c_prior2.customer_id = c2.customer_id
                AND normalize_phone(c_prior2.caller_phone) = normalize_phone(c2.caller_phone)
                AND c_prior2.start_time < c2.start_time
            ))) / 86400 >= 60
            AND NOT EXISTS (
              SELECT 1 FROM mv_funnel_leads fl2
              WHERE fl2.customer_id = c2.customer_id
                AND fl2.phone_normalized = normalize_phone(c2.caller_phone)
                AND (fl2.has_job_completed OR fl2.has_invoice)
            )
          )
        )
    ),
    unmatched_forms AS (
      SELECT DISTINCT normalize_phone(f2.customer_phone) as phone
      FROM form_submissions f2
      WHERE f2.customer_id IN (SELECT customer_id FROM client_ids)
        ${fmDateWhere} ${fmSourceWhere}
        AND normalize_phone(f2.customer_phone) IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM mv_funnel_leads fl WHERE fl.customer_id = $1 AND fl.phone_normalized = normalize_phone(f2.customer_phone))
        AND NOT EXISTS (SELECT 1 FROM unmatched_calls uc WHERE uc.phone = normalize_phone(f2.customer_phone))
        -- Exclude core spam phones
        AND NOT EXISTS (SELECT 1 FROM ghl_spam_phones sp WHERE sp.phone = normalize_phone(f2.customer_phone))
        -- Exclude abandoned-as-spam phones (only populated when rate > 20%)
        AND NOT EXISTS (SELECT 1 FROM ghl_abandoned_phones ap WHERE ap.phone = normalize_phone(f2.customer_phone))
        -- Exclude bot form spam: Direct source shortcut OR vowel ratio check
        AND NOT (
          f2.customer_name ~ '^[A-Z]{8,}\\s+[A-Z]{8,}$'
          AND (
            f2.source = 'Direct'
            OR LENGTH(REGEXP_REPLACE(UPPER(f2.customer_name), '[^AEIOU]', '', 'g'))::float
                / NULLIF(LENGTH(REGEXP_REPLACE(f2.customer_name, '\\s', '', 'g')), 0) < 0.25
          )
        )
        -- 60-day repeat form filter
        AND (
          NOT EXISTS (SELECT 1 FROM calls c_prior
            WHERE c_prior.customer_id IN (SELECT customer_id FROM client_ids)
              AND normalize_phone(c_prior.caller_phone) = normalize_phone(f2.customer_phone)
              AND c_prior.start_time < f2.submitted_at)
          OR (
            EXTRACT(EPOCH FROM (f2.submitted_at - (
              SELECT MAX(c_prior2.start_time) FROM calls c_prior2
              WHERE c_prior2.customer_id IN (SELECT customer_id FROM client_ids)
                AND normalize_phone(c_prior2.caller_phone) = normalize_phone(f2.customer_phone)
                AND c_prior2.start_time < f2.submitted_at
            ))) / 86400 >= 60
            AND NOT EXISTS (
              SELECT 1 FROM mv_funnel_leads fl2
              WHERE fl2.customer_id IN (SELECT customer_id FROM client_ids)
                AND fl2.phone_normalized = normalize_phone(f2.customer_phone)
                AND (fl2.has_job_completed OR fl2.has_invoice)
            )
          )
        )
    ),
    unmatched_count AS (
      SELECT (SELECT COUNT(*) FROM unmatched_calls) + (SELECT COUNT(*) FROM unmatched_forms) as count
    ),
    -- Unmatched leads excluded by spam/abandoned phone lists (for contacts total)
    unmatched_excluded AS (
      SELECT COUNT(DISTINCT phone) as count FROM (
        SELECT DISTINCT normalize_phone(c3.caller_phone) as phone
        FROM calls c3
        WHERE c3.customer_id IN (SELECT customer_id FROM client_ids)
          ${crDateWhere.replace(/c2\./g, 'c3.')} ${crSourceWhere.replace(/c2\./g, 'c3.')}
          AND normalize_phone(c3.caller_phone) IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM mv_funnel_leads fl WHERE fl.customer_id = $1 AND fl.phone_normalized = normalize_phone(c3.caller_phone))
          AND (EXISTS (SELECT 1 FROM ghl_spam_phones sp WHERE sp.phone = normalize_phone(c3.caller_phone))
               OR EXISTS (SELECT 1 FROM ghl_abandoned_phones ap WHERE ap.phone = normalize_phone(c3.caller_phone)))
        UNION
        SELECT DISTINCT normalize_phone(f3.customer_phone)
        FROM form_submissions f3
        WHERE f3.customer_id IN (SELECT customer_id FROM client_ids)
          ${fmDateWhere.replace(/f2\./g, 'f3.')} ${fmSourceWhere.replace(/f2\./g, 'f3.')}
          AND normalize_phone(f3.customer_phone) IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM mv_funnel_leads fl WHERE fl.customer_id = $1 AND fl.phone_normalized = normalize_phone(f3.customer_phone))
          AND NOT EXISTS (SELECT 1 FROM unmatched_calls uc WHERE uc.phone = normalize_phone(f3.customer_phone))
          AND (EXISTS (SELECT 1 FROM ghl_spam_phones sp WHERE sp.phone = normalize_phone(f3.customer_phone))
               OR EXISTS (SELECT 1 FROM ghl_abandoned_phones ap WHERE ap.phone = normalize_phone(f3.customer_phone)))
      ) excl
    ),
    -- Spam excluded (for contacts count) — mirrors matched CTE logic
    spam_excluded AS (
      SELECT COUNT(*) as total
      FROM mv_funnel_leads fl
      WHERE fl.customer_id = $1 ${mvSourceWhere} ${mvDateWhere}
        AND (
          COALESCE(fl.client_flag_reason,'') IN ('spam','out_of_area','wrong_service')
          OR (fl.ghl_spam AND NOT ${crmRescue})
          OR (fl.ghl_abandoned AND NOT ${crmRescue}
              AND ${abandonedAlwaysExclude ? 'true' : '(SELECT rate FROM abandoned_rate) > 0.20'})
        )
    ),
    -- Ad spend (filtered by source: LSA tab shows LSA spend, others exclude LSA)
    period_spend AS (
      SELECT COALESCE(SUM(cost), 0) as ad_spend
      FROM campaign_daily_metrics adm
      WHERE adm.customer_id IN (SELECT customer_id FROM client_ids)
        ${spendTypeWhere}
        ${dateWhere.replace(/lead_date/g, 'adm.date')}
    ),
    -- Guarantee end date (start_date + 12 months)
    guarantee_period AS (
      SELECT start_date, start_date + INTERVAL '12 months' AS end_date,
        EXTRACT(MONTH FROM age(CURRENT_DATE, start_date))::int + EXTRACT(YEAR FROM age(CURRENT_DATE, start_date))::int * 12 AS months_in
      FROM clients WHERE customer_id = $1
    ),
    -- Revenue for guarantee: capped at first 12 months from start_date
    all_time_rev AS (
      SELECT COALESCE(SUM(
        CASE WHEN NOT COALESCE(fl.exclude_from_ga_roas, false) THEN
          CASE
            WHEN fl.treat_invoice_cents > 0 OR fl.est_approved_cents > 0
              THEN fl.insp_invoice_cents + GREATEST(fl.treat_invoice_cents, fl.est_approved_cents)
            ELSE fl.job_cents + fl.insp_invoice_cents
          END
        ELSE 0 END
      ), 0) / 100.0 as total
      FROM mv_funnel_leads fl
      WHERE fl.customer_id = $1 AND fl.lead_source = 'google_ads'
        AND NOT (fl.ghl_spam AND NOT ${crmRescue})
        AND COALESCE(fl.client_flag_reason,'') NOT IN ('spam','out_of_area','wrong_service')
        AND fl.has_invoice
        AND fl.hcp_created_at < (SELECT end_date FROM guarantee_period)
    ),
    -- Ad spend for guarantee: capped at first 12 months from start_date (exclude LSA)
    all_time_spend AS (
      SELECT COALESCE(SUM(cost), 0) as total
      FROM campaign_daily_metrics
      WHERE customer_id IN (SELECT customer_id FROM client_ids)
        AND campaign_type != 'LOCAL_SERVICES'
        AND date < (SELECT end_date FROM guarantee_period)::date
    ),
    program_fee AS (
      SELECT COALESCE(SUM(program_price), 0) as total
      FROM clients WHERE customer_id IN (SELECT customer_id FROM client_ids)
    )
    SELECT
      ma.matched_leads + (SELECT count FROM unmatched_count) as leads,
      ma.matched_leads + (SELECT count FROM unmatched_count) + (SELECT total FROM spam_excluded) + (SELECT count FROM unmatched_excluded) as total_contacts,
      ma.matched_leads + (SELECT count FROM unmatched_count) as quality_leads,
      (SELECT total FROM spam_excluded) + (SELECT count FROM unmatched_excluded) as spam_count,
      ma.inspection_scheduled,
      ma.inspection_completed,
      ma.estimate_sent,
      ma.estimate_approved,
      ma.job_scheduled,
      ma.job_completed,
      ma.revenue_closed,
      ma.open_estimate_count,
      ma.estimate_sent_value,
      ma.estimate_approved_value,
      ma.job_value,
      (SELECT ad_spend FROM period_spend) as ad_spend,
      ma.closed_rev,
      ma.open_est_rev,
      (SELECT total FROM all_time_spend) as all_time_spend,
      (SELECT total FROM all_time_rev) as all_time_rev,
      (SELECT total FROM program_fee) as program_price,
      (SELECT months_in FROM guarantee_period) as months_in_program
      ,(SELECT array_agg(phone) FROM (
        SELECT DISTINCT phone_normalized as phone FROM matched
        UNION SELECT phone FROM unmatched_calls
        UNION SELECT phone FROM unmatched_forms
      ) qp) as quality_phones
    FROM matched_agg ma
  `, params);

  const result = rows[0] || {};
  // DEBUG: log funnel results for troubleshooting

  // Add projected close total
  const { rows: projRows } = await pool.query(
    `SELECT COALESCE(SUM(projected_revenue_cents), 0) / 100.0 as total FROM projected_closes WHERE customer_id = $1`,
    [customerId]
  );
  result.projected_close_total = parseFloat(projRows[0]?.total) || 0;

  return result;
}

async function getJobberFunnel(pool, customerId, params, dateWhere, sourceWhere, cidCTE) {
  // Date filter for Jobber: use jobber_created_at on the customer
  const jcDateWhere = dateWhere.replace(/lead_date/g, 'jc.jobber_created_at::date');

  // Source filter for Jobber
  let jcSourceWhere = '';
  if (sourceWhere.includes('is_google_ads_call')) {
    jcSourceWhere = `AND (
      jc.attribution_override = 'google_ads'
      OR jc.callrail_id LIKE 'WF_%'
      OR EXISTS (SELECT 1 FROM calls ca WHERE ca.callrail_id = jc.callrail_id AND is_google_ads_call(ca.source, ca.source_name, ca.gclid))
      OR EXISTS (SELECT 1 FROM calls ca WHERE normalize_phone(ca.caller_phone) = jc.phone_normalized
        AND ca.customer_id IN (SELECT customer_id FROM client_ids)
        AND is_google_ads_call(ca.source, ca.source_name, ca.gclid))
      OR EXISTS (SELECT 1 FROM form_submissions fs WHERE fs.customer_id IN (SELECT customer_id FROM client_ids)
        AND (fs.callrail_id = jc.callrail_id OR fs.phone_normalized = jc.phone_normalized)
        AND fs.gclid IS NOT NULL AND fs.gclid != '')
      OR EXISTS (SELECT 1 FROM ghl_contacts gc WHERE gc.customer_id = jc.customer_id
        AND (gc.phone_normalized = jc.phone_normalized OR (jc.email IS NOT NULL AND jc.email != '' AND LOWER(gc.email) = LOWER(jc.email)))
        AND gc.gclid IS NOT NULL AND gc.gclid != '')
    )`;
  }

  // Unmatched calls/forms (same pattern as HCP)
  let crSourceWhere = '';
  let fmSourceWhere = '';
  if (sourceWhere.includes('is_google_ads_call')) {
    crSourceWhere = `AND is_google_ads_call(c2.source, c2.source_name, c2.gclid)`;
    fmSourceWhere = `AND (f2.gclid IS NOT NULL OR f2.source = 'Google Ads')`;
  }
  const crDateWhere = dateWhere.replace(/lead_date/g, 'c2.start_time::date');
  const fmDateWhere = dateWhere.replace(/lead_date/g, 'f2.submitted_at::date');

  const spamExclude = `AND NOT (
    EXISTS (
      SELECT 1 FROM ghl_contacts gc
      WHERE gc.phone_normalized = jc.phone_normalized AND gc.customer_id = jc.customer_id
        AND LOWER(gc.lost_reason) SIMILAR TO '%(spam|not a lead|wrong number|out of area|wrong service|abandoned)%'
            AND NOT EXISTS (SELECT 1 FROM jobber_customers jc3
              JOIN jobber_quotes q3 ON q3.jobber_customer_id = jc3.jobber_customer_id AND q3.customer_id = jc3.customer_id
              WHERE jc3.customer_id IN (SELECT customer_id FROM client_ids) AND jc3.phone_normalized = gc.phone_normalized)
            AND NOT EXISTS (SELECT 1 FROM jobber_customers jc3
              JOIN jobber_jobs j3 ON j3.jobber_customer_id = jc3.jobber_customer_id AND j3.customer_id = jc3.customer_id
              WHERE jc3.customer_id IN (SELECT customer_id FROM client_ids) AND jc3.phone_normalized = gc.phone_normalized)
    )
    AND NOT (
      -- CRM activity override: never exclude leads with real Jobber activity
      EXISTS (SELECT 1 FROM jobber_quotes q2 WHERE q2.jobber_customer_id = jc.jobber_customer_id AND q2.customer_id = jc.customer_id)
      OR EXISTS (SELECT 1 FROM jobber_jobs j2 WHERE j2.jobber_customer_id = jc.jobber_customer_id AND j2.customer_id = jc.customer_id)
    )
  )`;

  const { rows } = await pool.query(`
    ${cidCTE},
    matched_leads AS (
      SELECT
        jc.jobber_customer_id,
        jc.phone_normalized,
        jc.email,
        jc.jobber_created_at,
        -- First GA touch time
        LEAST(
          (SELECT MIN(c.start_time) FROM calls c
           WHERE (c.callrail_id = jc.callrail_id OR (c.customer_id = jc.customer_id AND normalize_phone(c.caller_phone) = jc.phone_normalized))
             AND (c.source IN ('Google Ads','Google Ads 2') OR c.gclid IS NOT NULL OR c.classified_source = 'google_ads'
                  OR EXISTS (SELECT 1 FROM callrail_trackers ct WHERE ct.tracker_id = c.tracker_id AND ct.source_type = 'google_ad_extension'))),
          (SELECT MIN(f.submitted_at) FROM form_submissions f
           WHERE f.customer_id = jc.customer_id
             AND (f.callrail_id = jc.callrail_id OR normalize_phone(f.customer_phone) = jc.phone_normalized
                  OR (jc.email IS NOT NULL AND jc.email <> '' AND lower(f.customer_email) = lower(jc.email)))
             AND (f.gclid IS NOT NULL OR f.source = 'Google Ads')),
          (SELECT MIN(COALESCE(gc.kpi_date_created, gc.date_added)) FROM ghl_contacts gc
           WHERE gc.customer_id = jc.customer_id
             AND (gc.phone_normalized = jc.phone_normalized
                  OR (jc.email IS NOT NULL AND jc.email <> '' AND lower(gc.email) = lower(jc.email)))
             AND gc.gclid IS NOT NULL AND gc.gclid <> '')
        ) as first_ga_touch_time,
        -- Reactivation: exclude if recent activity (<60d) or had prior treatment
        CASE WHEN jc.jobber_created_at < LEAST(
            COALESCE((SELECT MIN(c.start_time) FROM calls c
             WHERE (c.callrail_id = jc.callrail_id OR (c.customer_id = jc.customer_id AND normalize_phone(c.caller_phone) = jc.phone_normalized))
               AND (c.source IN ('Google Ads','Google Ads 2') OR c.gclid IS NOT NULL OR c.classified_source = 'google_ads'
                    OR EXISTS (SELECT 1 FROM callrail_trackers ct WHERE ct.tracker_id = c.tracker_id AND ct.source_type = 'google_ad_extension'))),
            '9999-12-31'::timestamptz),
            COALESCE((SELECT MIN(f.submitted_at) FROM form_submissions f
             WHERE f.customer_id = jc.customer_id
               AND (f.callrail_id = jc.callrail_id OR normalize_phone(f.customer_phone) = jc.phone_normalized)
               AND (f.gclid IS NOT NULL OR f.source = 'Google Ads')),
            '9999-12-31'::timestamptz),
            COALESCE((SELECT MIN(COALESCE(gc.kpi_date_created, gc.date_added)) FROM ghl_contacts gc
             WHERE gc.customer_id = jc.customer_id
               AND (gc.phone_normalized = jc.phone_normalized OR (jc.email IS NOT NULL AND jc.email <> '' AND lower(gc.email) = lower(jc.email)))
               AND gc.gclid IS NOT NULL AND gc.gclid <> ''),
            '9999-12-31'::timestamptz)
          ) - INTERVAL '7 days'
          AND (
            -- Activity gap < 60 days (use jobber_created_at as proxy for last interaction)
            EXTRACT(EPOCH FROM (LEAST(
              COALESCE((SELECT MIN(c.start_time) FROM calls c
               WHERE (c.callrail_id = jc.callrail_id OR (c.customer_id = jc.customer_id AND normalize_phone(c.caller_phone) = jc.phone_normalized))
                 AND (c.source IN ('Google Ads','Google Ads 2') OR c.gclid IS NOT NULL OR c.classified_source = 'google_ads'
                      OR EXISTS (SELECT 1 FROM callrail_trackers ct WHERE ct.tracker_id = c.tracker_id AND ct.source_type = 'google_ad_extension'))),
              '9999-12-31'::timestamptz),
              COALESCE((SELECT MIN(f.submitted_at) FROM form_submissions f
               WHERE f.customer_id = jc.customer_id
                 AND (f.callrail_id = jc.callrail_id OR normalize_phone(f.customer_phone) = jc.phone_normalized)
                 AND (f.gclid IS NOT NULL OR f.source = 'Google Ads')),
              '9999-12-31'::timestamptz),
              COALESCE((SELECT MIN(COALESCE(gc.kpi_date_created, gc.date_added)) FROM ghl_contacts gc
               WHERE gc.customer_id = jc.customer_id
                 AND (gc.phone_normalized = jc.phone_normalized OR (jc.email IS NOT NULL AND jc.email <> '' AND lower(gc.email) = lower(jc.email)))
                 AND gc.gclid IS NOT NULL AND gc.gclid <> ''),
              '9999-12-31'::timestamptz)
            ) - GREATEST(
              jc.jobber_created_at,
              COALESCE((SELECT MAX(j.start_at) FROM jobber_jobs j WHERE j.jobber_customer_id = jc.jobber_customer_id AND j.customer_id = jc.customer_id), jc.jobber_created_at),
              COALESCE((SELECT MAX(q.created_at) FROM jobber_quotes q WHERE q.jobber_customer_id = jc.jobber_customer_id AND q.customer_id = jc.customer_id), jc.jobber_created_at)
            )) / 86400 <= 60
            -- OR had prior treatment job (non-inspection, completed, >= $1000)
            OR EXISTS (SELECT 1 FROM jobber_jobs j2 WHERE j2.jobber_customer_id = jc.jobber_customer_id AND j2.customer_id = jc.customer_id
              AND j2.status IN ('late','requires_invoicing') AND j2.total_cents >= 100000
              AND NOT (LOWER(j2.title) LIKE '%assessment%' OR LOWER(j2.title) LIKE '%instascope%' OR LOWER(j2.title) LIKE '%inspection%'
                OR LOWER(j2.title) LIKE '%mold test%' OR LOWER(j2.title) LIKE '%air quality%' OR LOWER(j2.title) LIKE '%air test%'))
          )
        THEN true ELSE false END as exclude_from_ga_roas,
        -- Inspection: request with assessment OR inspection-titled job
        GREATEST(
          COALESCE((SELECT COUNT(*) FROM jobber_requests jr WHERE jr.jobber_customer_id = jc.jobber_customer_id
            AND jr.has_assessment = true AND jr.assessment_start_at IS NOT NULL), 0),
          COALESCE((SELECT COUNT(*) FROM jobber_jobs jj WHERE jj.jobber_customer_id = jc.jobber_customer_id AND jj.customer_id = jc.customer_id
            AND (LOWER(jj.title) LIKE '%assessment%' OR LOWER(jj.title) LIKE '%instascope%' OR LOWER(jj.title) LIKE '%inspection%'
              OR LOWER(jj.title) LIKE '%mold test%' OR LOWER(jj.title) LIKE '%air quality%' OR LOWER(jj.title) LIKE '%air test%')), 0)
        ) > 0 as has_inspection_scheduled,
        GREATEST(
          COALESCE((SELECT COUNT(*) FROM jobber_requests jr WHERE jr.jobber_customer_id = jc.jobber_customer_id
            AND jr.assessment_completed_at IS NOT NULL), 0),
          COALESCE((SELECT COUNT(*) FROM jobber_jobs jj WHERE jj.jobber_customer_id = jc.jobber_customer_id AND jj.customer_id = jc.customer_id
            AND jj.status IN ('late', 'requires_invoicing')
            AND (LOWER(jj.title) LIKE '%assessment%' OR LOWER(jj.title) LIKE '%instascope%' OR LOWER(jj.title) LIKE '%inspection%'
              OR LOWER(jj.title) LIKE '%mold test%' OR LOWER(jj.title) LIKE '%air quality%' OR LOWER(jj.title) LIKE '%air test%')), 0)
        ) > 0 as has_inspection_completed,
        EXISTS (SELECT 1 FROM jobber_quotes q WHERE q.jobber_customer_id = jc.jobber_customer_id AND q.customer_id = jc.customer_id
          AND q.status IN ('awaiting_response','approved','converted','changes_requested') AND q.total_cents >= 100000) as has_estimate_sent,
        EXISTS (SELECT 1 FROM jobber_quotes q WHERE q.jobber_customer_id = jc.jobber_customer_id AND q.customer_id = jc.customer_id
          AND q.status IN ('approved','converted') AND q.total_cents >= 100000) as has_estimate_approved,
        EXISTS (SELECT 1 FROM jobber_jobs j WHERE j.jobber_customer_id = jc.jobber_customer_id AND j.customer_id = jc.customer_id
          AND j.status NOT IN ('archived')
          AND NOT (LOWER(j.title) LIKE '%assessment%' OR LOWER(j.title) LIKE '%instascope%' OR LOWER(j.title) LIKE '%inspection%'
            OR LOWER(j.title) LIKE '%mold test%' OR LOWER(j.title) LIKE '%air quality%' OR LOWER(j.title) LIKE '%air test%')
        ) as has_job_scheduled,
        EXISTS (SELECT 1 FROM jobber_jobs j WHERE j.jobber_customer_id = jc.jobber_customer_id AND j.customer_id = jc.customer_id
          AND j.status IN ('late', 'requires_invoicing')
          AND NOT (LOWER(j.title) LIKE '%assessment%' OR LOWER(j.title) LIKE '%instascope%' OR LOWER(j.title) LIKE '%inspection%'
            OR LOWER(j.title) LIKE '%mold test%' OR LOWER(j.title) LIKE '%air quality%' OR LOWER(j.title) LIKE '%air test%')
        ) as has_job_completed,
        -- Revenue: approved quotes
        COALESCE((SELECT SUM(q.total_cents) FROM jobber_quotes q WHERE q.jobber_customer_id = jc.jobber_customer_id AND q.customer_id = jc.customer_id
          AND q.status IN ('awaiting_response','approved','converted','changes_requested','draft') AND q.total_cents >= 100000), 0) as est_sent_cents,
        COALESCE((SELECT SUM(q.total_cents) FROM jobber_quotes q WHERE q.jobber_customer_id = jc.jobber_customer_id AND q.customer_id = jc.customer_id
          AND q.status IN ('approved','converted') AND q.total_cents >= 100000), 0) as est_approved_cents,
        COALESCE((SELECT SUM(j.total_cents) FROM jobber_jobs j WHERE j.jobber_customer_id = jc.jobber_customer_id AND j.customer_id = jc.customer_id
          AND j.status NOT IN ('archived')), 0) as job_cents
      FROM jobber_customers jc
      WHERE jc.customer_id IN (SELECT customer_id FROM client_ids)
        ${jcDateWhere}
        ${jcSourceWhere}
        ${spamExclude}
        AND jc.is_archived = false
    ),
    unmatched_calls AS (
      SELECT DISTINCT ON (normalize_phone(c2.caller_phone)) normalize_phone(c2.caller_phone) as phone
      FROM calls c2
      WHERE c2.customer_id IN (SELECT customer_id FROM client_ids)
        ${crDateWhere} ${crSourceWhere}
      ORDER BY normalize_phone(c2.caller_phone), c2.start_time
    ),
    unmatched_forms AS (
      SELECT DISTINCT ON (normalize_phone(f2.customer_phone)) normalize_phone(f2.customer_phone) as phone
      FROM form_submissions f2
      WHERE f2.customer_id IN (SELECT customer_id FROM client_ids)
        AND COALESCE(f2.is_spam, false) = false
        ${fmDateWhere} ${fmSourceWhere}
      ORDER BY normalize_phone(f2.customer_phone), f2.submitted_at
    ),
    unmatched_count AS (
      SELECT
        (SELECT COUNT(*) FROM unmatched_calls uc
         WHERE NOT EXISTS (SELECT 1 FROM matched_leads ml WHERE ml.phone_normalized = uc.phone)
        ) +
        (SELECT COUNT(*) FROM unmatched_forms uf
         WHERE NOT EXISTS (SELECT 1 FROM matched_leads ml WHERE ml.phone_normalized = uf.phone)
           AND NOT EXISTS (SELECT 1 FROM unmatched_calls uc WHERE uc.phone = uf.phone)
        ) as count
    ),
    -- Direct spam count for Jobber: leads excluded by GHL spam filter
    spam_excluded AS (
      SELECT COUNT(DISTINCT phone) as total FROM (
        SELECT jc.phone_normalized as phone
        FROM jobber_customers jc
        WHERE jc.customer_id IN (SELECT customer_id FROM client_ids)
          ${jcDateWhere}
          ${jcSourceWhere}
          AND jc.is_archived = false
          AND EXISTS (SELECT 1 FROM ghl_contacts gc
            WHERE gc.phone_normalized = jc.phone_normalized AND gc.customer_id = jc.customer_id
              AND LOWER(gc.lost_reason) SIMILAR TO '%(spam|not a lead|wrong number|out of area|wrong service|abandoned)%'
            AND NOT EXISTS (SELECT 1 FROM jobber_customers jc3
              JOIN jobber_quotes q3 ON q3.jobber_customer_id = jc3.jobber_customer_id AND q3.customer_id = jc3.customer_id
              WHERE jc3.customer_id IN (SELECT customer_id FROM client_ids) AND jc3.phone_normalized = gc.phone_normalized)
            AND NOT EXISTS (SELECT 1 FROM jobber_customers jc3
              JOIN jobber_jobs j3 ON j3.jobber_customer_id = jc3.jobber_customer_id AND j3.customer_id = jc3.customer_id
              WHERE jc3.customer_id IN (SELECT customer_id FROM client_ids) AND jc3.phone_normalized = gc.phone_normalized))
        UNION
        SELECT uc.phone FROM unmatched_calls uc
        WHERE NOT EXISTS (SELECT 1 FROM matched_leads ml WHERE ml.phone_normalized = uc.phone)
          AND EXISTS (SELECT 1 FROM ghl_contacts gc WHERE gc.phone_normalized = uc.phone
            AND gc.customer_id IN (SELECT customer_id FROM client_ids)
            AND LOWER(gc.lost_reason) SIMILAR TO '%(spam|not a lead|wrong number|out of area|wrong service|abandoned)%'
            AND NOT EXISTS (SELECT 1 FROM hcp_customers hc3
              JOIN hcp_inspections i3 ON i3.hcp_customer_id = hc3.hcp_customer_id AND i3.record_status = 'active'
              WHERE hc3.customer_id IN (SELECT customer_id FROM client_ids) AND hc3.phone_normalized = gc.phone_normalized)
            AND NOT EXISTS (SELECT 1 FROM hcp_customers hc3
              JOIN hcp_estimates e3 ON e3.hcp_customer_id = hc3.hcp_customer_id AND e3.record_status IN ('active','option')
              WHERE hc3.customer_id IN (SELECT customer_id FROM client_ids) AND hc3.phone_normalized = gc.phone_normalized)
            AND NOT EXISTS (SELECT 1 FROM hcp_customers hc3
              JOIN hcp_invoices inv3 ON inv3.hcp_customer_id = hc3.hcp_customer_id AND inv3.status NOT IN ('canceled','voided') AND inv3.amount_cents > 0
              WHERE hc3.customer_id IN (SELECT customer_id FROM client_ids) AND hc3.phone_normalized = gc.phone_normalized)
            AND NOT EXISTS (SELECT 1 FROM hcp_customers hc3
              JOIN hcp_jobs j3 ON j3.hcp_customer_id = hc3.hcp_customer_id AND j3.record_status = 'active'
              WHERE hc3.customer_id IN (SELECT customer_id FROM client_ids) AND hc3.phone_normalized = gc.phone_normalized))
        UNION
        SELECT uf.phone FROM unmatched_forms uf
        WHERE NOT EXISTS (SELECT 1 FROM matched_leads ml WHERE ml.phone_normalized = uf.phone)
          AND NOT EXISTS (SELECT 1 FROM unmatched_calls uc WHERE uc.phone = uf.phone)
          AND EXISTS (SELECT 1 FROM ghl_contacts gc WHERE gc.phone_normalized = uf.phone
            AND gc.customer_id IN (SELECT customer_id FROM client_ids)
            AND LOWER(gc.lost_reason) SIMILAR TO '%(spam|not a lead|wrong number|out of area|wrong service|abandoned)%'
            AND NOT EXISTS (SELECT 1 FROM hcp_customers hc3
              JOIN hcp_inspections i3 ON i3.hcp_customer_id = hc3.hcp_customer_id AND i3.record_status = 'active'
              WHERE hc3.customer_id IN (SELECT customer_id FROM client_ids) AND hc3.phone_normalized = gc.phone_normalized)
            AND NOT EXISTS (SELECT 1 FROM hcp_customers hc3
              JOIN hcp_estimates e3 ON e3.hcp_customer_id = hc3.hcp_customer_id AND e3.record_status IN ('active','option')
              WHERE hc3.customer_id IN (SELECT customer_id FROM client_ids) AND hc3.phone_normalized = gc.phone_normalized)
            AND NOT EXISTS (SELECT 1 FROM hcp_customers hc3
              JOIN hcp_invoices inv3 ON inv3.hcp_customer_id = hc3.hcp_customer_id AND inv3.status NOT IN ('canceled','voided') AND inv3.amount_cents > 0
              WHERE hc3.customer_id IN (SELECT customer_id FROM client_ids) AND hc3.phone_normalized = gc.phone_normalized)
            AND NOT EXISTS (SELECT 1 FROM hcp_customers hc3
              JOIN hcp_jobs j3 ON j3.hcp_customer_id = hc3.hcp_customer_id AND j3.record_status = 'active'
              WHERE hc3.customer_id IN (SELECT customer_id FROM client_ids) AND hc3.phone_normalized = gc.phone_normalized))
      ) s
    ),
        all_contacts AS (
      SELECT COUNT(DISTINCT phone) as total FROM (
        SELECT normalize_phone(c2.caller_phone) as phone FROM calls c2
        WHERE c2.customer_id IN (SELECT customer_id FROM client_ids)
          ${crDateWhere} ${crSourceWhere}
        UNION ALL
        SELECT COALESCE(normalize_phone(f2.customer_phone), 'form_' || f2.callrail_id) FROM form_submissions f2
        WHERE f2.customer_id IN (SELECT customer_id FROM client_ids)
          AND COALESCE(f2.is_spam, false) = false
          ${fmDateWhere} ${fmSourceWhere}
          AND NOT EXISTS (SELECT 1 FROM calls c3
            WHERE c3.customer_id IN (SELECT customer_id FROM client_ids)
              ${crSourceWhere.replace(/c2\./g, 'c3.')}
              AND normalize_phone(c3.caller_phone) = normalize_phone(f2.customer_phone))
      ) t
    ),
    period_spend AS (
      SELECT COALESCE(SUM(cost), 0) as ad_spend
      FROM campaign_daily_metrics
      WHERE customer_id IN (SELECT customer_id FROM client_ids)
        AND campaign_type != 'LOCAL_SERVICES'
        ${dateWhere.replace(/lead_date/g, 'date')}
    ),
    funnel_revenue AS (
      SELECT
        COALESCE(SUM(CASE WHEN NOT COALESCE(exclude_from_ga_roas, false) THEN est_approved_cents ELSE 0 END), 0) / 100.0 as closed_rev,
        COALESCE(SUM(CASE WHEN has_estimate_sent AND NOT has_estimate_approved
            AND NOT COALESCE(exclude_from_ga_roas, false)
          THEN est_sent_cents ELSE 0 END), 0) / 100.0 as open_est_rev
      FROM matched_leads
    ),
    -- All-time for guarantee (exclude LSA)
    all_time_spend_j AS (
      SELECT COALESCE(SUM(cost), 0) as total FROM campaign_daily_metrics
      WHERE customer_id IN (SELECT customer_id FROM client_ids)
        AND campaign_type != 'LOCAL_SERVICES'
    ),
    all_time_rev_j AS (
      SELECT COALESCE(SUM(CASE WHEN NOT COALESCE(ml.exclude_from_ga_roas, false) THEN q.total_cents ELSE 0 END), 0) / 100.0 as total
      FROM jobber_quotes q
      JOIN jobber_customers jc ON jc.jobber_customer_id = q.jobber_customer_id AND jc.customer_id = q.customer_id
      LEFT JOIN matched_leads ml ON ml.jobber_customer_id = jc.jobber_customer_id
      WHERE jc.customer_id IN (SELECT customer_id FROM client_ids)
        AND q.status IN ('approved','converted')
        AND (
          jc.attribution_override = 'google_ads'
          OR jc.callrail_id LIKE 'WF_%'
          OR EXISTS (SELECT 1 FROM calls ca WHERE ca.callrail_id = jc.callrail_id AND is_google_ads_call(ca.source, ca.source_name, ca.gclid))
          OR EXISTS (SELECT 1 FROM calls ca JOIN callrail_trackers ct ON ct.tracker_id = ca.tracker_id
            WHERE normalize_phone(ca.caller_phone) = jc.phone_normalized
            AND ca.customer_id IN (SELECT customer_id FROM client_ids) AND ct.source_type = 'google_ad_extension')
          OR EXISTS (SELECT 1 FROM calls ca WHERE normalize_phone(ca.caller_phone) = jc.phone_normalized
            AND ca.customer_id IN (SELECT customer_id FROM client_ids) AND is_google_ads_call(ca.source, ca.source_name, ca.gclid))
          OR EXISTS (SELECT 1 FROM form_submissions fs WHERE fs.customer_id IN (SELECT customer_id FROM client_ids)
            AND (fs.callrail_id = jc.callrail_id OR fs.phone_normalized = jc.phone_normalized)
            AND fs.gclid IS NOT NULL AND fs.gclid != '')
          OR EXISTS (SELECT 1 FROM ghl_contacts gc WHERE gc.customer_id = jc.customer_id
            AND (gc.phone_normalized = jc.phone_normalized OR (jc.email IS NOT NULL AND jc.email != '' AND LOWER(gc.email) = LOWER(jc.email)))
            AND gc.gclid IS NOT NULL AND gc.gclid != '')
        )
    ),
    program_fee_j AS (
      SELECT COALESCE(SUM(program_price), 0) as total FROM clients WHERE customer_id IN (SELECT customer_id FROM client_ids)
    ),
    funnel_counts AS (
      SELECT
        (SELECT COUNT(*) FROM matched_leads) + (SELECT count FROM unmatched_count) as leads,
        (SELECT COUNT(*) FROM matched_leads) + (SELECT count FROM unmatched_count) + (SELECT total FROM spam_excluded) as total_contacts,
        (SELECT COUNT(*) FROM matched_leads) + (SELECT count FROM unmatched_count) as quality_leads,
        (SELECT total FROM spam_excluded) as spam_count,
        COUNT(*) FILTER (WHERE has_inspection_scheduled) as inspection_scheduled,
        COUNT(*) FILTER (WHERE has_inspection_completed) as inspection_completed,
        COUNT(*) FILTER (WHERE has_estimate_sent) as estimate_sent,
        COUNT(*) FILTER (WHERE has_estimate_approved) as estimate_approved,
        COUNT(*) FILTER (WHERE has_job_scheduled) as job_scheduled,
        COUNT(*) FILTER (WHERE has_job_completed) as job_completed,
        COALESCE(SUM(est_sent_cents) FILTER (WHERE has_estimate_sent), 0) / 100.0 as estimate_sent_value,
        COALESCE(SUM(est_approved_cents) FILTER (WHERE has_estimate_approved), 0) / 100.0 as estimate_approved_value,
        COALESCE(SUM(job_cents) FILTER (WHERE has_job_scheduled), 0) / 100.0 as job_value,
        (SELECT ad_spend FROM period_spend) as ad_spend,
        (SELECT closed_rev FROM funnel_revenue) as closed_rev,
        (SELECT open_est_rev FROM funnel_revenue) as open_est_rev,
        (SELECT total FROM all_time_spend_j) as all_time_spend,
        (SELECT total FROM all_time_rev_j) as all_time_rev,
        (SELECT total FROM program_fee_j) as program_price
      FROM matched_leads
    )
    SELECT * FROM funnel_counts
  `, params);

  const result = rows[0] || {};
  const { rows: projRows } = await pool.query(
    `SELECT COALESCE(SUM(projected_revenue_cents), 0) / 100.0 as total FROM projected_closes WHERE customer_id = $1`,
    [customerId]
  );
  result.projected_close_total = parseFloat(projRows[0]?.total) || 0;
  return result;
}

// GHL funnel helper — starts from CallRail leads, enriches with GHL data
// Same pattern as HCP/Jobber: CallRail = source of truth, GHL = funnel enrichment
async function getGhlFunnel(pool, customerId, params, dateWhere, sourceWhere, cidCTE) {
  // Source filters for CallRail
  let crSourceWhere = '';
  let fmSourceWhere = '';
  if (sourceWhere.includes('is_google_ads_call')) {
    crSourceWhere = `AND is_google_ads_call(c.source, c.source_name, c.gclid)`;
    fmSourceWhere = `AND f.gclid IS NOT NULL AND f.gclid != ''`;
  }

  const crDateWhere = dateWhere.replace(/lead_date/g, 'lead_date');
  const spendDateWhere = dateWhere.replace(/lead_date/g, 'date');

  const { rows } = await pool.query(`
    ${cidCTE},
    -- Step 1: All CallRail leads (distinct by phone) — source of truth
    cr_leads AS (
      SELECT phone, MIN(lead_date) as lead_date FROM (
        SELECT normalize_phone(c.caller_phone) as phone, c.start_time::date as lead_date
        FROM calls c
        WHERE c.customer_id IN (SELECT customer_id FROM client_ids)
          ${crSourceWhere}
        UNION ALL
        SELECT normalize_phone(f.customer_phone) as phone, f.submitted_at::date as lead_date
        FROM form_submissions f
        WHERE f.customer_id IN (SELECT customer_id FROM client_ids)
          AND COALESCE(f.is_spam, false) = false
          ${fmSourceWhere}
      ) combined
      WHERE phone IS NOT NULL AND phone != ''
      GROUP BY phone
    ),
    -- Step 1b: GHL contacts with GCLID (fallback for leads that bypassed CallRail)
    ghl_gclid_leads AS (
      SELECT gc2.phone_normalized as phone, gc2.date_added::date as lead_date
      FROM ghl_contacts gc2
      WHERE gc2.customer_id IN (SELECT customer_id FROM client_ids)
        AND gc2.gclid IS NOT NULL
        AND gc2.phone_normalized IS NOT NULL
        ${crSourceWhere ? "" : "AND 1=0"}
    ),
    -- Merge CallRail + GHL GCLID leads
    all_leads AS (
      SELECT phone, MIN(lead_date) as lead_date FROM (
        SELECT phone, lead_date FROM cr_leads
        UNION ALL
        SELECT phone, lead_date FROM ghl_gclid_leads
      ) merged
      GROUP BY phone
    ),
    -- Step 2: Match CallRail leads to GHL contacts by phone (enrichment)
    matched_leads AS (
      SELECT
        cr.phone,
        cr.lead_date,
        gc.ghl_contact_id,
        -- Spam check from GHL
        CASE WHEN gc.ghl_contact_id IS NOT NULL
          AND LOWER(COALESCE(gc.lost_reason, '')) SIMILAR TO '%(spam|not a lead|wrong number|out of area|wrong service|abandoned)%'
          THEN true ELSE false END as is_spam,
        -- Inspection scheduled: GHL appointment type='inspection' not cancelled
        EXISTS (SELECT 1 FROM ghl_appointments ga WHERE ga.ghl_contact_id = gc.ghl_contact_id
          AND ga.customer_id IN (SELECT customer_id FROM client_ids)
          AND ga.appointment_type = 'inspection' AND ga.deleted = false
          AND ga.status != 'cancelled') as has_inspection_scheduled,
        -- Inspection completed: appointment showed OR opportunity stage past inspection
        EXISTS (SELECT 1 FROM ghl_appointments ga WHERE ga.ghl_contact_id = gc.ghl_contact_id
          AND ga.customer_id IN (SELECT customer_id FROM client_ids)
          AND ga.appointment_type = 'inspection' AND ga.deleted = false
          AND ga.status IN ('showed', 'completed')
        ) OR EXISTS (SELECT 1 FROM ghl_opportunities o WHERE o.ghl_contact_id = gc.ghl_contact_id
          AND o.customer_id IN (SELECT customer_id FROM client_ids)
          AND (o.stage_name ILIKE 'inspection completed%' OR o.stage_name ILIKE 'estimate given%'
            OR o.stage_name ILIKE 'job needs%' OR o.stage_name ILIKE 'job scheduled%'
            OR o.stage_name ILIKE 'job completed%' OR o.stage_name ILIKE 'job paid%' OR o.stage_name ILIKE 'request reviews%')
        ) as has_inspection_completed,
        -- Estimate sent: ghl_estimates by phone OR opportunity stage
        EXISTS (SELECT 1 FROM ghl_estimates ge WHERE ge.phone_normalized = cr.phone
          AND ge.customer_id IN (SELECT customer_id FROM client_ids)
          AND ge.status IN ('sent', 'accepted', 'invoiced')
        ) OR EXISTS (SELECT 1 FROM ghl_opportunities o WHERE o.ghl_contact_id = gc.ghl_contact_id
          AND o.customer_id IN (SELECT customer_id FROM client_ids)
          AND (o.stage_name ILIKE 'estimate given%' OR o.stage_name ILIKE 'job needs%'
            OR o.stage_name ILIKE 'job scheduled%' OR o.stage_name ILIKE 'job completed%'
            OR o.stage_name ILIKE 'job paid%' OR o.stage_name ILIKE 'request reviews%')
        ) as has_estimate_sent,
        -- Estimate approved: accepted/invoiced OR opportunity stage past estimate
        EXISTS (SELECT 1 FROM ghl_estimates ge WHERE ge.phone_normalized = cr.phone
          AND ge.customer_id IN (SELECT customer_id FROM client_ids)
          AND ge.status IN ('accepted', 'invoiced')
        ) OR EXISTS (SELECT 1 FROM ghl_opportunities o WHERE o.ghl_contact_id = gc.ghl_contact_id
          AND o.customer_id IN (SELECT customer_id FROM client_ids)
          AND (o.stage_name ILIKE 'job needs%' OR o.stage_name ILIKE 'job scheduled%'
            OR o.stage_name ILIKE 'job completed%' OR o.stage_name ILIKE 'job paid%' OR o.stage_name ILIKE 'request reviews%')
        ) as has_estimate_approved,
        -- Job scheduled: job appointment OR opportunity stage
        EXISTS (SELECT 1 FROM ghl_appointments ga WHERE ga.ghl_contact_id = gc.ghl_contact_id
          AND ga.customer_id IN (SELECT customer_id FROM client_ids)
          AND ga.appointment_type = 'job' AND ga.deleted = false AND ga.status != 'cancelled'
        ) OR EXISTS (SELECT 1 FROM ghl_opportunities o WHERE o.ghl_contact_id = gc.ghl_contact_id
          AND o.customer_id IN (SELECT customer_id FROM client_ids)
          AND (o.stage_name ILIKE 'job scheduled%' OR o.stage_name ILIKE 'job completed%'
            OR o.stage_name ILIKE 'job paid%' OR o.stage_name ILIKE 'request reviews%')
        ) as has_job_scheduled,
        -- Job completed: showed job appointment OR opportunity stage
        EXISTS (SELECT 1 FROM ghl_appointments ga WHERE ga.ghl_contact_id = gc.ghl_contact_id
          AND ga.customer_id IN (SELECT customer_id FROM client_ids)
          AND ga.appointment_type = 'job' AND ga.deleted = false
          AND ga.status IN ('showed', 'completed')
        ) OR EXISTS (SELECT 1 FROM ghl_opportunities o WHERE o.ghl_contact_id = gc.ghl_contact_id
          AND o.customer_id IN (SELECT customer_id FROM client_ids)
          AND (o.stage_name ILIKE 'job completed%' OR o.stage_name ILIKE 'job paid%' OR o.stage_name ILIKE 'request reviews%')
        ) as has_job_completed,
        -- Revenue: estimates matched by phone (not contact_id — many are null)
        COALESCE((SELECT SUM(ge.total_cents) FROM ghl_estimates ge
          WHERE ge.phone_normalized = cr.phone AND ge.customer_id IN (SELECT customer_id FROM client_ids)
          AND ge.status IN ('sent', 'accepted', 'invoiced')), 0) as est_sent_cents,
        -- Closed rev: GREATEST(invoiced, accepted) per RULES.md Section 33
        GREATEST(
          COALESCE((SELECT SUM(ge.total_cents) FROM ghl_estimates ge
            WHERE ge.phone_normalized = cr.phone AND ge.customer_id IN (SELECT customer_id FROM client_ids)
            AND ge.status = 'invoiced'), 0),
          COALESCE((SELECT SUM(ge.total_cents) FROM ghl_estimates ge
            WHERE ge.phone_normalized = cr.phone AND ge.customer_id IN (SELECT customer_id FROM client_ids)
            AND ge.status = 'accepted'), 0)
        ) as est_approved_cents,
        -- Open estimate value (sent only)
        COALESCE((SELECT SUM(ge.total_cents) FROM ghl_estimates ge
          WHERE ge.phone_normalized = cr.phone AND ge.customer_id IN (SELECT customer_id FROM client_ids)
          AND ge.status = 'sent'), 0) as est_open_cents,
        -- Transaction revenue: inspection (standalone) + treatment (estimate-linked)
        COALESCE((SELECT SUM(gt.amount_cents) FROM ghl_transactions gt
          WHERE gt.phone_normalized = cr.phone AND gt.customer_id IN (SELECT customer_id FROM client_ids)
          AND gt.status = 'succeeded'
          AND (gt.entity_source_sub_type IS NULL OR gt.entity_source_sub_type != 'estimate')), 0) as txn_insp_cents,
        COALESCE((SELECT SUM(gt.amount_cents) FROM ghl_transactions gt
          WHERE gt.phone_normalized = cr.phone AND gt.customer_id IN (SELECT customer_id FROM client_ids)
          AND gt.status = 'succeeded'
          AND gt.entity_source_sub_type = 'estimate'), 0) as txn_treat_cents
      FROM all_leads cr
      LEFT JOIN LATERAL (
        SELECT gc2.ghl_contact_id, gc2.lost_reason
        FROM ghl_contacts gc2
        WHERE gc2.phone_normalized = cr.phone
          AND gc2.customer_id IN (SELECT customer_id FROM client_ids)
        ORDER BY gc2.date_added ASC
        LIMIT 1
      ) gc ON true
      WHERE cr.lead_date IS NOT NULL
        ${crDateWhere}
    ),
    -- Quality leads (exclude GHL spam)
    quality_leads AS (
      SELECT * FROM matched_leads WHERE NOT is_spam
    ),
    -- Ad spend for the period (exclude LSA)
    period_spend AS (
      SELECT COALESCE(SUM(cost), 0) as ad_spend
      FROM campaign_daily_metrics
      WHERE customer_id IN (SELECT customer_id FROM client_ids)
        AND campaign_type != 'LOCAL_SERVICES'
        ${spendDateWhere}
    ),
    -- Funnel revenue from quality leads
    funnel_revenue AS (
      SELECT
        COALESCE(SUM(txn_insp_cents + GREATEST(txn_treat_cents, est_approved_cents)), 0) / 100.0 as closed_rev,
        COALESCE(SUM(CASE WHEN has_estimate_sent AND NOT has_estimate_approved AND txn_treat_cents = 0
          THEN est_sent_cents ELSE 0 END), 0) / 100.0 as open_est_rev
      FROM quality_leads
    ),
    -- All-time spend (exclude LSA)
    all_time_spend_g AS (
      SELECT COALESCE(SUM(cost), 0) as total FROM campaign_daily_metrics
      WHERE customer_id IN (SELECT customer_id FROM client_ids)
        AND campaign_type != 'LOCAL_SERVICES'
    ),
    -- All-time GA-attributed GHL revenue (all dates, GA-matched only)
    all_time_rev_g AS (
      SELECT COALESCE(SUM(rev), 0) / 100.0 as total FROM (
        SELECT
          ph.phone,
          COALESCE((SELECT SUM(gt.amount_cents) FROM ghl_transactions gt
            WHERE gt.phone_normalized = ph.phone AND gt.customer_id IN (SELECT customer_id FROM client_ids)
            AND gt.status = 'succeeded' AND (gt.entity_source_sub_type IS NULL OR gt.entity_source_sub_type != 'estimate')), 0)
          + GREATEST(
            COALESCE((SELECT SUM(gt.amount_cents) FROM ghl_transactions gt
              WHERE gt.phone_normalized = ph.phone AND gt.customer_id IN (SELECT customer_id FROM client_ids)
              AND gt.status = 'succeeded' AND gt.entity_source_sub_type = 'estimate'), 0),
            COALESCE((SELECT SUM(ge.total_cents) FROM ghl_estimates ge
              WHERE ge.phone_normalized = ph.phone AND ge.customer_id IN (SELECT customer_id FROM client_ids)
              AND ge.status IN ('accepted', 'invoiced')), 0)
          ) as rev
        FROM (
          SELECT DISTINCT phone FROM all_leads
        ) ph
      ) per_phone
    ),
    -- Program price for guarantee
    program_fee_g AS (
      SELECT COALESCE(SUM(program_price), 0) as total FROM clients WHERE customer_id IN (SELECT customer_id FROM client_ids)
    ),
    -- Final funnel counts
    funnel_counts AS (
      SELECT
        (SELECT COUNT(*) FROM quality_leads) as leads,
        (SELECT COUNT(*) FROM cr_leads) as total_contacts,
        (SELECT COUNT(*) FROM quality_leads) as quality_leads,
        (SELECT COUNT(*) FROM matched_leads WHERE is_spam) as spam_count,
        COUNT(*) FILTER (WHERE has_inspection_scheduled) as inspection_scheduled,
        COUNT(*) FILTER (WHERE has_inspection_completed) as inspection_completed,
        COUNT(*) FILTER (WHERE has_estimate_sent) as estimate_sent,
        COUNT(*) FILTER (WHERE has_estimate_approved) as estimate_approved,
        COUNT(*) FILTER (WHERE has_job_scheduled) as job_scheduled,
        COUNT(*) FILTER (WHERE has_job_completed) as job_completed,
        COALESCE(SUM(est_sent_cents) FILTER (WHERE has_estimate_sent), 0) / 100.0 as estimate_sent_value,
        COALESCE(SUM(est_approved_cents) FILTER (WHERE has_estimate_approved), 0) / 100.0 as estimate_approved_value,
        0 as job_value,
        (SELECT ad_spend FROM period_spend) as ad_spend,
        (SELECT closed_rev FROM funnel_revenue) as closed_rev,
        (SELECT open_est_rev FROM funnel_revenue) as open_est_rev,
        (SELECT total FROM all_time_spend_g) as all_time_spend,
        (SELECT total FROM all_time_rev_g) as all_time_rev,
        (SELECT total FROM program_fee_g) as program_price
      FROM quality_leads
    )
    SELECT * FROM funnel_counts
  `, params);

  const result = rows[0] || {};

  // Add projected close total
  const { rows: projRows } = await pool.query(
    `SELECT COALESCE(SUM(projected_revenue_cents), 0) / 100.0 as total FROM projected_closes WHERE customer_id = $1`,
    [customerId]
  );
  result.projected_close_total = parseFloat(projRows[0]?.total) || 0;

  return result;
}
// ============================================================
// Analytics — Ad Performance
// ============================================================

// Ad spend, CPL, ROAS for a client (uses get_dashboard_metrics for consistency)
fastify.get('/clients/:customerId/ad-performance', async (request) => {
  const { customerId } = request.params;
  const { days = 30 } = request.query;
  const endDate = new Date().toISOString().split('T')[0];
  const startDate = new Date(Date.now() - days * 86400000).toISOString().split('T')[0];

  const { rows } = await pool.query(
    `SELECT * FROM get_dashboard_metrics($1::date, $2::date) WHERE customer_id = $3`,
    [startDate, endDate, customerId]
  );
  if (rows.length === 0) return { error: 'No data' };

  const m = rows[0];
  return {
    ad_spend: +m.ad_spend,
    quality_leads: m.quality_leads,
    actual_quality_leads: m.actual_quality_leads,
    cpl: m.actual_quality_leads > 0 ? +(m.ad_spend / m.actual_quality_leads).toFixed(2) : 0,
    total_closed_rev: +m.total_closed_rev,
    total_open_est_rev: +m.total_open_est_rev,
    roas: m.ad_spend > 0 ? +(m.total_closed_rev / m.ad_spend).toFixed(2) : 0,
    all_time_rev: +m.all_time_rev,
    all_time_spend: +m.all_time_spend,
    guarantee: m.all_time_spend > 0 ? +(m.all_time_rev / m.all_time_spend).toFixed(2) : 0,
    lsa_spend: +m.lsa_spend,
    lsa_leads: m.lsa_leads,
  };
});

// Daily ad spend trend
fastify.get('/clients/:customerId/ad-spend-daily', async (request) => {
  const { customerId } = request.params;
  const { days = 30 } = request.query;

  const { rows } = await pool.query(`
    SELECT date, SUM(cost) as spend
    FROM account_daily_metrics
    WHERE customer_id IN (SELECT customer_id FROM clients WHERE customer_id = $1 OR parent_customer_id = $1)
      AND date >= CURRENT_DATE - $2::int
          GROUP BY date ORDER BY date
  `, [customerId, days]);
  return rows;
});

// Campaign breakdown for Google Ads panel
fastify.get('/clients/:customerId/campaign-breakdown', async (request) => {
  const { customerId } = request.params;
  const { date_from, date_to } = request.query;
  const endDate = date_to || new Date().toISOString().split('T')[0];
  const startDate = date_from || new Date(Date.now() - 90 * 86400000).toISOString().split('T')[0];

  const { rows } = await pool.query(`
    SELECT campaign_name, campaign_type,
      SUM(impressions)::int as impressions, SUM(clicks)::int as clicks,
      ROUND(SUM(cost)::numeric, 2) as cost, ROUND(SUM(conversions)::numeric, 1) as conversions,
      CASE WHEN SUM(impressions) > 0 THEN ROUND(SUM(clicks)::numeric / SUM(impressions) * 100, 2) ELSE 0 END as ctr
    FROM campaign_daily_metrics
    WHERE customer_id IN (SELECT customer_id FROM clients WHERE customer_id = $1 OR parent_customer_id = $1)
      AND date >= $2::date AND date <= $3::date
      AND campaign_status = 'ENABLED'
    GROUP BY campaign_name, campaign_type
    ORDER BY SUM(cost) DESC
  `, [customerId, startDate, endDate]);
  return rows;
});

// Top search terms for Google Ads panel
fastify.get('/clients/:customerId/search-terms', async (request) => {
  const { customerId } = request.params;
  const { date_from, date_to, limit = 10 } = request.query;
  const endDate = date_to || new Date().toISOString().split('T')[0];
  const startDate = date_from || new Date(Date.now() - 90 * 86400000).toISOString().split('T')[0];

  const { rows } = await pool.query(`
    SELECT search_term, SUM(impressions)::int as impressions, SUM(clicks)::int as clicks,
      ROUND(SUM(cost)::numeric, 2) as cost, ROUND(SUM(conversions)::numeric, 1) as conversions
    FROM search_terms_daily
    WHERE customer_id IN (SELECT customer_id FROM clients WHERE customer_id = $1 OR parent_customer_id = $1)
      AND date >= $2::date AND date <= $3::date
    GROUP BY search_term
    ORDER BY SUM(clicks) DESC
    LIMIT $4::int
  `, [customerId, startDate, endDate, limit]);
  return rows;
});

// ============================================================
// Analytics — Lead Contacts (calls + forms)
// ============================================================

// Recent leads (calls + forms) for drill-down
fastify.get('/clients/:customerId/leads', async (request) => {
  const { customerId } = request.params;
  const { source = 'google_ads', date_from, date_to, limit = 50 } = request.query;

  const endDate = date_to || new Date().toISOString().split('T')[0];
  const startDate = date_from || new Date(Date.now() - 30 * 86400000).toISOString().split('T')[0];

  let sourceWhere = '';
  if (source === 'google_ads') {
    sourceWhere = `AND is_google_ads_call(c.source, c.source_name, c.gclid)`;
  } else if (source === 'gbp') {
    sourceWhere = `AND c.source = 'Google My Business' AND NOT is_google_ads_call(c.source, c.source_name, c.gclid)`;
  }

  // Form source filter for leads drawer
  let formSourceWhere = `AND f.gclid IS NOT NULL AND f.gclid != ''`;
  if (source === 'gbp') {
    formSourceWhere = `AND f.source = 'Google My Business' AND f.gclid IS NULL`;
  }

  if (source === 'lsa') {
    // LSA leads: return from lsa_leads table with HCP funnel stage enrichment
    const lsaRows = await pool.query(`
      SELECT
        l.lead_creation_time as contact_date,
        COALESCE(l.contact_name, 'LSA Lead') as name,
        l.contact_phone_normalized as phone,
        l.lead_type as type,
        NULL::int as duration,
        'lsa' as answer_status,
        'LSA' as source_name,
        l.callrail_id,
        l.hcp_customer_id,
        l.contact_phone as phone_display,
        l.lead_charged as charged,
        -- HCP funnel stage
        CASE
          WHEN EXISTS (SELECT 1 FROM hcp_jobs j JOIN (SELECT phone_normalized, array_agg(hcp_customer_id) as ids FROM hcp_customers WHERE customer_id = $1 GROUP BY phone_normalized) pg ON j.hcp_customer_id = ANY(pg.ids) WHERE pg.phone_normalized = l.contact_phone_normalized AND j.record_status = 'active' AND j.status IN ('complete rated','complete unrated')) THEN 'Job Completed'
          WHEN EXISTS (SELECT 1 FROM hcp_jobs j JOIN (SELECT phone_normalized, array_agg(hcp_customer_id) as ids FROM hcp_customers WHERE customer_id = $1 GROUP BY phone_normalized) pg ON j.hcp_customer_id = ANY(pg.ids) WHERE pg.phone_normalized = l.contact_phone_normalized AND j.record_status = 'active' AND j.status IN ('scheduled','in progress')) THEN 'Job Scheduled'
          WHEN EXISTS (SELECT 1 FROM v_estimate_groups eg JOIN hcp_customers hc ON hc.hcp_customer_id = eg.hcp_customer_id WHERE hc.customer_id = $1 AND hc.phone_normalized = l.contact_phone_normalized AND eg.status = 'approved' AND eg.count_revenue) THEN 'Estimate Approved'
          WHEN EXISTS (SELECT 1 FROM v_estimate_groups eg JOIN hcp_customers hc ON hc.hcp_customer_id = eg.hcp_customer_id WHERE hc.customer_id = $1 AND hc.phone_normalized = l.contact_phone_normalized AND eg.status IN ('sent','approved','declined') AND eg.count_revenue) THEN 'Estimate Sent'
          WHEN EXISTS (SELECT 1 FROM hcp_inspections i JOIN (SELECT phone_normalized, array_agg(hcp_customer_id) as ids FROM hcp_customers WHERE customer_id = $1 GROUP BY phone_normalized) pg ON i.hcp_customer_id = ANY(pg.ids) WHERE pg.phone_normalized = l.contact_phone_normalized AND i.record_status = 'active' AND (i.status IN ('complete rated','complete unrated') OR i.inferred_complete)) THEN 'Inspection Complete'
          WHEN EXISTS (SELECT 1 FROM hcp_inspections i JOIN (SELECT phone_normalized, array_agg(hcp_customer_id) as ids FROM hcp_customers WHERE customer_id = $1 GROUP BY phone_normalized) pg ON i.hcp_customer_id = ANY(pg.ids) WHERE pg.phone_normalized = l.contact_phone_normalized AND i.record_status = 'active') THEN 'Inspection Scheduled'
          WHEN l.hcp_customer_id IS NOT NULL THEN 'In CRM'
          ELSE NULL
        END as stage
      FROM lsa_leads l
      WHERE l.customer_id = $1
        AND l.lead_creation_time::date BETWEEN $2::date AND $3::date
      ORDER BY l.lead_creation_time DESC
      LIMIT $4
    `, [customerId, startDate, endDate, limit]);
    return lsaRows.rows;
  }

  const { rows } = await pool.query(`
    WITH call_leads AS (
      SELECT
        c.start_time as contact_date,
        c.customer_name as name,
        normalize_phone(c.caller_phone) as phone,
        'call' as type,
        c.duration,
        COALESCE(c.ai_answered, CASE WHEN c.answered THEN 'answered' ELSE 'missed' END) as answer_status,
        c.source_name,
        c.callrail_id
      FROM calls c
      WHERE c.customer_id IN (SELECT customer_id FROM clients WHERE customer_id = $1 OR parent_customer_id = $1)
        AND c.start_time::date BETWEEN $2::date AND $3::date
        ${sourceWhere}
      ORDER BY c.start_time DESC
    ),
    form_leads AS (
      SELECT
        f.submitted_at as contact_date,
        COALESCE(f.customer_name, f.customer_email) as name,
        normalize_phone(f.customer_phone) as phone,
        'form' as type,
        NULL::int as duration,
        'form' as answer_status,
        f.source as source_name,
        f.callrail_id
      FROM form_submissions f
      WHERE f.customer_id IN (SELECT customer_id FROM clients WHERE customer_id = $1 OR parent_customer_id = $1)
        AND f.submitted_at::date BETWEEN $2::date AND $3::date
        ${formSourceWhere}
        AND COALESCE(f.is_spam, false) = false
      ORDER BY f.submitted_at DESC
    )
    SELECT * FROM call_leads
    UNION ALL
    SELECT * FROM form_leads
    ORDER BY contact_date DESC
    LIMIT $4
  `, [customerId, startDate, endDate, limit]);
  return rows;
});


// ============================================================
// Analytics — Call Analytics
// ============================================================

fastify.get('/clients/:customerId/call-analytics', async (request) => {
  const { customerId } = request.params;
  const endDate = request.query.date_to || new Date().toISOString().split('T')[0];
  const startDate = request.query.date_from || new Date(Date.now() - 30 * 86400000).toISOString().split('T')[0];

  // Calculate previous period (same length, shifted back)
  const start = new Date(startDate);
  const end = new Date(endDate);
  const diffMs = end - start;
  const prevEnd = new Date(start.getTime() - 86400000); // day before current start
  const prevStart = new Date(prevEnd.getTime() - diffMs);
  const prevStartStr = prevStart.toISOString().split('T')[0];
  const prevEndStr = prevEnd.toISOString().split('T')[0];

  // Get client info (timezone, biz hours)
  const clientInfo = await pool.query(`
    SELECT
      COALESCE(timezone, 'America/Denver') as timezone,
      COALESCE(biz_hours_start, '08:00') as biz_hours_start,
      COALESCE(biz_hours_end, '18:00') as biz_hours_end
    FROM clients
    WHERE customer_id = $1
    LIMIT 1
  `, [customerId]);

  const tz = clientInfo.rows[0]?.timezone || 'America/Denver';
  const bizStart = clientInfo.rows[0]?.biz_hours_start || '08:00';
  const bizEnd = clientInfo.rows[0]?.biz_hours_end || '18:00';
  const bizStartHour = parseInt(bizStart.split(':')[0], 10);
  const bizEndHour = parseInt(bizEnd.split(':')[0], 10);

  const clientIdsCTE = `
    WITH client_ids AS (
      SELECT customer_id FROM clients WHERE customer_id = $1 OR parent_customer_id = $1
    )
  `;

  // Run all queries concurrently
  const [summaryRes, prevRes, donutAdsRes, donutOverallRes, hourlyRes, attemptRes, missedTableRes] = await Promise.all([
    // Current period summary
    pool.query(`
      ${clientIdsCTE}
      SELECT
        COUNT(*)::int as total_calls,
        COUNT(*) FILTER (WHERE first_call = true)::int as first_time_calls,
        COUNT(*) FILTER (WHERE classified_status = 'missed')::int as missed_calls,
        COUNT(*) FILTER (WHERE classified_status = 'missed' AND classified_period = 'business_hours')::int as missed_biz_hours,
        COUNT(*) FILTER (WHERE classified_status = 'abandoned')::int as abandoned_calls,
        COUNT(*) FILTER (WHERE classified_status = 'answered')::int as answered_calls,
        COALESCE(AVG(duration) FILTER (WHERE classified_status = 'answered'), 0)::int as avg_duration
      FROM calls
      WHERE customer_id IN (SELECT customer_id FROM client_ids)
        AND start_time::date BETWEEN $2::date AND $3::date
    `, [customerId, startDate, endDate]),

    // Previous period summary (for trends)
    pool.query(`
      ${clientIdsCTE}
      SELECT
        COUNT(*)::int as total_calls,
        COUNT(*) FILTER (WHERE first_call = true)::int as first_time_calls,
        COUNT(*) FILTER (WHERE classified_status = 'missed')::int as missed_calls,
        COUNT(*) FILTER (WHERE classified_status = 'missed' AND classified_period = 'business_hours')::int as missed_biz_hours,
        COUNT(*) FILTER (WHERE classified_status = 'answered')::int as answered_calls
      FROM calls
      WHERE customer_id IN (SELECT customer_id FROM client_ids)
        AND start_time::date BETWEEN $2::date AND $3::date
    `, [customerId, prevStartStr, prevEndStr]),

    // Donut: Google Ads (biz hours, first-time only)
    pool.query(`
      ${clientIdsCTE}
      SELECT
        COUNT(*) FILTER (WHERE classified_status = 'answered')::int as answered,
        COUNT(*) FILTER (WHERE classified_status = 'missed')::int as missed,
        COUNT(*) FILTER (WHERE classified_status = 'abandoned')::int as abandoned,
        COUNT(*)::int as total
      FROM calls
      WHERE customer_id IN (SELECT customer_id FROM client_ids)
        AND start_time::date BETWEEN $2::date AND $3::date
        AND classified_source = 'google_ads'
        AND classified_period = 'business_hours'
        AND first_call = true
    `, [customerId, startDate, endDate]),

    // Donut: Overall (biz hours only)
    pool.query(`
      ${clientIdsCTE}
      SELECT
        COUNT(*) FILTER (WHERE classified_status = 'answered')::int as answered,
        COUNT(*) FILTER (WHERE classified_status = 'missed')::int as missed,
        COUNT(*) FILTER (WHERE classified_status = 'abandoned')::int as abandoned,
        COUNT(*)::int as total
      FROM calls
      WHERE customer_id IN (SELECT customer_id FROM client_ids)
        AND start_time::date BETWEEN $2::date AND $3::date
        AND classified_period = 'business_hours'
    `, [customerId, startDate, endDate]),

    // Hourly missed calls (in client timezone)
    pool.query(`
      ${clientIdsCTE}
      SELECT
        EXTRACT(HOUR FROM start_time AT TIME ZONE $4)::int as hour,
        COUNT(*)::int as cnt
      FROM calls
      WHERE customer_id IN (SELECT customer_id FROM client_ids)
        AND start_time::date BETWEEN $2::date AND $3::date
        AND classified_status IN ('missed', 'abandoned')
      GROUP BY 1
    `, [customerId, startDate, endDate, tz]),

    // Missed by attempt (funnel: 1st, 2nd, 3rd missed)
    pool.query(`
      ${clientIdsCTE},
      caller_attempts AS (
        SELECT
          caller_phone,
          classified_status,
          ROW_NUMBER() OVER (PARTITION BY caller_phone ORDER BY start_time) as attempt_num
        FROM calls
        WHERE customer_id IN (SELECT customer_id FROM client_ids)
          AND start_time::date BETWEEN $2::date AND $3::date
          AND classified_status != 'abandoned'
          AND caller_phone IS NOT NULL
      )
      SELECT
        COUNT(DISTINCT caller_phone) FILTER (
          WHERE attempt_num = 1 AND classified_status = 'missed'
        )::int as first_missed,
        COUNT(DISTINCT caller_phone) FILTER (
          WHERE attempt_num = 2 AND classified_status = 'missed'
          AND caller_phone IN (SELECT cp FROM (SELECT caller_phone as cp FROM caller_attempts WHERE attempt_num = 1 AND classified_status = 'missed') x)
        )::int as second_missed,
        COUNT(DISTINCT caller_phone) FILTER (
          WHERE attempt_num = 3 AND classified_status = 'missed'
          AND caller_phone IN (SELECT cp FROM (SELECT caller_phone as cp FROM caller_attempts WHERE attempt_num = 1 AND classified_status = 'missed') x)
          AND caller_phone IN (SELECT cp FROM (SELECT caller_phone as cp FROM caller_attempts WHERE attempt_num = 2 AND classified_status = 'missed') y)
        )::int as third_missed
      FROM caller_attempts
    `, [customerId, startDate, endDate]),

    // Missed calls table
    pool.query(`
      ${clientIdsCTE}
      SELECT
        caller_phone,
        start_time,
        customer_name,
        source_name,
        classified_source,
        classified_period,
        first_call,
        duration,
        classified_status
      FROM calls
      WHERE customer_id IN (SELECT customer_id FROM client_ids)
        AND start_time::date BETWEEN $2::date AND $3::date
        AND classified_status IN ('missed', 'abandoned')
      ORDER BY start_time DESC
      LIMIT 50
    `, [customerId, startDate, endDate])
  ]);

  const s = summaryRes.rows[0];
  const p = prevRes.rows[0];

  const answerDenom = s.answered_calls + s.missed_calls;
  const answerRate = answerDenom > 0 ? Math.round((s.answered_calls / answerDenom) * 1000) / 10 : 0;

  const prevAnswerDenom = p.answered_calls + p.missed_calls;
  const answerRatePrev = prevAnswerDenom > 0 ? Math.round((p.answered_calls / prevAnswerDenom) * 1000) / 10 : 0;

  // Build 24-element hourly array
  const hourlyMissed = new Array(24).fill(0);
  for (const row of hourlyRes.rows) {
    hourlyMissed[row.hour] = row.cnt;
  }

  const donutAds = donutAdsRes.rows[0];
  const donutAll = donutOverallRes.rows[0];

  return {
    summary: {
      total_calls: s.total_calls,
      first_time_calls: s.first_time_calls,
      missed_calls: s.missed_calls,
      missed_biz_hours: s.missed_biz_hours,
      abandoned_calls: s.abandoned_calls,
      answered_calls: s.answered_calls,
      avg_duration: s.avg_duration,
      answer_rate: answerRate
    },
    trends: {
      total_calls_prev: p.total_calls,
      first_time_calls_prev: p.first_time_calls,
      missed_calls_prev: p.missed_calls,
      missed_biz_hours_prev: p.missed_biz_hours,
      answered_calls_prev: p.answered_calls,
      answer_rate_prev: answerRatePrev
    },
    donut_google_ads: {
      answered: donutAds.answered,
      missed: donutAds.missed,
      abandoned: donutAds.abandoned,
      total: donutAds.total,
      label: 'Google Ads'
    },
    donut_overall: {
      answered: donutAll.answered,
      missed: donutAll.missed,
      abandoned: donutAll.abandoned,
      total: donutAll.total,
      label: 'Overall'
    },
    hourly_missed: hourlyMissed,
    biz_hours: { start: bizStartHour, end: bizEndHour },
    missed_by_attempt: {
      first: attemptRes.rows[0]?.first_missed || 0,
      second: attemptRes.rows[0]?.second_missed || 0,
      third: attemptRes.rows[0]?.third_missed || 0
    },
    missed_calls_table: missedTableRes.rows
  };
});
// ============================================================
// Analytics — Recent Activity
// ============================================================

fastify.get('/clients/:customerId/recent-activity', async (request) => {
  const { customerId } = request.params;
  const { limit = 10 } = request.query;

  const { rows } = await pool.query(`
    WITH activity AS (
      -- Recent jobs completed
      SELECT 'job_completed' as event_type,
        hc.first_name || ' ' || hc.last_name as customer_name,
        j.completed_at as event_date,
        j.total_amount_cents / 100.0 as amount,
        'Google Ads' as source
      FROM hcp_jobs j
      JOIN hcp_customers hc ON hc.hcp_customer_id = j.hcp_customer_id
      WHERE j.customer_id = $1
        AND j.status IN ('complete rated', 'complete unrated')
        AND j.record_status = 'active'
        AND j.completed_at IS NOT NULL
        AND EXISTS (SELECT 1 FROM calls c WHERE normalize_phone(c.caller_phone) = hc.phone_normalized
          AND c.customer_id = $1 AND is_google_ads_call(c.source, c.source_name, c.gclid))

      UNION ALL

      -- Recent estimates approved
      SELECT 'estimate_approved',
        hc.first_name || ' ' || hc.last_name,
        eg.approved_at,
        eg.approved_total_cents / 100.0,
        'Google Ads'
      FROM v_estimate_groups eg
      JOIN hcp_customers hc ON hc.hcp_customer_id = eg.hcp_customer_id
      WHERE eg.customer_id = $1
        AND eg.status = 'approved'
        AND eg.count_revenue
        AND EXISTS (SELECT 1 FROM calls c WHERE normalize_phone(c.caller_phone) = hc.phone_normalized
          AND c.customer_id = $1 AND is_google_ads_call(c.source, c.source_name, c.gclid))

      UNION ALL

      -- Recent inspections
      SELECT 'inspection',
        hc.first_name || ' ' || hc.last_name,
        COALESCE(i.completed_at, i.scheduled_at),
        i.total_amount_cents / 100.0,
        'Google Ads'
      FROM hcp_inspections i
      JOIN hcp_customers hc ON hc.hcp_customer_id = i.hcp_customer_id
      WHERE i.customer_id = $1
        AND i.record_status = 'active'
        AND i.status IN ('complete rated', 'complete unrated', 'scheduled', 'in progress')
        AND EXISTS (SELECT 1 FROM calls c WHERE normalize_phone(c.caller_phone) = hc.phone_normalized
          AND c.customer_id = $1 AND is_google_ads_call(c.source, c.source_name, c.gclid))
    )
    SELECT * FROM activity
    ORDER BY event_date DESC NULLS LAST
    LIMIT $2
  `, [customerId, limit]);
  return rows;
});

// ============================================================
// Analytics — Lead Spreadsheet (full funnel journey per lead)
// ============================================================

fastify.get('/clients/:customerId/lead-spreadsheet', async (request) => {
  const { customerId } = request.params;
  const { source = 'google_ads', date_from, date_to } = request.query;

  const endDate = date_to || new Date().toISOString().split('T')[0];
  const startDate = date_from || new Date(Date.now() - 30 * 86400000).toISOString().split('T')[0];

  // Check CRM type
  const clientResult = await pool.query(
    `SELECT field_management_software, spreadsheet_id, extra_spam_keywords FROM clients WHERE customer_id = $1`, [customerId]
  );
  const fms = clientResult.rows[0]?.field_management_software || 'housecall_pro';


  // Spreadsheet clients: return from spreadsheet_leads table
  if (clientResult.rows[0]?.spreadsheet_id) {
    let slSourceWhere = '';
    if (source === 'google_ads') slSourceWhere = `AND sl.source ILIKE '%google ads%'`;
    else if (source === 'gbp') slSourceWhere = `AND (sl.source ILIKE '%google my business%' OR sl.source ILIKE '%gbp%')`;

    const { rows: slRows } = await pool.query(`
      SELECT
        sl.ghl_contact_id as hcp_customer_id,
        COALESCE(NULLIF(TRIM(COALESCE(sl.first_name,'') || ' ' || COALESCE(sl.last_name,'')), ''), 'Unknown') as name,
        sl.phone_normalized as phone,
        sl.date_created as contact_date,
        'matched' as match_status,
        'call' as lead_type,
        NULL as answer_status,
        NULL::int as duration,
        sl.inspection_scheduled,
        sl.inspection_completed,
        (COALESCE(array_length(sl.inferred_stages, 1), 0) > 0
          AND 'inspection_completed' = ANY(sl.inferred_stages)) as inspection_completed_inferred,
        sl.estimate_sent,
        sl.estimate_approved,
        sl.job_scheduled,
        sl.job_completed,
        (sl.roas_revenue_cents > 0) as revenue_closed,
        sl.estimate_approved_cents / 100.0 as approved_revenue,
        sl.job_completed_cents / 100.0 as invoiced_revenue,
        '[]'::json as invoice_breakdown,
        sl.estimate_sent_cents / 100.0 as estimate_value,
        NULL as job_description,
        NULL as service_address,
        NULL as client_flag_reason,
        NULL as client_flag_at,
        sl.lost_reason,
        sl.roas_revenue_cents / 100.0 as roas_revenue,
        sl.estimate_open_cents / 100.0 as open_estimate_value,
        COALESCE(sl.source, 'Other') as source_label,
        (COALESCE(array_length(sl.inferred_stages, 1), 0) > 0) as inferred
      FROM spreadsheet_leads sl
      WHERE sl.customer_id = $1
        AND sl.date_created BETWEEN $2::date AND $3::date
        ${slSourceWhere}
        AND sl.is_quality_lead
      ORDER BY sl.date_created DESC
    `, [customerId, startDate, endDate]);
    return slRows;
  }

  if (fms === 'jobber') {
    return await getJobberLeadSpreadsheet(pool, customerId, startDate, endDate, source);
  }

  if (fms === 'ghl') {
    return await getGhlLeadSpreadsheet(pool, customerId, startDate, endDate, source);
  }

  // Source-specific attribution filter
  let attributionWhere = '';
  let unmatchedSourceWhere = '';
  if (source === 'gbp') {
    attributionWhere = `AND (
      hc.attribution_override = 'gbp'
      OR EXISTS (SELECT 1 FROM calls ca WHERE ca.callrail_id = hc.callrail_id AND ca.source = 'Google My Business')
    )`;
    unmatchedSourceWhere = `AND c.source = 'Google My Business' AND NOT is_google_ads_call(c.source, c.source_name, c.gclid)`;
  } else if (source === 'lsa') {
    attributionWhere = `AND (
      hc.attribution_override = 'lsa'
      OR EXISTS (SELECT 1 FROM calls ca WHERE ca.callrail_id = hc.callrail_id AND ca.source_name = 'LSA')
      OR EXISTS (SELECT 1 FROM lsa_leads l WHERE l.hcp_customer_id = hc.hcp_customer_id AND l.customer_id = hc.customer_id)
    )`;
    unmatchedSourceWhere = `AND c.source_name = 'LSA'`;
  } else {
    // Default: Google Ads
    attributionWhere = `AND (
      hc.attribution_override = 'google_ads'
      OR hc.callrail_id LIKE 'WF_%'
      OR EXISTS (SELECT 1 FROM calls ca WHERE ca.callrail_id = hc.callrail_id
        AND is_google_ads_call(ca.source, ca.source_name, ca.gclid) AND COALESCE(ca.source_name,'') <> 'LSA')
      OR EXISTS (SELECT 1 FROM form_submissions fs WHERE fs.customer_id = hc.customer_id
        AND (fs.callrail_id = hc.callrail_id OR fs.phone_normalized = hc.phone_normalized
          OR (hc.email IS NOT NULL AND LOWER(fs.customer_email) = LOWER(hc.email)))
        AND fs.gclid IS NOT NULL AND fs.gclid != '')
      OR EXISTS (SELECT 1 FROM ghl_contacts gc WHERE gc.customer_id = hc.customer_id
        AND (gc.phone_normalized = hc.phone_normalized OR (hc.email IS NOT NULL AND hc.email != '' AND LOWER(gc.email) = LOWER(hc.email)))
        AND gc.gclid IS NOT NULL AND gc.gclid != '')
    )`;
    unmatchedSourceWhere = `AND is_google_ads_call(c.source, c.source_name, c.gclid)`;
  }

  // Map source to mv_funnel_leads.lead_source (same as getHcpFunnel)
  let mvSourceWhere = '';
  if (source === 'gbp') mvSourceWhere = "AND fl.lead_source = 'gbp'";
  else if (source === 'lsa') mvSourceWhere = "AND fl.lead_source = 'lsa'";
  else if (source === 'google_ads') mvSourceWhere = "AND fl.lead_source = 'google_ads'";

  // Default: HCP lead spreadsheet
  const { rows } = await pool.query(`
    WITH client_ids AS (
      SELECT customer_id FROM clients WHERE customer_id = $1 OR parent_customer_id = $1
    ),
    phone_groups AS (
      SELECT phone_normalized, array_agg(hcp_customer_id) as all_ids
      FROM hcp_customers WHERE customer_id = $1
      GROUP BY phone_normalized
    ),
    source_callrail_ids AS (
      SELECT DISTINCT c.callrail_id FROM calls c
      WHERE c.customer_id IN (SELECT customer_id FROM client_ids)
        ${unmatchedSourceWhere}
        AND c.callrail_id IS NOT NULL
    ),
    -- All GA-attributed HCP customers in period with funnel data
    hcp_leads AS (
      SELECT
        hc.hcp_customer_id,
        COALESCE(
          NULLIF(TRIM(COALESCE(hc.first_name, '') || ' ' || COALESCE(hc.last_name, '')), ''),
          (SELECT c.customer_name FROM calls c WHERE c.callrail_id = hc.callrail_id LIMIT 1),
          'Unknown'
        ) as name,
        hc.phone_normalized as phone,
        hc.hcp_created_at as contact_date,
        'matched' as match_status,
        -- Call/form info
        CASE
          WHEN hc.callrail_id LIKE 'WF_%' THEN 'webflow'
          WHEN hc.callrail_id LIKE 'FRM%' THEN 'form'
          ELSE 'call'
        END as lead_type,
        (SELECT COALESCE(ca.ai_answered, CASE WHEN ca.answered THEN 'answered' ELSE 'missed' END)
         FROM calls ca WHERE ca.callrail_id = hc.callrail_id LIMIT 1) as answer_status,
        (SELECT ca.duration FROM calls ca WHERE ca.callrail_id = hc.callrail_id LIMIT 1) as duration,
        -- Funnel stages
        EXISTS (SELECT 1 FROM hcp_inspections i WHERE i.hcp_customer_id = ANY(pg.all_ids)
          AND i.record_status = 'active' AND (i.status IN ('scheduled','complete rated','complete unrated','in progress') OR i.scheduled_at IS NOT NULL OR i.inferred_complete = true)) as inspection_scheduled,
        EXISTS (SELECT 1 FROM hcp_inspections i WHERE i.hcp_customer_id = ANY(pg.all_ids)
          AND i.record_status = 'active' AND (i.status IN ('complete rated','complete unrated') OR i.inferred_complete = true)) as inspection_completed,
        -- Inferred flags
        EXISTS (SELECT 1 FROM hcp_inspections i WHERE i.hcp_customer_id = ANY(pg.all_ids)
          AND i.record_status = 'active' AND i.inferred_complete = true
          AND i.status NOT IN ('complete rated','complete unrated')) as inspection_completed_inferred,
        EXISTS (SELECT 1 FROM v_estimate_groups eg WHERE eg.hcp_customer_id = ANY(pg.all_ids)
          AND eg.status IN ('sent','approved','declined') AND eg.count_revenue AND eg.estimate_type = 'treatment') as estimate_sent,
        EXISTS (SELECT 1 FROM v_estimate_groups eg WHERE eg.hcp_customer_id = ANY(pg.all_ids)
          AND eg.status = 'approved' AND eg.count_revenue AND eg.estimate_type = 'treatment' AND eg.approved_total_cents >= 100000) as estimate_approved,
        EXISTS (SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = ANY(pg.all_ids)
          AND j.record_status = 'active' AND j.status IN ('scheduled','complete rated','complete unrated','in progress')
          AND NOT (
            -- Priority 0: inspection-priority phrases on small jobs (< $1K)
            -- Large jobs mentioning testing as a line item are still treatment
            LOWER(COALESCE(j.description,'')) SIMILAR TO '%(pre.treatment|pre treatment|air quality test|air test|mold test|visual assessment|complimentary estimate)%'
            AND j.total_amount_cents < 100000
          )
          AND NOT (
            -- Priority 2: inspection keywords, unless Priority 1 treatment keywords present
            LOWER(COALESCE(j.description,'')) SIMILAR TO '%(assessment|inspection|test|evaluat|consult|survey|sample|estimate|sampling|walk.through|instascope|scan|moisture check|mold report|clearance|ermi)%'
            AND LOWER(COALESCE(j.description,'')) NOT SIMILAR TO '%(remediation|dry fog|treatment|removal|abatement|encapsulation)%'
          )
          AND (
            (LOWER(COALESCE(j.description,'')) SIMILAR TO '%(remediation|dry fog|treatment|removal|abatement|encapsulation|instapure|everpure|demolition)%' AND j.total_amount_cents >= 10000)
            OR
            (LOWER(COALESCE(j.description,'')) NOT SIMILAR TO '%(remediation|dry fog|treatment|removal|abatement|encapsulation|instapure|everpure|demolition)%' AND j.total_amount_cents >= 100000)
          )) as job_scheduled,
        EXISTS (SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = ANY(pg.all_ids)
          AND j.record_status = 'active' AND j.status IN ('complete rated','complete unrated')
          AND NOT (
            -- Priority 0: inspection-priority phrases on small jobs (< $1K)
            -- Large jobs mentioning testing as a line item are still treatment
            LOWER(COALESCE(j.description,'')) SIMILAR TO '%(pre.treatment|pre treatment|air quality test|air test|mold test|visual assessment|complimentary estimate)%'
            AND j.total_amount_cents < 100000
          )
          AND NOT (
            -- Priority 2: inspection keywords, unless Priority 1 treatment keywords present
            LOWER(COALESCE(j.description,'')) SIMILAR TO '%(assessment|inspection|test|evaluat|consult|survey|sample|estimate|sampling|walk.through|instascope|scan|moisture check|mold report|clearance|ermi)%'
            AND LOWER(COALESCE(j.description,'')) NOT SIMILAR TO '%(remediation|dry fog|treatment|removal|abatement|encapsulation)%'
          )
          AND (
            (LOWER(COALESCE(j.description,'')) SIMILAR TO '%(remediation|dry fog|treatment|removal|abatement|encapsulation|instapure|everpure|demolition)%' AND j.total_amount_cents >= 10000)
            OR
            (LOWER(COALESCE(j.description,'')) NOT SIMILAR TO '%(remediation|dry fog|treatment|removal|abatement|encapsulation|instapure|everpure|demolition)%' AND j.total_amount_cents >= 100000)
          )) as job_completed,
        (EXISTS (SELECT 1 FROM hcp_invoices inv WHERE inv.hcp_customer_id = ANY(pg.all_ids) AND inv.status NOT IN ('canceled','voided') AND inv.amount_cents > 0)
        OR EXISTS (SELECT 1 FROM v_estimate_groups eg WHERE eg.hcp_customer_id = ANY(pg.all_ids)
          AND eg.status = 'approved' AND eg.count_revenue AND eg.estimate_type = 'treatment' AND eg.approved_total_cents >= 100000)) as revenue_closed,
        -- Revenue
        COALESCE((SELECT SUM(eg.approved_total_cents) FROM v_estimate_groups eg
          WHERE eg.hcp_customer_id = ANY(pg.all_ids) AND eg.status = 'approved' AND eg.count_revenue AND eg.estimate_type = 'treatment' AND eg.approved_total_cents >= 100000), 0) / 100.0 as approved_revenue,
        COALESCE((SELECT SUM(i.amount_cents) FROM hcp_invoices i
          WHERE i.hcp_customer_id = ANY(pg.all_ids) AND i.status NOT IN ('canceled','voided') AND i.amount_cents > 0), 0) / 100.0 as invoiced_revenue,
        COALESCE((SELECT json_agg(json_build_object('amount', i.amount_cents / 100.0, 'type', i.invoice_type, 'status', i.status) ORDER BY i.amount_cents) FROM hcp_invoices i
          WHERE i.hcp_customer_id = ANY(pg.all_ids) AND i.status NOT IN ('canceled','voided') AND i.amount_cents > 0), '[]'::json) as invoice_breakdown,
        -- Estimate pipeline value (sent but not yet approved)
        COALESCE((SELECT SUM(eg.highest_option_cents) FROM v_estimate_groups eg
          WHERE eg.hcp_customer_id = ANY(pg.all_ids) AND eg.status IN ('sent','approved','declined') AND eg.count_revenue AND eg.estimate_type = 'treatment'), 0) / 100.0 as estimate_value,
        -- Enrichment: job description + service address
        (SELECT j.description FROM hcp_jobs j WHERE j.hcp_customer_id = ANY(pg.all_ids)
          AND j.record_status = 'active' ORDER BY j.scheduled_at DESC NULLS LAST LIMIT 1) as job_description,
        (SELECT i.service_address FROM hcp_inspections i WHERE i.hcp_customer_id = ANY(pg.all_ids)
          AND i.record_status = 'active' AND i.service_address IS NOT NULL
          ORDER BY i.scheduled_at DESC NULLS LAST LIMIT 1) as service_address,
        -- Client flag status
        hc.client_flag_reason,
        hc.client_flag_at,
        -- GHL lost reason
        (SELECT gc.lost_reason FROM ghl_contacts gc
          WHERE gc.phone_normalized = hc.phone_normalized
            AND gc.customer_id IN (SELECT customer_id FROM client_ids)
            AND gc.lost_reason IS NOT NULL
          LIMIT 1) as lost_reason
      FROM hcp_customers hc
      JOIN phone_groups pg ON pg.phone_normalized = hc.phone_normalized
      WHERE hc.customer_id IN (SELECT customer_id FROM client_ids)
        AND hc.hcp_created_at::date BETWEEN $2::date AND $3::date
        ${attributionWhere}
        AND COALESCE(hc.client_flag_reason, '') NOT IN ('spam', 'out_of_area', 'wrong_service')
        AND NOT EXISTS (
          SELECT 1 FROM ghl_contacts gc
          WHERE gc.phone_normalized = hc.phone_normalized AND gc.customer_id = hc.customer_id
            AND LOWER(gc.lost_reason) SIMILAR TO '%(spam|not a lead|wrong number|out of area|wrong service)%'
        )
    ),
    -- Unmatched calls (no HCP record)
    unmatched AS (
      SELECT
        NULL as hcp_customer_id,
        COALESCE(NULLIF(c.customer_name, ''), 'Caller ID: ' || normalize_phone(c.caller_phone)) as name,
        normalize_phone(c.caller_phone) as phone,
        c.start_time as contact_date,
        'unmatched' as match_status,
        'call' as lead_type,
        COALESCE(c.ai_answered, CASE WHEN c.answered THEN 'answered' ELSE 'missed' END) as answer_status,
        c.duration,
        false as inspection_scheduled,
        false as inspection_completed,
        false as inspection_completed_inferred,
        false as estimate_sent,
        false as estimate_approved,
        false as job_scheduled,
        false as job_completed,
        false as revenue_closed,
        0::numeric as approved_revenue,
        0::numeric as invoiced_revenue,
        '[]'::json as invoice_breakdown,
        0::numeric as estimate_value,
        NULL as job_description,
        NULL as service_address,
        (SELECT cf.flag_reason FROM client_flagged_leads cf
          WHERE cf.customer_id IN (SELECT customer_id FROM client_ids)
            AND cf.phone_normalized = normalize_phone(c.caller_phone)
          LIMIT 1) as client_flag_reason,
        (SELECT cf.flagged_at FROM client_flagged_leads cf
          WHERE cf.customer_id IN (SELECT customer_id FROM client_ids)
            AND cf.phone_normalized = normalize_phone(c.caller_phone)
          LIMIT 1) as client_flag_at,
        (SELECT gc.lost_reason FROM ghl_contacts gc
          WHERE gc.phone_normalized = normalize_phone(c.caller_phone)
            AND gc.customer_id IN (SELECT customer_id FROM client_ids)
            AND gc.lost_reason IS NOT NULL
          LIMIT 1) as lost_reason
      FROM calls c
      WHERE c.customer_id IN (SELECT customer_id FROM client_ids)
        AND c.start_time::date BETWEEN $2::date AND $3::date
        ${unmatchedSourceWhere}
        AND NOT EXISTS (SELECT 1 FROM hcp_leads hl WHERE hl.phone = normalize_phone(c.caller_phone))
        AND c.first_call = true
    ),
    -- Unmatched form submissions (no HCP record, no matching call)
    unmatched_forms AS (
      SELECT DISTINCT ON (normalize_phone(f.customer_phone))
        NULL as hcp_customer_id,
        COALESCE(NULLIF(TRIM(f.customer_name), ''), 'Form: ' || normalize_phone(f.customer_phone)) as name,
        normalize_phone(f.customer_phone) as phone,
        f.submitted_at as contact_date,
        'unmatched' as match_status,
        'form' as lead_type,
        NULL as answer_status,
        NULL::int as duration,
        false as inspection_scheduled,
        false as inspection_completed,
        false as inspection_completed_inferred,
        false as estimate_sent,
        false as estimate_approved,
        false as job_scheduled,
        false as job_completed,
        false as revenue_closed,
        0::numeric as approved_revenue,
        0::numeric as invoiced_revenue,
        '[]'::json as invoice_breakdown,
        0::numeric as estimate_value,
        NULL as job_description,
        NULL as service_address,
        (SELECT cf.flag_reason FROM client_flagged_leads cf
          WHERE cf.customer_id IN (SELECT customer_id FROM client_ids)
            AND cf.phone_normalized = normalize_phone(f.customer_phone)
          LIMIT 1) as client_flag_reason,
        (SELECT cf.flagged_at FROM client_flagged_leads cf
          WHERE cf.customer_id IN (SELECT customer_id FROM client_ids)
            AND cf.phone_normalized = normalize_phone(f.customer_phone)
          LIMIT 1) as client_flag_at,
        (SELECT gc.lost_reason FROM ghl_contacts gc
          WHERE gc.phone_normalized = normalize_phone(f.customer_phone)
            AND gc.customer_id IN (SELECT customer_id FROM client_ids)
            AND gc.lost_reason IS NOT NULL
          LIMIT 1) as lost_reason
      FROM form_submissions f
      WHERE f.customer_id IN (SELECT customer_id FROM client_ids)
        AND f.submitted_at::date BETWEEN $2::date AND $3::date
        AND f.gclid IS NOT NULL AND f.gclid != ''
        AND normalize_phone(f.customer_phone) IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM hcp_leads hl WHERE hl.phone = normalize_phone(f.customer_phone))
        AND NOT EXISTS (SELECT 1 FROM unmatched u WHERE u.phone = normalize_phone(f.customer_phone))
      ORDER BY normalize_phone(f.customer_phone), f.submitted_at DESC
    ),
    -- GHL contacts with GCLIDs not captured by CallRail (Webflow forms, direct GHL submissions)
    unmatched_ghl AS (
      SELECT DISTINCT ON (gc.phone_normalized)
        NULL as hcp_customer_id,
        COALESCE(NULLIF(TRIM(COALESCE(gc.first_name,'') || ' ' || COALESCE(gc.last_name,'')), ''), 'Unknown') as name,
        gc.phone_normalized as phone,
        gc.date_added as contact_date,
        'unmatched' as match_status,
        'form' as lead_type,
        NULL as answer_status,
        NULL::int as duration,
        false as inspection_scheduled,
        false as inspection_completed,
        false as inspection_completed_inferred,
        false as estimate_sent,
        false as estimate_approved,
        false as job_scheduled,
        false as job_completed,
        false as revenue_closed,
        0::numeric as approved_revenue,
        0::numeric as invoiced_revenue,
        '[]'::json as invoice_breakdown,
        0::numeric as estimate_value,
        NULL as job_description,
        NULL as service_address,
        NULL as client_flag_reason,
        NULL::timestamptz as client_flag_at,
        gc.lost_reason
      FROM ghl_contacts gc
      WHERE gc.customer_id IN (SELECT customer_id FROM client_ids)
        AND gc.gclid IS NOT NULL AND gc.gclid != ''
        AND gc.phone_normalized IS NOT NULL AND gc.phone_normalized != ''
        AND gc.date_added::date BETWEEN $2::date AND $3::date
        AND NOT EXISTS (SELECT 1 FROM hcp_leads hl WHERE hl.phone = gc.phone_normalized)
        AND NOT EXISTS (SELECT 1 FROM unmatched u WHERE u.phone = gc.phone_normalized)
        AND NOT EXISTS (SELECT 1 FROM unmatched_forms uf WHERE uf.phone = gc.phone_normalized)
      ORDER BY gc.phone_normalized, gc.date_added DESC
    )
    SELECT l.*, COALESCE(fl.first_ga_touch_time IS NOT NULL
      AND fl.hcp_created_at < fl.first_ga_touch_time - INTERVAL '7 days'
      AND NOT COALESCE(fl.exclude_from_ga_roas, false), false) as reactivated
    FROM (
      SELECT * FROM hcp_leads
      UNION ALL
      SELECT * FROM unmatched
      UNION ALL
      SELECT * FROM unmatched_forms
      UNION ALL
      SELECT * FROM unmatched_ghl
    ) l
    LEFT JOIN mv_funnel_leads fl ON fl.hcp_customer_id = l.hcp_customer_id AND fl.customer_id = $1
    ORDER BY l.contact_date DESC
  `, [customerId, startDate, endDate]);
  // ====================================================================
  // CRITICAL: This post-filter ensures drawer count matches funnel count.
  // DO NOT remove or bypass. The drawer SQL and funnel SQL use different
  // query paths. This post-filter uses the funnel's quality_phones as
  // the authoritative phone list and fills any missing leads from fallback.
  // See: project_funnel_accuracy_session.md for full context.
  // ====================================================================
  // Post-filter: use getHcpFunnel's quality_phones as single source of truth
  const sourceWhere = source === 'google_ads' ? 'AND is_google_ads_call(c2.source, c2.source_name, c2.gclid)'
    : source === 'gbp' ? "AND c2.source = 'Google My Business' AND NOT is_google_ads_call(c2.source, c2.source_name, c2.gclid)"
    : source === 'lsa' ? "AND c2.source_name = 'LSA'" : '';
  const funnelDateWhere = `AND lead_date BETWEEN '${startDate}'::date AND '${endDate}'::date`;
  const funnelCidCTE = `WITH client_ids AS (SELECT customer_id FROM clients WHERE customer_id = ${customerId} OR parent_customer_id = ${customerId})`;
  const extraSpam = clientResult.rows[0]?.extra_spam_keywords || null;
  const funnelResult = await getHcpFunnel(pool, customerId, [customerId], funnelDateWhere, sourceWhere, funnelCidCTE, extraSpam);
  
  const qualityPhones = funnelResult.quality_phones || [];
  const qualityPhoneSet = new Set(qualityPhones);
  let filtered = rows.filter(r => r.phone && qualityPhoneSet.has(r.phone));
  
  // Fill missing phones from funnel that drawer SQL missed
  const drawerPhones = new Set(filtered.map(r => r.phone));
  const missingPhones = qualityPhones.filter(p => !drawerPhones.has(p));
  if (missingPhones.length > 0) {
    const { rows: missingRows } = await pool.query(`
      SELECT fl.hcp_customer_id,
        COALESCE(NULLIF(TRIM(COALESCE(fl.first_name,'') || ' ' || COALESCE(fl.last_name,'')), ''),
          COALESCE((SELECT c.customer_name FROM calls c WHERE c.customer_id = fl.customer_id AND normalize_phone(c.caller_phone) = fl.phone_normalized ORDER BY c.start_time DESC LIMIT 1),
            'Caller ID: ' || fl.phone_normalized)) as name,
        fl.phone_normalized as phone, fl.hcp_created_at as contact_date,
        'matched' as match_status, 'call' as lead_type, NULL as answer_status, NULL::int as duration,
        fl.has_inspection_scheduled as inspection_scheduled, fl.has_inspection_completed as inspection_completed,
        false as inspection_completed_inferred,
        fl.has_estimate_sent as estimate_sent, fl.has_estimate_approved as estimate_approved,
        fl.has_job_scheduled as job_scheduled, fl.has_job_completed as job_completed, fl.has_invoice as revenue_closed,
        fl.est_approved_cents / 100.0 as approved_revenue, (fl.insp_invoice_cents + fl.treat_invoice_cents) / 100.0 as invoiced_revenue,
        '[]'::json as invoice_breakdown, fl.est_sent_cents / 100.0 as estimate_value,
        NULL as job_description, NULL as service_address, fl.client_flag_reason, NULL::timestamptz as client_flag_at,
        NULL as lost_reason, false as reactivated
      FROM mv_funnel_leads fl
      WHERE fl.customer_id = $1 AND fl.phone_normalized = ANY($2) AND fl.lead_source = $3
      UNION ALL
      SELECT NULL, COALESCE(c.customer_name, 'Caller ID: ' || normalize_phone(c.caller_phone)),
        normalize_phone(c.caller_phone), c.start_time, 'unmatched', 'call',
        CASE WHEN c.answered THEN 'answered' ELSE 'missed' END, c.duration,
        false, false, false, false, false, false, false, false, 0, 0, '[]'::json, 0,
        NULL, NULL, NULL, NULL, NULL, false
      FROM calls c WHERE c.customer_id IN (SELECT customer_id FROM clients WHERE customer_id = $1 OR parent_customer_id = $1)
        AND normalize_phone(c.caller_phone) = ANY($2) AND c.first_call = true
        AND NOT EXISTS (SELECT 1 FROM mv_funnel_leads fl WHERE fl.customer_id = $1 AND fl.phone_normalized = normalize_phone(c.caller_phone))
      UNION ALL
      SELECT NULL, f.customer_name, normalize_phone(f.customer_phone), f.submitted_at, 'unmatched', 'form',
        NULL, NULL, false, false, false, false, false, false, false, false, 0, 0, '[]'::json, 0,
        NULL, NULL, NULL, NULL, NULL, false
      FROM form_submissions f WHERE f.customer_id IN (SELECT customer_id FROM clients WHERE customer_id = $1 OR parent_customer_id = $1)
        AND normalize_phone(f.customer_phone) = ANY($2)
        AND NOT EXISTS (SELECT 1 FROM mv_funnel_leads fl WHERE fl.customer_id = $1 AND fl.phone_normalized = normalize_phone(f.customer_phone))
        AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.customer_id IN (SELECT customer_id FROM clients WHERE customer_id = $1 OR parent_customer_id = $1)
          AND normalize_phone(c.caller_phone) = normalize_phone(f.customer_phone) AND c.first_call = true)
    `, [customerId, missingPhones, source === 'google_ads' ? 'google_ads' : source === 'gbp' ? 'gbp' : 'other']);
    const seen = new Set();
    for (const row of missingRows) {
      if (!seen.has(row.phone)) { seen.add(row.phone); filtered.push(row); }
    }
  }
  
  // Compute reactivated badge from mv_funnel_leads (cross-phone check)
  const { rows: reactivatedRows } = await pool.query(`
    SELECT DISTINCT fl.phone_normalized as phone
    FROM mv_funnel_leads fl
    WHERE fl.customer_id = $1 ${mvSourceWhere}
      AND fl.first_ga_touch_time IS NOT NULL
      AND fl.hcp_created_at < fl.first_ga_touch_time - INTERVAL '7 days'
      AND NOT COALESCE(fl.exclude_from_ga_roas, false)
  `, [customerId]);
  const reactivatedSet = new Set(reactivatedRows.map(r => r.phone));
  for (const lead of filtered) {
    if (lead.phone && reactivatedSet.has(lead.phone)) lead.reactivated = true;
  }
  
  filtered.sort((a, b) => new Date(b.contact_date) - new Date(a.contact_date));
  return filtered;
});

// Jobber lead spreadsheet helper
async function getJobberLeadSpreadsheet(pool, customerId, startDate, endDate, source) {
  let jcSourceWhere = '';
  let crSourceWhere = '';
  if (source === 'google_ads') {
    jcSourceWhere = `AND (
      jc.attribution_override = 'google_ads'
      OR jc.callrail_id LIKE 'WF_%'
      OR EXISTS (SELECT 1 FROM calls ca WHERE ca.callrail_id = jc.callrail_id AND is_google_ads_call(ca.source, ca.source_name, ca.gclid))
      OR EXISTS (SELECT 1 FROM calls ca WHERE normalize_phone(ca.caller_phone) = jc.phone_normalized
        AND ca.customer_id IN (SELECT customer_id FROM client_ids)
        AND is_google_ads_call(ca.source, ca.source_name, ca.gclid))
      OR EXISTS (SELECT 1 FROM form_submissions fs WHERE fs.customer_id IN (SELECT customer_id FROM client_ids)
        AND (fs.callrail_id = jc.callrail_id OR fs.phone_normalized = jc.phone_normalized)
        AND fs.gclid IS NOT NULL AND fs.gclid != '')
      OR EXISTS (SELECT 1 FROM ghl_contacts gc WHERE gc.customer_id = jc.customer_id
        AND (gc.phone_normalized = jc.phone_normalized OR (jc.email IS NOT NULL AND jc.email != '' AND LOWER(gc.email) = LOWER(jc.email)))
        AND gc.gclid IS NOT NULL AND gc.gclid != '')
    )`;
    crSourceWhere = `AND is_google_ads_call(c.source, c.source_name, c.gclid)`;
  }

  const { rows } = await pool.query(`
    WITH client_ids AS (
      SELECT customer_id FROM clients WHERE customer_id = $1 OR parent_customer_id = $1
    ),
    jobber_leads AS (
      SELECT
        jc.jobber_customer_id as hcp_customer_id,
        COALESCE(
          NULLIF(TRIM(COALESCE(jc.first_name, '') || ' ' || COALESCE(jc.last_name, '')), ''),
          jc.company_name,
          'Unknown'
        ) as name,
        jc.phone_normalized as phone,
        jc.jobber_created_at as contact_date,
        'matched' as match_status,
        CASE
          WHEN jc.callrail_id LIKE 'WF_%' THEN 'webflow'
          WHEN jc.callrail_id LIKE 'FRM%' THEN 'form'
          ELSE 'call'
        END as lead_type,
        (SELECT COALESCE(ca.ai_answered, CASE WHEN ca.answered THEN 'answered' ELSE 'missed' END)
         FROM calls ca WHERE ca.callrail_id = jc.callrail_id LIMIT 1) as answer_status,
        (SELECT ca.duration FROM calls ca WHERE ca.callrail_id = jc.callrail_id LIMIT 1) as duration,
        -- Inspection: request with assessment OR inspection-titled job
        GREATEST(
          COALESCE((SELECT COUNT(*) FROM jobber_requests jr WHERE jr.jobber_customer_id = jc.jobber_customer_id
            AND jr.has_assessment = true AND jr.assessment_start_at IS NOT NULL), 0),
          COALESCE((SELECT COUNT(*) FROM jobber_jobs jj WHERE jj.jobber_customer_id = jc.jobber_customer_id AND jj.customer_id = jc.customer_id
            AND (LOWER(jj.title) LIKE '%assessment%' OR LOWER(jj.title) LIKE '%instascope%' OR LOWER(jj.title) LIKE '%inspection%'
              OR LOWER(jj.title) LIKE '%mold test%' OR LOWER(jj.title) LIKE '%air quality%' OR LOWER(jj.title) LIKE '%air test%')), 0)
        ) > 0 as inspection_scheduled,
        GREATEST(
          COALESCE((SELECT COUNT(*) FROM jobber_requests jr WHERE jr.jobber_customer_id = jc.jobber_customer_id
            AND jr.assessment_completed_at IS NOT NULL), 0),
          COALESCE((SELECT COUNT(*) FROM jobber_jobs jj WHERE jj.jobber_customer_id = jc.jobber_customer_id AND jj.customer_id = jc.customer_id
            AND jj.status IN ('late', 'requires_invoicing')
            AND (LOWER(jj.title) LIKE '%assessment%' OR LOWER(jj.title) LIKE '%instascope%' OR LOWER(jj.title) LIKE '%inspection%'
              OR LOWER(jj.title) LIKE '%mold test%' OR LOWER(jj.title) LIKE '%air quality%' OR LOWER(jj.title) LIKE '%air test%')), 0)
        ) > 0 as inspection_completed,
        -- Inferred: has estimate but no explicit assessment completion
        (EXISTS (SELECT 1 FROM jobber_quotes q WHERE q.jobber_customer_id = jc.jobber_customer_id AND q.customer_id = jc.customer_id
          AND q.status IN ('awaiting_response','approved','converted','changes_requested'))
         AND NOT EXISTS (SELECT 1 FROM jobber_requests jr WHERE jr.jobber_customer_id = jc.jobber_customer_id AND jr.assessment_completed_at IS NOT NULL)
        ) as inspection_completed_inferred,
        EXISTS (SELECT 1 FROM jobber_quotes q WHERE q.jobber_customer_id = jc.jobber_customer_id AND q.customer_id = jc.customer_id
          AND q.status IN ('awaiting_response','approved','converted','changes_requested')) as estimate_sent,
        EXISTS (SELECT 1 FROM jobber_quotes q WHERE q.jobber_customer_id = jc.jobber_customer_id AND q.customer_id = jc.customer_id
          AND q.status IN ('approved','converted')) as estimate_approved,
        EXISTS (SELECT 1 FROM jobber_jobs j WHERE j.jobber_customer_id = jc.jobber_customer_id AND j.customer_id = jc.customer_id
          AND j.status NOT IN ('archived')
          AND NOT (LOWER(j.title) LIKE '%assessment%' OR LOWER(j.title) LIKE '%instascope%' OR LOWER(j.title) LIKE '%inspection%'
            OR LOWER(j.title) LIKE '%mold test%' OR LOWER(j.title) LIKE '%air quality%' OR LOWER(j.title) LIKE '%air test%')
        ) as job_scheduled,
        EXISTS (SELECT 1 FROM jobber_jobs j WHERE j.jobber_customer_id = jc.jobber_customer_id AND j.customer_id = jc.customer_id
          AND j.status IN ('late', 'requires_invoicing')
          AND NOT (LOWER(j.title) LIKE '%assessment%' OR LOWER(j.title) LIKE '%instascope%' OR LOWER(j.title) LIKE '%inspection%'
            OR LOWER(j.title) LIKE '%mold test%' OR LOWER(j.title) LIKE '%air quality%' OR LOWER(j.title) LIKE '%air test%')
        ) as job_completed,
        EXISTS (SELECT 1 FROM jobber_invoices i WHERE i.jobber_customer_id = jc.jobber_customer_id AND i.customer_id = jc.customer_id AND i.total_cents > 0) as revenue_closed,
        -- Revenue
        COALESCE((SELECT SUM(q.total_cents) FROM jobber_quotes q WHERE q.jobber_customer_id = jc.jobber_customer_id AND q.customer_id = jc.customer_id
          AND q.status IN ('approved','converted')), 0) / 100.0 as approved_revenue,
        COALESCE((SELECT SUM(i.total_cents) FROM jobber_invoices i WHERE i.jobber_customer_id = jc.jobber_customer_id AND i.customer_id = jc.customer_id), 0) / 100.0 as invoiced_revenue,
        COALESCE((SELECT json_agg(json_build_object('amount', i.total_cents / 100.0, 'type', 'treatment', 'status', COALESCE(i.status, 'paid')) ORDER BY i.total_cents) FROM jobber_invoices i
          WHERE i.jobber_customer_id = jc.jobber_customer_id AND i.customer_id = jc.customer_id AND i.total_cents > 0), '[]'::json) as invoice_breakdown,
        -- Quote sent value (all non-draft quotes)
        COALESCE((SELECT SUM(q.total_cents) FROM jobber_quotes q WHERE q.jobber_customer_id = jc.jobber_customer_id AND q.customer_id = jc.customer_id
          AND q.status IN ('awaiting_response','approved','converted','changes_requested')), 0) / 100.0 as estimate_value,
        -- Enrichment
        (SELECT j.title FROM jobber_jobs j WHERE j.jobber_customer_id = jc.jobber_customer_id AND j.customer_id = jc.customer_id
          ORDER BY j.jobber_created_at DESC LIMIT 1) as job_description,
        (SELECT jr.service_address FROM jobber_requests jr WHERE jr.jobber_customer_id = jc.jobber_customer_id
          AND jr.service_address IS NOT NULL ORDER BY jr.created_at DESC NULLS LAST LIMIT 1) as service_address,
        -- Flag status (reuse same columns if they exist, otherwise null)
        NULL::text as client_flag_reason,
        NULL::timestamptz as client_flag_at,
        -- Lost reason
        (SELECT gc.lost_reason FROM ghl_contacts gc
          WHERE gc.phone_normalized = jc.phone_normalized
            AND gc.customer_id IN (SELECT customer_id FROM client_ids)
            AND gc.lost_reason IS NOT NULL
          LIMIT 1) as lost_reason
      FROM jobber_customers jc
      WHERE jc.customer_id IN (SELECT customer_id FROM client_ids)
        AND jc.jobber_created_at::date BETWEEN $2::date AND $3::date
        ${jcSourceWhere}
        AND jc.is_archived = false
    ),
    -- Unmatched calls
    unmatched AS (
      SELECT
        NULL as hcp_customer_id,
        COALESCE(NULLIF(c.customer_name, ''), 'Caller ID: ' || normalize_phone(c.caller_phone)) as name,
        normalize_phone(c.caller_phone) as phone,
        c.start_time as contact_date,
        'unmatched' as match_status,
        'call' as lead_type,
        COALESCE(c.ai_answered, CASE WHEN c.answered THEN 'answered' ELSE 'missed' END) as answer_status,
        c.duration,
        false as inspection_scheduled,
        false as inspection_completed,
        false as inspection_completed_inferred,
        false as estimate_sent,
        false as estimate_approved,
        false as job_scheduled,
        false as job_completed,
        0::numeric as approved_revenue,
        false as revenue_closed,
        0::numeric as invoiced_revenue,
        '[]'::json as invoice_breakdown,
        0::numeric as estimate_value,
        NULL as job_description,
        NULL as service_address,
        (SELECT cf.flag_reason FROM client_flagged_leads cf
          WHERE cf.customer_id IN (SELECT customer_id FROM client_ids)
            AND cf.phone_normalized = normalize_phone(c.caller_phone)
          LIMIT 1) as client_flag_reason,
        (SELECT cf.flagged_at FROM client_flagged_leads cf
          WHERE cf.customer_id IN (SELECT customer_id FROM client_ids)
            AND cf.phone_normalized = normalize_phone(c.caller_phone)
          LIMIT 1) as client_flag_at,
        (SELECT gc.lost_reason FROM ghl_contacts gc
          WHERE gc.phone_normalized = normalize_phone(c.caller_phone)
            AND gc.customer_id IN (SELECT customer_id FROM client_ids)
            AND gc.lost_reason IS NOT NULL
          LIMIT 1) as lost_reason
      FROM calls c
      WHERE c.customer_id IN (SELECT customer_id FROM client_ids)
        AND c.start_time::date BETWEEN $2::date AND $3::date
        ${crSourceWhere}
        AND NOT EXISTS (SELECT 1 FROM jobber_leads jl WHERE jl.phone = normalize_phone(c.caller_phone))
        AND c.first_call = true
    )
    SELECT * FROM jobber_leads
    UNION ALL
    SELECT * FROM unmatched
    ORDER BY contact_date DESC
  `, [customerId, startDate, endDate]);
  return rows;
}

// ============================================================
// Analytics — Projected ROAS
// ============================================================

// Get open estimates + projected closes for a client
fastify.get('/clients/:customerId/projected-roas', async (request) => {
  const { customerId } = request.params;
  const { source = 'google_ads', date_from, date_to } = request.query;

  const endDate = date_to || new Date().toISOString().split('T')[0];
  const startDate = date_from || new Date(Date.now() - 90 * 86400000).toISOString().split('T')[0];

  // Get client CRM type
  const clientResult = await pool.query(
    `SELECT field_management_software, spreadsheet_id, extra_spam_keywords FROM clients WHERE customer_id = $1`, [customerId]
  );
  const fms = clientResult.rows[0]?.field_management_software || 'housecall_pro';

  let openEstimates = [];

  if (fms === 'housecall_pro') {
    const { rows } = await pool.query(`
      WITH client_ids AS (
        SELECT customer_id FROM clients WHERE customer_id = $1 OR parent_customer_id = $1
      )
      SELECT
        eg.hcp_estimate_id as estimate_id,
        'hcp' as estimate_type,
        hc.first_name || ' ' || hc.last_name as name,
        hc.hcp_created_at::date as lead_date,
        eg.highest_option_cents as value_cents,
        eg.status,
        pc.id IS NOT NULL as projected_close
      FROM v_estimate_groups eg
      JOIN hcp_customers hc ON hc.hcp_customer_id = eg.hcp_customer_id
      LEFT JOIN projected_closes pc ON pc.customer_id = $1 AND pc.estimate_id = eg.hcp_estimate_id
      WHERE hc.customer_id IN (SELECT customer_id FROM client_ids)
        AND eg.status IN ('sent', 'declined')
        AND eg.count_revenue
        AND hc.hcp_created_at::date BETWEEN $2::date AND $3::date
        AND (
          hc.attribution_override = 'google_ads'
          OR hc.callrail_id LIKE 'WF_%'
          OR hc.callrail_id IN (SELECT callrail_id FROM source_callrail_ids)
          OR EXISTS (SELECT 1 FROM calls ca WHERE normalize_phone(ca.caller_phone) = hc.phone_normalized
            AND ca.customer_id IN (SELECT customer_id FROM client_ids) AND is_google_ads_call(ca.source, ca.source_name, ca.gclid))
          OR EXISTS (SELECT 1 FROM form_submissions fs WHERE fs.customer_id IN (SELECT customer_id FROM client_ids)
            AND (fs.callrail_id = hc.callrail_id OR fs.phone_normalized = hc.phone_normalized)
            AND fs.gclid IS NOT NULL AND fs.gclid != '')
          OR EXISTS (SELECT 1 FROM ghl_contacts gc WHERE gc.customer_id = hc.customer_id
            AND (gc.phone_normalized = hc.phone_normalized OR (hc.email IS NOT NULL AND hc.email != '' AND LOWER(gc.email) = LOWER(hc.email)))
            AND gc.gclid IS NOT NULL AND gc.gclid != '')
        )
      ORDER BY eg.highest_option_cents DESC
    `, [customerId, startDate, endDate]);
    openEstimates = rows;
  } else if (fms === 'jobber') {
    const { rows } = await pool.query(`
      WITH client_ids AS (
        SELECT customer_id FROM clients WHERE customer_id = $1 OR parent_customer_id = $1
      )
      SELECT
        q.jobber_quote_id as estimate_id,
        'jobber' as estimate_type,
        jc.first_name || ' ' || jc.last_name as name,
        jc.jobber_created_at::date as lead_date,
        q.total_cents as value_cents,
        q.status,
        pc.id IS NOT NULL as projected_close
      FROM jobber_quotes q
      JOIN jobber_customers jc ON jc.jobber_customer_id = q.jobber_customer_id AND jc.customer_id = q.customer_id
      LEFT JOIN projected_closes pc ON pc.customer_id = $1 AND pc.estimate_id = q.jobber_quote_id
      WHERE q.customer_id IN (SELECT customer_id FROM client_ids)
        AND q.status IN ('awaiting_response', 'changes_requested')
        AND jc.jobber_created_at::date BETWEEN $2::date AND $3::date
        AND (
          jc.attribution_override = 'google_ads'
          OR jc.callrail_id LIKE 'WF_%'
          OR EXISTS (SELECT 1 FROM calls ca WHERE ca.callrail_id = jc.callrail_id AND is_google_ads_call(ca.source, ca.source_name, ca.gclid))
          OR EXISTS (SELECT 1 FROM calls ca WHERE normalize_phone(ca.caller_phone) = jc.phone_normalized
            AND ca.customer_id IN (SELECT customer_id FROM client_ids) AND is_google_ads_call(ca.source, ca.source_name, ca.gclid))
          OR EXISTS (SELECT 1 FROM form_submissions fs WHERE fs.customer_id IN (SELECT customer_id FROM client_ids)
            AND (fs.callrail_id = jc.callrail_id OR fs.phone_normalized = jc.phone_normalized)
            AND fs.gclid IS NOT NULL AND fs.gclid != '')
        )
      ORDER BY q.total_cents DESC
    `, [customerId, startDate, endDate]);
    openEstimates = rows;
  }

  // Get current closed rev + ad spend for context (exclude LSA)
  const { rows: spendRows } = await pool.query(`
    SELECT COALESCE(SUM(cost), 0) as ad_spend
    FROM campaign_daily_metrics
    WHERE customer_id IN (SELECT customer_id FROM clients WHERE customer_id = $1 OR parent_customer_id = $1)
      AND campaign_type != 'LOCAL_SERVICES'
      AND date BETWEEN $2::date AND $3::date
  `, [customerId, startDate, endDate]);

  // Sum of projected closes
  const { rows: projRows } = await pool.query(`
    SELECT COALESCE(SUM(projected_revenue_cents), 0) as total_cents
    FROM projected_closes WHERE customer_id = $1
  `, [customerId]);

  return {
    estimates: openEstimates,
    ad_spend: parseFloat(spendRows[0]?.ad_spend) || 0,
    projected_close_total: (parseInt(projRows[0]?.total_cents) || 0) / 100,
  };
});

// Toggle projected close for an estimate
fastify.post('/clients/:customerId/projected-roas/toggle', async (request) => {
  const { customerId } = request.params;
  const { estimate_id, estimate_type, value_cents, projected_close, marked_by } = request.body || {};

  if (projected_close) {
    await pool.query(`
      INSERT INTO projected_closes (customer_id, estimate_id, estimate_type, projected_revenue_cents, marked_by)
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (customer_id, estimate_id) DO UPDATE SET projected_revenue_cents = $4, marked_by = $5, marked_at = NOW()
    `, [customerId, estimate_id, estimate_type || 'hcp', value_cents || 0, marked_by || 'unknown']);
  } else {
    await pool.query(`
      DELETE FROM projected_closes WHERE customer_id = $1 AND estimate_id = $2
    `, [customerId, estimate_id]);
  }

  return { ok: true };
});

// Clear all projected closes for a client
fastify.post('/clients/:customerId/projected-roas/clear', async (request) => {
  const { customerId } = request.params;
  await pool.query(`DELETE FROM projected_closes WHERE customer_id = $1`, [customerId]);
  return { ok: true };
});

// ============================================================
// Analytics — Flag Lead (client-facing)
// ============================================================

fastify.post('/clients/:customerId/flag-lead', async (request) => {
  const { customerId } = request.params;
  const { hcp_customer_id, phone, callrail_id, name, reason, notes, flagged_by } = request.body || {};

  if (!reason) return { error: 'Reason is required' };

  const clientResult = await pool.query(
    `SELECT name FROM clients WHERE customer_id = $1`, [customerId]
  );
  const clientName = clientResult.rows[0]?.name || 'Unknown Client';
  const performer = flagged_by || `Client (${clientName})`;

  if (hcp_customer_id) {
    // Matched lead — update hcp_customers
    await pool.query(`
      UPDATE hcp_customers SET
        client_flag_reason = $2,
        client_flag_notes = $3,
        client_flag_at = NOW()
      WHERE hcp_customer_id = $1
    `, [hcp_customer_id, reason, notes || null]);

    // Audit trail
    await pool.query(`
      INSERT INTO lead_reviews (hcp_customer_id, customer_id, action, performed_by, reason, notes)
      VALUES ($1, $2, 'client_flag', $3, $4, $5)
    `, [hcp_customer_id, customerId, performer, reason, notes || null]);
  } else if (phone) {
    // Unmatched lead — insert into client_flagged_leads
    await pool.query(`
      INSERT INTO client_flagged_leads (customer_id, phone_normalized, callrail_id, flag_reason, flag_notes)
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT DO NOTHING
    `, [customerId, phone, callrail_id || null, reason, notes || null]);

    // Audit trail (no hcp_customer_id)
    await pool.query(`
      INSERT INTO lead_reviews (customer_id, action, performed_by, reason, notes)
      VALUES ($1, 'client_flag', $2, $3, $4)
    `, [customerId, performer, reason, `[Unmatched: ${name || phone}] ${notes || ''}`]);
  }

  return { ok: true };
});

// ============================================================
// Analytics — Lead Detail (full funnel journey for one lead)
// ============================================================

fastify.get('/clients/:customerId/lead-detail/:hcpCustomerId', async (request) => {
  const { customerId, hcpCustomerId } = request.params;

  // Check CRM type
  const clientResult = await pool.query(
    `SELECT field_management_software, spreadsheet_id, extra_spam_keywords FROM clients WHERE customer_id = $1`, [customerId]
  );
  const fms = clientResult.rows[0]?.field_management_software || 'housecall_pro';

  if (fms === 'jobber') {
    const { rows: jRows } = await pool.query(`
      SELECT
        jc.jobber_customer_id as hcp_customer_id,
        jc.first_name,
        jc.last_name,
        jc.phone_normalized as phone,
        jc.email,
        jc.jobber_created_at as contact_date,
        jc.callrail_id,
        -- Requests (inspections)
        (SELECT json_agg(json_build_object(
          'scheduled_at', jr.assessment_start_at,
          'completed_at', jr.assessment_completed_at,
          'status', jr.status,
          'total_cents', jr.total_amount_cents,
          'description', jr.title,
          'service_address', jr.service_address
        ) ORDER BY jr.assessment_start_at DESC NULLS LAST)
        FROM jobber_requests jr
        WHERE jr.jobber_customer_id = jc.jobber_customer_id AND jr.has_assessment = true
        ) as inspections,
        -- Quotes (estimates)
        (SELECT json_agg(json_build_object(
          'hcp_estimate_id', q.jobber_quote_id,
          'sent_at', q.jobber_created_at,
          'status', q.status,
          'highest_option_cents', q.total_cents,
          'approved_total_cents', CASE WHEN q.status IN ('approved','converted') THEN q.total_cents ELSE 0 END,
          'estimate_type', 'quote',
          'options', NULL
        ) ORDER BY q.jobber_created_at DESC)
        FROM jobber_quotes q
        WHERE q.jobber_customer_id = jc.jobber_customer_id AND q.customer_id = jc.customer_id
          AND q.status NOT IN ('draft')
        ) as estimates,
        -- Jobs
        (SELECT json_agg(json_build_object(
          'scheduled_at', j.jobber_created_at,
          'completed_at', CASE WHEN j.status IN ('late','requires_invoicing') THEN j.jobber_updated_at ELSE NULL END,
          'status', j.status,
          'description', j.title,
          'total_cents', j.total_cents
        ) ORDER BY j.jobber_created_at DESC)
        FROM jobber_jobs j
        WHERE j.jobber_customer_id = jc.jobber_customer_id AND j.customer_id = jc.customer_id
          AND j.status NOT IN ('archived')
        ) as jobs,
        -- Invoices
        (SELECT json_agg(json_build_object(
          'invoice_type', 'standard',
          'status', i.status,
          'amount_cents', i.total_cents,
          'invoice_date', i.invoice_date,
          'paid_at', CASE WHEN i.due_cents = 0 THEN i.updated_at ELSE NULL END
        ) ORDER BY i.invoice_date DESC)
        FROM jobber_invoices i
        WHERE i.jobber_customer_id = jc.jobber_customer_id AND i.customer_id = jc.customer_id
        ) as invoices,
        -- Call info
        (SELECT json_build_object(
          'start_time', c.start_time,
          'duration', c.duration,
          'answered', COALESCE(c.ai_answered, CASE WHEN c.answered THEN 'answered' ELSE 'missed' END),
          'source', c.source,
          'source_name', c.source_name
        ) FROM calls c WHERE c.callrail_id = jc.callrail_id LIMIT 1) as call_info
      FROM jobber_customers jc
      WHERE jc.jobber_customer_id = $1 AND jc.customer_id = $2
    `, [hcpCustomerId, customerId]);

    if (jRows.length === 0) return { error: 'Lead not found' };
    return jRows[0];
  }

  // Default: HCP lead detail
  const { rows } = await pool.query(`
    SELECT
      hc.hcp_customer_id,
      hc.first_name,
      hc.last_name,
      hc.phone_normalized as phone,
      hc.email,
      hc.hcp_created_at as contact_date,
      hc.callrail_id,
      -- Inspections
      (SELECT json_agg(json_build_object(
        'scheduled_at', i.scheduled_at,
        'completed_at', i.completed_at,
        'status', i.status,
        'total_cents', i.total_amount_cents,
        'employee_name', i.employee_name,
        'description', i.description,
        'service_address', i.service_address
      ) ORDER BY i.scheduled_at DESC)
      FROM hcp_inspections i
      WHERE i.hcp_customer_id = ANY(pg.all_ids) AND i.record_status = 'active'
      ) as inspections,
      -- Estimates with options
      (SELECT json_agg(json_build_object(
        'hcp_estimate_id', e.hcp_estimate_id,
        'sent_at', e.sent_at,
        'status', e.status,
        'highest_option_cents', e.highest_option_cents,
        'approved_total_cents', e.approved_total_cents,
        'estimate_type', e.estimate_type,
        'options', (SELECT json_agg(json_build_object(
          'name', eo.name,
          'total_cents', eo.total_amount_cents,
          'status', eo.status,
          'approval_status', eo.approval_status
        ) ORDER BY eo.option_number)
        FROM hcp_estimate_options eo WHERE eo.hcp_estimate_id = e.hcp_estimate_id)
      ) ORDER BY e.sent_at DESC)
      FROM hcp_estimates e
      WHERE e.hcp_customer_id = ANY(pg.all_ids) AND e.record_status = 'active'
      ) as estimates,
      -- Jobs
      (SELECT json_agg(json_build_object(
        'scheduled_at', j.scheduled_at,
        'completed_at', j.completed_at,
        'status', j.status,
        'description', j.description,
        'total_cents', j.total_amount_cents
      ) ORDER BY j.scheduled_at DESC)
      FROM hcp_jobs j
      WHERE j.hcp_customer_id = ANY(pg.all_ids) AND j.record_status = 'active'
      ) as jobs,
      -- Invoices
      (SELECT json_agg(json_build_object(
        'invoice_type', i.invoice_type,
        'status', i.status,
        'amount_cents', i.amount_cents,
        'invoice_date', i.invoice_date,
        'paid_at', i.paid_at
      ) ORDER BY i.invoice_date DESC)
      FROM hcp_invoices i
      WHERE i.hcp_customer_id = ANY(pg.all_ids) AND i.status NOT IN ('canceled','voided')
      ) as invoices,
      -- Call info
      (SELECT json_build_object(
        'start_time', c.start_time,
        'duration', c.duration,
        'answered', COALESCE(c.ai_answered, CASE WHEN c.answered THEN 'answered' ELSE 'missed' END),
        'source', c.source,
        'source_name', c.source_name
      ) FROM calls c WHERE c.callrail_id = hc.callrail_id LIMIT 1) as call_info,
      -- Reactivation badge
      COALESCE((SELECT fl.first_ga_touch_time IS NOT NULL
        AND fl.hcp_created_at < fl.first_ga_touch_time - INTERVAL '7 days'
        AND NOT COALESCE(fl.exclude_from_ga_roas, false)
        FROM mv_funnel_leads fl
        WHERE fl.hcp_customer_id = hc.hcp_customer_id AND fl.customer_id = hc.customer_id), false) as reactivated
    FROM hcp_customers hc
    JOIN (SELECT phone_normalized, array_agg(hcp_customer_id) as all_ids
          FROM hcp_customers WHERE customer_id = $2
          GROUP BY phone_normalized) pg ON pg.phone_normalized = hc.phone_normalized
    WHERE hc.hcp_customer_id = $1 AND hc.customer_id = $2
  `, [hcpCustomerId, customerId]);

  if (rows.length === 0) return { error: 'Lead not found' };
  return rows[0];
});

// ============================================================
// Analytics — Monthly Trend (for charts)
// ============================================================

fastify.get('/clients/:customerId/monthly-trend', async (request) => {
  const { customerId } = request.params;
  const { months = 6, campaign } = request.query;

  // Check client's field management software
  const clientResult = await pool.query(
    `SELECT field_management_software, spreadsheet_id, extra_spam_keywords FROM clients WHERE customer_id = $1`,
    [customerId]
  );
  const fms = clientResult.rows[0]?.field_management_software || 'housecall_pro';

  const { rows } = await pool.query(`
    WITH client_ids AS (
      SELECT customer_id FROM clients WHERE customer_id = $1 OR parent_customer_id = $1
    ),
    months AS (
      SELECT generate_series(
        DATE_TRUNC('month', CURRENT_DATE) - ($2::int - 1) * INTERVAL '1 month',
        DATE_TRUNC('month', CURRENT_DATE),
        '1 month'
      )::date as month_start
    ),
    -- GA-attributed contacts per month (calls + forms, unique phones)
    -- Two-step: first get all phones, then count spam via LEFT JOIN
    campaign_filter AS (
      SELECT DISTINCT gclid FROM gclid_campaign_map
      WHERE customer_id IN (SELECT customer_id FROM client_ids)
        AND campaign_name = COALESCE($3, campaign_name)
    ),
    all_monthly_phones AS (
      SELECT DATE_TRUNC('month', c.start_time)::date as month_start,
        normalize_phone(c.caller_phone) as phone
      FROM calls c
      WHERE c.customer_id IN (SELECT customer_id FROM client_ids)
        AND is_google_ads_call(c.source, c.source_name, c.gclid)
        AND c.start_time >= DATE_TRUNC('month', CURRENT_DATE) - ($2::int - 1) * INTERVAL '1 month'
        AND ($3 IS NULL OR c.gclid IN (SELECT gclid FROM campaign_filter))
      UNION
      SELECT DATE_TRUNC('month', fs.submitted_at)::date as month_start,
        COALESCE(normalize_phone(fs.customer_phone), 'form_' || fs.callrail_id) as phone
      FROM form_submissions fs
      WHERE fs.customer_id IN (SELECT customer_id FROM client_ids)
        AND (fs.gclid IS NOT NULL OR fs.source = 'Google Ads')
        AND COALESCE(fs.is_spam, false) = false
        AND fs.submitted_at >= DATE_TRUNC('month', CURRENT_DATE) - ($2::int - 1) * INTERVAL '1 month'
        AND ($3 IS NULL OR fs.gclid IN (SELECT gclid FROM campaign_filter))
        AND NOT EXISTS (SELECT 1 FROM calls c2
          WHERE c2.customer_id IN (SELECT customer_id FROM client_ids)
            AND is_google_ads_call(c2.source, c2.source_name, c2.gclid)
            AND normalize_phone(c2.caller_phone) = normalize_phone(fs.customer_phone))
    ),
    -- Spam phones from GHL
    spam_phones AS (
      SELECT DISTINCT gc.phone_normalized as phone
      FROM ghl_contacts gc
      WHERE gc.customer_id IN (SELECT customer_id FROM client_ids)
        AND LOWER(gc.lost_reason) SIMILAR TO '%(spam|not a lead|wrong number|out of area|wrong service)%'
    ),
    -- Abandoned phones from GHL (with CRM activity rescue)
    abandoned_phones AS (
      SELECT DISTINCT gc.phone_normalized as phone
      FROM ghl_contacts gc
      WHERE gc.customer_id IN (SELECT customer_id FROM client_ids)
        AND (
          gc.lost_reason ILIKE '%abandoned%'
          OR EXISTS (SELECT 1 FROM ghl_opportunities o WHERE o.ghl_contact_id = gc.ghl_contact_id
            AND o.customer_id = gc.customer_id AND o.status = 'abandoned')
        )
        AND LOWER(COALESCE(gc.lost_reason,'')) NOT SIMILAR TO '%(spam|not a lead|wrong number|out of area|wrong service)%'
        -- CRM activity rescue: if they have real HCP activity, don't count as abandoned
        AND NOT EXISTS (SELECT 1 FROM hcp_customers hc3
          WHERE hc3.customer_id IN (SELECT customer_id FROM client_ids) AND hc3.phone_normalized = gc.phone_normalized
          AND (
            EXISTS (SELECT 1 FROM hcp_inspections i3 WHERE i3.hcp_customer_id = hc3.hcp_customer_id AND i3.record_status = 'active')
            OR EXISTS (SELECT 1 FROM hcp_estimates e3 WHERE e3.hcp_customer_id = hc3.hcp_customer_id AND e3.record_status IN ('active','option'))
            OR EXISTS (SELECT 1 FROM hcp_invoices inv3 WHERE inv3.hcp_customer_id = hc3.hcp_customer_id AND inv3.status NOT IN ('canceled','voided') AND inv3.amount_cents > 0)
            OR EXISTS (SELECT 1 FROM hcp_jobs j3 WHERE j3.hcp_customer_id = hc3.hcp_customer_id AND j3.record_status = 'active')
          ))
    ),
    monthly_leads AS (
      SELECT amp.month_start,
        COUNT(DISTINCT amp.phone) as leads,
        COUNT(DISTINCT amp.phone) FILTER (WHERE sp.phone IS NOT NULL) as spam,
        COUNT(DISTINCT amp.phone) FILTER (WHERE ab.phone IS NOT NULL AND sp.phone IS NULL) as abandoned
      FROM all_monthly_phones amp
      LEFT JOIN spam_phones sp ON sp.phone = amp.phone
      LEFT JOIN abandoned_phones ab ON ab.phone = amp.phone
      GROUP BY amp.month_start
    ),
    -- Ad spend per month (exclude LSA)
    monthly_spend AS (
      SELECT DATE_TRUNC('month', date)::date as month_start,
        SUM(cost) as spend,
        SUM(conversions) as conversions
      FROM campaign_daily_metrics
      WHERE customer_id IN (SELECT customer_id FROM client_ids)
        AND campaign_type != 'LOCAL_SERVICES'
        AND date >= DATE_TRUNC('month', CURRENT_DATE) - ($2::int - 1) * INTERVAL '1 month'
        AND ($3 IS NULL OR campaign_name = $3)
      GROUP BY 1
    ),
    -- Pre-compute spam phones (excluding those with CRM activity)
    trend_spam_phones AS (
      SELECT DISTINCT gc.phone_normalized as phone
      FROM ghl_contacts gc
      WHERE gc.customer_id IN (SELECT customer_id FROM client_ids)
        AND LOWER(gc.lost_reason) SIMILAR TO '%(spam|not a lead|wrong number|out of area|wrong service|abandoned)%'
        AND NOT EXISTS (SELECT 1 FROM hcp_customers hc3
          WHERE hc3.customer_id IN (SELECT customer_id FROM client_ids) AND hc3.phone_normalized = gc.phone_normalized
          AND (
            EXISTS (SELECT 1 FROM hcp_inspections i3 WHERE i3.hcp_customer_id = hc3.hcp_customer_id AND i3.record_status = 'active')
            OR EXISTS (SELECT 1 FROM hcp_estimates e3 WHERE e3.hcp_customer_id = hc3.hcp_customer_id AND e3.record_status IN ('active','option'))
            OR EXISTS (SELECT 1 FROM hcp_invoices inv3 WHERE inv3.hcp_customer_id = hc3.hcp_customer_id AND inv3.status NOT IN ('canceled','voided') AND inv3.amount_cents > 0)
            OR EXISTS (SELECT 1 FROM hcp_jobs j3 WHERE j3.hcp_customer_id = hc3.hcp_customer_id AND j3.record_status = 'active')
          ))
    ),
    -- GA-attributed HCP customers (pre-filtered small set)
    ga_hcp AS (
      SELECT hc.hcp_customer_id, hc.phone_normalized, hc.hcp_created_at
      FROM hcp_customers hc
      WHERE hc.customer_id IN (SELECT customer_id FROM client_ids)
        AND hc.hcp_created_at >= DATE_TRUNC('month', CURRENT_DATE) - ($2::int - 1) * INTERVAL '1 month'
        AND COALESCE(hc.client_flag_reason, '') NOT IN ('spam', 'out_of_area', 'wrong_service')
        AND hc.phone_normalized NOT IN (SELECT phone FROM trend_spam_phones)
        AND (
          hc.attribution_override = 'google_ads'
          OR hc.callrail_id LIKE 'WF_%'
          OR EXISTS (SELECT 1 FROM calls ca WHERE ca.callrail_id = hc.callrail_id AND is_google_ads_call(ca.source, ca.source_name, ca.gclid))
          OR EXISTS (SELECT 1 FROM form_submissions fs WHERE fs.customer_id IN (SELECT customer_id FROM client_ids)
            AND (fs.callrail_id = hc.callrail_id OR fs.phone_normalized = hc.phone_normalized)
            AND fs.gclid IS NOT NULL AND fs.gclid != '')
          OR EXISTS (SELECT 1 FROM ghl_contacts gc WHERE gc.customer_id = hc.customer_id
            AND (gc.phone_normalized = hc.phone_normalized OR (hc.email IS NOT NULL AND hc.email != '' AND LOWER(gc.email) = LOWER(hc.email)))
            AND gc.gclid IS NOT NULL AND gc.gclid != '')
        )
    ),
    -- Aggregate funnel data only on GA-attributed customers
    monthly_hcp AS (
      SELECT DATE_TRUNC('month', hc.hcp_created_at)::date as month_start,
        COUNT(DISTINCT hc.hcp_customer_id) as matched_leads,
        COUNT(DISTINCT hc.hcp_customer_id) FILTER (WHERE EXISTS (
          SELECT 1 FROM hcp_inspections i WHERE i.hcp_customer_id = hc.hcp_customer_id
            AND i.record_status = 'active' AND (i.status IN ('scheduled','complete rated','complete unrated','in progress') OR i.scheduled_at IS NOT NULL)
        )) as inspections_booked,
        COUNT(DISTINCT hc.hcp_customer_id) FILTER (WHERE EXISTS (
          SELECT 1 FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id
            AND eg.status = 'approved' AND eg.count_revenue
        )) as estimates_approved,
        COALESCE(SUM(
          COALESCE((SELECT SUM(eg.approved_total_cents) FROM v_estimate_groups eg
            WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status = 'approved' AND eg.count_revenue), 0)
        ), 0) / 100.0 as revenue
      FROM ga_hcp hc
      GROUP BY 1
    )
    SELECT m.month_start,
      TO_CHAR(m.month_start, 'Mon YYYY') as label,
      TO_CHAR(m.month_start, 'Mon') as short_label,
      EXTRACT(YEAR FROM m.month_start)::int as year,
      COALESCE(ml.leads, 0) as leads,
      COALESCE(ml.spam, 0) as spam,
      COALESCE(ml.abandoned, 0) as abandoned,
      COALESCE(ms.spend, 0) as spend,
      COALESCE(mh.revenue, 0) as revenue,
      CASE WHEN COALESCE(ml.leads, 0) > 0 THEN ROUND(COALESCE(ms.spend, 0) / ml.leads, 2) ELSE 0 END as cpl,
      CASE WHEN COALESCE(ms.spend, 0) > 0 THEN ROUND(COALESCE(mh.revenue, 0) / ms.spend, 2) ELSE 0 END as roas,
      COALESCE(ms.conversions, 0) as conversions,
      COALESCE(mh.inspections_booked, 0) as inspections_booked,
      COALESCE(mh.estimates_approved, 0) as estimates_approved,
      CASE WHEN COALESCE(ml.leads, 0) > 0 THEN ROUND(COALESCE(mh.inspections_booked, 0)::numeric / ml.leads * 100, 1) ELSE 0 END as book_rate,
      CASE WHEN COALESCE(mh.inspections_booked, 0) > 0 THEN ROUND(COALESCE(mh.estimates_approved, 0)::numeric / mh.inspections_booked * 100, 1) ELSE 0 END as close_rate
    FROM months m
    LEFT JOIN monthly_leads ml ON ml.month_start = m.month_start
    LEFT JOIN monthly_spend ms ON ms.month_start = m.month_start
    LEFT JOIN monthly_hcp mh ON mh.month_start = m.month_start
    ORDER BY m.month_start
  `, [customerId, months, campaign || null]);

  // Add excluded_abandoned: abandoned count that should be excluded from quality leads
  // For clients with extra_spam_keywords including 'abandoned', all abandoned are excluded
  // For others, only when the month's abandoned rate exceeds 20%
  const extraSpam = clientResult.rows[0]?.extra_spam_keywords || [];
  const alwaysExcludeAbandoned = extraSpam.includes('abandoned');
  for (const row of rows) {
    const leads = parseInt(row.leads) || 0;
    const abandoned = parseInt(row.abandoned) || 0;
    if (alwaysExcludeAbandoned) {
      row.excluded_abandoned = abandoned;
    } else if (leads > 0 && (abandoned / leads) > 0.20) {
      row.excluded_abandoned = abandoned;
    } else {
      row.excluded_abandoned = 0;
    }
  }

  // ---------- Projection helper: historical pace + recent average ----------
  // Only compute for the current (incomplete) month
  const now = new Date();
  const currentDay = now.getDate();
  const currentMonthStart = new Date(Date.UTC(now.getFullYear(), now.getMonth(), 1)).toISOString().slice(0, 10);
  const currentRow = rows.find(r => r.month_start.toISOString?.().slice(0, 10) === currentMonthStart
    || new Date(r.month_start).toISOString().slice(0, 10) === currentMonthStart);

  if (currentRow && rows.length >= 4) {
    // Historical pace: what fraction of monthly leads typically arrive by this day?
    const paceResult = await pool.query(`
      WITH daily AS (
        SELECT DATE_TRUNC('month', c.start_time)::date AS month_start,
          EXTRACT(DAY FROM c.start_time)::int AS dom,
          normalize_phone(c.caller_phone) AS phone
        FROM calls c
        WHERE c.customer_id IN (SELECT customer_id FROM clients WHERE customer_id = $1 OR parent_customer_id = $1)
          AND is_google_ads_call(c.source, c.source_name, c.gclid)
          AND c.start_time >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12 months'
          AND c.start_time < DATE_TRUNC('month', CURRENT_DATE)
      ),
      monthly_totals AS (
        SELECT month_start, COUNT(DISTINCT phone) AS total
        FROM daily GROUP BY month_start HAVING COUNT(DISTINCT phone) >= 5
      ),
      cumulative AS (
        SELECT d.month_start,
          COUNT(DISTINCT d.phone) FILTER (WHERE d.dom <= $2) AS by_day,
          mt.total
        FROM daily d
        JOIN monthly_totals mt ON mt.month_start = d.month_start
        GROUP BY d.month_start, mt.total
      )
      SELECT CASE WHEN COUNT(*) >= 3 THEN ROUND(AVG(by_day::numeric / total), 4) ELSE NULL END AS pace_fraction,
        COUNT(*) AS months_used
      FROM cumulative WHERE total > 0
    `, [customerId, currentDay]);

    const paceFraction = paceResult.rows[0]?.pace_fraction ? parseFloat(paceResult.rows[0].pace_fraction) : null;

    // Recent average: last 3 complete months of quality leads
    const completeMonths = rows.filter(r => {
      const ms = new Date(r.month_start).toISOString().slice(0, 10);
      return ms !== currentMonthStart;
    }).slice(-3);
    const recentAvg = completeMonths.length >= 3
      ? completeMonths.reduce((sum, r) => {
          const leads = parseInt(r.leads) || 0;
          const spam = parseInt(r.spam) || 0;
          const excAbandoned = parseInt(r.excluded_abandoned) || 0;
          return sum + (leads - spam - excAbandoned);
        }, 0) / completeMonths.length
      : null;

    currentRow.projection_pace_fraction = paceFraction;
    currentRow.projection_recent_avg = recentAvg !== null ? Math.round(recentAvg) : null;
  }


  return rows;
});

// ============================================================
// Analytics — Campaign Trend (leads per campaign per month)
// ============================================================

fastify.get('/clients/:customerId/campaign-trend', async (request) => {
  const { customerId } = request.params;
  const { months = 12 } = request.query;

  const { rows } = await pool.query(`
    WITH cids AS (
      SELECT customer_id FROM clients WHERE customer_id = $1 OR parent_customer_id = $1
    ),
    months AS (
      SELECT generate_series(
        DATE_TRUNC('month', CURRENT_DATE) - ($2::int - 1) * INTERVAL '1 month',
        DATE_TRUNC('month', CURRENT_DATE),
        '1 month'
      )::date as month_start
    ),
    -- Map calls to campaigns via GCLID
    call_campaigns AS (
      SELECT
        DATE_TRUNC('month', c.start_time)::date AS month_start,
        normalize_phone(c.caller_phone) AS phone,
        COALESCE(gcm.campaign_name, 'Unknown') AS campaign_name
      FROM calls c
      JOIN gclid_campaign_map gcm ON gcm.gclid = c.gclid AND gcm.customer_id = c.customer_id
      WHERE c.customer_id IN (SELECT customer_id FROM cids)
        AND is_google_ads_call(c.source, c.source_name, c.gclid)
        AND c.start_time >= DATE_TRUNC('month', CURRENT_DATE) - ($2::int - 1) * INTERVAL '1 month'
      UNION ALL
      SELECT
        DATE_TRUNC('month', fs.submitted_at)::date AS month_start,
        fs.phone_normalized AS phone,
        gcm.campaign_name
      FROM form_submissions fs
      JOIN gclid_campaign_map gcm ON gcm.gclid = fs.gclid AND gcm.customer_id = fs.customer_id
      WHERE fs.customer_id IN (SELECT customer_id FROM cids)
        AND fs.gclid IS NOT NULL AND fs.gclid != ''
        AND COALESCE(fs.is_spam, false) = false
        AND fs.submitted_at >= DATE_TRUNC('month', CURRENT_DATE) - ($2::int - 1) * INTERVAL '1 month'
    ),
    -- Get unique campaigns with significant volume
    campaign_totals AS (
      SELECT campaign_name, COUNT(DISTINCT phone) AS total
      FROM call_campaigns
      GROUP BY campaign_name
    ),
    -- Monthly leads per campaign
    monthly AS (
      SELECT
        m.month_start,
        ct.campaign_name,
        COUNT(DISTINCT cc.phone) AS leads
      FROM months m
      CROSS JOIN campaign_totals ct
      LEFT JOIN call_campaigns cc ON cc.month_start = m.month_start AND cc.campaign_name = ct.campaign_name
      GROUP BY m.month_start, ct.campaign_name
    )
    SELECT
      month_start,
      TO_CHAR(month_start, 'Mon') AS short_label,
      campaign_name,
      COALESCE(leads, 0) AS leads
    FROM monthly
    ORDER BY month_start, campaign_name
  `, [customerId, months]);

  // Group by campaign
  const campaigns = {};
  for (const row of rows) {
    if (!campaigns[row.campaign_name]) {
      campaigns[row.campaign_name] = { name: row.campaign_name, data: [] };
    }
    campaigns[row.campaign_name].data.push({
      month_start: row.month_start,
      short_label: row.short_label,
      leads: parseInt(row.leads),
    });
  }

  return Object.values(campaigns);
});

// ============================================================
// Analytics — Source Tabs Config
// ============================================================

fastify.get('/clients/:customerId/source-tabs', async (request) => {
  const { customerId } = request.params;
  const { rows } = await pool.query(
    `SELECT dashboard_config, field_management_software FROM clients WHERE customer_id = $1`,
    [customerId]
  );
  if (rows.length === 0) return [];

  const config = rows[0].dashboard_config || {};
  if (config.source_tabs) return config.source_tabs;

  // Default tabs
  const fms = rows[0].field_management_software;

  // Check if client has GBP data
  const { rows: gbpRows } = await pool.query(
    `SELECT 1 FROM calls WHERE customer_id = $1 AND source = 'Google My Business' LIMIT 1`,
    [customerId]
  );
  const hasGbp = gbpRows.length > 0;

  const tabs = [
    { key: 'all', label: 'Full Business' },
    { key: 'google_ads', label: 'Google Ads' },
    ...(hasGbp
      ? [{ key: 'gbp', label: 'Google Business Profile' }]
      : [{ key: 'gbp', label: 'Google Business Profile', coming_soon: true }]),
  ];

  // Add LSA tab only if client has LSA spend
  const { rows: lsaRows } = await pool.query(
    `SELECT 1 FROM campaign_daily_metrics WHERE customer_id = $1 AND campaign_type = 'LOCAL_SERVICES' LIMIT 1`,
    [customerId]
  );
  if (lsaRows.length > 0) {
    tabs.push({ key: 'lsa', label: 'Local Services Ads' });
  }

  return tabs;
});

// ============================================================
// Dashboard Share — token-based client lookup (no API key needed for token validation)
// ============================================================

fastify.get('/share/validate/:token', async (request) => {
  const { token } = request.params;
  const { rows } = await pool.query(
    `SELECT customer_id, name, field_management_software, start_date, status FROM clients WHERE dashboard_token = $1 AND status = 'active'`,
    [token]
  );
  if (rows.length === 0) return { error: 'Invalid or expired link' };
  return rows[0];
});

// ============================================================
// GHL Embed — iframe auth (no API key needed)
// ============================================================

const crypto = require('crypto');

// Generate a short-lived embed token for a customer
fastify.get('/embed/token/:customerId', async (request) => {
  const { customerId } = request.params;
  const { secret } = request.query;

  // Validate with a shared secret (set in GHL custom link)
  const EMBED_SECRET = process.env.EMBED_SECRET || 'blueprint-embed-2026';
  if (secret !== EMBED_SECRET) {
    return { error: 'Invalid secret' };
  }

  // Generate a signed token (valid 24 hours)
  const expires = Date.now() + 24 * 60 * 60 * 1000;
  const payload = `${customerId}:${expires}`;
  const signature = crypto.createHmac('sha256', EMBED_SECRET).update(payload).digest('hex').slice(0, 16);
  const token = Buffer.from(`${payload}:${signature}`).toString('base64url');

  return { token, customerId, expires: new Date(expires).toISOString() };
});

// Validate embed token and return client data
fastify.get('/embed/validate/:token', async (request) => {
  const { token } = request.params;
  const EMBED_SECRET = process.env.EMBED_SECRET || 'blueprint-embed-2026';

  try {
    const decoded = Buffer.from(token, 'base64url').toString();
    const [customerId, expiresStr, signature] = decoded.split(':');
    const expires = parseInt(expiresStr);

    if (Date.now() > expires) return { error: 'Token expired' };

    const expectedSig = crypto.createHmac('sha256', EMBED_SECRET)
      .update(`${customerId}:${expiresStr}`).digest('hex').slice(0, 16);
    if (signature !== expectedSig) return { error: 'Invalid token' };

    const { rows } = await pool.query(
      `SELECT customer_id, name, start_date FROM clients WHERE customer_id = $1`,
      [customerId]
    );
    if (rows.length === 0) return { error: 'Client not found' };

    return { valid: true, customer_id: customerId, client_name: rows[0].name };
  } catch (err) {
    return { error: 'Invalid token' };
  }
});

// ============================================================
// Auth endpoints (unchanged from original)
// ============================================================

fastify.post('/auth/sign-in', async (request, reply) => {
  const { email, password } = request.body;
  const { rows } = await pool.query('SELECT * FROM app_users WHERE email = $1', [email]);
  if (rows.length === 0) return reply.code(401).send({ error: 'Invalid email or password' });
  const user = rows[0];
  const valid = await bcrypt.compare(password, user.password_hash);
  if (!valid) return reply.code(401).send({ error: 'Invalid email or password' });
  return formatUser(user);
});

fastify.post('/auth/sign-up', async (request, reply) => {
  const { email, password, displayName } = request.body;
  const existing = await pool.query('SELECT id FROM app_users WHERE email = $1', [email]);
  if (existing.rows.length > 0) return reply.code(409).send({ error: 'Email already registered' });
  const hash = await bcrypt.hash(password, SALT_ROUNDS);
  const role = email.endsWith('@blueprintforscale.com') ? '{admin}' : '{user}';
  const { rows } = await pool.query(
    `INSERT INTO app_users (email, password_hash, display_name, role) VALUES ($1, $2, $3, $4) RETURNING *`,
    [email, hash, displayName, role]
  );
  return formatUser(rows[0]);
});

fastify.get('/auth/user-by-email/:email', async (request, reply) => {
  const { email } = request.params;
  const { rows } = await pool.query('SELECT * FROM app_users WHERE email = $1', [email]);
  if (rows.length === 0) return reply.code(404).send({ error: 'User not found' });
  return formatUser(rows[0]);
});

fastify.get('/auth/user/:id', async (request, reply) => {
  const { id } = request.params;
  const { rows } = await pool.query('SELECT * FROM app_users WHERE id = $1', [id]);
  if (rows.length === 0) return reply.code(404).send({ error: 'User not found' });
  return formatUser(rows[0]);
});

fastify.put('/auth/user/:id', async (request, reply) => {
  const { id } = request.params;
  const { displayName, photoURL, role, shortcuts, settings, loginRedirectUrl } = request.body;
  const { rows } = await pool.query(
    `UPDATE app_users SET
       display_name = COALESCE($2, display_name),
       photo_url = COALESCE($3, photo_url),
       role = COALESCE($4, role),
       shortcuts = COALESCE($5, shortcuts),
       settings = COALESCE($6, settings),
       login_redirect_url = COALESCE($7, login_redirect_url),
       updated_at = NOW()
     WHERE id = $1 RETURNING *`,
    [id, displayName, photoURL, role, shortcuts, settings ? JSON.stringify(settings) : null, loginRedirectUrl]
  );
  if (rows.length === 0) return reply.code(404).send({ error: 'User not found' });
  return formatUser(rows[0]);
});

function formatUser(row) {
  return {
    id: row.id,
    email: row.email,
    displayName: row.display_name,
    photoURL: row.photo_url || '',
    role: row.role,
    shortcuts: row.shortcuts || [],
    settings: row.settings || {},
    loginRedirectUrl: row.login_redirect_url || '/',
  };
}
async function getGhlLeadSpreadsheet(pool, customerId, startDate, endDate, source) {
  let crSourceWhere = '';
  let fmSourceWhere = '';
  let gclidWhere = '';
  if (source === 'google_ads') {
    crSourceWhere = "AND is_google_ads_call(c.source, c.source_name, c.gclid)";
    fmSourceWhere = "AND f.gclid IS NOT NULL AND f.gclid != ''";
    gclidWhere = "UNION SELECT DISTINCT gc2.phone_normalized as phone FROM ghl_contacts gc2 WHERE gc2.customer_id = $1 AND gc2.gclid IS NOT NULL AND gc2.phone_normalized IS NOT NULL";
  }

  const { rows } = await pool.query(`
    WITH ga_phones AS (
      SELECT DISTINCT normalize_phone(c.caller_phone) as phone
      FROM calls c WHERE c.customer_id = $1 ${crSourceWhere}
      UNION
      SELECT DISTINCT normalize_phone(f.customer_phone) as phone
      FROM form_submissions f WHERE f.customer_id = $1 ${fmSourceWhere}
      ${gclidWhere}
    ),
    leads AS (
      SELECT
        gc.ghl_contact_id as hcp_customer_id,
        COALESCE(NULLIF(TRIM(COALESCE(gc.first_name,'') || ' ' || COALESCE(gc.last_name,'')), ''), 'Unknown') as name,
        gc.phone_normalized as phone,
        gc.date_added as contact_date,
        'matched' as match_status, 'call' as lead_type,
        NULL as answer_status, NULL::int as duration,
        EXISTS (SELECT 1 FROM ghl_appointments ga WHERE ga.ghl_contact_id = gc.ghl_contact_id
          AND ga.customer_id = gc.customer_id AND ga.appointment_type = 'inspection' AND ga.deleted = false AND ga.status != 'cancelled') as inspection_scheduled,
        (EXISTS (SELECT 1 FROM ghl_appointments ga WHERE ga.ghl_contact_id = gc.ghl_contact_id
          AND ga.customer_id = gc.customer_id AND ga.appointment_type = 'inspection' AND ga.deleted = false AND ga.status IN ('showed','completed'))
        OR EXISTS (SELECT 1 FROM ghl_opportunities o WHERE o.ghl_contact_id = gc.ghl_contact_id
          AND o.customer_id = gc.customer_id AND (o.stage_name ILIKE 'estimate given%' OR o.stage_name ILIKE 'job%' OR o.stage_name ILIKE 'request reviews%'))) as inspection_completed,
        false as inspection_completed_inferred,
        EXISTS (SELECT 1 FROM ghl_estimates ge WHERE ge.phone_normalized = gc.phone_normalized
          AND ge.customer_id = gc.customer_id AND ge.status IN ('sent','accepted','invoiced')) as estimate_sent,
        EXISTS (SELECT 1 FROM ghl_estimates ge WHERE ge.phone_normalized = gc.phone_normalized
          AND ge.customer_id = gc.customer_id AND ge.status IN ('accepted','invoiced')) as estimate_approved,
        (EXISTS (SELECT 1 FROM ghl_appointments ga WHERE ga.ghl_contact_id = gc.ghl_contact_id
          AND ga.customer_id = gc.customer_id AND ga.appointment_type = 'job' AND ga.deleted = false AND ga.status != 'cancelled')
        OR EXISTS (SELECT 1 FROM ghl_opportunities o WHERE o.ghl_contact_id = gc.ghl_contact_id
          AND o.customer_id = gc.customer_id AND (o.stage_name ILIKE 'job scheduled%' OR o.stage_name ILIKE 'job completed%' OR o.stage_name ILIKE 'job paid%' OR o.stage_name ILIKE 'request reviews%'))) as job_scheduled,
        (EXISTS (SELECT 1 FROM ghl_appointments ga WHERE ga.ghl_contact_id = gc.ghl_contact_id
          AND ga.customer_id = gc.customer_id AND ga.appointment_type = 'job' AND ga.deleted = false AND ga.status IN ('showed','completed'))
        OR EXISTS (SELECT 1 FROM ghl_opportunities o WHERE o.ghl_contact_id = gc.ghl_contact_id
          AND o.customer_id = gc.customer_id AND (o.stage_name ILIKE 'job completed%' OR o.stage_name ILIKE 'job paid%' OR o.stage_name ILIKE 'request reviews%'))) as job_completed,
        COALESCE((SELECT SUM(gt.amount_cents) FROM ghl_transactions gt
          WHERE gt.phone_normalized = gc.phone_normalized AND gt.customer_id = gc.customer_id
          AND gt.status = 'succeeded' AND (gt.entity_source_sub_type IS NULL OR gt.entity_source_sub_type != 'estimate')), 0) as insp_txn_cents,
        COALESCE((SELECT SUM(gt.amount_cents) FROM ghl_transactions gt
          WHERE gt.phone_normalized = gc.phone_normalized AND gt.customer_id = gc.customer_id
          AND gt.status = 'succeeded' AND gt.entity_source_sub_type = 'estimate'), 0) as treat_txn_cents,
        GREATEST(
          COALESCE((SELECT SUM(ge.total_cents) FROM ghl_estimates ge
            WHERE ge.phone_normalized = gc.phone_normalized AND ge.customer_id = gc.customer_id AND ge.status = 'invoiced'), 0),
          COALESCE((SELECT SUM(ge.total_cents) FROM ghl_estimates ge
            WHERE ge.phone_normalized = gc.phone_normalized AND ge.customer_id = gc.customer_id AND ge.status = 'accepted'), 0)
        ) as est_approved_cents,
        COALESCE((SELECT SUM(ge.total_cents) FROM ghl_estimates ge
          WHERE ge.phone_normalized = gc.phone_normalized AND ge.customer_id = gc.customer_id AND ge.status = 'sent'), 0) as est_open_cents,
        gc.lost_reason
      FROM ghl_contacts gc
      JOIN ga_phones gp ON gp.phone = gc.phone_normalized
      WHERE gc.customer_id = $1
        AND gc.date_added BETWEEN $2::date AND ($3::date + 1)
        AND NOT (gc.lost_reason IS NOT NULL AND LOWER(gc.lost_reason) SIMILAR TO '%(spam|not a lead|spoofed|duplicate)%')
    )
    SELECT
      hcp_customer_id, name, phone, contact_date, match_status, lead_type, answer_status, duration,
      inspection_scheduled, inspection_completed, inspection_completed_inferred,
      estimate_sent, estimate_approved, job_scheduled, job_completed,
      (insp_txn_cents + GREATEST(treat_txn_cents, est_approved_cents) > 0) as revenue_closed,
      est_approved_cents / 100.0 as approved_revenue,
      GREATEST(treat_txn_cents, est_approved_cents) / 100.0 as invoiced_revenue,
      '[]'::json as invoice_breakdown,
      (insp_txn_cents + GREATEST(treat_txn_cents, est_approved_cents)) / 100.0 as estimate_value,
      NULL as job_description, NULL as service_address,
      NULL as client_flag_reason, NULL as client_flag_at,
      lost_reason,
      (insp_txn_cents + GREATEST(treat_txn_cents, est_approved_cents)) / 100.0 as roas_revenue,
      est_open_cents / 100.0 as open_estimate_value,
      'Google Ads' as source_label,
      false as inferred
    FROM leads
    ORDER BY contact_date DESC
  `, [customerId, startDate, endDate]);

  // Also add unmatched CallRail leads
  const { rows: unmatchedCalls } = await pool.query(`
    SELECT DISTINCT ON (normalize_phone(c.caller_phone))
      NULL as hcp_customer_id,
      COALESCE(c.customer_name, 'Caller ID: ' || c.caller_phone) as name,
      normalize_phone(c.caller_phone) as phone,
      c.start_time as contact_date,
      'unmatched' as match_status, 'call' as lead_type,
      CASE WHEN c.voicemail THEN 'voicemail' WHEN c.duration > 0 THEN 'answered' ELSE 'missed' END as answer_status,
      c.duration,
      false as inspection_scheduled, false as inspection_completed, false as inspection_completed_inferred,
      false as estimate_sent, false as estimate_approved, false as job_scheduled, false as job_completed,
      false as revenue_closed, 0 as approved_revenue, 0 as invoiced_revenue, '[]'::json as invoice_breakdown,
      0 as estimate_value, NULL as job_description, NULL as service_address,
      NULL as client_flag_reason, NULL as client_flag_at, NULL as lost_reason,
      0 as roas_revenue, 0 as open_estimate_value, 'Google Ads' as source_label, false as inferred
    FROM calls c
    WHERE c.customer_id = $1 ${crSourceWhere}
      AND c.start_time BETWEEN $2::date AND ($3::date + 1)
      AND NOT EXISTS (SELECT 1 FROM ghl_contacts gc WHERE gc.phone_normalized = normalize_phone(c.caller_phone) AND gc.customer_id = $1)
    ORDER BY normalize_phone(c.caller_phone), c.start_time DESC
  `, [customerId, startDate, endDate]);

  return [...rows, ...unmatchedCalls].sort((a, b) => new Date(b.contact_date) - new Date(a.contact_date));
}



// ============================================================
// Start
// ============================================================
const start = async () => {
  try {
    await fastify.listen({ port: 3500, host: '0.0.0.0' });
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
