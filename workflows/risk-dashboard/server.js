#!/usr/bin/env node
/**
 * Risk Dashboard — Client health monitoring with risk/flag scoring
 * Port: 3100
 */

const express = require('express');
const { Pool } = require('pg');
const path = require('path');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 3100;
const PASSWORD = process.env.DASHBOARD_PASSWORD || 'blueprint-risk-2026';
const COOKIE_NAME = 'risk_dashboard_auth';
const COOKIE_MAX_AGE = 24 * 60 * 60 * 1000; // 24 hours

const pool = new Pool({
  host: 'localhost',
  port: 5432,
  user: 'blueprint',
  database: 'blueprint',
  max: 5,
  idleTimeoutMillis: 30000,
});

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ── Auth ──────────────────────────────────────────────────────

// Manager token cache: { token: ads_manager_name }
let _managerTokens = null;
async function getManagerTokens() {
  if (!_managerTokens) {
    const { rows } = await pool.query(
      `SELECT token, ads_manager FROM ads_manager_tokens`
    );
    _managerTokens = {};
    rows.forEach(r => { _managerTokens[r.token] = r.ads_manager; });
    setTimeout(() => { _managerTokens = null; }, 300000); // 5 min cache
  }
  return _managerTokens;
}

function checkAuth(req, res, next) {
  // Manager token auth takes priority (set by /m/:token route)
  if (req.managerName) return next();

  const cookie = req.headers.cookie || '';
  const match = cookie.match(new RegExp(`${COOKIE_NAME}=([^;]+)`));
  if (match && match[1] === hashPassword(PASSWORD)) {
    return next();
  }
  // Serve login page
  if (req.path.startsWith('/api/')) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  return res.send(loginPage());
}

function hashPassword(pw) {
  return crypto.createHash('sha256').update(pw).digest('hex').slice(0, 16);
}

function loginPage() {
  return `<!DOCTYPE html><html><head><title>Risk Dashboard Login</title>
<style>body{font-family:system-ui;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;background:#F5F1E8}
.login{background:#fff;padding:40px;border-radius:8px;box-shadow:0 2px 20px rgba(0,0,0,0.1);text-align:center}
h2{margin:0 0 20px;color:#000}input{padding:12px;width:250px;border:1px solid #ddd;border-radius:4px;font-size:16px;margin-bottom:12px}
button{padding:12px 40px;background:#000;color:#F5F1E8;border:none;border-radius:4px;font-size:16px;cursor:pointer}
button:hover{background:#333}.error{color:#E85D4D;margin-top:10px}</style></head>
<body><div class="login"><h2>Risk Dashboard</h2>
<form method="POST" action="/login"><input type="password" name="password" placeholder="Password" autofocus>
<br><button type="submit">Sign In</button></form></div></body></html>`;
}

app.post('/login', (req, res) => {
  if (req.body.password === PASSWORD) {
    res.setHeader('Set-Cookie',
      `${COOKIE_NAME}=${hashPassword(PASSWORD)}; Path=/; Max-Age=${COOKIE_MAX_AGE / 1000}; HttpOnly`);
    return res.redirect('/');
  }
  res.send(loginPage().replace('</form>', '</form><p class="error">Invalid password</p>'));
});

// ── Manager token routes ──────────────────────────────────────
// /m/:token — serve dashboard filtered to one ads manager
app.get('/m/:token', async (req, res) => {
  try {
    const tokens = await getManagerTokens();
    const manager = tokens[req.params.token];
    if (!manager) return res.status(404).send('Invalid link');

    // Read the HTML and inject manager context
    const fs = require('fs');
    let html = fs.readFileSync(path.join(__dirname, 'public', 'index.html'), 'utf8');

    // Inject manager config before closing </head>
    const injection = `<script>window.MANAGER_MODE = ${JSON.stringify(manager)}; window.MANAGER_TOKEN = ${JSON.stringify(req.params.token)};</script>`;
    html = html.replace('</head>', injection + '</head>');

    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.send(html);
  } catch (err) {
    res.status(500).send('Error: ' + err.message);
  }
});

// Manager token middleware for API calls
app.use(async (req, res, next) => {
  const mgrToken = req.headers['x-manager-token'];
  if (mgrToken) {
    const tokens = await getManagerTokens();
    const manager = tokens[mgrToken];
    if (!manager) return res.status(401).json({ error: 'Invalid manager token' });
    req.managerName = manager;
  }
  next();
});

// Static files with no-cache
app.use(checkAuth);
app.use(express.static(path.join(__dirname, 'public'), {
  setHeaders: (res) => {
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
  }
}));

// ── Child→Parent merge helper ──────────────────────────────────
let _childToParent = null;
async function getChildToParent() {
  if (!_childToParent) {
    const { rows } = await pool.query(
      `SELECT customer_id, parent_customer_id FROM clients WHERE parent_customer_id IS NOT NULL`
    );
    _childToParent = {};
    rows.forEach(l => { _childToParent[String(l.customer_id)] = String(l.parent_customer_id); });
    // Cache for 5 min
    setTimeout(() => { _childToParent = null; }, 300000);
  }
  return _childToParent;
}

function mergeChildRows(rows, childToParent, sumFields) {
  if (Object.keys(childToParent).length === 0) return rows;
  const parentRows = {};
  const childRows = [];
  rows.forEach(r => {
    const cid = String(r.customer_id);
    if (childToParent[cid]) childRows.push(r);
    else parentRows[cid] = r;
  });
  childRows.forEach(child => {
    const pid = childToParent[String(child.customer_id)];
    const parent = parentRows[pid];
    if (!parent) return;
    sumFields.forEach(f => {
      parent[f] = (Number(parent[f]) || 0) + (Number(child[f]) || 0);
    });
  });
  const childIds = new Set(Object.keys(childToParent));
  return rows.filter(r => !childIds.has(String(r.customer_id)));
}

// ── Manager access guard ──────────────────────────────────────
// Ensures manager-token users can only access their assigned clients
async function guardManagerAccess(req, res) {
  if (!req.managerName) return true; // full admin, no restriction
  const { rows } = await pool.query(
    `SELECT 1 FROM clients WHERE customer_id = $1 AND ads_manager = $2`,
    [req.params.id, req.managerName]
  );
  if (rows.length === 0) {
    res.status(403).json({ error: 'Access denied' });
    return false;
  }
  return true;
}

// ── API Routes ────────────────────────────────────────────────

// Main dashboard data
app.get('/api/dashboard', async (req, res) => {
  try {
    const start = req.query.start || null;
    const end = req.query.end || null;

    let query;
    let params;
    if (start && end) {
      query = 'SELECT * FROM get_dashboard_with_risk($1::date, $2::date)';
      params = [start, end];
    } else {
      query = 'SELECT * FROM get_dashboard_with_risk()';
      params = [];
    }

    const mainPromise = pool.query(query, params);

    // Compute historical baselines for drift detection
    // Best 3-month rolling CPL and quality lead pace (excluding last 30 days)
    const baselinePromise = pool.query(`
      WITH client_base AS (
        SELECT c.customer_id, c.callrail_company_id, c.extra_spam_keywords
        FROM clients c
        WHERE c.status = 'active' AND c.start_date IS NOT NULL
          AND c.budget IS NOT NULL AND c.budget > 0
          AND c.parent_customer_id IS NULL
      ),
      client_ids AS (
        SELECT cb.customer_id AS parent_id, cb.customer_id AS data_id FROM client_base cb
        UNION ALL
        SELECT ch.parent_customer_id AS parent_id, ch.customer_id AS data_id
        FROM clients ch
        WHERE ch.parent_customer_id IS NOT NULL AND ch.status = 'active'
          AND EXISTS (SELECT 1 FROM client_base cb WHERE cb.customer_id = ch.parent_customer_id)
      ),
      -- GA calls in baseline window (12 months, excluding last 30 days)
      baseline_call_contacts AS (
        SELECT ci.parent_id AS customer_id,
          normalize_phone(ca.caller_phone) AS dedup_phone,
          NULL::text AS lead_email,
          ca.start_time::date AS lead_date
        FROM calls ca
        INNER JOIN client_ids ci ON ci.data_id = ca.customer_id
        WHERE is_google_ads_call(ca.source, ca.source_name, ca.gclid)
          AND ca.start_time >= CURRENT_DATE - INTERVAL '12 months'
          AND ca.start_time < CURRENT_DATE - INTERVAL '30 days'
      ),
      -- GA forms in baseline window, deduped against calls
      baseline_form_contacts AS (
        SELECT ci.parent_id AS customer_id,
          COALESCE(normalize_phone(fs.customer_phone), 'form_' || fs.callrail_id) AS dedup_phone,
          LOWER(NULLIF(TRIM(fs.customer_email), '')) AS lead_email,
          fs.submitted_at::date AS lead_date
        FROM form_submissions fs
        INNER JOIN client_ids ci ON ci.data_id = fs.customer_id
        WHERE fs.gclid IS NOT NULL AND fs.gclid != ''
          AND fs.submitted_at >= CURRENT_DATE - INTERVAL '12 months'
          AND fs.submitted_at < CURRENT_DATE - INTERVAL '30 days'
          AND NOT EXISTS (
            SELECT 1 FROM baseline_call_contacts bc
            WHERE bc.customer_id = ci.parent_id
              AND bc.dedup_phone = normalize_phone(fs.customer_phone)
          )
      ),
      -- All baseline leads combined
      all_baseline_leads AS (
        SELECT customer_id, dedup_phone, lead_email, lead_date FROM baseline_call_contacts
        UNION ALL
        SELECT customer_id, dedup_phone, lead_email, lead_date FROM baseline_form_contacts
      ),
      -- Abandoned rate over baseline window (per client)
      ga_abandoned_baseline AS (
        SELECT DISTINCT al.customer_id, al.dedup_phone
        FROM all_baseline_leads al
        WHERE EXISTS (
          SELECT 1 FROM ghl_contacts gc
          INNER JOIN client_ids ci ON ci.data_id = gc.customer_id
          WHERE ci.parent_id = al.customer_id
            AND gc.phone_normalized = al.dedup_phone
            AND (
              EXISTS (SELECT 1 FROM ghl_opportunities o WHERE o.ghl_contact_id = gc.ghl_contact_id AND o.status = 'abandoned')
              OR LOWER(COALESCE(gc.lost_reason, '')) LIKE '%abandoned%'
            )
        )
      ),
      abandoned_rates_baseline AS (
        SELECT al.customer_id,
          CASE WHEN COUNT(DISTINCT al.dedup_phone) > 0
            THEN COUNT(DISTINCT ab.dedup_phone)::numeric / COUNT(DISTINCT al.dedup_phone)
            ELSE 0
          END AS rate
        FROM all_baseline_leads al
        LEFT JOIN ga_abandoned_baseline ab ON ab.customer_id = al.customer_id AND ab.dedup_phone = al.dedup_phone
        GROUP BY al.customer_id
      ),
      -- Spam phones (GHL-identified + abandoned-as-spam when rate > 20%)
      ghl_spam_phones AS (
        SELECT DISTINCT ci.parent_id AS customer_id, gc.phone_normalized
        FROM ghl_contacts gc
        INNER JOIN client_ids ci ON ci.data_id = gc.customer_id
        LEFT JOIN client_base cl ON cl.customer_id = ci.parent_id
        LEFT JOIN abandoned_rates_baseline ar ON ar.customer_id = ci.parent_id
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
            OR (COALESCE(ar.rate, 0) > 0.20
                AND LOWER(COALESCE(gc.lost_reason, '')) LIKE '%abandoned%')
          )
      ),
      -- Spam emails
      ghl_spam_emails AS (
        SELECT DISTINCT ci.parent_id AS customer_id, LOWER(gc.email) AS email
        FROM ghl_contacts gc
        INNER JOIN client_ids ci ON ci.data_id = gc.customer_id
        LEFT JOIN client_base cl ON cl.customer_id = ci.parent_id
        LEFT JOIN abandoned_rates_baseline ar ON ar.customer_id = ci.parent_id
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
            OR (COALESCE(ar.rate, 0) > 0.20
                AND LOWER(COALESCE(gc.lost_reason, '')) LIKE '%abandoned%')
          )
      ),
      -- Monthly quality leads (spam-filtered)
      monthly_leads AS (
        SELECT al.customer_id, DATE_TRUNC('month', al.lead_date)::date AS month,
          COUNT(DISTINCT al.dedup_phone) FILTER (
            WHERE NOT EXISTS (SELECT 1 FROM ghl_spam_phones sp WHERE sp.customer_id = al.customer_id AND sp.phone_normalized = al.dedup_phone)
              AND NOT EXISTS (SELECT 1 FROM ghl_spam_emails se WHERE se.customer_id = al.customer_id AND se.email = al.lead_email)
          ) AS leads
        FROM all_baseline_leads al
        GROUP BY al.customer_id, DATE_TRUNC('month', al.lead_date)
      ),
      -- Monthly ad spend
      monthly_data AS (
        SELECT ci.parent_id AS customer_id, DATE_TRUNC('month', d.date)::date AS month, SUM(d.cost) AS spend
        FROM campaign_daily_metrics d
        INNER JOIN client_ids ci ON ci.data_id = d.customer_id
        WHERE d.date >= CURRENT_DATE - INTERVAL '12 months'
          AND d.date < CURRENT_DATE - INTERVAL '30 days'
        GROUP BY ci.parent_id, DATE_TRUNC('month', d.date)
      ),
      combined AS (
        SELECT COALESCE(s.customer_id, l.customer_id) AS customer_id,
          COALESCE(s.month, l.month) AS month,
          COALESCE(s.spend, 0) AS spend, COALESCE(l.leads, 0) AS leads,
          CASE WHEN COALESCE(l.leads, 0) > 0 THEN COALESCE(s.spend, 0) / l.leads ELSE NULL END AS cpl
        FROM monthly_data s
        FULL OUTER JOIN monthly_leads l ON s.customer_id = l.customer_id AND s.month = l.month
      ),
      rolling_3mo AS (
        SELECT customer_id, month,
          AVG(cpl) OVER (PARTITION BY customer_id ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_cpl_3mo,
          AVG(leads) OVER (PARTITION BY customer_id ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_leads_3mo,
          COUNT(*) OVER (PARTITION BY customer_id ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS window_size
        FROM combined WHERE cpl IS NOT NULL
      )
      SELECT customer_id,
        ROUND(MIN(avg_cpl_3mo)::numeric, 2) AS baseline_cpl,
        ROUND(MAX(avg_leads_3mo)::numeric, 0) AS baseline_leads
      FROM rolling_3mo WHERE window_size >= 2
      GROUP BY customer_id
    `);

    // MTD pace: spend and quality leads this month, projected to full month
    const mtdPromise = pool.query(`
      WITH client_base AS (
        SELECT c.customer_id, c.callrail_company_id, c.extra_spam_keywords
        FROM clients c
        WHERE c.status = 'active' AND c.start_date IS NOT NULL
          AND c.budget IS NOT NULL AND c.budget > 0
          AND c.parent_customer_id IS NULL
      ),
      client_ids AS (
        SELECT cb.customer_id AS parent_id, cb.customer_id AS data_id FROM client_base cb
        UNION ALL
        SELECT ch.parent_customer_id AS parent_id, ch.customer_id AS data_id
        FROM clients ch
        WHERE ch.parent_customer_id IS NOT NULL AND ch.status = 'active'
          AND EXISTS (SELECT 1 FROM client_base cb WHERE cb.customer_id = ch.parent_customer_id)
      ),
      -- GA calls this month
      mtd_call_contacts AS (
        SELECT ci.parent_id AS customer_id,
          normalize_phone(ca.caller_phone) AS dedup_phone,
          NULL::text AS lead_email
        FROM calls ca
        INNER JOIN client_ids ci ON ci.data_id = ca.customer_id
        WHERE is_google_ads_call(ca.source, ca.source_name, ca.gclid)
          AND ca.start_time >= DATE_TRUNC('month', CURRENT_DATE)
      ),
      -- GA forms this month, deduped against calls
      mtd_form_contacts AS (
        SELECT ci.parent_id AS customer_id,
          COALESCE(normalize_phone(fs.customer_phone), 'form_' || fs.callrail_id) AS dedup_phone,
          LOWER(NULLIF(TRIM(fs.customer_email), '')) AS lead_email
        FROM form_submissions fs
        INNER JOIN client_ids ci ON ci.data_id = fs.customer_id
        WHERE fs.gclid IS NOT NULL AND fs.gclid != ''
          AND fs.submitted_at >= DATE_TRUNC('month', CURRENT_DATE)
          AND NOT EXISTS (
            SELECT 1 FROM mtd_call_contacts mc
            WHERE mc.customer_id = ci.parent_id
              AND mc.dedup_phone = normalize_phone(fs.customer_phone)
          )
      ),
      -- All MTD leads combined
      all_mtd_leads AS (
        SELECT customer_id, dedup_phone, lead_email FROM mtd_call_contacts
        UNION ALL
        SELECT customer_id, dedup_phone, lead_email FROM mtd_form_contacts
      ),
      -- Use 6-month window for abandoned rate (need enough data for meaningful rate)
      ga_contacts_6mo AS (
        SELECT ci.parent_id AS customer_id, normalize_phone(ca.caller_phone) AS dedup_phone
        FROM calls ca
        INNER JOIN client_ids ci ON ci.data_id = ca.customer_id
        WHERE is_google_ads_call(ca.source, ca.source_name, ca.gclid)
          AND ca.start_time >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '5 months'
        UNION
        SELECT ci.parent_id AS customer_id,
          COALESCE(normalize_phone(fs.customer_phone), 'form_' || fs.callrail_id)
        FROM form_submissions fs
        INNER JOIN client_ids ci ON ci.data_id = fs.customer_id
        WHERE fs.gclid IS NOT NULL AND fs.gclid != ''
          AND fs.submitted_at >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '5 months'
      ),
      abandoned_rates_mtd AS (
        SELECT ga.customer_id,
          CASE WHEN COUNT(DISTINCT ga.dedup_phone) > 0
            THEN COUNT(DISTINCT ga.dedup_phone) FILTER (WHERE EXISTS (
              SELECT 1 FROM ghl_contacts gc
              INNER JOIN client_ids ci ON ci.data_id = gc.customer_id
              WHERE ci.parent_id = ga.customer_id
                AND gc.phone_normalized = ga.dedup_phone
                AND (
                  EXISTS (SELECT 1 FROM ghl_opportunities o WHERE o.ghl_contact_id = gc.ghl_contact_id AND o.status = 'abandoned')
                  OR LOWER(COALESCE(gc.lost_reason, '')) LIKE '%abandoned%'
                )
            ))::numeric / COUNT(DISTINCT ga.dedup_phone)
            ELSE 0
          END AS rate
        FROM ga_contacts_6mo ga
        GROUP BY ga.customer_id
      ),
      ghl_spam_phones AS (
        SELECT DISTINCT ci.parent_id AS customer_id, gc.phone_normalized
        FROM ghl_contacts gc
        INNER JOIN client_ids ci ON ci.data_id = gc.customer_id
        LEFT JOIN client_base cl ON cl.customer_id = ci.parent_id
        LEFT JOIN abandoned_rates_mtd ar ON ar.customer_id = ci.parent_id
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
            OR (COALESCE(ar.rate, 0) > 0.20
                AND LOWER(COALESCE(gc.lost_reason, '')) LIKE '%abandoned%')
          )
      ),
      ghl_spam_emails AS (
        SELECT DISTINCT ci.parent_id AS customer_id, LOWER(gc.email) AS email
        FROM ghl_contacts gc
        INNER JOIN client_ids ci ON ci.data_id = gc.customer_id
        LEFT JOIN client_base cl ON cl.customer_id = ci.parent_id
        LEFT JOIN abandoned_rates_mtd ar ON ar.customer_id = ci.parent_id
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
            OR (COALESCE(ar.rate, 0) > 0.20
                AND LOWER(COALESCE(gc.lost_reason, '')) LIKE '%abandoned%')
          )
      ),
      mtd_spend AS (
        SELECT ci.parent_id AS customer_id, SUM(d.cost) AS spend
        FROM campaign_daily_metrics d
        INNER JOIN client_ids ci ON ci.data_id = d.customer_id
        WHERE d.date >= DATE_TRUNC('month', CURRENT_DATE)
        GROUP BY ci.parent_id
      ),
      mtd_quality AS (
        SELECT al.customer_id,
          COUNT(DISTINCT al.dedup_phone) FILTER (
            WHERE NOT EXISTS (SELECT 1 FROM ghl_spam_phones sp WHERE sp.customer_id = al.customer_id AND sp.phone_normalized = al.dedup_phone)
              AND NOT EXISTS (SELECT 1 FROM ghl_spam_emails se WHERE se.customer_id = al.customer_id AND se.email = al.lead_email)
          ) AS leads
        FROM all_mtd_leads al
        GROUP BY al.customer_id
      )
      SELECT COALESCE(s.customer_id, l.customer_id) AS customer_id,
        COALESCE(s.spend, 0) AS mtd_spend,
        COALESCE(l.leads, 0) AS mtd_leads,
        EXTRACT(DAY FROM CURRENT_DATE)::int AS days_elapsed,
        EXTRACT(DAY FROM DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::int AS days_in_month
      FROM mtd_spend s
      FULL OUTER JOIN mtd_quality l ON s.customer_id = l.customer_id
    `);


    // Merge child accounts into parent
    const childToParent = await getChildToParent();
    const sumFields = ['quality_leads','prior_quality_leads','total_calls','total_insp_booked',
      'total_closed_rev','total_open_est_rev','all_time_rev','all_time_spend','ad_spend',
      'on_cal_14d','on_cal_total','lsa_spend','lsa_leads','approved_no_inv'];
    const { rows } = await mainPromise;
    const merged = mergeChildRows(rows, childToParent, sumFields);

    // Recalculate derived metrics for parents that had children merged
    merged.forEach(r => {
      const pid = String(r.customer_id);
      const hasChildren = Object.values(childToParent).includes(pid);
      if (hasChildren) {
        r.cpl = r.quality_leads > 0 ? r.ad_spend / r.quality_leads : 0;
        r.insp_booked_pct = r.quality_leads > 0 ? r.total_insp_booked / r.quality_leads : 0;
        r.roas = r.ad_spend > 0 ? r.total_closed_rev / r.ad_spend : 0;
        r.period_potential_roas = r.ad_spend > 0 ? (r.total_closed_rev + r.total_open_est_rev) / r.ad_spend : 0;
        r.guarantee = r.all_time_spend > 0 ? r.all_time_rev / r.all_time_spend : 0;
        if (r.prior_quality_leads > 0) {
          r.lead_volume_change = (r.quality_leads - r.prior_quality_leads) / r.prior_quality_leads;
        }
      }
    });
    // Await both queries in parallel
    const [{ rows: baselines }, { rows: mtdRows }] = await Promise.all([baselinePromise, mtdPromise]);
    // Merge baselines + MTD into dashboard rows
    const baselineMap = {};
    baselines.forEach(b => { baselineMap[String(b.customer_id)] = b; });
    const mtdMap = {};
    mtdRows.forEach(m => { mtdMap[String(m.customer_id)] = m; });
    merged.forEach(r => {
      const b = baselineMap[String(r.customer_id)];
      const m = mtdMap[String(r.customer_id)];
      if (b) {
        r.baseline_cpl = Number(b.baseline_cpl);
        r.baseline_leads = Number(b.baseline_leads);
      } else {
        r.baseline_cpl = null;
        r.baseline_leads = null;
      }
      if (m && Number(m.days_elapsed) > 7) {
        const pace = Number(m.days_in_month) / Number(m.days_elapsed);
        r.mtd_projected_leads = Math.round(Number(m.mtd_leads) * pace);
        r.mtd_projected_cpl = Number(m.mtd_leads) > 0
          ? Number(m.mtd_spend) / Number(m.mtd_leads)  // actual MTD CPL, not projected
          : null;
      } else {
        r.mtd_projected_leads = null;
        r.mtd_projected_cpl = null;
      }
    });

    // Merge confirmed status (sticky status anti-flapping)
    const [confirmedResult, overrideResult, locationResult, manualRiskResult, liveContactResult, cadenceResult] = await Promise.all([
      pool.query(`SELECT customer_id, confirmed_status, confirmed_risk_type,
                         pending_status, pending_streak, confirmed_at
                  FROM client_confirmed_status`),
      pool.query(`SELECT customer_id FROM clients WHERE risk_override IS NOT NULL`),
      pool.query(`SELECT DISTINCT customer_id FROM client_location_groups`),
      pool.query(`SELECT customer_id FROM clients WHERE manual_risk = TRUE`),
      pool.query(`
        WITH last_contact AS (
          SELECT customer_id,
            MAX(interaction_date) FILTER (WHERE interaction_type IN ('call', 'meeting') AND interaction_date <= NOW()) AS last_live
          FROM client_interactions
          WHERE interaction_type IN ('call', 'meeting', 'call_attempt')
          GROUP BY customer_id
        )
        SELECT lc.customer_id,
          EXTRACT(DAY FROM NOW() - lc.last_live)::INT AS days_since_live_contact,
          lc.last_live::TEXT AS last_live_contact_date,
          (SELECT COUNT(*) FROM client_interactions ci2
           WHERE ci2.customer_id = lc.customer_id
             AND ci2.interaction_type = 'call_attempt'
             AND ci2.interaction_date > COALESCE(lc.last_live, '1970-01-01'::timestamptz)
          )::INT AS attempts_since_last_contact
        FROM last_contact lc
      `),
      pool.query(`SELECT customer_id, contact_cadence_override,
          CASE WHEN last_campaign_launch_date IS NOT NULL THEN
            EXTRACT(MONTH FROM age(CURRENT_DATE, last_campaign_launch_date))::int
              + EXTRACT(YEAR FROM age(CURRENT_DATE, last_campaign_launch_date))::int * 12
          END AS months_since_campaign_launch
        FROM clients WHERE status = 'active'`)
    ]);
    const manualRiskSet = new Set(manualRiskResult.rows.map(r => String(r.customer_id)));
    const liveContactMap = {};
    liveContactResult.rows.forEach(r => { liveContactMap[String(r.customer_id)] = r; });
    const cadenceMap = {};
    cadenceResult.rows.forEach(r => { cadenceMap[String(r.customer_id)] = { override: r.contact_cadence_override, months_since_campaign: r.months_since_campaign_launch != null ? Number(r.months_since_campaign_launch) : null }; });
    const confirmedMap = {};
    confirmedResult.rows.forEach(c => { confirmedMap[String(c.customer_id)] = c; });
    const overrideSet = new Set(overrideResult.rows.map(r => String(r.customer_id)));
    const locationSet = new Set(locationResult.rows.map(r => String(r.customer_id)));
    merged.forEach(r => {
      r.has_locations = locationSet.has(String(r.customer_id));
      r.manual_risk = manualRiskSet.has(String(r.customer_id));
      // Live contact tracking
      const lc = liveContactMap[String(r.customer_id)];
      r.days_since_live_contact = lc ? Number(lc.days_since_live_contact) : null;
      r.last_live_contact_date = lc ? lc.last_live_contact_date : null;
      r.attempts_since_last_contact = lc ? Number(lc.attempts_since_last_contact) : 0;
      const cadenceInfo = cadenceMap[String(r.customer_id)] || {};
      r.contact_cadence_override = cadenceInfo.override || null;
      r.months_since_campaign_launch = cadenceInfo.months_since_campaign;
      // Compute effective cadence days
      const override = r.contact_cadence_override;
      if (override === 'none') { r.contact_cadence = null; }
      else if (override) { r.contact_cadence = { weekly: 7, biweekly: 14, monthly: 30 }[override] || 30; }
      else {
        // Use the more recent of start_date or last_campaign_launch_date for onboarding cadence
        const months = Math.min(r.months_in_program ?? 999, r.months_since_campaign_launch ?? 999);
        if (months <= 1) { r.contact_cadence = 7; }
        else if (months <= 3) { r.contact_cadence = 14; }
        else if (r.manual_risk || r.status === 'Risk') { r.contact_cadence = 7; }
        else if (r.status === 'Flag') { r.contact_cadence = 14; }
        else { r.contact_cadence = 30; }
      }
    });
    merged.forEach(r => {
      const c = confirmedMap[String(r.customer_id)];
      // risk_override trumps everything — skip confirmed merge
      if (overrideSet.has(String(r.customer_id))) {
        r.computed_status = r.status;
        r.computed_risk_type = r.risk_type;
        r.pending_status = null;
        r.pending_streak = 0;
        return;
      }
      if (c) {
        r.computed_status = r.status;
        r.computed_risk_type = r.risk_type;
        r.status = c.confirmed_status;
        r.risk_type = c.confirmed_risk_type || '';
        r.pending_status = c.pending_status;
        r.pending_streak = parseInt(c.pending_streak) || 0;
        // Re-derive sort_priority from confirmed status
        if (c.confirmed_status === 'Risk') {
          r.sort_priority = c.confirmed_risk_type === 'Both Risk' ? 1
            : c.confirmed_risk_type === 'Ads Risk' ? 2 : 3;
        } else if (c.confirmed_status === 'Flag') {
          r.sort_priority = 4;
        } else {
          r.sort_priority = 5;
        }
      } else {
        r.computed_status = r.status;
        r.computed_risk_type = r.risk_type;
        r.pending_status = null;
        r.pending_streak = 0;
      }
    });
    // Re-sort: Both Risk (1) first, then manual_risk, then rest by sort_priority
    merged.sort((a, b) => {
      const aKey = a.sort_priority === 1 ? 0 : a.manual_risk ? 1 : 2;
      const bKey = b.sort_priority === 1 ? 0 : b.manual_risk ? 1 : 2;
      if (aKey !== bKey) return aKey - bKey;
      return (a.sort_priority - b.sort_priority) || a.client_name.localeCompare(b.client_name);
    });

    // Filter by manager if in manager mode
    const result = req.managerName
      ? merged.filter(r => r.ads_manager === req.managerName)
      : merged;

    res.json(result);
  } catch (err) {
    console.error('Dashboard query error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Client drill-down
app.get('/api/client/:id/details', async (req, res) => {
  try {
    if (!await guardManagerAccess(req, res)) return;
    const customerId = req.params.id;
    const start = req.query.start || null;
    const end = req.query.end || null;

    // Recent leads
    const leadsQuery = `
      SELECT
        c.callrail_id,
        c.start_time,
        c.caller_phone,
        c.classification,
        c.classified_source,
        c.classified_status,
        c.customer_name,
        c.duration,
        c.answered,
        c.call_type
      FROM calls c
      WHERE c.customer_id = $1
        AND is_google_ads_call(c.source, c.source_name, c.gclid)
      ORDER BY c.start_time DESC
      LIMIT 20
    `;

    // Funnel summary
    const funnelQuery = `
      SELECT
        COUNT(*) AS total_leads,
        COUNT(*) FILTER (WHERE lead_status = 'in_funnel') AS in_funnel,
        COUNT(*) FILTER (WHERE lead_status = 'lead_only') AS lead_only,
        COUNT(*) FILTER (WHERE roas_revenue_cents > 0) AS with_revenue,
        ROUND(SUM(roas_revenue_cents) / 100.0, 2) AS total_revenue,
        ROUND(SUM(pipeline_estimate_cents) / 100.0, 2) AS pipeline_revenue
      FROM v_lead_revenue
      WHERE customer_id = $1
        AND lead_source_type = 'google_ads'
    `;

    const [leads, funnel] = await Promise.all([
      pool.query(leadsQuery, [customerId]),
      pool.query(funnelQuery, [customerId]),
    ]);

    res.json({
      recent_leads: leads.rows,
      funnel: funnel.rows[0] || {},
    });
  } catch (err) {
    console.error('Client detail error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Client history (6 month trend)
app.get('/api/client/:id/history', async (req, res) => {
  try {
    if (!await guardManagerAccess(req, res)) return;
    const customerId = req.params.id;
    const query = `
      WITH cids AS (
        SELECT $1::bigint AS cid
        UNION ALL
        SELECT customer_id FROM clients WHERE parent_customer_id = $1::bigint
      ),
      -- GA contacts in last 6 months (for abandoned rate calculation)
      ga_contacts_6mo AS (
        SELECT DISTINCT normalize_phone(c.caller_phone) AS phone_norm
        FROM calls c
        INNER JOIN cids ci ON ci.cid = c.customer_id
        WHERE is_google_ads_call(c.source, c.source_name, c.gclid)
          AND c.start_time >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '5 months'
        UNION
        SELECT DISTINCT COALESCE(normalize_phone(fs.customer_phone), 'form_' || fs.callrail_id)
        FROM form_submissions fs
        INNER JOIN cids ci ON ci.cid = fs.customer_id
        WHERE fs.gclid IS NOT NULL AND fs.gclid != ''
          AND fs.submitted_at >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '5 months'
      ),
      -- Abandoned rate: GA contacts matched to GHL abandoned (period-scoped)
      client_aband_rate AS (
        SELECT
          CASE WHEN COUNT(*) > 0
            THEN COUNT(*) FILTER (WHERE EXISTS (
              SELECT 1 FROM ghl_contacts gc2
              INNER JOIN cids ci2 ON ci2.cid = gc2.customer_id
              WHERE gc2.phone_normalized = ga.phone_norm
                AND (
                  EXISTS (SELECT 1 FROM ghl_opportunities o WHERE o.ghl_contact_id = gc2.ghl_contact_id AND o.status = 'abandoned')
                  OR LOWER(COALESCE(gc2.lost_reason, '')) LIKE '%abandoned%'
                )
            ))::numeric / COUNT(*)
            ELSE 0
          END AS rate
        FROM ga_contacts_6mo ga
      ),
      spam_phones AS (
        SELECT DISTINCT gc.phone_normalized
        FROM ghl_contacts gc
        INNER JOIN cids ci ON ci.cid = gc.customer_id
        LEFT JOIN clients cl ON cl.customer_id = $1::bigint
        LEFT JOIN client_aband_rate car ON true
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
            OR (COALESCE(car.rate, 0) > 0.20
                AND LOWER(COALESCE(gc.lost_reason, '')) LIKE '%abandoned%')
          )
      ),
      -- Email-based spam (for form leads without phone)
      spam_emails AS (
        SELECT DISTINCT LOWER(gc.email) AS email
        FROM ghl_contacts gc
        INNER JOIN cids ci ON ci.cid = gc.customer_id
        LEFT JOIN clients cl ON cl.customer_id = $1::bigint
        LEFT JOIN client_aband_rate car ON true
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
            OR (COALESCE(car.rate, 0) > 0.20
                AND LOWER(COALESCE(gc.lost_reason, '')) LIKE '%abandoned%')
          )
      ),
      months AS (
        SELECT generate_series(
          DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '5 months',
          DATE_TRUNC('month', CURRENT_DATE),
          '1 month'::interval
        )::date AS month_start
      ),
      -- All GA calls per month (deduped by phone)
      call_phones AS (
        SELECT DISTINCT ON (normalize_phone(c.caller_phone), DATE_TRUNC('month', c.start_time))
          normalize_phone(c.caller_phone) AS phone_norm,
          DATE_TRUNC('month', c.start_time)::date AS month_start
        FROM calls c
        INNER JOIN cids ci ON ci.cid = c.customer_id
        WHERE is_google_ads_call(c.source, c.source_name, c.gclid)
          AND c.start_time >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '5 months'
      ),
      -- All GA forms per month (deduped against calls)
      form_phones AS (
        SELECT DISTINCT
          COALESCE(normalize_phone(fs.customer_phone), 'form_' || fs.callrail_id) AS phone_norm,
          DATE_TRUNC('month', fs.submitted_at)::date AS month_start
        FROM form_submissions fs
        INNER JOIN cids ci ON ci.cid = fs.customer_id
        WHERE fs.gclid IS NOT NULL AND fs.gclid != ''
          AND fs.submitted_at >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '5 months'
          AND NOT EXISTS (
            SELECT 1 FROM call_phones cp
            WHERE cp.phone_norm = normalize_phone(fs.customer_phone)
              AND cp.month_start = DATE_TRUNC('month', fs.submitted_at)::date
          )
      ),
      all_phones AS (
        SELECT phone_norm, month_start FROM call_phones
        UNION ALL
        SELECT phone_norm, month_start FROM form_phones
      ),
      monthly_spend AS (
        SELECT
          DATE_TRUNC('month', adm.date)::date AS month_start,
          SUM(adm.cost) AS spend
        FROM campaign_daily_metrics adm
        INNER JOIN cids ci ON ci.cid = adm.customer_id
        WHERE adm.campaign_type != 'LOCAL_SERVICES'
          AND adm.date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '5 months'
        GROUP BY DATE_TRUNC('month', adm.date)
      ),
      monthly_leads AS (
        SELECT
          ap.month_start,
          COUNT(DISTINCT ap.phone_norm)::int AS leads,
          COUNT(DISTINCT ap.phone_norm) FILTER (
            WHERE NOT EXISTS (SELECT 1 FROM spam_phones sp WHERE sp.phone_normalized = ap.phone_norm)
          )::int AS quality_leads
        FROM all_phones ap
        GROUP BY ap.month_start
      )
      SELECT
        TO_CHAR(ms.month_start, 'Mon YYYY') AS month_label,
        ms.month_start,
        COALESCE(ml.leads, 0) AS leads,
        COALESCE(ml.quality_leads, 0) AS quality_leads,
        COALESCE(msp.spend, 0) AS spend
      FROM months ms
      LEFT JOIN monthly_leads ml ON ml.month_start = ms.month_start
      LEFT JOIN monthly_spend msp ON msp.month_start = ms.month_start
      ORDER BY ms.month_start
    `;

    const { rows } = await pool.query(query, [customerId]);

    // Smart projection: historical pace fraction + recent average for current month
    const now = new Date();
    const currentDay = now.getDate();
    if (rows.length >= 4) {
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
        SELECT CASE WHEN COUNT(*) >= 3 THEN ROUND(AVG(by_day::numeric / total), 4) ELSE NULL END AS pace_fraction
        FROM cumulative WHERE total > 0
      `, [customerId, currentDay]);

      const paceFraction = paceResult.rows[0]?.pace_fraction ? parseFloat(paceResult.rows[0].pace_fraction) : null;

      // Recent average: last 3 complete months
      const completeMonths = rows.slice(0, -1).slice(-3);
      const recentAvg = completeMonths.length >= 3
        ? Math.round(completeMonths.reduce((s, r) => s + parseInt(r.quality_leads || 0), 0) / completeMonths.length)
        : null;

      // Attach to last row (current month)
      const lastRow = rows[rows.length - 1];
      if (lastRow) {
        lastRow.projection_pace_fraction = paceFraction;
        lastRow.projection_recent_avg = recentAvg;
      }
    }

    res.json(rows);
  } catch (err) {
    console.error('Client history error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── Metric Drill-downs ────────────────────────────────────────

// Leads drill-down (for lead count / CPL)
app.get('/api/client/:id/drilldown/leads', async (req, res) => {
  try {
    if (!await guardManagerAccess(req, res)) return;
    const cid = req.params.id;
    const { rows } = await pool.query(`
      WITH cids AS (
        SELECT $1::bigint AS cid
        UNION ALL
        SELECT customer_id FROM clients WHERE parent_customer_id = $1::bigint
      ),
      -- GA contacts in the period (for abandoned rate)
      ga_contacts_period AS (
        SELECT DISTINCT normalize_phone(c.caller_phone) AS phone_norm
        FROM calls c
        INNER JOIN cids ci ON ci.cid = c.customer_id
        WHERE is_google_ads_call(c.source, c.source_name, c.gclid)
          AND c.start_time::date BETWEEN $2::date AND $3::date
        UNION
        SELECT DISTINCT COALESCE(normalize_phone(fs.customer_phone), 'form_' || fs.callrail_id)
        FROM form_submissions fs
        INNER JOIN cids ci ON ci.cid = fs.customer_id
        WHERE fs.gclid IS NOT NULL AND fs.gclid != ''
          AND fs.submitted_at::date BETWEEN $2::date AND $3::date
      ),
      -- Abandoned rate: GA contacts matched to GHL abandoned (period-scoped)
      client_aband_rate AS (
        SELECT
          CASE WHEN COUNT(*) > 0
            THEN COUNT(*) FILTER (WHERE EXISTS (
              SELECT 1 FROM ghl_contacts gc2
              INNER JOIN cids ci2 ON ci2.cid = gc2.customer_id
              WHERE gc2.phone_normalized = ga.phone_norm
                AND (
                  EXISTS (SELECT 1 FROM ghl_opportunities o WHERE o.ghl_contact_id = gc2.ghl_contact_id AND o.status = 'abandoned')
                  OR LOWER(COALESCE(gc2.lost_reason, '')) LIKE '%abandoned%'
                )
            ))::numeric / COUNT(*)
            ELSE 0
          END AS rate
        FROM ga_contacts_period ga
      ),
      spam_phones AS (
        SELECT DISTINCT gc.phone_normalized
        FROM ghl_contacts gc
        INNER JOIN cids ci ON ci.cid = gc.customer_id
        LEFT JOIN clients cl ON cl.customer_id = $1::bigint
        LEFT JOIN client_aband_rate car ON true
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
            OR (COALESCE(car.rate, 0) > 0.20
                AND LOWER(COALESCE(gc.lost_reason, '')) LIKE '%abandoned%')
          )
      ),
      -- Email-based spam (for form leads without phone)
      spam_emails AS (
        SELECT DISTINCT LOWER(gc.email) AS email
        FROM ghl_contacts gc
        INNER JOIN cids ci ON ci.cid = gc.customer_id
        LEFT JOIN clients cl ON cl.customer_id = $1::bigint
        LEFT JOIN client_aband_rate car ON true
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
            OR (COALESCE(car.rate, 0) > 0.20
                AND LOWER(COALESCE(gc.lost_reason, '')) LIKE '%abandoned%')
          )
      ),
      -- Calls (deduped by phone, earliest call wins)
      call_leads AS (
        SELECT DISTINCT ON (normalize_phone(c.caller_phone))
          c.start_time AS contact_date,
          c.customer_name AS name,
          c.caller_phone AS phone,
          normalize_phone(c.caller_phone) AS phone_norm,
          'call' AS type,
          c.duration,
          c.answered,
          c.source_name
        FROM calls c
        INNER JOIN cids ci ON ci.cid = c.customer_id
        WHERE is_google_ads_call(c.source, c.source_name, c.gclid)
          AND c.start_time::date BETWEEN $2::date AND $3::date
        ORDER BY normalize_phone(c.caller_phone), c.start_time
      ),
      -- Forms (deduped against calls by phone)
      form_leads AS (
        SELECT
          fs.submitted_at AS contact_date,
          COALESCE(fs.customer_name, fs.customer_email) AS name,
          fs.customer_phone AS phone,
          COALESCE(normalize_phone(fs.customer_phone), 'form_' || fs.callrail_id) AS phone_norm,
          LOWER(NULLIF(TRIM(fs.customer_email), '')) AS lead_email,
          'form' AS type,
          NULL::int AS duration,
          NULL::boolean AS answered,
          'Google Ads Form' AS source_name
        FROM form_submissions fs
        INNER JOIN cids ci ON ci.cid = fs.customer_id
        WHERE fs.gclid IS NOT NULL AND fs.gclid != ''
          AND fs.submitted_at::date BETWEEN $2::date AND $3::date
          AND NOT EXISTS (
            SELECT 1 FROM call_leads cl WHERE cl.phone_norm = normalize_phone(fs.customer_phone)
          )
      ),
      all_leads AS (
        SELECT contact_date, name, phone, phone_norm, type, duration, answered, source_name,
          NOT EXISTS (SELECT 1 FROM spam_phones sp WHERE sp.phone_normalized = phone_norm)
          AS is_quality
        FROM call_leads
        UNION ALL
        SELECT contact_date, name, phone, phone_norm, type, duration, answered, source_name,
          NOT EXISTS (SELECT 1 FROM spam_phones sp WHERE sp.phone_normalized = phone_norm)
          AND NOT EXISTS (SELECT 1 FROM spam_emails se WHERE se.email = lead_email)
          AS is_quality
        FROM form_leads
      )
      SELECT contact_date, name, phone, type, duration, answered, source_name, is_quality
      FROM all_leads
      ORDER BY contact_date DESC
    `, [cid, req.query.start, req.query.end]);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Inspections drill-down (HCP + Jobber)
app.get('/api/client/:id/drilldown/inspections', async (req, res) => {
  try {
    if (!await guardManagerAccess(req, res)) return;
    const { rows } = await pool.query(`
      SELECT * FROM (
        -- HCP inspections
        SELECT
          lp.first_name || ' ' || lp.last_name as customer_name,
          lp.inspection_scheduled_at,
          lp.inspection_completed_at,
          hi.status as inspection_status,
          hi.description,
          hi.employee_name,
          hi.service_address,
          lr.match_method,
          lp.inspection_scheduled_inferred as inferred,
          'hcp' as platform
        FROM v_lead_pipeline lp
        JOIN v_lead_revenue lr ON lr.hcp_customer_id = lp.hcp_customer_id
          AND lr.customer_id = lp.customer_id
          AND lr.lead_source_type = 'google_ads'
        JOIN hcp_inspections hi ON hi.hcp_customer_id = lp.hcp_customer_id
          AND hi.customer_id = lp.customer_id
          AND hi.record_status = 'active' AND hi.count_revenue = true
        LEFT JOIN calls ca ON ca.callrail_id = lr.callrail_id
        LEFT JOIN form_submissions fs ON fs.callrail_id = lr.callrail_id
        WHERE lp.customer_id = $1
          AND lp.inspection_scheduled_at IS NOT NULL
          AND COALESCE(ca.start_time::date, fs.submitted_at::date) BETWEEN $2::date AND $3::date

        UNION ALL

        -- Jobber inspection jobs
        SELECT
          jlr.first_name || ' ' || jlr.last_name,
          j.jobber_created_at as inspection_scheduled_at,
          j.completed_at as inspection_completed_at,
          j.status,
          j.title,
          NULL,
          NULL,
          jlr.match_method,
          false as inferred,
          'jobber'
        FROM v_jobber_lead_revenue jlr
        JOIN jobber_jobs j ON j.jobber_customer_id = jlr.jobber_customer_id
          AND j.customer_id = jlr.customer_id
          AND (LOWER(j.title) LIKE '%assessment%' OR LOWER(j.title) LIKE '%instascope%'
            OR LOWER(j.title) LIKE '%inspection%' OR LOWER(j.title) LIKE '%mold test%'
            OR LOWER(j.title) LIKE '%air quality%' OR LOWER(j.title) LIKE '%air test%')
        LEFT JOIN calls ca2 ON ca2.callrail_id = jlr.callrail_id
        LEFT JOIN form_submissions fs2 ON fs2.callrail_id = jlr.callrail_id
        WHERE jlr.customer_id = $1
          AND jlr.lead_source_type = 'google_ads'
          AND jlr.inspection_scheduled > 0
          AND COALESCE(ca2.start_time::date, fs2.submitted_at::date) BETWEEN $2::date AND $3::date

        UNION ALL

        -- Jobber requests with assessments
        SELECT
          jlr.first_name || ' ' || jlr.last_name,
          jr.assessment_start_at,
          jr.assessment_completed_at,
          jr.status,
          jr.title,
          NULL,
          jr.service_address,
          jlr.match_method,
          false,
          'jobber'
        FROM v_jobber_lead_revenue jlr
        JOIN jobber_requests jr ON jr.jobber_customer_id = jlr.jobber_customer_id
          AND jr.customer_id = jlr.customer_id
          AND jr.has_assessment = true AND jr.assessment_start_at IS NOT NULL
        LEFT JOIN calls ca3 ON ca3.callrail_id = jlr.callrail_id
        LEFT JOIN form_submissions fs3 ON fs3.callrail_id = jlr.callrail_id
        WHERE jlr.customer_id = $1
          AND jlr.lead_source_type = 'google_ads'
          AND COALESCE(ca3.start_time::date, fs3.submitted_at::date) BETWEEN $2::date AND $3::date
      ) combined
      ORDER BY inspection_scheduled_at DESC
    `, [req.params.id, req.query.start, req.query.end]);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ROAS drill-down: invoices + estimates that make up revenue (HCP + Jobber)
app.get('/api/client/:id/drilldown/revenue', async (req, res) => {
  try {
    if (!await guardManagerAccess(req, res)) return;
    const { rows } = await pool.query(`
      SELECT * FROM (
        -- HCP revenue
        SELECT
          lr.first_name || ' ' || lr.last_name as customer_name,
          lr.revenue_source,
          lr.roas_revenue_cents / 100.0 as roas_revenue,
          lr.inspection_invoice_cents / 100.0 as inspection_inv,
          lr.treatment_invoice_cents / 100.0 as treatment_inv,
          lr.approved_estimate_cents / 100.0 as approved_est,
          COALESCE(ca.start_time::date, fs.submitted_at::date) as lead_date,
          lp.current_stage,
          'hcp' as platform
        FROM v_lead_revenue lr
        LEFT JOIN v_lead_pipeline lp ON lp.hcp_customer_id = lr.hcp_customer_id
          AND lp.customer_id = lr.customer_id
        LEFT JOIN calls ca ON ca.callrail_id = lr.callrail_id
        LEFT JOIN form_submissions fs ON fs.callrail_id = lr.callrail_id
        WHERE lr.customer_id = $1
          AND lr.lead_source_type = 'google_ads'
          AND lr.roas_revenue_cents > 0
          AND COALESCE(ca.start_time::date, fs.submitted_at::date) BETWEEN $2::date AND $3::date
        UNION ALL
        -- Jobber revenue
        SELECT
          jlr.first_name || ' ' || jlr.last_name as customer_name,
          jlr.revenue_source,
          jlr.roas_revenue_cents / 100.0 as roas_revenue,
          jlr.inspection_invoice_cents / 100.0 as inspection_inv,
          jlr.treatment_invoice_cents / 100.0 as treatment_inv,
          jlr.approved_quote_cents / 100.0 as approved_est,
          COALESCE(ca2.start_time::date, fs2.submitted_at::date) as lead_date,
          NULL as current_stage,
          'jobber' as platform
        FROM v_jobber_lead_revenue jlr
        LEFT JOIN calls ca2 ON ca2.callrail_id = jlr.callrail_id
        LEFT JOIN form_submissions fs2 ON fs2.callrail_id = jlr.callrail_id
        WHERE jlr.customer_id = $1
          AND jlr.lead_source_type = 'google_ads'
          AND jlr.roas_revenue_cents > 0
          AND COALESCE(ca2.start_time::date, fs2.submitted_at::date) BETWEEN $2::date AND $3::date
      ) combined
      ORDER BY roas_revenue DESC
    `, [req.params.id, req.query.start, req.query.end]);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Spam drill-down from GHL
app.get('/api/client/:id/drilldown/spam', async (req, res) => {
  try {
    if (!await guardManagerAccess(req, res)) return;
    const { rows } = await pool.query(`
      SELECT o.name, o.status, o.stage_name, c.lost_reason,
        o.source, o.date_added, o.ghl_updated_at
      FROM ghl_opportunities o
      LEFT JOIN ghl_contacts c ON c.ghl_contact_id = o.ghl_contact_id
        AND c.customer_id = o.customer_id
      WHERE o.customer_id = $1
        AND o.status = 'lost'
        AND (LOWER(COALESCE(c.lost_reason, '')) LIKE '%spam%'
          OR LOWER(o.stage_name) LIKE '%spam%')
      ORDER BY o.ghl_updated_at DESC NULLS LAST
    `, [req.params.id]);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Abandoned drill-down from GHL
app.get('/api/client/:id/drilldown/abandoned', async (req, res) => {
  try {
    if (!await guardManagerAccess(req, res)) return;
    const { rows } = await pool.query(`
      SELECT o.name, o.status, o.stage_name, o.source,
        o.date_added, o.ghl_updated_at, c.lost_reason
      FROM ghl_opportunities o
      LEFT JOIN ghl_contacts c ON c.ghl_contact_id = o.ghl_contact_id
        AND c.customer_id = o.customer_id
      WHERE o.customer_id = $1
        AND o.status = 'abandoned'
      ORDER BY o.ghl_updated_at DESC NULLS LAST
    `, [req.params.id]);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// On-calendar drill-down (HCP + Jobber inspections, GA flag)
app.get('/api/client/:id/drilldown/calendar', async (req, res) => {
  try {
    if (!await guardManagerAccess(req, res)) return;
    const { rows } = await pool.query(`
      SELECT * FROM (
        -- HCP inspections
        SELECT
          COALESCE(hc.first_name || ' ' || hc.last_name, 'Unknown') as customer_name,
          hi.scheduled_at,
          hi.status,
          hi.description,
          hi.employee_name,
          hi.service_address,
          CASE WHEN EXISTS (
            SELECT 1 FROM calls ca WHERE ca.customer_id = $1
              AND is_google_ads_call(ca.source, ca.source_name, ca.gclid)
              AND normalize_phone(ca.caller_phone) = hc.phone_normalized
          ) OR EXISTS (
            SELECT 1 FROM form_submissions fs WHERE fs.customer_id = $1
              AND (fs.gclid IS NOT NULL OR fs.source = 'Google Ads')
              AND normalize_phone(fs.customer_phone) = hc.phone_normalized
          ) OR hc.callrail_id LIKE 'WF_%'
          THEN true ELSE false END as is_google_ads,
          COALESCE(
            (SELECT c.source_name FROM calls c
             WHERE normalize_phone(c.caller_phone) = hc.phone_normalized AND c.customer_id = $1
             ORDER BY c.start_time DESC LIMIT 1),
            CASE WHEN hc.callrail_id LIKE 'WF_%' THEN 'Webflow Form'
                 WHEN hc.callrail_id LIKE 'FRM%' THEN 'CallRail Form'
                 ELSE hc.lead_source END
          ) as lead_source,
          'hcp' as platform
        FROM hcp_inspections hi
        LEFT JOIN hcp_customers hc ON hc.hcp_customer_id = hi.hcp_customer_id
        WHERE hi.customer_id = $1
          AND hi.status IN ('scheduled', 'in_progress', 'needs scheduling')
          AND hi.record_status = 'active'
          AND hi.scheduled_at >= CURRENT_DATE
          AND hi.scheduled_at < CURRENT_DATE + 14

        UNION ALL

        -- Jobber requests with future assessments
        SELECT
          COALESCE(jc.first_name || ' ' || jc.last_name, 'Unknown'),
          jr.assessment_start_at,
          jr.status,
          jr.title,
          NULL as employee_name,
          jr.service_address,
          CASE WHEN EXISTS (
            SELECT 1 FROM calls ca WHERE ca.customer_id = $1
              AND is_google_ads_call(ca.source, ca.source_name, ca.gclid)
              AND normalize_phone(ca.caller_phone) = jc.phone_normalized
          ) OR EXISTS (
            SELECT 1 FROM form_submissions fs WHERE fs.customer_id = $1
              AND (fs.gclid IS NOT NULL OR fs.source = 'Google Ads')
              AND normalize_phone(fs.customer_phone) = jc.phone_normalized
          ) THEN true ELSE false END,
          COALESCE(
            (SELECT c.source_name FROM calls c
             WHERE normalize_phone(c.caller_phone) = jc.phone_normalized AND c.customer_id = $1
             ORDER BY c.start_time DESC LIMIT 1),
            CASE WHEN jc.callrail_id LIKE 'FRM%' THEN 'CallRail Form' ELSE NULL END
          ),
          'jobber'
        FROM jobber_requests jr
        LEFT JOIN jobber_customers jc ON jc.jobber_customer_id = jr.jobber_customer_id AND jc.customer_id = jr.customer_id
        WHERE jr.customer_id = $1
          AND jr.has_assessment = true
          AND jr.assessment_start_at >= CURRENT_DATE
          AND jr.assessment_start_at < CURRENT_DATE + 14
          AND jr.assessment_completed_at IS NULL

        UNION ALL

        -- Jobber upcoming inspection-titled jobs
        SELECT
          COALESCE(jc.first_name || ' ' || jc.last_name, 'Unknown'),
          j.jobber_created_at,
          j.status,
          j.title,
          NULL,
          NULL,
          CASE WHEN EXISTS (
            SELECT 1 FROM calls ca WHERE ca.customer_id = $1
              AND is_google_ads_call(ca.source, ca.source_name, ca.gclid)
              AND normalize_phone(ca.caller_phone) = jc.phone_normalized
          ) OR EXISTS (
            SELECT 1 FROM form_submissions fs WHERE fs.customer_id = $1
              AND (fs.gclid IS NOT NULL OR fs.source = 'Google Ads')
              AND normalize_phone(fs.customer_phone) = jc.phone_normalized
          ) THEN true ELSE false END,
          COALESCE(
            (SELECT c.source_name FROM calls c
             WHERE normalize_phone(c.caller_phone) = jc.phone_normalized AND c.customer_id = $1
             ORDER BY c.start_time DESC LIMIT 1),
            CASE WHEN jc.callrail_id LIKE 'FRM%' THEN 'CallRail Form' ELSE NULL END
          ),
          'jobber'
        FROM jobber_jobs j
        LEFT JOIN jobber_customers jc ON jc.jobber_customer_id = j.jobber_customer_id AND jc.customer_id = j.customer_id
        WHERE j.customer_id = $1
          AND j.status = 'upcoming'
          AND (LOWER(j.title) LIKE '%assessment%' OR LOWER(j.title) LIKE '%instascope%'
            OR LOWER(j.title) LIKE '%inspection%' OR LOWER(j.title) LIKE '%mold test%'
            OR LOWER(j.title) LIKE '%air quality%' OR LOWER(j.title) LIKE '%air test%')
      ) combined
      ORDER BY scheduled_at NULLS LAST
    `, [req.params.id]);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Location breakdown: full per-location metrics (spend, leads, CPL, revenue, ROAS, book rate)
app.get('/api/client/:id/drilldown/locations', async (req, res) => {
  try {
    if (!await guardManagerAccess(req, res)) return;
    const customerId = req.params.id;
    const start = req.query.start || null;
    const end = req.query.end || null;

    // Check if client has location groups
    const { rows: groups } = await pool.query(
      `SELECT location_name, campaign_ids FROM client_location_groups WHERE customer_id = $1 ORDER BY location_name`,
      [customerId]
    );
    if (groups.length === 0) {
      return res.json({ has_locations: false });
    }

    const dateStart = start || new Date(Date.now() - 30 * 86400000).toISOString().slice(0, 10);
    const dateEnd = end || new Date().toISOString().slice(0, 10);

    // Single comprehensive query: spend + leads + revenue + inspections per location
    const { rows: locationRows } = await pool.query(`
      WITH location_groups AS (
        SELECT location_name, campaign_ids FROM client_location_groups WHERE customer_id = $1
      ),
      -- Calls attributed to campaigns via GCLID
      campaign_calls AS (
        SELECT gcm.campaign_id, c.callrail_id, normalize_phone(c.caller_phone) AS phone
        FROM gclid_campaign_map gcm
        JOIN calls c ON c.gclid = gcm.gclid AND c.customer_id = gcm.customer_id
        WHERE gcm.customer_id = $1
          AND c.start_time::date BETWEEN $2::date AND $3::date
          AND is_google_ads_call(c.source, c.source_name, c.gclid)
      ),
      -- Forms attributed to campaigns via GCLID
      campaign_forms AS (
        SELECT gcm.campaign_id, fs.callrail_id,
          COALESCE(normalize_phone(fs.customer_phone), 'form_' || fs.callrail_id) AS phone
        FROM gclid_campaign_map gcm
        JOIN form_submissions fs ON fs.gclid = gcm.gclid AND fs.customer_id = gcm.customer_id
        WHERE gcm.customer_id = $1
          AND fs.submitted_at::date BETWEEN $2::date AND $3::date
      ),
      -- All leads (calls + forms) per location
      location_leads AS (
        SELECT lg.location_name, cc.callrail_id, cc.phone
        FROM location_groups lg
        JOIN campaign_calls cc ON cc.campaign_id = ANY(lg.campaign_ids)
        UNION ALL
        SELECT lg.location_name, cf.callrail_id, cf.phone
        FROM location_groups lg
        JOIN campaign_forms cf ON cf.campaign_id = ANY(lg.campaign_ids)
      ),
      -- Lead counts per location
      lead_counts AS (
        SELECT location_name, COUNT(DISTINCT phone) AS leads
        FROM location_leads GROUP BY location_name
      ),
      -- Revenue per location via callrail_id → v_lead_revenue
      location_revenue AS (
        SELECT ll.location_name,
          SUM(COALESCE(lr.roas_revenue_cents, 0)) / 100.0 AS revenue,
          COUNT(DISTINCT CASE WHEN lr.lead_status IN ('inspection_completed','treatment_completed','estimate_sent','estimate_approved') THEN lr.hcp_customer_id END) AS booked
        FROM location_leads ll
        LEFT JOIN v_lead_revenue lr ON lr.customer_id = $1 AND lr.callrail_id = ll.callrail_id
        GROUP BY ll.location_name
      ),
      -- Spend per location from campaign_daily_metrics
      location_spend AS (
        SELECT lg.location_name, COALESCE(SUM(cdm.cost), 0) AS spend
        FROM location_groups lg
        LEFT JOIN campaign_daily_metrics cdm ON cdm.customer_id = $1
          AND cdm.campaign_id = ANY(lg.campaign_ids)
          AND cdm.date BETWEEN $2::date AND $3::date
          AND cdm.campaign_type != 'LOCAL_SERVICES'
        GROUP BY lg.location_name
      ),
      -- Other bucket (unassigned campaigns)
      other_spend AS (
        SELECT COALESCE(SUM(cdm.cost), 0) AS spend
        FROM campaign_daily_metrics cdm
        WHERE cdm.customer_id = $1
          AND cdm.date BETWEEN $2::date AND $3::date
          AND cdm.campaign_type != 'LOCAL_SERVICES'
          AND NOT EXISTS (
            SELECT 1 FROM (SELECT DISTINCT unnest(campaign_ids) AS cid FROM client_location_groups WHERE customer_id = $1) m
            WHERE m.cid = cdm.campaign_id
          )
      )
      SELECT
        lg.location_name,
        COALESCE(ls.spend, 0) AS spend,
        COALESCE(lc.leads, 0) AS leads,
        CASE WHEN COALESCE(lc.leads, 0) > 0 THEN COALESCE(ls.spend, 0) / lc.leads ELSE NULL END AS cpl,
        COALESCE(lr.revenue, 0) AS revenue,
        CASE WHEN COALESCE(ls.spend, 0) > 0 THEN COALESCE(lr.revenue, 0) / ls.spend ELSE NULL END AS roas,
        COALESCE(lr.booked, 0) AS booked,
        CASE WHEN COALESCE(lc.leads, 0) > 0 THEN ROUND(COALESCE(lr.booked, 0)::numeric / lc.leads, 3) ELSE NULL END AS book_rate
      FROM location_groups lg
      LEFT JOIN location_spend ls ON ls.location_name = lg.location_name
      LEFT JOIN lead_counts lc ON lc.location_name = lg.location_name
      LEFT JOIN location_revenue lr ON lr.location_name = lg.location_name
      UNION ALL
      SELECT 'Other', os.spend, 0, NULL, 0, NULL, 0, NULL
      FROM other_spend os WHERE os.spend > 0
    `, [customerId, dateStart, dateEnd]);

    // GCLID coverage
    const { rows: coverageRows } = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE c.gclid IS NOT NULL AND c.gclid != '') AS with_gclid,
        COUNT(*) AS total
      FROM calls c
      WHERE c.customer_id = $1
        AND c.start_time::date BETWEEN $2::date AND $3::date
        AND is_google_ads_call(c.source, c.source_name, c.gclid)
    `, [customerId, dateStart, dateEnd]);
    const cov = coverageRows[0];
    const gclidCoverage = cov.total > 0 ? parseFloat(cov.with_gclid) / parseFloat(cov.total) : 0;

    // Build response
    const locations = [];
    let totalSpend = 0, totalLeads = 0, totalRevenue = 0, totalBooked = 0;
    for (const r of locationRows) {
      const loc = {
        location_name: r.location_name,
        spend: parseFloat(r.spend) || 0,
        leads: parseInt(r.leads) || 0,
        cpl: r.cpl !== null ? parseFloat(r.cpl) : null,
        revenue: parseFloat(r.revenue) || 0,
        roas: r.roas !== null ? parseFloat(r.roas) : null,
        booked: parseInt(r.booked) || 0,
        book_rate: r.book_rate !== null ? parseFloat(r.book_rate) : null
      };
      locations.push(loc);
      if (r.location_name !== 'Other') {
        totalSpend += loc.spend;
        totalLeads += loc.leads;
        totalRevenue += loc.revenue;
        totalBooked += loc.booked;
      } else {
        totalSpend += loc.spend;
      }
    }

    res.json({
      has_locations: true,
      gclid_coverage: gclidCoverage,
      locations,
      total: {
        spend: totalSpend,
        leads: totalLeads,
        cpl: totalLeads > 0 ? totalSpend / totalLeads : null,
        revenue: totalRevenue,
        roas: totalSpend > 0 ? totalRevenue / totalSpend : null,
        booked: totalBooked,
        book_rate: totalLeads > 0 ? totalBooked / totalLeads : null
      }
    });
  } catch (err) {
    console.error('Location drilldown error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Trend comparison: current vs prior period metrics
app.get('/api/trends', async (req, res) => {
  try {
    const start = req.query.start || null;
    const end = req.query.end || null;

    // Current period
    let currentQuery, priorQuery;
    if (start && end) {
      const days = Math.round((new Date(end) - new Date(start)) / 86400000);
      const priorEnd = new Date(new Date(start).getTime() - 86400000);
      const priorStart = new Date(priorEnd.getTime() - days * 86400000);
      currentQuery = { q: 'SELECT * FROM get_dashboard_with_risk($1::date, $2::date)', p: [start, end] };
      priorQuery = { q: 'SELECT * FROM get_dashboard_with_risk($1::date, $2::date)', p: [priorStart.toISOString().split('T')[0], priorEnd.toISOString().split('T')[0]] };
    } else {
      currentQuery = { q: 'SELECT * FROM get_dashboard_with_risk()', p: [] };
      // Prior 30 days
      priorQuery = { q: 'SELECT * FROM get_dashboard_with_risk((CURRENT_DATE - 60)::date, (CURRENT_DATE - 31)::date)', p: [] };
    }

    const [current, prior] = await Promise.all([
      pool.query(currentQuery.q, currentQuery.p),
      pool.query(priorQuery.q, priorQuery.p)
    ]);

    const priorMap = {};
    prior.rows.forEach(r => {
      priorMap[r.customer_id] = {
        sort_priority: r.sort_priority,
        flag_count: r.flag_count,
        quality_leads: r.quality_leads,
        cpl: r.cpl,
        roas: r.roas,
        insp_booked_pct: r.insp_booked_pct
      };
    });

    const trends = {};
    current.rows.forEach(r => {
      const prev = priorMap[r.customer_id];
      if (!prev) { trends[r.customer_id] = { status: 'new' }; return; }

      // Status trend: lower sort_priority = worse (1=both risk, 5=healthy)
      const statusDelta = Number(r.sort_priority) - Number(prev.sort_priority);
      // Positive = improving (moved toward healthy), negative = worsening

      trends[r.customer_id] = {
        status: statusDelta > 0 ? 'improving' : statusDelta < 0 ? 'declining' : 'stable',
        prior_sort: Number(prev.sort_priority),
        flag_delta: Number(r.flag_count) - Number(prev.flag_count),
        cpl_delta: prev.cpl > 0 ? Number(r.cpl) - Number(prev.cpl) : null,
        roas_delta: prev.roas > 0 ? Number(r.roas) - Number(prev.roas) : null,
        book_delta: Number(r.insp_booked_pct) - Number(prev.insp_booked_pct)
      };
    });

    res.json(trends);
  } catch (err) {
    console.error('Trends error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Monthly pace for all clients (pro-rated current month vs prior month, calls + forms)
app.get('/api/monthly-pace', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT
        customer_id,
        COUNT(DISTINCT dedup_phone) FILTER (
          WHERE lead_date >= DATE_TRUNC('month', CURRENT_DATE)
        )::int AS this_month,
        COUNT(DISTINCT dedup_phone) FILTER (
          WHERE lead_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
            AND lead_date < DATE_TRUNC('month', CURRENT_DATE)
        )::int AS last_month
      FROM (
        SELECT c.customer_id, normalize_phone(c.caller_phone) AS dedup_phone, c.start_time::date AS lead_date
        FROM calls c
        JOIN clients cl ON cl.customer_id = c.customer_id
        WHERE cl.status = 'active' AND cl.start_date IS NOT NULL
          AND is_google_ads_call(c.source, c.source_name, c.gclid)
          AND c.start_time >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
        UNION ALL
        SELECT fs.customer_id, COALESCE(normalize_phone(fs.customer_phone), 'form_' || fs.callrail_id), fs.submitted_at::date
        FROM form_submissions fs
        JOIN clients cl ON cl.customer_id = fs.customer_id
        WHERE cl.status = 'active' AND cl.start_date IS NOT NULL
          AND (fs.gclid IS NOT NULL OR fs.source = 'Google Ads')
          AND fs.submitted_at >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
      ) all_contacts
      GROUP BY customer_id
    `);
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Rolling 14-day vs prior 14-day comparison for recovery signals
app.get('/api/rolling-14d', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      WITH lead_data AS (
        SELECT customer_id, dedup_phone, lead_date, spend
        FROM (
          SELECT c.customer_id, normalize_phone(c.caller_phone) AS dedup_phone,
                 c.start_time::date AS lead_date, 0 AS spend
          FROM calls c
          JOIN clients cl ON cl.customer_id = c.customer_id
          WHERE cl.status = 'active' AND cl.start_date IS NOT NULL
            AND is_google_ads_call(c.source, c.source_name, c.gclid)
            AND c.start_time >= CURRENT_DATE - 28
          UNION ALL
          SELECT fs.customer_id, COALESCE(normalize_phone(fs.customer_phone), 'form_' || fs.callrail_id),
                 fs.submitted_at::date, 0
          FROM form_submissions fs
          JOIN clients cl ON cl.customer_id = fs.customer_id
          WHERE cl.status = 'active' AND cl.start_date IS NOT NULL
            AND (fs.gclid IS NOT NULL OR fs.source = 'Google Ads')
            AND fs.submitted_at >= CURRENT_DATE - 28
        ) all_contacts
      ),
      spend_data AS (
        SELECT customer_id,
          SUM(cost) FILTER (WHERE date >= CURRENT_DATE - 14) AS spend_recent,
          SUM(cost) FILTER (WHERE date >= CURRENT_DATE - 28 AND date < CURRENT_DATE - 14) AS spend_prior,
          SUM(cost) FILTER (WHERE date >= CURRENT_DATE - 7) AS spend_7d,
          SUM(cost) FILTER (WHERE date >= CURRENT_DATE - 14 AND date < CURRENT_DATE - 7) AS spend_7d_prior
        FROM account_daily_metrics
        WHERE date >= CURRENT_DATE - 28
        GROUP BY customer_id
      ),
      lead_counts AS (
        SELECT customer_id,
          COUNT(DISTINCT dedup_phone) FILTER (WHERE lead_date >= CURRENT_DATE - 14) AS leads_recent,
          COUNT(DISTINCT dedup_phone) FILTER (WHERE lead_date >= CURRENT_DATE - 28 AND lead_date < CURRENT_DATE - 14) AS leads_prior,
          COUNT(DISTINCT dedup_phone) FILTER (WHERE lead_date >= CURRENT_DATE - 7) AS leads_7d,
          COUNT(DISTINCT dedup_phone) FILTER (WHERE lead_date >= CURRENT_DATE - 14 AND lead_date < CURRENT_DATE - 7) AS leads_7d_prior
        FROM lead_data
        GROUP BY customer_id
      )
      SELECT
        COALESCE(l.customer_id, s.customer_id) AS customer_id,
        COALESCE(l.leads_recent, 0) AS leads_recent,
        COALESCE(l.leads_prior, 0) AS leads_prior,
        COALESCE(s.spend_recent, 0) AS spend_recent,
        COALESCE(s.spend_prior, 0) AS spend_prior,
        COALESCE(l.leads_7d, 0) AS leads_7d,
        COALESCE(l.leads_7d_prior, 0) AS leads_7d_prior,
        COALESCE(s.spend_7d, 0) AS spend_7d,
        COALESCE(s.spend_7d_prior, 0) AS spend_7d_prior
      FROM lead_counts l
      FULL JOIN spend_data s ON s.customer_id = l.customer_id
    `);
    const childToParent = await getChildToParent();
    const merged = mergeChildRows(rows, childToParent, ['leads_recent','leads_prior','spend_recent','spend_prior','leads_7d','leads_7d_prior','spend_7d','spend_7d_prior']);
    res.json(merged);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Update client budget (admin only — blocked for manager tokens)
app.post('/api/client/:id/budget', async (req, res) => {
  try {
    if (req.managerName) return res.status(403).json({ error: 'Read-only access' });
    const customerId = req.params.id;
    const { budget } = req.body;
    if (budget === undefined || budget === null || isNaN(Number(budget))) {
      return res.status(400).json({ error: 'Invalid budget value' });
    }
    // Get old budget before updating
    const oldResult = await pool.query(
      'SELECT name, budget, ads_manager, slack_channel_id FROM clients WHERE customer_id = $1', [customerId]
    );
    const oldBudget = oldResult.rows.length ? parseFloat(oldResult.rows[0].budget) || 0 : 0;

    const { rows } = await pool.query(
      'UPDATE clients SET budget = $1, updated_at = NOW() WHERE customer_id = $2 RETURNING name, budget',
      [Number(budget), customerId]
    );
    if (rows.length === 0) return res.status(404).json({ error: 'Client not found' });
    console.log(`Budget updated: ${rows[0].name} → $${rows[0].budget}`);

    // Send instant Slack alert if budget actually changed
    const newBudget = parseFloat(rows[0].budget);
    if (oldBudget > 0 && newBudget > 0 && oldBudget !== newBudget) {
      const client = oldResult.rows[0];
      const displayName = client.name.includes('|') ? client.name.split('|').pop().trim() : client.name;
      const change = newBudget - oldBudget;
      const pct = ((change / oldBudget) * 100).toFixed(0);
      const sign = change >= 0 ? '+' : '';
      const fmtM = v => v >= 1000 ? `$${(v/1000).toFixed(1)}k` : `$${Math.round(v)}`;

      const msg = `:moneybag: *BUDGET UPDATE*\n*${displayName}*\nOld: ${fmtM(oldBudget)} → New: ${fmtM(newBudget)} (${sign}${pct}%)\nManager: ${client.ads_manager || '—'}`;

      // Post to all channels (fire and forget — don't block the response)
      const slackPost = (channel) => {
        const https = require('https');
        const payload = JSON.stringify({ channel, text: msg, mrkdwn: true });
        const opts = { hostname: 'slack.com', path: '/api/chat.postMessage', method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer xoxb-6594692085893-10476528834625-otlCrGN5kiu31kQYWDvrCAwC', 'Content-Length': Buffer.byteLength(payload) } };
        const r = https.request(opts, () => {});
        r.on('error', () => {});
        r.write(payload);
        r.end();
      };

      if (client.slack_channel_id) slackPost(client.slack_channel_id);
      if (client.ads_manager === 'Luke') slackPost('C08LQL3TPGA');

      // Update alert_last_state so hourly check doesn't double-fire
      pool.query(`UPDATE alert_last_state SET budget = $1, updated_at = NOW() WHERE customer_id = $2`, [newBudget, customerId]).catch(() => {});
    }

    res.json({ success: true, name: rows[0].name, budget: rows[0].budget });
  } catch (err) {
    console.error('Budget update error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Manual contact logging
app.post('/api/client/:id/log-contact', checkAuth, async (req, res) => {
  try {
    const customerId = req.params.id;
    const { type, summary, logged_by } = req.body;
    const interactionType = type === 'call_attempt' ? 'call_attempt' : type === 'meeting' ? 'meeting' : 'call';
    await pool.query(`
      INSERT INTO client_interactions (customer_id, interaction_type, interaction_date, source, summary, logged_by)
      VALUES ($1, $2, NOW(), 'manual', $3, $4)
    `, [customerId, interactionType, summary || 'Manual log', logged_by || 'Susie']);
    res.json({ ok: true });
  } catch (err) {
    console.error('Log contact error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Risk status changes (7-day movement + days in current status)
app.get('/api/status-changes', async (req, res) => {
  try {
    if (!await guardManagerAccess(req, res, true)) return;
    const { rows } = await pool.query(`
      WITH latest AS (
        SELECT MAX(snapshot_date) AS d FROM risk_status_snapshots
      ),
      today AS (
        SELECT s.customer_id, s.status, s.risk_type
        FROM risk_status_snapshots s, latest l
        WHERE s.snapshot_date = l.d
      ),
      week_ago AS (
        SELECT s.customer_id, s.status, s.risk_type
        FROM risk_status_snapshots s, latest l
        WHERE s.snapshot_date = l.d - 7
      ),
      -- Raw history for streak calculation (done in app code to handle blip-skipping)
      streak AS (
        SELECT t.customer_id, 0 AS days_in_status
        FROM today t
      )
      SELECT
        c.customer_id,
        c.name AS client_name,
        t.status AS current_status,
        t.risk_type AS current_risk_type,
        w.status AS prior_status,
        w.risk_type AS prior_risk_type,
        CASE
          WHEN w.status IS NULL THEN 'new'
          WHEN t.status = w.status AND COALESCE(t.risk_type,'') = COALESCE(w.risk_type,'') THEN 'unchanged'
          WHEN t.status = 'Risk' AND w.status != 'Risk' THEN 'entered_risk'
          WHEN t.status != 'Risk' AND w.status = 'Risk' THEN 'left_risk'
          WHEN t.status = 'Flag' AND w.status = 'Healthy' THEN 'entered_flag'
          WHEN t.status = 'Healthy' AND w.status = 'Flag' THEN 'left_flag'
          ELSE 'changed'
        END AS movement,
        COALESCE(sk.days_in_status, 1)::int AS days_in_status,
        -- True if streak goes all the way back to earliest snapshot (floor, not exact)
        CASE WHEN NOT EXISTS (
          SELECT 1 FROM risk_status_snapshots s
          WHERE s.customer_id = t.customer_id AND s.status != t.status
        ) THEN true ELSE false END AS days_at_floor,
        -- Date they entered current status
        COALESCE(
          (SELECT MAX(s.snapshot_date) + 1
           FROM risk_status_snapshots s
           WHERE s.customer_id = t.customer_id
             AND s.snapshot_date < (SELECT d FROM latest)
             AND s.status != t.status),
          (SELECT MIN(s.snapshot_date)
           FROM risk_status_snapshots s
           WHERE s.customer_id = t.customer_id)
        )::text AS status_since
      FROM today t
      JOIN clients c ON c.customer_id = t.customer_id
      LEFT JOIN week_ago w ON w.customer_id = t.customer_id
      LEFT JOIN streak sk ON sk.customer_id = t.customer_id
      ORDER BY c.name
    `);

    // Compute streaks with blip-skipping (ignore interruptions <= 7 days)
    const { rows: histRows } = await pool.query(`
      SELECT customer_id, snapshot_date::text AS snapshot_date, status
      FROM risk_status_snapshots
      ORDER BY customer_id, snapshot_date DESC
    `);

    // Group by customer
    const histByCustomer = {};
    histRows.forEach(h => {
      if (!histByCustomer[h.customer_id]) histByCustomer[h.customer_id] = [];
      histByCustomer[h.customer_id].push(h);
    });

    const earliest = histRows.length ? histRows[histRows.length - 1].snapshot_date : null;

    rows.forEach(r => {
      const hist = histByCustomer[r.customer_id];
      if (!hist || !hist.length) { r.days_in_status = 0; r.status_since = null; r.days_at_floor = false; return; }

      const currentStatus = hist[0].status; // most recent
      // Walk backwards, skipping blips of different status that last <= 7 days
      let streakDays = 0;
      let blipCount = 0;
      let statusSince = hist[0].snapshot_date;
      let hitFloor = true;

      for (let i = 0; i < hist.length; i++) {
        if (hist[i].status === currentStatus) {
          streakDays++;
          blipCount = 0;
          statusSince = hist[i].snapshot_date;
        } else {
          blipCount++;
          if (blipCount > 7) {
            hitFloor = false;
            break;
          }
          streakDays++; // count blip days in the total
        }
      }

      r.days_in_status = streakDays;
      r.status_since = statusSince;
      r.days_at_floor = hitFloor;
    });

    res.json(rows);
  } catch (err) {
    console.error('Status changes error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Weekly risk counts over time (for chart)
// ?raw=1 uses status_raw (ads manager view, without CRM spam)
app.get('/api/risk-trend', async (req, res) => {
  try {
    if (!await guardManagerAccess(req, res, true)) return;
    const useRaw = req.query.raw === '1';
    const statusCol = useRaw ? 'COALESCE(s.status_raw, s.status)' : 's.status';
    const { rows } = await pool.query(`
      WITH daily_counts AS (
        SELECT
          DATE_TRUNC('month', snapshot_date)::date AS month_start,
          COALESCE(c.parent_customer_id, s.customer_id) AS customer_id,
          COALESCE(p.name, c.name) AS client_name,
          COUNT(*) FILTER (WHERE ${statusCol} = 'Risk') AS risk_days,
          COUNT(*) FILTER (WHERE ${statusCol} = 'Flag') AS flag_days,
          COUNT(*) FILTER (WHERE ${statusCol} = 'Healthy') AS healthy_days,
          COUNT(*) AS total_days,
          -- Track client age: months since start_date at end of this month
          EXTRACT(YEAR FROM AGE(DATE_TRUNC('month', snapshot_date) + INTERVAL '1 month', COALESCE(MIN(c.start_date), '2020-01-01'::date))) * 12
            + EXTRACT(MONTH FROM AGE(DATE_TRUNC('month', snapshot_date) + INTERVAL '1 month', COALESCE(MIN(c.start_date), '2020-01-01'::date))) AS months_since_start
        FROM risk_status_snapshots s
        JOIN clients c ON c.customer_id = s.customer_id
        LEFT JOIN clients p ON p.customer_id = c.parent_customer_id
        WHERE s.snapshot_date >= COALESCE(c.start_date, '2020-01-01')
        GROUP BY DATE_TRUNC('month', snapshot_date), COALESCE(c.parent_customer_id, s.customer_id), COALESCE(p.name, c.name)
      ),
      with_prior AS (
        SELECT
          dc.*,
          -- For current month (< 14 days of data): use majority status (most days wins)
          -- For past months: require 14+ days in status
          CASE WHEN total_days < 14 THEN
            -- Current/partial month: risk if risk_days is the plurality
            risk_days > flag_days AND risk_days > healthy_days
          ELSE
            -- Sticky risk: disabled for new accounts (under 3 months)
            CASE
              WHEN months_since_start < 3 THEN risk_days >= 14
              WHEN risk_days >= 14 THEN true
              WHEN LAG(risk_days >= 14) OVER (PARTITION BY customer_id ORDER BY month_start)
                AND healthy_days < 14 THEN true
              ELSE false
            END
          END AS sticky_risk,
          CASE WHEN total_days < 14 THEN
            -- Current/partial month: flag if flag_days is plurality and not risk
            flag_days >= risk_days AND flag_days > healthy_days
          ELSE
            NOT (CASE
              WHEN months_since_start < 3 THEN risk_days >= 14
              WHEN risk_days >= 14 THEN true
              WHEN LAG(risk_days >= 14) OVER (PARTITION BY customer_id ORDER BY month_start)
                AND healthy_days < 14 THEN true
              ELSE false
            END) AND flag_days >= 14
          END AS is_flag,
          CASE
            WHEN risk_days >= 14 AND healthy_days >= 14 THEN true
            ELSE false
          END AS recovered
        FROM daily_counts dc
      )
      SELECT
        month_start AS snapshot_date,
        COUNT(*) FILTER (WHERE sticky_risk) AS ads_risk_count,
        COUNT(*) FILTER (WHERE is_flag) AS flag_count,
        COUNT(*) FILTER (WHERE NOT sticky_risk AND NOT is_flag) AS healthy_count,
        COUNT(*) AS total,
        COALESCE(ARRAY_AGG(client_name ORDER BY client_name) FILTER (WHERE sticky_risk), '{}') AS ads_risk_clients,
        COALESCE(ARRAY_AGG(client_name ORDER BY client_name) FILTER (WHERE is_flag), '{}') AS flag_clients,
        COALESCE(ARRAY_AGG(client_name ORDER BY client_name) FILTER (WHERE NOT sticky_risk AND NOT is_flag), '{}') AS healthy_clients,
        COALESCE(ARRAY_AGG(client_name ORDER BY client_name) FILTER (WHERE sticky_risk AND recovered), '{}') AS recovered_clients
      FROM with_prior
      GROUP BY month_start
      ORDER BY month_start
    `);
    res.json(rows);
  } catch (err) {
    console.error('Risk trend error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Monthly risk detail (for drawer when clicking a month on trend chart)
app.get('/api/risk-trend/:month', async (req, res) => {
  try {
    if (!await guardManagerAccess(req, res, true)) return;
    const monthStart = req.params.month; // e.g. '2025-12-01'
    const { rows } = await pool.query(`
      WITH month_snapshots AS (
        SELECT
          COALESCE(c.parent_customer_id, s.customer_id) AS customer_id,
          COALESCE(p.name, c.name) AS client_name,
          s.snapshot_date,
          s.status,
          s.risk_type,
          s.risk_triggers,
          s.flag_triggers,
          s.flag_count
        FROM risk_status_snapshots s
        JOIN clients c ON c.customer_id = s.customer_id
        LEFT JOIN clients p ON p.customer_id = c.parent_customer_id
        WHERE DATE_TRUNC('month', s.snapshot_date) = $1::date
          AND s.snapshot_date >= COALESCE(c.start_date, '2020-01-01')
      ),
      client_summary AS (
        SELECT
          customer_id,
          client_name,
          COUNT(*) FILTER (WHERE status = 'Risk') AS risk_days,
          COUNT(*) FILTER (WHERE status = 'Flag') AS flag_days,
          COUNT(*) FILTER (WHERE status = 'Healthy') AS healthy_days,
          COUNT(*) AS total_days,
          -- Dominant status (most days)
          CASE
            WHEN COUNT(*) FILTER (WHERE status = 'Risk') >= COUNT(*) FILTER (WHERE status = 'Flag')
              AND COUNT(*) FILTER (WHERE status = 'Risk') >= COUNT(*) FILTER (WHERE status = 'Healthy')
              THEN 'Risk'
            WHEN COUNT(*) FILTER (WHERE status = 'Flag') >= COUNT(*) FILTER (WHERE status = 'Healthy')
              THEN 'Flag'
            ELSE 'Healthy'
          END AS dominant_status,
          -- Most common risk_type when in risk
          (SELECT ms2.risk_type FROM month_snapshots ms2
           WHERE ms2.customer_id = ms.customer_id AND ms2.status = 'Risk' AND ms2.risk_type != ''
           GROUP BY ms2.risk_type ORDER BY COUNT(*) DESC LIMIT 1) AS risk_type,
          -- Triggers from the last risk day (most recent picture)
          (SELECT ms2.risk_triggers FROM month_snapshots ms2
           WHERE ms2.customer_id = ms.customer_id AND ms2.status = 'Risk'
           ORDER BY ms2.snapshot_date DESC LIMIT 1) AS risk_triggers,
          (SELECT ms2.flag_triggers FROM month_snapshots ms2
           WHERE ms2.customer_id = ms.customer_id
           ORDER BY ms2.snapshot_date DESC LIMIT 1) AS flag_triggers,
          -- Peak flag count
          MAX(flag_count) AS max_flag_count
        FROM month_snapshots ms
        GROUP BY customer_id, client_name
      )
      SELECT * FROM client_summary
      ORDER BY
        CASE dominant_status WHEN 'Risk' THEN 1 WHEN 'Flag' THEN 2 ELSE 3 END,
        risk_days DESC,
        client_name
    `, [monthStart]);
    res.json(rows);
  } catch (err) {
    console.error('Risk trend detail error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Monthly client status grid (for heatmap)
app.get('/api/status-history', async (req, res) => {
  try {
    if (!await guardManagerAccess(req, res, true)) return;
    const { rows } = await pool.query(`
      WITH monthly AS (
        SELECT
          DATE_TRUNC('month', snapshot_date)::date AS month_start,
          COALESCE(c.parent_customer_id, s.customer_id) AS customer_id,
          COALESCE(p.name, c.name) AS client_name,
          COUNT(*) FILTER (WHERE s.status = 'Risk') AS risk_days,
          COUNT(*) FILTER (WHERE s.status = 'Flag') AS flag_days,
          COUNT(*) FILTER (WHERE s.status = 'Healthy') AS healthy_days,
          COUNT(*) AS total_days,
          EXTRACT(YEAR FROM AGE(DATE_TRUNC('month', snapshot_date) + INTERVAL '1 month', COALESCE(MIN(c.start_date), '2020-01-01'::date))) * 12
            + EXTRACT(MONTH FROM AGE(DATE_TRUNC('month', snapshot_date) + INTERVAL '1 month', COALESCE(MIN(c.start_date), '2020-01-01'::date))) AS months_since_start
        FROM risk_status_snapshots s
        JOIN clients c ON c.customer_id = s.customer_id
        LEFT JOIN clients p ON p.customer_id = c.parent_customer_id
        WHERE s.snapshot_date >= COALESCE(c.start_date, '2020-01-01')
        GROUP BY DATE_TRUNC('month', snapshot_date), COALESCE(c.parent_customer_id, s.customer_id), COALESCE(p.name, c.name)
      ),
      with_sticky AS (
        SELECT m.*,
          CASE
            WHEN months_since_start < 3 THEN risk_days >= 14
            WHEN risk_days >= 14 THEN true
            WHEN LAG(risk_days >= 14) OVER (PARTITION BY customer_id ORDER BY month_start)
              AND healthy_days < 14 THEN true
            ELSE false
          END AS sticky_risk
        FROM monthly m
      )
      SELECT
        customer_id,
        client_name,
        month_start,
        CASE
          WHEN sticky_risk THEN 'ads_risk'
          WHEN flag_days >= 14 THEN 'flag'
          ELSE 'healthy'
        END AS status,
        risk_days, flag_days, healthy_days, total_days
      FROM with_sticky
      ORDER BY client_name, month_start
    `);
    res.json(rows);
  } catch (err) {
    console.error('Status history error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Data health check
app.get('/api/health', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM v_client_data_health');
    res.json(rows);
  } catch (err) {
    console.error('Health check error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── Admin: Manager token management ───────────────────────────

// List manager tokens
app.get('/api/admin/manager-tokens', async (req, res) => {
  if (req.managerName) return res.status(403).json({ error: 'Admin only' });
  try {
    const { rows } = await pool.query('SELECT ads_manager, token FROM ads_manager_tokens ORDER BY ads_manager');
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Generate token for a manager
app.post('/api/admin/generate-manager-token', async (req, res) => {
  if (req.managerName) return res.status(403).json({ error: 'Admin only' });
  try {
    const { ads_manager } = req.body;
    if (!ads_manager) return res.status(400).json({ error: 'ads_manager required' });
    const token = crypto.randomBytes(24).toString('base64url');
    await pool.query(
      `INSERT INTO ads_manager_tokens (ads_manager, token) VALUES ($1, $2)
       ON CONFLICT (ads_manager) DO UPDATE SET token = $2`,
      [ads_manager, token]
    );
    _managerTokens = null; // bust cache
    res.json({ ads_manager, token, url: `/m/${token}` });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Cohort Performance ───────────────────────────────────────

app.get('/api/cohort', checkAuth, async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM get_cohort_metrics()');

    // Build seasonal index from campaign_daily_metrics
    const { rows: seasonalRows } = await pool.query(`
      WITH monthly AS (
        SELECT EXTRACT(MONTH FROM date)::int AS cal_month, customer_id,
          SUM(clicks) AS clicks, COUNT(DISTINCT date) AS days
        FROM campaign_daily_metrics WHERE date >= '2024-09-01'
        GROUP BY 1, 2
      )
      SELECT cal_month, ROUND(AVG(clicks / NULLIF(days, 0)::numeric), 1) AS avg_clicks
      FROM monthly WHERE clicks > 0
      GROUP BY cal_month ORDER BY cal_month
    `);

    const seasonalRaw = {};
    seasonalRows.forEach(r => { seasonalRaw[r.cal_month] = parseFloat(r.avg_clicks); });
    const annualAvg = Object.values(seasonalRaw).reduce((a, b) => a + b, 0) / 12;
    const seasonalIndex = {};
    for (let m = 1; m <= 12; m++) {
      seasonalIndex[m] = seasonalRaw[m] ? +(seasonalRaw[m] / annualAvg).toFixed(3) : 1;
    }

    // Group by client
    const clients = {};
    rows.forEach(r => {
      const id = String(r.customer_id);
      if (!clients[id]) {
        clients[id] = {
          customer_id: id,
          name: r.client_name,
          start_date: r.start_date,
          months: []
        };
      }
      clients[id].months.push({
        program_month: r.program_month,
        month_start: r.month_start,
        month_end: r.month_end,
        cal_month: r.cal_month,
        contacts: r.contacts,
        quality_leads: r.quality_leads,
        ad_spend: parseFloat(r.ad_spend) || 0,
        revenue: parseInt(r.revenue_cents) / 100,
        rev_per_lead: parseFloat(r.rev_per_lead) || 0,
        cpl: parseFloat(r.cpl) || 0,
        seasonal_index: seasonalIndex[r.cal_month] || 1,
        quality_leads_adjusted: seasonalIndex[r.cal_month]
          ? Math.round(r.quality_leads / seasonalIndex[r.cal_month])
          : r.quality_leads
      });
    });

    res.json({ clients: Object.values(clients), seasonal_index: seasonalIndex });
  } catch (err) {
    console.error('Cohort error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── Start ─────────────────────────────────────────────────────

app.listen(PORT, () => {
  console.log(`Risk Dashboard running on port ${PORT}`);
});
