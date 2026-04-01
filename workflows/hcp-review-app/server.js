require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const path = require('path');

const app = express();
const port = process.env.PORT || 3456;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: false,
});

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public'), {
  etag: false,
  maxAge: 0,
  setHeaders: (res) => {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
  }
}));

// ════════════════════════════════════════════════════════════
// Auth: login route + cookie-based middleware
// ════════════════════════════════════════════════════════════
const LOGIN_HTML = `<!DOCTYPE html><html><head><title>Login</title>
  <style>body{font-family:system-ui;display:flex;justify-content:center;align-items:center;min-height:100vh;background:#f1f5f9;margin:0}
  form{background:#fff;padding:2rem;border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,.1)}
  input{display:block;margin:0.5rem 0;padding:0.5rem;border:1px solid #cbd5e1;border-radius:4px;width:250px}
  button{background:#3b82f6;color:#fff;border:none;padding:0.5rem 1.5rem;border-radius:4px;cursor:pointer;margin-top:0.5rem}
  .error{color:#dc2626;font-size:0.85rem;margin-top:0.5rem}</style>
  </head><body><form method="POST" action="/login"><h3>Review Login</h3>
  <input type="password" name="password" placeholder="Password" autofocus>
  <button type="submit">Login</button></form></body></html>`;

app.get('/login', (req, res) => {
  res.send(LOGIN_HTML);
});

app.post('/login', (req, res) => {
  if (req.body.password === process.env.REVIEW_PASSWORD) {
    res.setHeader('Set-Cookie', 'review_auth=1; Path=/; HttpOnly; Max-Age=86400');
    return res.redirect(req.query.next || '/review');
  }
  res.send(LOGIN_HTML.replace('</form>', '<p class="error">Wrong password</p></form>'));
});

function requireAuth(req, res, next) {
  console.log(`[AUTH] ${req.method} ${req.path} cookie: ${req.headers.cookie || 'none'}`);
  if (req.headers.cookie && req.headers.cookie.includes('review_auth=1')) {
    return next();
  }
  // API calls get a JSON error; pages get the login form
  if (req.path.startsWith('/api/')) {
    return res.status(401).json({ error: 'Not authenticated' });
  }
  res.send(LOGIN_HTML.replace('action="/login"', 'action="/login?next=' + encodeURIComponent(req.originalUrl) + '"'));
}

// ════════════════════════════════════════════════════════════
// API: Get review queue
// ════════════════════════════════════════════════════════════
app.get('/api/queue', requireAuth, async (req, res) => {
  try {
    const { client, priority, flag, limit = 50, offset = 0 } = req.query;
    let where = 'WHERE 1=1';
    const params = [];
    let paramIdx = 1;

    if (client) {
      where += ` AND eq.customer_id = $${paramIdx++}`;
      params.push(client);
    }
    if (priority) {
      where += ` AND eq.review_priority = $${paramIdx++}`;
      params.push(priority);
    }
    if (flag) {
      where += ` AND $${paramIdx++} = ANY(eq.exception_flags)`;
      params.push(flag);
    }

    const countResult = await pool.query(
      `SELECT COUNT(*) FROM v_exception_queue eq ${where}`, params
    );

    params.push(parseInt(limit), parseInt(offset));
    const result = await pool.query(
      `SELECT * FROM v_exception_queue eq ${where} LIMIT $${paramIdx++} OFFSET $${paramIdx++}`,
      params
    );

    res.json({
      total: parseInt(countResult.rows[0].count),
      items: result.rows,
    });
  } catch (err) {
    console.error('Queue error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ════════════════════════════════════════════════════════════
// API: Get lead detail
// ════════════════════════════════════════════════════════════
app.get('/api/lead/:hcp_customer_id', requireAuth, async (req, res) => {
  try {
    const { hcp_customer_id } = req.params;

    const [customer, inspections, estimates, jobs, segments, invoices, reviews] = await Promise.all([
      pool.query(`SELECT hc.*, cl.name as client_name FROM hcp_customers hc JOIN clients cl ON cl.customer_id = hc.customer_id WHERE hc.hcp_customer_id = $1`, [hcp_customer_id]),
      pool.query(`SELECT * FROM hcp_inspections WHERE hcp_customer_id = $1 ORDER BY scheduled_at DESC`, [hcp_customer_id]),
      pool.query(`SELECT e.*, json_agg(json_build_object('id', eo.id, 'option_number', eo.option_number, 'name', eo.name, 'total_amount_cents', eo.total_amount_cents, 'status', eo.status, 'approval_status', eo.approval_status, 'notes', eo.notes, 'follow_up_date', eo.follow_up_date, 'follow_up_raw', eo.follow_up_raw)) as options FROM hcp_estimates e LEFT JOIN hcp_estimate_options eo ON eo.hcp_estimate_id = e.hcp_estimate_id WHERE e.hcp_customer_id = $1 GROUP BY e.id ORDER BY e.sent_at DESC`, [hcp_customer_id]),
      pool.query(`SELECT * FROM hcp_jobs WHERE hcp_customer_id = $1 ORDER BY scheduled_at DESC`, [hcp_customer_id]),
      pool.query(`SELECT * FROM hcp_job_segments WHERE hcp_customer_id = $1 ORDER BY invoice_number`, [hcp_customer_id]),
      pool.query(`SELECT * FROM hcp_invoices WHERE hcp_customer_id = $1 ORDER BY invoice_date DESC`, [hcp_customer_id]),
      pool.query(`SELECT * FROM lead_reviews WHERE hcp_customer_id = $1 ORDER BY performed_at DESC`, [hcp_customer_id]),
    ]);

    // Get CallRail call info if matched
    let callrail = null;
    if (customer.rows[0]?.callrail_id) {
      const cr = await pool.query(
        `SELECT callrail_id, caller_phone, customer_name, start_time, gclid, source, medium, classified_source FROM calls WHERE callrail_id = $1`,
        [customer.rows[0].callrail_id]
      );
      if (cr.rows.length) callrail = cr.rows[0];
    }

    res.json({
      customer: customer.rows[0],
      callrail,
      inspections: inspections.rows,
      estimates: estimates.rows,
      jobs: jobs.rows,
      segments: segments.rows,
      invoices: invoices.rows,
      reviews: reviews.rows,
    });
  } catch (err) {
    console.error('Lead detail error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ════════════════════════════════════════════════════════════
// API: Review actions (call PostgreSQL functions)
// ════════════════════════════════════════════════════════════
app.post('/api/review/confirm-match', requireAuth, async (req, res) => {
  try {
    const { hcp_customer_id, performed_by, notes } = req.body;
    await pool.query('SELECT review_confirm_match($1, $2, $3)', [hcp_customer_id, performed_by, notes]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/review/reject-match', requireAuth, async (req, res) => {
  try {
    const { hcp_customer_id, performed_by, reason } = req.body;
    await pool.query('SELECT review_reject_match($1, $2, $3)', [hcp_customer_id, performed_by, reason]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/review/approve-lead', requireAuth, async (req, res) => {
  try {
    const { hcp_customer_id, performed_by, notes } = req.body;
    await pool.query(`
      UPDATE hcp_customers
      SET review_status = 'confirmed', reviewed_by = $2, exception_flags = '{}', updated_at = NOW()
      WHERE hcp_customer_id = $1`, [hcp_customer_id, performed_by]);
    await pool.query(`
      INSERT INTO lead_reviews (hcp_customer_id, customer_id, action, performed_by, reason, notes, previous_status)
      SELECT $1, customer_id, 'approve_lead', $2, 'Approved via audit - no changes needed', $3, review_status
      FROM hcp_customers WHERE hcp_customer_id = $1`, [hcp_customer_id, performed_by, notes]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/review/flag-for-manager', requireAuth, async (req, res) => {
  try {
    const { hcp_customer_id, performed_by, notes } = req.body;
    await pool.query(`
      UPDATE hcp_customers
      SET review_status = 'flagged', reviewed_by = $2, updated_at = NOW()
      WHERE hcp_customer_id = $1`, [hcp_customer_id, performed_by]);
    await pool.query(`
      INSERT INTO lead_reviews (hcp_customer_id, customer_id, action, performed_by, reason, notes, previous_status)
      SELECT $1, customer_id, 'flag_for_manager', $2, 'Flagged for manager review', $3, review_status
      FROM hcp_customers WHERE hcp_customer_id = $1`, [hcp_customer_id, performed_by, notes]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/review/override-attribution', requireAuth, async (req, res) => {
  try {
    const { hcp_customer_id, performed_by, override_to, reason } = req.body;
    await pool.query('SELECT review_override_attribution($1, $2, $3, $4)', [hcp_customer_id, performed_by, override_to, reason]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/review/dismiss', requireAuth, async (req, res) => {
  try {
    const { hcp_customer_id, performed_by, flag, reason } = req.body;
    await pool.query('SELECT review_dismiss($1, $2, $3, $4)', [hcp_customer_id, performed_by, flag, reason]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/review/reclassify-to-inspection', requireAuth, async (req, res) => {
  try {
    const { hcp_job_id, performed_by, reason } = req.body;
    await pool.query('SELECT review_reclassify_to_inspection($1, $2, $3)', [hcp_job_id, performed_by, reason]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/review/reclassify-to-job', requireAuth, async (req, res) => {
  try {
    const { hcp_inspection_id, performed_by, reason } = req.body;
    await pool.query('SELECT review_reclassify_to_job($1, $2, $3)', [hcp_inspection_id, performed_by, reason]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/review/reclassify-estimate', requireAuth, async (req, res) => {
  try {
    const { hcp_estimate_id, estimate_type, performed_by } = req.body;
    await pool.query('UPDATE hcp_estimates SET estimate_type = $2, updated_at = NOW() WHERE hcp_estimate_id = $1', [hcp_estimate_id, estimate_type]);
    const cust = await pool.query('SELECT customer_id, hcp_customer_id FROM hcp_estimates WHERE hcp_estimate_id = $1', [hcp_estimate_id]);
    if (cust.rows[0]) {
      await pool.query(`INSERT INTO lead_reviews (hcp_customer_id, customer_id, action, performed_by, reason)
        VALUES ($1, $2, 'reclassify_estimate', $3, $4)`,
        [cust.rows[0].hcp_customer_id, cust.rows[0].customer_id, performed_by, `${hcp_estimate_id} → ${estimate_type}`]);
    }
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/review/dismiss-fragmentation', requireAuth, async (req, res) => {
  try {
    const { hcp_customer_id, performed_by, flag, notes } = req.body;
    await pool.query('SELECT review_dismiss_fragmentation($1, $2, $3, $4)', [hcp_customer_id, performed_by, flag, notes]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/review/estimates-as-options', requireAuth, async (req, res) => {
  try {
    const { main_id, option_ids, performed_by, notes } = req.body;
    await pool.query('SELECT review_estimates_as_options($1, $2, $3, $4)', [main_id, option_ids, performed_by, notes]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/review/jobs-as-segments', requireAuth, async (req, res) => {
  try {
    const { main_id, segment_ids, performed_by, notes } = req.body;
    await pool.query('SELECT review_jobs_as_segments($1, $2, $3, $4)', [main_id, segment_ids, performed_by, notes]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/review/inspections-as-segments', requireAuth, async (req, res) => {
  try {
    const { main_id, segment_ids, performed_by, notes } = req.body;
    await pool.query('SELECT review_inspections_as_segments($1, $2, $3, $4)', [main_id, segment_ids, performed_by, notes]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/review/unnest-inspection-segment', requireAuth, async (req, res) => {
  try {
    const { hcp_id, performed_by } = req.body;
    await pool.query('SELECT review_unnest_inspection_segment($1, $2)', [hcp_id, performed_by]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/review/unnest-estimate-option', requireAuth, async (req, res) => {
  try {
    const { hcp_estimate_id, performed_by } = req.body;
    await pool.query('SELECT review_unnest_estimate_option($1, $2)', [hcp_estimate_id, performed_by]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/review/exclude-record', requireAuth, async (req, res) => {
  try {
    const { table, record_id, performed_by, reason } = req.body;
    await pool.query('SELECT review_exclude_record($1, $2, $3, $4)', [table, record_id, performed_by, reason]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/review/toggle-revenue', requireAuth, async (req, res) => {
  try {
    const { table, record_id, count_revenue, performed_by } = req.body;
    const idCol = {jobs:'hcp_job_id', estimates:'hcp_estimate_id', inspections:'hcp_id', segments:'hcp_job_id'}[table];
    const tbl = table === 'segments' ? 'hcp_job_segments' : 'hcp_' + table;
    if (!idCol) return res.status(400).json({error:'Invalid table'});
    await pool.query(`UPDATE ${tbl} SET count_revenue = $1, updated_at = NOW() WHERE ${idCol} = $2`, [count_revenue, record_id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/review/unnest-segment', requireAuth, async (req, res) => {
  try {
    const { hcp_job_id, performed_by } = req.body;
    // Move segment back to a regular job
    const seg = await pool.query('SELECT * FROM hcp_job_segments WHERE hcp_job_id = $1', [hcp_job_id]);
    if (!seg.rows[0]) return res.status(404).json({error:'Segment not found'});
    const s = seg.rows[0];
    // Reactivate the job record if it exists
    await pool.query(`UPDATE hcp_jobs SET record_status = 'active', parent_job_id = NULL, updated_at = NOW() WHERE hcp_job_id = $1`, [hcp_job_id]);
    // If no job record exists, create one
    const existing = await pool.query('SELECT 1 FROM hcp_jobs WHERE hcp_job_id = $1', [hcp_job_id]);
    if (!existing.rows.length) {
      await pool.query(`INSERT INTO hcp_jobs (hcp_job_id, customer_id, hcp_customer_id, description, total_amount_cents, status, scheduled_at, completed_at, employee_name, employee_id, notes, record_status)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'active')`,
        [s.hcp_job_id, s.customer_id, s.hcp_customer_id, s.description, s.total_amount_cents, s.status, s.scheduled_at, s.completed_at, s.employee_name, s.employee_id, s.notes]);
    }
    // Remove from segments
    await pool.query('DELETE FROM hcp_job_segments WHERE hcp_job_id = $1', [hcp_job_id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/review/toggle-segment-revenue', requireAuth, async (req, res) => {
  try {
    const { hcp_job_id, count_revenue, performed_by } = req.body;
    await pool.query('UPDATE hcp_job_segments SET count_revenue = $1, updated_at = NOW() WHERE hcp_job_id = $2', [count_revenue, hcp_job_id]);
    // Audit
    const seg = await pool.query('SELECT customer_id, hcp_customer_id FROM hcp_job_segments WHERE hcp_job_id = $1', [hcp_job_id]);
    if (seg.rows[0]) {
      await pool.query(
        'INSERT INTO lead_reviews (hcp_customer_id, customer_id, action, performed_by, reason) VALUES ($1, $2, $3, $4, $5)',
        [seg.rows[0].hcp_customer_id, seg.rows[0].customer_id, count_revenue ? 'segment_count_revenue' : 'segment_exclude_revenue', performed_by, 'Segment ' + hcp_job_id]
      );
    }
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/review/set-followup', requireAuth, async (req, res) => {
  try {
    const { estimate_option_id, follow_up_date, performed_by, notes } = req.body;
    await pool.query('SELECT review_set_followup($1, $2, $3, $4)', [estimate_option_id, follow_up_date, performed_by, notes]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ════════════════════════════════════════════════════════════
// API: Get clients list (for filter dropdown)
// ════════════════════════════════════════════════════════════
app.get('/api/clients', requireAuth, async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT customer_id, name FROM clients WHERE field_management_software = 'housecall_pro' AND hcp_api_key IS NOT NULL ORDER BY name`
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Client Flags endpoint (for VA review portal) ──
app.get('/api/audit/client-flags', requireAuth, async (req, res) => {
  try {
    const { customer_id, limit = 100, offset = 0 } = req.query;
    let whereClause = "lr.action = 'client_flag'";
    const params = [];
    let paramIdx = 1;

    if (customer_id && customer_id !== 'all') {
      whereClause += ` AND lr.customer_id = $${paramIdx}`;
      params.push(customer_id);
      paramIdx++;
    }

    const countResult = await pool.query(
      `SELECT COUNT(*) FROM lead_reviews lr WHERE ${whereClause}`, params
    );

    params.push(parseInt(limit), parseInt(offset));
    const result = await pool.query(`
      SELECT
        lr.id, lr.hcp_customer_id, lr.customer_id, lr.action,
        lr.performed_by, lr.performed_at, lr.reason, lr.notes,
        c.name as client_name,
        CASE WHEN lr.hcp_customer_id IS NOT NULL THEN
          (SELECT hc.first_name || ' ' || hc.last_name FROM hcp_customers hc WHERE hc.hcp_customer_id = lr.hcp_customer_id)
        ELSE NULL END as lead_name,
        CASE WHEN lr.hcp_customer_id IS NOT NULL THEN
          (SELECT hc.review_status FROM hcp_customers hc WHERE hc.hcp_customer_id = lr.hcp_customer_id)
        ELSE NULL END as review_status
      FROM lead_reviews lr
      LEFT JOIN clients c ON c.customer_id = lr.customer_id
      WHERE ${whereClause}
      ORDER BY lr.performed_at DESC
      LIMIT $${paramIdx++} OFFSET $${paramIdx++}
    `, params);

    res.json({ total: parseInt(countResult.rows[0].count), items: result.rows });
  } catch (err) {
    console.error('Client flags error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ════════════════════════════════════════════════════════════
// API: Audit data for a client
// ════════════════════════════════════════════════════════════
app.get('/api/audit/:customer_id', requireAuth, async (req, res) => {
  try {
    const cid = req.params.customer_id;

    // Summary metrics
    const summary = await pool.query(`
      SELECT
        COUNT(*) as leads,
        COUNT(*) FILTER (WHERE lr.lead_status = 'in_funnel') as in_funnel,
        COUNT(*) FILTER (WHERE lr.lead_status = 'lead_only') as lead_only,
        COUNT(*) FILTER (WHERE lr.roas_revenue_cents > 0) as leads_with_revenue,
        ROUND(SUM(lr.roas_revenue_cents) / 100.0, 2) as roas_revenue,
        ROUND(SUM(lr.pipeline_estimate_cents) / 100.0, 2) as pipeline_revenue,
        ROUND(SUM(lr.roas_revenue_cents) FILTER (WHERE lr.revenue_source = 'invoice') / 100.0, 2) as roas_from_invoices,
        ROUND(SUM(lr.roas_revenue_cents) FILTER (WHERE lr.revenue_source = 'approved_estimate') / 100.0, 2) as roas_from_estimates,
        ROUND(SUM(lr.roas_revenue_cents) FILTER (WHERE lr.revenue_source = 'job') / 100.0, 2) as roas_from_jobs
      FROM v_lead_revenue lr WHERE lr.customer_id = $1
    `, [cid]);

    const funnelCounts = await pool.query(`
      SELECT
        (SELECT COUNT(*) FROM hcp_inspections i JOIN v_lead_revenue lr ON lr.hcp_customer_id = i.hcp_customer_id WHERE i.customer_id = $1 AND i.record_status = 'active') as inspections,
        (SELECT COUNT(DISTINCT COALESCE(i.service_address, i.hcp_id)) FROM hcp_inspections i JOIN v_lead_revenue lr ON lr.hcp_customer_id = i.hcp_customer_id WHERE i.customer_id = $1 AND i.record_status = 'active') as inspections_unique,
        (SELECT COUNT(*) FROM hcp_inspections i JOIN v_lead_revenue lr ON lr.hcp_customer_id = i.hcp_customer_id WHERE i.customer_id = $1 AND i.record_status = 'active' AND i.status IN ('complete rated','complete unrated')) as inspections_completed,
        (SELECT COUNT(*) FROM hcp_estimates e JOIN v_lead_revenue lr ON lr.hcp_customer_id = e.hcp_customer_id WHERE e.customer_id = $1 AND e.record_status = 'active' AND e.estimate_type = 'treatment') as treatment_estimates,
        (SELECT COUNT(*) FROM hcp_estimates e JOIN v_lead_revenue lr ON lr.hcp_customer_id = e.hcp_customer_id WHERE e.customer_id = $1 AND e.record_status = 'active' AND e.status = 'approved') as estimates_approved,
        (SELECT COUNT(*) FROM hcp_jobs j JOIN v_lead_revenue lr ON lr.hcp_customer_id = j.hcp_customer_id WHERE j.customer_id = $1 AND j.record_status = 'active' AND j.status NOT IN ('user canceled','pro canceled')) as jobs,
        (SELECT COUNT(DISTINCT COALESCE(j.service_address, j.hcp_job_id)) FROM hcp_jobs j JOIN v_lead_revenue lr ON lr.hcp_customer_id = j.hcp_customer_id WHERE j.customer_id = $1 AND j.record_status = 'active' AND j.status NOT IN ('user canceled','pro canceled')) as jobs_unique,
        (SELECT COUNT(*) FROM hcp_jobs j JOIN v_lead_revenue lr ON lr.hcp_customer_id = j.hcp_customer_id WHERE j.customer_id = $1 AND j.record_status = 'active' AND j.status IN ('complete rated','complete unrated')) as jobs_completed
    `, [cid]);

    // Issue counts
    const issues = await pool.query(`
      SELECT * FROM (
        SELECT 'misclass_insp_to_job' as issue, COUNT(*) as count
        FROM hcp_inspections i JOIN v_lead_revenue lr ON lr.hcp_customer_id = i.hcp_customer_id
        WHERE i.customer_id = $1 AND i.record_status = 'active' AND i.total_amount_cents >= 100000
        UNION ALL
        SELECT 'misclass_job_to_insp', COUNT(*)
        FROM hcp_jobs j JOIN v_lead_revenue lr ON lr.hcp_customer_id = j.hcp_customer_id
        WHERE j.customer_id = $1 AND j.record_status = 'active' AND j.total_amount_cents > 0 AND j.total_amount_cents < 10000
        UNION ALL
        SELECT 'multi_inspections', COUNT(*)
        FROM (SELECT i.hcp_customer_id FROM hcp_inspections i JOIN v_lead_revenue lr ON lr.hcp_customer_id = i.hcp_customer_id WHERE i.customer_id = $1 AND i.record_status = 'active' GROUP BY 1 HAVING COUNT(*) > COUNT(DISTINCT i.service_address)) sub
        UNION ALL
        SELECT 'multi_jobs', COUNT(*)
        FROM (SELECT j.hcp_customer_id FROM hcp_jobs j JOIN v_lead_revenue lr ON lr.hcp_customer_id = j.hcp_customer_id WHERE j.customer_id = $1 AND j.record_status = 'active' GROUP BY 1 HAVING COUNT(*) > COUNT(DISTINCT j.service_address)) sub
        UNION ALL
        SELECT 'multi_estimates', COUNT(*)
        FROM (SELECT e.hcp_customer_id FROM hcp_estimates e JOIN v_lead_revenue lr ON lr.hcp_customer_id = e.hcp_customer_id WHERE e.customer_id = $1 AND e.record_status = 'active' GROUP BY 1 HAVING COUNT(*) > 1) sub
        UNION ALL
        SELECT 'zero_amount_estimates', COUNT(*)
        FROM hcp_estimates e JOIN v_lead_revenue lr ON lr.hcp_customer_id = e.hcp_customer_id
        WHERE e.customer_id = $1 AND e.record_status = 'active' AND e.highest_option_cents = 0 AND e.estimate_type = 'treatment'
        UNION ALL
        SELECT 'zero_amount_jobs', COUNT(*)
        FROM hcp_jobs j JOIN v_lead_revenue lr ON lr.hcp_customer_id = j.hcp_customer_id
        WHERE j.customer_id = $1 AND j.record_status = 'active' AND j.total_amount_cents = 0
        UNION ALL
        SELECT 'job_no_estimate', COUNT(DISTINCT j.hcp_customer_id)
        FROM hcp_jobs j JOIN v_lead_revenue lr ON lr.hcp_customer_id = j.hcp_customer_id
        WHERE j.customer_id = $1 AND j.record_status = 'active'
          AND NOT EXISTS (SELECT 1 FROM hcp_estimates e WHERE e.hcp_customer_id = j.hcp_customer_id AND e.record_status = 'active')
        UNION ALL
        SELECT 'name_mismatch', COUNT(*)
        FROM hcp_customers hc JOIN v_lead_revenue lr ON lr.hcp_customer_id = hc.hcp_customer_id
        WHERE hc.customer_id = $1 AND 'name_mismatch' = ANY(hc.exception_flags) AND hc.review_status = 'pending'
        UNION ALL
        SELECT 'insp_estimate_as_treatment', COUNT(*)
        FROM hcp_estimates e JOIN v_lead_revenue lr ON lr.hcp_customer_id = e.hcp_customer_id
        WHERE e.customer_id = $1 AND e.record_status = 'active' AND e.estimate_type = 'inspection'
      ) issues WHERE count > 0
    `, [cid]);

    res.json({
      summary: summary.rows[0],
      funnel: funnelCounts.rows[0],
      issues: issues.rows,
    });
  } catch (err) {
    console.error('Audit error:', err);
    res.status(500).json({ error: err.message });
  }
});

// API: Get issue details (the actual records for a specific issue)
app.get('/api/audit/:customer_id/issue/:issue', requireAuth, async (req, res) => {
  try {
    const { customer_id: cid, issue } = req.params;
    let result;

    switch (issue) {
      case 'misclass_insp_to_job':
        result = await pool.query(`
          SELECT i.hcp_id, i.hcp_customer_id, hc.first_name, hc.last_name, i.description, i.total_amount_cents/100 as amount, i.status, i.scheduled_at
          FROM hcp_inspections i
          JOIN v_lead_revenue lr ON lr.hcp_customer_id = i.hcp_customer_id
          JOIN hcp_customers hc ON hc.hcp_customer_id = i.hcp_customer_id
          WHERE i.customer_id = $1 AND i.record_status = 'active' AND i.total_amount_cents >= 100000
          ORDER BY i.total_amount_cents DESC`, [cid]);
        break;
      case 'misclass_job_to_insp':
        result = await pool.query(`
          SELECT j.hcp_job_id, j.hcp_customer_id, hc.first_name, hc.last_name, j.description, j.total_amount_cents/100 as amount, j.status, j.scheduled_at
          FROM hcp_jobs j
          JOIN v_lead_revenue lr ON lr.hcp_customer_id = j.hcp_customer_id
          JOIN hcp_customers hc ON hc.hcp_customer_id = j.hcp_customer_id
          WHERE j.customer_id = $1 AND j.record_status = 'active' AND j.total_amount_cents > 0 AND j.total_amount_cents < 10000
          ORDER BY j.total_amount_cents ASC`, [cid]);
        break;
      case 'multi_inspections':
      case 'multi_jobs':
      case 'multi_estimates':
        const table = issue === 'multi_inspections' ? 'hcp_inspections' : issue === 'multi_jobs' ? 'hcp_jobs' : 'hcp_estimates';
        const idCol = issue === 'multi_inspections' ? 'hcp_id' : issue === 'multi_jobs' ? 'hcp_job_id' : 'hcp_estimate_id';
        result = await pool.query(`
          SELECT hc.hcp_customer_id, hc.first_name, hc.last_name, sub.record_count,
                 sub.descriptions, sub.total_amount
          FROM (
            SELECT t.hcp_customer_id, COUNT(*) as record_count,
                   string_agg(COALESCE(LEFT(t.description, 50), t.status, ''), ' | ') as descriptions,
                   SUM(t.total_amount_cents)/100 as total_amount
            FROM ${table} t
            JOIN v_lead_revenue lr ON lr.hcp_customer_id = t.hcp_customer_id
            WHERE t.customer_id = $1 AND t.record_status = 'active'
            GROUP BY t.hcp_customer_id HAVING COUNT(*) > 1
          ) sub
          JOIN hcp_customers hc ON hc.hcp_customer_id = sub.hcp_customer_id
          ORDER BY sub.record_count DESC`, [cid]);
        break;
      case 'zero_amount_estimates':
        result = await pool.query(`
          SELECT e.hcp_estimate_id, e.hcp_customer_id, hc.first_name, hc.last_name, e.status, e.estimate_type, e.sent_at, e.employee_name
          FROM hcp_estimates e
          JOIN v_lead_revenue lr ON lr.hcp_customer_id = e.hcp_customer_id
          JOIN hcp_customers hc ON hc.hcp_customer_id = e.hcp_customer_id
          WHERE e.customer_id = $1 AND e.record_status = 'active' AND e.highest_option_cents = 0 AND e.estimate_type = 'treatment'
          ORDER BY e.sent_at DESC`, [cid]);
        break;
      case 'zero_amount_jobs':
        result = await pool.query(`
          SELECT j.hcp_job_id, j.hcp_customer_id, hc.first_name, hc.last_name, j.description, j.status, j.scheduled_at, j.employee_name
          FROM hcp_jobs j
          JOIN v_lead_revenue lr ON lr.hcp_customer_id = j.hcp_customer_id
          JOIN hcp_customers hc ON hc.hcp_customer_id = j.hcp_customer_id
          WHERE j.customer_id = $1 AND j.record_status = 'active' AND j.total_amount_cents = 0
          ORDER BY j.scheduled_at DESC`, [cid]);
        break;
      case 'job_no_estimate':
        result = await pool.query(`
          SELECT DISTINCT ON (j.hcp_customer_id) j.hcp_job_id, j.hcp_customer_id, hc.first_name, hc.last_name,
                 j.description, j.total_amount_cents/100 as amount, j.status
          FROM hcp_jobs j
          JOIN v_lead_revenue lr ON lr.hcp_customer_id = j.hcp_customer_id
          JOIN hcp_customers hc ON hc.hcp_customer_id = j.hcp_customer_id
          WHERE j.customer_id = $1 AND j.record_status = 'active'
            AND NOT EXISTS (SELECT 1 FROM hcp_estimates e WHERE e.hcp_customer_id = j.hcp_customer_id AND e.record_status = 'active')
          ORDER BY j.hcp_customer_id, j.total_amount_cents DESC`, [cid]);
        break;
      case 'name_mismatch':
        result = await pool.query(`
          SELECT hc.hcp_customer_id, hc.first_name, hc.last_name, hc.phone_primary, c.customer_name as callrail_name, c.caller_phone
          FROM hcp_customers hc
          JOIN v_lead_revenue lr ON lr.hcp_customer_id = hc.hcp_customer_id
          JOIN calls c ON c.callrail_id = hc.callrail_id
          WHERE hc.customer_id = $1 AND 'name_mismatch' = ANY(hc.exception_flags) AND hc.review_status = 'pending'
          ORDER BY hc.last_name`, [cid]);
        break;
      default:
        result = { rows: [] };
    }

    res.json(result.rows);
  } catch (err) {
    console.error('Issue detail error:', err);
    res.status(500).json({ error: err.message });
  }
});

// API: All leads for a client with filters
app.get('/api/audit/:customer_id/leads', requireAuth, async (req, res) => {
  try {
    const cid = req.params.customer_id;
    const isAll = cid === 'all';
    const { limit = 100, offset = 0, filter = 'all', search = '' } = req.query;

    let extraWhere = '';
    const params = [];
    let paramIdx = 1;
    if (!isAll) {
      extraWhere += ` AND hc.customer_id = $${paramIdx}`;
      params.push(cid);
      paramIdx++;
    }
    // For "all clients" view, only show last 6 months with reviewable flags
    if (isAll) {
      extraWhere += ` AND hc.hcp_created_at >= NOW() - INTERVAL '6 months'`;
    }

    if (filter === 'in_funnel') {
      extraWhere += ` AND lr.lead_status = 'in_funnel'`;
    } else if (filter === 'lead_only') {
      extraWhere += ` AND lr.lead_status = 'lead_only'`;
    } else if (filter === 'issues') {
      extraWhere += ` AND (
        (SELECT COUNT(*) FROM hcp_inspections i WHERE i.hcp_customer_id = lr.hcp_customer_id AND i.record_status = 'active') > (SELECT COUNT(DISTINCT i2.service_address) FROM hcp_inspections i2 WHERE i2.hcp_customer_id = lr.hcp_customer_id AND i2.record_status = 'active')
        OR (SELECT COUNT(*) FROM hcp_estimates e WHERE e.hcp_customer_id = lr.hcp_customer_id AND e.record_status = 'active') > 1
        OR (SELECT COUNT(*) FROM hcp_jobs j WHERE j.hcp_customer_id = lr.hcp_customer_id AND j.record_status = 'active') > (SELECT COUNT(DISTINCT j2.service_address) FROM hcp_jobs j2 WHERE j2.hcp_customer_id = lr.hcp_customer_id AND j2.record_status = 'active')
        OR 'name_mismatch' = ANY(hc.exception_flags)
        OR 'classification_fallback' = ANY(hc.exception_flags)
      )`;
    } else if (filter === 'needs_review') {
      extraWhere += ` AND COALESCE(hc.review_status, 'pending') = 'pending'`;
    } else if (filter === 'va_needs_review') {
      extraWhere += ` AND COALESCE(hc.review_status, 'pending') = 'pending' AND (
        hc.exception_flags && ARRAY['missing_funnel_step', 'job_no_estimate', 'segment_revenue_suspicious', 'pre_lead']
        OR (lr.roas_revenue_cents = 0 AND (SELECT COUNT(*) FROM hcp_jobs j WHERE j.hcp_customer_id = lr.hcp_customer_id AND j.record_status = 'active' AND j.status NOT IN ('user canceled','pro canceled')) > 0)
        OR (
          (SELECT COUNT(*) FROM hcp_inspections i WHERE i.hcp_customer_id = lr.hcp_customer_id AND i.record_status = 'active') > 1
          OR (SELECT COUNT(*) FROM hcp_estimates e WHERE e.hcp_customer_id = lr.hcp_customer_id AND e.record_status = 'active') > 1
          OR (SELECT COUNT(*) FROM hcp_jobs j WHERE j.hcp_customer_id = lr.hcp_customer_id AND j.record_status = 'active') > 1
        )
      )`;
    } else if (filter === 'approved') {
      extraWhere += ` AND hc.review_status IN ('confirmed','overridden','resolved')`;
    } else if (filter === 'revenue') {
      extraWhere += ' AND lr.roas_revenue_cents > 0';
    } else if (filter === 'no_revenue') {
      extraWhere += ' AND lr.roas_revenue_cents = 0';
    }

    const { source: sourceFilter } = req.query;
    if (sourceFilter && sourceFilter !== 'all') {
      extraWhere += ` AND COALESCE(hc.attribution_override, CASE
        WHEN EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source_name = 'LSA') THEN 'lsa'
        WHEN EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads')) THEN 'google_ads'
        ELSE 'unknown'
      END) = $${paramIdx}`;
      params.push(sourceFilter);
      paramIdx++;
    }

    if (search) {
      extraWhere += ` AND (hc.first_name ILIKE $${paramIdx} OR hc.last_name ILIKE $${paramIdx} OR hc.phone_primary ILIKE $${paramIdx})`;
      params.push('%' + search + '%');
      paramIdx++;
    }

    const countResult = await pool.query(
      `SELECT COUNT(*) FROM v_lead_revenue lr LEFT JOIN hcp_customers hc ON hc.hcp_customer_id = lr.hcp_customer_id WHERE 1=1 ${extraWhere}`, params
    );

    params.push(parseInt(limit), parseInt(offset));
    const result = await pool.query(`
      SELECT
        lr.hcp_customer_id, lr.first_name, lr.last_name, lr.match_method, lr.revenue_source,
        lr.lead_status, lr.lead_source_type, lr.callrail_id as lr_callrail_id,
        lr.roas_revenue_cents/100 as roas_revenue,
        lr.invoice_total_cents/100 as invoice_total,
        lr.job_total_cents/100 as job_total,
        lr.approved_estimate_cents/100 as approved_estimate,
        lr.pipeline_estimate_cents/100 as pipeline_estimate,
        COALESCE(hc.phone_primary, (SELECT normalize_phone(c.caller_phone) FROM calls c WHERE c.callrail_id = lr.callrail_id LIMIT 1)) as phone_primary,
        COALESCE(hc.email, (SELECT f.customer_email FROM form_submissions f WHERE f.callrail_id = lr.callrail_id LIMIT 1)) as email,
        hc.lead_source, COALESCE(hc.review_status, 'pending') as review_status, hc.exception_flags,
        hc.hcp_created_at,
        (SELECT MIN(c.start_time) FROM calls c WHERE c.callrail_id = lr.callrail_id) as callrail_date,
        CASE WHEN lr.hcp_customer_id IS NOT NULL THEN
          (SELECT COUNT(*) FROM hcp_inspections i WHERE i.hcp_customer_id = lr.hcp_customer_id AND i.record_status = 'active')
        ELSE 0 END as insp_count,
        CASE WHEN lr.hcp_customer_id IS NOT NULL THEN
          (SELECT COUNT(DISTINCT i.service_address) FROM hcp_inspections i WHERE i.hcp_customer_id = lr.hcp_customer_id AND i.record_status = 'active')
        ELSE 0 END as insp_addr_count,
        CASE WHEN lr.hcp_customer_id IS NOT NULL THEN
          (SELECT COUNT(*) FROM hcp_estimates e WHERE e.hcp_customer_id = lr.hcp_customer_id AND e.record_status = 'active')
        ELSE 0 END as est_count,
        CASE WHEN lr.hcp_customer_id IS NOT NULL THEN
          (SELECT COUNT(*) FROM hcp_jobs j WHERE j.hcp_customer_id = lr.hcp_customer_id AND j.record_status = 'active')
        ELSE 0 END as job_count,
        CASE WHEN lr.hcp_customer_id IS NOT NULL THEN
          (SELECT COUNT(DISTINCT j.service_address) FROM hcp_jobs j WHERE j.hcp_customer_id = lr.hcp_customer_id AND j.record_status = 'active')
        ELSE 0 END as job_addr_count
      FROM v_lead_revenue lr
      LEFT JOIN hcp_customers hc ON hc.hcp_customer_id = lr.hcp_customer_id
      WHERE 1=1 ${extraWhere}
      ORDER BY ${filter === 'va_needs_review' ? 'COALESCE(array_length(hc.exception_flags, 1), 0) DESC, lr.roas_revenue_cents ASC' : 'lr.roas_revenue_cents DESC'}, lr.lead_status ASC
      LIMIT $${paramIdx++} OFFSET $${paramIdx++}
    `, params);

    res.json({ total: parseInt(countResult.rows[0].count), items: result.rows });
  } catch (err) {
    console.error('Leads error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ════════════════════════════════════════════════════════════
// Client Portal: token-based auth + read-only API + flagging
// ════════════════════════════════════════════════════════════
const crypto = require('crypto');

// Middleware: resolve client token → customer_id
async function requireClientToken(req, res, next) {
  const token = req.headers['x-client-token'] || req.query.token;
  if (!token) return res.status(401).json({ error: 'Missing client token' });
  try {
    const result = await pool.query(
      `SELECT customer_id, name, start_date, field_management_software FROM clients WHERE client_portal_token = $1`, [token]
    );
    if (!result.rows.length) return res.status(401).json({ error: 'Invalid token' });
    req.clientId = result.rows[0].customer_id;
    req.clientName = result.rows[0].name;
    req.clientStartDate = result.rows[0].start_date;
    req.clientFMS = result.rows[0].field_management_software;
    next();
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

// Serve the client portal page (no auth needed — token is in URL, validated via API)
app.get('/portal/:token', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'client.html'));
});

// Client portal: get client info
app.get('/api/client-portal/info', requireClientToken, async (req, res) => {
  // Load per-client dashboard config (source tabs)
  const client = await pool.query(
    'SELECT has_lsa, dashboard_config FROM clients WHERE customer_id = $1',
    [req.clientId]
  );
  const row = client.rows[0] || {};
  const config = row.dashboard_config || {};

  // Default tabs based on client features
  const defaultTabs = [
    { key: 'all', label: 'Full Business' },
    { key: 'google_ads', label: 'Google Ads' },
  ];
  if (row.has_lsa) defaultTabs.push({ key: 'lsa', label: 'LSA' });
  defaultTabs.push({ key: 'gbp', label: 'Google Business Profile' });

  res.json({
    client_name: req.clientName,
    customer_id: req.clientId,
    start_date: req.clientStartDate,
    source_tabs: config.source_tabs || defaultTabs,
  });
});

// Client portal: summary stats (fast — bypasses v_lead_revenue)
app.get('/api/client-portal/summary', requireClientToken, async (req, res) => {
  try {
    const cid = req.clientId;
    const { date_from, date_to } = req.query;
    const params = [cid];
    let paramIdx = 2;

    let hcDateWhere = '';
    let crDateWhere = '';
    if (date_from) {
      hcDateWhere += ` AND hc.hcp_created_at >= $${paramIdx}`;
      crDateWhere += ` AND c2.start_time >= $${paramIdx}`;
      params.push(date_from);
      paramIdx++;
    }
    if (date_to) {
      hcDateWhere += ` AND hc.hcp_created_at < ($${paramIdx}::date + 1)`;
      crDateWhere += ` AND c2.start_time < ($${paramIdx}::date + 1)`;
      params.push(date_to);
      paramIdx++;
    }

    const result = await pool.query(`
      WITH matched AS (
        SELECT
          hc.hcp_customer_id,
          hc.review_status,
          COALESCE((SELECT SUM(i.amount_cents) FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'inspection'), 0) as insp_invoice_cents,
          COALESCE((SELECT SUM(i.amount_cents) FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'treatment'), 0) as treat_invoice_cents,
          COALESCE((SELECT SUM(e.approved_total_cents) FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'approved' AND e.record_status = 'active' AND e.count_revenue = true), 0) as approved_estimate_cents,
          COALESCE((SELECT SUM(j.total_amount_cents) FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status NOT IN ('user canceled','pro canceled') AND j.count_revenue = true), 0) as job_cents,
          COALESCE((SELECT SUM(ins.total_amount_cents) FROM hcp_inspections ins WHERE ins.hcp_customer_id = hc.hcp_customer_id AND ins.record_status = 'active' AND ins.count_revenue = true AND ins.status NOT IN ('user canceled','pro canceled')), 0) as insp_total_cents,
          COALESCE((SELECT SUM(e.highest_option_cents) FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'sent' AND e.record_status = 'active' AND e.estimate_type = 'treatment' AND e.count_revenue = true), 0) as pipeline_cents
        FROM hcp_customers hc
        WHERE hc.customer_id = $1 ${hcDateWhere}
          AND (hc.attribution_override IS NOT NULL
            OR EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads' OR c.source_name = 'LSA'))
            OR EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND (f.gclid IS NOT NULL OR f.source ILIKE '%google%')))
          AND COALESCE(hc.client_flag_reason, '') NOT IN ('spam', 'out_of_area', 'wrong_service')
      ),
      unmatched_calls AS (
        SELECT COUNT(*) as cnt FROM (
          SELECT DISTINCT ON (normalize_phone(c2.caller_phone)) c2.callrail_id, c2.caller_phone
          FROM calls c2
          WHERE c2.customer_id = $1 ${crDateWhere}
            AND (c2.gclid IS NOT NULL OR c2.classified_source = 'google_ads' OR c2.source_name = 'LSA')
          ORDER BY normalize_phone(c2.caller_phone), c2.start_time
        ) c
        WHERE NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.callrail_id = c.callrail_id AND hc.customer_id = $1)
          AND NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.phone_normalized = normalize_phone(c.caller_phone) AND hc.customer_id = $1)
          AND NOT EXISTS (SELECT 1 FROM client_flagged_leads fl WHERE fl.customer_id = $1 AND fl.flag_reason IN ('spam','out_of_area','wrong_service') AND (fl.phone_normalized = normalize_phone(c.caller_phone) OR fl.callrail_id = c.callrail_id))
      ),
      unmatched_forms AS (
        SELECT COUNT(*) as cnt FROM (
          SELECT DISTINCT ON (normalize_phone(f2.customer_phone)) f2.callrail_id, f2.customer_phone
          FROM form_submissions f2
          WHERE f2.customer_id = $1 ${crDateWhere.replace(/c2\.start_time/g, 'f2.submitted_at')}
            AND (f2.gclid IS NOT NULL OR f2.source ILIKE '%google%')
          ORDER BY normalize_phone(f2.customer_phone), f2.submitted_at
        ) f
        WHERE NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.callrail_id = f.callrail_id AND hc.customer_id = $1)
          AND NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.phone_normalized = normalize_phone(f.customer_phone) AND hc.customer_id = $1)
          AND NOT EXISTS (SELECT 1 FROM client_flagged_leads fl WHERE fl.customer_id = $1 AND fl.flag_reason IN ('spam','out_of_area','wrong_service') AND (fl.phone_normalized = normalize_phone(f.customer_phone) OR fl.callrail_id = f.callrail_id))
      )
      SELECT
        (SELECT COUNT(*) FROM matched) + (SELECT cnt FROM unmatched_calls) + (SELECT cnt FROM unmatched_forms) as total_leads,
        (SELECT COUNT(*) FROM matched) as in_funnel,
        (SELECT cnt FROM unmatched_calls) + (SELECT cnt FROM unmatched_forms) as lead_only,
        (SELECT COUNT(*) FROM matched WHERE GREATEST(insp_invoice_cents, treat_invoice_cents, approved_estimate_cents, job_cents, insp_total_cents) > 0) as leads_with_revenue,
        ROUND((SELECT COALESCE(SUM(
          insp_invoice_cents +
          CASE
            WHEN treat_invoice_cents > 0 OR approved_estimate_cents > 0
              THEN GREATEST(treat_invoice_cents, approved_estimate_cents)
            ELSE job_cents + insp_total_cents
          END
        ), 0) FROM matched) / 100.0, 2) as roas_revenue,
        ROUND((SELECT COALESCE(SUM(pipeline_cents), 0) FROM matched) / 100.0, 2) as pipeline_revenue,
        (SELECT COUNT(*) FROM matched WHERE review_status IN ('approved','confirmed','overridden','resolved')) as audited
    `, params);

    const row = result.rows[0];
    res.json({
      total_leads: parseInt(row.total_leads),
      in_funnel: parseInt(row.in_funnel),
      lead_only: parseInt(row.lead_only),
      leads_with_revenue: parseInt(row.leads_with_revenue),
      roas_revenue: parseFloat(row.roas_revenue || 0),
      pipeline_revenue: parseFloat(row.pipeline_revenue || 0),
      audited: parseInt(row.audited),
    });
  } catch (err) {
    console.error('Client summary error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Client portal: leads list (fast — direct query, includes calls + forms)
app.get('/api/client-portal/leads', requireClientToken, async (req, res) => {
  try {
    const cid = req.clientId;
    const { limit = 25, offset = 0, filter = 'all', search = '', source, date_from, date_to } = req.query;

    // Build the UNION query for all lead types
    const params = [cid];
    let paramIdx = 2;

    // Shared date params — collect them first so both halves reference the same $N
    let dateParams = [];
    if (date_from) { dateParams.push({ idx: paramIdx++, val: date_from }); params.push(date_from); }
    if (date_to)   { dateParams.push({ idx: paramIdx++, val: date_to });   params.push(date_to); }

    const dfIdx = dateParams.find(d => d.val === date_from)?.idx;
    const dtIdx = dateParams.find(d => d.val === date_to)?.idx;

    let hcDateWhere = '';
    let crDateWhere = '';
    let fmDateWhere = '';
    if (dfIdx) { hcDateWhere += ` AND hc.hcp_created_at >= $${dfIdx}`; crDateWhere += ` AND c2.start_time >= $${dfIdx}`; fmDateWhere += ` AND f2.submitted_at >= $${dfIdx}`; }
    if (dtIdx) { hcDateWhere += ` AND hc.hcp_created_at < ($${dtIdx}::date + 1)`; crDateWhere += ` AND c2.start_time < ($${dtIdx}::date + 1)`; fmDateWhere += ` AND f2.submitted_at < ($${dtIdx}::date + 1)`; }

    // Source filter
    let sourceFilter = '';
    if (source && source !== 'all') {
      sourceFilter = ` AND lead_source_type = $${paramIdx}`;
      params.push(source);
      paramIdx++;
    }

    // Search filter
    let searchFilter = '';
    if (search) {
      searchFilter = ` AND (first_name ILIKE $${paramIdx} OR last_name ILIKE $${paramIdx})`;
      params.push('%' + search + '%');
      paramIdx++;
    }

    // Status filter
    let statusFilter = '';
    if (filter === 'in_funnel') statusFilter = ` AND lead_status = 'in_funnel'`;
    else if (filter === 'lead_only') statusFilter = ` AND lead_status = 'lead_only'`;
    else if (filter === 'revenue') statusFilter = ` AND roas_revenue > 0`;
    else if (filter === 'pipeline') statusFilter = ` AND pipeline_estimate > 0`;
    else if (filter === 'approved') statusFilter = ` AND review_status IN ('approved','confirmed','overridden','resolved')`;
    else if (filter === 'needs_review') statusFilter = ` AND review_status = 'pending'`;

    const allLeadsCTE = `
      WITH all_leads AS (
        -- HCP-matched leads
        SELECT
          hc.hcp_customer_id,
          hc.first_name,
          hc.last_name,
          hc.match_method,
          'in_funnel' as lead_status,
          COALESCE(hc.attribution_override,
            CASE
              WHEN EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source_name = 'LSA') THEN 'lsa'
              WHEN EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads')) THEN 'google_ads'
              WHEN EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND (f.gclid IS NOT NULL OR f.source ILIKE '%google%')) THEN 'google_ads'
              ELSE 'unknown'
            END
          ) as lead_source_type,
          COALESCE(hc.review_status, 'pending') as review_status,
          hc.hcp_created_at as lead_date,
          hc.client_flag_reason,
          (
            COALESCE((SELECT SUM(i.amount_cents) FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'inspection'), 0)
            + CASE
                WHEN COALESCE((SELECT SUM(i.amount_cents) FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'treatment'), 0) > 0
                  OR COALESCE((SELECT SUM(e.approved_total_cents) FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'approved' AND e.record_status = 'active' AND e.count_revenue = true), 0) > 0
                THEN GREATEST(
                  COALESCE((SELECT SUM(i.amount_cents) FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'treatment'), 0),
                  COALESCE((SELECT SUM(e.approved_total_cents) FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'approved' AND e.record_status = 'active' AND e.count_revenue = true), 0)
                )
                ELSE
                  COALESCE((SELECT SUM(j.total_amount_cents) FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status NOT IN ('user canceled','pro canceled') AND j.count_revenue = true), 0)
                  + COALESCE((SELECT SUM(ins.total_amount_cents) FROM hcp_inspections ins WHERE ins.hcp_customer_id = hc.hcp_customer_id AND ins.record_status = 'active' AND ins.count_revenue = true AND ins.status NOT IN ('user canceled','pro canceled')), 0)
              END
          ) / 100 as roas_revenue,
          CASE
            WHEN EXISTS (SELECT 1 FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.amount_cents > 0) THEN 'invoice'
            WHEN EXISTS (SELECT 1 FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'approved' AND e.record_status = 'active') THEN 'approved_estimate'
            WHEN EXISTS (SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status NOT IN ('user canceled','pro canceled')) THEN 'job'
            ELSE 'none'
          END as revenue_source,
          COALESCE((SELECT SUM(e.highest_option_cents) FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'sent' AND e.record_status = 'active' AND e.estimate_type = 'treatment' AND e.count_revenue = true), 0) / 100 as pipeline_estimate,
          -- Pipeline stage: lost reason wins, then take the FURTHER of GHL stage vs HCP stage
          -- HCP stage ranked: 10=Invoice Paid, 9=Invoiced, 8=Job Complete, 7=Job Scheduled, 6=Estimate Approved, 5=Estimate Sent, 4=Estimate Declined, 3=Inspection Complete, 2=Inspection Scheduled, 1=Lead
          -- GHL stages ranked similarly, anything below HCP rank gets overridden by HCP
          COALESCE(
            (SELECT gc.lost_reason FROM ghl_contacts gc WHERE gc.phone_normalized = hc.phone_normalized AND gc.customer_id = hc.customer_id AND gc.lost_reason IS NOT NULL LIMIT 1),
            (SELECT CASE
              WHEN hcp_rank >= ghl_rank THEN hcp_stage ELSE ghl_stage
            END FROM (SELECT
              COALESCE((SELECT go.stage_name FROM ghl_opportunities go JOIN ghl_contacts gc ON gc.ghl_contact_id = go.ghl_contact_id WHERE gc.phone_normalized = hc.phone_normalized AND gc.customer_id = hc.customer_id ORDER BY go.ghl_updated_at DESC LIMIT 1), '') as ghl_stage,
              CASE (SELECT go.stage_name FROM ghl_opportunities go JOIN ghl_contacts gc ON gc.ghl_contact_id = go.ghl_contact_id WHERE gc.phone_normalized = hc.phone_normalized AND gc.customer_id = hc.customer_id ORDER BY go.ghl_updated_at DESC LIMIT 1)
                WHEN 'Job Completed' THEN 8 WHEN 'Job Scheduled' THEN 7 WHEN 'Job Needs Scheduling' THEN 7
                WHEN 'Estimate Given/Waiting Approval' THEN 5 WHEN 'Inspection Completed' THEN 3
                WHEN 'Waiting Inspection' THEN 2 ELSE 0
              END as ghl_rank,
              CASE
                WHEN EXISTS (SELECT 1 FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status = 'paid') THEN 'Invoice Paid'
                WHEN EXISTS (SELECT 1 FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status NOT IN ('canceled','voided')) THEN 'Invoiced'
                WHEN EXISTS (SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status IN ('complete rated','complete unrated')) THEN 'Job Complete'
                WHEN EXISTS (SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status IN ('in progress','scheduled','needs scheduling','created job from estimate') AND j.status NOT IN ('user canceled','pro canceled')) THEN 'Job Scheduled'
                WHEN EXISTS (SELECT 1 FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'approved' AND e.record_status = 'active') THEN 'Estimate Approved'
                WHEN EXISTS (SELECT 1 FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'sent' AND e.record_status = 'active') THEN 'Estimate Sent'
                WHEN EXISTS (SELECT 1 FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'declined' AND e.record_status = 'active') THEN 'Estimate Declined'
                WHEN EXISTS (SELECT 1 FROM hcp_inspections i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.record_status = 'active' AND i.status IN ('complete rated','complete unrated')) THEN 'Inspection Complete'
                WHEN EXISTS (SELECT 1 FROM hcp_inspections i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.record_status = 'active' AND i.status NOT IN ('user canceled','pro canceled')) THEN 'Inspection Scheduled'
                ELSE 'Lead'
              END as hcp_stage,
              CASE
                WHEN EXISTS (SELECT 1 FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status = 'paid') THEN 10
                WHEN EXISTS (SELECT 1 FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status NOT IN ('canceled','voided')) THEN 9
                WHEN EXISTS (SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status IN ('complete rated','complete unrated')) THEN 8
                WHEN EXISTS (SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status IN ('in progress','scheduled','needs scheduling','created job from estimate') AND j.status NOT IN ('user canceled','pro canceled')) THEN 7
                WHEN EXISTS (SELECT 1 FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'approved' AND e.record_status = 'active') THEN 6
                WHEN EXISTS (SELECT 1 FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'sent' AND e.record_status = 'active') THEN 5
                WHEN EXISTS (SELECT 1 FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'declined' AND e.record_status = 'active') THEN 4
                WHEN EXISTS (SELECT 1 FROM hcp_inspections i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.record_status = 'active' AND i.status IN ('complete rated','complete unrated')) THEN 3
                WHEN EXISTS (SELECT 1 FROM hcp_inspections i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.record_status = 'active' AND i.status NOT IN ('user canceled','pro canceled')) THEN 2
                ELSE 1
              END as hcp_rank
            ) sub)
          ) as pipeline_stage,
          hc.phone_normalized as caller_phone,
          hc.callrail_id as cr_id,
          COALESCE(
            (SELECT g.campaign_name FROM gclid_campaign_map g JOIN calls c ON c.gclid = g.gclid AND g.customer_id = $1 WHERE c.callrail_id = hc.callrail_id AND c.gclid IS NOT NULL LIMIT 1),
            (SELECT f.campaign FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized) AND f.campaign IS NOT NULL AND f.campaign <> '' LIMIT 1)
          ) as campaign_name
        FROM hcp_customers hc
        WHERE hc.customer_id = $1 ${hcDateWhere}
          AND (hc.attribution_override IS NOT NULL
            OR EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads' OR c.source_name = 'LSA'))
            OR EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND (f.gclid IS NOT NULL OR f.source ILIKE '%google%')))
          AND COALESCE(hc.client_flag_reason, '') NOT IN ('spam', 'out_of_area', 'wrong_service')

        UNION ALL

        -- Unmatched calls (deduped by normalized phone, earliest call wins)
        SELECT
          NULL as hcp_customer_id,
          split_part(c.customer_name, ' ', 1) as first_name,
          CASE WHEN position(' ' in COALESCE(c.customer_name,'')) > 0
               THEN substring(c.customer_name from position(' ' in c.customer_name) + 1)
               ELSE NULL END as last_name,
          'call' as match_method,
          'lead_only' as lead_status,
          CASE WHEN c.source_name = 'LSA' THEN 'lsa' ELSE 'google_ads' END as lead_source_type,
          'pending' as review_status,
          c.start_time as lead_date,
          NULL as client_flag_reason,
          0 as roas_revenue,
          'none' as revenue_source,
          0 as pipeline_estimate,
          COALESCE(
            (SELECT gc.lost_reason FROM ghl_contacts gc WHERE gc.phone_normalized = normalize_phone(c.caller_phone) AND gc.customer_id = $1 AND gc.lost_reason IS NOT NULL LIMIT 1),
            (SELECT go.stage_name FROM ghl_opportunities go JOIN ghl_contacts gc ON gc.ghl_contact_id = go.ghl_contact_id WHERE gc.phone_normalized = normalize_phone(c.caller_phone) AND gc.customer_id = $1 ORDER BY go.ghl_updated_at DESC LIMIT 1),
            'New Lead'
          ) as pipeline_stage,
          normalize_phone(c.caller_phone) as caller_phone,
          c.callrail_id as cr_id,
          (SELECT g.campaign_name FROM gclid_campaign_map g WHERE g.gclid = c.gclid AND g.customer_id = $1 LIMIT 1) as campaign_name
        FROM (
          SELECT DISTINCT ON (normalize_phone(c2.caller_phone)) c2.*
          FROM calls c2
          WHERE c2.customer_id = $1 ${crDateWhere}
            AND (c2.gclid IS NOT NULL OR c2.classified_source = 'google_ads' OR c2.source_name = 'LSA')
          ORDER BY normalize_phone(c2.caller_phone), c2.start_time
        ) c
        WHERE NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.callrail_id = c.callrail_id AND hc.customer_id = $1)
          AND NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.phone_normalized = normalize_phone(c.caller_phone) AND hc.customer_id = $1)
          AND NOT EXISTS (SELECT 1 FROM client_flagged_leads fl WHERE fl.customer_id = $1 AND fl.flag_reason IN ('spam','out_of_area','wrong_service') AND (fl.phone_normalized = normalize_phone(c.caller_phone) OR fl.callrail_id = c.callrail_id))

        UNION ALL

        -- Unmatched forms (deduped by normalized phone)
        SELECT
          NULL as hcp_customer_id,
          split_part(COALESCE(f.customer_name, f.customer_email), ' ', 1) as first_name,
          CASE WHEN position(' ' in COALESCE(f.customer_name,'')) > 0
               THEN substring(f.customer_name from position(' ' in f.customer_name) + 1)
               ELSE NULL END as last_name,
          'form' as match_method,
          'lead_only' as lead_status,
          'google_ads' as lead_source_type,
          'pending' as review_status,
          f.submitted_at as lead_date,
          NULL as client_flag_reason,
          0 as roas_revenue,
          'none' as revenue_source,
          0 as pipeline_estimate,
          COALESCE(
            (SELECT gc.lost_reason FROM ghl_contacts gc WHERE gc.phone_normalized = normalize_phone(f.customer_phone) AND gc.customer_id = $1 AND gc.lost_reason IS NOT NULL LIMIT 1),
            (SELECT go.stage_name FROM ghl_opportunities go JOIN ghl_contacts gc ON gc.ghl_contact_id = go.ghl_contact_id WHERE gc.phone_normalized = normalize_phone(f.customer_phone) AND gc.customer_id = $1 ORDER BY go.ghl_updated_at DESC LIMIT 1),
            'New Lead'
          ) as pipeline_stage,
          normalize_phone(f.customer_phone) as caller_phone,
          f.callrail_id as cr_id,
          COALESCE(f.campaign, (SELECT g.campaign_name FROM gclid_campaign_map g WHERE g.gclid = f.gclid AND g.customer_id = $1 LIMIT 1)) as campaign_name
        FROM (
          SELECT DISTINCT ON (normalize_phone(f2.customer_phone)) f2.*
          FROM form_submissions f2
          WHERE f2.customer_id = $1 ${fmDateWhere}
            AND (f2.gclid IS NOT NULL OR f2.source ILIKE '%google%')
          ORDER BY normalize_phone(f2.customer_phone), f2.submitted_at
        ) f
        WHERE NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.callrail_id = f.callrail_id AND hc.customer_id = $1)
          AND NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.phone_normalized = normalize_phone(f.customer_phone) AND hc.customer_id = $1)
          AND NOT EXISTS (SELECT 1 FROM client_flagged_leads fl WHERE fl.customer_id = $1 AND fl.flag_reason IN ('spam','out_of_area','wrong_service') AND (fl.phone_normalized = normalize_phone(f.customer_phone) OR fl.callrail_id = f.callrail_id))
      )`;

    const outerWhere = `WHERE 1=1${sourceFilter}${searchFilter}${statusFilter}`;

    // Get summary stats + count in one query (applies all filters)
    const summaryResult = await pool.query(
      `${allLeadsCTE} SELECT
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE lead_status = 'in_funnel') as in_funnel,
        COUNT(*) FILTER (WHERE lead_status = 'lead_only') as lead_only,
        COUNT(*) FILTER (WHERE review_status IN ('approved','confirmed','overridden','resolved')) as audited,
        COUNT(*) FILTER (WHERE roas_revenue > 0) as leads_with_revenue,
        COALESCE(SUM(roas_revenue), 0) as total_revenue,
        COALESCE(SUM(pipeline_estimate), 0) as pipeline_revenue
      FROM all_leads ${outerWhere}`, params
    );

    params.push(parseInt(limit), parseInt(offset));
    const result = await pool.query(`
      ${allLeadsCTE}
      SELECT * FROM all_leads ${outerWhere}
      ORDER BY lead_date DESC NULLS LAST
      LIMIT $${paramIdx++} OFFSET $${paramIdx++}
    `, params);

    const s = summaryResult.rows[0];
    res.json({
      total: parseInt(s.total),
      summary: {
        total_leads: parseInt(s.total),
        in_funnel: parseInt(s.in_funnel),
        lead_only: parseInt(s.lead_only),
        audited: parseInt(s.audited),
        leads_with_revenue: parseInt(s.leads_with_revenue),
        roas_revenue: parseFloat(s.total_revenue),
        pipeline_revenue: parseFloat(s.pipeline_revenue),
      },
      items: result.rows
    });
  } catch (err) {
    console.error('Client leads error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Client portal: lead detail (limited info — no internal flags)
app.get('/api/client-portal/lead/:hcp_customer_id', requireClientToken, async (req, res) => {
  try {
    const { hcp_customer_id } = req.params;
    const cid = req.clientId;

    const [customer, inspections, estimates, jobs, invoices, reviews] = await Promise.all([
      pool.query(`SELECT hc.hcp_customer_id, hc.first_name, hc.last_name, hc.phone_primary, hc.email,
                         hc.lead_source, hc.match_method, hc.review_status, hc.reviewed_by, hc.hcp_created_at
                  FROM hcp_customers hc WHERE hc.hcp_customer_id = $1 AND hc.customer_id = $2`, [hcp_customer_id, cid]),
      pool.query(`SELECT scheduled_at, status, total_amount_cents, record_status FROM hcp_inspections WHERE hcp_customer_id = $1 AND customer_id = $2 ORDER BY scheduled_at DESC`, [hcp_customer_id, cid]),
      pool.query(`SELECT sent_at, status, highest_option_cents, estimate_type, record_status FROM hcp_estimates WHERE hcp_customer_id = $1 AND customer_id = $2 ORDER BY sent_at DESC`, [hcp_customer_id, cid]),
      pool.query(`SELECT scheduled_at, status, total_amount_cents, record_status FROM hcp_jobs WHERE hcp_customer_id = $1 AND customer_id = $2 ORDER BY scheduled_at DESC`, [hcp_customer_id, cid]),
      pool.query(`SELECT invoice_date, status, amount_cents FROM hcp_invoices WHERE hcp_customer_id = $1 AND customer_id = $2 ORDER BY invoice_date DESC`, [hcp_customer_id, cid]),
      pool.query(`SELECT action, performed_by, performed_at FROM lead_reviews WHERE hcp_customer_id = $1 AND customer_id = $2 ORDER BY performed_at DESC`, [hcp_customer_id, cid]),
    ]);

    if (!customer.rows.length) return res.status(404).json({ error: 'Not found' });

    // Get callrail info
    let callrail = null;
    const crCheck = await pool.query(
      `SELECT hc.callrail_id FROM hcp_customers hc WHERE hc.hcp_customer_id = $1`, [hcp_customer_id]
    );
    if (crCheck.rows[0]?.callrail_id) {
      const cr = await pool.query(
        `SELECT start_time, gclid, classified_source FROM calls WHERE callrail_id = $1`, [crCheck.rows[0].callrail_id]
      );
      if (cr.rows.length) callrail = cr.rows[0];
    }

    res.json({
      customer: customer.rows[0],
      callrail,
      inspections: inspections.rows,
      estimates: estimates.rows,
      jobs: jobs.rows,
      invoices: invoices.rows,
      reviews: reviews.rows,
    });
  } catch (err) {
    console.error('Client lead detail error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Client portal: flag a lead (matched or unmatched)
app.post('/api/client-portal/flag', requireClientToken, async (req, res) => {
  try {
    const { hcp_customer_id, name, lead_date, match_method, reason, notes } = req.body;
    const cid = req.clientId;

    if (hcp_customer_id) {
      // Matched lead — update hcp_customers + audit trail
      const check = await pool.query(
        `SELECT 1 FROM hcp_customers WHERE hcp_customer_id = $1 AND customer_id = $2`, [hcp_customer_id, cid]
      );
      if (!check.rows.length) return res.status(404).json({ error: 'Lead not found' });

      await pool.query(`
        UPDATE hcp_customers
        SET client_flag_reason = $2, client_flag_notes = $3, client_flag_at = NOW(), updated_at = NOW()
        WHERE hcp_customer_id = $1
      `, [hcp_customer_id, reason, notes || null]);

      await pool.query(`
        INSERT INTO lead_reviews (hcp_customer_id, customer_id, action, performed_by, reason, notes)
        VALUES ($1, $2, 'client_flag', $3, $4, $5)
      `, [hcp_customer_id, cid, 'Client (' + req.clientName + ')', reason, notes || null]);
    } else {
      // Unmatched call/form — log to lead_reviews + store in flagged table for filtering
      const { caller_phone, callrail_id: crId } = req.body;
      const flagNote = `[Unmatched ${match_method || 'lead'}] ${name || 'Unknown'} (${lead_date || 'no date'})${notes ? ' — ' + notes : ''}`;
      await pool.query(`
        INSERT INTO lead_reviews (hcp_customer_id, customer_id, action, performed_by, reason, notes)
        VALUES (NULL, $1, 'client_flag', $2, $3, $4)
      `, [cid, 'Client (' + req.clientName + ')', reason, flagNote]);

      // Store phone/callrail_id so we can exclude from future queries
      if (caller_phone || crId) {
        await pool.query(`
          INSERT INTO client_flagged_leads (customer_id, phone_normalized, callrail_id, flag_reason, flag_notes)
          VALUES ($1, $2, $3, $4, $5)
        `, [cid, caller_phone || null, crId || null, reason, notes || null]);
      }
    }

    res.json({ success: true });
  } catch (err) {
    console.error('Client flag error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Generate AI-style velocity summary
function generateVelocitySummary(vel, totalLeads, jobsCompleted, bizName) {
  const parts = [];
  function pv(x) { return x !== null && x !== undefined ? parseFloat(x) : null; }
  const v = {
    book: pv(vel.lead_to_insp_scheduled),
    insp: pv(vel.insp_scheduled_to_completed),
    est: pv(vel.insp_completed_to_est_sent),
    approve: pv(vel.est_sent_to_approved),
    job: pv(vel.est_approved_to_job),
    total: pv(vel.lead_to_job_completed),
  };

  if (v.book !== null) {
    if (v.book < 1) parts.push('Leads are getting booked for inspections almost immediately — same day as first contact.');
    else if (v.book <= 1) parts.push('Leads are getting booked for inspections quickly — within about a day of first contact.');
    else if (v.book <= 3) parts.push('Inspections are typically booked within ' + v.book + ' days of a lead coming in.');
    else parts.push('It takes an average of ' + v.book + ' days to get a lead booked for an inspection — there may be an opportunity to speed up initial scheduling.');
  }

  if (v.insp !== null) {
    if (v.insp <= 3) parts.push('Once booked, inspections happen fast — ' + v.insp + ' days on average.');
    else if (v.insp <= 7) parts.push('Inspections are completed about ' + v.insp + ' days after booking.');
    else parts.push('There\'s a ' + v.insp + '-day gap between booking and completing inspections.');
  }

  if (v.est !== null) {
    const estDays = Math.max(v.est, 0);
    if (estDays <= 1) parts.push('Estimates go out same day or next day after inspection — excellent turnaround.');
    else if (estDays <= 3) parts.push('Estimates are sent within ' + estDays + ' days of completing the inspection.');
    else parts.push('Estimates take ' + estDays + ' days to go out after inspection — faster estimate delivery could improve close rates.');
  }

  if (v.total !== null) {
    parts.push('The full pipeline from first contact to job completion averages ' + v.total + ' days.');
  }

  // Find the bottleneck
  const stages = [
    { name: 'booking inspections', days: v.book },
    { name: 'completing inspections', days: v.insp },
    { name: 'sending estimates', days: v.est ? Math.max(v.est, 0) : null },
    { name: 'getting estimates approved', days: v.approve },
    { name: 'scheduling jobs after approval', days: v.job },
  ].filter(s => s.days !== null && s.days > 0);

  if (stages.length > 0) {
    const bottleneck = stages.reduce((a, b) => a.days > b.days ? a : b);
    if (bottleneck.days > 5) {
      parts.push('The biggest opportunity to accelerate the pipeline is in ' + bottleneck.name + ' (' + bottleneck.days + ' days).');
    }
  }

  return parts.join(' ');
}

// ════════════════════════════════════════════════════════════
// GHL Funnel Handler
// ════════════════════════════════════════════════════════════
async function handleGhlFunnel(req, res, cid, source, date_from, date_to) {
  const params = [cid];
  let paramIdx = 2;

  // Date filters
  let ghlDateWhere = '';
  let crDateWhere = '';
  let fmDateWhere = '';
  let gcDateWhere = '';
  if (date_from) {
    ghlDateWhere += ` AND ge.created_at >= $${paramIdx}`;
    crDateWhere += ` AND c2.start_time >= $${paramIdx}`;
    fmDateWhere += ` AND f2.submitted_at >= $${paramIdx}`;
    gcDateWhere += ` AND gc.date_added >= $${paramIdx}`;
    params.push(date_from);
    paramIdx++;
  }
  if (date_to) {
    ghlDateWhere += ` AND ge.created_at < ($${paramIdx}::date + 1)`;
    crDateWhere += ` AND c2.start_time < ($${paramIdx}::date + 1)`;
    fmDateWhere += ` AND f2.submitted_at < ($${paramIdx}::date + 1)`;
    gcDateWhere += ` AND gc.date_added < ($${paramIdx}::date + 1)`;
    params.push(date_to);
    paramIdx++;
  }

  // Source attribution filters (same as HCP — applied to CallRail data)
  let crSourceWhere = '';
  let fmSourceWhere = '';
  if (source === 'google_ads') {
    crSourceWhere = `AND (c2.gclid IS NOT NULL OR c2.classified_source = 'google_ads') AND COALESCE(c2.source_name,'') <> 'LSA'`;
    fmSourceWhere = `AND (f2.gclid IS NOT NULL OR f2.source ILIKE '%google ads%')`;
  } else if (source === 'lsa') {
    crSourceWhere = `AND c2.source_name = 'LSA'`;
    fmSourceWhere = `AND 1=0`;
  } else if (source === 'gbp') {
    crSourceWhere = `AND c2.source = 'Google My Business' AND c2.gclid IS NULL AND COALESCE(c2.classified_source,'') <> 'google_ads' AND COALESCE(c2.source_name,'') <> 'LSA'`;
    fmSourceWhere = `AND f2.source = 'Google My Business' AND f2.gclid IS NULL`;
  }

  // For source-filtered GHL estimate matching, we need to join through CallRail
  // If source != 'all', only count estimates whose contact phone matches a CallRail lead from that source
  // Fallback: GHL contacts with a GCLID stored in their custom fields (for leads that bypassed CallRail)
  let ghlSourceJoin = '';
  let ghlSourceWhere = '';
  if (source !== 'all') {
    let ghlGclidUnion = '';
    if (source === 'google_ads') {
      ghlGclidUnion = `
        UNION
        SELECT DISTINCT gc2.phone_normalized as phone
        FROM ghl_contacts gc2
        WHERE gc2.customer_id = $1 AND gc2.gclid IS NOT NULL AND gc2.phone_normalized IS NOT NULL`;
    }
    ghlSourceJoin = `
      INNER JOIN (
        SELECT DISTINCT normalize_phone(c2.caller_phone) as phone
        FROM calls c2
        WHERE c2.customer_id = $1 ${crDateWhere} ${crSourceWhere}
        UNION
        SELECT DISTINCT normalize_phone(f2.customer_phone) as phone
        FROM form_submissions f2
        WHERE f2.customer_id = $1 ${fmDateWhere} ${fmSourceWhere}
        ${ghlGclidUnion}
      ) src ON src.phone = ge.phone_normalized`;
  }

  const spamPattern = `gc.lost_reason IS NOT NULL AND (gc.lost_reason ILIKE '%spam%' OR gc.lost_reason ILIKE '%not a lead%' OR gc.lost_reason ILIKE '%spoofed%' OR gc.lost_reason ILIKE '%duplicate%')`;

  const result = await pool.query(`
    WITH ghl_est AS (
      SELECT
        ge.ghl_estimate_id,
        ge.phone_normalized,
        ge.contact_name,
        ge.status,
        ge.total_cents,
        ge.issue_date,
        ge.created_at
      FROM ghl_estimates ge
      ${ghlSourceJoin}
      WHERE ge.customer_id = $1 ${ghlDateWhere}
    ),
    -- Transaction revenue per phone, split by type (inspection vs treatment)
    txn_by_phone AS (
      SELECT gt.phone_normalized,
        SUM(gt.amount_cents) as paid_cents,
        -- Standalone invoices = inspection/testing fees
        COALESCE(SUM(gt.amount_cents) FILTER (WHERE gt.entity_source_sub_type IS NULL OR gt.entity_source_sub_type NOT IN ('estimate')), 0) as inspection_paid_cents,
        -- Estimate-linked invoices = treatment payments
        COALESCE(SUM(gt.amount_cents) FILTER (WHERE gt.entity_source_sub_type = 'estimate'), 0) as treatment_paid_cents
      FROM ghl_transactions gt
      ${source !== 'all' ? ghlSourceJoin.replace(/\bge\b/g, 'gt') : ''}
      WHERE gt.customer_id = $1 AND gt.status = 'succeeded'
        AND gt.phone_normalized IS NOT NULL
      GROUP BY gt.phone_normalized
    ),
    -- Distinct contacts from GHL estimates (matched leads)
    matched_leads AS (
      SELECT
        ge.phone_normalized,
        ge.contact_name,
        MAX(CASE WHEN ge.status IN ('sent','accepted','invoiced') THEN 1 ELSE 0 END) as has_est_sent,
        MAX(CASE WHEN ge.status IN ('accepted','invoiced') THEN 1 ELSE 0 END) as has_est_approved,
        MAX(CASE WHEN ge.status = 'invoiced' THEN 1 ELSE 0 END) as has_invoiced,
        COALESCE(SUM(ge.total_cents) FILTER (WHERE ge.status IN ('sent','accepted','invoiced')), 0) as est_sent_cents,
        COALESCE(SUM(ge.total_cents) FILTER (WHERE ge.status IN ('accepted','invoiced')), 0) as est_approved_cents,
        COALESCE(SUM(ge.total_cents) FILTER (WHERE ge.status = 'invoiced'), 0) as invoiced_cents,
        COALESCE(SUM(ge.total_cents) FILTER (WHERE ge.status = 'sent'), 0) as pipeline_cents,
        COALESCE(tp.paid_cents, 0) as txn_paid_cents,
        COALESCE(tp.inspection_paid_cents, 0) as insp_paid_cents,
        COALESCE(tp.treatment_paid_cents, 0) as treat_paid_cents
      FROM ghl_est ge
      LEFT JOIN txn_by_phone tp ON tp.phone_normalized = ge.phone_normalized
      WHERE ge.phone_normalized IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM ghl_contacts gc
          WHERE gc.phone_normalized = ge.phone_normalized AND gc.customer_id = $1 AND ${spamPattern}
        )
        AND NOT EXISTS (
          SELECT 1 FROM client_flagged_leads fl
          WHERE fl.customer_id = $1 AND fl.phone_normalized = ge.phone_normalized
            AND fl.flag_reason IN ('spam','out_of_area','wrong_service')
        )
      GROUP BY ge.phone_normalized, ge.contact_name, tp.paid_cents, tp.inspection_paid_cents, tp.treatment_paid_cents
    ),
    -- Contacts with transactions but NO estimates (inspection-only invoices)
    txn_only_leads AS (
      SELECT tp.phone_normalized, tp.paid_cents as txn_paid_cents,
        gc.first_name || ' ' || gc.last_name as contact_name
      FROM txn_by_phone tp
      JOIN ghl_contacts gc ON gc.phone_normalized = tp.phone_normalized AND gc.customer_id = $1
      WHERE NOT EXISTS (SELECT 1 FROM ghl_est ge WHERE ge.phone_normalized = tp.phone_normalized)
        AND NOT EXISTS (
          SELECT 1 FROM ghl_contacts gc2
          WHERE gc2.phone_normalized = tp.phone_normalized AND gc2.customer_id = $1 AND ${spamPattern}
        )
        AND NOT EXISTS (
          SELECT 1 FROM client_flagged_leads fl
          WHERE fl.customer_id = $1 AND fl.phone_normalized = tp.phone_normalized
            AND fl.flag_reason IN ('spam','out_of_area','wrong_service')
        )
    ),
    -- Unmatched CallRail leads (not in GHL estimates, not spam)
    unmatched_calls AS (
      SELECT DISTINCT ON (normalize_phone(c2.caller_phone)) c2.callrail_id, normalize_phone(c2.caller_phone) as phone
      FROM calls c2
      WHERE c2.customer_id = $1 ${crDateWhere} ${crSourceWhere}
      ORDER BY normalize_phone(c2.caller_phone), c2.start_time
    ),
    unmatched_forms AS (
      SELECT DISTINCT ON (normalize_phone(f2.customer_phone)) f2.callrail_id, normalize_phone(f2.customer_phone) as phone
      FROM form_submissions f2
      WHERE f2.customer_id = $1 ${fmDateWhere} ${fmSourceWhere}
      ORDER BY normalize_phone(f2.customer_phone), f2.submitted_at
    ),
    unmatched_count AS (
      SELECT
        (SELECT COUNT(*) FROM unmatched_calls uc
         WHERE NOT EXISTS (SELECT 1 FROM ghl_est ge WHERE ge.phone_normalized = uc.phone)
           AND NOT EXISTS (SELECT 1 FROM txn_by_phone tp WHERE tp.phone_normalized = uc.phone)
           AND NOT EXISTS (SELECT 1 FROM client_flagged_leads fl WHERE fl.customer_id = $1 AND fl.flag_reason IN ('spam','out_of_area','wrong_service') AND fl.callrail_id = uc.callrail_id)
           AND NOT EXISTS (SELECT 1 FROM ghl_contacts gc WHERE gc.phone_normalized = uc.phone AND gc.customer_id = $1 AND ${spamPattern})
        ) +
        (SELECT COUNT(*) FROM unmatched_forms uf
         WHERE NOT EXISTS (SELECT 1 FROM ghl_est ge WHERE ge.phone_normalized = uf.phone)
           AND NOT EXISTS (SELECT 1 FROM txn_by_phone tp WHERE tp.phone_normalized = uf.phone)
           AND NOT EXISTS (SELECT 1 FROM unmatched_calls uc WHERE uc.phone = uf.phone)
           AND NOT EXISTS (SELECT 1 FROM client_flagged_leads fl WHERE fl.customer_id = $1 AND fl.flag_reason IN ('spam','out_of_area','wrong_service') AND fl.callrail_id = uf.callrail_id)
           AND NOT EXISTS (SELECT 1 FROM ghl_contacts gc WHERE gc.phone_normalized = uf.phone AND gc.customer_id = $1 AND ${spamPattern})
        ) as unmatched_lead_count
    ),
    agg AS (
      SELECT
        COUNT(*) as matched_leads,
        COUNT(*) FILTER (WHERE has_est_sent = 1) as estimate_sent,
        COUNT(*) FILTER (WHERE has_est_approved = 1) as estimate_approved,
        COUNT(*) FILTER (WHERE has_invoiced = 1 OR txn_paid_cents > 0) as invoiced,
        COALESCE(SUM(est_sent_cents), 0) as total_est_sent_cents,
        COALESCE(SUM(est_approved_cents), 0) as total_est_approved_cents,
        COALESCE(SUM(GREATEST(invoiced_cents, txn_paid_cents)), 0) as total_invoiced_cents,
        COALESCE(SUM(pipeline_cents), 0) as total_pipeline_cents,
        -- ROAS = inspection_paid + GREATEST(treatment_paid, approved_estimate)
        COALESCE(SUM(
          insp_paid_cents + GREATEST(treat_paid_cents, est_approved_cents, invoiced_cents)
        ), 0) as roas_cents
      FROM matched_leads
    ),
    -- Add txn-only leads (inspection invoices without estimates)
    txn_only_agg AS (
      SELECT
        COUNT(*) as txn_only_count,
        COALESCE(SUM(txn_paid_cents), 0) as txn_only_roas_cents,
        COALESCE(SUM(txn_paid_cents), 0) as txn_only_invoiced_cents
      FROM txn_only_leads
    ),
    -- Total contacts (no spam exclusion)
    contacts_ghl AS (
      SELECT COUNT(DISTINCT gc.phone_normalized) as cnt
      FROM ghl_contacts gc
      WHERE gc.customer_id = $1
        ${gcDateWhere.replace(/\bge\b/g, 'gc')}
    ),
    contacts_unmatched AS (
      SELECT
        (SELECT COUNT(*) FROM unmatched_calls uc
         WHERE NOT EXISTS (SELECT 1 FROM ghl_contacts gc WHERE gc.phone_normalized = uc.phone AND gc.customer_id = $1)
        ) +
        (SELECT COUNT(*) FROM unmatched_forms uf
         WHERE NOT EXISTS (SELECT 1 FROM ghl_contacts gc WHERE gc.phone_normalized = uf.phone AND gc.customer_id = $1)
           AND NOT EXISTS (SELECT 1 FROM unmatched_calls uc WHERE uc.phone = uf.phone)
        ) as cnt
    ),
    spam_count AS (
      SELECT COUNT(DISTINCT gc.phone_normalized) as cnt
      FROM ghl_contacts gc
      WHERE gc.customer_id = $1 AND ${spamPattern.replace(/\bgc\b/g, 'gc')}
        ${gcDateWhere.replace(/\bge\b/g, 'gc')}
    )
    SELECT
      a.*,
      t.txn_only_count, t.txn_only_roas_cents, t.txn_only_invoiced_cents,
      u.unmatched_lead_count,
      (a.matched_leads + t.txn_only_count + u.unmatched_lead_count) as total_leads,
      ((SELECT cnt FROM contacts_ghl) + (SELECT cnt FROM contacts_unmatched)) as total_contacts,
      (SELECT cnt FROM spam_count) as spam_count
    FROM agg a, txn_only_agg t, unmatched_count u
  `, params);

  const r = result.rows[0];
  const txnOnlyRoas = parseInt(r.txn_only_roas_cents) || 0;
  const txnOnlyInvoiced = parseInt(r.txn_only_invoiced_cents) || 0;
  const totalLeads = parseInt(r.total_leads) || 0;
  const invoicedCount = (parseInt(r.invoiced) || 0) + (parseInt(r.txn_only_count) || 0);

  // Client name
  const bizName = req.clientName.includes('|') ? req.clientName.split('|').pop().trim() : req.clientName;

  // Ad spend (same logic as HCP)
  let spendWhere = '';
  const spendParams = [cid];
  let spIdx = 2;
  if (date_from) { spendWhere += ` AND date >= $${spIdx}`; spendParams.push(date_from); spIdx++; }
  if (date_to) { spendWhere += ` AND date <= $${spIdx}`; spendParams.push(date_to); spIdx++; }

  let spendQuery;
  if (source === 'google_ads') {
    spendQuery = `SELECT COALESCE(SUM(cost), 0) as spend FROM campaign_daily_metrics WHERE customer_id = $1 AND campaign_type IN ('SEARCH', 'PERFORMANCE_MAX')${spendWhere}`;
  } else if (source === 'lsa') {
    spendQuery = `SELECT COALESCE(SUM(cost), 0) as spend FROM campaign_daily_metrics WHERE customer_id = $1 AND campaign_type = 'LOCAL_SERVICES'${spendWhere}`;
  } else if (source === 'gbp') {
    spendQuery = null;
  } else {
    spendQuery = `SELECT COALESCE(SUM(cost), 0) as spend FROM account_daily_metrics WHERE customer_id IN (SELECT customer_id FROM clients WHERE customer_id = $1 OR parent_customer_id = $1)${spendWhere}`;
  }

  let adSpend = 0;
  if (spendQuery) {
    const spendResult = await pool.query(spendQuery, spendParams);
    adSpend = Math.round(parseFloat(spendResult.rows[0].spend) * 100) / 100;
  }

  const roasRevenue = Math.round((parseInt(r.roas_cents) + txnOnlyRoas) / 100);
  const roas = adSpend > 0 ? Math.round((roasRevenue / adSpend) * 100) / 100 : 0;
  const cpl = totalLeads > 0 && adSpend > 0 ? Math.round((adSpend / totalLeads) * 100) / 100 : 0;

  // Velocity: Lead -> Estimate Sent, Lead -> Invoiced
  const velParams = [cid];
  let velIdx = 2;
  let velDateWhere = '';
  if (date_from) { velDateWhere += ` AND ge.issue_date >= $${velIdx}`; velParams.push(date_from); velIdx++; }
  if (date_to) { velDateWhere += ` AND ge.issue_date < ($${velIdx}::date + 1)`; velParams.push(date_to); velIdx++; }

  const velResult = await pool.query(`
    SELECT
      ROUND((AVG(EXTRACT(EPOCH FROM (ge.issue_date - c.start_time))/86400)
        FILTER (WHERE ge.status IN ('sent','accepted','invoiced') AND c.start_time IS NOT NULL AND ge.issue_date IS NOT NULL))::numeric, 1)
        as lead_to_est_sent,
      ROUND((AVG(EXTRACT(EPOCH FROM (ge.issue_date - c.start_time))/86400)
        FILTER (WHERE ge.status = 'invoiced' AND c.start_time IS NOT NULL AND ge.issue_date IS NOT NULL))::numeric, 1)
        as lead_to_job_completed
    FROM ghl_estimates ge
    LEFT JOIN LATERAL (
      SELECT MIN(c2.start_time) as start_time
      FROM calls c2
      WHERE c2.customer_id = $1
        AND normalize_phone(c2.caller_phone) = ge.phone_normalized
    ) c ON true
    WHERE ge.customer_id = $1 ${velDateWhere}
      AND ge.phone_normalized IS NOT NULL
  `, velParams);

  const vel = velResult.rows[0] || {};

  const velocityObj = {
    lead_to_insp_scheduled: null,
    insp_scheduled_to_completed: null,
    insp_completed_to_est_sent: vel.lead_to_est_sent !== null ? parseFloat(vel.lead_to_est_sent) : null,
    est_sent_to_approved: null,
    est_approved_to_job: null,
    lead_to_job_completed: vel.lead_to_job_completed !== null ? parseFloat(vel.lead_to_job_completed) : null,
    summary: generateVelocitySummary({
      lead_to_insp_scheduled: null,
      insp_scheduled_to_completed: null,
      insp_completed_to_est_sent: vel.lead_to_est_sent,
      est_sent_to_approved: null,
      est_approved_to_job: null,
      lead_to_job_completed: vel.lead_to_job_completed,
    }, totalLeads, invoicedCount, bizName),
  };

  res.json({
    client_name: bizName,
    stages: {
      leads:                { count: totalLeads, revenue: 0 },
      inspection_scheduled: { count: 0, revenue: 0 },
      inspection_completed: { count: 0, revenue: 0 },
      estimate_sent:        { count: parseInt(r.estimate_sent) || 0, revenue: Math.round(parseInt(r.total_est_sent_cents) / 100) },
      estimate_approved:    { count: parseInt(r.estimate_approved) || 0, revenue: Math.round(parseInt(r.total_est_approved_cents) / 100) },
      job_scheduled:        { count: invoicedCount, revenue: Math.round((parseInt(r.total_invoiced_cents) + txnOnlyInvoiced) / 100) },
      job_completed:        { count: invoicedCount, revenue: Math.round((parseInt(r.total_invoiced_cents) + txnOnlyInvoiced) / 100) },
      invoiced:             { count: invoicedCount, revenue: Math.round((parseInt(r.total_invoiced_cents) + txnOnlyInvoiced) / 100) },
    },
    kpis: {
      roas_revenue: roasRevenue,
      pipeline_revenue: Math.round(parseInt(r.total_pipeline_cents) / 100),
      contacts: parseInt(r.total_contacts) || 0,
      spam: parseInt(r.spam_count) || 0,
      ad_spend: adSpend,
      roas: roas,
      cpl: cpl,
    },
    velocity: velocityObj,
  });
}

// Serve funnel dashboard
app.get('/portal/:token/funnel', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'funnel.html'));
});

// Client portal: funnel stats by source
app.get('/api/client-portal/funnel', requireClientToken, async (req, res) => {
  try {
    const cid = req.clientId;
    const { source = 'all', date_from, date_to } = req.query;

    // Branch to GHL handler if client uses GHL
    if (req.clientFMS === 'ghl') {
      return handleGhlFunnel(req, res, cid, source, date_from, date_to);
    }

    const params = [cid];
    let paramIdx = 2;

    // Date filters
    let hcDateWhere = '';
    let crDateWhere = '';
    let fmDateWhere = '';
    if (date_from) {
      hcDateWhere += ` AND hc.hcp_created_at >= $${paramIdx}`;
      crDateWhere += ` AND c2.start_time >= $${paramIdx}`;
      fmDateWhere += ` AND f2.submitted_at >= $${paramIdx}`;
      params.push(date_from);
      paramIdx++;
    }
    if (date_to) {
      hcDateWhere += ` AND hc.hcp_created_at < ($${paramIdx}::date + 1)`;
      crDateWhere += ` AND c2.start_time < ($${paramIdx}::date + 1)`;
      fmDateWhere += ` AND f2.submitted_at < ($${paramIdx}::date + 1)`;
      params.push(date_to);
      paramIdx++;
    }

    // Source attribution filter for HCP-matched leads
    let hcSourceWhere = '';
    let crSourceWhere = '';
    let fmSourceWhere = '';
    if (source === 'google_ads') {
      hcSourceWhere = `AND (hc.attribution_override = 'google_ads'
        OR EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads') AND COALESCE(c.source_name,'') <> 'LSA')
        OR EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND (f.gclid IS NOT NULL OR f.source ILIKE '%google ads%')))`;
      crSourceWhere = `AND (c2.gclid IS NOT NULL OR c2.classified_source = 'google_ads') AND COALESCE(c2.source_name,'') <> 'LSA'`;
      fmSourceWhere = `AND (f2.gclid IS NOT NULL OR f2.source ILIKE '%google ads%')`;
    } else if (source === 'lsa') {
      hcSourceWhere = `AND (hc.attribution_override = 'lsa'
        OR EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source_name = 'LSA'))`;
      crSourceWhere = `AND c2.source_name = 'LSA'`;
      fmSourceWhere = `AND 1=0`; // LSA doesn't come through forms
    } else if (source === 'gbp') {
      hcSourceWhere = `AND (COALESCE(hc.attribution_override, '') = 'gbp'
        OR EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source = 'Google My Business')
        OR EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND f.source = 'Google My Business'))
      AND COALESCE(hc.attribution_override, '') <> 'google_ads'
      AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads'))
      AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source_name = 'LSA')
      AND NOT EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND (f.gclid IS NOT NULL OR f.source ILIKE '%google ads%'))`;
      crSourceWhere = `AND c2.source = 'Google My Business' AND c2.gclid IS NULL AND COALESCE(c2.classified_source,'') <> 'google_ads' AND COALESCE(c2.source_name,'') <> 'LSA'`;
      fmSourceWhere = `AND f2.source = 'Google My Business' AND f2.gclid IS NULL`;
    } else if (source === 'direct') {
      hcSourceWhere = `AND (
        EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.medium = 'direct' OR c.medium = 'Organic' OR c.source ILIKE '%organic%'))
      )
      AND COALESCE(hc.attribution_override, '') NOT IN ('google_ads', 'lsa', 'gbp')
      AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads' OR c.source_name = 'LSA' OR c.source = 'Google My Business'))`;
      crSourceWhere = `AND (c2.medium = 'direct' OR c2.medium = 'Organic' OR c2.source ILIKE '%organic%') AND c2.gclid IS NULL AND COALESCE(c2.classified_source,'') <> 'google_ads' AND COALESCE(c2.source_name,'') <> 'LSA' AND c2.source <> 'Google My Business'`;
      fmSourceWhere = `AND f2.gclid IS NULL AND COALESCE(f2.source,'') NOT ILIKE '%google%' AND COALESCE(f2.source,'') <> 'Google My Business' AND (f2.medium = 'direct' OR f2.medium IS NULL OR f2.medium = 'Organic')`;
    } else if (source === 'other') {
      hcSourceWhere = `AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads' OR c.source_name = 'LSA' OR c.source = 'Google My Business' OR c.medium IN ('direct','Organic') OR c.source ILIKE '%organic%'))
      AND NOT EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND (f.gclid IS NOT NULL OR f.source ILIKE '%google%'))
      AND COALESCE(hc.attribution_override, '') NOT IN ('google_ads', 'lsa', 'gbp')`;
      crSourceWhere = `AND c2.gclid IS NULL AND COALESCE(c2.classified_source,'') <> 'google_ads' AND COALESCE(c2.source_name,'') <> 'LSA' AND c2.source <> 'Google My Business' AND COALESCE(c2.medium,'') NOT IN ('direct','Organic') AND COALESCE(c2.source,'') NOT ILIKE '%organic%'`;
      fmSourceWhere = `AND f2.gclid IS NULL AND COALESCE(f2.source,'') NOT ILIKE '%google%' AND COALESCE(f2.source,'') <> 'Google My Business' AND COALESCE(f2.medium,'') NOT IN ('direct','Organic')`;
    }
    // source === 'all' → no source filter (full business)

    const flagExclude = `AND COALESCE(hc.client_flag_reason, '') NOT IN ('spam', 'out_of_area', 'wrong_service')`;
    const spamExclude = `AND NOT EXISTS (
      SELECT 1 FROM ghl_contacts gc
      WHERE gc.phone_normalized = hc.phone_normalized AND gc.customer_id = hc.customer_id
        AND (
          gc.lost_reason SIMILAR TO '%(spam|not a lead|wrong number|out of area|wrong service|abandoned)%'
          OR EXISTS (SELECT 1 FROM ghl_opportunities o WHERE o.ghl_contact_id = gc.ghl_contact_id
            AND o.stage_name SIMILAR TO '%(spam|not a lead|out of area|wrong service)%')
        )
    )`;

    const result = await pool.query(`
      WITH matched_leads AS (
        SELECT
          hc.hcp_customer_id,
          -- Funnel stage booleans (cumulative)
          TRUE as is_lead,
          EXISTS (SELECT 1 FROM hcp_inspections ins WHERE ins.hcp_customer_id = hc.hcp_customer_id AND ins.record_status = 'active' AND ins.status IN ('scheduled','complete rated','complete unrated','in progress')) as has_inspection_scheduled,
          EXISTS (SELECT 1 FROM hcp_inspections ins WHERE ins.hcp_customer_id = hc.hcp_customer_id AND ins.record_status = 'active' AND (ins.status IN ('complete rated','complete unrated') OR ins.inferred_complete = true)) as has_inspection_completed,
          EXISTS (SELECT 1 FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status IN ('sent','approved','declined') AND eg.count_revenue) as has_estimate_sent,
          EXISTS (SELECT 1 FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status = 'approved' AND eg.count_revenue) as has_estimate_approved,
          EXISTS (SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status IN ('scheduled','complete rated','complete unrated','in progress')) as has_job_scheduled,
          EXISTS (SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status IN ('complete rated','complete unrated')) as has_job_completed,
          EXISTS (SELECT 1 FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status NOT IN ('canceled','voided') AND i.amount_cents > 0) as has_invoice,

          -- Revenue calculations
          COALESCE((SELECT SUM(ins.total_amount_cents) FROM hcp_inspections ins WHERE ins.hcp_customer_id = hc.hcp_customer_id AND ins.record_status = 'active' AND ins.status IN ('complete rated','complete unrated') AND ins.count_revenue = true), 0) as inspection_revenue_cents,
          COALESCE((SELECT SUM(eg.approved_total_cents) FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status = 'approved' AND eg.count_revenue), 0) as approved_est_cents,
          COALESCE((SELECT SUM(j.total_amount_cents) FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status NOT IN ('user canceled','pro canceled') AND j.count_revenue = true), 0) as job_cents,
          COALESCE((SELECT SUM(j.total_amount_cents) FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status IN ('complete rated','complete unrated') AND j.count_revenue = true), 0) as job_completed_cents,
          COALESCE((SELECT SUM(i.amount_cents) FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'inspection'), 0) as insp_invoice_cents,
          COALESCE((SELECT SUM(i.amount_cents) FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'treatment'), 0) as treat_invoice_cents,
          COALESCE((SELECT SUM(i.amount_cents) FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status NOT IN ('canceled','voided') AND i.amount_cents > 0), 0) as total_invoice_cents,
          CASE WHEN NOT EXISTS (SELECT 1 FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status = 'approved' AND eg.count_revenue)
               AND NOT EXISTS (SELECT 1 FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status NOT IN ('canceled','voided') AND i.invoice_type = 'treatment' AND i.amount_cents > 0)
            THEN COALESCE((SELECT SUM(eg.highest_option_cents) FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status = 'sent' AND eg.estimate_type = 'treatment' AND eg.count_revenue), 0)
            ELSE 0 END as pipeline_cents,
          COALESCE((SELECT SUM(eg.highest_option_cents) FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status IN ('sent','approved','declined') AND eg.count_revenue), 0) as estimate_sent_cents
        FROM hcp_customers hc
        WHERE hc.customer_id = $1 ${hcDateWhere} ${hcSourceWhere} ${flagExclude} ${spamExclude}
      ),
      unmatched_calls AS (
        SELECT DISTINCT ON (normalize_phone(c2.caller_phone)) c2.callrail_id, normalize_phone(c2.caller_phone) as phone
        FROM calls c2
        WHERE c2.customer_id = $1 ${crDateWhere} ${crSourceWhere}
        ORDER BY normalize_phone(c2.caller_phone), c2.start_time
      ),
      unmatched_forms AS (
        SELECT DISTINCT ON (normalize_phone(f2.customer_phone)) f2.callrail_id, normalize_phone(f2.customer_phone) as phone
        FROM form_submissions f2
        WHERE f2.customer_id = $1 AND COALESCE(f2.is_spam, false) = false ${fmDateWhere} ${fmSourceWhere}
        ORDER BY normalize_phone(f2.customer_phone), f2.submitted_at
      ),
      unmatched_count AS (
        SELECT
          (SELECT COUNT(*) FROM unmatched_calls uc
           WHERE NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.callrail_id = uc.callrail_id AND hc.customer_id = $1)
             AND NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.phone_normalized = uc.phone AND hc.customer_id = $1)
             AND NOT EXISTS (SELECT 1 FROM client_flagged_leads fl WHERE fl.customer_id = $1 AND fl.flag_reason IN ('spam','out_of_area','wrong_service') AND (fl.callrail_id = uc.callrail_id OR fl.phone_normalized = uc.phone))
             AND NOT EXISTS (SELECT 1 FROM calls cc JOIN ghl_contacts gc ON gc.phone_normalized = normalize_phone(cc.caller_phone) AND gc.customer_id = $1 WHERE cc.callrail_id = uc.callrail_id AND gc.lost_reason IS NOT NULL AND (gc.lost_reason ILIKE '%spam%' OR gc.lost_reason ILIKE '%not a lead%' OR gc.lost_reason ILIKE '%spoofed%' OR gc.lost_reason ILIKE '%duplicate%'))
          ) +
          (SELECT COUNT(*) FROM unmatched_forms uf
           WHERE NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.callrail_id = uf.callrail_id AND hc.customer_id = $1)
             AND NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.phone_normalized = uf.phone AND hc.customer_id = $1)
             AND NOT EXISTS (SELECT 1 FROM client_flagged_leads fl WHERE fl.customer_id = $1 AND fl.flag_reason IN ('spam','out_of_area','wrong_service') AND (fl.callrail_id = uf.callrail_id OR fl.phone_normalized = uf.phone))
             AND NOT EXISTS (SELECT 1 FROM unmatched_calls uc WHERE uc.phone = uf.phone)
          ) as unmatched_lead_count
      ),
      agg AS (
        SELECT
          COUNT(*) as matched_leads,
          COUNT(*) FILTER (WHERE has_inspection_scheduled) as inspection_scheduled,
          COUNT(*) FILTER (WHERE has_inspection_completed) as inspection_completed,
          COUNT(*) FILTER (WHERE has_estimate_sent) as estimate_sent,
          COUNT(*) FILTER (WHERE has_estimate_approved) as estimate_approved,
          COUNT(*) FILTER (WHERE has_job_scheduled) as job_scheduled,
          COUNT(*) FILTER (WHERE has_job_completed) as job_completed,
          COUNT(*) FILTER (WHERE has_invoice) as invoiced,
          COALESCE(SUM(inspection_revenue_cents), 0) as total_inspection_rev,
          COALESCE(SUM(approved_est_cents), 0) as total_approved_est,
          COALESCE(SUM(job_cents), 0) as total_job_rev,
          COALESCE(SUM(job_completed_cents), 0) as total_job_completed_rev,
          COALESCE(SUM(estimate_sent_cents), 0) as total_estimate_sent_rev,
          COALESCE(SUM(total_invoice_cents), 0) as total_invoice_rev,
          -- ROAS revenue: insp_invoice + GREATEST(treat_invoice, approved_est), fallback to job+insp totals
          COALESCE(SUM(
            insp_invoice_cents +
            CASE
              WHEN treat_invoice_cents > 0 OR approved_est_cents > 0
                THEN GREATEST(treat_invoice_cents, approved_est_cents)
              ELSE job_cents + inspection_revenue_cents
            END
          ), 0) as roas_revenue_cents,
          COALESCE(SUM(pipeline_cents), 0) as pipeline_cents
        FROM matched_leads
      ),
      contacts_matched AS (
        SELECT COUNT(*) as cnt FROM hcp_customers hc
        WHERE hc.customer_id = $1 ${hcDateWhere} ${hcSourceWhere}
      ),
      contacts_unmatched AS (
        SELECT
          (SELECT COUNT(*) FROM unmatched_calls uc
           WHERE NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.callrail_id = uc.callrail_id AND hc.customer_id = $1)
             AND NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.phone_normalized = uc.phone AND hc.customer_id = $1)
          ) +
          (SELECT COUNT(*) FROM unmatched_forms uf
           WHERE NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.callrail_id = uf.callrail_id AND hc.customer_id = $1)
             AND NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.phone_normalized = uf.phone AND hc.customer_id = $1)
             AND NOT EXISTS (SELECT 1 FROM unmatched_calls uc WHERE uc.phone = uf.phone)
          ) as cnt
      )
      SELECT
        a.*,
        u.unmatched_lead_count,
        (a.matched_leads + u.unmatched_lead_count) as total_leads,
        ((SELECT cnt FROM contacts_matched) + (SELECT cnt FROM contacts_unmatched)) as total_contacts
      FROM agg a, unmatched_count u
    `, params);

    const r = result.rows[0];
    const totalLeads = parseInt(r.total_leads) || 0;

    // Get client name
    const bizName = req.clientName.includes('|') ? req.clientName.split('|').pop().trim() : req.clientName;

    // Get ad spend for the date range, filtered by source
    let spendWhere = '';
    const spendParams = [cid];
    let spIdx = 2;
    if (date_from) { spendWhere += ` AND date >= $${spIdx}`; spendParams.push(date_from); spIdx++; }
    if (date_to) { spendWhere += ` AND date <= $${spIdx}`; spendParams.push(date_to); spIdx++; }

    let spendQuery;
    if (source === 'google_ads') {
      spendQuery = `SELECT COALESCE(SUM(cost), 0) as spend FROM campaign_daily_metrics WHERE customer_id = $1 AND campaign_type IN ('SEARCH', 'PERFORMANCE_MAX')${spendWhere}`;
    } else if (source === 'lsa') {
      spendQuery = `SELECT COALESCE(SUM(cost), 0) as spend FROM campaign_daily_metrics WHERE customer_id = $1 AND campaign_type = 'LOCAL_SERVICES'${spendWhere}`;
    } else if (source === 'gbp') {
      // GBP has no ad spend
      spendQuery = null;
    } else {
      // Full Business — total account spend (all paid channels)
      spendQuery = `SELECT COALESCE(SUM(cost), 0) as spend FROM account_daily_metrics WHERE customer_id IN (SELECT customer_id FROM clients WHERE customer_id = $1 OR parent_customer_id = $1)${spendWhere}`;
    }

    let adSpend = 0;
    if (spendQuery) {
      const spendResult = await pool.query(spendQuery, spendParams);
      adSpend = Math.round(parseFloat(spendResult.rows[0].spend) * 100) / 100;
    }
    const roasRevenue = Math.round(parseInt(r.roas_revenue_cents) / 100);
    const roas = adSpend > 0 ? Math.round((roasRevenue / adSpend) * 100) / 100 : 0;
    const cpl = totalLeads > 0 && adSpend > 0 ? Math.round((adSpend / totalLeads) * 100) / 100 : 0;

    // Pipeline velocity: avg days between stages
    const velParams = [cid];
    let velIdx = 2;
    let velDateWhere = '';
    if (date_from) { velDateWhere += ` AND hc.hcp_created_at >= $${velIdx}`; velParams.push(date_from); velIdx++; }
    if (date_to) { velDateWhere += ` AND hc.hcp_created_at < ($${velIdx}::date + 1)`; velParams.push(date_to); velIdx++; }

    const velResult = await pool.query(`
      SELECT
        -- Lead → Inspection Scheduled: lead date to when inspection was booked (created in HCP)
        ROUND(AVG(EXTRACT(EPOCH FROM (ins_d.first_created - hc.hcp_created_at))/86400)::numeric, 1) as lead_to_insp_scheduled,
        -- Inspection Scheduled → Completed: calendar date to actual completion date (scheduled_at for completed inspections)
        ROUND(AVG(EXTRACT(EPOCH FROM (ins_d.first_completed_date - ins_d.first_created))/86400)::numeric, 1) as insp_scheduled_to_completed,
        -- Inspection Completed → Estimate Sent
        ROUND(AVG(EXTRACT(EPOCH FROM (eg_d.first_sent - ins_d.first_completed_date))/86400)::numeric, 1) as insp_completed_to_est_sent,
        -- Estimate Sent → Approved (only if approved_at available)
        ROUND(AVG(EXTRACT(EPOCH FROM (eg_d.first_approved - eg_d.first_sent))/86400)::numeric, 1) as est_sent_to_approved,
        -- Estimate Approved → Job Scheduled
        ROUND(AVG(EXTRACT(EPOCH FROM (j_d.first_scheduled - eg_d.first_approved))/86400)::numeric, 1) as est_approved_to_job,
        -- Lead → Job Completed (treatment jobs only, >$1000)
        ROUND(AVG(EXTRACT(EPOCH FROM (COALESCE(j_d.first_completed, j_d.first_scheduled) - hc.hcp_created_at))/86400)::numeric, 1) as lead_to_job_completed
      FROM hcp_customers hc
      LEFT JOIN LATERAL (
        SELECT
          MIN(ins.hcp_created_at) as first_created,
          -- For completed inspections (actual or inferred), use scheduled_at as the completion date
          MIN(ins.scheduled_at) FILTER (WHERE ins.status IN ('complete rated','complete unrated') OR ins.inferred_complete = true) as first_completed_date
        FROM hcp_inspections ins WHERE ins.hcp_customer_id = hc.hcp_customer_id AND ins.record_status = 'active' AND ins.status NOT IN ('user canceled','pro canceled')
      ) ins_d ON true
      LEFT JOIN LATERAL (
        SELECT
          MIN(eg.sent_at) as first_sent,
          MIN(eg.approved_at) FILTER (WHERE eg.status = 'approved') as first_approved
        FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status IN ('sent','approved','declined') AND eg.count_revenue
      ) eg_d ON true
      LEFT JOIN LATERAL (
        SELECT
          MIN(j.scheduled_at) as first_scheduled,
          MIN(COALESCE(j.completed_at, j.scheduled_at)) FILTER (WHERE j.status IN ('complete rated','complete unrated')) as first_completed
        FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status NOT IN ('user canceled','pro canceled')
          AND j.total_amount_cents > 100000
      ) j_d ON true
      WHERE hc.customer_id = $1 ${velDateWhere} ${hcSourceWhere} ${flagExclude} ${spamExclude}
    `, velParams);

    const vel = velResult.rows[0] || {};

    res.json({
      client_name: bizName,
      stages: {
        leads:                { count: totalLeads, revenue: 0 },
        inspection_scheduled: { count: parseInt(r.inspection_scheduled) || 0, revenue: 0 },
        inspection_completed: { count: parseInt(r.inspection_completed) || 0, revenue: Math.round(parseInt(r.total_inspection_rev) / 100) },
        estimate_sent:        { count: parseInt(r.estimate_sent) || 0, revenue: Math.round(parseInt(r.total_estimate_sent_rev) / 100) },
        estimate_approved:    { count: parseInt(r.estimate_approved) || 0, revenue: Math.round(parseInt(r.total_approved_est) / 100) },
        job_scheduled:        { count: parseInt(r.job_scheduled) || 0, revenue: Math.round(parseInt(r.total_job_rev) / 100) },
        job_completed:        { count: parseInt(r.job_completed) || 0, revenue: Math.round(parseInt(r.total_job_completed_rev) / 100) },
        invoiced:             { count: parseInt(r.invoiced) || 0, revenue: Math.round(parseInt(r.total_invoice_rev) / 100) },
      },
      kpis: {
        roas_revenue: roasRevenue,
        pipeline_revenue: Math.round(parseInt(r.pipeline_cents) / 100),
        contacts: parseInt(r.total_contacts) || 0,
        spam: (parseInt(r.total_contacts) || 0) - totalLeads,
        ad_spend: adSpend,
        roas: roas,
        cpl: cpl,
      },
      velocity: {
        lead_to_insp_scheduled: vel.lead_to_insp_scheduled !== null ? parseFloat(vel.lead_to_insp_scheduled) : null,
        insp_scheduled_to_completed: vel.insp_scheduled_to_completed !== null ? parseFloat(vel.insp_scheduled_to_completed) : null,
        insp_completed_to_est_sent: vel.insp_completed_to_est_sent !== null ? parseFloat(vel.insp_completed_to_est_sent) : null,
        est_sent_to_approved: vel.est_sent_to_approved !== null ? parseFloat(vel.est_sent_to_approved) : null,
        est_approved_to_job: vel.est_approved_to_job !== null ? parseFloat(vel.est_approved_to_job) : null,
        lead_to_job_completed: vel.lead_to_job_completed !== null ? parseFloat(vel.lead_to_job_completed) : null,
        summary: generateVelocitySummary(vel, totalLeads, parseInt(r.job_completed) || 0, bizName),
      }
    });
  } catch (err) {
    console.error('Client funnel error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Client portal: campaign trends — leads per week grouped by campaign
app.get('/api/client-portal/campaign-trends', requireClientToken, async (req, res) => {
  try {
    const cid = req.clientId;
    const { date_from, date_to } = req.query;

    let dateFilter = '';
    const params = [cid];
    let pi = 2;
    if (date_from) { dateFilter += ` AND lead_date >= $${pi}`; params.push(date_from); pi++; }
    if (date_to)   { dateFilter += ` AND lead_date < ($${pi}::date + 1)`; params.push(date_to); pi++; }

    const result = await pool.query(`
      WITH lead_campaigns AS (
        -- HCP-matched leads with campaign
        SELECT
          hc.hcp_created_at as lead_date,
          COALESCE(
            (SELECT g.campaign_name FROM gclid_campaign_map g JOIN calls c ON c.gclid = g.gclid AND g.customer_id = $1 WHERE c.callrail_id = hc.callrail_id AND c.gclid IS NOT NULL LIMIT 1),
            (SELECT f.campaign FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized) AND f.campaign IS NOT NULL AND f.campaign <> '' LIMIT 1),
            'Unknown'
          ) as campaign_name
        FROM hcp_customers hc
        WHERE hc.customer_id = $1
          AND (hc.attribution_override IS NOT NULL
            OR EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads'))
            OR EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized) AND (f.gclid IS NOT NULL OR f.source ILIKE '%google%')))

        UNION ALL

        -- Unmatched calls with campaign
        SELECT
          c.start_time as lead_date,
          COALESCE(
            (SELECT g.campaign_name FROM gclid_campaign_map g WHERE g.gclid = c.gclid AND g.customer_id = $1 LIMIT 1),
            'Unknown'
          ) as campaign_name
        FROM (
          SELECT DISTINCT ON (normalize_phone(c2.caller_phone)) c2.*
          FROM calls c2
          WHERE c2.customer_id = $1
            AND (c2.gclid IS NOT NULL OR c2.classified_source = 'google_ads')
            AND COALESCE(c2.source_name,'') <> 'LSA'
          ORDER BY normalize_phone(c2.caller_phone), c2.start_time
        ) c
        WHERE NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.phone_normalized = normalize_phone(c.caller_phone) AND hc.customer_id = $1)

        UNION ALL

        -- Unmatched forms with campaign
        SELECT
          f.submitted_at as lead_date,
          COALESCE(f.campaign, (SELECT g.campaign_name FROM gclid_campaign_map g WHERE g.gclid = f.gclid AND g.customer_id = $1 LIMIT 1), 'Unknown') as campaign_name
        FROM (
          SELECT DISTINCT ON (normalize_phone(f2.customer_phone)) f2.*
          FROM form_submissions f2
          WHERE f2.customer_id = $1
            AND (f2.gclid IS NOT NULL OR f2.source ILIKE '%google%')
          ORDER BY normalize_phone(f2.customer_phone), f2.submitted_at
        ) f
        WHERE NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.phone_normalized = normalize_phone(f.customer_phone) AND hc.customer_id = $1)
      )
      SELECT
        date_trunc('week', lead_date)::date as week,
        campaign_name,
        COUNT(*) as lead_count
      FROM lead_campaigns
      WHERE lead_date IS NOT NULL ${dateFilter}
      GROUP BY 1, 2
      ORDER BY 1, 2
    `, params);

    // Pivot into { weeks: [...], campaigns: { name: [counts] } }
    const weekSet = new Set();
    const campaignData = {};
    for (const row of result.rows) {
      const w = row.week.toISOString().slice(0, 10);
      weekSet.add(w);
      if (!campaignData[row.campaign_name]) campaignData[row.campaign_name] = {};
      campaignData[row.campaign_name][w] = parseInt(row.lead_count);
    }
    const weeks = [...weekSet].sort();
    const campaigns = {};
    for (const [name, data] of Object.entries(campaignData)) {
      campaigns[name] = weeks.map(w => data[w] || 0);
    }

    res.json({ weeks, campaigns });
  } catch (err) {
    console.error('Campaign trends error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Client portal: recent activity — wins, inspections, and recent leads
app.get('/api/client-portal/recent-activity', requireClientToken, async (req, res) => {
  try {
    const cid = req.clientId;
    const { source = 'all' } = req.query;

    // Source attribution filter (same logic as funnel endpoint)
    let hcSourceWhere = '';
    if (source === 'google_ads') {
      hcSourceWhere = `AND (hc.attribution_override = 'google_ads'
        OR EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads') AND COALESCE(c.source_name,'') <> 'LSA')
        OR EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND (f.gclid IS NOT NULL OR f.source ILIKE '%google ads%')))`;
    } else if (source === 'lsa') {
      hcSourceWhere = `AND (hc.attribution_override = 'lsa'
        OR EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source_name = 'LSA'))`;
    } else if (source === 'gbp') {
      hcSourceWhere = `AND (COALESCE(hc.attribution_override, '') = 'gbp'
        OR EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source = 'Google My Business')
        OR EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND f.source = 'Google My Business'))
      AND COALESCE(hc.attribution_override, '') <> 'google_ads'
      AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads'))
      AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source_name = 'LSA')
      AND NOT EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND (f.gclid IS NOT NULL OR f.source ILIKE '%google ads%'))`;
    } else if (source === 'direct') {
      hcSourceWhere = `AND (
        EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.medium = 'direct' OR c.medium = 'Organic' OR c.source ILIKE '%organic%'))
      )
      AND COALESCE(hc.attribution_override, '') NOT IN ('google_ads', 'lsa', 'gbp')
      AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads' OR c.source_name = 'LSA' OR c.source = 'Google My Business'))`;
    } else if (source === 'other') {
      hcSourceWhere = `AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads' OR c.source_name = 'LSA' OR c.source = 'Google My Business' OR c.medium IN ('direct','Organic') OR c.source ILIKE '%organic%'))
      AND NOT EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND (f.gclid IS NOT NULL OR f.source ILIKE '%google%'))
      AND COALESCE(hc.attribution_override, '') NOT IN ('google_ads', 'lsa', 'gbp')`;
    }
    const flagExclude = `AND COALESCE(hc.client_flag_reason, '') NOT IN ('spam', 'out_of_area', 'wrong_service')`;
    const spamExclude = `AND NOT EXISTS (
      SELECT 1 FROM ghl_contacts gc
      WHERE gc.phone_normalized = hc.phone_normalized AND gc.customer_id = hc.customer_id
        AND (
          gc.lost_reason SIMILAR TO '%(spam|not a lead|wrong number|out of area|wrong service|abandoned)%'
          OR EXISTS (SELECT 1 FROM ghl_opportunities o WHERE o.ghl_contact_id = gc.ghl_contact_id
            AND o.stage_name SIMILAR TO '%(spam|not a lead|out of area|wrong service)%')
        )
    )`;

    // Derive source label per lead
    const sourceLabel = `CASE
      WHEN hc.attribution_override IS NOT NULL THEN hc.attribution_override
      WHEN EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source_name = 'LSA') THEN 'lsa'
      WHEN EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads') AND COALESCE(c.source_name,'') <> 'LSA') THEN 'google_ads'
      WHEN EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND (f.gclid IS NOT NULL OR f.source ILIKE '%google ads%')) THEN 'google_ads'
      WHEN EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source = 'Google My Business') THEN 'gbp'
      WHEN EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND f.source = 'Google My Business') THEN 'gbp'
      ELSE 'other'
    END`;

    // Query 1: Recent wins — jobs completed or estimates approved (sorted by EVENT date, not lead date)
    const winsResult = await pool.query(`
      (
        SELECT
          hc.first_name || ' ' || hc.last_name as name,
          'job_completed' as event_type,
          COALESCE(j.completed_at, j.scheduled_at) as event_date,
          ROUND(j.total_amount_cents / 100.0) as amount,
          hc.hcp_created_at as lead_date,
          ${sourceLabel} as lead_source,
          EXTRACT(DAY FROM COALESCE(j.completed_at, j.scheduled_at) - hc.hcp_created_at)::int as days_to_close
        FROM hcp_customers hc
        JOIN hcp_jobs j ON j.hcp_customer_id = hc.hcp_customer_id
          AND j.record_status = 'active'
          AND j.status IN ('complete rated','complete unrated')
          AND j.total_amount_cents > 0
        WHERE hc.customer_id = $1 ${hcSourceWhere} ${flagExclude} ${spamExclude}
          AND COALESCE(j.completed_at, j.scheduled_at) >= NOW() - INTERVAL '60 days'
        ORDER BY COALESCE(j.completed_at, j.scheduled_at) DESC
        LIMIT 5
      )
      UNION ALL
      (
        SELECT
          hc.first_name || ' ' || hc.last_name as name,
          'estimate_approved' as event_type,
          COALESCE(eg.approved_at, eg.sent_at) as event_date,
          ROUND(eg.approved_total_cents / 100.0) as amount,
          hc.hcp_created_at as lead_date,
          ${sourceLabel} as lead_source,
          EXTRACT(DAY FROM COALESCE(eg.approved_at, eg.sent_at) - hc.hcp_created_at)::int as days_to_close
        FROM hcp_customers hc
        JOIN v_estimate_groups eg ON eg.hcp_customer_id = hc.hcp_customer_id
          AND eg.status = 'approved' AND eg.count_revenue
          AND eg.approved_total_cents > 0
        WHERE hc.customer_id = $1 ${hcSourceWhere} ${flagExclude} ${spamExclude}
          AND COALESCE(eg.approved_at, eg.sent_at) >= NOW() - INTERVAL '90 days'
        ORDER BY COALESCE(eg.approved_at, eg.sent_at) DESC
        LIMIT 5
      )
      ORDER BY event_date DESC
      LIMIT 5
    `, [cid]);

    // Query 2a: Upcoming inspections (scheduled in the future, soonest first)
    const upcomingInspResult = await pool.query(`
      SELECT
        hc.first_name || ' ' || hc.last_name as name,
        'upcoming' as event_type,
        ins.scheduled_at,
        hc.hcp_created_at as lead_date,
        ${sourceLabel} as lead_source,
        EXTRACT(DAY FROM ins.hcp_created_at - hc.hcp_created_at)::int as days_to_booking
      FROM hcp_customers hc
      JOIN hcp_inspections ins ON ins.hcp_customer_id = hc.hcp_customer_id
        AND ins.record_status = 'active'
        AND ins.status NOT IN ('user canceled','pro canceled','complete rated','complete unrated')
        AND COALESCE(ins.inferred_complete, false) = false
        AND ins.scheduled_at >= NOW()
      WHERE hc.customer_id = $1 ${hcSourceWhere} ${flagExclude} ${spamExclude}
      ORDER BY ins.scheduled_at ASC
      LIMIT 3
    `, [cid]);

    // Query 2b: Recent past inspections (completed or past scheduled, most recent first)
    const recentInspResult = await pool.query(`
      SELECT
        hc.first_name || ' ' || hc.last_name as name,
        CASE
          WHEN ins.status IN ('complete rated','complete unrated') OR ins.inferred_complete = true
            THEN 'completed'
          ELSE 'booked'
        END as event_type,
        ins.scheduled_at,
        hc.hcp_created_at as lead_date,
        ${sourceLabel} as lead_source,
        EXTRACT(DAY FROM ins.hcp_created_at - hc.hcp_created_at)::int as days_to_booking
      FROM hcp_customers hc
      JOIN hcp_inspections ins ON ins.hcp_customer_id = hc.hcp_customer_id
        AND ins.record_status = 'active'
        AND ins.status NOT IN ('user canceled','pro canceled')
        AND (ins.status IN ('complete rated','complete unrated') OR ins.inferred_complete = true
             OR ins.scheduled_at < NOW())
      WHERE hc.customer_id = $1 ${hcSourceWhere} ${flagExclude} ${spamExclude}
        AND ins.scheduled_at >= NOW() - INTERVAL '30 days'
      ORDER BY ins.scheduled_at DESC
      LIMIT 3
    `, [cid]);

    // Query 3: Recent quality leads with current stage
    const leadsResult = await pool.query(`
      SELECT
        hc.first_name || ' ' || hc.last_name as name,
        hc.hcp_created_at as lead_date,
        ${sourceLabel} as lead_source,
        CASE
          WHEN EXISTS (SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status IN ('complete rated','complete unrated')) THEN 'Job Completed'
          WHEN EXISTS (SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status NOT IN ('user canceled','pro canceled')) THEN 'Job Scheduled'
          WHEN EXISTS (SELECT 1 FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status = 'approved' AND eg.count_revenue) THEN 'Estimate Approved'
          WHEN EXISTS (SELECT 1 FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status IN ('sent','approved','declined') AND eg.count_revenue) THEN 'Estimate Sent'
          WHEN EXISTS (SELECT 1 FROM hcp_inspections ins WHERE ins.hcp_customer_id = hc.hcp_customer_id AND ins.record_status = 'active' AND (ins.status IN ('complete rated','complete unrated') OR ins.inferred_complete = true)) THEN 'Inspection Done'
          WHEN EXISTS (SELECT 1 FROM hcp_inspections ins WHERE ins.hcp_customer_id = hc.hcp_customer_id AND ins.record_status = 'active' AND ins.status NOT IN ('user canceled','pro canceled')) THEN 'Inspection Booked'
          ELSE 'New Lead'
        END as current_stage,
        -- Get call duration for context
        (SELECT ROUND(c.duration / 60.0, 1) FROM calls c WHERE c.callrail_id = hc.callrail_id LIMIT 1) as call_minutes
      FROM hcp_customers hc
      WHERE hc.customer_id = $1 ${hcSourceWhere} ${flagExclude} ${spamExclude}
      ORDER BY hc.hcp_created_at DESC
      LIMIT 10
    `, [cid]);

    // Query 4: Outstanding estimates (sent but not approved — fallback when no wins)
    const estResult = await pool.query(`
      SELECT
        hc.first_name || ' ' || hc.last_name as name,
        eg.sent_at as estimate_date,
        ROUND(eg.highest_option_cents / 100.0) as amount,
        eg.status,
        hc.hcp_created_at as lead_date,
        ${sourceLabel} as lead_source,
        EXTRACT(DAY FROM NOW() - eg.sent_at)::int as days_waiting
      FROM hcp_customers hc
      JOIN v_estimate_groups eg ON eg.hcp_customer_id = hc.hcp_customer_id
        AND eg.status = 'sent' AND eg.count_revenue
        AND eg.highest_option_cents > 0
      WHERE hc.customer_id = $1 ${hcSourceWhere} ${flagExclude} ${spamExclude}
      ORDER BY eg.sent_at DESC
      LIMIT 5
    `, [cid]);

    res.json({
      wins: winsResult.rows,
      open_estimates: estResult.rows,
      upcoming_inspections: upcomingInspResult.rows,
      recent_inspections: recentInspResult.rows,
      leads: leadsResult.rows,
    });
  } catch (err) {
    console.error('Recent activity error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Client portal: funnel stage drill-down — returns leads for a given stage
app.get('/api/client-portal/funnel/leads', requireClientToken, async (req, res) => {
  try {
    const cid = req.clientId;
    const { stage, source = 'all', date_from, date_to } = req.query;
    if (!stage) return res.status(400).json({ error: 'stage required' });

    // GHL clients: drill-down from ghl_estimates
    if (req.clientFMS === 'ghl') {
      const ghlParams = [cid];
      let gIdx = 2;
      let geDateWhere = '';
      if (date_from) { geDateWhere += ` AND ge.issue_date >= $${gIdx}`; ghlParams.push(date_from); gIdx++; }
      if (date_to) { geDateWhere += ` AND ge.issue_date < ($${gIdx}::date + 1)`; ghlParams.push(date_to); gIdx++; }

      // Stage filter
      let ghlStageWhere = '';
      if (stage === 'estimate_sent') ghlStageWhere = `AND ge.status IN ('sent','accepted','invoiced')`;
      else if (stage === 'estimate_approved') ghlStageWhere = `AND ge.status IN ('accepted','invoiced')`;
      else if (stage === 'job_scheduled' || stage === 'job_completed' || stage === 'revenue_closed') ghlStageWhere = `AND ge.status = 'invoiced'`;
      else if (stage === 'open_estimates') ghlStageWhere = `AND ge.status = 'sent'`;

      // For leads/contacts stages, return GHL estimates + unmatched CallRail
      if (stage === 'leads' || stage === 'contacts') {
        // Get all GHL estimate contacts
        const matchedResult = await pool.query(`
          SELECT DISTINCT ON (ge.phone_normalized)
            ge.ghl_contact_id as hcp_customer_id,
            split_part(ge.contact_name, ' ', 1) as first_name,
            CASE WHEN position(' ' in COALESCE(ge.contact_name,'')) > 0
              THEN substring(ge.contact_name from position(' ' in ge.contact_name)+1) ELSE NULL END as last_name,
            ge.contact_phone as phone,
            ge.issue_date as lead_date,
            ge.total_cents / 100 as revenue,
            CASE ge.status WHEN 'invoiced' THEN 'Invoiced' WHEN 'accepted' THEN 'Accepted' WHEN 'sent' THEN 'Estimate Sent' ELSE 'Draft' END as current_stage,
            'Other' as lead_source,
            NULL as flagged,
            NULL as callrail_id,
            false as inferred
          FROM ghl_estimates ge
          WHERE ge.customer_id = $1 ${geDateWhere} AND ge.status != 'draft'
          ORDER BY ge.phone_normalized, ge.issue_date DESC
          LIMIT 100
        `, ghlParams);
        return res.json({ stage, source, count: matchedResult.rows.length, leads: matchedResult.rows });
      }

      // For estimate/job/revenue stages
      const result = await pool.query(`
        SELECT
          ge.ghl_estimate_id as hcp_customer_id,
          split_part(ge.contact_name, ' ', 1) as first_name,
          CASE WHEN position(' ' in COALESCE(ge.contact_name,'')) > 0
            THEN substring(ge.contact_name from position(' ' in ge.contact_name)+1) ELSE NULL END as last_name,
          ge.contact_phone as phone,
          ge.issue_date as lead_date,
          ge.total_cents / 100 as revenue,
          CASE ge.status WHEN 'invoiced' THEN 'Invoiced' WHEN 'accepted' THEN 'Accepted' WHEN 'sent' THEN 'Estimate Sent' ELSE ge.status END as current_stage,
          'Other' as lead_source,
          NULL as flagged,
          NULL as callrail_id,
          false as inferred
        FROM ghl_estimates ge
        WHERE ge.customer_id = $1 ${geDateWhere} ${ghlStageWhere} AND ge.status != 'draft'
        ORDER BY ge.issue_date DESC
        LIMIT 100
      `, ghlParams);
      return res.json({ stage, source, count: result.rows.length, leads: result.rows.map(r => ({
        hcp_customer_id: r.hcp_customer_id,
        first_name: r.first_name,
        last_name: r.last_name,
        phone: r.phone,
        lead_date: r.lead_date,
        revenue: parseFloat(r.revenue) || 0,
        current_stage: r.current_stage,
        lead_source: r.lead_source,
        flagged: null,
        callrail_id: null,
        inferred: false,
      })) });
    }

    const params = [cid];
    let paramIdx = 2;

    let hcDateWhere = '';
    let crDateWhere = '';
    let fmDateWhere = '';
    if (date_from) { hcDateWhere += ` AND hc.hcp_created_at >= $${paramIdx}`; crDateWhere += ` AND c2.start_time >= $${paramIdx}`; fmDateWhere += ` AND f2.submitted_at >= $${paramIdx}`; params.push(date_from); paramIdx++; }
    if (date_to) { hcDateWhere += ` AND hc.hcp_created_at < ($${paramIdx}::date + 1)`; crDateWhere += ` AND c2.start_time < ($${paramIdx}::date + 1)`; fmDateWhere += ` AND f2.submitted_at < ($${paramIdx}::date + 1)`; params.push(date_to); paramIdx++; }

    // Source filter
    let hcSourceWhere = '';
    let crSourceWhere = '';
    let fmSourceWhere = '';
    if (source === 'google_ads') {
      hcSourceWhere = `AND (COALESCE(hc.attribution_override,'') = 'google_ads'
        OR EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads') AND COALESCE(c.source_name,'') <> 'LSA')
        OR EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND (f.gclid IS NOT NULL OR f.source ILIKE '%google ads%')))`;
      crSourceWhere = `AND (c2.gclid IS NOT NULL OR c2.classified_source = 'google_ads') AND COALESCE(c2.source_name,'') <> 'LSA'`;
      fmSourceWhere = `AND (f2.gclid IS NOT NULL OR f2.source ILIKE '%google ads%')`;
    } else if (source === 'lsa') {
      hcSourceWhere = `AND (COALESCE(hc.attribution_override,'') = 'lsa'
        OR EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source_name = 'LSA'))`;
      crSourceWhere = `AND c2.source_name = 'LSA'`;
      fmSourceWhere = `AND 1=0`; // LSA doesn't come through forms
    } else if (source === 'gbp') {
      hcSourceWhere = `AND (COALESCE(hc.attribution_override,'') = 'gbp'
        OR EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source = 'Google My Business')
        OR EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND f.source = 'Google My Business'))
      AND COALESCE(hc.attribution_override,'') <> 'google_ads'
      AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads'))
      AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source_name = 'LSA')
      AND NOT EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND (f.gclid IS NOT NULL OR f.source ILIKE '%google ads%'))`;
      crSourceWhere = `AND c2.source = 'Google My Business' AND c2.gclid IS NULL AND COALESCE(c2.classified_source,'') <> 'google_ads' AND COALESCE(c2.source_name,'') <> 'LSA'`;
      fmSourceWhere = `AND f2.source = 'Google My Business' AND f2.gclid IS NULL`;
    } else if (source === 'direct') {
      hcSourceWhere = `AND (
        EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.medium = 'direct' OR c.medium = 'Organic' OR c.source ILIKE '%organic%'))
      )
      AND COALESCE(hc.attribution_override, '') NOT IN ('google_ads', 'lsa', 'gbp')
      AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads' OR c.source_name = 'LSA' OR c.source = 'Google My Business'))`;
      crSourceWhere = `AND (c2.medium = 'direct' OR c2.medium = 'Organic' OR c2.source ILIKE '%organic%') AND c2.gclid IS NULL AND COALESCE(c2.classified_source,'') <> 'google_ads' AND COALESCE(c2.source_name,'') <> 'LSA' AND c2.source <> 'Google My Business'`;
      fmSourceWhere = `AND f2.gclid IS NULL AND COALESCE(f2.source,'') NOT ILIKE '%google%' AND COALESCE(f2.source,'') <> 'Google My Business' AND (f2.medium = 'direct' OR f2.medium IS NULL OR f2.medium = 'Organic')`;
    } else if (source === 'other') {
      hcSourceWhere = `AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads' OR c.source_name = 'LSA' OR c.source = 'Google My Business' OR c.medium IN ('direct','Organic') OR c.source ILIKE '%organic%'))
      AND NOT EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND (f.gclid IS NOT NULL OR f.source ILIKE '%google%'))
      AND COALESCE(hc.attribution_override, '') NOT IN ('google_ads', 'lsa', 'gbp')`;
      crSourceWhere = `AND c2.gclid IS NULL AND COALESCE(c2.classified_source,'') <> 'google_ads' AND COALESCE(c2.source_name,'') <> 'LSA' AND c2.source <> 'Google My Business' AND COALESCE(c2.medium,'') NOT IN ('direct','Organic') AND COALESCE(c2.source,'') NOT ILIKE '%organic%'`;
      fmSourceWhere = `AND f2.gclid IS NULL AND COALESCE(f2.source,'') NOT ILIKE '%google%' AND COALESCE(f2.source,'') <> 'Google My Business' AND COALESCE(f2.medium,'') NOT IN ('direct','Organic')`;
    }

    const flagExclude = `AND COALESCE(hc.client_flag_reason, '') NOT IN ('spam', 'out_of_area', 'wrong_service')`;

    // Stage filter for HCP-matched leads
    const stageFilters = {
      leads: '',
      contacts: '',
      inspection_scheduled: `AND EXISTS (SELECT 1 FROM hcp_inspections ins WHERE ins.hcp_customer_id = hc.hcp_customer_id AND ins.record_status = 'active' AND ins.status NOT IN ('user canceled','pro canceled'))`,
      inspection_completed: `AND EXISTS (SELECT 1 FROM hcp_inspections ins WHERE ins.hcp_customer_id = hc.hcp_customer_id AND ins.record_status = 'active' AND (ins.status IN ('complete rated','complete unrated') OR ins.inferred_complete = true))`,
      estimate_sent: `AND EXISTS (SELECT 1 FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status IN ('sent','approved','declined') AND eg.count_revenue)`,
      estimate_approved: `AND EXISTS (SELECT 1 FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status = 'approved' AND eg.count_revenue)`,
      job_scheduled: `AND EXISTS (SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status NOT IN ('user canceled','pro canceled'))`,
      job_completed: `AND EXISTS (SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status IN ('complete rated','complete unrated'))`,
      revenue_closed: `AND (
        COALESCE((SELECT SUM(i.amount_cents) FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled'), 0) > 0
        OR COALESCE((SELECT SUM(eg.approved_total_cents) FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status = 'approved' AND eg.count_revenue), 0) > 0
      )`,
      open_estimates: `AND EXISTS (SELECT 1 FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status = 'sent' AND eg.estimate_type = 'treatment' AND eg.count_revenue)
        AND NOT EXISTS (SELECT 1 FROM v_estimate_groups eg2 WHERE eg2.hcp_customer_id = hc.hcp_customer_id AND eg2.status = 'approved' AND eg2.count_revenue)
        AND NOT EXISTS (SELECT 1 FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status NOT IN ('canceled','voided') AND i.invoice_type = 'treatment' AND i.amount_cents > 0)`,
    };

    const stageWhere = stageFilters[stage] || '';
    // For contacts, skip spam exclusion
    const useFlag = (stage === 'contacts') ? '' : flagExclude;

    // Get matched leads
    const matchedResult = await pool.query(`
      SELECT
        hc.hcp_customer_id,
        COALESCE(NULLIF(hc.first_name,''), (SELECT split_part(gc.first_name,' ',1) FROM ghl_contacts gc WHERE gc.phone_normalized = hc.phone_normalized AND gc.customer_id = hc.customer_id LIMIT 1), (SELECT split_part(c.customer_name,' ',1) FROM calls c WHERE c.callrail_id = hc.callrail_id LIMIT 1)) as first_name,
        COALESCE(NULLIF(hc.last_name,''), (SELECT gc.last_name FROM ghl_contacts gc WHERE gc.phone_normalized = hc.phone_normalized AND gc.customer_id = hc.customer_id LIMIT 1), (SELECT CASE WHEN position(' ' in COALESCE(c.customer_name,'')) > 0 THEN substring(c.customer_name from position(' ' in c.customer_name)+1) ELSE NULL END FROM calls c WHERE c.callrail_id = hc.callrail_id LIMIT 1)) as last_name,
        hc.phone_primary,
        hc.hcp_created_at as lead_date,
        hc.client_flag_reason,
        hc.callrail_id,
        COALESCE((SELECT ins.inferred_complete FROM hcp_inspections ins WHERE ins.hcp_customer_id = hc.hcp_customer_id AND ins.record_status = 'active' AND ins.inferred_complete = true LIMIT 1), false) as inferred_inspection,
        COALESCE((SELECT SUM(i.amount_cents) FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled'), 0) / 100 as invoice_revenue,
        COALESCE((SELECT SUM(eg.approved_total_cents) FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status = 'approved' AND eg.count_revenue), 0) / 100 as approved_revenue,
        COALESCE((SELECT SUM(eg.highest_option_cents) FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status IN ('sent','approved','declined') AND eg.count_revenue), 0) / 100 as estimate_sent_revenue,
        CASE
          WHEN EXISTS (SELECT 1 FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status = 'paid') THEN 'Invoice Paid'
          WHEN EXISTS (SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status IN ('complete rated','complete unrated')) THEN 'Job Complete'
          WHEN EXISTS (SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status NOT IN ('user canceled','pro canceled')) THEN 'Job Scheduled'
          WHEN EXISTS (SELECT 1 FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status = 'approved' AND eg.count_revenue) THEN 'Estimate Approved'
          WHEN EXISTS (SELECT 1 FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status IN ('sent','approved','declined') AND eg.count_revenue) THEN 'Estimate Sent'
          WHEN EXISTS (SELECT 1 FROM hcp_inspections ins WHERE ins.hcp_customer_id = hc.hcp_customer_id AND ins.record_status = 'active' AND (ins.status IN ('complete rated','complete unrated') OR ins.inferred_complete = true)) THEN 'Inspection Complete'
          WHEN EXISTS (SELECT 1 FROM hcp_inspections ins WHERE ins.hcp_customer_id = hc.hcp_customer_id AND ins.record_status = 'active' AND ins.status NOT IN ('user canceled','pro canceled')) THEN 'Inspection Scheduled'
          ELSE 'Lead'
        END as current_stage,
        COALESCE(hc.attribution_override,
          CASE
            WHEN EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source_name = 'LSA') THEN 'LSA'
            WHEN EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads')) THEN 'Google Ads'
            WHEN EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source = 'Google My Business') THEN 'GBP'
            WHEN EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND (f.gclid IS NOT NULL OR f.source ILIKE '%google ads%')) THEN 'Google Ads'
            WHEN EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND f.source = 'Google My Business') THEN 'GBP'
            WHEN EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source = 'Google Organic') THEN 'Organic'
            WHEN EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source = 'Direct') THEN 'Direct'
            ELSE 'Other'
          END
        ) as lead_source
      FROM hcp_customers hc
      WHERE hc.customer_id = $1 ${hcDateWhere} ${hcSourceWhere} ${useFlag} ${stageWhere}
      ORDER BY hc.hcp_created_at DESC
      LIMIT 100
    `, params);

    // For "leads" and "contacts" stages also include unmatched calls AND forms
    let unmatched = [];
    if (stage === 'leads' || stage === 'contacts') {
      const skipSpamFilter = stage === 'contacts';
      const unmatchedCallsResult = await pool.query(`
        SELECT
          split_part(c.customer_name, ' ', 1) as first_name,
          CASE WHEN position(' ' in COALESCE(c.customer_name,'')) > 0
               THEN substring(c.customer_name from position(' ' in c.customer_name) + 1) ELSE NULL END as last_name,
          normalize_phone(c.caller_phone) as phone_primary,
          c.start_time as lead_date,
          'New Lead' as current_stage,
          CASE WHEN c.source_name = 'LSA' THEN 'LSA'
               WHEN c.gclid IS NOT NULL OR c.classified_source = 'google_ads' THEN 'Google Ads'
               WHEN c.source = 'Google My Business' THEN 'GBP'
               WHEN c.source = 'Google Organic' THEN 'Organic'
               WHEN c.source = 'Direct' THEN 'Direct'
               ELSE 'Other' END as lead_source,
          c.callrail_id,
          'call' as interaction_type
        FROM (
          SELECT DISTINCT ON (normalize_phone(c2.caller_phone)) c2.*
          FROM calls c2
          WHERE c2.customer_id = $1 ${crDateWhere} ${crSourceWhere}
          ORDER BY normalize_phone(c2.caller_phone), c2.start_time
        ) c
        WHERE NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.callrail_id = c.callrail_id AND hc.customer_id = $1)
          AND NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.phone_normalized = normalize_phone(c.caller_phone) AND hc.customer_id = $1)
          ${skipSpamFilter ? '' : `AND NOT EXISTS (SELECT 1 FROM client_flagged_leads fl WHERE fl.customer_id = $1 AND fl.flag_reason IN ('spam','out_of_area','wrong_service') AND (fl.phone_normalized = normalize_phone(c.caller_phone) OR fl.callrail_id = c.callrail_id))`}
        ORDER BY c.start_time DESC
        LIMIT 100
      `, params);

      const unmatchedFormsResult = await pool.query(`
        SELECT
          split_part(f.customer_name, ' ', 1) as first_name,
          CASE WHEN position(' ' in COALESCE(f.customer_name,'')) > 0
               THEN substring(f.customer_name from position(' ' in f.customer_name) + 1) ELSE NULL END as last_name,
          normalize_phone(f.customer_phone) as phone_primary,
          f.submitted_at as lead_date,
          'New Lead (Form)' as current_stage,
          CASE WHEN f.gclid IS NOT NULL OR f.source ILIKE '%google ads%' THEN 'Google Ads'
               WHEN f.source = 'Google My Business' THEN 'GBP'
               ELSE 'Other' END as lead_source,
          f.callrail_id,
          'form' as interaction_type
        FROM (
          SELECT DISTINCT ON (normalize_phone(f2.customer_phone)) f2.*
          FROM form_submissions f2
          WHERE f2.customer_id = $1 AND COALESCE(f2.is_spam, false) = false ${fmDateWhere} ${fmSourceWhere}
          ORDER BY normalize_phone(f2.customer_phone), f2.submitted_at
        ) f
        WHERE NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.callrail_id = f.callrail_id AND hc.customer_id = $1)
          AND NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.phone_normalized = normalize_phone(f.customer_phone) AND hc.customer_id = $1)
          ${skipSpamFilter ? '' : `AND NOT EXISTS (SELECT 1 FROM client_flagged_leads fl WHERE fl.customer_id = $1 AND fl.flag_reason IN ('spam','out_of_area','wrong_service') AND (fl.phone_normalized = normalize_phone(f.customer_phone) OR fl.callrail_id = f.callrail_id))`}
        ORDER BY f.submitted_at DESC
        LIMIT 100
      `, params);

      // Collect call phones for cross-dedup
      const callPhones = new Set(unmatchedCallsResult.rows.map(r => r.phone_primary));

      unmatched = unmatchedCallsResult.rows.map(r => ({
        hcp_customer_id: null,
        first_name: r.first_name,
        last_name: r.last_name,
        phone: r.phone_primary,
        lead_date: r.lead_date,
        revenue: 0,
        current_stage: r.current_stage,
        lead_source: r.lead_source,
        flagged: null,
        callrail_id: r.callrail_id,
      }));

      // Add forms, skipping any whose phone already appeared in calls
      for (const r of unmatchedFormsResult.rows) {
        if (!callPhones.has(r.phone_primary)) {
          unmatched.push({
            hcp_customer_id: null,
            first_name: r.first_name,
            last_name: r.last_name,
            phone: r.phone_primary,
            lead_date: r.lead_date,
            revenue: 0,
            current_stage: r.current_stage,
            lead_source: r.lead_source,
            flagged: null,
            callrail_id: r.callrail_id,
          });
        }
      }
    }

    const leads = matchedResult.rows.map(r => {
      var rev;
      if (stage === 'estimate_sent' || stage === 'estimate_approved' || stage === 'open_estimates') {
        // Always show estimate value for estimate stages
        rev = parseFloat(r.estimate_sent_revenue) || 0;
      } else {
        rev = Math.max(parseFloat(r.invoice_revenue) || 0, parseFloat(r.approved_revenue) || 0);
        if (rev === 0) rev = parseFloat(r.estimate_sent_revenue) || 0;
      }
      return {
        hcp_customer_id: r.hcp_customer_id,
        first_name: r.first_name,
        last_name: r.last_name,
        phone: r.phone_primary,
        lead_date: r.lead_date,
        revenue: rev,
        current_stage: r.current_stage,
        lead_source: r.lead_source,
        flagged: r.client_flag_reason || null,
        callrail_id: r.callrail_id || null,
        inferred: r.inferred_inspection || false,
      };
    }).concat(unmatched);

    res.json({ stage, source, count: leads.length, leads });
  } catch (err) {
    console.error('Funnel drill-down error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Admin: generate portal token for a client
app.post('/api/admin/generate-portal-token', requireAuth, async (req, res) => {
  try {
    const { customer_id } = req.body;
    const token = crypto.randomBytes(24).toString('base64url');
    await pool.query(
      `UPDATE clients SET client_portal_token = $1 WHERE customer_id = $2`, [token, customer_id]
    );
    res.json({ token, url: `/portal/${token}` });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ════════════════════════════════════════════════════════════
// Serve the review page
// ════════════════════════════════════════════════════════════
app.get('/review', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'review.html'));
});
app.post('/review', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'review.html'));
});

app.get('/va-training', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'va-training.html'));
});
app.get('/va-review', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'va-review.html'));
});
app.get('/va-review/:customer_id', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'va-review.html'));
});

app.get('/audit/:customer_id', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'audit.html'));
});
app.get('/audit', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'audit.html'));
});

// ════════════════════════════════════════════════════════════
// Cohort Benchmark: aggregate funnel averages across all clients
// ════════════════════════════════════════════════════════════
app.get('/cohort', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'cohort.html'));
});

app.get('/api/cohort/funnel', requireAuth, async (req, res) => {
  try {
    const { date_from, date_to, source = 'all' } = req.query;

    // Get all active HCP + Jobber clients
    let clientQuery = `SELECT customer_id, name, start_date, field_management_software, inspection_type FROM clients WHERE status = 'active' AND field_management_software IN ('housecall_pro', 'jobber')`;
    // Filter by inspection type for free/paid breakdowns
    if (source === 'google_ads_free') clientQuery += ` AND inspection_type = 'free'`;
    if (source === 'google_ads_paid') clientQuery += ` AND inspection_type = 'paid'`;
    const clientsResult = await pool.query(clientQuery);
    const clients = clientsResult.rows;
    const hcpClients = clients.filter(c => c.field_management_software === 'housecall_pro');
    const jobberClients = clients.filter(c => c.field_management_software === 'jobber');

    if (!clients.length) return res.json({ error: 'No active clients' });

    // Date filter fragments
    let hcDateWhere = '';
    const dateParams = [];
    let dIdx = 1;
    if (date_from) { hcDateWhere += ` AND hc.hcp_created_at >= $${dIdx}`; dateParams.push(date_from); dIdx++; }
    if (date_to) { hcDateWhere += ` AND hc.hcp_created_at < ($${dIdx}::date + 1)`; dateParams.push(date_to); dIdx++; }

    // Source filter (same logic as client funnel but without parameterized customer_id position)
    // google_ads_free and google_ads_paid use same attribution filter — client list is already filtered by inspection_type
    const sourceForFilter = source.startsWith('google_ads') ? 'google_ads' : source;
    let hcSourceWhere = '';
    if (sourceForFilter === 'google_ads') {
      hcSourceWhere = `AND (hc.attribution_override = 'google_ads'
        OR EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads') AND COALESCE(c.source_name,'') <> 'LSA')
        OR EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND (f.gclid IS NOT NULL OR f.source ILIKE '%google ads%')))`;
    } else if (source === 'lsa') {
      hcSourceWhere = `AND (hc.attribution_override = 'lsa'
        OR EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source_name = 'LSA'))`;
    } else if (source === 'gbp') {
      hcSourceWhere = `AND (COALESCE(hc.attribution_override, '') = 'gbp'
        OR EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source = 'Google My Business')
        OR EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND f.source = 'Google My Business'))
      AND COALESCE(hc.attribution_override, '') <> 'google_ads'
      AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads'))
      AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source_name = 'LSA')
      AND NOT EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND (f.gclid IS NOT NULL OR f.source ILIKE '%google ads%'))`;
    }

    const flagExclude = `AND COALESCE(hc.client_flag_reason, '') NOT IN ('spam', 'out_of_area', 'wrong_service')`;
    const spamExclude = `AND NOT EXISTS (
      SELECT 1 FROM ghl_contacts gc
      WHERE gc.phone_normalized = hc.phone_normalized AND gc.customer_id = hc.customer_id
        AND (
          gc.lost_reason SIMILAR TO '%(spam|not a lead|wrong number|out of area|wrong service|abandoned)%'
          OR EXISTS (SELECT 1 FROM ghl_opportunities o WHERE o.ghl_contact_id = gc.ghl_contact_id
            AND o.stage_name SIMILAR TO '%(spam|not a lead|out of area|wrong service)%')
        )
    )`;

    // Query per-client funnel counts in one shot
    const perClientQuery = `
      SELECT
        hc.customer_id,
        COUNT(*) as leads,
        COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM hcp_inspections ins WHERE ins.hcp_customer_id = hc.hcp_customer_id AND ins.record_status = 'active' AND ins.status IN ('scheduled','complete rated','complete unrated','in progress'))) as inspection_scheduled,
        COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM hcp_inspections ins WHERE ins.hcp_customer_id = hc.hcp_customer_id AND ins.record_status = 'active' AND (ins.status IN ('complete rated','complete unrated') OR ins.inferred_complete = true))) as inspection_completed,
        COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status IN ('sent','approved','declined') AND eg.count_revenue)) as estimate_sent,
        COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status = 'approved' AND eg.count_revenue)) as estimate_approved,
        COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status IN ('scheduled','complete rated','complete unrated','in progress'))) as job_scheduled,
        COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status IN ('complete rated','complete unrated'))) as job_completed,
        -- Revenue
        COALESCE(SUM(
          COALESCE((SELECT SUM(i.amount_cents) FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'inspection'), 0) +
          CASE
            WHEN COALESCE((SELECT SUM(i.amount_cents) FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'treatment'), 0) > 0
              OR COALESCE((SELECT SUM(eg.approved_total_cents) FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status = 'approved' AND eg.count_revenue), 0) > 0
            THEN GREATEST(
              COALESCE((SELECT SUM(i.amount_cents) FROM hcp_invoices i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.status <> 'canceled' AND i.invoice_type = 'treatment'), 0),
              COALESCE((SELECT SUM(eg.approved_total_cents) FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status = 'approved' AND eg.count_revenue), 0)
            )
            ELSE
              COALESCE((SELECT SUM(j.total_amount_cents) FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status NOT IN ('user canceled','pro canceled') AND j.count_revenue = true), 0) +
              COALESCE((SELECT SUM(ins.total_amount_cents) FROM hcp_inspections ins WHERE ins.hcp_customer_id = hc.hcp_customer_id AND ins.record_status = 'active' AND ins.status IN ('complete rated','complete unrated') AND ins.count_revenue = true), 0)
          END
        ), 0) as roas_revenue_cents
      FROM hcp_customers hc
      WHERE hc.customer_id = ANY($${dIdx}::bigint[])
        ${hcDateWhere} ${hcSourceWhere} ${flagExclude} ${spamExclude}
      GROUP BY hc.customer_id
    `;

    const hcpIds = hcpClients.map(c => c.customer_id);
    const customerIds = clients.map(c => c.customer_id);
    const queryParams = [...dateParams, hcpIds];
    const perClientResult = hcpIds.length
      ? await pool.query(perClientQuery, queryParams)
      : { rows: [] };

    // Jobber clients: same funnel structure, different tables
    if (jobberClients.length) {
      const jobberIds = jobberClients.map(c => c.customer_id);
      let jcDateWhere = '';
      const jDateParams = [];
      let jIdx = 1;
      if (date_from) { jcDateWhere += ` AND jc.jobber_created_at >= $${jIdx}`; jDateParams.push(date_from); jIdx++; }
      if (date_to) { jcDateWhere += ` AND jc.jobber_created_at < ($${jIdx}::date + 1)`; jDateParams.push(date_to); jIdx++; }

      // Jobber source filter uses same CallRail attribution
      let jcSourceWhere = '';
      if (sourceForFilter === 'google_ads') {
        jcSourceWhere = `AND (jc.attribution_override = 'google_ads'
          OR EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = jc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads') AND COALESCE(c.source_name,'') <> 'LSA')
          OR EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = jc.customer_id AND (f.callrail_id = jc.callrail_id OR normalize_phone(f.customer_phone) = jc.phone_normalized) AND (f.gclid IS NOT NULL OR f.source ILIKE '%google ads%')))`;
      } else if (source === 'lsa') {
        jcSourceWhere = `AND (jc.attribution_override = 'lsa'
          OR EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = jc.callrail_id AND c.source_name = 'LSA'))`;
      } else if (source === 'gbp') {
        jcSourceWhere = `AND (COALESCE(jc.attribution_override, '') = 'gbp'
          OR EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = jc.callrail_id AND c.source = 'Google My Business'))
        AND COALESCE(jc.attribution_override, '') <> 'google_ads'
        AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = jc.callrail_id AND (c.gclid IS NOT NULL OR c.classified_source = 'google_ads'))
        AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = jc.callrail_id AND c.source_name = 'LSA')`;
      }

      const jobberQuery = `
        SELECT
          jc.customer_id,
          COUNT(*) as leads,
          COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM jobber_quotes jq WHERE jq.jobber_customer_id = jc.jobber_customer_id AND jq.customer_id = jc.customer_id AND jq.status NOT IN ('draft','archived'))
            OR EXISTS (SELECT 1 FROM jobber_requests jr WHERE jr.jobber_customer_id = jc.jobber_customer_id AND jr.customer_id = jc.customer_id AND jr.has_assessment = true)) as inspection_scheduled,
          COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM jobber_quotes jq WHERE jq.jobber_customer_id = jc.jobber_customer_id AND jq.customer_id = jc.customer_id AND jq.status NOT IN ('draft','archived'))
            OR EXISTS (SELECT 1 FROM jobber_requests jr WHERE jr.jobber_customer_id = jc.jobber_customer_id AND jr.customer_id = jc.customer_id AND jr.assessment_completed_at IS NOT NULL)) as inspection_completed,
          COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM jobber_quotes jq WHERE jq.jobber_customer_id = jc.jobber_customer_id AND jq.customer_id = jc.customer_id AND jq.status IN ('awaiting_response','approved','converted','changes_requested'))) as estimate_sent,
          COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM jobber_quotes jq WHERE jq.jobber_customer_id = jc.jobber_customer_id AND jq.customer_id = jc.customer_id AND jq.status IN ('approved','converted'))) as estimate_approved,
          COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM jobber_jobs jj WHERE jj.jobber_customer_id = jc.jobber_customer_id AND jj.customer_id = jc.customer_id AND jj.status NOT IN ('archived'))) as job_scheduled,
          COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM jobber_jobs jj WHERE jj.jobber_customer_id = jc.jobber_customer_id AND jj.customer_id = jc.customer_id AND jj.status IN ('late','requires_invoicing'))) as job_completed,
          COALESCE(SUM(
            COALESCE((SELECT SUM(ji.total_cents) FROM jobber_invoices ji WHERE ji.jobber_customer_id = jc.jobber_customer_id AND ji.customer_id = jc.customer_id AND ji.status NOT IN ('draft','void')), 0)
          ), 0) as roas_revenue_cents
        FROM jobber_customers jc
        WHERE jc.customer_id = ANY($${jIdx}::bigint[])
          AND jc.is_archived = false
          ${jcDateWhere} ${jcSourceWhere}
        GROUP BY jc.customer_id
      `;
      jDateParams.push(jobberIds);
      const jobberResult = await pool.query(jobberQuery, jDateParams);
      // Merge Jobber results into perClientResult
      jobberResult.rows.forEach(r => { perClientResult.rows.push(r); });
    }

    // Get unmatched call/form leads per client (same logic as client funnel)
    let crSourceWhere = '';
    let fmSourceWhere = '';
    let crDateWhere = '';
    let fmDateWhere = '';
    // Build date filters for calls/forms using positional params
    const unmatchedParams = [];
    let umIdx = 1;
    if (date_from) { crDateWhere += ` AND c2.start_time >= $${umIdx}`; fmDateWhere += ` AND f2.submitted_at >= $${umIdx}`; unmatchedParams.push(date_from); umIdx++; }
    if (date_to) { crDateWhere += ` AND c2.start_time < ($${umIdx}::date + 1)`; fmDateWhere += ` AND f2.submitted_at < ($${umIdx}::date + 1)`; unmatchedParams.push(date_to); umIdx++; }
    if (sourceForFilter === 'google_ads') {
      crSourceWhere = `AND (c2.gclid IS NOT NULL OR c2.classified_source = 'google_ads') AND COALESCE(c2.source_name,'') <> 'LSA'`;
      fmSourceWhere = `AND (f2.gclid IS NOT NULL OR f2.source ILIKE '%google ads%')`;
    } else if (source === 'lsa') {
      crSourceWhere = `AND c2.source_name = 'LSA'`;
      fmSourceWhere = `AND 1=0`;
    } else if (source === 'gbp') {
      crSourceWhere = `AND c2.source = 'Google My Business' AND c2.gclid IS NULL AND COALESCE(c2.classified_source,'') <> 'google_ads' AND COALESCE(c2.source_name,'') <> 'LSA'`;
      fmSourceWhere = `AND f2.source = 'Google My Business' AND f2.gclid IS NULL`;
    }
    // source === 'all' → no source filter

    const unmatchedQuery = `
      SELECT customer_id, SUM(cnt) as unmatched_leads FROM (
        SELECT c2.customer_id, COUNT(DISTINCT normalize_phone(c2.caller_phone)) as cnt
        FROM calls c2
        WHERE c2.customer_id = ANY($${umIdx}::bigint[])
          ${crDateWhere} ${crSourceWhere}
          AND NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.callrail_id = c2.callrail_id AND hc.customer_id = c2.customer_id)
          AND NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.phone_normalized = normalize_phone(c2.caller_phone) AND hc.customer_id = c2.customer_id)
          AND NOT EXISTS (SELECT 1 FROM jobber_customers jc WHERE jc.callrail_id = c2.callrail_id AND jc.customer_id = c2.customer_id)
          AND NOT EXISTS (SELECT 1 FROM jobber_customers jc WHERE jc.phone_normalized = normalize_phone(c2.caller_phone) AND jc.customer_id = c2.customer_id)
          AND NOT EXISTS (SELECT 1 FROM client_flagged_leads fl WHERE fl.customer_id = c2.customer_id AND fl.flag_reason IN ('spam','out_of_area','wrong_service') AND (fl.callrail_id = c2.callrail_id OR fl.phone_normalized = normalize_phone(c2.caller_phone)))
          AND NOT EXISTS (SELECT 1 FROM ghl_contacts gc WHERE gc.phone_normalized = normalize_phone(c2.caller_phone) AND gc.customer_id = c2.customer_id
            AND (gc.lost_reason SIMILAR TO '%(spam|not a lead|wrong number|out of area|wrong service|abandoned)%'
              OR EXISTS (SELECT 1 FROM ghl_opportunities o WHERE o.ghl_contact_id = gc.ghl_contact_id AND o.stage_name SIMILAR TO '%(spam|not a lead|out of area|wrong service)%')))
        GROUP BY c2.customer_id
        UNION ALL
        SELECT f2.customer_id, COUNT(DISTINCT normalize_phone(f2.customer_phone)) as cnt
        FROM form_submissions f2
        WHERE f2.customer_id = ANY($${umIdx}::bigint[])
          AND COALESCE(f2.is_spam, false) = false
          ${fmDateWhere} ${fmSourceWhere}
          AND NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.callrail_id = f2.callrail_id AND hc.customer_id = f2.customer_id)
          AND NOT EXISTS (SELECT 1 FROM hcp_customers hc WHERE hc.phone_normalized = normalize_phone(f2.customer_phone) AND hc.customer_id = f2.customer_id)
          AND NOT EXISTS (SELECT 1 FROM jobber_customers jc WHERE jc.callrail_id = f2.callrail_id AND jc.customer_id = f2.customer_id)
          AND NOT EXISTS (SELECT 1 FROM jobber_customers jc WHERE jc.phone_normalized = normalize_phone(f2.customer_phone) AND jc.customer_id = f2.customer_id)
          AND NOT EXISTS (SELECT 1 FROM client_flagged_leads fl WHERE fl.customer_id = f2.customer_id AND fl.flag_reason IN ('spam','out_of_area','wrong_service') AND (fl.callrail_id = f2.callrail_id OR fl.phone_normalized = normalize_phone(f2.customer_phone)))
          AND NOT EXISTS (SELECT 1 FROM ghl_contacts gc WHERE gc.phone_normalized = normalize_phone(f2.customer_phone) AND gc.customer_id = f2.customer_id
            AND (gc.lost_reason SIMILAR TO '%(spam|not a lead|wrong number|out of area|wrong service|abandoned)%'
              OR EXISTS (SELECT 1 FROM ghl_opportunities o WHERE o.ghl_contact_id = gc.ghl_contact_id AND o.stage_name SIMILAR TO '%(spam|not a lead|out of area|wrong service)%')))
          AND NOT EXISTS (SELECT 1 FROM calls cc WHERE cc.customer_id = f2.customer_id AND normalize_phone(cc.caller_phone) = normalize_phone(f2.customer_phone) ${crSourceWhere.replace(/c2\./g, 'cc.')})
        GROUP BY f2.customer_id
      ) sub
      GROUP BY customer_id
    `;
    unmatchedParams.push(customerIds);
    const unmatchedResult = await pool.query(unmatchedQuery, unmatchedParams);
    const unmatchedMap = {};
    unmatchedResult.rows.forEach(r => { unmatchedMap[r.customer_id] = parseInt(r.unmatched_leads); });

    // Merge unmatched leads into per-client results
    perClientResult.rows.forEach(r => {
      r.leads = parseInt(r.leads) + (unmatchedMap[r.customer_id] || 0);
    });

    // Compute per-client conversion rates, then average them
    const clientFunnels = perClientResult.rows.filter(r => {
      const leads = parseInt(r.leads);
      const insp = parseInt(r.inspection_scheduled);
      return leads >= 5 && insp > 0 && (insp / leads) >= 0.05; // min 5 leads, >0 inspections, >=5% book rate
    });

    if (!clientFunnels.length) return res.json({ client_count: 0, stages: {}, kpis: {}, velocity: {} });

    // Conversion rates per client
    const rates = clientFunnels.map(r => {
      const leads = parseInt(r.leads);
      const insp_sched = parseInt(r.inspection_scheduled);
      const insp_comp = parseInt(r.inspection_completed);
      const est_sent = parseInt(r.estimate_sent);
      const est_appr = parseInt(r.estimate_approved);
      const job_sched = parseInt(r.job_scheduled);
      const job_comp = parseInt(r.job_completed);
      return {
        customer_id: r.customer_id,
        leads,
        lead_to_insp_sched: leads > 0 ? insp_sched / leads : null,
        insp_sched_to_comp: insp_sched > 0 ? insp_comp / insp_sched : null,
        insp_comp_to_est_sent: insp_comp > 0 ? est_sent / insp_comp : null,
        est_sent_to_approved: est_sent > 0 ? est_appr / est_sent : null,
        est_appr_to_job_sched: est_appr > 0 ? job_sched / est_appr : null,
        job_sched_to_comp: job_sched > 0 ? job_comp / job_sched : null,
        overall: leads > 0 ? job_comp / leads : null,
        revenue_per_lead: leads > 0 ? parseInt(r.roas_revenue_cents) / 100 / leads : 0,
      };
    });

    function avg(arr) {
      const valid = arr.filter(v => v !== null && v !== undefined);
      return valid.length ? valid.reduce((a, b) => a + b, 0) / valid.length : null;
    }

    const avgRates = {
      lead_to_insp_sched: avg(rates.map(r => r.lead_to_insp_sched)),
      insp_sched_to_comp: avg(rates.map(r => r.insp_sched_to_comp)),
      insp_comp_to_est_sent: avg(rates.map(r => r.insp_comp_to_est_sent)),
      est_sent_to_approved: avg(rates.map(r => r.est_sent_to_approved)),
      est_appr_to_job_sched: avg(rates.map(r => r.est_appr_to_job_sched)),
      job_sched_to_comp: avg(rates.map(r => r.job_sched_to_comp)),
      overall: avg(rates.map(r => r.overall)),
    };

    // Total counts (for display context)
    const totalLeads = clientFunnels.reduce((s, r) => s + parseInt(r.leads), 0);
    const totalInspSched = clientFunnels.reduce((s, r) => s + parseInt(r.inspection_scheduled), 0);
    const totalInspComp = clientFunnels.reduce((s, r) => s + parseInt(r.inspection_completed), 0);
    const totalEstSent = clientFunnels.reduce((s, r) => s + parseInt(r.estimate_sent), 0);
    const totalEstAppr = clientFunnels.reduce((s, r) => s + parseInt(r.estimate_approved), 0);
    const totalJobSched = clientFunnels.reduce((s, r) => s + parseInt(r.job_scheduled), 0);
    const totalJobComp = clientFunnels.reduce((s, r) => s + parseInt(r.job_completed), 0);
    const totalRevenue = clientFunnels.reduce((s, r) => s + parseInt(r.roas_revenue_cents), 0);

    // Avg revenue per lead
    const avgRevenuePerLead = avg(rates.map(r => r.revenue_per_lead));

    // Ad spend averages (for google_ads/lsa source)
    let avgAdSpend = 0, avgCPL = 0, avgROAS = 0, clientsWithSpend = 0;
    if (sourceForFilter === 'google_ads' || source === 'lsa' || source === 'all') {
      let spendDateWhere = '';
      const spendParams = [customerIds];
      let spIdx = 2;
      if (date_from) { spendDateWhere += ` AND date >= $${spIdx}`; spendParams.push(date_from); spIdx++; }
      if (date_to) { spendDateWhere += ` AND date <= $${spIdx}`; spendParams.push(date_to); spIdx++; }

      let spendQuery;
      if (sourceForFilter === 'google_ads') {
        spendQuery = `SELECT customer_id, COALESCE(SUM(cost), 0) as spend FROM campaign_daily_metrics WHERE customer_id = ANY($1::bigint[]) AND campaign_type IN ('SEARCH', 'PERFORMANCE_MAX')${spendDateWhere} GROUP BY customer_id`;
      } else if (source === 'lsa') {
        spendQuery = `SELECT customer_id, COALESCE(SUM(cost), 0) as spend FROM campaign_daily_metrics WHERE customer_id = ANY($1::bigint[]) AND campaign_type = 'LOCAL_SERVICES'${spendDateWhere} GROUP BY customer_id`;
      } else {
        spendQuery = `SELECT customer_id, COALESCE(SUM(cost), 0) as spend FROM account_daily_metrics WHERE customer_id = ANY($1::bigint[])${spendDateWhere} GROUP BY customer_id`;
      }

      const spendResult = await pool.query(spendQuery, spendParams);
      const spendMap = {};
      spendResult.rows.forEach(r => { spendMap[r.customer_id] = parseFloat(r.spend); });

      // Per-client CPL and ROAS
      const cpls = [];
      const roases = [];
      const spends = [];
      clientFunnels.forEach(cf => {
        const spend = spendMap[cf.customer_id] || 0;
        if (spend > 0) {
          const leads = parseInt(cf.leads);
          const rev = parseInt(cf.roas_revenue_cents) / 100;
          spends.push(spend);
          if (leads > 0) cpls.push(spend / leads);
          if (rev > 0) roases.push(rev / spend);
          clientsWithSpend++;
        }
      });
      avgAdSpend = spends.length ? spends.reduce((a, b) => a + b, 0) / spends.length : 0;
      avgCPL = cpls.length ? cpls.reduce((a, b) => a + b, 0) / cpls.length : 0;
      avgROAS = roases.length ? roases.reduce((a, b) => a + b, 0) / roases.length : 0;
    }

    // Pipeline velocity averages across all clients
    const velQuery = `
      SELECT
        ROUND(AVG(v.lead_to_insp)::numeric, 1) as avg_lead_to_insp,
        ROUND(AVG(v.insp_to_comp)::numeric, 1) as avg_insp_to_comp,
        ROUND(AVG(v.comp_to_est)::numeric, 1) as avg_comp_to_est,
        ROUND(AVG(v.est_to_appr)::numeric, 1) as avg_est_to_appr,
        ROUND(AVG(v.appr_to_job)::numeric, 1) as avg_appr_to_job,
        ROUND(AVG(v.lead_to_done)::numeric, 1) as avg_lead_to_done
      FROM (
        SELECT
          hc.customer_id,
          AVG(EXTRACT(EPOCH FROM (ins_d.first_created - hc.hcp_created_at))/86400) as lead_to_insp,
          AVG(EXTRACT(EPOCH FROM (ins_d.first_completed_date - ins_d.first_created))/86400) as insp_to_comp,
          AVG(EXTRACT(EPOCH FROM (eg_d.first_sent - ins_d.first_completed_date))/86400) as comp_to_est,
          AVG(EXTRACT(EPOCH FROM (eg_d.first_approved - eg_d.first_sent))/86400) as est_to_appr,
          AVG(EXTRACT(EPOCH FROM (j_d.first_scheduled - eg_d.first_approved))/86400) as appr_to_job,
          AVG(EXTRACT(EPOCH FROM (COALESCE(j_d.first_completed, j_d.first_scheduled) - hc.hcp_created_at))/86400) as lead_to_done
        FROM hcp_customers hc
        LEFT JOIN LATERAL (
          SELECT
            MIN(ins.hcp_created_at) as first_created,
            MIN(ins.scheduled_at) FILTER (WHERE ins.status IN ('complete rated','complete unrated') OR ins.inferred_complete = true) as first_completed_date
          FROM hcp_inspections ins WHERE ins.hcp_customer_id = hc.hcp_customer_id AND ins.record_status = 'active' AND ins.status NOT IN ('user canceled','pro canceled')
        ) ins_d ON true
        LEFT JOIN LATERAL (
          SELECT
            MIN(eg.sent_at) as first_sent,
            MIN(eg.approved_at) FILTER (WHERE eg.status = 'approved') as first_approved
          FROM v_estimate_groups eg WHERE eg.hcp_customer_id = hc.hcp_customer_id AND eg.status IN ('sent','approved','declined') AND eg.count_revenue
        ) eg_d ON true
        LEFT JOIN LATERAL (
          SELECT
            MIN(j.scheduled_at) as first_scheduled,
            MIN(COALESCE(j.completed_at, j.scheduled_at)) FILTER (WHERE j.status IN ('complete rated','complete unrated')) as first_completed
          FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status NOT IN ('user canceled','pro canceled')
            AND j.total_amount_cents > 100000
        ) j_d ON true
        WHERE hc.customer_id = ANY($${dIdx}::bigint[])
          ${hcDateWhere} ${hcSourceWhere} ${flagExclude} ${spamExclude}
        GROUP BY hc.customer_id
      ) v
    `;
    const velResult = await pool.query(velQuery, queryParams);
    const vel = velResult.rows[0] || {};

    // Per-client data for the table
    const clientDetails = clientFunnels.map(cf => {
      const cInfo = clients.find(c => String(c.customer_id) === String(cf.customer_id));
      const leads = parseInt(cf.leads);
      const insp = parseInt(cf.inspection_scheduled);
      const comp = parseInt(cf.inspection_completed);
      const sent = parseInt(cf.estimate_sent);
      const appr = parseInt(cf.estimate_approved);
      const done = parseInt(cf.job_completed);
      return {
        name: cInfo ? (cInfo.name.includes('|') ? cInfo.name.split('|').pop().trim() : cInfo.name) : cf.customer_id,
        leads,
        book_rate: leads > 0 ? (insp / leads * 100).toFixed(1) : null,
        completion_rate: insp > 0 ? (comp / insp * 100).toFixed(1) : null,
        close_rate: sent > 0 ? (appr / sent * 100).toFixed(1) : null,
        overall_rate: leads > 0 ? (done / leads * 100).toFixed(1) : null,
        revenue: Math.round(parseInt(cf.roas_revenue_cents) / 100),
      };
    });

    // Sort by leads descending
    clientDetails.sort((a, b) => b.leads - a.leads);

    res.json({
      client_count: clientFunnels.length,
      total_clients: clients.length,
      conversion_rates: {
        lead_to_insp_sched: avgRates.lead_to_insp_sched,
        insp_sched_to_comp: avgRates.insp_sched_to_comp,
        insp_comp_to_est_sent: avgRates.insp_comp_to_est_sent,
        est_sent_to_approved: avgRates.est_sent_to_approved,
        est_appr_to_job_sched: avgRates.est_appr_to_job_sched,
        job_sched_to_comp: avgRates.job_sched_to_comp,
        overall: avgRates.overall,
      },
      stages: {
        leads:                { count: totalLeads },
        inspection_scheduled: { count: totalInspSched },
        inspection_completed: { count: totalInspComp },
        estimate_sent:        { count: totalEstSent },
        estimate_approved:    { count: totalEstAppr },
        job_scheduled:        { count: totalJobSched },
        job_completed:        { count: totalJobComp },
      },
      kpis: {
        total_revenue: Math.round(totalRevenue / 100),
        avg_revenue_per_lead: Math.round(avgRevenuePerLead || 0),
        avg_ad_spend: Math.round(avgAdSpend),
        avg_cpl: Math.round(avgCPL),
        avg_roas: Math.round(avgROAS * 10) / 10,
        clients_with_spend: clientsWithSpend,
      },
      velocity: {
        lead_to_insp_scheduled: vel.avg_lead_to_insp ? parseFloat(vel.avg_lead_to_insp) : null,
        insp_scheduled_to_completed: vel.avg_insp_to_comp ? parseFloat(vel.avg_insp_to_comp) : null,
        insp_completed_to_est_sent: vel.avg_comp_to_est ? parseFloat(vel.avg_comp_to_est) : null,
        est_sent_to_approved: vel.avg_est_to_appr ? parseFloat(vel.avg_est_to_appr) : null,
        est_approved_to_job: vel.avg_appr_to_job ? parseFloat(vel.avg_appr_to_job) : null,
        lead_to_job_completed: vel.avg_lead_to_done ? parseFloat(vel.avg_lead_to_done) : null,
      },
      clients: clientDetails,
    });
  } catch (err) {
    console.error('Cohort funnel error:', err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/', (req, res) => {
  res.redirect('/review');
});

app.listen(port, () => {
  console.log(`Review app running at http://localhost:${port}/review`);
});
