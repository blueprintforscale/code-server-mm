#!/usr/bin/env node
/**
 * Client Intelligence Dashboard — Internal tool for account managers
 *
 * Provides:
 * - Client card grid with health indicators
 * - Detailed client view with timeline
 * - "Who needs attention?" priority list
 * - Search and filter
 *
 * Run: node server.js
 * Port: 3090 (configurable via PORT env var)
 */

const express = require('express');
const { Pool } = require('pg');

const app = express();
const PORT = process.env.PORT || 3090;

const pool = new Pool({
  host: 'localhost',
  port: 5432,
  user: 'blueprint',
  database: 'blueprint',
  max: 5,
  idleTimeoutMillis: 30000,
});

const path = require('path');
const fs = require('fs');

app.use(express.json());
app.use('/static', express.static(path.join(__dirname, 'static')));

// ── API Routes ──────────────────────────────────────────────

// All clients health overview (for the card grid)
app.get('/api/clients', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT
        ch.customer_id,
        ch.client_name,
        ch.client_state,
        ch.account_manager,
        ch.monthly_retainer,
        ch.client_tier,
        ch.onboarding_status,
        ch.program_month,
        ch.days_until_renewal,
        ch.last_interaction_date,
        ch.days_since_interaction,
        ch.open_tasks,
        ch.overdue_tasks,
        ch.open_alerts,
        ch.critical_alerts,
        ch.last_sentiment,
        ch.slack_channel_name,
        ch.client_goals,
        -- Lead metrics (current month)
        COALESCE(lm.leads_this_month, 0) as leads_this_month,
        COALESCE(lm.leads_last_month, 0) as leads_last_month,
        CASE WHEN COALESCE(lm.leads_last_month, 0) > 0
          THEN ROUND((lm.leads_this_month::numeric / lm.leads_last_month - 1) * 100)
          ELSE NULL END as lead_change_pct,
        -- ROAS metrics
        COALESCE(r.roas_revenue, 0) as roas_revenue,
        COALESCE(r.ad_spend, 0) as ad_spend,
        COALESCE(r.roas_ratio, 0) as roas_ratio,
        COALESCE(r.cost_per_lead, 0) as cost_per_lead
      FROM v_client_health ch
      LEFT JOIN LATERAL (
        SELECT
          COUNT(*) FILTER (WHERE start_time >= DATE_TRUNC('month', CURRENT_DATE)) as leads_this_month,
          COUNT(*) FILTER (WHERE start_time >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
                            AND start_time < DATE_TRUNC('month', CURRENT_DATE)) as leads_last_month
        FROM calls
        WHERE customer_id = ch.customer_id
          AND classified_status NOT IN ('spam', 'irrelevant', 'brand')
      ) lm ON TRUE
      LEFT JOIN v_hcp_roas r ON r.customer_id = ch.customer_id
      ORDER BY ch.client_name
    `);
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Clients needing attention
app.get('/api/attention', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT * FROM v_clients_needing_attention
      ORDER BY
        CASE attention_level
          WHEN 'critical' THEN 1
          WHEN 'high' THEN 2
          WHEN 'medium' THEN 3
          ELSE 4 END,
        client_name
    `);
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Single client detail
app.get('/api/clients/:customerId', async (req, res) => {
  const { customerId } = req.params;
  try {
    // Profile
    const { rows: [profile] } = await pool.query(`
      SELECT ch.*, cp.preferences, cp.notes as profile_notes, cp.client_goals
      FROM v_client_health ch
      LEFT JOIN client_profiles cp ON cp.customer_id = ch.customer_id
      WHERE ch.customer_id = $1
    `, [customerId]);

    // Recent interactions
    const { rows: interactions } = await pool.query(`
      SELECT interaction_type, interaction_date, logged_by, attendees,
             summary, action_items, sentiment, follow_up_date, source
      FROM client_interactions
      WHERE customer_id = $1
      ORDER BY interaction_date DESC
      LIMIT 10
    `, [customerId]);

    // Open tasks
    const { rows: tasks } = await pool.query(`
      SELECT title, status, assigned_to, due_date, task_type, priority
      FROM client_tasks
      WHERE customer_id = $1
        AND status NOT IN ('done', 'cancelled')
      ORDER BY
        CASE priority WHEN 'urgent' THEN 1 WHEN 'high' THEN 2 WHEN 'normal' THEN 3 ELSE 4 END,
        due_date NULLS LAST
    `, [customerId]);

    // Personal notes
    const { rows: personalNotes } = await pool.query(`
      SELECT note, category, captured_date, source
      FROM client_personal_notes
      WHERE customer_id = $1
      ORDER BY captured_date DESC
      LIMIT 20
    `, [customerId]);

    // Active alerts
    const { rows: alerts } = await pool.query(`
      SELECT alert_type, severity, message, created_at
      FROM client_alerts
      WHERE customer_id = $1 AND resolved_at IS NULL
      ORDER BY
        CASE severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END,
        created_at DESC
    `, [customerId]);

    // Contacts
    const { rows: contacts } = await pool.query(`
      SELECT name, role, phone, email, preferred_channel, is_primary
      FROM client_contacts
      WHERE customer_id = $1
      ORDER BY is_primary DESC, name
    `, [customerId]);

    // Lead metrics
    const { rows: [metrics] } = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE start_time >= DATE_TRUNC('month', CURRENT_DATE)) as leads_this_month,
        COUNT(*) FILTER (WHERE start_time >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
                          AND start_time < DATE_TRUNC('month', CURRENT_DATE)) as leads_last_month
      FROM calls
      WHERE customer_id = $1
        AND classified_status NOT IN ('spam', 'irrelevant', 'brand')
    `, [customerId]);

    // ROAS metrics
    const { rows: [roas] } = await pool.query(`
      SELECT roas_revenue, ad_spend, roas_ratio, cost_per_lead, total_leads, pipeline_revenue
      FROM v_hcp_roas
      WHERE customer_id = $1
    `, [customerId]);

    res.json({
      profile,
      interactions,
      tasks,
      personalNotes,
      alerts,
      contacts,
      metrics,
      roas: roas || null,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Recent Slack messages for a client
app.get('/api/clients/:customerId/slack', async (req, res) => {
  const { customerId } = req.params;
  const limit = parseInt(req.query.limit) || 50;
  try {
    const { rows } = await pool.query(`
      SELECT user_name, message_text, posted_at, thread_ts IS NOT NULL as is_thread_reply
      FROM slack_messages
      WHERE customer_id = $1
        AND message_text IS NOT NULL
        AND message_text != ''
      ORDER BY posted_at DESC
      LIMIT $2
    `, [customerId, limit]);
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Prep Dashboard API Routes ───────────────────────────────

// Today's and this week's calendar events matched to clients
app.get('/api/prep/calendar', async (req, res) => {
  const daysAhead = parseInt(req.query.days) || 7;
  try {
    const { rows } = await pool.query(`
      SELECT DISTINCT ON (ci.customer_id, ci.interaction_date::date, ci.summary)
        ci.customer_id,
        c.name as client_name,
        ci.interaction_date,
        ci.summary,
        ci.attendees,
        ci.logged_by,
        ci.source_id,
        cp.account_manager
      FROM client_interactions ci
      JOIN clients c ON c.customer_id = ci.customer_id
      LEFT JOIN client_profiles cp ON cp.customer_id = ci.customer_id
      WHERE ci.source = 'calendar'
        AND ci.interaction_date >= CURRENT_DATE
        AND ci.interaction_date < CURRENT_DATE + ($1 || ' days')::interval
      ORDER BY ci.customer_id, ci.interaction_date::date, ci.summary, ci.interaction_date ASC
    `, [daysAhead]);
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Full client prep data (all sources combined)
app.get('/api/prep/clients/:customerId', async (req, res) => {
  const { customerId } = req.params;
  try {
    // Profile
    const { rows: [profile] } = await pool.query(`
      SELECT ch.*, cp.preferences, cp.notes as profile_notes, cp.client_goals, cp.client_bio
      FROM v_client_health ch
      LEFT JOIN client_profiles cp ON cp.customer_id = ch.customer_id
      WHERE ch.customer_id = $1
    `, [customerId]);

    // Recent interactions — prioritize meetings/calls, limit emails
    const { rows: interactions } = await pool.query(`
      (SELECT interaction_type, interaction_date, logged_by, attendees,
             summary, action_items, sentiment, source, source_id
       FROM client_interactions
       WHERE customer_id = $1 AND source IN ('calendar', 'fireflies', 'slack')
       ORDER BY interaction_date DESC LIMIT 10)
      UNION ALL
      (SELECT interaction_type, interaction_date, logged_by, attendees,
             summary, action_items, sentiment, source, source_id
       FROM client_interactions
       WHERE customer_id = $1 AND source = 'gmail'
         AND logged_by NOT IN ('Info', 'Susie', 'Martin', 'Josh', 'Kiana')
       ORDER BY interaction_date DESC LIMIT 5)
      ORDER BY interaction_date DESC
    `, [customerId]);

    // GHL messages — calls with transcripts first, then recent SMS
    const { rows: messages } = await pool.query(`
      (SELECT channel, direction, contact_name, message_body, duration,
              message_date, source_id
       FROM crm_messages
       WHERE customer_id = $1 AND source = 'ghl' AND channel = 'call'
         AND message_body IS NOT NULL AND message_body != ''
       ORDER BY message_date DESC LIMIT 10)
      UNION ALL
      (SELECT channel, direction, contact_name, message_body, duration,
              message_date, source_id
       FROM crm_messages
       WHERE customer_id = $1 AND source = 'ghl' AND channel = 'sms'
         AND message_body IS NOT NULL AND message_body != ''
       ORDER BY message_date DESC LIMIT 10)
      ORDER BY message_date DESC
    `, [customerId]);

    // Open tasks
    const { rows: tasks } = await pool.query(`
      SELECT title, status, assigned_to, due_date, task_type, priority
      FROM client_tasks
      WHERE customer_id = $1
        AND status NOT IN ('done', 'cancelled')
      ORDER BY
        CASE priority WHEN 'urgent' THEN 1 WHEN 'high' THEN 2 WHEN 'normal' THEN 3 ELSE 4 END,
        due_date NULLS LAST
    `, [customerId]);

    // Personal notes
    const { rows: personalNotes } = await pool.query(`
      SELECT note, category, captured_date, source
      FROM client_personal_notes
      WHERE customer_id = $1
      ORDER BY captured_date DESC
      LIMIT 30
    `, [customerId]);

    // Active alerts
    const { rows: alerts } = await pool.query(`
      SELECT alert_type, severity, message, created_at
      FROM client_alerts
      WHERE customer_id = $1 AND resolved_at IS NULL
      ORDER BY
        CASE severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END,
        created_at DESC
    `, [customerId]);

    // Contacts
    const { rows: contacts } = await pool.query(`
      SELECT name, role, phone, email, preferred_channel, is_primary
      FROM client_contacts
      WHERE customer_id = $1
      ORDER BY is_primary DESC, name
    `, [customerId]);

    // Lead metrics
    const { rows: [metrics] } = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE start_time >= DATE_TRUNC('month', CURRENT_DATE)) as leads_this_month,
        COUNT(*) FILTER (WHERE start_time >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
                          AND start_time < DATE_TRUNC('month', CURRENT_DATE)) as leads_last_month
      FROM calls
      WHERE customer_id = $1
        AND classified_status NOT IN ('spam', 'irrelevant', 'brand')
    `, [customerId]);

    // ROAS metrics
    const { rows: [roas] } = await pool.query(`
      SELECT roas_revenue, ad_spend, roas_ratio, cost_per_lead, total_leads, pipeline_revenue
      FROM v_hcp_roas
      WHERE customer_id = $1
    `, [customerId]);

    // Location targeting
    const { rows: locations } = await pool.query(`
      SELECT DISTINCT location_name, target_type, canonical_name, is_negative
      FROM ads_location_targets
      WHERE customer_id = $1 AND is_negative = false
      ORDER BY target_type, location_name
    `, [customerId]);

    // Days since last call
    const { rows: [lastCall] } = await pool.query(`
      SELECT MAX(message_date)::date as last_call_date,
             CURRENT_DATE - MAX(message_date)::date as days_since_call
      FROM crm_messages
      WHERE customer_id = $1 AND source = 'ghl' AND channel = 'call'
    `, [customerId]);

    // Upcoming meetings
    const { rows: upcoming } = await pool.query(`
      SELECT interaction_date, summary, attendees, logged_by
      FROM client_interactions
      WHERE customer_id = $1 AND source = 'calendar'
        AND interaction_date >= NOW()
      ORDER BY interaction_date ASC
      LIMIT 5
    `, [customerId]);

    res.json({
      profile,
      interactions,
      messages,
      tasks,
      personalNotes,
      alerts,
      contacts,
      metrics,
      roas: roas || null,
      locations,
      lastCall: lastCall || null,
      upcoming,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// AI prep summary endpoint
app.post('/api/prep/summary/:customerId', async (req, res) => {
  const { customerId } = req.params;
  const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
  if (!ANTHROPIC_KEY) return res.status(500).json({ error: 'ANTHROPIC_API_KEY not set' });

  try {
    // Profile + program info
    const { rows: [profile] } = await pool.query(`
      SELECT c.name, c.start_date, cp.account_manager, cp.client_goals,
             cp.preferences, cp.notes as profile_notes, cp.onboarding_status,
             cp.client_tier, cp.monthly_retainer,
             CASE WHEN c.start_date IS NOT NULL
               THEN EXTRACT(MONTH FROM age(CURRENT_DATE, c.start_date))::INTEGER + 1
               ELSE NULL END AS program_month
      FROM clients c LEFT JOIN client_profiles cp ON cp.customer_id = c.customer_id
      WHERE c.customer_id = $1
    `, [customerId]);

    // Last call date to scope "since last call"
    const { rows: [lastCallRow] } = await pool.query(`
      SELECT MAX(message_date) as last_call
      FROM crm_messages
      WHERE customer_id = $1 AND source = 'ghl' AND channel = 'call'
    `, [customerId]);
    const sinceDate = lastCallRow?.last_call || new Date(Date.now() - 30*86400000).toISOString();

    // Call transcripts (most valuable — actual conversations)
    const { rows: transcripts } = await pool.query(`
      SELECT direction, LEFT(message_body, 800) as body, message_date::date as date, duration
      FROM crm_messages
      WHERE customer_id = $1 AND source = 'ghl' AND channel = 'call'
        AND message_body IS NOT NULL AND LENGTH(message_body) > 20
      ORDER BY message_date DESC LIMIT 3
    `, [customerId]);

    // Key interactions since last call (meetings, emails — not every one)
    const { rows: interactions } = await pool.query(`
      SELECT interaction_type, interaction_date::date as date, summary, action_items, sentiment, source
      FROM client_interactions
      WHERE customer_id = $1 AND interaction_date >= $2
      ORDER BY interaction_date DESC LIMIT 8
    `, [customerId, sinceDate]);

    // Recent SMS (short, just last few)
    const { rows: recentSms } = await pool.query(`
      SELECT direction, LEFT(message_body, 150) as body, message_date::date as date
      FROM crm_messages
      WHERE customer_id = $1 AND source = 'ghl' AND channel = 'sms'
        AND message_body IS NOT NULL AND message_body != ''
      ORDER BY message_date DESC LIMIT 5
    `, [customerId]);

    // Personal notes
    const { rows: notes } = await pool.query(`
      SELECT note, category FROM client_personal_notes
      WHERE customer_id = $1 ORDER BY captured_date DESC LIMIT 8
    `, [customerId]);

    // Open tasks
    const { rows: tasks } = await pool.query(`
      SELECT title, status, assigned_to, due_date FROM client_tasks
      WHERE customer_id = $1 AND status NOT IN ('done', 'cancelled')
    `, [customerId]);

    // Performance snapshot
    const { rows: [perf] } = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE start_time >= DATE_TRUNC('month', CURRENT_DATE)) as leads_this_month,
        COUNT(*) FILTER (WHERE start_time >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
                          AND start_time < DATE_TRUNC('month', CURRENT_DATE)) as leads_last_month
      FROM calls
      WHERE customer_id = $1
        AND classified_status NOT IN ('spam', 'irrelevant', 'brand')
    `, [customerId]);

    // Budget/spend trend (last 4 months for budget change detection)
    const { rows: spendTrend } = await pool.query(`
      SELECT DATE_TRUNC('month', date)::date as month, SUM(cost)::numeric(10,2) as spend
      FROM account_daily_metrics
      WHERE customer_id = $1 AND date >= CURRENT_DATE - INTERVAL '4 months'
      GROUP BY DATE_TRUNC('month', date)
      ORDER BY month DESC
    `, [customerId]);

    // Open estimates aging
    const { rows: openEst } = await pool.query(`
      SELECT customer_name, amount, days_waiting
      FROM v_open_estimates WHERE customer_id = $1
      ORDER BY days_waiting DESC LIMIT 5
    `, [customerId]);

    const context = JSON.stringify({
      profile, transcripts, interactions, recentSms, notes, tasks, perf,
      spendTrend, openEst, last_call_date: sinceDate
    }, null, 1);

    const prompt = `You are a call prep assistant for Blueprint for Scale, a Google Ads agency managing mold remediation clients.

Generate a concise prep summary for the account manager before their call with this client.

Format EXACTLY like this:

**QUICK CONTEXT**
One sentence: who they are, what program month, their goals.

**LAST CALL RECAP** (${lastCallRow?.last_call ? new Date(lastCallRow.last_call).toLocaleDateString('en-US') : 'unknown'})
What was discussed last time? What was promised? What should we pick up where we left off? This is the #1 most important section.

**ACTIVE ISSUES**
Things the client is currently working through that the AM should ask about:
- GBP verification, service area changes, campaign launches, phone porting
- New hires, equipment purchases, CRM switches, website changes
- Budget changes — when was the last increase? How did it affect leads?
Format as: "[Issue] — [current status if known]"

**TALKING POINTS**
3-5 suggested topics based on what's changed since last call:
- Performance changes worth discussing (celebrate wins or address drops)
- Aging estimates to ask about
- Overdue tasks to mention
- Upcoming milestones or renewals

**ACTION ITEMS**
Open tasks, promises made, things to follow up on. Include due dates.

**PERSONAL TOUCH**
One personal detail to mention that shows you remember and care.

**WATCH OUT**
Any concerns: performance drops, sentiment shifts, upcoming renewal, unresolved complaints.

Rules:
- Be specific with names, dates, and numbers
- Reference actual transcript content when available
- LAST CALL RECAP is the most important section — what did we talk about and promise?
- ACTIVE ISSUES should surface things the client is dealing with, not just our tasks
- Keep each section to 2-4 lines max
- Skip any section that has no relevant data
- Don't list every email — summarize themes

Client data:
${context.substring(0, 8000)}`;

    const https = require('https');
    const apiBody = JSON.stringify({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 800,
      messages: [{ role: 'user', content: prompt }],
    });

    const apiReq = https.request({
      hostname: 'api.anthropic.com',
      path: '/v1/messages',
      method: 'POST',
      headers: {
        'x-api-key': ANTHROPIC_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(apiBody),
      },
    }, (apiRes) => {
      let data = '';
      apiRes.on('data', chunk => data += chunk);
      apiRes.on('end', () => {
        try {
          const result = JSON.parse(data);
          const text = result.content?.[0]?.text || 'Unable to generate summary';
          res.json({ summary: text });
        } catch (e) {
          res.status(500).json({ error: 'Failed to parse AI response' });
        }
      });
    });
    apiReq.on('error', e => res.status(500).json({ error: e.message }));
    apiReq.write(apiBody);
    apiReq.end();
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// AI communications summary — digest recent calls, texts, emails
app.post('/api/prep/comms/:customerId', async (req, res) => {
  const { customerId } = req.params;
  const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
  if (!ANTHROPIC_KEY) return res.status(500).json({ error: 'ANTHROPIC_API_KEY not set' });

  try {
    // Recent GHL calls with transcripts
    const { rows: calls } = await pool.query(`
      SELECT direction, LEFT(message_body, 500) as body, message_date::date as date, duration, contact_name
      FROM crm_messages
      WHERE customer_id = $1 AND source = 'ghl' AND channel = 'call'
        AND message_body IS NOT NULL AND LENGTH(message_body) > 20
      ORDER BY message_date DESC LIMIT 5
    `, [customerId]);

    // Recent SMS threads
    const { rows: sms } = await pool.query(`
      SELECT direction, LEFT(message_body, 200) as body, message_date::date as date, contact_name
      FROM crm_messages
      WHERE customer_id = $1 AND source = 'ghl' AND channel = 'sms'
        AND message_body IS NOT NULL AND message_body != ''
      ORDER BY message_date DESC LIMIT 15
    `, [customerId]);

    // Recent email interactions
    const { rows: emails } = await pool.query(`
      SELECT summary, interaction_date::date as date, logged_by, sentiment
      FROM client_interactions
      WHERE customer_id = $1 AND source = 'gmail'
      ORDER BY interaction_date DESC LIMIT 5
    `, [customerId]);

    // Recent meetings
    const { rows: meetings } = await pool.query(`
      SELECT summary, interaction_date::date as date, source
      FROM client_interactions
      WHERE customer_id = $1 AND source IN ('calendar', 'fireflies')
      ORDER BY interaction_date DESC LIMIT 5
    `, [customerId]);

    const context = JSON.stringify({ calls, sms, emails, meetings }, null, 1);

    const prompt = `List the TOPICS discussed in recent communications with this client. Just the substantive topics — not logistics.

Group topics under headers. Format:

**Strategy & Growth**
• [topic] — [brief context]

**Active Issues**
• [topic] — [brief context]

**Routine Edits**
• Website, CRM email, and copy changes — [list them briefly in one bullet]

Use these headers as needed (skip any with no topics):
- **Strategy & Growth** — budget changes, new campaigns, market expansion, lead gen strategy, PPL services
- **Active Issues** — things in progress: GBP verification, phone porting, new hires, equipment, CRM setup
- **Performance** — ROAS changes, lead volume shifts, conversion rate discussions
- **Client Operations** — hiring, employee changes, service area changes, business decisions
- **Routine Edits** — group ALL website tweaks, copy edits, photo swaps, typo fixes, CRM email template changes, and minor adjustments into ONE bullet under this header

Examples of what TO include:
• Budget increase discussion — considering going from $6k to $8k
• GBP verification rejected — resubmitting with new photos
• New employee hired — training on phone scripts
• Website rebrand — new logo and landing pages in progress
• ROAS dropped below 2x — reviewing keyword strategy

Examples of what to SKIP:
• Meeting scheduling, calendar invites, confirming times
• "Sounds good", "thanks", small talk
• Internal team coordination
• Automated notifications

Rules:
- Maximum 8 bullets total across all headers
- Most important/recent first within each group
- One line per topic, no sub-bullets, no emojis
- Extract topics from call transcripts, texts, and emails
- Focus on business decisions, changes, issues, and wins
- Start immediately with the first header — NO intro sentence
- Keep context under 10 words
- Plain text bullets only: • [topic] — [context]

Communications data:
${context.substring(0, 6000)}`;

    const https = require('https');
    const apiBody = JSON.stringify({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 600,
      messages: [{ role: 'user', content: prompt }],
    });

    const apiReq = https.request({
      hostname: 'api.anthropic.com',
      path: '/v1/messages',
      method: 'POST',
      headers: {
        'x-api-key': ANTHROPIC_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(apiBody),
      },
    }, (apiRes) => {
      let data = '';
      apiRes.on('data', chunk => data += chunk);
      apiRes.on('end', () => {
        try {
          const result = JSON.parse(data);
          res.json({ summary: result.content?.[0]?.text || 'No recent communications' });
        } catch (e) {
          res.status(500).json({ error: 'Failed to parse' });
        }
      });
    });
    apiReq.on('error', e => res.status(500).json({ error: e.message }));
    apiReq.write(apiBody);
    apiReq.end();
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Save call notes
app.post('/api/prep/notes/:customerId', async (req, res) => {
  const { customerId } = req.params;
  const { note, category } = req.body;
  try {
    await pool.query(`
      INSERT INTO client_personal_notes (customer_id, note, category, source, captured_by, auto_extracted)
      VALUES ($1, $2, $3, 'manual', 'account_manager', FALSE)
    `, [customerId, note, category || 'personal']);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Map data — client territory + neighboring clients (with pre-geocoded lat/lng)
app.get('/api/prep/map/:customerId', async (req, res) => {
  const { customerId } = req.params;
  try {
    // This client's locations — include lat/lng for instant map rendering
    const { rows: myLocations } = await pool.query(`
      SELECT DISTINCT ON (location_name)
        location_name, target_type, canonical_name, location_id,
        latitude::float as lat, longitude::float as lng
      FROM ads_location_targets
      WHERE customer_id = $1 AND is_negative = false
      ORDER BY location_name, pulled_at DESC
    `, [customerId]);

    // Get the state(s) this client operates in
    const states = [...new Set(myLocations
      .map(l => (l.canonical_name || '').split(',')[1]?.trim())
      .filter(Boolean))];

    // Find neighboring clients (other Blueprint clients in the same state(s))
    let neighbors = [];
    if (states.length > 0) {
      const { rows } = await pool.query(`
        SELECT DISTINCT ON (alt.customer_id, alt.location_name)
          alt.customer_id, c.name as client_name,
          alt.location_name, alt.target_type, alt.canonical_name,
          alt.latitude::float as lat, alt.longitude::float as lng
        FROM ads_location_targets alt
        JOIN clients c ON c.customer_id = alt.customer_id
        WHERE alt.customer_id != $1
          AND alt.is_negative = false
          AND c.status = 'active'
          AND alt.canonical_name ILIKE ANY($2)
        ORDER BY alt.customer_id, alt.location_name, alt.pulled_at DESC
      `, [customerId, states.map(s => '%,' + s + ',%')]);
      neighbors = rows;
    }

    // Count how many locations still need geocoding
    const needsGeo = myLocations.filter(l => !l.lat || !l.lng).length;

    res.json({ locations: myLocations, neighbors, states, needsGeo });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get/save client bio
app.get('/api/prep/bio/:customerId', async (req, res) => {
  const { customerId } = req.params;
  try {
    const { rows: [row] } = await pool.query(
      'SELECT client_bio FROM client_profiles WHERE customer_id = $1', [customerId]
    );
    res.json({ bio: row?.client_bio || '' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// AI-generate a client bio from all available data
app.post('/api/prep/bio/:customerId/generate', async (req, res) => {
  const { customerId } = req.params;
  const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
  if (!ANTHROPIC_KEY) return res.status(500).json({ error: 'ANTHROPIC_API_KEY not set' });

  try {
    // Gather everything we know
    const { rows: [profile] } = await pool.query(`
      SELECT c.name, c.start_date, cp.account_manager, cp.client_goals,
             cp.preferences, cp.notes as profile_notes, cp.client_bio,
             cp.onboarding_status, cp.client_tier, cp.monthly_retainer
      FROM clients c LEFT JOIN client_profiles cp ON cp.customer_id = c.customer_id
      WHERE c.customer_id = $1
    `, [customerId]);

    const { rows: contacts } = await pool.query(`
      SELECT name, role, phone, email FROM client_contacts
      WHERE customer_id = $1 ORDER BY is_primary DESC
    `, [customerId]);

    const { rows: notes } = await pool.query(`
      SELECT note, category FROM client_personal_notes
      WHERE customer_id = $1 ORDER BY captured_date DESC LIMIT 20
    `, [customerId]);

    const { rows: transcripts } = await pool.query(`
      SELECT LEFT(message_body, 600) as body, message_date::date as date
      FROM crm_messages
      WHERE customer_id = $1 AND source = 'ghl' AND channel = 'call'
        AND message_body IS NOT NULL AND LENGTH(message_body) > 50
      ORDER BY message_date DESC LIMIT 5
    `, [customerId]);

    const { rows: interactions } = await pool.query(`
      SELECT summary, action_items, sentiment, source, interaction_date::date as date
      FROM client_interactions
      WHERE customer_id = $1
      ORDER BY interaction_date DESC LIMIT 10
    `, [customerId]);

    const { rows: locations } = await pool.query(`
      SELECT DISTINCT location_name, target_type FROM ads_location_targets
      WHERE customer_id = $1 AND is_negative = false
      ORDER BY target_type, location_name LIMIT 20
    `, [customerId]);

    const context = JSON.stringify({ profile, contacts, notes, transcripts, interactions, locations }, null, 1);

    const existingBio = profile?.client_bio || '';

    const prompt = `You are writing a client bio for an account manager at Blueprint for Scale, a Google Ads agency for mold remediation companies.

${existingBio ? 'The client already has a bio. Update it with any new information. Keep existing bullet points that are still accurate. Add new ones.\n\nExisting bio:\n' + existingBio + '\n\n' : ''}Write a concise bullet-point bio. This is a quick-reference card the account manager glances at before every call.

Format EXACTLY like this (use bullet points, not paragraphs):

**Personal**
• Lives in [city, state]
• Background: [what they did before mold — prior career, industry, how long in home services]
• Family: [spouse/partner name, kids + names if known]
• Communication style: [direct, chatty, detail-oriented, hands-off, etc.]
• [Any hobbies, interests, life events mentioned in calls]

**Team**
• [First name] — [role], [one key detail]
• Phone answering: [who answers? them, office manager, Currie Miller, voicemail?]
• Uses Currie Miller (call center)? [yes/no if known]
• [Dispatcher, office manager, sales person — names if known]

**Business**
• [Company name], [city/state], [service area summary]
• [Pure Maintenance franchise/licensee or independent?]
• [CRM/phone system — HCP, Jobber, OpenPhone, etc.]
• [Any unique details — size, specialty, expansion plans]

**Before Blueprint**
• [What they did for lead gen before us — agency? PPL? Angie's? Thumbtack? Nothing?]
• [How they found Blueprint]
• [What wasn't working before]

**Goals**
• [Their stated business goals]
• [What success looks like to them]
• [Current challenges they're focused on]

**Program**
• Started [date], month [X], [phase]
• Budget: [amount], ROAS: [X]x

**Preferences**
• [Communication preference]
• [Scheduling preference]
• [Terminology or sensitivity notes]

Rules:
- Maximum 3-4 bullets per section
- Be specific — names, numbers, dates
- Skip any section with no data
- No sentences longer than 10 words per bullet
- Actively look for: family details, prior career, lead gen history, team structure, Currie Miller usage
- Extract personality and personal details from call transcripts

Client data:
${context.substring(0, 8000)}`;

    const https = require('https');
    const apiBody = JSON.stringify({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1000,
      messages: [{ role: 'user', content: prompt }],
    });

    const apiReq = https.request({
      hostname: 'api.anthropic.com',
      path: '/v1/messages',
      method: 'POST',
      headers: {
        'x-api-key': ANTHROPIC_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(apiBody),
      },
    }, (apiRes) => {
      let data = '';
      apiRes.on('data', chunk => data += chunk);
      apiRes.on('end', async () => {
        try {
          const result = JSON.parse(data);
          const text = result.content?.[0]?.text || 'Unable to generate bio';
          // Auto-save to DB so it's cached
          await pool.query(
            'UPDATE client_profiles SET client_bio = $1, updated_at = NOW() WHERE customer_id = $2',
            [text, customerId]
          );
          res.json({ bio: text });
        } catch (e) {
          res.status(500).json({ error: 'Failed to parse AI response' });
        }
      });
    });
    apiReq.on('error', e => res.status(500).json({ error: e.message }));
    apiReq.write(apiBody);
    apiReq.end();
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/prep/bio/:customerId', async (req, res) => {
  const { customerId } = req.params;
  const { bio } = req.body;
  try {
    await pool.query(`
      UPDATE client_profiles SET client_bio = $1, updated_at = NOW()
      WHERE customer_id = $2
    `, [bio, customerId]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Prep dashboard HTML routes
app.get('/prep', (req, res) => {
  res.sendFile(path.join(__dirname, 'static', 'prep.html'));
});

app.get('/prep/:customerId', (req, res) => {
  res.sendFile(path.join(__dirname, 'static', 'prep-client.html'));
});

// ── Dashboard HTML ──────────────────────────────────────────

app.get('/', (req, res) => {
  res.send(DASHBOARD_HTML);
});

app.get('/client/:customerId', (req, res) => {
  res.send(CLIENT_DETAIL_HTML);
});

const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Client Intelligence | Blueprint for Scale</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #F5F1E8; color: #1a1a1a; }

  .header { background: #000; color: #F5F1E8; padding: 20px 32px; display: flex; justify-content: space-between; align-items: center; }
  .header h1 { font-size: 20px; font-weight: 600; letter-spacing: -0.3px; }
  .header .stats { font-size: 13px; opacity: 0.7; }

  .toolbar { padding: 16px 32px; display: flex; gap: 12px; align-items: center; border-bottom: 1px solid #ddd; background: #fff; }
  .toolbar input { padding: 8px 14px; border: 1px solid #ccc; border-radius: 6px; font-size: 14px; width: 300px; }
  .toolbar select { padding: 8px 14px; border: 1px solid #ccc; border-radius: 6px; font-size: 14px; background: #fff; }
  .toolbar .filter-btn { padding: 8px 16px; border: 1px solid #E85D4D; background: transparent; color: #E85D4D; border-radius: 6px; cursor: pointer; font-size: 13px; font-weight: 500; }
  .toolbar .filter-btn.active { background: #E85D4D; color: #fff; }

  .grid { padding: 24px 32px; display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 16px; }

  .card { background: #fff; border-radius: 10px; padding: 20px; border: 1px solid #e0dcd4; cursor: pointer; transition: all 0.15s; position: relative; }
  .card:hover { border-color: #000; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
  .card.has-critical { border-left: 4px solid #dc3545; }
  .card.has-warning { border-left: 4px solid #ffc107; }

  .card-header { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 12px; }
  .card-name { font-size: 15px; font-weight: 600; line-height: 1.3; }
  .card-meta { font-size: 12px; color: #666; margin-top: 2px; }
  .card-badge { font-size: 11px; padding: 2px 8px; border-radius: 10px; font-weight: 500; white-space: nowrap; }
  .badge-green { background: #d4edda; color: #155724; }
  .badge-yellow { background: #fff3cd; color: #856404; }
  .badge-red { background: #f8d7da; color: #721c24; }
  .badge-gray { background: #e9ecef; color: #495057; }

  .card-metrics { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 8px; margin-bottom: 12px; }
  .metric { text-align: center; }
  .metric-value { font-size: 18px; font-weight: 700; }
  .metric-label { font-size: 10px; color: #888; text-transform: uppercase; letter-spacing: 0.5px; }
  .metric-change { font-size: 11px; }
  .change-up { color: #28a745; }
  .change-down { color: #dc3545; }

  .card-footer { display: flex; gap: 8px; flex-wrap: wrap; }
  .card-tag { font-size: 11px; padding: 2px 8px; background: #f0ede5; border-radius: 4px; color: #555; }
  .card-tag.alert { background: #fff3cd; color: #856404; }
  .card-tag.critical { background: #f8d7da; color: #721c24; }

  .attention-section { padding: 0 32px 24px; }
  .attention-title { font-size: 16px; font-weight: 600; margin-bottom: 12px; padding-top: 20px; }
  .attention-list { background: #fff; border-radius: 10px; border: 1px solid #e0dcd4; overflow: hidden; }
  .attention-row { padding: 12px 16px; display: flex; align-items: center; gap: 12px; border-bottom: 1px solid #f0f0f0; cursor: pointer; }
  .attention-row:hover { background: #f9f8f5; }
  .attention-row:last-child { border-bottom: none; }
  .attention-dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
  .dot-critical { background: #dc3545; }
  .dot-high { background: #fd7e14; }
  .dot-medium { background: #ffc107; }
  .dot-low { background: #6c757d; }
  .attention-name { font-weight: 600; font-size: 14px; min-width: 200px; }
  .attention-reasons { font-size: 13px; color: #666; }
</style>
</head>
<body>
<div class="header">
  <h1>Client Intelligence</h1>
  <div class="stats" id="header-stats"></div>
</div>
<div class="toolbar">
  <input type="text" id="search" placeholder="Search clients..." oninput="filterCards()">
  <select id="filter-manager" onchange="filterCards()">
    <option value="">All managers</option>
  </select>
  <button class="filter-btn" onclick="toggleAttention(this)" id="attention-btn">Needs Attention</button>
</div>
<div class="attention-section" id="attention-section" style="display:none">
  <div class="attention-title">Clients Needing Attention</div>
  <div class="attention-list" id="attention-list"></div>
</div>
<div class="grid" id="client-grid"></div>

<script>
let allClients = [];
let showAttentionOnly = false;

async function loadClients() {
  const resp = await fetch('/api/clients');
  allClients = await resp.json();

  // Populate manager filter
  const managers = [...new Set(allClients.map(c => c.account_manager).filter(Boolean))].sort();
  const sel = document.getElementById('filter-manager');
  managers.forEach(m => {
    const opt = document.createElement('option');
    opt.value = m; opt.textContent = m;
    sel.appendChild(opt);
  });

  document.getElementById('header-stats').textContent =
    allClients.length + ' clients | ' +
    allClients.reduce((s,c) => s + (c.open_alerts || 0), 0) + ' alerts | ' +
    allClients.reduce((s,c) => s + (c.overdue_tasks || 0), 0) + ' overdue tasks';

  renderCards(allClients);
  loadAttention();
}

function renderCards(clients) {
  const grid = document.getElementById('client-grid');
  grid.innerHTML = clients.map(c => {
    const changeClass = (c.lead_change_pct || 0) >= 0 ? 'change-up' : 'change-down';
    const changeText = c.lead_change_pct !== null ? ((c.lead_change_pct >= 0 ? '+' : '') + c.lead_change_pct + '%') : '-';
    const cardClass = c.critical_alerts > 0 ? 'has-critical' : c.open_alerts > 0 ? 'has-warning' : '';

    let sentimentBadge = '';
    if (c.last_sentiment === 'positive') sentimentBadge = '<span class="card-badge badge-green">Positive</span>';
    else if (c.last_sentiment === 'at_risk') sentimentBadge = '<span class="card-badge badge-red">At Risk</span>';
    else if (c.last_sentiment === 'negative') sentimentBadge = '<span class="card-badge badge-yellow">Negative</span>';

    let tags = [];
    if (c.overdue_tasks > 0) tags.push('<span class="card-tag alert">' + c.overdue_tasks + ' overdue</span>');
    if (c.critical_alerts > 0) tags.push('<span class="card-tag critical">' + c.critical_alerts + ' critical</span>');
    else if (c.open_alerts > 0) tags.push('<span class="card-tag alert">' + c.open_alerts + ' alert(s)</span>');
    if (c.days_until_renewal && c.days_until_renewal <= 30) tags.push('<span class="card-tag alert">Renews ' + c.days_until_renewal + 'd</span>');
    if (c.account_manager) tags.push('<span class="card-tag">' + c.account_manager + '</span>');

    const retainer = c.monthly_retainer ? '$' + Number(c.monthly_retainer).toLocaleString() : '-';
    const state = c.client_state || '';
    const month = c.program_month ? 'Mo ' + c.program_month : '';

    return '<div class="card ' + cardClass + '" onclick="location.href=\\'/client/' + c.customer_id + '\\'">' +
      '<div class="card-header">' +
        '<div><div class="card-name">' + c.client_name + '</div>' +
        '<div class="card-meta">' + [state, month, retainer].filter(Boolean).join(' · ') + '</div></div>' +
        sentimentBadge +
      '</div>' +
      '<div class="card-metrics">' +
        '<div class="metric"><div class="metric-value">' + (c.leads_this_month || 0) + '</div><div class="metric-label">Leads</div><div class="metric-change ' + changeClass + '">' + changeText + '</div></div>' +
        '<div class="metric"><div class="metric-value">' + (c.roas_ratio > 0 ? parseFloat(c.roas_ratio).toFixed(1) + 'x' : '-') + '</div><div class="metric-label">ROAS</div></div>' +
        '<div class="metric"><div class="metric-value">' + (c.open_tasks || 0) + '</div><div class="metric-label">Tasks</div><div class="stat-sub">' + (c.overdue_tasks > 0 ? c.overdue_tasks + ' overdue' : '') + '</div></div>' +
      '</div>' +
      '<div class="card-footer">' + tags.join('') + '</div>' +
    '</div>';
  }).join('');
}

function filterCards() {
  const q = document.getElementById('search').value.toLowerCase();
  const mgr = document.getElementById('filter-manager').value;
  let filtered = allClients;
  if (q) filtered = filtered.filter(c => c.client_name.toLowerCase().includes(q));
  if (mgr) filtered = filtered.filter(c => c.account_manager === mgr);
  if (showAttentionOnly) filtered = filtered.filter(c => c.open_alerts > 0 || c.overdue_tasks > 0);
  renderCards(filtered);
}

function toggleAttention(btn) {
  showAttentionOnly = !showAttentionOnly;
  btn.classList.toggle('active');
  document.getElementById('attention-section').style.display = showAttentionOnly ? 'block' : 'none';
  filterCards();
}

async function loadAttention() {
  const resp = await fetch('/api/attention');
  const data = await resp.json();
  const list = document.getElementById('attention-list');
  list.innerHTML = data.map(c => {
    const reasons = (c.attention_reasons || []).join(' · ');
    return '<div class="attention-row" onclick="location.href=\\'/client/' + c.customer_id + '\\'">' +
      '<div class="attention-dot dot-' + c.attention_level + '"></div>' +
      '<div class="attention-name">' + c.client_name + '</div>' +
      '<div class="attention-reasons">' + reasons + '</div>' +
    '</div>';
  }).join('');
}

loadClients();
</script>
</body>
</html>`;

const CLIENT_DETAIL_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Client Detail | Blueprint for Scale</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #F5F1E8; color: #1a1a1a; }

  .header { background: #000; color: #F5F1E8; padding: 16px 32px; display: flex; align-items: center; gap: 16px; }
  .header a { color: #E85D4D; text-decoration: none; font-size: 14px; }
  .header h1 { font-size: 18px; font-weight: 600; }

  .content { max-width: 1200px; margin: 0 auto; padding: 24px 32px; }

  .top-row { display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 16px; margin-bottom: 24px; }
  .stat-card { background: #fff; border-radius: 10px; padding: 16px; border: 1px solid #e0dcd4; text-align: center; }
  .stat-value { font-size: 28px; font-weight: 700; }
  .stat-label { font-size: 12px; color: #888; text-transform: uppercase; margin-top: 4px; }
  .stat-sub { font-size: 12px; color: #666; margin-top: 4px; }

  .sections { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
  .section { background: #fff; border-radius: 10px; padding: 20px; border: 1px solid #e0dcd4; }
  .section-title { font-size: 14px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; color: #888; margin-bottom: 12px; }
  .section-full { grid-column: 1 / -1; }

  .interaction { padding: 10px 0; border-bottom: 1px solid #f0f0f0; }
  .interaction:last-child { border-bottom: none; }
  .interaction-header { display: flex; justify-content: space-between; font-size: 13px; margin-bottom: 4px; }
  .interaction-type { font-weight: 600; text-transform: capitalize; }
  .interaction-date { color: #888; }
  .interaction-summary { font-size: 13px; line-height: 1.5; }
  .interaction-actions { font-size: 12px; color: #E85D4D; margin-top: 4px; }
  .sentiment-tag { font-size: 11px; padding: 1px 6px; border-radius: 8px; }
  .sent-positive { background: #d4edda; color: #155724; }
  .sent-negative { background: #f8d7da; color: #721c24; }
  .sent-at_risk { background: #f8d7da; color: #721c24; }
  .sent-neutral { background: #e9ecef; color: #495057; }

  .task { padding: 8px 0; border-bottom: 1px solid #f0f0f0; display: flex; justify-content: space-between; align-items: center; }
  .task:last-child { border-bottom: none; }
  .task-title { font-size: 13px; }
  .task-meta { font-size: 11px; color: #888; display: flex; gap: 8px; }
  .task-overdue { color: #dc3545; font-weight: 600; }

  .alert { padding: 8px 12px; border-radius: 6px; margin-bottom: 8px; font-size: 13px; }
  .alert-critical { background: #f8d7da; color: #721c24; }
  .alert-warning { background: #fff3cd; color: #856404; }
  .alert-info { background: #d1ecf1; color: #0c5460; }

  .note { padding: 8px 0; border-bottom: 1px solid #f0f0f0; font-size: 13px; line-height: 1.5; }
  .note:last-child { border-bottom: none; }
  .note-category { font-size: 11px; color: #888; text-transform: capitalize; }

  .contact { padding: 8px 0; border-bottom: 1px solid #f0f0f0; }
  .contact:last-child { border-bottom: none; }
  .contact-name { font-size: 13px; font-weight: 600; }
  .contact-detail { font-size: 12px; color: #666; }

  .profile-row { display: flex; justify-content: space-between; padding: 6px 0; font-size: 13px; border-bottom: 1px solid #f0f0f0; }
  .profile-row:last-child { border-bottom: none; }
  .profile-label { color: #888; }
  .profile-value { font-weight: 500; text-align: right; max-width: 60%; }
</style>
</head>
<body>
<div class="header">
  <a href="/">&larr; All Clients</a>
  <h1 id="client-name">Loading...</h1>
</div>
<div class="content">
  <div class="top-row" id="top-stats"></div>
  <div class="sections" id="sections"></div>
</div>

<script>
const customerId = location.pathname.split('/').pop();

async function load() {
  const resp = await fetch('/api/clients/' + customerId);
  const data = await resp.json();
  const p = data.profile || {};
  const m = data.metrics || {};

  document.getElementById('client-name').textContent = p.client_name || 'Unknown';
  document.title = (p.client_name || 'Client') + ' | Client Intelligence';

  // Top stats
  const leadsChange = m.leads_last_month > 0
    ? Math.round((m.leads_this_month / m.leads_last_month - 1) * 100) : null;
  const changeHtml = leadsChange !== null
    ? '<div class="stat-sub" style="color:' + (leadsChange >= 0 ? '#28a745' : '#dc3545') + '">' + (leadsChange >= 0 ? '+' : '') + leadsChange + '% vs last month</div>'
    : '';

  const r = data.roas || {};
  const roasVal = r.roas_ratio ? parseFloat(r.roas_ratio).toFixed(1) + 'x' : '-';
  const revenueVal = r.roas_revenue ? '$' + Number(r.roas_revenue).toLocaleString(undefined, {maximumFractionDigits:0}) : '-';
  const spendVal = r.ad_spend ? '$' + Number(r.ad_spend).toLocaleString(undefined, {maximumFractionDigits:0}) : '-';
  const cplVal = r.cost_per_lead ? '$' + Number(r.cost_per_lead).toFixed(0) : '-';

  document.getElementById('top-stats').innerHTML =
    '<div class="stat-card"><div class="stat-value">' + (m.leads_this_month || 0) + '</div><div class="stat-label">Leads This Month</div>' + changeHtml + '</div>' +
    '<div class="stat-card"><div class="stat-value">' + roasVal + '</div><div class="stat-label">ROAS</div><div class="stat-sub">' + revenueVal + ' rev / ' + spendVal + ' spend</div></div>' +
    '<div class="stat-card"><div class="stat-value">' + (p.open_tasks || 0) + '</div><div class="stat-label">Open Tasks</div><div class="stat-sub">' + (p.overdue_tasks || 0) + ' overdue</div></div>' +
    '<div class="stat-card"><div class="stat-value">' + (p.program_month || '-') + '</div><div class="stat-label">Program Month</div><div class="stat-sub">' + (p.monthly_retainer ? '$' + Number(p.monthly_retainer).toLocaleString() + '/yr' : '') + ' · CPL ' + cplVal + '</div></div>';

  // Build sections
  let html = '';

  // Alerts
  if (data.alerts && data.alerts.length > 0) {
    html += '<div class="section section-full"><div class="section-title">Active Alerts</div>';
    data.alerts.forEach(a => {
      html += '<div class="alert alert-' + a.severity + '">' + a.message + '</div>';
    });
    html += '</div>';
  }

  // Profile info
  html += '<div class="section"><div class="section-title">Profile</div>';
  const profileRows = [
    ['Account Manager', p.account_manager],
    ['State', p.client_state],
    ['Tier', p.client_tier],
    ['Status', p.onboarding_status],
    ['Renewal', p.days_until_renewal ? p.days_until_renewal + ' days' : null],
    ['Last Contact', p.last_interaction_date ? new Date(p.last_interaction_date).toLocaleDateString() : 'Never'],
    ['Sentiment', p.last_sentiment],
    ['Slack', p.slack_channel_name ? '#' + p.slack_channel_name : null],
  ];
  profileRows.forEach(([label, value]) => {
    if (value) html += '<div class="profile-row"><span class="profile-label">' + label + '</span><span class="profile-value">' + value + '</span></div>';
  });
  if (p.client_goals) html += '<div style="margin-top:8px;font-size:13px;"><strong>Goals:</strong> ' + p.client_goals + '</div>';
  html += '</div>';

  // Contacts
  html += '<div class="section"><div class="section-title">Contacts</div>';
  if (data.contacts && data.contacts.length > 0) {
    data.contacts.forEach(c => {
      html += '<div class="contact"><div class="contact-name">' + c.name + (c.role ? ' — ' + c.role : '') + '</div>';
      if (c.phone) html += '<div class="contact-detail">' + c.phone + '</div>';
      if (c.email) html += '<div class="contact-detail">' + c.email + '</div>';
      html += '</div>';
    });
  } else {
    html += '<div style="font-size:13px;color:#888;">No contacts yet</div>';
  }
  html += '</div>';

  // Recent interactions
  html += '<div class="section"><div class="section-title">Recent Interactions</div>';
  if (data.interactions && data.interactions.length > 0) {
    data.interactions.forEach(i => {
      const sentClass = i.sentiment ? 'sent-' + i.sentiment : '';
      const sentHtml = i.sentiment ? ' <span class="sentiment-tag ' + sentClass + '">' + i.sentiment + '</span>' : '';
      html += '<div class="interaction">' +
        '<div class="interaction-header"><span class="interaction-type">' + i.interaction_type + sentHtml + '</span><span class="interaction-date">' + new Date(i.interaction_date).toLocaleDateString() + '</span></div>' +
        (i.summary ? '<div class="interaction-summary">' + i.summary + '</div>' : '') +
        (i.action_items ? '<div class="interaction-actions">Action: ' + i.action_items + '</div>' : '') +
      '</div>';
    });
  } else {
    html += '<div style="font-size:13px;color:#888;">No interactions logged yet</div>';
  }
  html += '</div>';

  // Tasks
  html += '<div class="section"><div class="section-title">Open Tasks</div>';
  if (data.tasks && data.tasks.length > 0) {
    data.tasks.forEach(t => {
      const overdue = t.due_date && new Date(t.due_date) < new Date() ? ' task-overdue' : '';
      const dueText = t.due_date ? new Date(t.due_date).toLocaleDateString() : '';
      html += '<div class="task"><div class="task-title">' + t.title + '</div><div class="task-meta">' +
        (t.assigned_to ? '<span>' + t.assigned_to + '</span>' : '') +
        (dueText ? '<span class="' + overdue + '">' + dueText + '</span>' : '') +
      '</div></div>';
    });
  } else {
    html += '<div style="font-size:13px;color:#888;">No open tasks</div>';
  }
  html += '</div>';

  // Personal notes
  html += '<div class="section"><div class="section-title">Personal Notes</div>';
  if (data.personalNotes && data.personalNotes.length > 0) {
    data.personalNotes.forEach(n => {
      html += '<div class="note"><span class="note-category">' + n.category + '</span> — ' + n.note + '</div>';
    });
  } else {
    html += '<div style="font-size:13px;color:#888;">No personal notes yet</div>';
  }
  html += '</div>';

  document.getElementById('sections').innerHTML = html;
}

load();
</script>
</body>
</html>`;

// Recent activity for account manager prep — shows all sources with labels
app.get('/api/prep/recent-activity/:customerId', async (req, res) => {
  const cid = req.params.customerId;
  try {
    const sourceLabel = `CASE
      WHEN hc.attribution_override IS NOT NULL THEN hc.attribution_override
      WHEN EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source_name = 'LSA') THEN 'lsa'
      WHEN EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND is_google_ads_call(c.source, c.source_name, c.gclid) AND COALESCE(c.source_name,'') <> 'LSA') THEN 'google_ads'
      WHEN EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND (f.gclid IS NOT NULL OR f.source ILIKE '%google ads%')) THEN 'google_ads'
      WHEN EXISTS (SELECT 1 FROM calls c WHERE c.callrail_id = hc.callrail_id AND c.source = 'Google My Business') THEN 'gbp'
      WHEN EXISTS (SELECT 1 FROM form_submissions f WHERE f.customer_id = hc.customer_id AND (f.callrail_id = hc.callrail_id OR normalize_phone(f.customer_phone) = hc.phone_normalized OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email))) AND f.source = 'Google My Business') THEN 'gbp'
      ELSE 'other'
    END`;
    const flagExclude = `AND COALESCE(hc.client_flag_reason, '') NOT IN ('spam', 'out_of_area', 'wrong_service')`;
    const spamExclude = `AND NOT EXISTS (SELECT 1 FROM ghl_contacts gc WHERE gc.phone_normalized = hc.phone_normalized AND gc.customer_id = hc.customer_id AND gc.lost_reason IS NOT NULL AND (gc.lost_reason ILIKE '%spam%' OR gc.lost_reason ILIKE '%not a lead%' OR gc.lost_reason ILIKE '%spoofed%' OR gc.lost_reason ILIKE '%duplicate%'))`;

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
        WHERE hc.customer_id = $1 ${flagExclude} ${spamExclude}
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
        WHERE hc.customer_id = $1 ${flagExclude} ${spamExclude}
          AND COALESCE(eg.approved_at, eg.sent_at) >= NOW() - INTERVAL '90 days'
        ORDER BY COALESCE(eg.approved_at, eg.sent_at) DESC
        LIMIT 5
      )
      ORDER BY event_date DESC
      LIMIT 5
    `, [cid]);

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
      WHERE hc.customer_id = $1 ${flagExclude} ${spamExclude}
      ORDER BY ins.scheduled_at ASC
      LIMIT 3
    `, [cid]);

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
      WHERE hc.customer_id = $1 ${flagExclude} ${spamExclude}
        AND ins.scheduled_at >= NOW() - INTERVAL '30 days'
      ORDER BY ins.scheduled_at DESC
      LIMIT 3
    `, [cid]);

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
        (SELECT ROUND(c.duration / 60.0, 1) FROM calls c WHERE c.callrail_id = hc.callrail_id LIMIT 1) as call_minutes
      FROM hcp_customers hc
      WHERE hc.customer_id = $1 ${flagExclude} ${spamExclude}
      ORDER BY hc.hcp_created_at DESC
      LIMIT 10
    `, [cid]);

    // Outstanding estimates (sent but not approved — shown when no wins)
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
      WHERE hc.customer_id = $1 ${flagExclude} ${spamExclude}
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
    console.error('Prep recent activity error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── Start Server ────────────────────────────────────────────

app.listen(PORT, () => {
  console.log('[Client Intelligence] Dashboard running on port ' + PORT);
});
