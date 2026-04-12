#!/usr/bin/env node
/**
 * Real-Time State Change Alerts — runs hourly via launchd
 *
 * Compares current risk status against last known state and sends alerts for:
 *   - New risk entry (client enters Risk from Healthy/Flag)
 *   - New flag entry (client enters Flag from Healthy)
 *   - Recovery (client returns to Healthy from Risk/Flag)
 *   - Guarantee hit (guarantee crosses 100% for the first time)
 *   - 30-lead milestone (quality leads crosses 30 for the first time)
 *   - No recent leads (7+ days without a lead, alerted once per occurrence)
 *   - Budget change (budget column changes in clients table)
 *
 * Posts to: #client_notifications + client's Slack channel + manager's channel/DM
 *
 * Usage:
 *   node slack-realtime-alerts.js              # Run checks and send alerts
 *   node slack-realtime-alerts.js --dry-run    # Print what would be sent, don't post
 */

const { Pool } = require('pg');
const https = require('https');

const pool = new Pool({
  user: 'blueprint',
  database: 'blueprint',
  host: 'localhost',
  port: 5432,
  ssl: false,
});

const SLACK_TOKEN = 'xoxb-6594692085893-10476528834625-otlCrGN5kiu31kQYWDvrCAwC';
const NOTIFICATIONS_CHANNEL = 'C09AZ1MCLN7'; // #client_notifications

const MANAGER_CHANNELS = {
  'Martin': 'C0AN42R5YJE',
  'Luke':   'C08LQL3TPGA',
  'Nima':   'C09P10LJZJ5',
};

// ── Slack API ──────────────────────────────────────────────

function postToSlack(channel, text) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify({ channel, text, mrkdwn: true });
    const options = {
      hostname: 'slack.com',
      path: '/api/chat.postMessage',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${SLACK_TOKEN}`,
        'Content-Length': Buffer.byteLength(payload),
      },
    };
    const req = https.request(options, (res) => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => {
        try {
          const data = JSON.parse(body);
          if (!data.ok) reject(new Error(`Slack: ${data.error}`));
          else resolve(data);
        } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

async function sendToAll(msg, clientChannelId, manager, dryRun) {
  const targets = [NOTIFICATIONS_CHANNEL];
  if (clientChannelId) targets.push(clientChannelId);
  const mgrChannel = MANAGER_CHANNELS[manager];
  if (mgrChannel) targets.push(mgrChannel);

  // Deduplicate
  const unique = [...new Set(targets)];

  for (const ch of unique) {
    if (dryRun) {
      console.log(`  [DRY RUN] Would post to ${ch}`);
    } else {
      try {
        await postToSlack(ch, msg);
        console.log(`  Posted to ${ch}`);
      } catch (e) {
        console.error(`  Error posting to ${ch}: ${e.message}`);
      }
      await sleep(500);
    }
  }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── Helpers ────────────────────────────────────────────────

function clientName(name) {
  return name.includes('|') ? name.split('|').pop().trim() : name;
}

function fmtMoney(v) {
  if (!v || v === 0) return '$0';
  if (v >= 1000000) return `$${(v / 1000000).toFixed(1)}M`;
  if (v >= 1000) return `$${(v / 1000).toFixed(1)}k`;
  return `$${Math.round(v)}`;
}

function coerceRow(r) {
  for (const k of ['cpl', 'roas', 'guarantee', 'ad_spend', 'spam_rate', 'budget',
    'total_closed_rev', 'total_open_est_rev', 'insp_booked_pct', 'lead_volume_change']) {
    if (r[k] !== null && r[k] !== undefined) r[k] = parseFloat(r[k]);
  }
  for (const k of ['actual_quality_leads', 'months_in_program', 'days_since_lead',
    'on_cal_14d', 'flag_count']) {
    if (r[k] !== null && r[k] !== undefined) r[k] = parseInt(r[k]);
  }
  return r;
}

// ── Alert formatters ───────────────────────────────────────

function formatRiskAlert(row) {
  const name = clientName(row.client_name);
  let msg = `:rotating_light: *RISK ALERT* :rotating_light:\n`;
  msg += `*${name}* has entered Risk status\n`;
  msg += `Risk Type: ${row.risk_type}\n`;
  msg += `Manager: ${row.ads_manager || '—'}\n\n`;

  if (row.risk_triggers && row.risk_triggers.length) {
    msg += `*Triggering Metrics:*\n`;
    for (const t of row.risk_triggers) msg += `• ${t}\n`;
  }
  msg += `\n*Action required:* Review account immediately`;
  return msg;
}

function formatFlagAlert(row) {
  const name = clientName(row.client_name);
  let msg = `:warning: *FLAG ALERT* :warning:\n`;
  msg += `*${name}* has entered Flag status\n`;
  msg += `Manager: ${row.ads_manager || '—'}\n\n`;

  if (row.flag_triggers && row.flag_triggers.length) {
    msg += `*Flagged Metrics (${row.flag_triggers.length}):*\n`;
    for (const t of row.flag_triggers) msg += `• ${t}\n`;
  }
  msg += `\n*Action:* Monitor closely`;
  return msg;
}

function formatRecoveryAlert(row, fromStatus) {
  const name = clientName(row.client_name);
  let msg = `:white_check_mark: *RECOVERY* :white_check_mark:\n`;
  msg += `*${name}* has recovered to Healthy!\n`;
  msg += `Was in: ${fromStatus}\n`;
  msg += `Manager: ${row.ads_manager || '—'}`;
  return msg;
}

function formatGuaranteeHit(row) {
  const name = clientName(row.client_name);
  let msg = `:tada: *GUARANTEE HIT!* :tada:\n`;
  msg += `*${name}* has reached 100% of their guarantee!\n\n`;
  msg += `Guarantee: ${(row.guarantee * 100).toFixed(0)}%\n`;
  msg += `Manager: ${row.ads_manager || '—'}`;
  return msg;
}

function format30Leads(row) {
  const name = clientName(row.client_name);
  let msg = `:chart_with_upwards_trend: *30-LEAD MILESTONE!* :chart_with_upwards_trend:\n`;
  msg += `*${name}* has hit 30+ leads in 30 days!\n\n`;
  msg += `Lead Count: ${row.actual_quality_leads}\n`;
  msg += `Manager: ${row.ads_manager || '—'}`;
  return msg;
}

function formatNoRecentLeads(row) {
  const name = clientName(row.client_name);
  let msg = `:clock3: *NO RECENT LEADS*\n`;
  msg += `*${name}* — ${row.days_since_lead} days since last lead\n`;
  msg += `Manager: ${row.ads_manager || '—'}\n`;
  msg += `*Action:* Check campaign delivery`;
  return msg;
}

function formatBudgetChange(row, oldBudget, newBudget) {
  const name = clientName(row.client_name);
  const change = newBudget - oldBudget;
  const pct = oldBudget > 0 ? ((change / oldBudget) * 100).toFixed(0) : '∞';
  const sign = change >= 0 ? '+' : '';
  let msg = `:moneybag: *BUDGET UPDATE*\n`;
  msg += `*${name}*\n`;
  msg += `Old: ${fmtMoney(oldBudget)} → New: ${fmtMoney(newBudget)} (${sign}${pct}%)\n`;
  msg += `Manager: ${row.ads_manager || '—'}`;
  return msg;
}

// ── Main ───────────────────────────────────────────────────

async function main() {
  const dryRun = process.argv.includes('--dry-run');
  if (dryRun) console.log('=== DRY RUN MODE ===\n');

  // 1. Get current dashboard state + confirmed statuses
  const [dashResult, confirmedResult] = await Promise.all([
    pool.query(`SELECT d.*, c.slack_channel_id FROM get_dashboard_with_risk() d JOIN clients c ON c.customer_id = d.customer_id`),
    pool.query(`SELECT customer_id, confirmed_status, confirmed_risk_type,
                       pending_status, pending_streak FROM client_confirmed_status`)
  ]);
  const currentRows = dashResult.rows.map(coerceRow);
  const confirmedMap = {};
  for (const c of confirmedResult.rows) {
    confirmedMap[String(c.customer_id)] = c;
  }

  // 2. Get last known state
  const lastResult = await pool.query(`SELECT * FROM alert_last_state`);
  const lastState = {};
  for (const r of lastResult.rows) {
    lastState[r.customer_id] = r;
  }

  // 3. Check for already-sent pending alerts (only send once per pending direction)
  const pendingAlertResult = await pool.query(`
    SELECT customer_id, details->>'pending_status' AS pending_status
    FROM alert_status_log
    WHERE event_type = 'pending_heads_up'
      AND NOT EXISTS (
        SELECT 1 FROM alert_status_log a2
        WHERE a2.customer_id = alert_status_log.customer_id
          AND a2.event_type IN ('risk_entered','flag_entered','risk_resolved','flag_resolved')
          AND a2.event_date > alert_status_log.event_date
      )
  `);
  const sentPending = new Set();
  for (const r of pendingAlertResult.rows) {
    if (r.pending_status) sentPending.add(`${r.customer_id}_${r.pending_status}`);
  }

  // 4. Check for already-celebrated milestones
  const milestoneResult = await pool.query(`
    SELECT customer_id, event_type FROM alert_status_log
    WHERE event_type IN ('guarantee_hit', '30_leads')
  `);
  const celebrated = new Set();
  for (const r of milestoneResult.rows) {
    celebrated.add(`${r.customer_id}_${r.event_type}`);
  }

  // 4. Check for active no-leads alerts (don't re-alert)
  const noLeadsResult = await pool.query(`
    SELECT customer_id FROM alert_status_log
    WHERE event_type = 'no_recent_leads'
      AND NOT EXISTS (
        SELECT 1 FROM alert_status_log a2
        WHERE a2.customer_id = alert_status_log.customer_id
          AND a2.event_type = 'no_recent_leads_resolved'
          AND a2.event_date > alert_status_log.event_date
      )
  `);
  const activeNoLeadsAlerts = new Set(noLeadsResult.rows.map(r => String(r.customer_id)));

  let alertCount = 0;

  for (const row of currentRows) {
    const cid = row.customer_id;
    const last = lastState[cid];
    const name = clientName(row.client_name);
    const channelId = row.slack_channel_id || null;
    const manager = row.ads_manager;

    // ── Status transitions (using CONFIRMED status, not raw computed) ──
    const confirmed = confirmedMap[String(cid)];
    const currStatus = confirmed ? confirmed.confirmed_status : row.status;
    const currRiskType = confirmed ? (confirmed.confirmed_risk_type || '') : row.risk_type;

    if (last) {
      const prevStatus = last.status;

      // New Risk (confirmed)
      if (currStatus === 'Risk' && prevStatus !== 'Risk') {
        console.log(`RISK (confirmed): ${name} (${prevStatus} → Risk)`);
        const msg = formatRiskAlert(row);
        await sendToAll(msg, channelId, manager, dryRun);
        await logEvent(cid, 'risk_entered', prevStatus, currStatus, currRiskType, row.risk_triggers);
        alertCount++;
      }

      // New Flag (confirmed)
      if (currStatus === 'Flag' && prevStatus === 'Healthy') {
        console.log(`FLAG (confirmed): ${name} (Healthy → Flag)`);
        const msg = formatFlagAlert(row);
        await sendToAll(msg, channelId, manager, dryRun);
        await logEvent(cid, 'flag_entered', prevStatus, currStatus, null, row.flag_triggers);
        alertCount++;
      }

      // Recovery from Risk (confirmed)
      if (currStatus === 'Healthy' && prevStatus === 'Risk') {
        console.log(`RECOVERY (confirmed): ${name} (Risk → Healthy)`);
        const msg = formatRecoveryAlert(row, 'Risk');
        await sendToAll(msg, channelId, manager, dryRun);
        await logEvent(cid, 'risk_resolved', prevStatus, currStatus, null, null);
        alertCount++;
      }

      // Recovery from Flag (confirmed)
      if (currStatus === 'Healthy' && prevStatus === 'Flag') {
        console.log(`RECOVERY (confirmed): ${name} (Flag → Healthy)`);
        const msg = formatRecoveryAlert(row, 'Flag');
        await sendToAll(msg, channelId, manager, dryRun);
        await logEvent(cid, 'flag_resolved', prevStatus, currStatus, null, null);
        alertCount++;
      }

      // ── Soft heads-up for pending status changes (once per pending direction) ──
      if (confirmed && confirmed.pending_status && parseInt(confirmed.pending_streak) >= 1
          && !sentPending.has(`${cid}_${confirmed.pending_status}`)) {
        const isGettingWorse = confirmed.pending_status === 'Risk'
          || (confirmed.pending_status === 'Flag' && currStatus === 'Healthy');
        const statusCircle = confirmed.pending_status === 'Risk' ? ':red_circle:'
          : confirmed.pending_status === 'Flag' ? ':large_yellow_circle:'
          : ':large_green_circle:';
        // Asymmetric thresholds for day count display
        const needed = confirmed.pending_status === 'Risk' ? 1
          : confirmed.pending_status === 'Flag' && currStatus === 'Healthy' ? 2
          : confirmed.pending_status === 'Flag' && currStatus === 'Risk' ? 3
          : confirmed.pending_status === 'Healthy' && currStatus === 'Risk' ? 5
          : 3;
        // Include the triggering metrics
        const triggers = isGettingWorse
          ? (row.risk_triggers && row.risk_triggers.length ? row.risk_triggers : row.flag_triggers || [])
          : [];
        const improvingMetrics = !isGettingWorse ? [] : [];
        let metricsLine = '';
        if (isGettingWorse && triggers.length > 0) {
          metricsLine = triggers.join(' · ');
        } else if (!isGettingWorse) {
          // Show key positive stats for improving clients
          const parts = [];
          if (parseFloat(row.roas) > 0) parts.push(`ROAS ${parseFloat(row.roas).toFixed(1)}x`);
          if (parseFloat(row.trailing_3mo_roas) > 0) parts.push(`3mo ROAS ${parseFloat(row.trailing_3mo_roas).toFixed(1)}x`);
          if (parseFloat(row.guarantee) > 0) parts.push(`Guarantee ${parseFloat(row.guarantee).toFixed(1)}x`);
          if (parseInt(row.quality_leads) > 0) parts.push(`${row.quality_leads} leads`);
          if (parts.length) metricsLine = parts.join(' · ');
        }
        const dirLabel = isGettingWorse ? 'worsening' : 'improving';
        const pendingMsg = `${statusCircle} *HEADS UP* — *${name}* trending toward ${confirmed.pending_status} (${dirLabel})${metricsLine ? '\n' + metricsLine.trim() : ''} (day 1/${needed})`;
        console.log(`PENDING: ${name} → ${confirmed.pending_status} (day 1/${needed})`);
        // Post to client channel only (keeps manager channels clean)
        if (channelId && !dryRun) {
          try { await postToSlack(channelId, pendingMsg); } catch (e) { console.error(`  Error posting to client channel: ${e.message}`); }
        }
        if (dryRun) console.log(`  [DRY RUN] Would post to client channel`);
        await logEvent(cid, 'pending_heads_up', currStatus, confirmed.pending_status, null,
          { pending_status: confirmed.pending_status, direction: dirLabel });
      }

      // ── Budget change ──
      const oldBudget = parseFloat(last.budget) || 0;
      const newBudget = row.budget || 0;
      if (oldBudget > 0 && newBudget > 0 && oldBudget !== newBudget) {
        console.log(`BUDGET: ${name} ($${oldBudget} → $${newBudget})`);
        const msg = formatBudgetChange(row, oldBudget, newBudget);
        await sendToAll(msg, channelId, manager, dryRun);
        await logEvent(cid, 'budget_change', null, null, null, { old: oldBudget, new: newBudget });
        alertCount++;
      }
    }

    // ── Milestones (one-time celebrations) ──

    // Guarantee hit
    if (row.guarantee >= 1.0 && !celebrated.has(`${cid}_guarantee_hit`)) {
      console.log(`MILESTONE: ${name} hit guarantee (${(row.guarantee * 100).toFixed(0)}%)`);
      const msg = formatGuaranteeHit(row);
      await sendToAll(msg, channelId, manager, dryRun);
      await logEvent(cid, 'guarantee_hit', null, null, null, { guarantee: row.guarantee });
      alertCount++;
    }

    // 30-lead milestone
    if (row.actual_quality_leads >= 30 && !celebrated.has(`${cid}_30_leads`)) {
      console.log(`MILESTONE: ${name} hit 30 leads (${row.actual_quality_leads})`);
      const msg = format30Leads(row);
      await sendToAll(msg, channelId, manager, dryRun);
      await logEvent(cid, '30_leads', null, null, null, { leads: row.actual_quality_leads });
      alertCount++;
    }

    // ── No recent leads (7+ days, alert once per occurrence) ──
    if (row.days_since_lead !== null && row.days_since_lead >= 7) {
      if (!activeNoLeadsAlerts.has(String(cid))) {
        console.log(`NO LEADS: ${name} (${row.days_since_lead} days)`);
        const msg = formatNoRecentLeads(row);
        // Only send to client + manager channels, not main notifications
        if (channelId && !dryRun) {
          try { await postToSlack(channelId, msg); } catch (e) { console.error(`  Error: ${e.message}`); }
        }
        const mgrCh = MANAGER_CHANNELS[manager];
        if (mgrCh && !dryRun) {
          try { await postToSlack(mgrCh, msg); } catch (e) { console.error(`  Error: ${e.message}`); }
        }
        if (dryRun) console.log(`  [DRY RUN] Would post to client + manager channels`);
        await logEvent(cid, 'no_recent_leads', null, null, null, { days: row.days_since_lead });
        alertCount++;
      }
    } else if (row.days_since_lead !== null && row.days_since_lead < 7) {
      // Resolve active no-leads alert if leads came back
      if (activeNoLeadsAlerts.has(String(cid))) {
        await logEvent(cid, 'no_recent_leads_resolved', null, null, null, null);
      }
    }

    // ── Update last known state (use confirmed status for next comparison) ──
    if (!dryRun) {
      await pool.query(`
        INSERT INTO alert_last_state (customer_id, status, risk_type, guarantee, quality_leads, budget, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        ON CONFLICT (customer_id) DO UPDATE SET
          status = EXCLUDED.status, risk_type = EXCLUDED.risk_type,
          guarantee = EXCLUDED.guarantee, quality_leads = EXCLUDED.quality_leads,
          budget = EXCLUDED.budget, updated_at = NOW()
      `, [cid, currStatus, currRiskType, row.guarantee, row.actual_quality_leads, row.budget]);
    }
  }

  console.log(`\nDone. ${alertCount} alerts sent.`);
  await pool.end();
}

async function logEvent(customerId, eventType, prevStatus, newStatus, riskType, details) {
  await pool.query(`
    INSERT INTO alert_status_log (customer_id, event_type, previous_status, new_status, risk_type, details, alerted)
    VALUES ($1, $2, $3, $4, $5, $6, true)
  `, [customerId, eventType, prevStatus, newStatus, riskType, details ? JSON.stringify(details) : null]);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
