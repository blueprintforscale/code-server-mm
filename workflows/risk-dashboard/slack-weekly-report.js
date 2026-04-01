#!/usr/bin/env node
require('dotenv').config({ path: __dirname + '/.env' });
/**
 * Weekly Risk Report — Posts to #client_notifications every Sunday night (UK time)
 *
 * Queries get_dashboard_with_risk() and formats a Slack summary of all client health.
 *
 * Usage:
 *   node slack-weekly-report.js                  # Send to #client_notifications
 *   node slack-weekly-report.js --test           # Send to #client_notifications (same, for testing)
 *   node slack-weekly-report.js --channel C1234  # Override channel
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

const SLACK_TOKEN = process.env.SLACK_BOT_TOKEN;
const DEFAULT_CHANNEL = 'C0ADG0XCYE7'; // #reports

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
          if (!data.ok) reject(new Error(`Slack API: ${data.error}`));
          else resolve(data);
        } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

// ── Formatting helpers ─────────────────────────────────────

function fmtMoney(v) {
  if (!v || v === 0) return '$0';
  if (v >= 1000000) return `$${(v / 1000000).toFixed(1)}M`;
  if (v >= 1000) return `$${(v / 1000).toFixed(1)}k`;
  return `$${Math.round(v)}`;
}

function fmtPct(v) {
  if (v === null || v === undefined) return '—';
  return `${(v * 100).toFixed(0)}%`;
}

function fmtDuration(days) {
  if (!days || days === 0) return 'today';
  if (days < 7) return `${days}d`;
  if (days < 30) return `${Math.floor(days / 7)}w`;
  return `${Math.floor(days / 30)}mo`;
}

function clientDisplayName(name) {
  // "Owner Name | Business Name" → "Business Name"
  return name.includes('|') ? name.split('|').pop().trim() : name;
}

// ── Build risk metrics line ────────────────────────────────

function buildMetricsLine(row) {
  const parts = [];
  const leads = row.actual_quality_leads || 0;
  const months = row.months_in_program || 0;

  if (leads > 0 && leads < 30) parts.push(`${leads} leads`);
  if (row.cpl > 140) parts.push(`CPL $${Math.round(row.cpl)}`);
  if (row.lead_volume_change !== null && row.lead_volume_change < -0.1)
    parts.push(`Vol ${(row.lead_volume_change * 100).toFixed(0)}%`);
  if (row.insp_booked_pct !== null && row.insp_booked_pct < 0.28)
    parts.push(`Book ${(row.insp_booked_pct * 100).toFixed(0)}%`);
  if (row.roas !== null && row.roas < 2)
    parts.push(`ROAS ${row.roas.toFixed(1)}x`);
  if (row.guarantee !== null && row.guarantee < 1 && months >= 5)
    parts.push(`Guar ${(row.guarantee * 100).toFixed(0)}%`);
  if (row.total_open_est_rev > 0)
    parts.push(`Pipeline ${fmtMoney(row.total_open_est_rev)}`);
  if (row.spam_rate > 0.11)
    parts.push(`Spam ${(row.spam_rate * 100).toFixed(0)}%`);
  if (row.on_cal_14d !== null && row.on_cal_14d <= 1)
    parts.push(`${row.on_cal_14d} on cal`);

  return parts.join(' · ');
}

// ── Main report builder ────────────────────────────────────

async function buildWeeklyReport() {
  const result = await pool.query(`
    SELECT * FROM get_dashboard_with_risk()
    ORDER BY sort_priority, client_name
  `);

  // Coerce numeric fields from string to number (PG returns NUMERIC as string)
  const rows = result.rows.map(r => {
    for (const key of ['cpl', 'roas', 'guarantee', 'ad_spend', 'all_time_spend', 'all_time_rev',
      'spam_rate', 'abandoned_rate', 'insp_booked_pct', 'lead_volume_change', 'call_answer_rate',
      'total_closed_rev', 'total_open_est_rev', 'trailing_6mo_roas', 'trailing_3mo_roas',
      'trailing_6mo_potential_roas', 'trailing_3mo_potential_roas', 'lsa_spend', 'budget']) {
      if (r[key] !== null && r[key] !== undefined) r[key] = parseFloat(r[key]);
    }
    for (const key of ['quality_leads', 'actual_quality_leads', 'prior_actual_quality_leads',
      'spam_contacts', 'total_calls', 'months_in_program', 'days_since_lead',
      'total_insp_booked', 'on_cal_14d', 'on_cal_total', 'lsa_leads', 'sort_priority', 'flag_count']) {
      if (r[key] !== null && r[key] !== undefined) r[key] = parseInt(r[key]);
    }
    return r;
  });
  if (!rows.length) return 'No client data available.';

  // Categorize
  const bothRisk = [];
  const adsRisk = [];
  const funnelRisk = [];
  const flagged = [];
  const healthy = [];

  for (const row of rows) {
    const status = row.status;
    const riskType = row.risk_type;

    if (status === 'Risk') {
      if (riskType === 'Both Risk') bothRisk.push(row);
      else if (riskType === 'Ads Risk') adsRisk.push(row);
      else if (riskType === 'Funnel Risk') funnelRisk.push(row);
    } else if (status === 'Flag') {
      flagged.push(row);
    } else {
      healthy.push(row);
    }
  }

  const riskCount = bothRisk.length + adsRisk.length + funnelRisk.length;
  const flagCount = flagged.length;
  const healthyCount = healthy.length;
  const totalClients = rows.length;
  const riskPct = totalClients > 0 ? ((riskCount / totalClients) * 100).toFixed(0) : 0;
  const healthyPct = totalClients > 0 ? ((healthyCount / totalClients) * 100).toFixed(0) : 0;

  // Date range
  const now = new Date();
  const thirtyAgo = new Date(now.getTime() - 30 * 86400000);
  const fmt = d => d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });

  let msg = `*WEEKLY CLIENT HEALTH REPORT*\n`;
  msg += `${fmt(thirtyAgo)} – ${fmt(now)}\n\n`;

  // Overall summary
  msg += `*Overall:* ${totalClients} clients\n`;
  msg += `:red_circle: Risk: ${riskCount} (${riskPct}%) · :large_yellow_circle: Flag: ${flagCount} · :white_check_mark: Healthy: ${healthyCount} (${healthyPct}%)\n`;

  // Ads scorecard
  const adsRiskTotal = bothRisk.length + adsRisk.length;
  const adsHealthy = totalClients - adsRiskTotal;
  const adsHealthyPct = totalClients > 0 ? ((adsHealthy / totalClients) * 100).toFixed(0) : 0;
  msg += `:dart: Ads Scorecard: ${adsHealthy} of ${totalClients} ads-healthy (${adsHealthyPct}%)\n\n`;

  // ── RISK ACCOUNTS ──
  if (riskCount > 0) {
    msg += `━━━━━━━━━━━━━━━━━━━━━━\n`;
    msg += `*RISK ACCOUNTS*\n\n`;

    if (bothRisk.length > 0) {
      msg += `:fire: *Both Ads & Funnel Risk:*\n`;
      for (const row of bothRisk) {
        msg += formatRiskClient(row);
      }
      msg += '\n';
    }

    if (adsRisk.length > 0) {
      msg += `:money_with_wings: *Ads Risk:*\n`;
      for (const row of adsRisk) {
        msg += formatRiskClient(row);
      }
      msg += '\n';
    }

    if (funnelRisk.length > 0) {
      msg += `:dart: *Funnel Risk:*\n`;
      for (const row of funnelRisk) {
        msg += formatRiskClient(row);
      }
    }
  }

  // ── FLAGGED ACCOUNTS ──
  if (flagCount > 0) {
    msg += `\n━━━━━━━━━━━━━━━━━━━━━━\n`;
    msg += `*FLAGGED ACCOUNTS*\n\n`;
    for (const row of flagged) {
      const metrics = buildMetricsLine(row);
      const name = clientDisplayName(row.client_name);
      const manager = row.ads_manager || '—';
      msg += `:large_yellow_circle: ${name} — ${manager}\n`;
      if (metrics) msg += `   ${metrics}\n`;
      msg += '\n';
    }
  }

  // ── HEALTHY ACCOUNTS ──
  if (healthyCount > 0) {
    msg += `\n━━━━━━━━━━━━━━━━━━━━━━\n`;
    msg += `*HEALTHY (${healthyCount}):* `;
    msg += healthy.map(r => clientDisplayName(r.client_name)).join(', ');
    msg += '\n';
  }

  if (riskCount === 0 && flagCount === 0) {
    msg += `\n:tada: All clients are performing well!\n`;
  }

  return msg;
}

function formatRiskClient(row) {
  const name = clientDisplayName(row.client_name);
  const manager = row.ads_manager || '—';
  const metrics = buildMetricsLine(row);

  // Parse risk/flag triggers from the arrays
  const triggers = [];
  if (row.risk_triggers && row.risk_triggers.length) {
    triggers.push(...row.risk_triggers);
  }

  let line = `• *${name}* — ${manager}`;
  if (row.months_in_program) line += ` (mo ${row.months_in_program})`;
  line += '\n';

  if (triggers.length) {
    line += `   _${triggers.join(' · ')}_\n`;
  } else if (metrics) {
    line += `   ${metrics}\n`;
  }
  line += '\n';
  return line;
}

// ── Main ───────────────────────────────────────────────────

async function main() {
  try {
    const args = process.argv.slice(2);
    const channelOverride = args.includes('--channel') ? args[args.indexOf('--channel') + 1] : null;
    const channel = channelOverride || DEFAULT_CHANNEL;

    console.log('Building weekly risk report...');
    const report = await buildWeeklyReport();

    console.log(`Posting to channel ${channel}...`);
    console.log('---');
    console.log(report);
    console.log('---');

    await postToSlack(channel, report);
    console.log('Report sent successfully!');

  } catch (err) {
    console.error('Error:', err.message);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();
