#!/usr/bin/env node
require('dotenv').config({ path: __dirname + '/.env' });
/**
 * Weekly Ads Manager Brief — Posts per-manager risk summaries
 *
 * Each ads manager gets their portfolio summary in their dedicated channel.
 * Martin gets a DM. Shows risk/flag clients with triggers and duration.
 *
 * Usage:
 *   node slack-manager-briefs.js              # Send to all managers
 *   node slack-manager-briefs.js --test       # Print only, don't send
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

// Manager → Slack channel/DM mapping
const MANAGER_CHANNELS = {
  'Martin': 'C0AN42R5YJE',   // #martin-clients channel
  'Luke':   'C08LQL3TPGA',   // #luke-clients channel
  'Nima':   'C09P10LJZJ5',   // #nima-clients channel
};

// Manual priority overrides — add client names here to force them to top of risk list
// Clear these after use. Format: { 'Business Name': position (1 = top) }
// TODO: Remove after 2026-03-23 Sunday send
const PRIORITY_OVERRIDES = {
  'PureAir Restored': 1,
  'Pure Maintenance of Pueblo': 2,
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
          if (!data.ok) reject(new Error(`Slack API (${channel}): ${data.error}`));
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

function fmtDuration(days) {
  if (!days || days <= 0) return 'today';
  if (days < 7) return `${days}d`;
  if (days < 30) return `${Math.floor(days / 7)}w`;
  return `${Math.floor(days / 30)}mo`;
}

function clientDisplayName(name) {
  return name.includes('|') ? name.split('|').pop().trim() : name;
}

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

  return parts.join(' \u00b7 ');
}

// ── Risk urgency scoring (ads manager perspective) ─────────
// Higher score = needs attention first
// Focus: what can the ads manager action? Ads triggers, CPL, lead volume, staleness.
// Funnel metrics (guarantee, presentation risk) inform context but don't drive urgency.
function riskUrgencyScore(row, daysInRisk) {
  let score = 0;
  const months = parseInt(row.months_in_program) || 0;
  const pipeline = parseFloat(row.total_open_est_rev) || 0;
  const trailing3mo = parseFloat(row.trailing_3mo_roas) || 0;
  const trailing3moPotential = parseFloat(row.trailing_3mo_potential_roas) || 0;
  const cpl = parseFloat(row.cpl) || 0;
  const leads = parseInt(row.actual_quality_leads) || 0;
  const budget = parseFloat(row.budget) || 0;

  // Count ads-specific risk triggers only (not funnel/presentation triggers)
  const adsRiskTriggers = (row.risk_triggers || []).filter(t =>
    !t.includes('Guarantee') && !t.includes('Presentation risk') && !t.includes('funnel')
  );
  const flagTriggers = row.flag_triggers || [];

  // ── Both Risk = ads problems + failing guarantee/presentation ──
  // This is red alert — the ads aren't working AND the client story is bad
  if (row.risk_type === 'Both Risk') score += 40;

  // ── Ads trigger severity ──
  // Each ads risk trigger is significant — multiple = compounding problems
  score += adsRiskTriggers.length * 25;

  // CPL severity (higher = more waste)
  if (cpl > 300) score += 30;
  else if (cpl > 200) score += 20;
  else if (cpl > 170) score += 10;

  // Lead volume — very low leads with high budget = burning money
  if (leads <= 10 && budget >= 3000) score += 25;
  else if (leads < 20 && budget >= 3000) score += 15;

  // Staleness — no recent leads = campaign may be broken
  const daysSinceLead = parseInt(row.days_since_lead) || 0;
  if (daysSinceLead >= 10) score += 25;
  else if (daysSinceLead >= 7) score += 15;

  // Flag count adds fragility
  score += flagTriggers.length * 3;

  // ── Duration in ads risk (longer = more entrenched, harder to fix) ──
  score += Math.min(daysInRisk, 60);

  // ── De-escalators ──
  // Strong pipeline = money is coming even if ads metrics look bad
  if (pipeline > 0 && trailing3moPotential >= 2.0) score -= 20;
  // Strong trailing ROAS = recent performance is actually good
  if (trailing3mo >= 3.0) score -= 15;

  return score;
}

// ── Main ───────────────────────────────────────────────────

async function main() {
  const testMode = process.argv.includes('--test');
  const previewMode = process.argv.includes('--preview'); // Send all briefs as DM to Susie for review
  const PREVIEW_CHANNEL = 'U08C7TEGBJ4'; // Susie's DM
  const managerFilter = process.argv.includes('--manager')
    ? process.argv[process.argv.indexOf('--manager') + 1]
    : null;

  // 1. Get dashboard data
  const dashResult = await pool.query(`
    SELECT * FROM get_dashboard_with_risk()
    ORDER BY sort_priority, client_name
  `);

  // Coerce numeric fields
  const rows = dashResult.rows.map(r => {
    for (const key of ['cpl', 'roas', 'guarantee', 'ad_spend', 'spam_rate', 'abandoned_rate',
      'insp_booked_pct', 'lead_volume_change', 'total_closed_rev', 'total_open_est_rev',
      'trailing_6mo_roas', 'trailing_6mo_potential_roas', 'budget']) {
      if (r[key] !== null && r[key] !== undefined) r[key] = parseFloat(r[key]);
    }
    for (const key of ['quality_leads', 'actual_quality_leads', 'months_in_program',
      'total_insp_booked', 'on_cal_14d', 'sort_priority', 'flag_count']) {
      if (r[key] !== null && r[key] !== undefined) r[key] = parseInt(r[key]);
    }
    return r;
  });

  // 2. Get duration in current status from snapshots
  // Two durations: overall (for general display) and ads-only (for ads manager briefs)
  // Ads duration: Funnel Risk days count as "not in ads risk" (like Healthy)
  const durationResult = await pool.query(`
    WITH current_status AS (
      SELECT customer_id, status, risk_type FROM risk_status_snapshots
      WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM risk_status_snapshots)
    ),
    -- Overall duration: find last time they were Healthy for 4+ consecutive days
    smoothed_streak AS (
      SELECT cs.customer_id, cs.status, cs.risk_type,
        (
          SELECT MAX(gap_end) FROM (
            SELECT s2.snapshot_date as gap_end,
              (SELECT COUNT(*) FROM risk_status_snapshots s3
               WHERE s3.customer_id = cs.customer_id
                 AND s3.snapshot_date >= s2.snapshot_date
                 AND s3.snapshot_date < s2.snapshot_date + 4
                 AND s3.status = 'Healthy'
              ) as healthy_run
            FROM risk_status_snapshots s2
            WHERE s2.customer_id = cs.customer_id
              AND s2.status = 'Healthy'
              AND s2.snapshot_date < (SELECT MAX(snapshot_date) FROM risk_status_snapshots)
          ) gaps
          WHERE healthy_run >= 4
        ) as last_real_healthy,
        -- Ads-specific: last time they were NOT in ads risk for 4+ days
        -- (Healthy, Flag, or Funnel Risk only = not ads risk)
        (
          SELECT MAX(gap_end) FROM (
            SELECT s2.snapshot_date as gap_end,
              (SELECT COUNT(*) FROM risk_status_snapshots s3
               WHERE s3.customer_id = cs.customer_id
                 AND s3.snapshot_date >= s2.snapshot_date
                 AND s3.snapshot_date < s2.snapshot_date + 4
                 AND (s3.status IN ('Healthy', 'Flag') OR (s3.status = 'Risk' AND s3.risk_type = 'Funnel Risk'))
              ) as non_ads_risk_run
            FROM risk_status_snapshots s2
            WHERE s2.customer_id = cs.customer_id
              AND (s2.status IN ('Healthy', 'Flag') OR (s2.status = 'Risk' AND s2.risk_type = 'Funnel Risk'))
              AND s2.snapshot_date < (SELECT MAX(snapshot_date) FROM risk_status_snapshots)
          ) gaps
          WHERE non_ads_risk_run >= 4
        ) as last_non_ads_risk
      FROM current_status cs
    )
    SELECT customer_id, status, risk_type,
      CASE WHEN last_real_healthy IS NOT NULL
        THEN CURRENT_DATE - last_real_healthy
        ELSE CURRENT_DATE - (SELECT MIN(snapshot_date) FROM risk_status_snapshots WHERE customer_id = smoothed_streak.customer_id)
      END as days_in_status,
      CASE WHEN last_non_ads_risk IS NOT NULL
        THEN CURRENT_DATE - last_non_ads_risk
        ELSE CURRENT_DATE - (SELECT MIN(snapshot_date) FROM risk_status_snapshots WHERE customer_id = smoothed_streak.customer_id)
      END as days_in_ads_risk
    FROM smoothed_streak
    WHERE status IN ('Risk', 'Flag')
  `);

  const durationMap = {};
  const adsDurationMap = {};
  for (const r of durationResult.rows) {
    durationMap[r.customer_id] = parseInt(r.days_in_status);
    adsDurationMap[r.customer_id] = parseInt(r.days_in_ads_risk);
  }

  // 2b. Get status from 7 days ago for each client (to detect improvements)
  const priorResult = await pool.query(`
    SELECT customer_id, status, risk_type
    FROM risk_status_snapshots
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM risk_status_snapshots WHERE snapshot_date <= CURRENT_DATE - 7)
  `);
  const priorStatusMap = {};
  for (const r of priorResult.rows) {
    priorStatusMap[r.customer_id] = { status: r.status, risk_type: r.risk_type };
  }

  // 3. Group by manager
  const managers = {};
  for (const row of rows) {
    const mgr = row.ads_manager || 'Unassigned';
    if (!managers[mgr]) managers[mgr] = [];
    managers[mgr].push(row);
  }

  // 4. Build and send per-manager briefs
  const now = new Date();
  const thirtyAgo = new Date(now.getTime() - 30 * 86400000);
  const fmt = d => d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });

  for (const [manager, clients] of Object.entries(managers)) {
    // Skip if filtering to a specific manager
    if (managerFilter && manager !== managerFilter) continue;

    const channel = MANAGER_CHANNELS[manager];
    if (!channel) {
      console.log(`No channel configured for manager: ${manager} (${clients.length} clients)`);
      continue;
    }

    // Only show ads-actionable risk (Ads Risk or Both Risk — not Funnel Risk only)
    const riskClients = clients.filter(c => c.status === 'Risk' && c.risk_type !== 'Funnel Risk');
    const flagClients = clients.filter(c => c.status === 'Flag');
    const healthyClients = clients.filter(c => c.status === 'Healthy' || (c.status === 'Risk' && c.risk_type === 'Funnel Risk'));

    // Sort risk clients by urgency — who needs attention most?
    // Key insight: a client with low guarantee near program end is more urgent
    // than a client with high guarantee and a temporary CPL spike
    riskClients.sort((a, b) => {
      // Check manual priority overrides first
      const aName = clientDisplayName(a.client_name);
      const bName = clientDisplayName(b.client_name);
      const aOverride = PRIORITY_OVERRIDES[aName];
      const bOverride = PRIORITY_OVERRIDES[bName];
      if (aOverride && !bOverride) return -1;
      if (!aOverride && bOverride) return 1;
      if (aOverride && bOverride) return aOverride - bOverride;

      const aScore = riskUrgencyScore(a, adsDurationMap[a.customer_id] || durationMap[a.customer_id] || 0);
      const bScore = riskUrgencyScore(b, adsDurationMap[b.customer_id] || durationMap[b.customer_id] || 0);
      return bScore - aScore;
    });

    // Sort flag clients similarly
    flagClients.sort((a, b) => {
      const aScore = (a.flag_count || 0) * 5 + Math.min(durationMap[a.customer_id] || 0, 30);
      const bScore = (b.flag_count || 0) * 5 + Math.min(durationMap[b.customer_id] || 0, 30);
      return bScore - aScore;
    });

    let msg = `*YOUR WEEKLY CLIENT BRIEF*\n`;
    msg += `${fmt(thirtyAgo)} \u2013 ${fmt(now)}\n\n`;
    msg += `Portfolio: ${clients.length} clients\n`;
    msg += `:red_circle: Risk: ${riskClients.length} \u00b7 :large_yellow_circle: Flag: ${flagClients.length} \u00b7 :white_check_mark: Healthy: ${healthyClients.length}\n`;

    // Risk section
    if (riskClients.length > 0) {
      msg += `\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n`;
      msg += `*RISK*\n\n`;

      for (let i = 0; i < riskClients.length; i++) {
        const row = riskClients[i];
        const name = clientDisplayName(row.client_name);
        // Use ads-specific duration for ads managers (ignores funnel-only risk periods)
        const days = adsDurationMap[row.customer_id] || durationMap[row.customer_id] || 0;
        const durationStr = days > 0 ? ` \u2014 ${fmtDuration(days)} in ads risk` : '';
        const triggerCount = (row.risk_triggers ? row.risk_triggers.length : 0)
          + (row.flag_triggers ? row.flag_triggers.length : 0);

        // Fire scale based on urgency score — top client always gets most fire
        const urgency = riskUrgencyScore(row, days);
        let fire;
        if (i === 0) {
          fire = ':fire::fire::fire:';  // #1 priority always gets 3 fire
        } else if (urgency >= 60) {
          fire = ':fire::fire:';
        } else {
          fire = ':fire:';
        }

        msg += `${fire} *${name}* \u2014 ${row.risk_type}${durationStr}\n`;

        // Show risk triggers
        if (row.risk_triggers && row.risk_triggers.length) {
          for (const trigger of row.risk_triggers) {
            msg += `   ${trigger}\n`;
          }
        }
        msg += '\n';
      }
    }

    // Flag section
    if (flagClients.length > 0) {
      msg += `\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n`;
      msg += `*FLAG*\n\n`;

      for (const row of flagClients) {
        const name = clientDisplayName(row.client_name);
        const days = durationMap[row.customer_id] || 0;
        const durationStr = days > 0 ? ` \u2014 ${fmtDuration(days)} flagged` : '';
        const metrics = buildMetricsLine(row);

        msg += `:large_yellow_circle: *${name}*${durationStr}\n`;
        if (metrics) msg += `   ${metrics}\n`;
        msg += '\n';
      }
    }

    // Volume Alerts — clients with >30% volume drop (may be in Flag or Healthy now, not just Risk)
    const volumeAlerts = clients.filter(c =>
      c.lead_volume_change !== null && c.lead_volume_change < -0.3 && c.status !== 'Risk'
    );
    if (volumeAlerts.length > 0) {
      msg += `\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n`;
      msg += `*:chart_with_downwards_trend: VOLUME ALERTS*\n\n`;
      for (const row of volumeAlerts) {
        const name = clientDisplayName(row.client_name);
        const dropPct = Math.abs(Math.round(row.lead_volume_change * 100));
        const cplStr = row.cpl > 0 ? ` · CPL $${Math.round(row.cpl)}` : '';
        msg += `:warning: *${name}* — Vol \u2193${dropPct}%${cplStr}\n`;
      }
      msg += '\n';
    }

    // Healthy section
    if (healthyClients.length > 0) {
      msg += `\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n`;
      msg += `*HEALTHY (${healthyClients.length}):* `;
      msg += healthyClients.map(r => clientDisplayName(r.client_name)).join(', ');
      msg += '\n';
    }

    // Wins & improvements — compare to 7 days ago
    const statusRank = { 'Risk': 1, 'Flag': 2, 'Healthy': 3 };
    const wins = []; // moved to healthy
    const improvements = []; // improved but not yet healthy (risk→flag)

    for (const row of clients) {
      const prior = priorStatusMap[row.customer_id];
      if (!prior) continue;
      const name = clientDisplayName(row.client_name);
      const currentStatus = row.status;
      const priorStatus = prior.status;

      if (currentStatus === 'Healthy' && priorStatus !== 'Healthy') {
        wins.push({ name, from: priorStatus });
      } else if ((statusRank[currentStatus] || 0) > (statusRank[priorStatus] || 0) && currentStatus !== 'Healthy') {
        improvements.push({ name, from: priorStatus, to: currentStatus });
      }
    }

    if (wins.length > 0 || improvements.length > 0) {
      msg += `\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n`;
      msg += `*WINS THIS WEEK*\n\n`;
      for (const w of wins) {
        msg += `:goal_net: *${w.name}* moved to Healthy (was ${w.from})\n`;
      }
      for (const imp of improvements) {
        msg += `:chart_with_upwards_trend: *${imp.name}* improved: ${imp.from} \u2192 ${imp.to}\n`;
      }
    }

    if (testMode) {
      console.log(`\n${'='.repeat(50)}`);
      console.log(`Manager: ${manager} → ${channel}`);
      console.log(`${'='.repeat(50)}`);
      console.log(msg);
    } else if (previewMode) {
      // Preview mode: send all briefs as DM to Susie with manager label
      try {
        const previewMsg = `*[PREVIEW — ${manager}'s brief]*\n\n` + msg;
        await postToSlack(PREVIEW_CHANNEL, previewMsg);
        console.log(`Preview sent to Susie for ${manager}`);
      } catch (err) {
        console.error(`Error sending preview for ${manager}: ${err.message}`);
      }
    } else {
      try {
        await postToSlack(channel, msg);
        console.log(`Sent brief to ${manager} (${channel})`);
      } catch (err) {
        console.error(`Error sending to ${manager}: ${err.message}`);
      }
    }

    // Rate limit
    if (!testMode) await new Promise(r => setTimeout(r, 1000));
  }

  await pool.end();
  console.log('Done.');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
