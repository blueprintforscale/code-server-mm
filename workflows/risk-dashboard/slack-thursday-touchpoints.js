#!/usr/bin/env node
require('dotenv').config({ path: __dirname + '/.env' });
/**
 * Thursday Touchpoint Reminder — Posts to #client_notifications on Thursday afternoons (UT)
 *
 * Reminds the team to do weekly touchpoints for manual_risk clients.
 *
 * Usage:
 *   node slack-thursday-touchpoints.js            # Send to #client_notifications
 *   node slack-thursday-touchpoints.js --test      # Dry run (prints to console only)
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
const CLICKUP_API_KEY = process.env.CLICKUP_API_KEY || 'pk_50313409_WWF9IOF9PJP60BYRC3LSAME866GPBYDP';
const SUSIE_CLICKUP_ID = 50313409;
const CHANNEL = 'C09AZ1MCLN7'; // #client_notifications

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

function clientDisplayName(name) {
  return name.includes('|') ? name.split('|').pop().trim() : name;
}

function createClickUpTask(listId, name, description) {
  return new Promise((resolve, reject) => {
    // Due date: end of this week (Friday)
    const now = new Date();
    const friday = new Date(now);
    friday.setDate(now.getDate() + (5 - now.getDay() + 7) % 7);
    friday.setHours(17, 0, 0, 0);
    const dueDate = friday.getTime();

    const payload = JSON.stringify({
      name,
      description,
      assignees: [SUSIE_CLICKUP_ID],
      priority: 2, // 2 = High
      due_date: dueDate,
      due_date_time: true,
    });
    const options = {
      hostname: 'api.clickup.com',
      path: `/api/v2/list/${listId}/task`,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': CLICKUP_API_KEY,
        'Content-Length': Buffer.byteLength(payload),
      },
    };
    const req = https.request(options, (res) => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => {
        try {
          const data = JSON.parse(body);
          if (data.id) resolve(data);
          else reject(new Error(data.err || 'Unknown ClickUp error'));
        } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

async function main() {
  try {
    const args = process.argv.slice(2);
    const isTest = args.includes('--test');

    const { rows } = await pool.query(`
      SELECT c.customer_id, c.name, c.ads_manager, c.slack_channel_id, c.clickup_list_id,
        m.actual_quality_leads, m.cpl, m.roas, m.days_since_lead
      FROM clients c
      JOIN get_dashboard_metrics() m ON m.customer_id = c.customer_id
      WHERE c.manual_risk = TRUE AND c.status = 'active'
      ORDER BY c.name
    `);

    if (rows.length === 0) {
      console.log('No manual risk clients found.');
      return;
    }

    let msg = `:warning: *WEEKLY TOUCHPOINT REMINDER*\n\n`;
    msg += `The following ${rows.length} client${rows.length > 1 ? 's need' : ' needs'} their weekly touchpoint this week:\n\n`;

    for (const r of rows) {
      const name = clientDisplayName(r.name);
      const manager = r.ads_manager || '—';
      const metrics = [];
      if (r.actual_quality_leads !== null) metrics.push(`${r.actual_quality_leads} leads`);
      if (r.cpl !== null) metrics.push(`CPL $${Math.round(parseFloat(r.cpl))}`);
      if (r.roas !== null) metrics.push(`ROAS ${parseFloat(r.roas).toFixed(1)}x`);
      if (r.days_since_lead !== null) metrics.push(`${r.days_since_lead}d since last lead`);

      msg += `• *${name}* — ${manager}\n`;
      if (metrics.length) msg += `   ${metrics.join(' · ')}\n`;
    }

    msg += `\nPlease ensure each client gets a check-in call or message before end of week.`;

    if (isTest) {
      console.log('--- DRY RUN ---');
      console.log(msg);
      console.log('--- Per-client alerts ---');
      for (const r of rows) {
        if (r.slack_channel_id) {
          console.log(`[Slack ${r.slack_channel_id}] ${clientDisplayName(r.name)}: touchpoint reminder`);
        }
        if (r.clickup_list_id) {
          console.log(`[ClickUp ${r.clickup_list_id}] ${clientDisplayName(r.name)}: high-priority task → Susie`);
        }
      }
      console.log('---');
    } else {
      console.log(`Posting touchpoint reminder to ${CHANNEL}...`);
      await postToSlack(CHANNEL, msg);
      console.log('Sent!');

      // Post per-client alert to each client's Slack channel
      for (const r of rows) {
        if (!r.slack_channel_id) continue;
        const name = clientDisplayName(r.name);
        const manager = r.ads_manager || '—';
        const metrics = [];
        if (r.actual_quality_leads !== null) metrics.push(`${r.actual_quality_leads} leads`);
        if (r.cpl !== null) metrics.push(`CPL $${Math.round(parseFloat(r.cpl))}`);
        if (r.roas !== null) metrics.push(`ROAS ${parseFloat(r.roas).toFixed(1)}x`);
        if (r.days_since_lead !== null) metrics.push(`${r.days_since_lead}d since last lead`);

        let clientMsg = `:warning: *Weekly Touchpoint Reminder — ${name}*\n`;
        clientMsg += `Manager: ${manager}\n`;
        if (metrics.length) clientMsg += `30-day: ${metrics.join(' · ')}\n`;
        clientMsg += `\nThis client needs a check-in this week.`;

        try {
          await postToSlack(r.slack_channel_id, clientMsg);
          console.log(`  → Sent to ${name} channel (${r.slack_channel_id})`);
        } catch (err) {
          console.error(`  → Failed for ${name}: ${err.message}`);
        }

        // Create ClickUp task
        if (r.clickup_list_id) {
          const taskName = `Weekly touchpoint — ${name}`;
          const taskDesc = `Weekly check-in for ${name} (Needs TLC client).\nManager: ${manager}\n30-day: ${metrics.join(' · ')}\n\nEnsure client gets a call or message this week.`;
          try {
            const task = await createClickUpTask(r.clickup_list_id, taskName, taskDesc);
            console.log(`  → ClickUp task created for ${name} (${task.id})`);
          } catch (err) {
            console.error(`  → ClickUp failed for ${name}: ${err.message}`);
          }
        }
      }
    }

  } catch (err) {
    console.error('Error:', err.message);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();
