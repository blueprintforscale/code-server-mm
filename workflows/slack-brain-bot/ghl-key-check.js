#!/usr/bin/env node
/**
 * GHL API Key Health Check — runs daily via launchd
 * Tests every client's PIT token and alerts Slack when keys die.
 */

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });

const { Pool } = require('pg');
const https = require('https');

const pool = new Pool({
  host: 'localhost',
  port: 5432,
  user: 'blueprint',
  database: 'blueprint',
  max: 3,
  idleTimeoutMillis: 10000,
});

const SLACK_TOKEN = process.env.SLACK_BOT_TOKEN;
const ALERT_CHANNEL = 'C09AZ1MCLN7'; // #client_notifications

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

function testGhlKey(apiKey, locationId) {
  return new Promise((resolve) => {
    const options = {
      hostname: 'services.leadconnectorhq.com',
      path: `/locations/${locationId}`,
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Version': '2021-07-28',
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0',
      },
    };
    const req = https.request(options, (res) => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => resolve(res.statusCode === 200));
    });
    req.on('error', () => resolve(false));
    req.setTimeout(10000, () => { req.destroy(); resolve(false); });
    req.end();
  });
}

(async () => {
  try {
    const { rows } = await pool.query(`
      SELECT name, ghl_api_key, ghl_location_id
      FROM clients
      WHERE ghl_api_key IS NOT NULL
        AND ghl_location_id IS NOT NULL
        AND status = 'active'
      ORDER BY name
    `);

    console.log(`Testing ${rows.length} GHL API keys...`);

    const dead = [];
    const alive = [];

    for (const row of rows) {
      const ok = await testGhlKey(row.ghl_api_key, row.ghl_location_id);
      if (ok) {
        alive.push(row.name);
        console.log(`  ✓ ${row.name}`);
      } else {
        dead.push(row.name);
        console.log(`  ✗ ${row.name}`);
      }
    }

    console.log(`\nAlive: ${alive.length} | Dead: ${dead.length}`);

    if (dead.length > 0) {
      const msg = `:rotating_light: *GHL API Key Alert* — ${dead.length} expired key${dead.length > 1 ? 's' : ''}\n\n` +
        dead.map(n => `• ${n}`).join('\n') +
        `\n\n${alive.length} keys still working. Regenerate dead keys in each client's GHL sub-account → Settings → Private Integrations.`;

      await postToSlack(ALERT_CHANNEL, msg);
      console.log(`\nSlack alert sent to #client_notifications`);
    } else {
      console.log(`\nAll keys healthy — no alert needed.`);
    }
  } catch (err) {
    console.error('Error:', err.message);
  } finally {
    await pool.end();
  }
})();
