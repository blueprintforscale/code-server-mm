#!/usr/bin/env node
require('dotenv').config({ path: __dirname + '/.env' });
/**
 * Slack Reaction Logger — Detects 📞 and ☎️ reactions in client channels
 *
 * When a team member reacts to their own message with:
 *   📞 (telephone_receiver) → logs as successful call
 *   ☎️ (telephone) → logs as call attempt
 *
 * The message text becomes the call summary.
 * Runs every 30 min via launchd.
 *
 * Usage:
 *   node slack-reaction-logger.js          # Process recent reactions
 *   node slack-reaction-logger.js --test   # Dry run
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
const CALL_EMOJI = 'telephone_receiver';    // 📞
const ATTEMPT_EMOJI = 'telephone';          // ☎️
const LOOKBACK_HOURS = 2;

// ── Slack API ──────────────────────────────────────────────

function slackApi(method, params = {}) {
  return new Promise((resolve, reject) => {
    const qs = Object.entries(params)
      .map(([k, v]) => `${k}=${encodeURIComponent(v)}`)
      .join('&');
    const path = `/api/${method}?${qs}`;
    const options = {
      hostname: 'slack.com',
      path,
      method: 'GET',
      headers: { 'Authorization': `Bearer ${SLACK_TOKEN}` },
    };
    const req = https.request(options, (res) => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => {
        try {
          const data = JSON.parse(body);
          resolve(data);
        } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

async function getChannelHistory(channelId, oldest) {
  const data = await slackApi('conversations.history', {
    channel: channelId,
    oldest: oldest.toString(),
    limit: '100',
    inclusive: 'true',
  });
  return data.ok ? (data.messages || []) : [];
}

async function getReactions(channelId, timestamp) {
  const data = await slackApi('reactions.get', {
    channel: channelId,
    timestamp,
    full: 'true',
  });
  return data.ok ? (data.message?.reactions || []) : [];
}

// ── Main ───────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const lookbackMs = LOOKBACK_HOURS * 3600 * 1000;
  const oldest = ((Date.now() - lookbackMs) / 1000).toFixed(6);

  try {
    // Get all client channels
    const { rows: clients } = await pool.query(`
      SELECT customer_id, name, slack_channel_id
      FROM clients
      WHERE status = 'active' AND slack_channel_id IS NOT NULL AND slack_channel_id != ''
    `);

    console.log(`Checking ${clients.length} client channels for call reactions...`);
    let logged = 0;
    let skipped = 0;

    for (const client of clients) {
      const channelId = client.slack_channel_id;
      let messages;
      try {
        messages = await getChannelHistory(channelId, oldest);
      } catch (e) {
        // Channel might be archived or bot not invited
        continue;
      }

      for (const msg of messages) {
        if (!msg.reactions) continue;

        const hasCall = msg.reactions.some(r => r.name === CALL_EMOJI);
        const hasAttempt = msg.reactions.some(r => r.name === ATTEMPT_EMOJI);
        if (!hasCall && !hasAttempt) continue;

        const interactionType = hasCall ? 'call' : 'call_attempt';
        const sourceId = `slack-reaction-${channelId}-${msg.ts}`;

        // Check if already logged
        const { rows: existing } = await pool.query(
          `SELECT 1 FROM client_interactions WHERE source_id = $1`,
          [sourceId]
        );
        if (existing.length > 0) { skipped++; continue; }

        // Get the message text as summary
        const summary = (msg.text || '').slice(0, 500) || 'Call logged via Slack reaction';
        const msgDate = new Date(parseFloat(msg.ts) * 1000);

        // Find who reacted (the person who made the call)
        const callReaction = msg.reactions.find(r => r.name === (hasCall ? CALL_EMOJI : ATTEMPT_EMOJI));
        const reactorId = callReaction?.users?.[0] || msg.user;

        if (isTest) {
          const clientName = client.name.includes('|') ? client.name.split('|').pop().trim() : client.name;
          console.log(`  [${interactionType}] ${clientName} — ${msgDate.toLocaleDateString()} — "${summary.slice(0, 60)}..."`);
        } else {
          await pool.query(`
            INSERT INTO client_interactions (customer_id, interaction_type, interaction_date, source, summary, logged_by, source_id)
            VALUES ($1, $2, $3, 'slack_reaction', $4, $5, $6)
          `, [client.customer_id, interactionType, msgDate, summary, reactorId || 'unknown', sourceId]);
        }
        logged++;
      }

      // Rate limit
      await new Promise(r => setTimeout(r, 200));
    }

    console.log(`Done. ${logged} new contacts logged, ${skipped} already processed.`);

  } catch (err) {
    console.error('Error:', err.message);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();
