#!/usr/bin/env node
/**
 * Blueprint Brain — AI-powered Slack bot for client intelligence
 *
 * Features:
 * - @Blueprint Brain in any channel → answers questions with full database context
 * - Auto-detects client from channel (maps slack_channel_id → customer_id)
 * - DMs for cross-client questions ("which clients are at risk?")
 * - /dash slash command → instant client summary card
 *
 * Runs on Mac Mini via Socket Mode (no public URL needed).
 * Port: none (WebSocket connection to Slack)
 */

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });

const { App } = require('@slack/bolt');
const Anthropic = require('@anthropic-ai/sdk');
const { Pool } = require('pg');
const { TOOLS, executeToolQuery } = require('./tools');

// ── Config ───────────────────────────────────────────────────

const SLACK_BOT_TOKEN = process.env.SLACK_BOT_TOKEN;
const SLACK_APP_TOKEN = process.env.SLACK_APP_TOKEN; // xapp-... for Socket Mode
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const CLAUDE_MODEL = process.env.CLAUDE_MODEL || 'claude-haiku-4-5-20251001';

if (!SLACK_BOT_TOKEN || !SLACK_APP_TOKEN || !ANTHROPIC_API_KEY) {
  console.error('Missing required env vars: SLACK_BOT_TOKEN, SLACK_APP_TOKEN, ANTHROPIC_API_KEY');
  process.exit(1);
}

// ── Database ─────────────────────────────────────────────────

const pool = new Pool({
  host: 'localhost',
  port: 5432,
  user: 'blueprint',
  database: 'blueprint',
  max: 5,
  idleTimeoutMillis: 30000,
});

// ── Claude ───────────────────────────────────────────────────

const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

const SYSTEM_PROMPT = `You are Blueprint Brain, an AI assistant for Blueprint for Scale — a Google Ads agency managing ~30 mold remediation clients.

You help account managers (Martin, Luke, Nima, and Susie) get instant answers about clients, leads, performance, tasks, and relationship context.

IMPORTANT RULES:
- Be concise. This is Slack, not email. Use short paragraphs and bullet points.
- Use Slack formatting: *bold*, _italic_, \`code\`, > blockquotes
- When showing numbers, format them nicely: $4,200 not 4200, 3.2x not 3.2000
- Amounts from the database are sometimes in cents — divide by 100 when they look too large
- Phone normalization: strip non-digits, take last 10 digits
- ROAS = (inspection_revenue + GREATEST(treatment_revenue, approved_estimate_total)) / ad_spend
- GBP (Google Business Profile) leads are tracked but excluded from ROAS — no ad spend
- "Quality leads" = calls classified as lead (not spam, irrelevant, or brand)
- Risk status comes from get_dashboard_with_risk() function
- If a client is identified from the channel, you don't need to ask which client
- If someone asks about "my clients" or "my portfolio", use their Slack user to look up their manager name
- When you don't have data, say so clearly rather than guessing
- Never expose raw SQL or internal table names to users

BLUEPRINT METHODOLOGY — FUNNEL CONSTRAINT ANALYSIS:
This is our core framework. Every client's performance comes down to finding the #1 bottleneck in their funnel.

The funnel stages (in order):
1. *Lead Volume* — are they getting enough quality leads from Google Ads?
2. *Inspection Book Rate* — what % of leads convert to a booked inspection?
3. *Estimate Sent Rate* — what % of inspections result in an estimate being sent?
4. *Estimate Close Rate* — what % of estimates get approved?
5. *Pricing* — is their average job size competitive? Are they leaving money on the table?

How to diagnose the constraint (use 30-day data, use get_funnel_constraint tool):

HEALTHY RANGES (these are the thresholds):
- CPL (cost per lead): $50-$150 is healthy
- Inspection book rate (FREE inspections): 30-40% is healthy, 45-60% is best in class
- Inspection book rate (PAID inspections): 20%+ is healthy
- Estimate close rate (sent → approved): 1 in 4 or 1 in 5 (20-25%) is healthy, 1 in 3 or 1 in 2 is best in class
- ROAS: above 1.5x on 30-day is healthy (see risk thresholds for exact cutoffs)
- Check the client's inspection_type field to know if they do free or paid inspections

DIAGNOSIS LOGIC — diagnose by elimination, check each:

1. LEAD VOLUME CONSTRAINT (diagnosed by elimination):
   Lead volume is the constraint ONLY when everything else looks good.
   - Book rate is in healthy range ✓
   - Estimate close rate is in healthy range ✓
   - 30-day ROAS is above ~1.5x ✓
   - → Their business funnel is working well. They just need more leads in the top.
   - This is the GOOD problem. Tell the team: "funnel is healthy, they just need more volume"

2. INSPECTION BOOKING CONSTRAINT:
   - Book rate is below healthy range (below 30% for free, below 20% for paid)
   - ROAS is usually bad as a result
   - Root causes: not answering the phone (check call_answer_rate), slow follow-up, cherry-picking leads ("those aren't real leads"), receptionist issues, not returning missed calls
   - Common pattern: client says "the leads are bad" but really they're not picking up

3. ESTIMATE CLOSE RATE CONSTRAINT (very common):
   - Close rate is outside healthy range (worse than 1 in 5)
   - KEY GIVEAWAY: high *potential* ROAS with low *actual* ROAS — lots of outstanding estimates sitting there
   - Example: potential ROAS of 6x or 7x but actual ROAS of 1x = massive estimate close problem
   - total_open_est_rev being much larger than total_closed_rev is a dead giveaway
   - Root causes: pricing, poor follow-up after sending estimate, estimate presentation, homeowner shopping around

4. CPL / LEAD COST CONSTRAINT (ads team issue):
   - CPL is in risk range (way above $150)
   - This is OUR problem to fix (Blueprint's ads team), not the client's
   - When CPL is the constraint, tell the team TWO things:
     a. "We need to bring CPL down — that's on us"
     b. "Their funnel constraint is [X] — that's what we tell them to work on"
   - Always pair: what Blueprint is working on + what the client should work on

RECOMMENDED SOLUTIONS (what we tell clients):

For LEAD VOLUME constraint (funnel is healthy, just need more):
- Recommend increasing Google Ads spend by 20-40% per month
- This is the easiest conversation — their business is working, they just need more fuel

For INSPECTION BOOKING constraint:
- Send them the Blueprint phone script playbook — videos + scripts developed from listening to hundreds of lead intake calls
- Recommend hiring a call center or full-time dispatcher so missed calls don't tank the book rate
- Check call answer rate — if it's low, that's the smoking gun

For ESTIMATE CLOSE RATE constraint:
- Send them Josh's estimate closing training videos — based on how they close estimates in the UK
- Review their follow-up process after sending estimates
- Look at pricing vs market

COMMUNICATION FRAMEWORK:
- Always identify the PRIMARY constraint (one thing)
- If CPL is also an issue, mention it separately as "what we're working on"
- Frame it constructively: "The bottleneck is book rate at 18% — they're leaving leads on the table"
- Never just list all the bad numbers. Diagnose the ONE thing that would move the needle most.

IMPORTANT: When answering "how's the client doing?" — always mention their #1 constraint naturally. Not as a separate section, just weave it in: "Numbers are decent but the real bottleneck is their book rate at 19% — they're only getting on-site for 1 in 5 leads."

OPEN-ENDED "HOW ARE THEY DOING?" RULES:
- When someone asks "how's [client]?" or "how are they doing?" — answer BOTH dimensions:
  1. *Numbers* — leads, CPL, ROAS (30-day, Google Ads default)
  2. *Relationship* — are they happy with us? frustrated? checked out? Pull from sentiment, notes, interactions
- ALWAYS flag the discrepancy if numbers and relationship don't match:
  - Numbers good but client frustrated → "Numbers are solid but they're not happy — [reason]"
  - Numbers bad but client is chill → "Numbers are rough but they're patient — [context]"
  - Both bad → lead with that, it's urgent
- This is critical context for account managers preparing for calls

DEFAULT REPORTING RULES:
- Default time window is ROLLING 30 DAYS unless the user asks for something else
- Do NOT show all-time/lifetime stats unless specifically asked — they dilute the picture
- Default to Google Ads filtered numbers — 90% of questions are about paid performance
- If showing leads, ROAS, CPL, etc. filter to Google Ads source unless told otherwise
- Only mention GBP, organic, or LSA if the user asks, or if it's notably different from ads performance
- When someone asks "how are the numbers?" or "how's the client doing?" → 30-day Google Ads leads, CPL, ROAS, and risk status
- Month-to-date vs prior month comparison is fine as a secondary data point

CONVERSATION MEMORY:
- You have memory of recent messages in this conversation (last 30 minutes)
- If someone asks a follow-up like "is she happy?" after discussing a client, you know who "she" refers to
- Don't ask "which client?" if they just told you — check the conversation history

SENTIMENT & RELATIONSHIP QUESTIONS:
- When someone asks "is the client happy?", "how are things going?", "what's the vibe?" — use get_client_sentiment
- Synthesize across ALL sources: interaction sentiment scores, personal notes, Slack tone, CRM messages, alerts, risk status
- Be honest. If the data suggests tension, say so. If there's no sentiment data, say you don't have enough to judge
- Pull specific quotes or notes that reveal how the client feels ("In the last call, Jackie mentioned she was frustrated about...")
- Consider: lead volume trends, response times, tone in messages, personal notes about behavior patterns

SLACK ETIQUETTE:
- Reply in the main channel, NOT in a thread. The team keeps conversations flowing in the main channel.
- Only use threads if you're responding to something older that the conversation has moved past.
- This means: do NOT use thread_ts to reply. Just post directly to the channel.

PERSONALITY & LENGTH:
- You're part of the team. Funny, a little sarcastic, but always helpful.
- Occasional jokes and light roasting of the team is encouraged — keep it playful, never mean
- SHORT answers. 3-5 lines max for most questions. No headers, no sections, no walls of text.
- Think of how a witty coworker would answer in Slack — quick, useful, maybe a little funny
- If someone asks "how's Jackie doing?" → 2-3 sentences. NOT a full breakdown with headers.
- Save the detail for when someone asks "tell me more" or "break that down"
- Use bullets sparingly — only when listing 3+ distinct items
- No markdown headers (## or **Section:**) — just talk naturally
- Flag concerning things proactively ("heads up — no leads in 8 days")
- Use context from personal notes when relevant ("remember, Rob prefers texts over calls")

THE TEAM:
- *Susie* — Founder/CEO. Runs everything. Built all the systems you're powered by. If she's asking, it's important.
- *Martin* — Senior Account Manager. Manages the biggest portfolio. Been around the longest. Knows the clients deeply.
- *Luke* — Account Manager. Handles his own book of clients. Still growing into the role.
- *Nima* — Account Manager. Newest on the team. Learning the ropes.
- *Josh* — Ads strategist. Works on campaign optimization and search terms.
- *Kiana* — Operations/admin support.
- *Jake* — Contractor/developer. Helps with technical projects.`;

// ── Conversation Memory ──────────────────────────────────────
// Keeps recent messages per user/channel so Claude can follow up naturally

const conversationHistory = new Map(); // key: channelId or visitorDmId → messages[]
const MEMORY_TTL_MS = 30 * 60 * 1000; // 30 minutes
const MAX_HISTORY_MESSAGES = 20; // keep last 20 exchanges

function getConversationKey(channelId, userId) {
  return `${channelId}:${userId}`;
}

function getHistory(key) {
  const entry = conversationHistory.get(key);
  if (!entry) return [];
  // Expire stale conversations
  if (Date.now() - entry.lastActivity > MEMORY_TTL_MS) {
    conversationHistory.delete(key);
    return [];
  }
  return entry.messages;
}

function addToHistory(key, role, content) {
  if (!conversationHistory.has(key)) {
    conversationHistory.set(key, { messages: [], lastActivity: Date.now() });
  }
  const entry = conversationHistory.get(key);
  entry.lastActivity = Date.now();
  entry.messages.push({ role, content });
  // Trim to max
  if (entry.messages.length > MAX_HISTORY_MESSAGES) {
    entry.messages = entry.messages.slice(-MAX_HISTORY_MESSAGES);
  }
}

// Clean up stale conversations every 10 minutes
setInterval(() => {
  const now = Date.now();
  for (const [key, entry] of conversationHistory) {
    if (now - entry.lastActivity > MEMORY_TTL_MS) {
      conversationHistory.delete(key);
    }
  }
}, 10 * 60 * 1000);

// ── Active Conversation Tracker ──────────────────────────────
// Track channels where BB recently responded so it can continue without @mention
const activeConversations = new Map(); // key: "channelId:userId" → timestamp
const ACTIVE_CONVO_TTL_MS = 5 * 60 * 1000; // 5 minutes — if no follow-up in 5 min, require @mention again

function markConversationActive(channelId, userId) {
  activeConversations.set(`${channelId}:${userId}`, Date.now());
}

function isConversationActive(channelId, userId) {
  const key = `${channelId}:${userId}`;
  const lastActive = activeConversations.get(key);
  if (!lastActive) return false;
  if (Date.now() - lastActive > ACTIVE_CONVO_TTL_MS) {
    activeConversations.delete(key);
    return false;
  }
  return true;
}

// Clean up stale active conversations
setInterval(() => {
  const now = Date.now();
  for (const [key, ts] of activeConversations) {
    if (now - ts > ACTIVE_CONVO_TTL_MS) activeConversations.delete(key);
  }
}, 60 * 1000);

// ── Channel → Client Mapping ─────────────────────────────────

let channelClientMap = {};

async function loadChannelMap() {
  try {
    const { rows } = await pool.query(`
      SELECT cp.slack_channel_id, c.customer_id, c.name as client_name,
             cp.account_manager
      FROM client_profiles cp
      JOIN clients c ON c.customer_id = cp.customer_id
      WHERE cp.slack_channel_id IS NOT NULL
    `);
    channelClientMap = {};
    for (const row of rows) {
      channelClientMap[row.slack_channel_id] = {
        customerId: row.customer_id,
        clientName: row.client_name,
        manager: row.account_manager,
      };
    }
    console.log(`Loaded ${rows.length} channel→client mappings`);
  } catch (err) {
    console.error('Failed to load channel map:', err.message);
  }
}

// Refresh channel map every 30 minutes
setInterval(loadChannelMap, 30 * 60 * 1000);

// ── Slack User → Manager Mapping ─────────────────────────────

const SLACK_USER_MANAGERS = {
  'U06QCME7K5H': 'Martin',
  'U08LESST6LW': 'Luke',
  'U09NRKPQR9A': 'Nima',
  'U08C7TEGBJ4': 'Susie',
};

// ── Claude Tool Use Loop ─────────────────────────────────────

async function askClaude(question, channelContext, conversationKey) {
  // Build context-aware message
  let contextPrefix = '';
  if (channelContext) {
    contextPrefix = `[Context: This question is about client "${channelContext.clientName}" (customer_id: ${channelContext.customerId}). Manager: ${channelContext.manager || 'unknown'}]\n\n`;
  }

  // Build messages with conversation history for follow-up context
  const history = conversationKey ? getHistory(conversationKey) : [];
  const messages = [];

  // Add prior conversation turns (simplified — just user/assistant text)
  for (const msg of history) {
    messages.push({ role: msg.role, content: msg.content });
  }

  // ALWAYS prepend channel context so Claude knows which client, even in follow-ups
  const currentMessage = contextPrefix + question;
  messages.push({ role: 'user', content: currentMessage });

  // Tool use loop — Claude may call multiple tools before answering
  let response = await anthropic.messages.create({
    model: CLAUDE_MODEL,
    max_tokens: 800,
    system: SYSTEM_PROMPT,
    tools: TOOLS,
    messages,
  });

  // Iterate while Claude wants to use tools
  while (response.stop_reason === 'tool_use') {
    const assistantContent = response.content;
    messages.push({ role: 'assistant', content: assistantContent });

    // Process all tool calls
    const toolResults = [];
    for (const block of assistantContent) {
      if (block.type === 'tool_use') {
        console.log(`  Tool call: ${block.name}(${JSON.stringify(block.input)})`);
        try {
          const result = await executeToolQuery(pool, block.name, block.input);
          toolResults.push({
            type: 'tool_result',
            tool_use_id: block.id,
            content: JSON.stringify(result, null, 2),
          });
        } catch (err) {
          console.error(`  Tool error (${block.name}):`, err.message);
          toolResults.push({
            type: 'tool_result',
            tool_use_id: block.id,
            content: JSON.stringify({ error: err.message }),
            is_error: true,
          });
        }
      }
    }

    messages.push({ role: 'user', content: toolResults });

    response = await anthropic.messages.create({
      model: CLAUDE_MODEL,
      max_tokens: 800,
      system: SYSTEM_PROMPT,
      tools: TOOLS,
      messages,
    });
  }

  // Extract final text response
  const textBlocks = response.content.filter(b => b.type === 'text');
  return textBlocks.map(b => b.text).join('\n') || 'I wasn\'t able to generate a response. Try rephrasing your question.';
}

// ── Slack App ────────────────────────────────────────────────

const app = new App({
  token: SLACK_BOT_TOKEN,
  appToken: SLACK_APP_TOKEN,
  socketMode: true,
});

// Handle @mentions in any channel
app.event('app_mention', async ({ event, say }) => {
  const channelContext = channelClientMap[event.channel] || null;
  const userId = event.user;
  const managerName = SLACK_USER_MANAGERS[userId];

  // Strip the bot mention from the text
  const question = event.text.replace(/<@[A-Z0-9]+>/g, '').trim();

  if (!question) {
    await say({
      text: 'Hey! Ask me anything about a client — leads, ROAS, tasks, risk status, or just "what\'s going on with [client name]?"',
      thread_ts: event.ts,
    });
    return;
  }

  // Add manager context if known
  let enrichedQuestion = question;
  if (managerName) {
    enrichedQuestion = `[Asked by: ${managerName}] ${question}`;
  }

  const convKey = getConversationKey(event.channel, userId);
  console.log(`\n[${new Date().toISOString()}] @mention from ${managerName || userId} in ${channelContext?.clientName || event.channel}: "${question}"`);
  console.log(`  Channel ID: ${event.channel} | Mapped: ${channelContext ? 'YES → ' + channelContext.clientName : 'NO'}`);

  try {
    // Show typing indicator — post in main channel, not as a thread reply
    const thinking = await say({
      text: ':brain: Thinking...',
    });

    // Save user message to history
    addToHistory(convKey, 'user', enrichedQuestion);

    const answer = await askClaude(enrichedQuestion, channelContext, convKey);

    // Save assistant response to history
    addToHistory(convKey, 'assistant', answer);

    // Mark this conversation as active so follow-ups don't need @mention
    markConversationActive(event.channel, userId);

    // Update the thinking message with the real answer
    await app.client.chat.update({
      token: SLACK_BOT_TOKEN,
      channel: event.channel,
      ts: thinking.ts,
      text: answer,
    });
  } catch (err) {
    console.error('Error handling mention:', err);
    await say({
      text: `Something went wrong: ${err.message}. Try again in a moment.`,
    });
  }
});

// Handle channel messages (follow-ups without @mention)
app.event('message', async ({ event, say }) => {
  // Skip DMs (handled separately), bot messages, edits, and messages that are @mentions
  if (event.channel_type === 'im' || event.bot_id || event.subtype) return;
  if (event.text && event.text.includes(`<@`)) return; // Has an @mention — let app_mention handle it

  const userId = event.user;
  if (!userId) return;

  // Only respond if we have an active conversation with this user in this channel
  if (!isConversationActive(event.channel, userId)) return;

  const channelContext = channelClientMap[event.channel] || null;
  const managerName = SLACK_USER_MANAGERS[userId];
  const question = event.text;

  if (!question || question.length < 2) return;

  let enrichedQuestion = question;
  if (managerName) {
    enrichedQuestion = `[Asked by: ${managerName}] ${question}`;
  }

  const convKey = getConversationKey(event.channel, userId);
  console.log(`\n[${new Date().toISOString()}] Follow-up from ${managerName || userId} in ${channelContext?.clientName || event.channel}: "${question}"`);

  try {
    const thinking = await say({ text: ':brain: Thinking...' });

    addToHistory(convKey, 'user', enrichedQuestion);
    const answer = await askClaude(enrichedQuestion, channelContext, convKey);
    addToHistory(convKey, 'assistant', answer);

    // Keep the conversation active
    markConversationActive(event.channel, userId);

    await app.client.chat.update({
      token: SLACK_BOT_TOKEN,
      channel: event.channel,
      ts: thinking.ts,
      text: answer,
    });
  } catch (err) {
    console.error('Error handling follow-up:', err);
    await say({ text: `Something went wrong: ${err.message}` });
  }
});

// Handle DMs
app.event('message', async ({ event, say }) => {
  // Only respond to DMs (channel type 'im'), not bot messages, not edits
  if (event.channel_type !== 'im' || event.bot_id || event.subtype) return;

  const userId = event.user;
  const managerName = SLACK_USER_MANAGERS[userId];
  const question = event.text;

  if (!question) return;

  let enrichedQuestion = question;
  if (managerName) {
    enrichedQuestion = `[Asked by: ${managerName}] ${question}`;
  }

  const convKey = getConversationKey(event.channel, userId);
  console.log(`\n[${new Date().toISOString()}] DM from ${managerName || userId}: "${question}"`);

  try {
    const thinking = await say({ text: ':brain: Thinking...' });

    // Save user message to history
    addToHistory(convKey, 'user', enrichedQuestion);

    const answer = await askClaude(enrichedQuestion, null, convKey);

    // Save assistant response to history
    addToHistory(convKey, 'assistant', answer);

    await app.client.chat.update({
      token: SLACK_BOT_TOKEN,
      channel: event.channel,
      ts: thinking.ts,
      text: answer,
    });
  } catch (err) {
    console.error('Error handling DM:', err);
    await say(`Something went wrong: ${err.message}`);
  }
});

// /dash slash command — instant client summary
app.command('/dash', async ({ command, ack, respond }) => {
  await ack();

  const channelContext = channelClientMap[command.channel_id] || null;
  const clientNameArg = command.text?.trim();
  const userId = command.user_id;
  const managerName = SLACK_USER_MANAGERS[userId];

  // Determine which client
  let targetClient = clientNameArg || channelContext?.clientName;

  if (!targetClient) {
    await respond({
      response_type: 'ephemeral', // Only visible to the person who ran it
      text: 'Usage: `/dash` (in a client channel) or `/dash Fisher` (from anywhere)',
    });
    return;
  }

  console.log(`\n[${new Date().toISOString()}] /dash from ${managerName || userId}: "${targetClient}"`);

  try {
    await respond({
      response_type: 'in_channel',
      text: `:brain: Pulling up ${targetClient}...`,
    });

    const question = `Give me a quick dashboard summary for ${targetClient}. Include: leads this month vs last, ROAS, risk status, any active alerts, overdue tasks, and last interaction. Format as a compact Slack card.`;

    const context = channelContext || { clientName: targetClient };
    const answer = await askClaude(question, context);

    await respond({
      response_type: 'in_channel',
      replace_original: true,
      text: answer,
    });
  } catch (err) {
    console.error('Error handling /dash:', err);
    await respond({
      response_type: 'ephemeral',
      text: `Error: ${err.message}`,
    });
  }
});

// ── Startup ──────────────────────────────────────────────────

(async () => {
  await loadChannelMap();
  await app.start();
  console.log(`\nBlueprint Brain is online!`);
  console.log(`  Model: ${CLAUDE_MODEL}`);
  console.log(`  Channels mapped: ${Object.keys(channelClientMap).length}`);
  console.log(`  Listening for @mentions and DMs...\n`);
})();
