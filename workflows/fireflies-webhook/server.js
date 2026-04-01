/**
 * Fireflies Webhook Receiver
 *
 * Receives Fireflies "Transcription complete" webhooks,
 * pulls the full transcript, matches to a client, generates
 * Slack summary + email draft, posts to Slack, creates Gmail draft,
 * and stores everything in PostgreSQL.
 */

const express = require('express');
const { Pool } = require('pg');
const { google } = require('googleapis');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3120;

// ── Config ──────────────────────────────────────────────────

const FIREFLIES_API_KEY = process.env.FIREFLIES_API_KEY || '';
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || '';
const SLACK_BOT_TOKEN = process.env.SLACK_BOT_TOKEN || '';
const WEBHOOK_SECRET = process.env.FIREFLIES_WEBHOOK_SECRET || '';

// Gmail OAuth (env vars or local files)
const fs = require('fs');
const path = require('path');
const GMAIL_CREDS_FILE = path.join(__dirname, '..', 'client-intelligence', 'gmail_credentials.json');
const GMAIL_TOKEN_FILE = path.join(__dirname, '..', 'client-intelligence', 'gmail_tokens', 'info@blueprintforscale.com.json');

function loadJson(envVar, filePath) {
  if (process.env[envVar]) return JSON.parse(process.env[envVar]);
  try { return JSON.parse(fs.readFileSync(filePath, 'utf8')); } catch { return null; }
}
const GMAIL_CREDENTIALS = loadJson('GMAIL_CREDENTIALS', GMAIL_CREDS_FILE);
const GMAIL_TOKEN = loadJson('GMAIL_TOKEN', GMAIL_TOKEN_FILE);
const GMAIL_SENDER = 'info@blueprintforscale.com';
const BLUEPRINT_DOMAINS = new Set(['blueprintforscale.com']);

const pool = new Pool(process.env.DATABASE_URL
  ? { connectionString: process.env.DATABASE_URL, ssl: false, max: 5, idleTimeoutMillis: 30000 }
  : { host: 'localhost', port: 5432, user: 'blueprint', database: 'blueprint', max: 5, idleTimeoutMillis: 30000 }
);

// ── Fireflies GraphQL ───────────────────────────────────────

async function firefliesQuery(query, variables = {}) {
  const resp = await fetch('https://api.fireflies.ai/graphql', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${FIREFLIES_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query, variables }),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Fireflies API ${resp.status}: ${text.slice(0, 200)}`);
  }

  const data = await resp.json();
  if (data.errors) {
    throw new Error(`GraphQL errors: ${JSON.stringify(data.errors)}`);
  }
  return data.data;
}

async function getTranscriptDetail(transcriptId) {
  const query = `
    query GetTranscript($transcriptId: String!) {
      transcript(id: $transcriptId) {
        id
        title
        date
        duration
        organizer_email
        participants
        speakers { id name }
        sentences { index text speaker_name start_time end_time }
        summary { overview action_items keywords short_summary }
      }
    }
  `;
  const data = await firefliesQuery(query, { transcriptId });
  return data?.transcript;
}

// ── Claude API ──────────────────────────────────────────────

async function claudeRequest(prompt, { model = 'claude-sonnet-4-20250514', maxTokens = 1000 } = {}) {
  const resp = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model,
      max_tokens: maxTokens,
      messages: [{ role: 'user', content: prompt }],
    }),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Claude API ${resp.status}: ${text.slice(0, 200)}`);
  }

  const data = await resp.json();
  return data.content[0].text;
}

// ── Client Matching ─────────────────────────────────────────

async function buildClientLookup() {
  const { rows: clients } = await pool.query(`
    SELECT c.customer_id, c.name, c.owner_email, c.contact_email,
           c.slack_channel_id
    FROM clients c
    WHERE c.status = 'active'
  `);

  const { rows: contacts } = await pool.query(`
    SELECT customer_id, name, email
    FROM client_contacts
    WHERE email IS NOT NULL
  `);

  // Email -> customer_id
  const emailMap = {};
  for (const c of clients) {
    for (const field of ['owner_email', 'contact_email']) {
      if (c[field]) emailMap[c[field].toLowerCase().trim()] = c.customer_id;
    }
  }
  for (const c of contacts) {
    if (c.email) emailMap[c.email.toLowerCase().trim()] = c.customer_id;
  }

  // Name fragments -> customer_id
  const nameMap = {};
  const skipWords = new Set(['pure', 'maintenance', 'mold', 'the', 'and', 'of']);
  for (const c of clients) {
    const parts = c.name.replace(/\|/g, ' ').split(/\s+/);
    for (const part of parts) {
      if (part.length > 3 && !skipWords.has(part.toLowerCase())) {
        nameMap[part.toLowerCase()] = c.customer_id;
      }
    }
  }

  return { clients, emailMap, nameMap };
}

function matchMeetingToClient(transcript, emailMap, nameMap) {
  // 1. Match by participant email
  for (const p of (transcript.participants || [])) {
    const email = String(p).toLowerCase().trim();
    if (emailMap[email]) return emailMap[email];
  }

  // 2. Match by title fragments
  const title = (transcript.title || '').toLowerCase();
  for (const [fragment, customerId] of Object.entries(nameMap)) {
    if (title.includes(fragment)) return customerId;
  }

  return null;
}

async function aiMatchClient(transcript, clients) {
  if (!ANTHROPIC_API_KEY) return null;

  const title = transcript.title || '';
  const participants = (transcript.participants || []).join(', ');
  const summary = transcript.summary?.overview || transcript.summary?.short_summary || '';
  const clientList = clients.map(c => `- ${c.name} (ID: ${c.customer_id})`).join('\n');

  const prompt = `Match this meeting to one of our clients. Return ONLY the customer_id number, or "unknown" if no match.

Meeting title: ${title}
Participants: ${participants}
Summary: ${summary.slice(0, 500)}

Our clients:
${clientList}

Return only the customer_id number or "unknown". Nothing else.`;

  try {
    const answer = await claudeRequest(prompt, { model: 'claude-haiku-4-5-20251001', maxTokens: 50 });
    const trimmed = answer.trim();
    if (trimmed !== 'unknown' && /^\d+$/.test(trimmed)) return trimmed;
  } catch (e) {
    console.error('AI match failed:', e.message);
  }
  return null;
}

// ── Store Meeting ───────────────────────────────────────────

async function storeMeeting(customerId, transcript) {
  const ffId = transcript.id;
  const title = transcript.title || '';

  // Parse date
  let meetingDate;
  const dateVal = transcript.date;
  if (typeof dateVal === 'number') {
    meetingDate = new Date(dateVal > 1e12 ? dateVal : dateVal * 1000);
  } else {
    meetingDate = new Date(dateVal || Date.now());
  }

  const attendees = (transcript.participants || []).map(String);
  const summaryData = transcript.summary || {};
  const summary = summaryData.overview || summaryData.short_summary || '';
  let actionItems = summaryData.action_items || '';
  if (Array.isArray(actionItems)) actionItems = actionItems.map(i => `- ${i}`).join('\n');

  // Build transcript text
  let transcriptText = null;
  if (transcript.sentences?.length) {
    transcriptText = transcript.sentences
      .map(s => `${s.speaker_name || 'Unknown'}: ${s.text || ''}`)
      .join('\n');
  }

  const result = await pool.query(`
    INSERT INTO client_interactions (
      customer_id, interaction_type, interaction_date,
      logged_by, attendees, summary, action_items,
      source, source_id, transcript
    ) VALUES ($1, 'meeting', $2, $3, $4, $5, $6, 'fireflies', $7, $8)
    ON CONFLICT (source, source_id) WHERE source_id IS NOT NULL
    DO UPDATE SET
      summary = EXCLUDED.summary,
      action_items = EXCLUDED.action_items,
      transcript = EXCLUDED.transcript,
      attendees = EXCLUDED.attendees,
      updated_at = NOW()
    RETURNING id
  `, [
    customerId,
    meetingDate,
    transcript.organizer_email,
    attendees.length ? attendees : null,
    summary ? summary.slice(0, 5000) : null,
    actionItems ? actionItems.slice(0, 5000) : null,
    `ff-${ffId}`,
    transcriptText ? transcriptText.slice(0, 50000) : null,
  ]);

  return result.rows[0]?.id;
}

// ── Extract Insights ────────────────────────────────────────

async function extractInsights(customerId, transcript) {
  if (!ANTHROPIC_API_KEY) return;

  let summary = transcript.summary?.overview || transcript.summary?.short_summary || '';
  // Fall back to transcript sentences if no summary available
  if (!summary && transcript.sentences?.length) {
    summary = transcript.sentences
      .map(s => `${s.speaker_name || 'Unknown'}: ${s.text || ''}`)
      .join('\n')
      .slice(0, 5000);
  }
  if (!summary) return;

  const { rows } = await pool.query('SELECT name FROM clients WHERE customer_id = $1', [customerId]);
  const clientName = rows[0]?.name || 'Unknown';

  const prompt = `Analyze this meeting summary for client "${clientName}" (mold remediation company).
Extract personal notes and sentiment. Return JSON only.

{
  "sentiment": "positive|neutral|negative|at_risk",
  "personal_notes": [
    {
      "note": "the personal/business detail",
      "category": "personal|preference|business_change|milestone"
    }
  ]
}

Only include items clearly stated. Return empty array if nothing found.

Meeting summary:
${summary.slice(0, 3000)}`;

  try {
    const content = await claudeRequest(prompt, { model: 'claude-haiku-4-5-20251001' });
    let parsed = content;
    if (parsed.includes('```')) {
      parsed = parsed.split('```json').pop().split('```')[0].trim();
    }
    const insights = JSON.parse(parsed);

    if (insights.sentiment) {
      await pool.query(
        `UPDATE client_interactions SET sentiment = $1 WHERE source = 'fireflies' AND source_id = $2`,
        [insights.sentiment, `ff-${transcript.id}`]
      );
    }

    for (const note of (insights.personal_notes || [])) {
      if (note.note) {
        await pool.query(
          `INSERT INTO client_personal_notes (customer_id, note, category, source, auto_extracted)
           VALUES ($1, $2, $3, 'fireflies', TRUE)`,
          [customerId, note.note, note.category || 'personal']
        );
      }
    }

    console.log(`  Extracted ${insights.personal_notes?.length || 0} notes, sentiment: ${insights.sentiment}`);
  } catch (e) {
    console.error('Insight extraction failed:', e.message);
  }
}

// ── Slack Summary ───────────────────────────────────────────

async function generateSlackSummary(meeting) {
  if (!ANTHROPIC_API_KEY) return formatFallback(meeting);

  const clientName = meeting.client_name;
  const parts = clientName.split(' | ');
  const bizName = parts.length > 1 ? parts[parts.length - 1].trim() : clientName;

  // Use transcript text (truncated) if that's what we have
  const summaryText = (meeting.summary || 'No summary available').slice(0, 8000);
  const isTranscript = summaryText.includes(': ') && summaryText.length > 2000;

  const prompt = `You are formatting a call summary for a Slack message. The call was with a Google Ads client named ${bizName}.

${isTranscript ? 'Here is the full transcript from the call (read it carefully and extract the key points):' : 'Here is the raw summary from the call:'}
${summaryText}

Here are the action items:
${meeting.action_items || 'No action items'}

The sentiment was marked as: ${meeting.sentiment || 'unknown'}

Format this into a scannable Slack message with these exact sections. Use Slack markdown (bold with *, not **). Keep it concise — each bullet should be one line. Focus on what matters to the team:

1. A sentiment emoji and one-line sentiment description
2. "What's on their mind" — the client's concerns, questions, or requests (2-4 bullets)
3. "Key takeaways" — major decisions or discussion points (2-4 bullets)
4. "Personal notes" — any biographical or personal details mentioned (family, travel, background). If none, skip this section.
5. "Our action items" — what Blueprint team needs to do (from the action items above)
6. "Client action items" — what the client needs to do (from the action items above)

Rules:
- Don't include stats/metrics unless they were a major talking point
- Use plain language, not marketing speak
- Keep the whole message under 15 lines
- Use Slack emoji where appropriate
- NEVER use em dashes (—). Use commas or periods instead.
- NEVER use "thrilled", "excited", "fantastic", "wonderful", "incredibly", "invaluable", "positioned perfectly"
- NEVER use "dive into", "leverage", "streamline", "spearhead"
- Write like a human taking notes, not an AI summarizing
- Return ONLY the formatted message, no preamble`;

  try {
    return await claudeRequest(prompt);
  } catch (e) {
    console.error('Slack summary generation failed:', e.message);
    return formatFallback(meeting);
  }
}

function formatFallback(meeting) {
  const emojiMap = { positive: '😊', neutral: '😐', at_risk: '⚠️', negative: '😟' };
  const emoji = emojiMap[meeting.sentiment] || '📞';
  let lines = [`${emoji} *Sentiment:* ${(meeting.sentiment || 'unknown').replace(/_/g, ' ')}`];
  if (meeting.summary) {
    lines.push('', '*Summary:*', meeting.summary.slice(0, 500));
  }
  if (meeting.action_items) {
    lines.push('', '*Action Items:*', meeting.action_items.slice(0, 500));
  }
  return lines.join('\n');
}

// ── Post to Slack ───────────────────────────────────────────

async function postToSlack(channelId, header, body) {
  if (!SLACK_BOT_TOKEN) {
    console.error('No SLACK_BOT_TOKEN');
    return false;
  }

  const resp = await fetch('https://slack.com/api/chat.postMessage', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      'Authorization': `Bearer ${SLACK_BOT_TOKEN}`,
    },
    body: JSON.stringify({
      channel: channelId,
      text: header,
      blocks: [
        { type: 'header', text: { type: 'plain_text', text: header, emoji: true } },
        { type: 'section', text: { type: 'mrkdwn', text: body } },
      ],
    }),
  });

  const result = await resp.json();
  if (!result.ok) {
    console.error('Slack error:', result.error);
    return false;
  }
  return true;
}

// ── Email Draft ─────────────────────────────────────────────

async function generateEmailBody(meeting) {
  if (!ANTHROPIC_API_KEY) return formatEmailFallback(meeting);

  const clientName = meeting.client_name;
  const parts = clientName.split(' | ');
  const bizName = parts.length > 1 ? parts[parts.length - 1].trim() : clientName;
  const ownerName = parts.length > 1 ? parts[0].trim() : '';

  // Parse greeting name
  let greetingName = 'there';
  if (ownerName) {
    const nameParts = ownerName.replace(/&/g, ',').split(',');
    const firstNames = nameParts.map(p => p.trim().split(/\s+/)[0]).filter(Boolean);
    if (firstNames.length > 1) {
      greetingName = firstNames.slice(0, -1).join(', ') + ' & ' + firstNames[firstNames.length - 1];
    } else if (firstNames.length === 1) {
      greetingName = firstNames[0];
    }
  }

  const summaryText = (meeting.summary || '').slice(0, 8000);
  const isTranscript = summaryText.includes(': ') && summaryText.length > 2000;

  const prompt = `Write a short, warm follow-up email after a client call. The client is ${bizName} (${ownerName}).

${isTranscript ? 'Here is the full transcript from the call (read it carefully and extract the key points):' : 'Here is the call summary:'}
${summaryText}

Here are the action items:
${meeting.action_items || ''}

Write the email body only (no subject line) in HTML format. Guidelines:
- Start with "Hi Everyone," (if multiple people) or "Hi ${greetingName}," (if one person)
- Keep it warm and professional but casual. Write like a real person texting a colleague, not a corporate template.
- 2-3 sentences of opening. Reference something specific from the call. Be genuine. Use casual emphasis like repeating letters ("hugeee", "sooo good") sparingly when it fits.
- A "<b>Key Things</b>" section (NO colon after header) with 3-4 short items using dashes. Bold the key phrase at the start of each item. Add brief context or reassurance (e.g. "we're on it!")
- An "<b>Our next steps</b>" section (NO colon) — what WE (Blueprint) are doing for them. Dashes. These are our commitments. Be specific with names if multiple team members are involved.
- A "<b>Your Next Steps:</b>" section (WITH colon) — things the CLIENT said they'd do or need to do. Dashes. Only include things actually discussed on the call. Frame as reminders of what they agreed to, not assignments. Add parenthetical names if a specific person owns the task. It's OK to add a light joke if the context fits naturally.
- An "<b>Opportunities for Growth</b>" section (NO colon) ONLY if revenue opportunities were discussed (open estimates, new service areas, upsells). Use dashes. Skip entirely if nothing relevant.
- End with "Talk soon!<br><br>Best,<br>Susie"
- If other team members were on the call and had notable contributions, add a "P.S. [Name] - " shoutout at the very end
- Keep the whole email under 250 words
- Use ONLY dashes (- ) for all list items, never <ul><li> bullets. Use <b> for bold headers and key phrases, <br> for line breaks.

CRITICAL STYLE RULES — the email must sound human, not AI-generated:
- NEVER use em dashes (—). Use commas or periods instead.
- NEVER use "I'm thrilled", "I'm excited", "fantastic", "wonderful", "incredibly", "invaluable"
- NEVER use "don't hesitate to reach out" or "please don't hesitate"
- NEVER use "dive into", "deep dive", "leverage", "streamline", "spearhead"
- NEVER use "positioned perfectly" or "well-positioned"
- Use short, punchy sentences. Mix in sentence fragments. Like this.
- Use contractions naturally (we'll, you're, that's)
- Use casual transitions ("Also", "Oh and", "One more thing")
- Sound like you actually remember the conversation, not like you're summarizing a doc
- It's ok to be brief. Shorter is better.

Return ONLY the HTML email body, no preamble.`;

  try {
    return await claudeRequest(prompt, { maxTokens: 800 });
  } catch (e) {
    console.error('Email generation failed:', e.message);
    return formatEmailFallback(meeting);
  }
}

function formatEmailFallback(meeting) {
  return `Hi,<br><br>Thanks for taking the time to chat today! Here's a quick recap:<br><br>${(meeting.summary || 'No summary available.').slice(0, 500)}<br><br>Action items:<br>${(meeting.action_items || 'None noted.').slice(0, 500)}<br><br>Let me know if I missed anything.<br><br>Best,<br>Susie`;
}

async function getGmailService() {
  if (!GMAIL_CREDENTIALS || !GMAIL_TOKEN) {
    console.log('Gmail credentials not configured');
    return null;
  }

  try {
    const { client_id, client_secret } = GMAIL_CREDENTIALS.installed || GMAIL_CREDENTIALS.web || {};
    const oauth2Client = new google.auth.OAuth2(client_id, client_secret);
    oauth2Client.setCredentials(GMAIL_TOKEN);

    // Refresh token if expired
    if (GMAIL_TOKEN.expiry && new Date(GMAIL_TOKEN.expiry) < new Date()) {
      console.log('Gmail token expired, refreshing...');
      const { credentials } = await oauth2Client.refreshAccessToken();
      oauth2Client.setCredentials(credentials);
      // Save refreshed token to disk if using file-based tokens
      if (!process.env.GMAIL_TOKEN && fs.existsSync(GMAIL_TOKEN_FILE)) {
        const updated = { ...GMAIL_TOKEN, token: credentials.access_token, expiry: credentials.expiry_date ? new Date(credentials.expiry_date).toISOString() : GMAIL_TOKEN.expiry };
        fs.writeFileSync(GMAIL_TOKEN_FILE, JSON.stringify(updated));
        console.log('Gmail token refreshed and saved');
      }
    }

    return google.gmail({ version: 'v1', auth: oauth2Client });
  } catch (e) {
    console.error('Gmail service error:', e.message);
    return null;
  }
}

async function createGmailDraft(gmail, toEmails, subject, body) {
  const raw = Buffer.from(
    `From: ${GMAIL_SENDER}\r\n` +
    `To: ${toEmails.join(', ')}\r\n` +
    `Subject: ${subject}\r\n` +
    `Content-Type: text/html; charset=utf-8\r\n\r\n` +
    body
  ).toString('base64url');

  try {
    const draft = await gmail.users.drafts.create({
      userId: 'me',
      requestBody: { message: { raw } },
    });
    return draft.data.id;
  } catch (e) {
    console.error('Gmail draft error:', e.message);
    return null;
  }
}

// ── Process a single transcript ─────────────────────────────

async function processTranscript(transcriptId) {
  console.log(`\nProcessing transcript: ${transcriptId}`);

  // 1. Pull full transcript from Fireflies
  const transcript = await getTranscriptDetail(transcriptId);
  if (!transcript) {
    console.error('Could not fetch transcript from Fireflies');
    return { success: false, reason: 'fetch_failed' };
  }

  console.log(`  Title: ${transcript.title}`);
  console.log(`  Participants: ${(transcript.participants || []).join(', ')}`);

  // 2. Check if already processed
  const { rows: existing } = await pool.query(
    `SELECT id, slack_posted_at FROM client_interactions WHERE source = 'fireflies' AND source_id = $1`,
    [`ff-${transcriptId}`]
  );
  if (existing.length && existing[0].slack_posted_at) {
    console.log('  Already processed and posted — skipping');
    return { success: true, reason: 'already_posted' };
  }

  // 3. Match to client
  const { clients, emailMap, nameMap } = await buildClientLookup();
  let customerId = matchMeetingToClient(transcript, emailMap, nameMap);

  if (!customerId) {
    customerId = await aiMatchClient(transcript, clients);
  }

  if (!customerId) {
    console.log('  Could not match to any client — skipping');
    return { success: false, reason: 'no_client_match' };
  }

  // Get client details
  const { rows: clientRows } = await pool.query(
    'SELECT name, slack_channel_id FROM clients WHERE customer_id = $1',
    [customerId]
  );
  const client = clientRows[0];
  if (!client) {
    console.log(`  Client ${customerId} not found in DB`);
    return { success: false, reason: 'client_not_found' };
  }

  console.log(`  Matched to: ${client.name}`);

  // 4. Store meeting in DB
  const interactionId = await storeMeeting(customerId, transcript);
  console.log(`  Stored interaction: ${interactionId}`);

  // 5. Extract insights (sentiment + personal notes)
  await extractInsights(customerId, transcript);

  // 6. Check for client attendees (skip internal meetings)
  const hasClientAttendee = (transcript.participants || []).some(p => {
    const email = String(p).toLowerCase();
    if (!email.includes('@')) return false;
    const domain = email.split('@')[1];
    return !BLUEPRINT_DOMAINS.has(domain);
  });

  if (!hasClientAttendee) {
    console.log('  Internal meeting (no client attendees) — storing but not posting');
    await pool.query(
      'UPDATE client_interactions SET slack_posted_at = NOW() WHERE source = $1 AND source_id = $2',
      ['fireflies', `ff-${transcriptId}`]
    );
    return { success: true, reason: 'internal_meeting' };
  }

  // 7. Build meeting object for summary generation
  const { rows: meetingRows } = await pool.query(
    `SELECT ci.*, c.name as client_name, c.slack_channel_id
     FROM client_interactions ci
     JOIN clients c ON c.customer_id = ci.customer_id
     WHERE ci.source = 'fireflies' AND ci.source_id = $1`,
    [`ff-${transcriptId}`]
  );
  const meeting = meetingRows[0];

  // If Fireflies summary was empty but we have the transcript, use it
  if (!meeting.summary && meeting.transcript) {
    console.log('  No Fireflies summary — generating from transcript text');
    meeting.summary = meeting.transcript;
  }

  // 8. Generate and post Slack summary
  const slackBody = await generateSlackSummary(meeting);
  await pool.query(
    'UPDATE client_interactions SET slack_summary = $1 WHERE id = $2',
    [slackBody, meeting.id]
  );

  const parts = client.name.split(' | ');
  const bizName = parts.length > 1 ? parts[parts.length - 1].trim() : client.name;
  const meetingDate = new Date(meeting.interaction_date);
  const dateStr = meetingDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });

  // Get team attendee names for header
  const teamNames = (meeting.attendees || [])
    .filter(a => String(a).includes('@blueprintforscale.com'))
    .map(a => String(a).split('@')[0].replace('.', ' '))
    .filter(n => !['info', 'jake'].includes(n.toLowerCase()))
    .map(n => n.split(' ')[0].charAt(0).toUpperCase() + n.split(' ')[0].slice(1));

  let header = `📞 Call with ${bizName} — ${dateStr}`;
  if (teamNames.length) header += ` · ${teamNames.slice(0, 3).join(', ')}`;

  if (client.slack_channel_id) {
    const posted = await postToSlack(client.slack_channel_id, header, slackBody);
    if (posted) {
      await pool.query(
        'UPDATE client_interactions SET slack_posted_at = NOW() WHERE id = $1',
        [meeting.id]
      );
      console.log('  ✓ Posted to Slack');
    } else {
      console.error('  ✗ Slack post failed');
    }
  } else {
    console.log('  No Slack channel configured for this client');
  }

  // 9. Generate and create email draft — owner_email first as primary To
  const ownerEmail = await pool.query(
    'SELECT owner_email FROM clients WHERE customer_id = $1', [customerId]
  ).then(r => (r.rows[0]?.owner_email || '').toLowerCase().trim());

  const otherEmails = (meeting.attendees || [])
    .map(a => String(a).toLowerCase().trim())
    .filter(a => a.includes('@') && !BLUEPRINT_DOMAINS.has(a.split('@')[1]) && a !== ownerEmail);

  const clientEmails = ownerEmail
    ? [ownerEmail, ...otherEmails.filter(e => e !== ownerEmail)]
    : otherEmails;

  if (clientEmails.length) {
    const emailBody = await generateEmailBody(meeting);
    await pool.query(
      'UPDATE client_interactions SET email_draft = $1 WHERE id = $2',
      [emailBody, meeting.id]
    );

    const gmail = await getGmailService();
    if (gmail) {
      const subject = `Today's Call - ${meetingDate.toLocaleDateString('en-US', { month: 'numeric', day: 'numeric' })}`;
      const draftId = await createGmailDraft(gmail, clientEmails, subject, emailBody);
      if (draftId) {
        console.log(`  ✓ Email draft created → ${clientEmails.join(', ')}`);
      } else {
        console.error('  ✗ Email draft failed');
      }
    } else {
      console.log('  Gmail not configured — draft stored in DB only');
    }
  } else {
    console.log('  No client emails — skipping email draft');
  }

  return { success: true, reason: 'processed', client: client.name };
}

// ── Webhook Endpoint ────────────────────────────────────────

app.post('/webhook/fireflies', async (req, res) => {
  console.log('\n═══ Fireflies webhook received ═══');
  console.log('Body:', JSON.stringify(req.body).slice(0, 500));

  // Respond immediately (Fireflies expects 200 quickly)
  res.status(200).json({ status: 'received' });

  // Process async
  try {
    const transcriptId = req.body.meetingId
      || req.body.meeting_id
      || req.body.data?.meetingId
      || req.body.data?.meeting_id
      || req.body.data?.transcriptId
      || req.body.transcriptId;

    if (!transcriptId) {
      console.error('No transcript ID in webhook payload');
      return;
    }

    // Delay — Fireflies may still be finalizing the transcript/summary
    await new Promise(r => setTimeout(r, 15000));

    const result = await processTranscript(transcriptId);
    console.log(`  Result: ${JSON.stringify(result)}`);
  } catch (e) {
    console.error('Webhook processing error:', e);
  }
});

// ── Manual trigger endpoint (for testing) ───────────────────

app.post('/process/:transcriptId', async (req, res) => {
  try {
    const result = await processTranscript(req.params.transcriptId);
    res.json(result);
  } catch (e) {
    console.error('Manual process error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ── Health check ────────────────────────────────────────────

app.get('/health', async (req, res) => {
  let dbOk = false;
  try {
    const client = await Promise.race([
      pool.query('SELECT 1'),
      new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 3000)),
    ]);
    dbOk = true;
  } catch (e) {
    console.error('DB health check failed:', e.message);
  }

  res.json({
    status: dbOk ? 'ok' : 'degraded',
    db: dbOk,
    fireflies: !!FIREFLIES_API_KEY,
    slack: !!SLACK_BOT_TOKEN,
    anthropic: !!ANTHROPIC_API_KEY,
    gmail: !!(GMAIL_CREDENTIALS && GMAIL_TOKEN),
  });
});

app.get('/ping', (req, res) => res.json({ status: 'ok' }));

// ── Start ───────────────────────────────────────────────────

app.listen(PORT, () => {
  console.log(`Fireflies webhook server listening on port ${PORT}`);
  console.log(`  Fireflies API: ${FIREFLIES_API_KEY ? '✓' : '✗'}`);
  console.log(`  Slack: ${SLACK_BOT_TOKEN ? '✓' : '✗'}`);
  console.log(`  Anthropic: ${ANTHROPIC_API_KEY ? '✓' : '✗'}`);
  console.log(`  Gmail: ${GMAIL_CREDENTIALS && GMAIL_TOKEN ? '✓' : '✗'}`);
  pool.query('SELECT 1').then(() => console.log('  DB: ✓')).catch(() => console.log('  DB: ✗'));
});
