// ============================================================
// Poller — Batch processes recent calls and form submissions
// Iterates through ALL configured CallRail accounts.
// ============================================================

const config = require('../config');
const callrailApi = require('./callrail-api');
const { detectAnswerStatus } = require('./answer-detector');
const { scoreCallLead } = require('./lead-scorer');
const { scoreFormSubmission } = require('./form-scorer');
const db = require('./db');

const POLL_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes
const LOOKBACK_MINUTES = 15;
const BACKFILL_DAYS = 7; // For manual backfill

// Track processed IDs to avoid re-processing
const processedCalls = new Set();
const processedForms = new Set();
const MAX_TRACKED = 5000;

/**
 * Starts the polling loop.
 */
function startPoller() {
    const accountCount = config.CALLRAIL_ACCOUNTS.length;
    console.log(`[Poller] Starting — ${accountCount} account(s), polling every ${POLL_INTERVAL_MS / 1000}s`);

    if (accountCount === 0) {
        console.error('[Poller] ⚠ No accounts configured! Set CALLRAIL_ACCOUNTS env var.');
        return;
    }

    config.CALLRAIL_ACCOUNTS.forEach(a => {
        console.log(`[Poller]   Account: ${a.accountId}`);
    });

    // Run immediately on start
    pollOnce().catch(err => console.error('[Poller] Error on initial run:', err.message));

    // Then schedule
    setInterval(() => {
        pollOnce().catch(err => console.error('[Poller] Error:', err.message));
    }, POLL_INTERVAL_MS);
}

/**
 * Single poll cycle: process all accounts.
 */
async function pollOnce() {
    const now = new Date();
    const lookbackDate = new Date(now.getTime() - LOOKBACK_MINUTES * 60 * 1000);
    const startDate = lookbackDate.toISOString();
    const endDate = now.toISOString();

    console.log(`\n[Poller] ═══ Poll at ${now.toISOString()} ═══`);

    for (const account of config.CALLRAIL_ACCOUNTS) {
        console.log(`[Poller] --- Account ${account.accountId} ---`);
        await Promise.all([
            processCalls(account, startDate, endDate),
            processForms(account, startDate, endDate),
        ]);
    }

    trimSet(processedCalls, MAX_TRACKED);
    trimSet(processedForms, MAX_TRACKED);
}

/**
 * Fetch and process recent calls for one account.
 */
async function processCalls(account, startDate, endDate) {
    try {
        const calls = await callrailApi.listRecentCalls(account.accountId, account.apiKey, startDate, endDate);
        console.log(`[Poller] Found ${calls.length} calls`);

        let processed = 0;
        let skipped = 0;

        for (const call of calls) {
            const callId = call.id;

            if (processedCalls.has(callId)) {
                skipped++;
                continue;
            }

            // Skip if no transcription yet (will catch on next poll)
            const transcription = getTranscriptionText(call);
            if (!transcription && call.duration > 10) continue;

            // Skip if we already tagged this call (answered/missed/abandoned)
            if (hasOurTags(call)) {
                processedCalls.add(callId);
                skipped++;
                continue;
            }

            try {
                await processCall(account, call);
                processedCalls.add(callId);
                processed++;
            } catch (err) {
                console.error(`[Poller] Error processing call ${callId}:`, err.message);
            }

            await new Promise(r => setTimeout(r, 500));
        }

        console.log(`[Poller] Calls: ${processed} processed, ${skipped} skipped`);
    } catch (err) {
        console.error('[Poller] Error fetching calls:', err.message);
    }
}

/**
 * Process a single call.
 */
async function processCall(account, call) {
    const callId = call.id;
    const isFirstCall = call.first_call === true;
    console.log(`[Call ${callId}] ${call.company_name} | ${call.duration}s | ${call.source} | first_call=${isFirstCall}`);

    // Step 1: ALWAYS detect and tag answered/missed status
    const answerResult = detectAnswerStatus(call);
    console.log(`[Call ${callId}] → Tag: ${answerResult.tag} (${(answerResult.confidence * 100).toFixed(0)}%) — ${answerResult.reason}`);

    try {
        await callrailApi.addCallTag(account.accountId, account.apiKey, callId, answerResult.tag, call.tags);
        console.log(`[Call ${callId}] ✓ Tag "${answerResult.tag}" applied`);
    } catch (tagErr) {
        console.error(`[Call ${callId}] ✗ Failed to apply tag:`, tagErr.message);
    }

    // Write to database
    try {
        await db.updateAnswerStatus(callId, answerResult.tag, answerResult.reason, answerResult.confidence);
        console.log(`[Call ${callId}] ✓ DB updated`);
    } catch (dbErr) {
        console.error(`[Call ${callId}] ✗ DB write failed:`, dbErr.message);
    }

    // Step 2: If the call was missed/abandoned, don't set lead_status at all.
    // We have no conversation to evaluate — leave it blank so the callback
    // exception can still score them if they call back.
    if (answerResult.tag === 'missed' || answerResult.tag === 'abandoned') {
        console.log(`[Call ${callId}] Call was ${answerResult.tag} — skipping lead scoring (no conversation)`);
        return;
    }

    // Step 3: Decide whether to lead-score this answered call
    //
    // Score IF:
    //   a) First-time caller — always score
    //   b) Repeat caller — ONLY if the call has no lead_status yet
    //      (callback-after-missed scenario)
    //
    const shouldScore = isFirstCall || !call.lead_status;

    if (!shouldScore) {
        console.log(`[Call ${callId}] Skipping lead scoring (repeat caller, already scored)`);
        return;
    }

    if (!isFirstCall) {
        console.log(`[Call ${callId}] ★ Repeat caller exception — scoring callback (no prior lead_status)`);
    }

    call._answerResult = answerResult;
    const leadResult = await scoreCallLead(call);
    const leadStatus = leadResult.is_good_lead ? 'good_lead' : 'not_a_lead';
    console.log(`[Call ${callId}] → Lead: ${leadStatus} (${(leadResult.confidence * 100).toFixed(0)}%) — ${leadResult.reason}`);

    await callrailApi.updateCallLeadStatus(account.accountId, account.apiKey, callId, leadStatus);
    console.log(`[Call ${callId}] ✓ Done`);
}

/**
 * Fetch and process recent form submissions for one account.
 */
async function processForms(account, startDate, endDate) {
    try {
        const forms = await callrailApi.listRecentFormSubmissions(account.accountId, account.apiKey, startDate, endDate);
        console.log(`[Poller] Found ${forms.length} form submissions`);

        let processed = 0;
        let skipped = 0;

        for (const form of forms) {
            const formId = form.id;

            if (processedForms.has(formId)) {
                skipped++;
                continue;
            }

            if (form.lead_status) {
                processedForms.add(formId);
                skipped++;
                continue;
            }

            try {
                await processForm(account, form);
                processedForms.add(formId);
                processed++;
            } catch (err) {
                console.error(`[Poller] Error processing form ${formId}:`, err.message);
            }

            await new Promise(r => setTimeout(r, 500));
        }

        console.log(`[Poller] Forms: ${processed} processed, ${skipped} skipped`);
    } catch (err) {
        console.error('[Poller] Error fetching forms:', err.message);
    }
}

/**
 * Process a single form submission.
 */
async function processForm(account, formData) {
    const formId = formData.id;
    console.log(`[Form ${formId}] ${formData.company_name}`);

    const result = await scoreFormSubmission(formData);
    const leadStatus = result.is_good_lead ? 'good_lead' : 'not_a_lead';
    console.log(`[Form ${formId}] → ${leadStatus} (${(result.confidence * 100).toFixed(0)}%) — ${result.reason}`);

    await callrailApi.updateFormLeadStatus(account.accountId, account.apiKey, formId, leadStatus);
    console.log(`[Form ${formId}] ✓`);
}

// ============================================================
// Helpers
// ============================================================

function getTranscriptionText(call) {
    if (!call.transcription) return '';
    if (typeof call.transcription === 'string') return call.transcription;
    if (Array.isArray(call.transcription)) {
        return call.transcription.map(u => u.content || u.text || '').join(' ');
    }
    return '';
}

function hasOurTags(call) {
    if (!call.tags) return false;
    const tagNames = call.tags.map(t => typeof t === 'string' ? t.toLowerCase() : (t.name || '').toLowerCase());
    return tagNames.includes('answered') || tagNames.includes('missed') || tagNames.includes('abandoned');
}

function trimSet(set, maxSize) {
    if (set.size > maxSize) {
        const iterator = set.values();
        const toRemove = set.size - maxSize;
        for (let i = 0; i < toRemove; i++) {
            set.delete(iterator.next().value);
        }
    }
}

/**
 * Backfill: re-process all calls from the last N days.
 * Used to fix calls that were scored by old rules.
 */
async function backfill(days) {
    const now = new Date();
    const startDate = new Date(now.getTime() - (days || BACKFILL_DAYS) * 24 * 60 * 60 * 1000).toISOString();
    const endDate = now.toISOString();

    console.log(`\n[Backfill] ═══ Re-processing ${days || BACKFILL_DAYS} days of calls ═══`);

    for (const account of config.CALLRAIL_ACCOUNTS) {
        console.log(`[Backfill] --- Account ${account.accountId} ---`);
        // Clear processed set so we re-evaluate everything
        processedCalls.clear();
        processedForms.clear();
        await Promise.all([
            processCalls(account, startDate, endDate),
            processForms(account, startDate, endDate),
        ]);
    }
}

/**
 * DB-only backfill: run answer detection on calls already in the database
 * that have transcripts but no ai_answered value. Does NOT hit CallRail API.
 */
async function backfillDb(batchSize = 500) {
    const total = await db.getUnprocessedCount();
    console.log(`\n[DB Backfill] ═══ ${total} calls need processing ═══`);

    let processed = 0;
    while (true) {
        const calls = await db.getUnprocessedCalls(batchSize);
        if (calls.length === 0) break;

        for (const row of calls) {
            // Adapt DB row to look like a CallRail API call object
            const call = {
                id: row.callrail_id,
                transcription: row.transcript,
                duration: row.duration,
                call_type: row.call_type,
                voicemail: row.voicemail,
                speaker_percent: row.speaker_percent,
                answered: row.callrail_status === 'answered',
            };

            const result = detectAnswerStatus(call);
            await db.updateAnswerStatus(row.callrail_id, result.tag, result.reason, result.confidence);
            processed++;
        }

        console.log(`[DB Backfill] Processed ${processed}/${total}`);
    }

    console.log(`[DB Backfill] Complete — ${processed} calls updated`);
}

module.exports = { startPoller, pollOnce, backfill, backfillDb };
