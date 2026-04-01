// ============================================================
// CallRail Intelligence Service — Main Server
// ============================================================

const express = require('express');
const config = require('./config');
const callrailApi = require('./lib/callrail-api');
const { detectAnswerStatus } = require('./lib/answer-detector');
const { scoreCallLead } = require('./lib/lead-scorer');
const { scoreFormSubmission } = require('./lib/form-scorer');
const { startPoller, pollOnce, backfill, backfillDb } = require('./lib/poller');
const db = require('./lib/db');

const app = express();
app.use(express.json());

// ============================================================
// Health Check
// ============================================================
app.get('/', (req, res) => {
    res.json({
        service: 'callrail-intelligence',
        status: 'ok',
        version: '3.0.0',
        mode: 'batch-polling',
        accounts: config.CALLRAIL_ACCOUNTS.map(a => a.accountId),
    });
});

app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        mode: 'batch-polling',
        accounts: config.CALLRAIL_ACCOUNTS.length,
        timestamp: new Date().toISOString(),
    });
});

// ============================================================
// Manual Test Endpoint — Process a specific call by ID
// Requires accountId query param for multi-account support
// ============================================================
app.get('/test/call/:callId', async (req, res) => {
    try {
        // Use first account by default, or specify ?account=INDEX
        const accountIdx = parseInt(req.query.account || '0', 10);
        const account = config.CALLRAIL_ACCOUNTS[accountIdx];
        if (!account) {
            return res.status(400).json({ error: `No account at index ${accountIdx}. Available: ${config.CALLRAIL_ACCOUNTS.length}` });
        }

        const call = await callrailApi.getCall(account.accountId, account.apiKey, req.params.callId);
        const answerResult = detectAnswerStatus(call);
        call._answerResult = answerResult;
        const leadResult = await scoreCallLead(call);

        res.json({
            call_id: req.params.callId,
            account: account.accountId,
            company: call.company_name,
            duration: call.duration,
            source: call.source,
            answer_detection: answerResult,
            lead_scoring: leadResult,
            transcription_preview: (typeof call.transcription === 'string'
                ? call.transcription.substring(0, 500)
                : JSON.stringify(call.transcription)?.substring(0, 500)) || 'none',
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ============================================================
// Manual trigger — force a poll cycle
// ============================================================
app.post('/poll', async (req, res) => {
    res.json({ message: 'Poll triggered' });
    try {
        await pollOnce();
    } catch (err) {
        console.error('[Manual Poll] Error:', err.message);
    }
});

// ============================================================
// Backfill — re-score historical calls (default: 7 days)
// ============================================================
app.post('/backfill', async (req, res) => {
    const days = parseInt(req.query.days || '7', 10);
    res.json({ message: `Backfill triggered for ${days} days` });
    try {
        await backfill(days);
        console.log('[Backfill] Complete');
    } catch (err) {
        console.error('[Backfill] Error:', err.message);
    }
});

// ============================================================
// DB Backfill — process historical calls from database only
// Does NOT hit CallRail API — uses transcripts already in DB
// ============================================================
app.post('/backfill-db', async (req, res) => {
    const batch = parseInt(req.query.batch || '500', 10);
    const count = await db.getUnprocessedCount();
    res.json({ message: `DB backfill started — ${count} calls to process`, batch_size: batch });
    try {
        await backfillDb(batch);
        console.log('[DB Backfill] Complete');
    } catch (err) {
        console.error('[DB Backfill] Error:', err.message);
    }
});

// ============================================================
// GHL → CallRail Reverse Sync
// Receives feedback from GHL workflows when clients mark
// opportunities as lost (spam) or qualified (good lead).
//
// Expected payload from GHL webhook:
//   { phone: "+14146883406", action: "spam" | "qualified" }
//
// Loss reasons that map to "spam":
//   spam, not a lead, wrong area
// ============================================================
const SPAM_LOSS_REASONS = ['spam', 'not a lead', 'wrong area'];

app.post('/feedback', async (req, res) => {
    const { phone, action, loss_reason } = req.body;

    if (!phone) {
        return res.status(400).json({ error: 'Missing phone number' });
    }

    // Determine the lead status based on action or loss_reason
    let leadStatus;
    if (action === 'spam' || action === 'not_a_lead') {
        leadStatus = 'not_a_lead';
    } else if (action === 'qualified' || action === 'good_lead') {
        leadStatus = 'good_lead';
    } else if (loss_reason && SPAM_LOSS_REASONS.includes(loss_reason.toLowerCase())) {
        leadStatus = 'not_a_lead';
    } else {
        return res.status(400).json({ error: `Unknown action: ${action}. Use 'spam' or 'qualified'.` });
    }

    console.log(`\n[Feedback] Phone: ${phone} → ${leadStatus} (action: ${action || loss_reason})`);
    res.json({ received: true, phone, lead_status: leadStatus });

    // Search all accounts for calls from this phone number
    let totalUpdated = 0;

    for (const account of config.CALLRAIL_ACCOUNTS) {
        try {
            const calls = await callrailApi.searchCallsByPhone(
                account.accountId, account.apiKey, phone
            );

            if (calls.length === 0) continue;

            console.log(`[Feedback] Found ${calls.length} call(s) in account ${account.accountId}`);

            for (const call of calls) {
                // Update lead_status if it differs
                if (call.lead_status !== leadStatus) {
                    await callrailApi.updateCallLeadStatus(
                        account.accountId, account.apiKey, call.id, leadStatus
                    );
                    console.log(`[Feedback] ✓ Updated call ${call.id} → ${leadStatus}`);
                    totalUpdated++;
                } else {
                    console.log(`[Feedback] Call ${call.id} already ${leadStatus}, skipping`);
                }

                await new Promise(r => setTimeout(r, 300));
            }
        } catch (err) {
            console.error(`[Feedback] Error in account ${account.accountId}:`, err.message);
        }
    }

    console.log(`[Feedback] Done — updated ${totalUpdated} call(s)`);
});

// ============================================================
// Start Server + Poller
// ============================================================
app.listen(config.PORT, () => {
    console.log(`\n====================================`);
    console.log(`CallRail Intelligence Service v3.1`);
    console.log(`Mode: Batch Polling (every 5 min)`);
    console.log(`Accounts: ${config.CALLRAIL_ACCOUNTS.length}`);
    config.CALLRAIL_ACCOUNTS.forEach((a, i) => {
        console.log(`  [${i}] ${a.accountId}`);
    });
    console.log(`Port: ${config.PORT}`);
    console.log(`LLM: ${config.ANTHROPIC_MODEL}`);
    console.log(`====================================\n`);

    startPoller();
});
