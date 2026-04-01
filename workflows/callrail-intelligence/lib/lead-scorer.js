// ============================================================
// Lead Scorer — Anthropic Claude-based call lead scoring
// ============================================================

const Anthropic = require('@anthropic-ai/sdk');
const config = require('../config');

let anthropic;
function getClient() {
    if (!anthropic) {
        anthropic = new Anthropic({ apiKey: config.ANTHROPIC_API_KEY });
    }
    return anthropic;
}

/**
 * Scores a call as a good lead or not using Claude.
 * Falls back to keyword check if Anthropic is unavailable.
 *
 * @param {Object} call - CallRail call object with transcription
 * @returns {{ is_good_lead: boolean, confidence: number, reason: string }}
 */
async function scoreCallLead(call) {
    const transcription = getTranscriptionText(call);

    // If no transcription, can't score
    if (!transcription || transcription.trim().length < 10) {
        return {
            is_good_lead: false,
            confidence: 0.3,
            reason: 'No meaningful transcription available — cannot score',
        };
    }

    // If call was missed/abandoned, it's not a lead
    if (call._answerResult && call._answerResult.tag === 'missed') {
        return {
            is_good_lead: false,
            confidence: 0.9,
            reason: `Call was ${call._answerResult.reason}`,
        };
    }

    try {
        const client = getClient();

        const userMessage = `Analyze this call transcription and determine if the caller is a qualified lead for a mold remediation company.

Company: ${call.company_name || 'Unknown'}
Duration: ${call.duration || 0} seconds
Source: ${call.source || 'Unknown'}

Transcription:
${transcription.substring(0, 3000)}`;

        const response = await client.messages.create({
            model: config.ANTHROPIC_MODEL,
            max_tokens: 200,
            system: config.LEAD_SCORING_SYSTEM_PROMPT,
            messages: [
                { role: 'user', content: userMessage },
            ],
        });

        const content = response.content[0]?.text || '';

        // Parse response JSON
        try {
            // Extract JSON from response (handle markdown code blocks)
            const jsonMatch = content.match(/\{[\s\S]*\}/);
            const result = JSON.parse(jsonMatch ? jsonMatch[0] : content);
            return {
                is_good_lead: result.is_good_lead === true,
                confidence: typeof result.confidence === 'number' ? result.confidence : 0.5,
                reason: result.reason || 'Claude scored',
            };
        } catch (parseErr) {
            console.error('[LeadScorer] Failed to parse Claude response:', content);
            return {
                is_good_lead: false,
                confidence: 0.3,
                reason: 'Claude response could not be parsed',
            };
        }
    } catch (err) {
        console.error('[LeadScorer] Anthropic API error:', err.message);
        return fallbackKeywordScore(transcription);
    }
}

/**
 * Fallback keyword-based scoring if Claude is unavailable.
 */
function fallbackKeywordScore(transcription) {
    const lower = transcription.toLowerCase();

    const goodKeywords = [
        'mold', 'mildew', 'moisture', 'air quality', 'mycotoxin',
        'odor', 'musty', 'water damage', 'black mold',
        'inspection', 'testing', 'removal', 'remediation',
        'estimate', 'quote', 'pricing', 'appointment', 'schedule',
    ];

    const badKeywords = [
        'seo', 'marketing', 'website', 'google ranking',
        'business listing', 'wrong number', 'vendor',
    ];

    const goodCount = goodKeywords.filter(k => lower.includes(k)).length;
    const badCount = badKeywords.filter(k => lower.includes(k)).length;

    if (badCount > 0) {
        return { is_good_lead: false, confidence: 0.7, reason: 'Spam/solicitation keywords detected (fallback)' };
    }

    if (goodCount >= 2) {
        return { is_good_lead: true, confidence: 0.7, reason: `${goodCount} mold-related keywords found (fallback)` };
    }

    return { is_good_lead: false, confidence: 0.3, reason: 'Insufficient signals to classify (fallback)' };
}

/**
 * Extract transcription text from call object.
 */
function getTranscriptionText(call) {
    if (!call.transcription) return '';
    if (typeof call.transcription === 'string') return call.transcription;
    if (Array.isArray(call.transcription)) {
        return call.transcription.map(u => u.content || u.text || '').join(' ');
    }
    return '';
}

module.exports = { scoreCallLead };
