// ============================================================
// Form Scorer — Anthropic Claude-based form scoring + spam detection
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
 * Scores a form submission as a good lead or spam.
 * Quick spam patterns first, then Claude for nuanced scoring.
 *
 * @param {Object} formData - Form submission data from CallRail
 * @returns {{ is_good_lead: boolean, confidence: number, reason: string }}
 */
async function scoreFormSubmission(formData) {
    // Step 1: Quick spam pattern check
    const spamResult = checkSpamPatterns(formData);
    if (spamResult) return spamResult;

    // Step 2: Claude scoring
    try {
        const client = getClient();
        const fields = formatFormFields(formData);

        const userMessage = `Analyze this form submission and determine if it's a legitimate lead for a mold remediation company.

Company: ${formData.company_name || 'Unknown'}
Source: ${formData.source || 'Unknown'}

Form Fields:
${fields}`;

        const response = await client.messages.create({
            model: config.ANTHROPIC_MODEL,
            max_tokens: 200,
            system: config.FORM_SCORING_SYSTEM_PROMPT,
            messages: [
                { role: 'user', content: userMessage },
            ],
        });

        const content = response.content[0]?.text || '';

        try {
            const jsonMatch = content.match(/\{[\s\S]*\}/);
            const result = JSON.parse(jsonMatch ? jsonMatch[0] : content);
            return {
                is_good_lead: result.is_good_lead === true,
                confidence: typeof result.confidence === 'number' ? result.confidence : 0.5,
                reason: result.reason || 'Claude scored',
            };
        } catch (parseErr) {
            console.error('[FormScorer] Failed to parse Claude response:', content);
            return { is_good_lead: true, confidence: 0.3, reason: 'Could not parse Claude — defaulting to lead' };
        }
    } catch (err) {
        console.error('[FormScorer] Anthropic API error:', err.message);
        return fallbackFormScore(formData);
    }
}

/**
 * Quick spam pattern check — catches obvious spam before hitting Claude.
 */
function checkSpamPatterns(formData) {
    const allText = getAllFormText(formData);

    for (const pattern of config.FORM_SPAM_PATTERNS) {
        if (pattern.test(allText)) {
            return {
                is_good_lead: false,
                confidence: 0.9,
                reason: `Spam pattern detected: ${pattern}`,
            };
        }
    }

    const name = (formData.first_name || '') + (formData.last_name || '') + (formData.person_name || '');
    if (name && config.GIBBERISH_NAME_PATTERN.test(name.trim())) {
        return {
            is_good_lead: false,
            confidence: 0.85,
            reason: `Gibberish name detected: "${name.trim()}"`,
        };
    }

    return null;
}

/**
 * Fallback scoring without Claude.
 */
function fallbackFormScore(formData) {
    const allText = getAllFormText(formData).toLowerCase();

    const goodKeywords = [
        'mold', 'mildew', 'moisture', 'air quality', 'mycotoxin',
        'odor', 'musty', 'water damage', 'inspection', 'testing',
        'removal', 'remediation',
    ];

    const goodCount = goodKeywords.filter(k => allText.includes(k)).length;

    if (goodCount >= 1) {
        return { is_good_lead: true, confidence: 0.7, reason: 'Mold-related keywords found (fallback)' };
    }

    return { is_good_lead: true, confidence: 0.4, reason: 'No spam detected, defaulting to lead (fallback)' };
}

/**
 * Concatenate all form text for pattern matching.
 */
function getAllFormText(formData) {
    const fields = formData.form_data || formData.fields || {};
    const parts = [];

    if (formData.first_name) parts.push(formData.first_name);
    if (formData.last_name) parts.push(formData.last_name);
    if (formData.email) parts.push(formData.email);
    if (formData.person_name) parts.push(formData.person_name);

    if (typeof fields === 'object') {
        for (const [key, value] of Object.entries(fields)) {
            if (typeof value === 'string') parts.push(value);
        }
    }

    return parts.join(' ');
}

/**
 * Format form fields for Claude prompt.
 */
function formatFormFields(formData) {
    const lines = [];

    if (formData.first_name) lines.push(`First Name: ${formData.first_name}`);
    if (formData.last_name) lines.push(`Last Name: ${formData.last_name}`);
    if (formData.person_name) lines.push(`Name: ${formData.person_name}`);
    if (formData.email) lines.push(`Email: ${formData.email}`);
    if (formData.phone_number) lines.push(`Phone: ${formData.phone_number}`);

    const fields = formData.form_data || formData.fields || {};
    if (typeof fields === 'object') {
        for (const [key, value] of Object.entries(fields)) {
            if (typeof value === 'string' && value.trim()) {
                lines.push(`${key}: ${value}`);
            }
        }
    }

    return lines.join('\n') || 'No form fields available';
}

module.exports = { scoreFormSubmission };
