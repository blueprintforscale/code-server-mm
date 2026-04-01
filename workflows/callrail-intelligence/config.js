// ============================================================
// Configuration for CallRail Intelligence Service
// ============================================================

/**
 * Parse CALLRAIL_ACCOUNTS env var: "accountId:apiKey,accountId:apiKey"
 */
function parseAccounts(str) {
  if (!str) return [];
  return str.split(',').map(entry => {
    const [accountId, apiKey] = entry.trim().split(':');
    return { accountId: accountId.trim(), apiKey: apiKey.trim() };
  }).filter(a => a.accountId && a.apiKey);
}

module.exports = {
  // Server
  PORT: process.env.PORT || 3000,

  // CallRail — supports multiple accounts
  // Format: "accountId:apiKey,accountId:apiKey"
  CALLRAIL_ACCOUNTS: parseAccounts(process.env.CALLRAIL_ACCOUNTS || ''),

  // Anthropic (for lead scoring)
  ANTHROPIC_API_KEY: process.env.ANTHROPIC_API_KEY || '',
  ANTHROPIC_MODEL: process.env.ANTHROPIC_MODEL || 'claude-haiku-4-5-20251001',

  // Webhook secret (optional — for validating CallRail webhook signatures)
  WEBHOOK_SECRET: process.env.WEBHOOK_SECRET || '',

  // Business hours (for reference, not used in this service directly)
  BIZ_HOUR_START: 8,
  BIZ_HOUR_END: 18,

  // ============================================================
  // Answer Detection Keywords
  // ============================================================
  VOICEMAIL_KEYWORDS: [
    'leave a message',
    'leave your message',
    'at the tone',
    'after the tone',
    'after the beep',
    'you\'ve reached',
    'you have reached',
    'no one is available',
    'not available to take your call',
    'unavailable to take your call',
    'please leave your name',
    'we\'ll get back to you',
    'we will get back to you',
    'return your call',
    'mailbox is full',
    'voicemail',
  ],

  GREETING_KEYWORDS: [
    'thank you for calling',
    'thanks for calling',
    'your call is very important',
    'your call is important',
    'please stay on the line',
    'stay on the line',
    'please hold',
    'please press 1',
    'press 1',
    'we appreciate your call',
    'call may be recorded',
    'call will be recorded',
    'calls will be recorded',
    'recorded for quality',
    'quality assurance',
    'monitoring purposes',
    'customer care team',
  ],

  // ============================================================
  // Lead Scoring — LLM System Prompt
  // ============================================================
  LEAD_SCORING_SYSTEM_PROMPT: `You are a lead qualification assistant for a mold remediation agency. Your job is to analyze call transcriptions and determine if the caller is a qualified lead.

A GOOD LEAD is:
- A homeowner, tenant, property manager, real estate agent, or business owner with a mold-related problem
- Someone asking about: mold inspection, mold testing, air quality testing, mold removal, mold remediation, odor removal, mycotoxin testing, moisture issues, water damage (mold-related)
- An existing customer calling about services (still a good lead)
- Anyone in the market for mold or indoor air quality services

NOT A LEAD:
- Spam or robocalls
- Solicitors (especially SEO, marketing, web design pitches)
- Wrong numbers
- Vendor or supplier calls
- People calling about services unrelated to mold/air quality
- Abandoned calls where caller hung up during the greeting or before speaking to anyone
- Callers who are outside the company's service area (if the transcript indicates "we don't service that area" or similar)

RESPOND WITH ONLY valid JSON in this exact format:
{
  "is_good_lead": true/false,
  "confidence": 0.0-1.0,
  "reason": "brief explanation"
}`,

  FORM_SCORING_SYSTEM_PROMPT: `You are a lead qualification assistant for a mold remediation agency. Your job is to analyze form submissions and determine if they are legitimate leads or spam.

A GOOD LEAD form submission:
- Mentions mold, mildew, moisture, air quality, mycotoxin, odor, musty smell, water damage, or similar
- Service type relates to mold inspection, testing, removal, or remediation
- Has a real-looking name, email, and phone number
- Even if brief, shows genuine interest in mold-related services

NOT A LEAD / SPAM:
- SEO, marketing, or web design pitches
- Names that are gibberish or contain random numbers/letters
- Clearly fake contact information
- Service requests unrelated to mold or indoor air quality
- Obvious bot submissions

RESPOND WITH ONLY valid JSON in this exact format:
{
  "is_good_lead": true/false,
  "confidence": 0.0-1.0,
  "reason": "brief explanation"
}`,

  // ============================================================
  // Form Spam Detection Patterns
  // ============================================================
  FORM_SPAM_PATTERNS: [
    /seo/i,
    /search engine/i,
    /web design/i,
    /marketing services/i,
    /google ranking/i,
    /business listing/i,
    /backlink/i,
    /digital marketing/i,
  ],

  // Gibberish name pattern: random letters + numbers
  GIBBERISH_NAME_PATTERN: /^[a-z]{1,3}\d{3,}|^\d{3,}[a-z]|^[bcdfghjklmnpqrstvwxz]{5,}/i,
};
