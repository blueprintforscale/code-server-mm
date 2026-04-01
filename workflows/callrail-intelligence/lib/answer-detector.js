// ============================================================
// Answer Detector — Determines if a call was truly answered
// Uses keyword matching on transcription + speaker analysis
// ============================================================

const config = require('../config');

/**
 * Analyzes a call to determine if it was truly answered, missed,
 * or abandoned. Returns the correct tag and reasoning.
 *
 * @param {Object} call - CallRail call object with transcription, tags, etc.
 * @returns {{ tag: string, reason: string, confidence: number }}
 */
function detectAnswerStatus(call) {
    const transcription = getTranscriptionText(call);
    const duration = call.duration || 0;
    const speakerPercent = call.speaker_percent || null;
    const callType = call.call_type || '';
    const voicemail = call.voicemail || false;

    // Check for abandoned call type
    if (callType === 'abandoned') {
        return { tag: 'abandoned', reason: 'Call type is abandoned', confidence: 0.95 };
    }

    // Very short calls — caller hung up before greeting could finish
    if (duration < 5) {
        return { tag: 'abandoned', reason: `Very short duration (${duration}s) — caller hung up immediately`, confidence: 0.9 };
    }

    // No transcription available — fall back to duration
    // Recording may be off, so duration is our best signal
    if (!transcription) {
        if (duration < 10) {
            return { tag: 'abandoned', reason: `No transcription, short duration (${duration}s) — likely hung up during greeting`, confidence: 0.7 };
        }
        if (duration >= 60) {
            return { tag: 'answered', reason: `No transcription but long duration (${duration}s) — real conversation`, confidence: 0.8 };
        }
        if (duration >= 20) {
            return { tag: 'answered', reason: `No transcription but decent duration (${duration}s) — likely answered`, confidence: 0.6 };
        }
        // 10-19 seconds with no transcript — could be voicemail or brief missed call
        if (voicemail) {
            return { tag: 'missed', reason: `No transcription, short duration (${duration}s), voicemail flag set`, confidence: 0.7 };
        }
        return { tag: 'missed', reason: `No transcription, short duration (${duration}s) — uncertain`, confidence: 0.4 };
    }

    const transcriptionLower = transcription.toLowerCase();
    const speakerCount = getSpeakerCount(speakerPercent);

    // Check for voicemail patterns
    const voicemailMatch = findKeywordMatch(transcriptionLower, config.VOICEMAIL_KEYWORDS);
    if (voicemailMatch && speakerCount <= 1) {
        return { tag: 'missed', reason: `Voicemail detected: "${voicemailMatch}" (${speakerCount} speaker)`, confidence: 0.95 };
    }

    // Check for greeting-only calls (abandoned during greeting)
    // e.g. "Thank you for calling Pure Maintenance, please press 1..." but caller hung up
    const greetingMatch = findKeywordMatch(transcriptionLower, config.GREETING_KEYWORDS);
    if (greetingMatch && speakerCount <= 1) {
        // Greeting plays but caller never spoke — abandoned
        return { tag: 'abandoned', reason: `Caller hung up during greeting: "${greetingMatch}" (${speakerCount} speaker)`, confidence: 0.9 };
    }

    // Greeting detected but there IS a second speaker — real call
    if (greetingMatch && speakerCount >= 2 && duration >= 20) {
        return { tag: 'answered', reason: `Greeting + ${speakerCount} speakers + ${duration}s duration — real conversation`, confidence: 0.9 };
    }

    // Voicemail keywords but multiple speakers — might be a greeting before live answer
    if (voicemailMatch && speakerCount >= 2 && duration >= 30) {
        return { tag: 'answered', reason: `Voicemail keywords but ${speakerCount} speakers and ${duration}s — likely answered after greeting`, confidence: 0.7 };
    }

    // No voicemail/greeting keywords, single speaker, short duration — likely abandoned
    if (speakerCount <= 1 && duration < 15) {
        return { tag: 'abandoned', reason: `Single speaker, short duration (${duration}s) — likely hung up before connecting`, confidence: 0.8 };
    }

    // Multiple speakers and decent duration — answered
    if (speakerCount >= 2 && duration >= 15) {
        return { tag: 'answered', reason: `${speakerCount} speakers, ${duration}s duration — real conversation`, confidence: 0.85 };
    }

    // Single speaker but long duration — could be leaving a long voicemail
    if (speakerCount <= 1 && duration >= 30) {
        if (voicemailMatch) {
            return { tag: 'missed', reason: `Long voicemail left (${duration}s)`, confidence: 0.8 };
        }
        // Could be a monologue or recording — uncertain
        return { tag: 'answered', reason: `Long duration (${duration}s) but single speaker — uncertain, defaulting to answered`, confidence: 0.4 };
    }

    // Default: use CallRail's determination with low confidence
    return {
        tag: call.answered ? 'answered' : 'missed',
        reason: 'No strong signals — using CallRail default',
        confidence: 0.3,
    };
}

// ============================================================
// Helpers
// ============================================================

/**
 * Extract text from transcription (handles different formats).
 */
function getTranscriptionText(call) {
    if (!call.transcription) return '';

    // Transcription can be a string or an object
    if (typeof call.transcription === 'string') {
        return call.transcription;
    }

    // If it's an object/array of utterances, join them
    if (Array.isArray(call.transcription)) {
        return call.transcription.map(u => u.content || u.text || '').join(' ');
    }

    return '';
}

/**
 * Count unique speakers from speaker_percent data.
 */
function getSpeakerCount(speakerPercent) {
    if (!speakerPercent) return 0;

    if (typeof speakerPercent === 'object') {
        return Object.keys(speakerPercent).length;
    }

    return 0;
}

/**
 * Find the first matching keyword in the text.
 */
function findKeywordMatch(text, keywords) {
    for (const keyword of keywords) {
        if (text.includes(keyword)) {
            return keyword;
        }
    }
    return null;
}

module.exports = { detectAnswerStatus };
