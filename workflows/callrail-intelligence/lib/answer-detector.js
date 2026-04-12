// ============================================================
// Answer Detector — determines if a call was truly answered,
// missed, or abandoned using transcript analysis + duration
// ============================================================

const config = require('../config');

function detectAnswerStatus(call) {
  const transcription = getTranscriptionText(call);
  const hasDuration = call.duration != null;
  const duration = call.duration || 0;
  const speakerPercent = call.speaker_percent || null;
  const callType = call.call_type || '';
  const callrailStatus = (call.callrail_status || '').toLowerCase();

  // ── NULL duration with transcript = don't trust duration ──
  // Some calls have NULL duration but a full transcript. Check transcript first.
  if (!hasDuration && transcription) {
    const textSpeakers = countSpeakersFromText(transcription);
    if (textSpeakers >= 2) {
      return { tag: 'answered', reason: `No duration data but transcript has ${textSpeakers} speakers`, confidence: 0.9 };
    }
    // Single speaker with transcript — check for voicemail/greeting patterns below
  }

  // ── Early exits (only when we have duration data or no transcript) ──
  if (callType === 'abandoned' && !transcription) {
    return { tag: 'abandoned', reason: 'Call type is abandoned', confidence: 0.95 };
  }

  if (hasDuration && duration < 5 && !transcription) {
    return { tag: 'abandoned', reason: `Very short duration (${duration}s)`, confidence: 0.9 };
  }

  if (hasDuration && duration < 5 && transcription) {
    // Very short but has a transcript — let transcript analysis decide
    // (falls through to transcript-based classification below)
  } else if (!hasDuration && !transcription) {
    // No duration, no transcript — use CallRail signals
    if (callrailStatus === 'answered' || call.answered === true) {
      return { tag: 'answered', reason: 'No duration/transcript but CallRail says answered', confidence: 0.4 };
    }
    return { tag: 'abandoned', reason: 'No duration or transcript data', confidence: 0.5 };
  }

  // ── CallRail missed/abandoned signal ──
  // CallRail is highly reliable when it says a call was NOT answered (99.85% accurate).
  // Only override if we have strong transcript evidence of a real conversation.
  const callrailSaysMissed = call.answered === false
    || callrailStatus === 'missed'
    || callrailStatus === 'abandoned';

  if (callrailSaysMissed) {
    // Even if CallRail says missed, a long two-way transcript overrides
    const textSpeakers = countSpeakersFromText(transcription);
    if (transcription && duration >= 60 && textSpeakers >= 2) {
      return { tag: 'answered', reason: `CallRail said missed but transcript shows ${duration}s conversation with ${textSpeakers} speakers`, confidence: 0.9 };
    }
    // Trust CallRail — classify as missed or abandoned based on duration
    if (duration < 10) {
      return { tag: 'abandoned', reason: `CallRail: not answered, short duration (${duration}s)`, confidence: 0.9 };
    }
    return { tag: 'missed', reason: `CallRail: not answered (${duration}s)`, confidence: 0.9 };
  }

  // ── From here on, CallRail says answered=true (which is unreliable) ──
  // We use transcript + duration to verify

  // ── No transcript fallback (duration-only) ──
  if (!transcription) {
    if (duration < 10) {
      return { tag: 'abandoned', reason: `No transcription, short duration (${duration}s)`, confidence: 0.7 };
    }
    // Stricter threshold: voicemails often run 20-40s, so require 45s+
    if (duration >= 45) {
      return { tag: 'answered', reason: `No transcription but solid duration (${duration}s)`, confidence: 0.5 };
    }
    if (duration >= 20) {
      return { tag: 'missed', reason: `No transcription, short-moderate duration (${duration}s) — likely voicemail`, confidence: 0.5 };
    }
    return { tag: 'missed', reason: 'No transcription and uncertain', confidence: 0.4 };
  }

  // ── Transcript-based classification ──
  const transcriptionLower = transcription.toLowerCase();

  // Parse speaker count from transcript text (more reliable than speaker_percent)
  const textSpeakerCount = countSpeakersFromText(transcription);
  const metaSpeakerCount = getSpeakerCount(speakerPercent);
  const speakerCount = Math.max(textSpeakerCount, metaSpeakerCount);

  const voicemailMatch = findKeywordMatch(transcriptionLower, config.VOICEMAIL_KEYWORDS);
  const greetingMatch = findKeywordMatch(transcriptionLower, config.GREETING_KEYWORDS);

  // ── Long call override: 60s+ with real two-way conversation = answered ──
  if (duration >= 60 && textSpeakerCount >= 2) {
    return { tag: 'answered', reason: `Long call override: ${duration}s with ${textSpeakerCount} speakers in transcript`, confidence: 0.95 };
  }

  // ── Voicemail detection ──
  if (voicemailMatch && speakerCount <= 1) {
    return { tag: 'missed', reason: `Voicemail detected: "${voicemailMatch}"`, confidence: 0.95 };
  }

  // Voicemail with caller leaving a message (Agent is IVR, Caller leaves msg)
  if (voicemailMatch && duration < 60) {
    return { tag: 'missed', reason: `Voicemail with message left: "${voicemailMatch}" (${duration}s)`, confidence: 0.85 };
  }

  // ── Greeting/IVR detection ──
  // Check if a real person introduced themselves (e.g. "This is Jill", "Hello, this is Ethan")
  // This distinguishes a live answer from an IVR greeting, even with only 1 speaker
  const personalIntro = hasPersonalIntroduction(transcriptionLower);

  if (greetingMatch && speakerCount <= 1) {
    if (personalIntro) {
      return { tag: 'answered', reason: `Agent introduced themselves: "${personalIntro}" — real person answered`, confidence: 0.85 };
    }
    if (duration < 10) {
      return { tag: 'abandoned', reason: `Caller hung up during greeting: "${greetingMatch}"`, confidence: 0.9 };
    }
    return { tag: 'missed', reason: `IVR/greeting only, no conversation: "${greetingMatch}" (${duration}s)`, confidence: 0.85 };
  }

  // Two speakers with greeting + decent duration = real call
  if (greetingMatch && speakerCount >= 2 && duration >= 20) {
    return { tag: 'answered', reason: `Greeting + ${speakerCount} speakers + ${duration}s`, confidence: 0.9 };
  }

  // Voicemail keywords but clearly a conversation
  if (voicemailMatch && speakerCount >= 2 && duration >= 30) {
    return { tag: 'answered', reason: `Voicemail keywords but ${speakerCount} speakers and ${duration}s`, confidence: 0.7 };
  }

  // ── Speaker + duration heuristics ──
  if (speakerCount <= 1 && duration < 15) {
    return { tag: 'abandoned', reason: `Single speaker, short duration (${duration}s)`, confidence: 0.8 };
  }

  if (speakerCount >= 2 && duration >= 15) {
    return { tag: 'answered', reason: `${speakerCount} speakers, ${duration}s duration`, confidence: 0.85 };
  }

  if (speakerCount <= 1 && duration >= 30) {
    if (voicemailMatch) {
      return { tag: 'missed', reason: `Long voicemail left (${duration}s)`, confidence: 0.8 };
    }
    return { tag: 'answered', reason: `Long duration (${duration}s) but single speaker — uncertain`, confidence: 0.4 };
  }

  // ── Fallback ──
  return {
    tag: call.answered ? 'answered' : 'missed',
    reason: 'No strong signals — using CallRail default',
    confidence: 0.3,
  };
}

/**
 * Detect if the agent personally introduced themselves (not just an IVR greeting).
 * E.g. "This is Jill", "Hello, this is Ethan", "My name is Bailey"
 * Returns the matched phrase or null.
 */
function hasPersonalIntroduction(transcriptionLower) {
  // "this is [Name]" pattern — but exclude IVR-style phrases
  const introMatch = transcriptionLower.match(/\bthis is ([a-z]+)/);
  if (introMatch) {
    const name = introMatch[1];
    // Exclude common IVR words that follow "this is"
    const ivrWords = ['a', 'an', 'the', 'your', 'our', 'not', 'pure', 'mold', 'no', 'being'];
    if (!ivrWords.includes(name) && name.length >= 2) {
      return `this is ${name}`;
    }
  }
  // "my name is [Name]"
  const nameMatch = transcriptionLower.match(/\bmy name is ([a-z]+)/);
  if (nameMatch && nameMatch[1].length >= 2) {
    return `my name is ${nameMatch[1]}`;
  }
  return null;
}

/**
 * Count distinct speakers by parsing "Agent:" and "Caller:" labels in transcript text.
 * More reliable than speaker_percent which is often missing.
 */
function countSpeakersFromText(transcription) {
  if (!transcription) return 0;
  const hasAgent = /\bAgent:/i.test(transcription);
  const hasCaller = /\bCaller:/i.test(transcription);
  return (hasAgent ? 1 : 0) + (hasCaller ? 1 : 0);
}

function getTranscriptionText(call) {
  if (!call.transcription) return '';
  if (typeof call.transcription === 'string') return call.transcription;
  if (Array.isArray(call.transcription)) {
    return call.transcription.map(u => u.content || u.text || '').join(' ');
  }
  return '';
}

function getSpeakerCount(speakerPercent) {
  if (!speakerPercent) return 0;
  if (typeof speakerPercent === 'object' && !Array.isArray(speakerPercent)) {
    return Object.keys(speakerPercent).length;
  }
  return 0;
}

function findKeywordMatch(text, keywords) {
  for (const keyword of keywords) {
    if (text.includes(keyword)) return keyword;
  }
  return null;
}

module.exports = { detectAnswerStatus, countSpeakersFromText };
