// ============================================================
// CallRail API Wrapper — Multi-account support
// ============================================================

const config = require('../config');

/**
 * Create headers for a specific account.
 */
function getHeaders(apiKey) {
    return {
        'Authorization': `Token token=${apiKey}`,
        'Content-Type': 'application/json',
    };
}

function getBaseUrl(accountId) {
    return `https://api.callrail.com/v3/a/${accountId}`;
}

/**
 * Fetch a single call with all fields needed for analysis.
 */
async function getCall(accountId, apiKey, callId) {
    const url = `${getBaseUrl(accountId)}/calls/${callId}.json?fields=gclid,source,source_name,company_name,company_id,tags,answered,first_call,start_time,company_time_zone,transcription,speaker_percent,call_type,duration,voicemail,call_summary,call_highlights,lead_status`;

    const res = await fetch(url, { headers: getHeaders(apiKey) });
    if (!res.ok) {
        const text = await res.text();
        throw new Error(`CallRail API error (getCall ${callId}): ${res.status} — ${text}`);
    }
    return res.json();
}

/**
 * Fetch a single form submission.
 */
async function getFormSubmission(accountId, apiKey, formSubmissionId) {
    const url = `${getBaseUrl(accountId)}/form_submissions/${formSubmissionId}.json`;

    const res = await fetch(url, { headers: getHeaders(apiKey) });
    if (!res.ok) {
        const text = await res.text();
        throw new Error(`CallRail API error (getForm ${formSubmissionId}): ${res.status} — ${text}`);
    }
    return res.json();
}

/**
 * Update a call's tags. Replaces ALL tags on the call.
 */
async function updateCallTags(accountId, apiKey, callId, tagList) {
    const url = `${getBaseUrl(accountId)}/calls/${callId}.json`;
    const hdrs = getHeaders(apiKey);

    const res = await fetch(url, {
        method: 'PUT',
        headers: hdrs,
        body: JSON.stringify({ tags: tagList, append_tags: false }),
    });
    if (!res.ok) {
        const text = await res.text();
        throw new Error(`CallRail API error (updateTags ${callId}): ${res.status} — ${text}`);
    }
    return res.json();
}

/**
 * Update a call's lead status.
 */
async function updateCallLeadStatus(accountId, apiKey, callId, leadStatus) {
    const url = `${getBaseUrl(accountId)}/calls/${callId}.json`;

    const res = await fetch(url, {
        method: 'PUT',
        headers: getHeaders(apiKey),
        body: JSON.stringify({ lead_status: leadStatus }),
    });

    if (!res.ok) {
        const text = await res.text();
        throw new Error(`CallRail API error (updateLeadStatus ${callId}): ${res.status} — ${text}`);
    }
    return res.json();
}

/**
 * Update a form submission's lead status.
 */
async function updateFormLeadStatus(accountId, apiKey, formSubmissionId, leadStatus) {
    const url = `${getBaseUrl(accountId)}/form_submissions/${formSubmissionId}.json`;

    const res = await fetch(url, {
        method: 'PUT',
        headers: getHeaders(apiKey),
        body: JSON.stringify({ lead_status: leadStatus }),
    });

    if (!res.ok) {
        const text = await res.text();
        throw new Error(`CallRail API error (updateFormLead ${formSubmissionId}): ${res.status} — ${text}`);
    }
    return res.json();
}

/**
 * Add a specific tag to a call without removing existing tags.
 */
async function addCallTag(accountId, apiKey, callId, newTag, existingTags) {
    const conflicting = ['answered', 'missed', 'abandoned'];
    const cleanedTags = (existingTags || [])
        .map(t => typeof t === 'string' ? t : (t.name || ''))
        .filter(t => !conflicting.includes(t.toLowerCase()));

    cleanedTags.push(newTag);
    return updateCallTags(accountId, apiKey, callId, cleanedTags);
}

/**
 * List recent calls with full details. Paginates through all results.
 */
async function listRecentCalls(accountId, apiKey, startDate, endDate) {
    let allCalls = [];
    let page = 1;
    let totalPages = 1;

    do {
        const url = `${getBaseUrl(accountId)}/calls.json`
            + `?start_date=${startDate}`
            + `&end_date=${endDate}`
            + `&fields=gclid,source,source_name,company_name,company_id,tags,answered,first_call,start_time,company_time_zone,transcription,speaker_percent,call_type,duration,voicemail,call_summary,lead_status`
            + `&per_page=100`
            + `&page=${page}`;

        const res = await fetch(url, { headers: getHeaders(apiKey) });
        if (!res.ok) {
            const text = await res.text();
            console.error(`[API] Error listing calls page ${page}: ${res.status} — ${text}`);
            break;
        }

        const data = await res.json();
        if (data.calls && data.calls.length > 0) {
            allCalls = allCalls.concat(data.calls);
        }

        totalPages = data.total_pages || 1;
        page++;

        if (page <= totalPages) {
            await new Promise(r => setTimeout(r, 1100));
        }
    } while (page <= totalPages);

    return allCalls;
}

/**
 * List recent form submissions. Paginates through all results.
 */
async function listRecentFormSubmissions(accountId, apiKey, startDate, endDate) {
    let allForms = [];
    let page = 1;
    let totalPages = 1;

    do {
        const url = `${getBaseUrl(accountId)}/form_submissions.json`
            + `?start_date=${startDate}`
            + `&end_date=${endDate}`
            + `&per_page=100`
            + `&page=${page}`;

        const res = await fetch(url, { headers: getHeaders(apiKey) });
        if (!res.ok) {
            const text = await res.text();
            console.error(`[API] Error listing forms page ${page}: ${res.status} — ${text}`);
            break;
        }

        const data = await res.json();
        if (data.form_submissions && data.form_submissions.length > 0) {
            allForms = allForms.concat(data.form_submissions);
        }

        totalPages = data.total_pages || 1;
        page++;

        if (page <= totalPages) {
            await new Promise(r => setTimeout(r, 1100));
        }
    } while (page <= totalPages);

    return allForms;
}

/**
 * Search for calls by customer phone number.
 * Returns most recent calls first.
 */
async function searchCallsByPhone(accountId, apiKey, phoneNumber) {
    // Normalize: strip everything except digits, keep last 10
    const normalized = phoneNumber.replace(/\D/g, '').slice(-10);
    const url = `${getBaseUrl(accountId)}/calls.json`
        + `?customer_phone_number=${normalized}`
        + `&fields=tags,lead_status,first_call,answered,duration,start_time,company_name,company_id`
        + `&sort=start_time`
        + `&order=desc`
        + `&per_page=10`;

    const res = await fetch(url, { headers: getHeaders(apiKey) });
    if (!res.ok) {
        const text = await res.text();
        throw new Error(`CallRail API error (searchByPhone ${normalized}): ${res.status} — ${text}`);
    }

    const data = await res.json();
    return data.calls || [];
}

module.exports = {
    getCall,
    getFormSubmission,
    updateCallTags,
    updateCallLeadStatus,
    updateFormLeadStatus,
    addCallTag,
    listRecentCalls,
    listRecentFormSubmissions,
    searchCallsByPhone,
};
