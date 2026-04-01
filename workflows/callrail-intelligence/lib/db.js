// ============================================================
// Database — Write answer detection results to PostgreSQL
// ============================================================

const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.DB_HOST || 'localhost',
  port: process.env.DB_PORT || 5432,
  database: process.env.DB_NAME || 'blueprint',
  user: process.env.DB_USER || 'blueprint',
  password: process.env.DB_PASSWORD || '',
  max: 5,
});

/**
 * Update a call's AI answer detection result in the database.
 * Matches by callrail_id since that's what we get from the API.
 */
async function updateAnswerStatus(callrailId, tag, reason, confidence) {
  await pool.query(
    `UPDATE calls SET ai_answered = $1, ai_answered_reason = $2, ai_answered_confidence = $3
     WHERE callrail_id = $4`,
    [tag, reason, confidence, callrailId]
  );
}

/**
 * Backfill: run answer detection on all calls in the DB that have
 * transcripts but no ai_answered value yet.
 * Returns calls with their transcript and speaker data.
 */
async function getUnprocessedCalls(limit = 500) {
  const { rows } = await pool.query(`
    SELECT callrail_id, transcript, duration, call_type, voicemail,
           speaker_percent, answered, callrail_status, source_name
    FROM calls
    WHERE ai_answered IS NULL
    ORDER BY start_time DESC
    LIMIT $1
  `, [limit]);
  return rows;
}

/**
 * Get count of calls needing processing.
 */
async function getUnprocessedCount() {
  const { rows } = await pool.query(`
    SELECT COUNT(*) as count FROM calls WHERE ai_answered IS NULL
  `);
  return parseInt(rows[0].count);
}

module.exports = { pool, updateAnswerStatus, getUnprocessedCalls, getUnprocessedCount };
