#!/usr/bin/env node
require('dotenv').config({ path: __dirname + '/.env' });
/**
 * Sunday Touchpoint Task Manager — Creates/updates ClickUp tasks for all clients
 *
 * For each active client, creates up to 2 ClickUp tasks:
 *   1. "Scheduled Call" — the regular monthly/recurring call (unassigned, normal priority)
 *   2. "Check-in" — mid-cycle touchpoint (assigned to Susie, high priority)
 *      Only for clients needing check-ins (Risk, TLC, Flag, onboarding months 1-3)
 *
 * Runs Sunday nights via launchd. Replaces the old slack-thursday-touchpoints.js.
 *
 * Usage:
 *   node sunday-touchpoint-tasks.js          # Create/update ClickUp tasks
 *   node sunday-touchpoint-tasks.js --test   # Dry run (prints to console only)
 */

const { Pool } = require('pg');
const https = require('https');

const pool = new Pool({
  user: 'blueprint',
  database: 'blueprint',
  host: 'localhost',
  port: 5432,
  ssl: false,
});

const CLICKUP_API_KEY = process.env.CLICKUP_API_KEY || 'pk_50313409_WWF9IOF9PJP60BYRC3LSAME866GPBYDP';
const SUSIE_CLICKUP_ID = 50313409;
const NEXT_TOUCHPOINT_TYPE_ID = 1005; // Native ClickUp custom task type "Next Touchpoint"

// ── ClickUp API ────────────────────────────────────────────

function clickupRequest(method, path, body = null) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : null;
    const options = {
      hostname: 'api.clickup.com',
      path: `/api/v2${path}`,
      method,
      headers: {
        'Content-Type': 'application/json',
        'Authorization': CLICKUP_API_KEY,
      },
    };
    if (payload) options.headers['Content-Length'] = Buffer.byteLength(payload);
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', d => data += d);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

async function findOpenTouchpointTask(listId, taskNamePrefix) {
  // Search for existing open tasks matching the prefix
  const data = await clickupRequest('GET', `/list/${listId}/task?statuses[]=to%20do&statuses[]=open&statuses[]=in%20progress&include_closed=false`);
  if (!data.tasks) return null;
  return data.tasks.find(t => t.name.startsWith(taskNamePrefix)) || null;
}

async function createTask(listId, task) {
  return await clickupRequest('POST', `/list/${listId}/task`, task);
}

async function updateTask(taskId, updates) {
  return await clickupRequest('PUT', `/task/${taskId}`, updates);
}

// ── Cadence Logic ──────────────────────────────────────────

function getEffectiveCadence(r) {
  const override = r.contact_cadence_override;
  if (override === 'none') return null;
  if (override) return { weekly: 7, biweekly: 14, monthly: 30 }[override] || 30;
  // Use the more recent of start_date or last_campaign_launch_date for onboarding cadence
  const months = Math.min(
    r.months_in_program ?? 999,
    r.months_since_campaign_launch ?? 999
  );
  if (months <= 1) return 7;    // onboarding/launch month 1
  if (months <= 3) return 14;   // onboarding/launch months 2-3
  if (r.manual_risk || r.confirmed_status === 'Risk') return 7;
  if (r.confirmed_status === 'Flag') return 14;
  return 30; // Healthy
}

function needsCheckin(r) {
  const cadence = getEffectiveCadence(r);
  if (cadence === null) return false;   // opted out
  if (cadence >= 30) return false;      // monthly = scheduled call only
  return true;                          // weekly or biweekly need check-ins
}

function clientDisplayName(name) {
  return name.includes('|') ? name.split('|').pop().trim() : name;
}

function getWeekNumber() {
  const now = new Date();
  const start = new Date(now.getFullYear(), 0, 1);
  const diff = now - start;
  return Math.ceil((diff / 86400000 + start.getDay() + 1) / 7);
}

function getFriday() {
  const d = new Date();
  return snapToFriday(d);
}

function snapToFriday(d) {
  // Snap to the nearest Friday (prefer same week: go back if Sat/Sun, forward if Mon-Thu)
  const day = d.getDay(); // 0=Sun, 1=Mon, ..., 5=Fri, 6=Sat
  let offset;
  if (day === 5) offset = 0;       // Friday — keep it
  else if (day === 6) offset = -1; // Saturday — go back to Friday
  else if (day === 0) offset = -2; // Sunday — go back to Friday
  else offset = 5 - day;           // Mon-Thu — go forward to Friday
  const fri = new Date(d);
  fri.setDate(fri.getDate() + offset);
  fri.setHours(12, 0, 0, 0); // Noon local — safe from timezone boundary issues
  return fri;
}

// ── Main ───────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const isTest = args.includes('--test');
  const weekNum = getWeekNumber();

  try {
    // Get all active clients with contact data
    const { rows } = await pool.query(`
      WITH last_contact AS (
        SELECT customer_id,
          MAX(interaction_date) FILTER (WHERE interaction_type IN ('call', 'meeting')) AS last_live
        FROM client_interactions
        WHERE interaction_type IN ('call', 'meeting')
        GROUP BY customer_id
      ),
      upcoming_meetings AS (
        SELECT customer_id,
          MIN(interaction_date) AS next_meeting
        FROM client_interactions
        WHERE interaction_type = 'meeting' AND source = 'calendar'
          AND interaction_date > NOW()
        GROUP BY customer_id
      )
      SELECT c.customer_id, c.name, c.clickup_list_id, c.manual_risk,
        c.contact_cadence_override,
        COALESCE(ccs.confirmed_status, 'Healthy') AS confirmed_status,
        EXTRACT(MONTH FROM age(CURRENT_DATE, c.start_date))::int
          + EXTRACT(YEAR FROM age(CURRENT_DATE, c.start_date))::int * 12 AS months_in_program,
        CASE WHEN c.last_campaign_launch_date IS NOT NULL THEN
          EXTRACT(MONTH FROM age(CURRENT_DATE, c.last_campaign_launch_date))::int
            + EXTRACT(YEAR FROM age(CURRENT_DATE, c.last_campaign_launch_date))::int * 12
        END AS months_since_campaign_launch,
        lc.last_live,
        EXTRACT(DAY FROM NOW() - lc.last_live)::INT AS days_since_contact,
        um.next_meeting
      FROM clients c
      LEFT JOIN client_confirmed_status ccs ON ccs.customer_id = c.customer_id
      LEFT JOIN last_contact lc ON lc.customer_id = c.customer_id
      LEFT JOIN upcoming_meetings um ON um.customer_id = c.customer_id
      WHERE c.status = 'active' AND c.parent_customer_id IS NULL
      ORDER BY c.name
    `);

    console.log(`Processing ${rows.length} clients (Week ${weekNum})...`);
    let created = 0, updated = 0, skipped = 0;

    for (const r of rows) {
      if (!r.clickup_list_id) { skipped++; continue; }

      const cadence = getEffectiveCadence(r);
      if (cadence === null) { skipped++; continue; } // opted out

      const name = clientDisplayName(r.name);
      const daysSince = r.days_since_contact !== null ? Number(r.days_since_contact) : null;
      const isOverdue = daysSince === null || daysSince > cadence;

      // ── Task 1: Scheduled Call ──
      // Due date = next calendar event if exists, else last_contact + cadence_days
      const scheduledPrefix = '📞 Scheduled call —';
      let scheduledDue;
      if (r.next_meeting) {
        scheduledDue = new Date(r.next_meeting);
      } else if (r.last_live) {
        scheduledDue = new Date(r.last_live);
        scheduledDue.setDate(scheduledDue.getDate() + cadence);
      } else {
        scheduledDue = getFriday(); // no data, due this week
      }
      // Don't set due dates in the past — bump to this week
      if (scheduledDue < new Date()) scheduledDue = getFriday();

      // Format the scheduled date for the task name
      const scheduledDateStr = scheduledDue.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
      const scheduledTaskName = `${scheduledPrefix} ${scheduledDateStr}`;

      if (isTest) {
        console.log(`  ${name} (${r.confirmed_status}, ${cadence}d cadence, ${daysSince !== null ? daysSince + 'd ago' : 'never'}):`);
        console.log(`    Scheduled Call → "${scheduledTaskName}"${r.next_meeting ? ' (from calendar)' : ''} — no due date`);
      } else {
        const existing = await findOpenTouchpointTask(r.clickup_list_id, scheduledPrefix);
        if (existing) {
          // Update name if date changed
          if (existing.name !== scheduledTaskName) {
            await updateTask(existing.id, { name: scheduledTaskName });
            updated++;
          }
        } else {
          await createTask(r.clickup_list_id, {
            name: scheduledTaskName,
            description: `Regular scheduled call for ${name}.\nCadence: ${cadence} days\nStatus: ${r.confirmed_status}${r.manual_risk ? ' (TLC)' : ''}`,
            priority: 3, // Normal
            custom_item_id: NEXT_TOUCHPOINT_TYPE_ID,
            // No due_date — the calendar event is the source of truth
          });
          created++;
        }
        await new Promise(r => setTimeout(r, 300)); // rate limit
      }

      // ── Task 2: Check-in (only if client needs mid-cycle touches AND no upcoming meeting this week) ──
      const hasUpcomingMeeting = r.next_meeting && new Date(r.next_meeting) < new Date(Date.now() + cadence * 86400000);
      if (needsCheckin(r) && !hasUpcomingMeeting) {
        const checkinPrefix = '📞 Check-in —';
        // Due date = midpoint between last contact and next scheduled call, snapped to Friday
        let checkinDue;
        if (r.last_live) {
          checkinDue = new Date(r.last_live);
          checkinDue.setDate(checkinDue.getDate() + Math.floor(cadence / 2));
        } else {
          checkinDue = new Date();
        }
        if (checkinDue < new Date()) checkinDue = new Date(); // if in the past, start from today
        checkinDue = snapToFriday(checkinDue); // always land on a Friday at 4pm MT

        if (isTest) {
          console.log(`    Check-in → due ${checkinDue.toLocaleDateString()} (assigned to Susie, HIGH priority)`);
        } else {
          const existing = await findOpenTouchpointTask(r.clickup_list_id, checkinPrefix);
          if (existing) {
            const existingDue = existing.due_date ? new Date(parseInt(existing.due_date)) : null;
            if (!existingDue || Math.abs(existingDue - checkinDue) > 86400000) {
              await updateTask(existing.id, {
                due_date: checkinDue.getTime(),
                due_date_time: false,
                priority: isOverdue ? 1 : 2,
              });
              updated++;
            }
          } else {
            await createTask(r.clickup_list_id, {
              name: `📞 Check-in`,
              description: `Mid-cycle check-in for ${name}.\nCadence: ${cadence} days\nStatus: ${r.confirmed_status}${r.manual_risk ? ' (TLC)' : ''}\n\nThis is an impromptu touchpoint between scheduled calls.`,
              assignees: [SUSIE_CLICKUP_ID],
              priority: isOverdue ? 1 : 2,
              due_date: checkinDue.getTime(),
              due_date_time: false,
              custom_item_id: NEXT_TOUCHPOINT_TYPE_ID,
            });
            created++;
          }
          await new Promise(r => setTimeout(r, 300));
        }
      } else if (isTest) {
        if (hasUpcomingMeeting) {
          console.log(`    No check-in needed (meeting scheduled ${new Date(r.next_meeting).toLocaleDateString()})`);
        } else {
          console.log(`    No check-in needed (monthly cadence)`);
        }
      }
    }

    console.log(`\nDone. Created: ${created}, Updated: ${updated}, Skipped: ${skipped}`);

  } catch (err) {
    console.error('Error:', err.message);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();
