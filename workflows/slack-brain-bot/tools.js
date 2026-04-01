/**
 * Claude Tool Definitions — database query tools for Blueprint Brain
 *
 * Each tool maps to a parameterized SQL query against the blueprint database.
 * All queries are read-only. Claude picks the right tool(s) based on the user's question.
 *
 * ACTUAL SCHEMA (verified 2026-03-22):
 *   client_tasks: id, customer_id, clickup_task_id, task_type, title, description, status, assigned_to, due_date, completed_date, priority, program_milestone, tags, created_at, updated_at
 *   client_personal_notes: id, customer_id, note, category, source, source_id, captured_date, captured_by, auto_extracted, created_at
 *   client_interactions: id, customer_id, interaction_type, interaction_date, logged_by, attendees, summary, action_items, sentiment, follow_up_date, source, source_id, recording_url, transcript, created_at, updated_at, slack_posted_at, slack_summary, email_draft
 *   calls: id, callrail_id, callrail_company_id, customer_id, caller_phone, gclid, start_time, duration, transcript, ..., classified_status, classified_source, source, customer_name, customer_city, customer_state, answered, call_summary, ...
 *   client_alerts: id, customer_id, alert_type, severity, message, auto_generated, resolved_at, resolved_by, created_at
 *   crm_messages: id, source, customer_id, contact_name, phone_number, phone_normalized, direction, channel, message_body, duration, message_date, source_id, created_at
 *   slack_messages: id, customer_id, channel_id, message_ts, thread_ts, user_id, user_name, message_text, has_files, reactions, posted_at, created_at
 *   get_dashboard_with_risk(): uses risk_type (not risk_status), status, risk_triggers, flag_triggers, guarantee, quality_leads, roas, ad_spend, cpl, etc.
 */

// ── Tool Definitions (sent to Claude API) ────────────────────

const TOOLS = [
  {
    name: 'get_client_summary',
    description: 'Get a comprehensive summary for a specific client including leads, ROAS, risk status, open tasks, recent interactions, and personal notes. Use this when someone asks about a specific client.',
    input_schema: {
      type: 'object',
      properties: {
        client_name: {
          type: 'string',
          description: 'Client name or partial match (e.g. "Fisher", "Blagg", "Rob Brown")',
        },
      },
      required: ['client_name'],
    },
  },
  {
    name: 'get_client_leads',
    description: 'Get lead details for a client — calls and form submissions with classification, source, and funnel stage. Filterable by date range.',
    input_schema: {
      type: 'object',
      properties: {
        client_name: {
          type: 'string',
          description: 'Client name or partial match',
        },
        days_back: {
          type: 'number',
          description: 'Number of days to look back (default 30)',
        },
        source_filter: {
          type: 'string',
          description: 'Optional: filter by source like "google_ads", "organic", "gbp", "lsa"',
        },
      },
      required: ['client_name'],
    },
  },
  {
    name: 'get_client_roas',
    description: 'Get ROAS (Return on Ad Spend) breakdown for a client — ad spend, revenue from inspections + treatments, ROAS ratio, cost per lead.',
    input_schema: {
      type: 'object',
      properties: {
        client_name: {
          type: 'string',
          description: 'Client name or partial match',
        },
      },
      required: ['client_name'],
    },
  },
  {
    name: 'get_client_interactions',
    description: 'Get recent interactions with a client — meetings, calls, Slack conversations, emails. Shows what was discussed and any action items.',
    input_schema: {
      type: 'object',
      properties: {
        client_name: {
          type: 'string',
          description: 'Client name or partial match',
        },
        limit: {
          type: 'number',
          description: 'Number of interactions to return (default 10)',
        },
      },
      required: ['client_name'],
    },
  },
  {
    name: 'get_client_tasks',
    description: 'Get open tasks for a client from ClickUp — with status, assignee, due date, and priority.',
    input_schema: {
      type: 'object',
      properties: {
        client_name: {
          type: 'string',
          description: 'Client name or partial match',
        },
        include_closed: {
          type: 'boolean',
          description: 'Include completed tasks (default false)',
        },
      },
      required: ['client_name'],
    },
  },
  {
    name: 'get_client_personal_notes',
    description: 'Get personal/relationship notes about a client — preferences, personality quirks, family details, communication style. Things an account manager should know.',
    input_schema: {
      type: 'object',
      properties: {
        client_name: {
          type: 'string',
          description: 'Client name or partial match',
        },
      },
      required: ['client_name'],
    },
  },
  {
    name: 'get_risk_dashboard',
    description: 'Get the current risk/flag status for all clients or a specific client. Shows risk triggers, guarantee status, and recommended actions.',
    input_schema: {
      type: 'object',
      properties: {
        client_name: {
          type: 'string',
          description: 'Optional: specific client name. If omitted, returns all clients with risk/flag status.',
        },
        status_filter: {
          type: 'string',
          enum: ['Risk', 'Flag', 'Healthy'],
          description: 'Optional: filter by status (Risk, Flag, or Healthy)',
        },
      },
    },
  },
  {
    name: 'get_manager_portfolio',
    description: 'Get all clients for a specific account manager with their status, leads, ROAS, and attention items.',
    input_schema: {
      type: 'object',
      properties: {
        manager_name: {
          type: 'string',
          description: 'Manager name (e.g. "Martin", "Luke", "Nima")',
        },
      },
      required: ['manager_name'],
    },
  },
  {
    name: 'get_recent_activity',
    description: 'Get recent activity across all clients or a specific client — new leads, estimates sent, jobs completed, invoices. Good for "what happened today/this week" questions.',
    input_schema: {
      type: 'object',
      properties: {
        client_name: {
          type: 'string',
          description: 'Optional: specific client name',
        },
        days_back: {
          type: 'number',
          description: 'Number of days to look back (default 7)',
        },
      },
    },
  },
  {
    name: 'get_estimates_pipeline',
    description: 'Get outstanding estimates for a client — sent but not yet approved, with amounts and days outstanding. Great for follow-up questions.',
    input_schema: {
      type: 'object',
      properties: {
        client_name: {
          type: 'string',
          description: 'Optional: specific client. If omitted, shows all outstanding estimates.',
        },
      },
    },
  },
  {
    name: 'search_leads_by_phone',
    description: 'Look up a specific lead by phone number across all systems — CallRail, HCP, Jobber, GHL. Shows full timeline for that lead.',
    input_schema: {
      type: 'object',
      properties: {
        phone_number: {
          type: 'string',
          description: 'Phone number (any format — will be normalized)',
        },
      },
      required: ['phone_number'],
    },
  },
  {
    name: 'run_custom_query',
    description: 'Run a custom read-only SQL query against the blueprint database. Use this only when the other tools cannot answer the question. Key tables: clients, calls, form_submissions, hcp_customers, hcp_estimates, hcp_jobs, hcp_invoices, jobber_customers, jobber_quotes, jobber_jobs, jobber_invoices, ghl_contacts, ghl_opportunities, ad_spend, lsa_leads, client_interactions, client_tasks, client_alerts, client_personal_notes, crm_messages, slack_messages, webflow_submissions. Key views: v_client_health, v_hcp_roas, v_lead_pipeline, v_clients_needing_attention, v_estimate_groups. Function: get_dashboard_with_risk(). IMPORTANT column names: client_tasks uses "title" (not task_name), "assigned_to" (not assignee); client_personal_notes uses "note" (not note_text); calls uses "customer_name" (not caller_name); client_interactions has NO "decisions" column; get_dashboard_with_risk() uses "status" and "risk_type" (not risk_status).',
    input_schema: {
      type: 'object',
      properties: {
        query: {
          type: 'string',
          description: 'SELECT query only. No INSERT/UPDATE/DELETE/DROP. Do NOT add LIMIT if one already exists.',
        },
      },
      required: ['query'],
    },
  },
  {
    name: 'get_client_sentiment',
    description: 'Get the overall sentiment and relationship health for a client. Pulls from: recent interactions (meetings, calls) with sentiment scores, personal notes about the client relationship, Slack message tone, CRM messages, and any alerts or risk flags. Use this when someone asks "is the client happy?", "how are things going with them?", "what\'s the vibe?", or any question about client satisfaction or relationship quality.',
    input_schema: {
      type: 'object',
      properties: {
        client_name: {
          type: 'string',
          description: 'Client name or partial match',
        },
      },
      required: ['client_name'],
    },
  },
  {
    name: 'get_funnel_constraint',
    description: 'Analyze a client\'s funnel to find their #1 bottleneck using the Blueprint methodology. Compares their metrics against cohort averages across all clients. Returns: lead volume, inspection book rate, estimate sent rate, estimate close rate, average job size, and how each compares to the cohort. Use this when someone asks "what\'s their constraint?", "where\'s the bottleneck?", "why is ROAS low?", or any open-ended "how are they doing?" question.',
    input_schema: {
      type: 'object',
      properties: {
        client_name: {
          type: 'string',
          description: 'Client name or partial match',
        },
      },
      required: ['client_name'],
    },
  },
  {
    name: 'get_client_contacts',
    description: 'Get the team/contacts for a client — owners, employees, dispatchers, call center, techs. Also can search across all clients to find which ones use a specific person (e.g. "which clients use Currie?").',
    input_schema: {
      type: 'object',
      properties: {
        client_name: {
          type: 'string',
          description: 'Optional: specific client name to get their team',
        },
        contact_name: {
          type: 'string',
          description: 'Optional: search for a person across all clients (e.g. "Currie", "Miller")',
        },
      },
    },
  },
  {
    name: 'get_client_alerts',
    description: 'Get active alerts for a client or all clients — lead drops, overdue tasks, no leads, contract expiring.',
    input_schema: {
      type: 'object',
      properties: {
        client_name: {
          type: 'string',
          description: 'Optional: specific client name',
        },
        severity: {
          type: 'string',
          enum: ['critical', 'warning', 'info'],
          description: 'Optional: filter by severity',
        },
      },
    },
  },
];

// ── Query Implementations ────────────────────────────────────

const CUST_ID = `(SELECT customer_id FROM clients WHERE name ILIKE $1 LIMIT 1)`;

async function executeToolQuery(pool, toolName, input) {
  switch (toolName) {
    case 'get_client_summary': {
      const name = `%${input.client_name}%`;
      const [clientRow, leadsRow, roasRow, interactionsRow, tasksRow, notesRow, alertsRow, riskRow] = await Promise.all([
        pool.query(`
          SELECT c.customer_id, c.name, c.field_management_software, c.budget, c.status,
                 c.start_date, c.ads_manager, c.inspection_type,
                 cp.account_manager, cp.monthly_retainer, cp.client_tier,
                 cp.onboarding_status, cp.slack_channel_name, cp.client_goals, cp.client_bio,
                 cp.contract_renewal_date
          FROM clients c
          LEFT JOIN client_profiles cp ON cp.customer_id = c.customer_id
          WHERE c.name ILIKE $1
          LIMIT 1
        `, [name]),
        pool.query(`
          SELECT
            COUNT(*) FILTER (WHERE start_time >= DATE_TRUNC('month', CURRENT_DATE)) as leads_this_month,
            COUNT(*) FILTER (WHERE start_time >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
                              AND start_time < DATE_TRUNC('month', CURRENT_DATE)) as leads_last_month,
            COUNT(*) FILTER (WHERE start_time >= CURRENT_DATE - INTERVAL '7 days') as leads_this_week
          FROM calls
          WHERE customer_id = ${CUST_ID}
            AND classified_status NOT IN ('spam', 'irrelevant', 'brand')
        `, [name]),
        pool.query(`SELECT * FROM v_hcp_roas WHERE customer_id = ${CUST_ID}`, [name]),
        pool.query(`
          SELECT interaction_type, summary, interaction_date, source, sentiment
          FROM client_interactions
          WHERE customer_id = ${CUST_ID}
          ORDER BY interaction_date DESC LIMIT 3
        `, [name]),
        pool.query(`
          SELECT title, status, assigned_to, due_date, priority
          FROM client_tasks
          WHERE customer_id = ${CUST_ID}
            AND status NOT IN ('closed', 'complete', 'done')
          ORDER BY CASE WHEN due_date < CURRENT_DATE THEN 0 ELSE 1 END, due_date
          LIMIT 5
        `, [name]),
        pool.query(`
          SELECT note, category, created_at
          FROM client_personal_notes
          WHERE customer_id = ${CUST_ID}
          ORDER BY created_at DESC LIMIT 5
        `, [name]),
        pool.query(`
          SELECT alert_type, severity, message, created_at
          FROM client_alerts
          WHERE customer_id = ${CUST_ID}
            AND resolved_at IS NULL
          ORDER BY CASE severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END
        `, [name]),
        pool.query(`
          SELECT * FROM get_dashboard_with_risk()
          WHERE customer_id = ${CUST_ID}
        `, [name]),
      ]);
      return {
        client: clientRow.rows[0] || null,
        leads: leadsRow.rows[0] || null,
        roas: roasRow.rows[0] || null,
        recent_interactions: interactionsRow.rows,
        open_tasks: tasksRow.rows,
        personal_notes: notesRow.rows,
        active_alerts: alertsRow.rows,
        risk_status: riskRow.rows[0] || null,
      };
    }

    case 'get_client_leads': {
      const name = `%${input.client_name}%`;
      const daysBack = input.days_back || 30;
      const sourceFilter = input.source_filter;
      let query = `
        SELECT c.customer_name, c.caller_phone, c.start_time, c.duration,
               c.classified_status, c.source, c.call_type, c.answered, c.call_summary,
               lp.current_stage
        FROM calls c
        LEFT JOIN v_lead_pipeline lp ON lp.callrail_id = c.callrail_id AND lp.customer_id = c.customer_id
        WHERE c.customer_id = ${CUST_ID}
          AND c.start_time >= CURRENT_DATE - $2::int * INTERVAL '1 day'
          AND c.classified_status NOT IN ('spam', 'irrelevant', 'brand')
      `;
      const params = [name, daysBack];
      if (sourceFilter) {
        query += ` AND c.source ILIKE $3`;
        params.push(`%${sourceFilter}%`);
      }
      query += ` ORDER BY c.start_time DESC LIMIT 50`;
      const { rows } = await pool.query(query, params);
      return rows;
    }

    case 'get_client_roas': {
      const name = `%${input.client_name}%`;
      const { rows } = await pool.query(`SELECT * FROM v_hcp_roas WHERE customer_id = ${CUST_ID}`, [name]);
      return rows[0] || { message: 'No ROAS data found for this client' };
    }

    case 'get_client_interactions': {
      const name = `%${input.client_name}%`;
      const limit = input.limit || 10;
      const { rows } = await pool.query(`
        SELECT interaction_type, summary, interaction_date, source,
               sentiment, action_items, attendees
        FROM client_interactions
        WHERE customer_id = ${CUST_ID}
        ORDER BY interaction_date DESC
        LIMIT $2
      `, [name, limit]);
      return rows;
    }

    case 'get_client_tasks': {
      const name = `%${input.client_name}%`;
      let statusFilter = `AND status NOT IN ('closed', 'complete', 'done')`;
      if (input.include_closed) statusFilter = '';
      const { rows } = await pool.query(`
        SELECT title, status, assigned_to, due_date, priority,
               CASE WHEN due_date < CURRENT_DATE THEN true ELSE false END as overdue
        FROM client_tasks
        WHERE customer_id = ${CUST_ID}
          ${statusFilter}
        ORDER BY CASE WHEN due_date < CURRENT_DATE THEN 0 ELSE 1 END, due_date
      `, [name]);
      return rows;
    }

    case 'get_client_personal_notes': {
      const name = `%${input.client_name}%`;
      const { rows } = await pool.query(`
        SELECT note, category, source, created_at
        FROM client_personal_notes
        WHERE customer_id = ${CUST_ID}
        ORDER BY created_at DESC
      `, [name]);
      return rows;
    }

    case 'get_risk_dashboard': {
      let query = `SELECT * FROM get_dashboard_with_risk()`;
      const params = [];
      if (input.client_name) {
        query += ` WHERE client_name ILIKE $1`;
        params.push(`%${input.client_name}%`);
      }
      if (input.status_filter) {
        query += params.length ? ' AND' : ' WHERE';
        query += ` status = $${params.length + 1}`;
        params.push(input.status_filter);
      }
      query += ` ORDER BY sort_priority, client_name`;
      const { rows } = await pool.query(query, params);
      return rows;
    }

    case 'get_manager_portfolio': {
      const mgr = `%${input.manager_name}%`;
      const { rows } = await pool.query(`
        SELECT
          c.name as client_name,
          c.budget, c.status as client_status,
          cp.monthly_retainer, cp.client_tier,
          COALESCE(r.roas_ratio, 0) as roas,
          COALESCE(r.ad_spend, 0) as ad_spend,
          d.status as risk_status, d.quality_leads, d.guarantee,
          (SELECT COUNT(*) FROM client_tasks ct
           WHERE ct.customer_id = c.customer_id
             AND ct.status NOT IN ('closed','complete','done')
             AND ct.due_date < CURRENT_DATE) as overdue_tasks,
          (SELECT COUNT(*) FROM client_alerts ca
           WHERE ca.customer_id = c.customer_id
             AND ca.resolved_at IS NULL) as active_alerts
        FROM clients c
        JOIN client_profiles cp ON cp.customer_id = c.customer_id
        LEFT JOIN v_hcp_roas r ON r.customer_id = c.customer_id
        LEFT JOIN get_dashboard_with_risk() d ON d.customer_id = c.customer_id
        WHERE cp.account_manager ILIKE $1
        ORDER BY c.name
      `, [mgr]);
      return rows;
    }

    case 'get_recent_activity': {
      const daysBack = input.days_back || 7;
      let whereClause = '';
      const params = [daysBack];
      if (input.client_name) {
        whereClause = `AND c.customer_id = (SELECT customer_id FROM clients WHERE name ILIKE $2 LIMIT 1)`;
        params.push(`%${input.client_name}%`);
      }
      const { rows } = await pool.query(`
        SELECT cl.name as client_name, 'lead' as activity_type,
               c.customer_name, c.start_time as activity_date, c.source
        FROM calls c
        JOIN clients cl ON cl.customer_id = c.customer_id
        WHERE c.start_time >= CURRENT_DATE - $1::int * INTERVAL '1 day'
          AND c.classified_status NOT IN ('spam', 'irrelevant', 'brand')
          ${whereClause}
        ORDER BY c.start_time DESC
        LIMIT 30
      `, params);
      return rows;
    }

    case 'get_estimates_pipeline': {
      let whereClause = '';
      const params = [];
      if (input.client_name) {
        whereClause = `WHERE cl.name ILIKE $1`;
        params.push(`%${input.client_name}%`);
      }
      const { rows } = await pool.query(`
        SELECT cl.name as client_name,
               eg.group_label, eg.total_amount / 100.0 as amount,
               eg.status, eg.sent_date,
               CURRENT_DATE - eg.sent_date::date as days_outstanding
        FROM v_estimate_groups eg
        JOIN clients cl ON cl.customer_id = eg.customer_id
        ${whereClause}
        ORDER BY eg.sent_date DESC
        LIMIT 30
      `, params);
      return rows;
    }

    case 'search_leads_by_phone': {
      const phone = input.phone_number.replace(/\D/g, '').slice(-10);
      const results = {};

      const calls = await pool.query(`
        SELECT c.customer_name, c.caller_phone, c.start_time, c.duration,
               c.source, c.classified_status, cl.name as client_name
        FROM calls c
        JOIN clients cl ON cl.customer_id = c.customer_id
        WHERE normalize_phone(c.caller_phone) = $1
        ORDER BY c.start_time DESC
      `, [phone]);
      results.calls = calls.rows;

      const hcp = await pool.query(`
        SELECT h.first_name, h.last_name, h.email, h.phone_normalized,
               cl.name as client_name,
               (SELECT COUNT(*) FROM hcp_estimates e WHERE e.customer_id = h.customer_id AND e.hcp_customer_id = h.id) as estimates,
               (SELECT COUNT(*) FROM hcp_jobs j WHERE j.customer_id = h.customer_id AND j.hcp_customer_id = h.id) as jobs
        FROM hcp_customers h
        JOIN clients cl ON cl.customer_id = h.customer_id
        WHERE h.phone_normalized = $1
      `, [phone]);
      results.hcp_customers = hcp.rows;

      const ghl = await pool.query(`
        SELECT g.contact_name, g.email, g.phone, g.tags,
               cl.name as client_name
        FROM ghl_contacts g
        JOIN clients cl ON cl.customer_id = g.customer_id
        WHERE normalize_phone(g.phone) = $1
      `, [phone]);
      results.ghl_contacts = ghl.rows;

      return results;
    }

    case 'run_custom_query': {
      let query = input.query.trim();
      // Safety: only allow SELECT statements
      if (!/^SELECT\s/i.test(query) || /;\s*\w/i.test(query)) {
        return { error: 'Only single SELECT statements are allowed.' };
      }
      if (/\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE)\b/i.test(query)) {
        return { error: 'Only SELECT queries are allowed. No mutations.' };
      }
      // Add LIMIT if not present
      if (!/\bLIMIT\b/i.test(query)) {
        query += ' LIMIT 50';
      }
      const { rows } = await pool.query(query);
      return rows;
    }

    case 'get_client_sentiment': {
      const name = `%${input.client_name}%`;

      const [clientRow, interactionsRow, notesRow, slackRow, crmRow, alertsRow, riskRow] = await Promise.all([
        pool.query(`
          SELECT c.name, c.status, c.start_date, c.budget,
                 cp.account_manager, cp.client_tier, cp.contract_renewal_date, cp.client_goals
          FROM clients c
          LEFT JOIN client_profiles cp ON cp.customer_id = c.customer_id
          WHERE c.customer_id = ${CUST_ID}
        `, [name]),

        pool.query(`
          SELECT interaction_type, summary, interaction_date, source,
                 sentiment, action_items
          FROM client_interactions
          WHERE customer_id = ${CUST_ID}
          ORDER BY interaction_date DESC
          LIMIT 10
        `, [name]),

        pool.query(`
          SELECT note, category, source, created_at
          FROM client_personal_notes
          WHERE customer_id = ${CUST_ID}
          ORDER BY created_at DESC
          LIMIT 15
        `, [name]),

        pool.query(`
          SELECT user_name, message_text, posted_at
          FROM slack_messages
          WHERE channel_id = (SELECT slack_channel_id FROM client_profiles WHERE customer_id = ${CUST_ID})
            AND posted_at > NOW() - INTERVAL '30 days'
          ORDER BY posted_at DESC
          LIMIT 20
        `, [name]),

        pool.query(`
          SELECT contact_name, direction, channel, message_body, message_date
          FROM crm_messages
          WHERE customer_id = ${CUST_ID}
          ORDER BY message_date DESC
          LIMIT 10
        `, [name]),

        pool.query(`
          SELECT alert_type, severity, message, created_at
          FROM client_alerts
          WHERE customer_id = ${CUST_ID}
            AND resolved_at IS NULL
          ORDER BY created_at DESC
        `, [name]),

        pool.query(`
          SELECT * FROM get_dashboard_with_risk()
          WHERE customer_id = ${CUST_ID}
        `, [name]),
      ]);

      return {
        client: clientRow.rows[0] || null,
        recent_interactions: interactionsRow.rows,
        personal_notes: notesRow.rows,
        recent_slack_messages: slackRow.rows,
        recent_crm_messages: crmRow.rows,
        active_alerts: alertsRow.rows,
        risk_status: riskRow.rows[0] || null,
      };
    }

    case 'get_funnel_constraint': {
      const name = `%${input.client_name}%`;

      const [clientFunnel, cohortStats] = await Promise.all([
        // Client's 30-day funnel metrics
        pool.query(`
          SELECT
            c.name as client_name,
            c.budget,
            c.field_management_software,
            d.quality_leads,
            d.prior_quality_leads,
            d.lead_volume_change,
            d.cpl,
            d.prior_cpl,
            d.ad_spend,
            d.call_answer_rate,
            d.total_insp_booked,
            d.insp_booked_pct as book_rate,
            d.on_cal_14d,
            d.on_cal_total,
            d.total_closed_rev,
            d.total_open_est_rev,
            d.roas,
            d.guarantee,
            d.days_since_lead,
            d.spam_rate,
            d.abandoned_rate,
            -- 30-day estimate funnel (HCP clients)
            (SELECT COUNT(*) FROM hcp_estimates e
             WHERE e.customer_id = c.customer_id
               AND e.created_at >= CURRENT_DATE - INTERVAL '30 days') as est_sent_30d,
            (SELECT COUNT(*) FROM hcp_estimates e
             WHERE e.customer_id = c.customer_id
               AND e.status = 'approved'
               AND e.created_at >= CURRENT_DATE - INTERVAL '30 days') as est_approved_30d,
            -- 30-day inspections that got an estimate
            (SELECT COUNT(DISTINCT lp.hcp_customer_id) FROM v_lead_pipeline lp
             WHERE lp.customer_id = c.customer_id
               AND lp.lead_at >= CURRENT_DATE - INTERVAL '30 days'
               AND lp.estimate_sent_at IS NOT NULL) as leads_with_estimate_30d,
            -- 30-day average job size from invoices
            (SELECT ROUND(AVG(total_amount_cents / 100.0), 2) FROM hcp_invoices i
             WHERE i.customer_id = c.customer_id
               AND i.created_at >= CURRENT_DATE - INTERVAL '30 days') as avg_job_size_30d,
            -- All-time average job size for comparison
            (SELECT ROUND(AVG(total_amount_cents / 100.0), 2) FROM hcp_invoices i
             WHERE i.customer_id = c.customer_id) as avg_job_size_alltime,
            -- Jobber 30-day estimates
            (SELECT COUNT(*) FROM jobber_quotes q
             WHERE q.customer_id = c.customer_id
               AND q.created_at >= CURRENT_DATE - INTERVAL '30 days') as jobber_quotes_30d,
            (SELECT COUNT(*) FROM jobber_quotes q
             WHERE q.customer_id = c.customer_id
               AND q.status = 'approved'
               AND q.created_at >= CURRENT_DATE - INTERVAL '30 days') as jobber_approved_30d
          FROM clients c
          JOIN get_dashboard_with_risk() d ON d.customer_id = c.customer_id
          WHERE c.name ILIKE $1
          LIMIT 1
        `, [name]),

        // Cohort averages and percentiles (all active clients with >10 leads)
        pool.query(`
          SELECT
            COUNT(*) as cohort_size,
            ROUND(AVG(quality_leads), 0) as avg_leads,
            ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY quality_leads)::numeric, 0) as median_leads,
            ROUND(AVG(insp_booked_pct::numeric), 3) as avg_book_rate,
            ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY insp_booked_pct)::numeric, 3) as median_book_rate,
            ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY insp_booked_pct)::numeric, 3) as book_rate_p25,
            ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY insp_booked_pct)::numeric, 3) as book_rate_p75,
            ROUND(AVG(cpl::numeric), 2) as avg_cpl,
            ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY cpl)::numeric, 2) as median_cpl,
            ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY cpl)::numeric, 2) as cpl_p25,
            ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY cpl)::numeric, 2) as cpl_p75,
            ROUND(AVG(roas::numeric), 2) as avg_roas,
            ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY roas)::numeric, 2) as median_roas,
            ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY roas)::numeric, 2) as roas_p25,
            ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY roas)::numeric, 2) as roas_p75
          FROM get_dashboard_with_risk()
          WHERE quality_leads > 10
        `),
      ]);

      return {
        client: clientFunnel.rows[0] || null,
        cohort: cohortStats.rows[0] || null,
      };
    }

    case 'get_client_contacts': {
      if (input.contact_name) {
        // Search for a person across all clients
        const { rows } = await pool.query(`
          SELECT cc.name as contact_name, cc.role, cc.phone, cc.email, cc.notes,
                 c.name as client_name
          FROM client_contacts cc
          JOIN clients c ON c.customer_id = cc.customer_id
          WHERE cc.name ILIKE $1 AND c.status = 'active'
          ORDER BY c.name
        `, [`%${input.contact_name}%`]);
        return rows;
      }
      if (input.client_name) {
        // Get all contacts for a specific client
        const { rows } = await pool.query(`
          SELECT cc.name as contact_name, cc.role, cc.phone, cc.email,
                 cc.is_primary, cc.notes
          FROM client_contacts cc
          WHERE cc.customer_id = ${CUST_ID}
          ORDER BY cc.is_primary DESC, cc.role
        `, [`%${input.client_name}%`]);
        return rows;
      }
      return { error: 'Provide either client_name or contact_name' };
    }

    case 'get_client_alerts': {
      let query = `
        SELECT ca.alert_type, ca.severity, ca.message, ca.created_at,
               cl.name as client_name
        FROM client_alerts ca
        JOIN clients cl ON cl.customer_id = ca.customer_id
        WHERE ca.resolved_at IS NULL
      `;
      const params = [];
      if (input.client_name) {
        query += ` AND cl.name ILIKE $${params.length + 1}`;
        params.push(`%${input.client_name}%`);
      }
      if (input.severity) {
        query += ` AND ca.severity = $${params.length + 1}`;
        params.push(input.severity);
      }
      query += ` ORDER BY CASE ca.severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END, ca.created_at DESC`;
      const { rows } = await pool.query(query, params);
      return rows;
    }

    default:
      return { error: `Unknown tool: ${toolName}` };
  }
}

module.exports = { TOOLS, executeToolQuery };
