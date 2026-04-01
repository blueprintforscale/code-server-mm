-- Client Intelligence Bot — Core Schema
-- Adds profile, interaction, task, alert, and personal notes tables
-- All tables reference clients(customer_id) for consistency with existing schema
--
-- Run: psql -U blueprint blueprint -f 001_client_intelligence_schema.sql

BEGIN;

-- ============================================================
-- 1. CLIENT PROFILES — extended metadata beyond clients table
-- ============================================================
CREATE TABLE IF NOT EXISTS client_profiles (
    customer_id         BIGINT PRIMARY KEY REFERENCES clients(customer_id),
    account_manager     TEXT,
    google_ads_manager  TEXT,
    monthly_retainer    NUMERIC(10,2),
    contract_renewal_date DATE,
    billing_status      TEXT DEFAULT 'current'
                        CHECK (billing_status IN ('current', 'overdue', 'paused', 'cancelled')),
    client_tier         TEXT DEFAULT 'standard'
                        CHECK (client_tier IN ('gold', 'silver', 'bronze', 'standard')),
    onboarding_status   TEXT DEFAULT 'active'
                        CHECK (onboarding_status IN ('onboarding', 'active', 'offboarding', 'paused', 'churned')),
    slack_channel_id    TEXT,
    slack_channel_name  TEXT,
    clickup_space_id    TEXT,
    clickup_folder_id   TEXT,
    clickup_list_id     TEXT,
    fireflies_team_id   TEXT,
    google_calendar_id  TEXT,
    client_goals        TEXT,           -- "wants 20 leads/month, focused on treatment jobs"
    preferences         TEXT,           -- "prefers text over email, don't call Mondays"
    notes               TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- 2. CLIENT CONTACTS — people at the client's business
-- ============================================================
CREATE TABLE IF NOT EXISTS client_contacts (
    id                  SERIAL PRIMARY KEY,
    customer_id         BIGINT NOT NULL REFERENCES clients(customer_id),
    ghl_contact_id      TEXT UNIQUE,
    name                TEXT NOT NULL,
    role                TEXT,           -- owner, office_manager, tech, spouse, etc.
    phone               TEXT,
    phone_normalized    TEXT,
    email               TEXT,
    preferred_channel   TEXT,           -- call, text, email
    notes               TEXT,
    is_primary          BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_client_contacts_customer
    ON client_contacts(customer_id);
CREATE INDEX IF NOT EXISTS idx_client_contacts_phone
    ON client_contacts(phone_normalized);

-- ============================================================
-- 3. CLIENT INTERACTIONS — calls, meetings, emails, Slack threads
-- ============================================================
CREATE TABLE IF NOT EXISTS client_interactions (
    id                  SERIAL PRIMARY KEY,
    customer_id         BIGINT NOT NULL REFERENCES clients(customer_id),
    interaction_type    TEXT NOT NULL
                        CHECK (interaction_type IN ('call', 'meeting', 'email', 'slack_thread', 'note')),
    interaction_date    TIMESTAMPTZ NOT NULL,
    logged_by           TEXT,           -- team member who logged or was on the call
    attendees           TEXT[],         -- ["Sarah", "Mike", "John (client)"]
    summary             TEXT,
    action_items        TEXT,
    sentiment           TEXT CHECK (sentiment IN ('positive', 'neutral', 'negative', 'at_risk')),
    follow_up_date      DATE,
    source              TEXT,           -- fireflies, slack, gmail, manual
    source_id           TEXT,           -- external ID for dedup (fireflies meeting ID, slack thread_ts, etc.)
    recording_url       TEXT,
    transcript          TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_client_interactions_customer
    ON client_interactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_client_interactions_date
    ON client_interactions(interaction_date DESC);
CREATE INDEX IF NOT EXISTS idx_client_interactions_source
    ON client_interactions(source, source_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_client_interactions_dedup
    ON client_interactions(source, source_id) WHERE source_id IS NOT NULL;

-- ============================================================
-- 4. CLIENT PERSONAL NOTES — the "secret weapon"
-- ============================================================
CREATE TABLE IF NOT EXISTS client_personal_notes (
    id                  SERIAL PRIMARY KEY,
    customer_id         BIGINT NOT NULL REFERENCES clients(customer_id),
    note                TEXT NOT NULL,
    category            TEXT DEFAULT 'personal'
                        CHECK (category IN ('personal', 'preference', 'business_change', 'milestone')),
    source              TEXT,           -- slack, call, email, manual
    source_id           TEXT,           -- dedup reference
    captured_date       DATE DEFAULT CURRENT_DATE,
    captured_by         TEXT,           -- who noticed / which system extracted it
    auto_extracted      BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_client_personal_notes_customer
    ON client_personal_notes(customer_id);

-- ============================================================
-- 5. CLIENT TASKS — synced from ClickUp
-- ============================================================
CREATE TABLE IF NOT EXISTS client_tasks (
    id                  SERIAL PRIMARY KEY,
    customer_id         BIGINT NOT NULL REFERENCES clients(customer_id),
    clickup_task_id     TEXT UNIQUE,
    task_type           TEXT DEFAULT 'custom'
                        CHECK (task_type IN ('routine', 'custom', 'website_edit', 'one_off', 'milestone')),
    title               TEXT NOT NULL,
    description         TEXT,
    status              TEXT DEFAULT 'todo'
                        CHECK (status IN ('todo', 'in_progress', 'done', 'blocked', 'cancelled')),
    assigned_to         TEXT,
    due_date            DATE,
    completed_date      DATE,
    priority            TEXT CHECK (priority IN ('urgent', 'high', 'normal', 'low')),
    program_milestone   TEXT,           -- "Month 3: launch retargeting campaign"
    tags                TEXT[],
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_client_tasks_customer
    ON client_tasks(customer_id);
CREATE INDEX IF NOT EXISTS idx_client_tasks_status
    ON client_tasks(status) WHERE status NOT IN ('done', 'cancelled');
CREATE INDEX IF NOT EXISTS idx_client_tasks_due
    ON client_tasks(due_date) WHERE status NOT IN ('done', 'cancelled');

-- ============================================================
-- 6. CLIENT ALERTS — auto-generated and manual flags
-- ============================================================
CREATE TABLE IF NOT EXISTS client_alerts (
    id                  SERIAL PRIMARY KEY,
    customer_id         BIGINT NOT NULL REFERENCES clients(customer_id),
    alert_type          TEXT NOT NULL,
        -- Auto: no_leads, roas_low, spend_spike, lead_drop, contract_expiring,
        --       overdue_task, no_interaction, uninvoiced_estimate
        -- Manual: churn_risk, billing_dispute, client_complaint, escalation
    severity            TEXT DEFAULT 'info'
                        CHECK (severity IN ('info', 'warning', 'critical')),
    message             TEXT NOT NULL,
    auto_generated      BOOLEAN DEFAULT FALSE,
    resolved_at         TIMESTAMPTZ,
    resolved_by         TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_client_alerts_customer
    ON client_alerts(customer_id);
CREATE INDEX IF NOT EXISTS idx_client_alerts_open
    ON client_alerts(severity, created_at DESC) WHERE resolved_at IS NULL;

-- ============================================================
-- 7. SLACK MESSAGE LOG — raw messages from client channels
-- ============================================================
CREATE TABLE IF NOT EXISTS slack_messages (
    id                  SERIAL PRIMARY KEY,
    customer_id         BIGINT REFERENCES clients(customer_id),
    channel_id          TEXT NOT NULL,
    message_ts          TEXT NOT NULL,       -- Slack's unique timestamp ID
    thread_ts           TEXT,                -- parent thread if this is a reply
    user_id             TEXT,
    user_name           TEXT,
    message_text        TEXT,
    has_files           BOOLEAN DEFAULT FALSE,
    reactions           JSONB,
    posted_at           TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(channel_id, message_ts)
);

CREATE INDEX IF NOT EXISTS idx_slack_messages_customer
    ON slack_messages(customer_id);
CREATE INDEX IF NOT EXISTS idx_slack_messages_channel_ts
    ON slack_messages(channel_id, posted_at DESC);
CREATE INDEX IF NOT EXISTS idx_slack_messages_thread
    ON slack_messages(channel_id, thread_ts) WHERE thread_ts IS NOT NULL;

-- ============================================================
-- 8. PULL LOG — track ETL runs for each source
-- ============================================================
CREATE TABLE IF NOT EXISTS client_intelligence_pull_log (
    id                  SERIAL PRIMARY KEY,
    source              TEXT NOT NULL,       -- slack, clickup, fireflies, calendar, gmail
    customer_id         BIGINT REFERENCES clients(customer_id),
    started_at          TIMESTAMPTZ DEFAULT NOW(),
    finished_at         TIMESTAMPTZ,
    records_processed   INTEGER DEFAULT 0,
    errors              TEXT[],
    status              TEXT DEFAULT 'running'
                        CHECK (status IN ('running', 'completed', 'failed'))
);

-- ============================================================
-- 9. VIEWS — quick access for the bot and dashboard
-- ============================================================

-- Client health snapshot
CREATE OR REPLACE VIEW v_client_health AS
SELECT
    c.customer_id,
    c.name AS client_name,
    c.client_state,
    c.start_date,
    cp.account_manager,
    cp.google_ads_manager,
    cp.monthly_retainer,
    cp.contract_renewal_date,
    cp.billing_status,
    cp.client_tier,
    cp.onboarding_status,
    cp.slack_channel_name,
    cp.client_goals,
    -- Program month
    CASE WHEN c.start_date IS NOT NULL
        THEN EXTRACT(MONTH FROM age(CURRENT_DATE, c.start_date))::INTEGER + 1
        ELSE NULL
    END AS program_month,
    -- Days until contract renewal
    CASE WHEN cp.contract_renewal_date IS NOT NULL
        THEN (cp.contract_renewal_date - CURRENT_DATE)
        ELSE NULL
    END AS days_until_renewal,
    -- Last interaction
    (SELECT MAX(interaction_date) FROM client_interactions ci
     WHERE ci.customer_id = c.customer_id) AS last_interaction_date,
    -- Days since last interaction
    (SELECT EXTRACT(DAY FROM NOW() - MAX(interaction_date))::INTEGER
     FROM client_interactions ci
     WHERE ci.customer_id = c.customer_id) AS days_since_interaction,
    -- Open tasks count
    (SELECT COUNT(*) FROM client_tasks ct
     WHERE ct.customer_id = c.customer_id
       AND ct.status NOT IN ('done', 'cancelled')) AS open_tasks,
    -- Overdue tasks count
    (SELECT COUNT(*) FROM client_tasks ct
     WHERE ct.customer_id = c.customer_id
       AND ct.status NOT IN ('done', 'cancelled')
       AND ct.due_date < CURRENT_DATE) AS overdue_tasks,
    -- Open alerts count
    (SELECT COUNT(*) FROM client_alerts ca
     WHERE ca.customer_id = c.customer_id
       AND ca.resolved_at IS NULL) AS open_alerts,
    -- Critical alerts count
    (SELECT COUNT(*) FROM client_alerts ca
     WHERE ca.customer_id = c.customer_id
       AND ca.resolved_at IS NULL
       AND ca.severity = 'critical') AS critical_alerts,
    -- Last sentiment
    (SELECT sentiment FROM client_interactions ci
     WHERE ci.customer_id = c.customer_id
       AND ci.sentiment IS NOT NULL
     ORDER BY ci.interaction_date DESC LIMIT 1) AS last_sentiment
FROM clients c
LEFT JOIN client_profiles cp ON cp.customer_id = c.customer_id
WHERE c.status = 'active'
ORDER BY c.name;

-- Clients needing attention (for "who needs attention today?" query)
CREATE OR REPLACE VIEW v_clients_needing_attention AS
SELECT
    ch.*,
    CASE
        WHEN ch.critical_alerts > 0 THEN 'critical'
        WHEN ch.overdue_tasks > 2 THEN 'high'
        WHEN ch.days_since_interaction > 14 THEN 'high'
        WHEN ch.last_sentiment = 'at_risk' THEN 'high'
        WHEN ch.days_until_renewal IS NOT NULL AND ch.days_until_renewal <= 30 THEN 'medium'
        WHEN ch.open_alerts > 0 THEN 'medium'
        WHEN ch.overdue_tasks > 0 THEN 'medium'
        WHEN ch.days_since_interaction > 7 THEN 'low'
        ELSE 'none'
    END AS attention_level,
    -- Reason summary
    ARRAY_REMOVE(ARRAY[
        CASE WHEN ch.critical_alerts > 0
            THEN ch.critical_alerts || ' critical alert(s)' END,
        CASE WHEN ch.overdue_tasks > 0
            THEN ch.overdue_tasks || ' overdue task(s)' END,
        CASE WHEN ch.days_since_interaction > 14
            THEN 'No interaction in ' || ch.days_since_interaction || ' days' END,
        CASE WHEN ch.last_sentiment = 'at_risk'
            THEN 'Last sentiment: at risk' END,
        CASE WHEN ch.days_until_renewal IS NOT NULL AND ch.days_until_renewal <= 30
            THEN 'Contract renews in ' || ch.days_until_renewal || ' days' END
    ], NULL) AS attention_reasons
FROM v_client_health ch
WHERE ch.critical_alerts > 0
   OR ch.overdue_tasks > 0
   OR ch.days_since_interaction > 7
   OR ch.last_sentiment = 'at_risk'
   OR (ch.days_until_renewal IS NOT NULL AND ch.days_until_renewal <= 30)
   OR ch.open_alerts > 0
ORDER BY
    CASE
        WHEN ch.critical_alerts > 0 THEN 1
        WHEN ch.overdue_tasks > 2 OR ch.days_since_interaction > 14 OR ch.last_sentiment = 'at_risk' THEN 2
        WHEN ch.days_until_renewal IS NOT NULL AND ch.days_until_renewal <= 30 THEN 3
        ELSE 4
    END,
    ch.client_name;

COMMIT;
