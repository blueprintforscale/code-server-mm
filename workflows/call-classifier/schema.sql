-- Call Classifier Pipeline Schema
-- Run: psql -U blueprint blueprint -f schema.sql

-- Add CallRail fields to clients table
ALTER TABLE clients ADD COLUMN IF NOT EXISTS callrail_company_id TEXT;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS conversion_value NUMERIC(10,2) DEFAULT 1.00;

-- Populate 3 test clients
UPDATE clients SET callrail_company_id = 'COM6264de5022534b00843cb1663019277f'
WHERE customer_id = 9699974772;

UPDATE clients SET callrail_company_id = 'COM995abb7e06ca41d78c1c9669478e8365'
WHERE customer_id = 6213328850;

UPDATE clients SET callrail_company_id = 'COMdaf9301c562e44429b56d50f93a42bf3'
WHERE customer_id = 7123434733;

-- Calls table: tracks full lifecycle of each call
CREATE TABLE IF NOT EXISTS calls (
    id SERIAL PRIMARY KEY,
    callrail_id TEXT UNIQUE NOT NULL,
    callrail_company_id TEXT NOT NULL,
    customer_id BIGINT REFERENCES clients(customer_id),
    caller_phone TEXT,
    gclid TEXT,
    start_time TIMESTAMPTZ,
    duration INTEGER,
    transcript TEXT,
    classification TEXT,  -- 'spam', 'legitimate', 'error', NULL=pending
    classification_reason TEXT,
    classification_attempts INTEGER DEFAULT 0,
    uploaded_to_gads BOOLEAN DEFAULT FALSE,
    upload_error TEXT,
    conversion_value NUMERIC(10,2),
    source TEXT,
    medium TEXT,
    first_call BOOLEAN,            -- CallRail: is this the caller's first call?
    callrail_status TEXT,          -- Derived: answered, missed, abandoned
    call_type TEXT,                -- CallRail call_type: e.g. voicemail_transcription, abandoned
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE calls ADD COLUMN IF NOT EXISTS first_call BOOLEAN;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS callrail_status TEXT;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS call_type TEXT;

CREATE INDEX IF NOT EXISTS idx_calls_classification ON calls(classification);
CREATE INDEX IF NOT EXISTS idx_calls_customer_id ON calls(customer_id);
CREATE INDEX IF NOT EXISTS idx_calls_start_time ON calls(start_time);
CREATE INDEX IF NOT EXISTS idx_calls_upload_pending ON calls(classification, uploaded_to_gads) WHERE classification = 'legitimate' AND uploaded_to_gads = FALSE;

-- Form submissions table: tracks form leads from CallRail
CREATE TABLE IF NOT EXISTS form_submissions (
    id SERIAL PRIMARY KEY,
    callrail_id TEXT UNIQUE NOT NULL,
    callrail_company_id TEXT NOT NULL,
    customer_id BIGINT REFERENCES clients(customer_id),
    customer_name TEXT,
    customer_email TEXT,
    customer_phone TEXT,
    gclid TEXT,
    form_data JSONB,
    form_url TEXT,
    submitted_at TIMESTAMPTZ,
    source TEXT,
    medium TEXT,
    campaign TEXT,
    classification TEXT,  -- 'spam', 'legitimate', NULL=pending
    classification_reason TEXT,
    classification_attempts INTEGER DEFAULT 0,
    uploaded_to_gads BOOLEAN DEFAULT FALSE,
    upload_error TEXT,
    conversion_value NUMERIC(10,2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_forms_classification ON form_submissions(classification);
CREATE INDEX IF NOT EXISTS idx_forms_customer_id ON form_submissions(customer_id);
CREATE INDEX IF NOT EXISTS idx_forms_submitted_at ON form_submissions(submitted_at);
CREATE INDEX IF NOT EXISTS idx_forms_upload_pending ON form_submissions(classification, uploaded_to_gads) WHERE classification = 'legitimate' AND uploaded_to_gads = FALSE;

-- Pipeline run log (mirrors pull_log pattern)
CREATE TABLE IF NOT EXISTS call_pipeline_log (
    id SERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT DEFAULT 'running',  -- 'running', 'completed', 'completed_with_errors', 'failed'
    calls_fetched INTEGER DEFAULT 0,
    calls_classified INTEGER DEFAULT 0,
    calls_spam INTEGER DEFAULT 0,
    calls_legitimate INTEGER DEFAULT 0,
    calls_uploaded INTEGER DEFAULT 0,
    errors TEXT[],
    clients_processed TEXT[]
);
