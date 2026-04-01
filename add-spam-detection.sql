-- ============================================================================
-- Spam Detection for Form Submissions
-- ============================================================================
-- Adds a reusable is_gibberish() function, an is_spam column on
-- form_submissions, backfills existing spam, and updates v_weekly_leads
-- to exclude spam from all reports/dashboards.
-- ============================================================================

-- ── 1. Gibberish detection function ─────────────────────────────────────────
-- Returns TRUE if a name looks like random keyboard mashing.
-- Heuristics:
--   a) Word has 6+ consecutive consonants
--   b) Vowel ratio < 20% in a word of 8+ chars
--   c) Both "words" in a two-word name are 8+ chars with low vowel ratio
-- ──────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION is_gibberish(name TEXT) RETURNS BOOLEAN AS $$
DECLARE
    clean TEXT;
    words TEXT[];
    word TEXT;
    vowel_count INT;
    word_len INT;
BEGIN
    IF name IS NULL OR LENGTH(TRIM(name)) < 6 THEN
        RETURN FALSE;
    END IF;

    clean := UPPER(TRIM(name));

    -- Check for 6+ consecutive consonants (no vowels) in any part
    IF clean ~ '[B-DF-HJ-NP-TV-Z]{6,}' THEN
        RETURN TRUE;
    END IF;

    -- Split into words and check each
    words := string_to_array(clean, ' ');

    FOR i IN 1..array_length(words, 1) LOOP
        word := words[i];
        word_len := LENGTH(word);

        -- Skip short words
        IF word_len < 8 THEN
            CONTINUE;
        END IF;

        -- Count vowels
        vowel_count := LENGTH(word) - LENGTH(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(word, 'A', ''), 'E', ''), 'I', ''), 'O', ''), 'U', ''));

        -- Very low vowel ratio = gibberish
        IF vowel_count::FLOAT / word_len < 0.18 THEN
            RETURN TRUE;
        END IF;
    END LOOP;

    RETURN FALSE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Quick test
-- SELECT is_gibberish('XPYKKMJKNUUJBMGAAEI ZXBWFXBUFACBQJQVDZSJG');  -- TRUE
-- SELECT is_gibberish('Marguerite Collingwood');  -- FALSE
-- SELECT is_gibberish('PRECIOUS DICKERSON-SMOOT');  -- FALSE
-- SELECT is_gibberish('TBKITJGVPZQNMUFWSUN JKGUPDMKRRTUXFEADI');  -- TRUE

-- ── 2. Add is_spam column ───────────────────────────────────────────────────
ALTER TABLE form_submissions ADD COLUMN IF NOT EXISTS is_spam BOOLEAN DEFAULT FALSE;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS is_spam BOOLEAN DEFAULT FALSE;

-- ── 3. Backfill: flag existing spam form submissions ────────────────────────
UPDATE form_submissions
SET is_spam = TRUE
WHERE is_gibberish(customer_name)
  AND is_spam = FALSE;

-- ── 4. Update v_weekly_leads to exclude spam ────────────────────────────────
CREATE OR REPLACE VIEW v_weekly_leads AS
 SELECT cl.customer_id,
    'call'::text AS lead_type,
    c.callrail_id AS lead_id,
    COALESCE(
        NULLIF(TRIM(BOTH FROM ((hc.first_name || ' '::text) || hc.last_name)), ''::text),
        NULLIF(TRIM(BOTH FROM ((jc.first_name || ' '::text) || jc.last_name)), ''::text),
        NULLIF(TRIM(BOTH FROM c.customer_name), ''::text),
        c.caller_phone
    ) AS contact_name,
    c.caller_phone AS phone,
    c.start_time AS lead_date,
    c.source AS source,
    c.source_name AS source_detail,
    c.classified_status AS call_status,
    c.first_call,
    ((c.lead_score ->> 'is_good_lead'::text))::boolean AS is_good_lead,
    gc.lost_reason,
    opp.stage_name AS ghl_stage,
    opp.status AS ghl_status
   FROM (((((calls c
     JOIN clients cl ON ((cl.callrail_company_id = c.callrail_company_id)))
     LEFT JOIN hcp_customers hc ON (((hc.phone_normalized = normalize_phone(c.caller_phone)) AND (hc.customer_id = cl.customer_id))))
     LEFT JOIN jobber_customers jc ON (((jc.phone_normalized = normalize_phone(c.caller_phone)) AND (jc.customer_id = cl.customer_id))))
     LEFT JOIN ghl_contacts gc ON (((gc.phone_normalized = normalize_phone(c.caller_phone)) AND (gc.customer_id = cl.customer_id))))
     LEFT JOIN ghl_opportunities opp ON (((opp.ghl_contact_id = gc.ghl_contact_id) AND (opp.customer_id = cl.customer_id))))
  WHERE cl.status = 'active'::text
    AND COALESCE(c.is_spam, FALSE) = FALSE
UNION ALL
 SELECT cl.customer_id,
    'form'::text AS lead_type,
    f.callrail_id AS lead_id,
    COALESCE(
        NULLIF(TRIM(BOTH FROM ((hc.first_name || ' '::text) || hc.last_name)), ''::text),
        NULLIF(TRIM(BOTH FROM ((jc.first_name || ' '::text) || jc.last_name)), ''::text),
        NULLIF(TRIM(BOTH FROM f.customer_name), ''::text),
        f.customer_phone
    ) AS contact_name,
    f.customer_phone AS phone,
    f.submitted_at AS lead_date,
        CASE
            WHEN ((f.source ~~* '%google%'::text) OR (f.medium = 'cpc'::text) OR (f.gclid IS NOT NULL)) THEN 'google_ads'::text
            ELSE 'other'::text
        END AS source,
    f.source_name AS source_detail,
    NULL::text AS call_status,
    f.first_form AS first_call,
    NULL::boolean AS is_good_lead,
    gc.lost_reason,
    opp.stage_name AS ghl_stage,
    opp.status AS ghl_status
   FROM (((((form_submissions f
     JOIN clients cl ON ((cl.callrail_company_id = f.callrail_company_id)))
     LEFT JOIN hcp_customers hc ON (((hc.phone_normalized = normalize_phone(f.customer_phone)) AND (hc.customer_id = cl.customer_id))))
     LEFT JOIN jobber_customers jc ON (((jc.phone_normalized = normalize_phone(f.customer_phone)) AND (jc.customer_id = cl.customer_id))))
     LEFT JOIN ghl_contacts gc ON (((gc.phone_normalized = normalize_phone(f.customer_phone)) AND (gc.customer_id = cl.customer_id))))
     LEFT JOIN ghl_opportunities opp ON (((opp.ghl_contact_id = gc.ghl_contact_id) AND (opp.customer_id = cl.customer_id))))
  WHERE cl.status = 'active'::text
    AND COALESCE(f.is_spam, FALSE) = FALSE;
