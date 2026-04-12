# Blueprint for Scale — Business Rules Reference

> **Single source of truth** for all business logic, thresholds, and classification rules.
> Any agent, dashboard, or view that implements these rules should reference this document.
> Last updated: 2026-03-23

---

## Table of Contents

1. [Phone Normalization](#1-phone-normalization)
2. [CallRail Matching (HCP + Jobber)](#2-callrail-matching)
3. [Job vs Inspection Classification](#3-job-vs-inspection-classification)
4. [Estimate Type Classification](#4-estimate-type-classification)
5. [Invoice Type Classification](#5-invoice-type-classification)
6. [ROAS Revenue Calculation](#6-roas-revenue-calculation)
7. [GBP Exclusion from ROAS](#7-gbp-exclusion-from-roas)
8. [LSA Exclusion from CPL & ROAS](#8-lsa-exclusion-from-cpl--roas)
9. [Contacts vs Quality Leads](#9-contacts-vs-quality-leads)
10. [Segment Detection & Grouping](#10-segment-detection--grouping)
11. [Nesting Rules (Address-Based)](#11-nesting-rules-address-based)
12. [count_revenue Auto-Fix Rules](#12-count_revenue-auto-fix-rules)
13. [Record Status Values](#13-record-status-values)
14. [Funnel Counting Rules](#14-funnel-counting-rules)
15. [Pipeline Inference](#15-pipeline-inference)
16. [Lead Date Calculation](#16-lead-date-calculation)
17. [Risk & Flag Scoring](#17-risk--flag-scoring) *(thresholds in separate file: [RISK_THRESHOLDS.md](RISK_THRESHOLDS.md))*
18. [Exception Flags](#18-exception-flags)
19. [VA Review Queue Filter](#19-va-review-queue-filter)
20. [VA Review Actions & Database Effects](#20-va-review-actions--database-effects)
21. [Spam Detection](#21-spam-detection)
22. [Lead Attribution & Source Detection](#22-lead-attribution--source-detection)
23. [Manual Override System](#23-manual-override-system)
24. [Client Portal Rules](#24-client-portal-rules)
25. [Phone Priority](#25-phone-priority)
26. [Call Classification](#26-call-classification)
27. [First Contact Detection](#27-first-contact-detection)
28. [Large Account Handling](#28-large-account-handling)
29. [Guarantee Calculation](#29-guarantee-calculation)
30. [Date Conventions](#30-date-conventions)
31. [Display Standards](#31-display-standards)
32. [On Calendar Metric](#32-on-calendar-metric)
33. [GHL-as-CRM Integration](#33-ghl-as-crm-integration)
34. [Slack Alert System](#34-slack-alert-system)
35. [Data Ownership — Dashboard vs Call Classifier](#35-data-ownership--dashboard-vs-call-classifier)
36. [Answer Rate Detection](#36-answer-rate-detection)

---

## 1. Phone Normalization

**Canonical implementation:** SQL function `normalize_phone()`, replicated in Python.

**Logic:**
1. Strip all non-digit characters
2. Take the rightmost 10 digits (handles leading `1` in 11-digit numbers)

```
SQL:  RIGHT(regexp_replace(phone, '[^0-9]', '', 'g'), 10)
Python: re.sub(r'\D', '', phone)[-10:]
```

**Where used:** Every matching operation, every phone comparison, all CallRail joins.
**Source files:**
- SQL: `hcp-sync/migrations/003_hcp_schema.sql`
- Python (HCP): `hcp-sync/pull_hcp_data.py`
- Python (GHL): `ghl-sync/pull_ghl_data.py`
- Python (Jobber): `jobber-sync/pull_jobber_data.py`

---

## 2. CallRail Matching

**Applies to:** Both HCP (`hcp_customers`) and Jobber (`jobber_customers`). Same 4-step logic (Steps 1-4). Step 5 applies to HCP and Jobber clients (uses GHL contacts as a bridge). Does not apply to GHL-as-CRM clients (Pamela, Brawley) since the GHL contact already has the correct phone.

**Priority order (first match wins, per customer):**

| Step | Source Table | Match Field | Method |
|------|-------------|-------------|--------|
| 1 | `calls` | `normalize_phone(caller_phone)` = `phone_normalized` | Earliest call wins |
| 2 | `form_submissions` | `LOWER(customer_email)` = `LOWER(email)` | Only if Step 1 unmatched |
| 3 | `form_submissions` | `normalize_phone(customer_phone)` = `phone_normalized` | Only if Steps 1-2 unmatched |
| 4 | `webflow_submissions` | `phone_normalized` match + `gclid IS NOT NULL` | Only if Steps 1-3 unmatched |
| 5 | `ghl_contacts` → `calls` | Name match (GHL bridge) | Only if Steps 1-4 unmatched |

**Result:** Sets `callrail_id` and `match_method` ('phone', 'email', 'webflow', 'name') on the customer record.

**Webflow special case:** Webflow matches use synthetic ID: `'WF_' || webflow_submissions.id` as the callrail_id.

**Step 5 — Name match via GHL (HCP-only):**

When Steps 1-4 fail to match an HCP customer to CallRail, attempt a name-based match using GHL contacts as a bridge:

1. Find a `ghl_contacts` record where `LOWER(TRIM(first_name))` and `LOWER(TRIM(last_name))` exactly match the HCP customer
2. The GHL contact must have a different `phone_normalized` than the HCP customer (same phone would have matched in Step 1)
3. The GHL contact's phone must match a CallRail `calls` record via `normalize_phone(caller_phone)`
4. The CallRail call must be **within 3 days** of `hcp_created_at` (using `ABS(EXTRACT(EPOCH FROM hcp_created_at - start_time)) <= 3 * 86400`)
5. The name must be **unique** within that customer account for the 3-day window — if multiple HCP customers or multiple GHL contacts share the same first+last name, skip the match (flag for manual review instead)

**Why this exists:** Clients sometimes enter a lead in HCP with a different phone number than the one they called from (home vs. cell, typos). The GHL contact has the correct phone from CallRail but the HCP record doesn't. This bridges the gap using the name as a cross-reference.

**match_method:** `'name'` — distinguishes these from phone/email/webflow matches for auditing.

**Source files:**
- HCP: `hcp-sync/pull_hcp_data.py` → `match_callrail()` (~line 705)
- Jobber: `jobber-sync/pull_jobber_data.py` → `match_callrail()` (~line 607)

---

## 3. Job vs Inspection Classification

**Applies to:** HCP jobs that need to be categorized as treatment work vs. inspection/testing work.

**Function:** `classify_job()` in `~/projects/workflows/hcp-sync/classifier.py`

**Signal priority** (first rule to match wins; all rules evaluated in order):

| Priority | Signal | Result |
|---|---|---|
| 1 | `status IN ('user canceled', 'pro canceled', 'canceled')` | **canceled** (excluded from funnel) |
| 2 | `parent_job_id IS NOT NULL` (segment inherits from parent) | inherit parent's `work_category` |
| 3 | `hcp_jobs.job_type` (HCP custom field set by client) matches treatment/inspection keywords | client's pick wins |
| 3b | **Linked estimate option** (the `est_xxx` ID the job was created from). The option's own tags/name/message is checked — top signal for clients like Pure Air Pros who use estimate-driven workflows. Became the #1 signal for Sy Elijah (1,171 / 4,059 jobs). | option's category |
| 4 | Job `tags` contain a treatment tag (`Mold Treatment`, `Water Mitigation`, `Crawl Space`, `Encapsulation`, `Remediation`, `Retreatment`, `Warranty Mold Treatment`, `Containment`, `Demolition`, `Dehumidifier`) OR an inspection tag (`Inspection`, `Assessments & Testing`, `Assessment`, `Testing`, `Mold Test`, `Air Quality Test`) | first match wins |
| 4a | Plumber-referral tag patterns: tag matches `^BDR[\s-]`, `^Plumber[\s-]`, `^PBR[\s-]?`, or contains `Plumber Warranty` (always water mitigation referrals) | **treatment** |
| 4b | `Dehumidifier` tag alone (no other tag) with no clarifying description | **unknown** — VA review |
| 5 | **Line items** (all of them, via  table populated from ) — per-item majority by $ with priority inspection phrases overriding treatment branding | first match wins |
| 6 | Description keyword matching (after stripping HCP boilerplate like "Work Authorization - Anticipated Scope and Terms & Conditions"): priority phrases `pre-treatment\|air quality test\|mold test\|before & after mold test` on jobs <$1k → inspection; otherwise treatment keywords → treatment; otherwise inspection keywords → inspection | first match wins |
| 7 | Linked full estimate (via csr_xxx, for future use) | — |
| 8 | Amount floor: ≥$10k → treatment; $1k–$10k → treatment (mid-$ rescue); <$1k → inspection; $0 with no signals → **unknown** (VA review) | amount-based |

**Treatment keywords:** `remediation`, `dry fog`, `treatment`, `removal`, `abatement`, `encapsulation`, `instapure`, `everpure`, `containment`, `demolition`, `demo`, `retreatment`, `water mitigation`, `mold remediation`, `mold treatment`, `crawl space encapsulation`, `crawlspace door`

**Inspection keywords:** `assessment`, `inspection`, `test`, `evaluat`, `consult`, `survey`, `sample`, `sampling`, `walk-through`, `instascope`, `scan`, `moisture check`, `mold report`, `clearance`, `ermi`, `visual assess`, `air quality`, `air test`

**Priority inspection phrases** (override treatment branding on small-$ jobs): `pre-treatment`, `air quality test`, `air test`, `mold test`, `testing + estimate`, `visual assessment`, `complimentary estimate`, `before & after mold test`

**HCP boilerplate descriptions** (stripped as zero-signal): `Work Authorization - Anticipated Scope and Terms & Conditions`, `Work Authorization`, `Terms and Conditions`, `Terms & Conditions`, empty string.

**Stored on `hcp_jobs`:**
- `work_category TEXT` — treatment | inspection | canceled | unknown
- `review_needed BOOLEAN` — true for VA queue
- `review_reason TEXT` — why classifier couldn't decide
- `classifier_signal TEXT` — which rule fired (e.g., `linked_option`, `tags`, `amount_mid_default`)
- `classified_at TIMESTAMPTZ`

**Source files:**
- `~/projects/workflows/hcp-sync/classifier.py` — `classify_job()`
- `~/projects/workflows/hcp-sync/pull_hcp_data.py` — `upsert_job()` calls the classifier at ingest and looks up the linked option via `hcp_estimate_options.hcp_option_id = hcp_jobs.original_estimate_id`

---

## 4. Estimate Type Classification

**Applies to:** HCP estimates, used to separate treatment estimates from inspection-only estimates.

**Function:** `classify_estimate()` in `~/projects/workflows/hcp-sync/classifier.py`

**Approach:** Per-option evaluation. An estimate often has multiple options (e.g., the original inspection tier plus an upgraded treatment tier). We evaluate each option independently:
1. Skip "dirty" options (options that have an explicit inspection tag, priority inspection phrase in name, or inspection keyword without a treatment keyword)
2. For any "clean" option, check for a treatment signal in this priority:
   - Treatment tag on the option → treatment
   - Plumber-referral tag pattern on the option → treatment
   - Treatment keyword in non-boilerplate option name → treatment
   - Treatment keyword in `message_from_pro` → treatment
   - Option amount ≥ $1,000 → treatment (`option_amount_treatment`)
3. If no option yielded a treatment signal, fall back to estimate-level inspection signals (first inspection tag found → inspection; first inspection phrase/keyword in a name → inspection)
4. Linked job fallback: if any job created from this estimate has `work_category='treatment'` → treatment
5. Top-level amount fallback (for estimates with no active options): use `highest_option_cents` on the estimate row itself, with the same ≥$10k / ≥$1k / <$1k tiers

**Boilerplate option names** (skipped in keyword matching): `Option #1`, `Option #2`, `Option #3`, `Copy of Option #1`, `Worksheet`, `Standard`.

**Why per-option matters:** Clients often show an inspection quote AND a treatment quote in the same estimate. A first-option-wins classifier misclassifies these as inspection. The per-option approach correctly identifies the estimate as treatment if ANY clean option signals treatment — matching the real business intent ("was a treatment option offered?").

**Stored on `hcp_estimates`:**
- `work_category TEXT` — treatment | inspection | canceled | unknown
- `review_needed` / `review_reason` / `classifier_signal` / `classified_at` — same shape as jobs
- Legacy `estimate_type TEXT` is kept in sync (`v_estimate_groups` reads it, `mv_funnel_leads` joins via the group view)

**Impact:** Estimate type determines which revenue bucket feeds ROAS (treatment vs inspection).

**Source files:**
- `~/projects/workflows/hcp-sync/classifier.py` — `classify_estimate()`
- `~/projects/workflows/hcp-sync/pull_hcp_data.py` — `upsert_estimate()` builds option dicts and calls the classifier at ingest

---

## 5. Invoice Type Classification

**Applies to:** HCP invoices, determines if revenue is treatment or inspection.

**Function:** `classify_invoice()` in `~/projects/workflows/hcp-sync/classifier.py`

**Approach:** Line-item majority. Every invoice in `hcp_invoice_items` is summed into category buckets, and whichever has more $ wins.

**Line item keyword rules:**
- **Priority inspection phrases** (override treatment branding): `air quality test`, `mold test`, `tape test`, `tape sample`, `petri`, `swab`, `before & after mold test`. These win over treatment keywords because e.g. "Mold Remediation - Air Quality Test/ Tape Test Sample" is a test despite the "remediation" branding.
- **Treatment keywords:** `remediation`, `treatment`, `removal`, `abatement`, `encapsulation`, `demolition`, `retreatment`, `dehumidifier`, `vapor barrier`, `insulation`, `dry fog`, `fog`, `instapure`, `everpure`, `install`, `containment`, `vaporshield`, `pure install`, `crawl space debris`, `janitorial`, `viper`
- **Inspection keywords:** `inspection`, `assessment`, `evaluation`, `consultation`, `survey`, `sample`, `test`, `moisture check`, `mold report`, `clearance`, `ermi`, `visual`, `walk-through`, `instascope`

**Fallback priority:**
1. `status IN ('canceled', 'voided')` → canceled
2. No line items in our DB → linked job's `work_category`
3. No line items → amount floor (<$1k = inspection, ≥$1k = treatment, $0 = unknown)
4. Sum line items by category → majority wins
5. Tie → linked job category → amount floor

**Stored on `hcp_invoices`:**
- `work_category TEXT` + `review_needed` / `review_reason` / `classifier_signal` / `classified_at`
- Legacy `invoice_type TEXT` is kept in sync for existing consumers (mv_funnel_leads, review app, risk dashboard)

**Impact:** Invoice type determines which bucket (`insp_invoice_cents` vs `treat_invoice_cents`) feeds `mv_funnel_leads` and therefore ROAS revenue.

**Backfill impact:** 2,887 invoices reclassified vs the old heuristic (had a linked job → treatment, no linked job → inspection), primarily correcting no-linked-job invoices that actually have treatment line items.

**Source files:**
- `~/projects/workflows/hcp-sync/classifier.py` — `classify_invoice()`
- `~/projects/workflows/hcp-sync/pull_hcp_data.py` — `upsert_invoice()` builds line item dicts from the HCP API response and calls the classifier at ingest

---

## 5b. mv_funnel_leads Fallback Rules (added 2026-04-11)

Since HCP job records are sometimes incomplete (e.g., Pure Air Pros bills treatment via estimate and leaves the job record at $0), `mv_funnel_leads` applies fallback signals:

```
has_job_scheduled = EXISTS(qualifying treatment job: work_category='treatment', amount ≥ $1k, valid status)
                 OR EXISTS(approved treatment estimate ≥ $1k)

has_job_completed = EXISTS(qualifying completed treatment job: work_category='treatment', status 'complete rated'/'complete unrated', amount ≥ $1k)
                 OR EXISTS(treatment invoice > $0, invoice_type='treatment', status not canceled/voided)
```

This means a customer with an approved treatment estimate but no qualifying HCP job still counts as `job_scheduled`. Same for treatment invoices → `job_completed`. Catches the Pure Air Pros workflow where work is committed via estimate approval rather than a formal job record.


**GREATEST threshold (added 2026-04-11):** Estimate approval threshold uses `GREATEST(approved_total_cents, highest_option_cents) >= $1k` instead of just `approved_total_cents`. This catches the "Work Authorization" pattern where the client clicks approve on a $0 authorization option, but the actual pricing option has real $$. Applied in both `mv_funnel_leads.sql` and `apps/blueprintos-api/index.js` (6 occurrences each).

**Same-day tolerance (added 2026-04-11):** The GA touch-time check on estimates uses DATE comparison (`eg.sent_at::date >= lb.first_ga_touch_time::date`) instead of timestamp, so estimates created within minutes of the GA touch on the same day always pass. Previously, a 2-minute difference (estimate at 3:40 PM, GA touch at 3:42 PM) would exclude the estimate.

**Referral source (added 2026-04-11):** `lead_source = 'referral'` is set when a GHL contact has `LOWER(source) = 'lead source form'`. Referral leads have no CallRail trail — funnel uses `AND 1=0` for unmatched calls/forms (only HCP-matched leads count). Source tab enabled for Sy Elijah Atlanta + Raleigh.

**Source file:** `~/projects/workflows/views/mv_funnel_leads.sql`

---

## 6. ROAS Revenue Calculation

**The most critical business rule.** Determines revenue attributed to each lead for return-on-ad-spend.

**Formula:**
```
roas_revenue = inspection_invoice_cents
             + GREATEST(treatment_invoice_cents, approved_estimate_cents)

If BOTH treatment_invoice_cents = 0 AND approved_estimate_cents = 0:
  Fallback: job_total_cents + inspection_total_cents (where count_revenue = true)
```

**In plain English:**
1. Always include inspection invoice revenue (any amount)
2. For treatment revenue, take whichever is higher: actual invoices paid OR approved estimate amount
3. If neither exists (no invoices, no approved estimate), fall back to raw job totals

**Exclusions:**
- Canceled invoices/jobs excluded
- Records with `count_revenue = false` excluded
- Records with `record_status != 'active'` excluded
- GBP-only leads excluded (see Section 7)
- **Post-touch rule (mv_funnel_leads only):** For Google Ads leads, only invoices/estimates created after `first_ga_touch_time` count. See Section 22 "Post-Touch Revenue Rule".
- **Reactivation exclusion (mv_funnel_leads only):** Leads flagged with `exclude_from_ga_roas = true` should be filtered out of ROAS calculations. See Section 22 "Reactivation Protocol".

**Source files:**
- `risk-dashboard/fix_lead_revenue.sql` → view `v_lead_revenue` (older, does not include post-touch or reactivation rules)
- `mv_funnel_leads` materialized view (canonical, includes all rules)

**Note:** `v_lead_revenue` and `get_dashboard_metrics()` do not yet implement post-touch or reactivation rules. They will show slightly higher ROAS than BlueprintOS. Unification planned — see Section 22.

---

## 7. GBP Exclusion from ROAS

**Rule:** Google Business Profile leads are tracked but excluded from ROAS calculations because there's no ad spend to measure return against.

**GBP detection:**
- Call `source_name` matches: `%gmb%`, `%gbp%`, or `'Main Business Line'`

**Multi-touch override:**
- If the same phone number has a PRIOR call with a GCLID from a non-GBP source → classified as `google_ads` (the ad drove the initial awareness)
- Otherwise → classified as `google_business_profile` (excluded from ROAS)

**Source file:** `risk-dashboard/fix_lead_revenue.sql` (lines 18-51)

---

## 8. LSA Exclusion from CPL & ROAS

**Rule:** When LSA (Local Service Ads) campaigns are running, their spend and leads are excluded from CPL and ROAS calculations. These metrics should reflect the main Google Ads account only, unless explicitly stated otherwise.

**How it works:**
- Ad spend is pulled from `campaign_daily_metrics` table
- LSA campaigns are identified by `campaign_type = 'LOCAL_SERVICES'`
- Main ad spend: `WHERE campaign_type != 'LOCAL_SERVICES'` — **excludes LSA**
- LSA spend: tracked separately as `lsa_spend` metric

**CPL calculation:**
```
CPL = ad_spend (non-LSA only) / quality_leads (non-LSA only)
```
- `quality_leads` counts distinct phones from calls with `classified_source = 'google_ads'` + forms with GCLIDs
- LSA calls (`source_name = 'LSA'`) are NOT counted in quality_leads
- LSA spend is NOT in the denominator

**ROAS calculation:**
```
ROAS = period_google_ads_revenue / ad_spend (non-LSA only)
```
- Only revenue from `lead_source_type = 'google_ads'` is included
- LSA leads are classified as `lead_source_type = 'lsa'` and contribute $0 to ROAS revenue
- LSA spend is NOT in the denominator

**LSA metrics tracked separately:**
- `lsa_spend` — total LSA campaign spend for the period
- `lsa_leads` — distinct legitimate LSA calls for the period

**Key principle:** CPL and ROAS are Google Ads performance metrics. LSA is a separate channel with its own economics. Mixing them would make both metrics meaningless.

**Source file:** `risk-dashboard/migration.sql` (lines 128-156, 427-484)

---

## 9. Contacts vs Quality Leads

**These are two different metrics.** The DB column name `quality_leads` is misleading — it's actually the raw contacts count.

### Contacts (shown as "Contacts" on risk dashboard)
- **DB column:** `quality_leads` (confusing name — it's really ALL contacts)
- **Definition:** Distinct phone numbers from Google Ads calls + GCLID forms in the period
- **Includes:** Spam, abandoned, wrong numbers — everything
- **Excludes:** LSA calls, non-GA calls, forms without GCLIDs
- **Dedup:** By normalized phone number. If same phone called AND submitted a form, counted once (forms deduped against calls)

```
Contacts = COUNT(DISTINCT phone) FROM (
  calls WHERE classified_source = 'google_ads'
  UNION ALL
  forms WHERE gclid IS NOT NULL
    AND phone NOT IN (calls already counted)
)
```

### Quality Leads (shown as "Quality" on risk dashboard)
- **DB column:** `actual_quality_leads` (computed in SQL, not frontend)
- **Definition:** Contacts minus contacts confirmed as spam/junk in GHL — literal period-specific counts
- **Matching:** GA contacts are matched against GHL by **phone** (all contacts) AND **email** (form leads without phone). A contact is spam if:
  1. **Contact lost reason:** `ghl_contacts.lost_reason` (the "Lost Reason - Official" custom field) contains: "spam", "not a lead", "wrong number", "out of area", or "wrong service" (case-insensitive)
  2. **Opportunity stage:** Any `ghl_opportunities` record for that contact has a `stage_name` containing: "spam", "not a lead", "out of area", or "wrong service" (case-insensitive)
  3. **Client-specific keywords:** `clients.extra_spam_keywords` array matches lost_reason (e.g., Liz & Scott: `{abandoned}`)
  4. **Abandoned > 20% rule:** When a client's period-scoped abandoned rate exceeds 20%, GHL contacts with `lost_reason` containing "abandoned" are reclassified as spam and excluded from quality leads (see Abandoned Rate below)
- **Formula:** `Quality = Contacts - Spam Contacts` (actual counts, not rate estimates)
- **Spam list is all-time:** If a phone was ever flagged as spam in GHL, it's excluded from quality leads in any period (permanent marking)

```sql
-- Spam identified via GHL: by phone (CTE: ghl_spam_phones) + by email (CTE: ghl_spam_emails)
-- Phone matching (all contacts):
SELECT DISTINCT customer_id, phone_normalized FROM ghl_contacts gc
WHERE lost_reason SIMILAR TO '%(spam|not a lead|wrong number|out of area|wrong service)%'
   OR EXISTS (SELECT 1 FROM ghl_opportunities o
              WHERE o.ghl_contact_id = gc.ghl_contact_id
                AND stage_name SIMILAR TO '%(spam|not a lead|out of area|wrong service)%')
   OR (extra_spam_keywords match)

-- Email matching (form leads without phone, CTE: ghl_spam_emails):
-- Same keyword logic but matches on LOWER(gc.email)

-- Abandoned-as-spam (CTE: ghl_abandoned_as_spam):
-- When abandoned_rate > 0.20, phones with lost_reason LIKE '%abandoned%' added to spam

-- Quality = contacts NOT in any spam list (phone OR email)
actual_quality_leads = COUNT(DISTINCT phone) WHERE phone NOT IN (ghl_spam_phones + ghl_abandoned_as_spam)
                                               AND email NOT IN ghl_spam_emails
```

### Abandoned Rate (shown as "Aband %" on risk dashboard)
- **Definition:** GA contacts matched to GHL abandoned / total GA contacts in the period
- **Period-scoped:** Uses same date window as the dashboard (default: last 30 days by lead create date)
- **Abandoned identification:** GA contact phone matched against `ghl_contacts` where:
  - `ghl_opportunities.status = 'abandoned'` for that contact, OR
  - `ghl_contacts.lost_reason` contains "abandoned"
- **GA-only:** Only counts Google Ads leads (calls with `classified_source = 'google_ads'` + forms with GCLID). Non-GA abandoned opps do not count.
- **Denominator:** Total GA contacts (the `quality_leads` / contacts count)
- **Flag trigger:** >11% triggers a flag in `compute_risk_status`

### Abandoned > 20% Reclassification Rule
- When a client's **period-scoped** abandoned rate exceeds 20%, GHL contacts with `lost_reason` containing "abandoned" are **reclassified as spam**
- These contacts are removed from quality lead counts (added to `ghl_abandoned_as_spam` CTE)
- The threshold is evaluated per-period — a December spike does not affect February counts
- **CTE:** `abandoned_rates` computes the rate, `ghl_abandoned_as_spam` applies the reclassification

### CRM Activity Override (applies to ALL spam/abandoned filtering)

**Rule:** If a lead has **any** active record in HCP or Jobber — inspections, estimates, invoices, or jobs — it is **never** excluded by the GHL spam filter, regardless of lost_reason or abandoned rate.

**Rationale:** If the client actually did business with a lead (booked an inspection, sent an estimate, completed a job), then the GHL spam/abandoned label is either a data entry error or a pipeline status (e.g., "Abandoned" meaning "stopped responding"), not a true spam classification. The CRM activity is the stronger signal.

**Activity checks:**

| CRM | Tables Checked |
|-----|---------------|
| HCP | `hcp_inspections` (active), `hcp_estimates` (active/option), `hcp_invoices` (non-canceled, amount > 0), `hcp_jobs` (active) |
| Jobber | `jobber_quotes` (any), `jobber_jobs` (any) |

**Scope:** Applies everywhere GHL spam filtering occurs:
- Funnel `matched_leads` CTE (`spamExclude` variable)
- `unmatched_count` CTE (inline spam checks)
- `spam_excluded` CTE (spam counting)
- Lead spreadsheet endpoint

**Impact:** ~156 leads across 21 clients rescued from incorrect spam filtering (as of 2026-03-24).

**Source file:** `blueprintos-api/index.js` — all `spamExclude` variables and inline spam patterns

### BlueprintOS Dashboard: Contacts vs Quality Leads (Added 2026-04-01)

The BlueprintOS funnel and lead trend chart now correctly separate contacts from quality leads:

- **Contacts card**: Total GA contacts (calls + forms) before ANY exclusions — includes spam, abandoned, everything
- **Quality leads card / funnel top**: Contacts minus spam minus excluded abandoned
- **Lead trend chart**: Shows quality leads per month (spam + excluded_abandoned subtracted). Uses a **trailing 3-month window** to compute the abandoned rate for each month (current month + up to 2 prior months). This smooths the threshold so it does not flip on/off between adjacent months, preventing artificial spikes or cliffs in the trendline.

**`extra_spam_keywords` support:** The funnel endpoint now respects `clients.extra_spam_keywords`. For clients with `{abandoned}`, all abandoned leads are excluded from quality regardless of the 20% threshold (e.g., Liz & Scott).

**`excluded_abandoned` field:** The monthly trend API returns this per-month field. It equals `abandoned` when:
1. Client has `extra_spam_keywords` containing 'abandoned', OR
2. The month's abandoned rate exceeds 20%

Otherwise `excluded_abandoned = 0` (abandoned counted as quality).

**Applies to:** All client types (HCP, Jobber, GHL). HCP funnel includes `unmatched_excluded` CTE to correctly count unmatched CallRail leads filtered by spam/abandoned phone lists in the contacts total.

**Source files:**
- `blueprintos-api/index.js` — `getHcpFunnel()`, `getJobberFunnel()`, `getGhlFunnel()`, monthly-trend endpoint
- `MonthlyTrendChart.tsx` — quality = leads - spam - excluded_abandoned
- `HistoricalPerformance.tsx` — same logic for Leads metric

### mv_funnel_leads Materialized View (Updated 2026-04-01)

The `mv_funnel_leads` materialized view now has split spam columns:
- **`ghl_spam`**: Core spam only (spam, not a lead, wrong number, out of area, wrong service) + GHL opportunity stage matching. Does NOT include abandoned.
- **`ghl_abandoned`**: Abandoned via lost_reason or opportunity status. Only excluded when period abandoned rate > 20%.

The `getHcpFunnel()` function in the BlueprintOS API implements:
1. **Core spam exclusion** with CRM activity rescue (leads with HCP inspections/estimates/invoices/jobs are never excluded)
2. **20% abandoned rule** via `abandoned_rate` CTE (period-scoped)
3. **Unmatched lead spam filtering** via `ghl_spam_phones` + `ghl_abandoned_phones` CTEs

### Guarantee Calculation (Updated 2026-04-01)

Revenue and ad spend for the guarantee metric are **capped at the first 12 months** from the client's `start_date`. This ensures the guarantee measures performance within the guarantee period, not all-time.

- **`guarantee_period` CTE**: Computes `start_date + 12 months` as `end_date`
- Revenue: Only HCP leads created before `end_date`
- Ad spend: Only daily metrics before `end_date`
- `months_in_program` returned in funnel response for frontend display

### Spam Rate (shown as "Spam %" on risk dashboard)
- **Definition:** `spam_contacts / quality_leads` (actual spam contacts divided by total contacts in the period)
- **Includes** abandoned-as-spam contacts when the >20% threshold is active
- **Not cumulative** — reflects the actual spam ratio for the selected date range
- Clients with no GHL data will show 0% (no way to identify spam without GHL)

### CPL (Cost Per Lead)
- **Uses actual quality leads:** `CPL = ad_spend / actual_quality_leads`
- If no quality leads, CPL = 0
- No rate-based estimation or fallback needed

### Lead Volume Change
- **30-day rolling comparison** of actual quality leads (current vs prior 30-day window)
- Both periods subtract their own spam contacts independently
```
lead_volume_change = (current_actual_quality - prior_actual_quality) / prior_actual_quality
```

### Book Rate
- **Uses actual quality leads:** `book_rate = inspections_booked / actual_quality_leads`

### Important Notes
- Quality is computed entirely in SQL (CTEs `ghl_spam_phones` + `ghl_spam_emails` + `ghl_abandoned_as_spam` + `lead_counts`), not in the frontend
- "Contacts" on the dashboard = ALL Google Ads contacts including spam
- "Quality" on the dashboard = contacts minus GHL-confirmed spam (real count)
- Spam identification is **phone-based AND email-based**: a GA contact is spam if the same phone OR email appears on a spam-flagged GHL contact
- Abandoned rate is **GA-only and period-scoped** — not all GHL opps
- Clients without GHL data: Quality = Contacts (no spam can be identified), Abandoned = 0%
- Risk scoring (`compute_risk_status`) receives `actual_quality_leads` for lead count triggers

**Source files:**
- Spam phone identification: `risk-dashboard/migration.sql` (CTE `ghl_spam_phones`)
- Spam email identification: `risk-dashboard/migration.sql` (CTE `ghl_spam_emails`)
- Abandoned matching: `risk-dashboard/migration.sql` (CTEs `ga_abandoned_contacts`, `abandoned_rates`, `ghl_abandoned_as_spam`)
- Contact + quality counting: `risk-dashboard/migration.sql` (CTE `lead_counts` — `actual_quality_leads`, `spam_contacts`)
- CPL / book rate / lead volume: `risk-dashboard/migration.sql` (final SELECT in `get_dashboard_metrics`)
- Drilldown spam/email matching: `risk-dashboard/server.js` (CTEs `spam_phones`, `spam_emails`, `client_aband_rate`)
- Frontend display: `risk-dashboard/public/index.html` (`normalizeRow` function)

---
## 10. Segment Detection & Grouping

### Automatic (ETL)
**Invoice number dash rule:** Jobs with invoice numbers containing `-` are segments.
- `"12345"` → parent job
- `"12345-2"` → segment of parent, `segment_number = 2`

Parent job ID resolved by matching the base invoice number (left of dash).

### Manual (VA Review)
When the VA sees multiple records at the same address, they can:
- **Group** items → first becomes parent, rest become segments/options
- **Keep separate** → dismisses the fragmentation flag
- **Flag** → escalates to manager

**Record status after grouping:**
- Parent: `record_status = 'active'`
- Segments: `record_status = 'segment'` (jobs) or `record_status = 'option'` (estimates)

**Source file:** `hcp-sync/pull_hcp_data.py` (~line 750)

---

## 11. Nesting Rules (Address-Based)

**For VA review decisions:**

| Scenario | Action |
|----------|--------|
| Same address, close dates (within weeks) | **Group** — likely segments of one project |
| Same address, months apart | **Keep separate** — likely return customer or new project |
| Different addresses | **Always keep separate** — different projects |
| Long time gap at same address | **Keep separate** — restart the count |

**Source:** VA review app card logic + training guide

---

## 12. count_revenue Auto-Fix Rules

**Applied automatically during ETL sync.** These prevent double-counting.

| # | Rule | Sets count_revenue to |
|---|------|-----------------------|
| 1 | Canceled jobs (`status IN ('user canceled', 'pro canceled')`) | `false` |
| 2 | Canceled inspections (same statuses) | `false` |
| 3 | $0 estimates (`estimate_type = 'unknown'`) | `false` |
| 4 | Canceled segments | `false` |
| 5 | Segments within 10% of parent job amount (`segment >= parent * 0.9`) | `false` |
| 6 | Restored: approved treatment estimates (any amount) that were previously $0/unknown placeholders | `true` |

**Rule 6 detail (updated 2026-04-11):** When an estimate starts as a $0 placeholder (triggering rule 3), but the classifier later sets `estimate_type = 'treatment'` and the estimate status becomes `approved`, the ETL restores `count_revenue = true` **regardless of dollar amount**. This catches the Pure Air Pros "Work Authorization" pattern where the approved option is $0 (an authorization document) but the real pricing is on the invoice. Previously required `approved_total_cents >= $1,000` which missed 749 estimates cross-client. The `estimate_type = 'treatment'` classifier now handles treatment/inspection separation, making the dollar floor redundant for approved estimates.

**Revenue exceeds estimate flag:** If total segment revenue > 120% of approved estimate, flag `revenue_exceeds_estimate` is added.

**Source file:** `hcp-sync/pull_hcp_data.py` (auto-fix section, ~line 1100+)

---

## 13. Record Status Values

| Status | Meaning | Counts toward revenue? |
|--------|---------|----------------------|
| `active` | Primary record | Yes (if count_revenue = true) |
| `segment` | Part of a grouped job | Yes (if count_revenue = true) |
| `option` | Estimate option (grouped under parent) | Only if approved |
| `excluded` | Manually excluded by reviewer | No |

**Source file:** `hcp-sync/migrations/003_hcp_schema.sql`

---

## 14. Funnel Counting Rules

**Counts are CUMULATIVE** (ever reached that stage, not current status). Revenue is POINT-IN-TIME.

| Metric | How Counted |
|--------|-------------|
| Inspection Scheduled | Any inspection with status `scheduled` or `completed`, OR `scheduled_at IS NOT NULL` (fallback for HCP status overwrites like `created job from estimate`) |
| Inspection Completed | Any inspection with status `completed` |
| Estimate Sent | Treatment estimates only (estimate_type = 'treatment', i.e. >= $1,000). Inspection/testing estimates excluded from funnel stages but still counted in revenue. |
| Estimate Approved | Treatment estimates with status `approved` AND approved_total >= $1,000. Catches multi-option estimates where only the inspection option (<$1K) was approved. |
| Job Scheduled | Distinct non-segment jobs with status `scheduled` or `completed` |
| Job Completed | Distinct non-segment jobs with status `completed` |
| Closed Revenue | Inspection invoices < $1,000 + approved estimate totals |

**Key:** Segments (`is_segment = true`) are excluded from job counts but included in revenue. Options count as 1 estimate (approved option wins).

**Source file:** `hcp-sync/dashboard_views.sql` → view `v_hcp_funnel`

---

## 14.5. Phone Dedup in Funnel Counts

**Applies to:** HCP clients in BlueprintOS funnel dashboard (`getHcpFunnel` in `blueprintos-api/index.js`).

**Problem:** HCP sometimes has multiple customer records for the same phone number (e.g., auto-created from caller ID + manually entered by client). The `phone_groups` CTE in `mv_funnel_leads` merges invoices/estimates/jobs across all records for a phone via `all_ids`, but outputs one row per HCP customer. This causes double-counting in funnel stages and revenue.

**Rule:** The funnel dashboard deduplicates `mv_funnel_leads` by `phone_normalized` before counting. One phone = one lead.

**Record selection priority (DISTINCT ON phone_normalized ORDER BY):**
1. Named records preferred over anonymous/caller-ID (`first_name NOT SIMILAR TO '%(Wireless|Caller)%'`)
2. Records with email preferred over records without
3. Earliest `hcp_created_at` wins ties

**Revenue is not lost:** Because `phone_groups.all_ids` already includes all HCP customer IDs for a phone, the surviving row's invoice/estimate/job lookups see all records. Even if invoices were split across two HCP records, the deduped row captures both.

**Where applied:**
- `matched` → `matched_deduped` CTE (period funnel counts + revenue)
- `spam_excluded` CTE (total contacts count)
- `all_time_rev` CTE (guarantee ROAS calculation)

**Where NOT applied:**
- `mv_funnel_leads` view itself (keeps all records for auditing)
- Drill-down drawers (may show individual records)
- Risk dashboard (uses its own queries)

**Source:** `blueprintos-api/index.js` → `getHcpFunnel()` (~line 410)

---

## 15. Pipeline Inference

**When actual dates are missing, the system infers them from downstream events.**

| Missing Date | Inferred From | Condition |
|-------------|---------------|-----------|
| Inspection scheduled | MIN(hcp_created_at, estimate_sent_at, invoice_date) | Inspection record exists + downstream activity |
| Inspection completed | Treatment estimate sent_at | **Treatment** estimate exists but no completion date (inspection-fee estimates do NOT count) |
| Inspection completed | Job exists | Any active job record for this customer |
| Inspection completed | Invoice date | Invoice exists but no completion date |

**What does NOT trigger inspection completion inference:**
- **Inspection-type estimates** (e.g., $100 inspection fee) — these are the inspection itself, not proof it was completed
- **Past scheduled date** — an inspection scheduled 2+ days ago is not proof it happened; the client may have canceled or rescheduled

**Implementation:** `infer_inspection_completions()` PostgreSQL function (3 signals: treatment estimate, job, invoice). Called by ETL after HCP data sync.

**Funnel stage priority (highest wins):**
1. Job paid → 2. Job completed → 3. Job scheduled → 4. Estimate approved → 5. Estimate sent → 6. Inspection completed → 7. Inspection paid → 8. Inspection scheduled → 9. Lead → 10. Unknown

**Flags:** `inspection_scheduled_inferred`, `inspection_completed_inferred` (true/false/null)

**Source file:** `risk-dashboard/fix_pipeline_inference.sql` → view `v_lead_pipeline`

---

## 16. Lead Date Calculation

```
lead_date = LEAST(callrail_first_contact_time, hcp_customer_created_at)
```

Whichever comes first: the first CallRail call/form OR when the customer was created in HCP.

**Source file:** `hcp-sync/dashboard_views.sql`

---

## 17. Risk & Flag Scoring

**Full thresholds are in a separate file:** [`RISK_THRESHOLDS.md`](RISK_THRESHOLDS.md)

These thresholds are specific to the risk dashboard and are NOT needed for general data pulls or dashboard building. Only reference them when working on risk scoring or the risk dashboard itself.

**Key concepts (quick reference):**
- **Ads Risk** = any ads risk trigger fired (lead count, CPL, lead volume drop, days since lead, $0 spend)
- **Funnel Risk** = presentation risk (guarantee + trailing ROAS stories all failing) or guarantee < 0.5x at month 7+
- **Both Risk** = ads + funnel triggers both fired
- **Flag** = ≥3 flag triggers → client needs monitoring
- **Healthy** = no issues

**Ads risk triggers (pure ads metrics only):**
- Lead count too low (stage-dependent thresholds)
- CPL > $170 (downgraded to flag if ROAS > 3x for CRM-connected clients)
- Lead volume drop > 30%
- Days since last lead (≥7 at months 4+, >10 at months 1-3)
- $0 spend with budget set

**NOT ads risk triggers (removed 2026-03-21):**
- Budget over/underspend — admin issues, not ad performance
- ROAS, book rate, guarantee — funnel metrics, not ads
- On-calendar — funnel metric

**Funnel risk triggers (presentation story framework):**
- Guarantee < 0.5x at month 7+ (non-negotiable)
- Presentation risk: no good ROAS story available (see below)

**Three stories that save a client from presentation risk:**
1. **Lifetime story** — guarantee meets threshold (1.5x/2x/2.5x by stage)
2. **Ramp-up story** — trailing 6mo or 3mo closed ROAS meets threshold (2x/2.5x/3x by stage)
3. **Potential story** — trailing 6mo or 3mo potential ROAS (closed + open estimates) meets ramp-up threshold

Presentation risk only fires when ALL THREE stories fail at months 5+.

**Trailing ROAS (new 2026-03-21):**
- 6-month trailing ROAS: primary dashboard metric, used in funnel risk evaluation
- 3-month trailing ROAS: shown on dashboard with trend arrow (▲/▼ vs 6-month), used as additional escape hatch
- Potential ROAS: closed + open estimate pipeline, shown when meaningfully higher than closed

**Risk override:** `clients.risk_override` column — `'risk'`, `'flag'`, `'healthy'`, or `NULL` (computed)

**Client-specific spam keywords:** `clients.extra_spam_keywords` array — per-client lost reasons to exclude from quality leads (e.g., Liz & Scott: `{abandoned}`). Note: the abandoned > 20% reclassification rule (see Rule 9) makes manual `{abandoned}` keywords redundant for clients that cross the threshold, but the keyword still applies when the rate is under 20%.

**Parent/child accounts:** `clients.parent_customer_id` — child account data rolls up into parent for all metrics. Child accounts excluded from dashboard rows. (e.g., Daniel Clay: Georgia rolls into Pure Air Pros)

**Source file:** `risk-dashboard/migration.sql` → function `compute_risk_status()`

---
## 18. Exception Flags

**Automated flags set during ETL, used to populate VA review queue.**

| Flag | Trigger | VA Actionable? |
|------|---------|---------------|
| `no_phone` | HCP customer has no phone number | No (info only) |
| `no_phone_match` | Has phone but no CallRail call matched | No (info only) |
| `classification_fallback` | Used amount-based fallback (Priority 4) to classify | Yes (verify/reclassify) |
| `name_mismatch` | CallRail caller name ≠ HCP customer name | Yes (usually fine) |
| `missing_funnel_step` | Quality lead with job >$2K but no estimate | Yes (verify pipeline) |
| `job_no_estimate` | Has job but no estimate record | Yes (verify) |
| `segment_revenue_suspicious` | Segment revenue > parent job revenue | Yes (check amounts) |
| `revenue_exceeds_estimate` | Segment total > 120% of approved estimate | Yes (check amounts) |
| `pre_lead` | HCP/Jobber customer created 7+ days before first CallRail contact | Yes (verify attribution) |
| `invoice_mismatch` | Invoice total ≠ expected amount | No (accounting) |
| `invoice_below_estimate` | Invoice less than approved estimate | No (accounting) |

**Pre-lead rule detail:** If the customer record in HCP or Jobber was created more than 7 days before their first CallRail call, they're flagged as a pre-existing customer. This means they were in the field management system before any ad-driven contact — their revenue may not be attributable to Google Ads. Shown as an amber "Pre-lead" pill on dashboard drilldowns and VA review cards.

**Applies to:** Both HCP (`hcp_customers.hcp_created_at`) and Jobber (`jobber_customers.jobber_created_at`)

**Step 5 — Name match via GHL (HCP-only):**

When Steps 1-4 fail to match an HCP customer to CallRail, attempt a name-based match using GHL contacts as a bridge:

1. Find a `ghl_contacts` record where `LOWER(TRIM(first_name))` and `LOWER(TRIM(last_name))` exactly match the HCP customer
2. The GHL contact must have a different `phone_normalized` than the HCP customer (same phone would have matched in Step 1)
3. The GHL contact's phone must match a CallRail `calls` record via `normalize_phone(caller_phone)`
4. The CallRail call must be **within 3 days** of `hcp_created_at` (using `ABS(EXTRACT(EPOCH FROM hcp_created_at - start_time)) <= 3 * 86400`)
5. The name must be **unique** within that customer account for the 3-day window — if multiple HCP customers or multiple GHL contacts share the same first+last name, skip the match (flag for manual review instead)

**Why this exists:** Clients sometimes enter a lead in HCP with a different phone number than the one they called from (home vs. cell, typos). The GHL contact has the correct phone from CallRail but the HCP record doesn't. This bridges the gap using the name as a cross-reference.

**match_method:** `'name'` — distinguishes these from phone/email/webflow matches for auditing.

**Source files:**
- HCP: `hcp-sync/pull_hcp_data.py` → `detect_exceptions()` (~line 933)
- Jobber: `jobber-sync/pull_jobber_data.py` (~line 793)

---

## 19. VA Review Queue Filter

**What puts a lead in the "Need Review" queue:**

```
review_status = 'pending'
AND (
  exception_flags includes: missing_funnel_step, job_no_estimate, segment_revenue_suspicious, pre_lead
  OR ($0 revenue with active non-canceled job)
  OR (multiple active inspections > 1)
  OR (multiple active estimates > 1)
  OR (multiple active jobs > 1)
)
```

**Sort order:** Most exception flags first, then lowest revenue first (so problem leads surface at top).

**NOT in VA queue** (shown in card detail if present, but don't trigger queueing):
- `invoice_mismatch`, `invoice_below_estimate` — accounting flags, not VA-actionable
- `classification_fallback`, `name_mismatch` — too noisy as queue triggers, but shown as card issues

**Source file:** `hcp-review-app/server.js` (~line 626)

---

## 20. VA Review Actions & Database Effects

**Every VA action is logged to the `lead_reviews` table** with: hcp_customer_id, customer_id, action, performed_by, reason, notes, previous_status, resolved_flag.

### Lead-Level Actions

| Action | What It Does | DB Effect |
|--------|-------------|-----------|
| **Approve** | VA confirms lead looks correct | Sets `review_status = 'confirmed'`, clears ALL `exception_flags = '{}'` |
| **Flag for Manager** | VA unsure, escalates | Sets `review_status = 'flagged'`, keeps exception flags |
| **Skip** | Close card, no action | No DB change — lead stays in queue |

### Record Reclassification

| Action | What It Does | DB Effect |
|--------|-------------|-----------|
| **Reclassify job → inspection** | Move a misclassified job to inspections | Inserts into `hcp_inspections`, deletes from `hcp_jobs`, removes `classification_fallback` flag |
| **Reclassify inspection → job** | Move a misclassified inspection to jobs | Inserts into `hcp_jobs`, deletes from `hcp_inspections`, removes `classification_fallback` flag |
| **Reclassify estimate type** | Change estimate between treatment/inspection | Updates `hcp_estimates.estimate_type` |

### Grouping Actions

| Action | What It Does | DB Effect |
|--------|-------------|-----------|
| **Group jobs as segments** | Mark multiple jobs as parts of one project | First job = parent, rest get `record_status = 'segment'` + `parent_hcp_job_id` set. Resolves `multiple_jobs` flag |
| **Group inspections as segments** | Mark multiple inspections as parts of one project | Same pattern — first = parent, rest = segments. Resolves `multiple_inspections` flag |
| **Group estimates as options** | Mark multiple estimates as pricing options | First = main, rest get `record_status = 'option'`. Resolves `multiple_estimates` flag |
| **Keep separate** | Confirm items are genuinely separate projects | Removes fragmentation flag (e.g., `multi_jobs`), sets `review_status = 'resolved'` if no flags remain |
| **Undo group** | Reverse a grouping mistake | Unnests segments/options back to active records via `review_unnest_segment`, `review_unnest_inspection_segment`, `review_unnest_estimate_option` |

### Revenue & Attribution Actions

| Action | What It Does | DB Effect |
|--------|-------------|-----------|
| **Toggle revenue** (checkbox) | Include/exclude a record from ROAS | Sets `count_revenue = true/false` on the specific job, inspection, estimate, or segment |
| **Override attribution** | Change lead source | Sets `attribution_override` on `hcp_customers` (e.g., `google_ads`, `google_business_profile`, `lsa`, `referral`, `direct`, `other`). Sets `review_status = 'overridden'` |
| **Exclude record** | Remove a record entirely | Sets `record_status = 'excluded'` — removed from all views and revenue |

### Match Verification (Advanced View)

| Action | What It Does | DB Effect |
|--------|-------------|-----------|
| **Confirm match** | Phone matched, names differ but it IS the same person | Sets `review_status = 'confirmed'`, removes `name_mismatch` flag |
| **Reject match** | Phone matched but it's the wrong person | Nulls `callrail_id` and `match_method`, sets `review_status = 'rejected'`, removes `name_mismatch` flag |

### Auto-Approve Rule
When all issue sections in a card are resolved (grouped, kept separate, or flagged), the lead **automatically approves** — sets `review_status = 'confirmed'` and clears flags without the VA clicking Approve.

### Dismiss Flag
Removes a specific exception flag from the array. If no flags remain after removal, sets `review_status = 'resolved'`.

**Source files:**
- SQL functions: `hcp-sync/migrations/006_review_functions.sql`
- API endpoints: `hcp-review-app/server.js` (lines 151-380)

---

## 21. Spam Detection

### Gibberish Name Detection
**Applied to:** calls and form submissions

1. If name < 6 characters → NOT gibberish
2. If 6+ consecutive consonants (no vowels) → GIBBERISH
3. For each word ≥8 characters: if vowel ratio < 18% → GIBBERISH
4. If both words in 2-word name are ≥8 chars with low vowel ratio → GIBBERISH

**Impact:** Sets `is_spam = TRUE`. Spam leads excluded from `v_weekly_leads` and lead counts.

### Bot Form Spam Detection (Added 2026-04-08)

**Applied to:** `form_submissions` — primarily affects unmatched form leads in funnel contact counts.

**Problem:** Coordinated bot campaigns click Google Ads (generating real GCLIDs and costing ad spend), then submit forms with gibberish data. They inflate contact counts and waste ad budget.

**Detection (OR logic — any condition triggers exclusion):**

| Condition | Pattern | Vowel Check | Example |
|-----------|---------|-------------|---------|
| `source = 'Direct'` + two-word 8+ uppercase name | `customer_name ~ '^[A-Z]{8,}\s+[A-Z]{8,}$'` AND `source = 'Direct'` | Not needed | `WCKMKQFSQLZRGIWSSL AYPUDBNGLUNDHHVHUZXLTR` |
| Other source + two-word 8+ uppercase name + low vowels | Same name pattern AND `source != 'Direct'` | < 0.25 | `DIUDUDUDHFXHXGHX SJJDUDJYDJDYDUHEGD` (source = 'Google Ads') |
| `source = 'Direct'` + single-word 12+ uppercase name | `customer_name ~ '^[A-Z]{12,}$'` AND `source = 'Direct'` | Not needed | `SUNDAZKXANJNCXCPUQQT` (single word) |

**Why the source shortcut works:** Real Google Ads form leads always have `source = 'Google Ads'` from CallRail's session tracking. Bots click the ad (getting a GCLID) but submit the form directly, so CallRail tags them as `source = 'Direct'`. No legitimate lead has `source = 'Direct'` with a two-word gibberish uppercase name — verified with zero false positives across all data.

**Vowel ratio formula:** `LENGTH(REGEXP_REPLACE(UPPER(name), '[^AEIOU]', '', 'g')) / LENGTH(REGEXP_REPLACE(name, '\s', '', 'g'))`

**Why 0.25 for non-Direct sources:** Some bot names contain enough random vowels to pass 0.2 (e.g., ratio 0.216). The lowest real name is MCKINNON CHAPPELL at exactly 0.25, so strict `< 0.25` is safe. Only needed for `source != 'Direct'` forms where the source shortcut can't apply.

**Rejected signals:**
- Dotted gmail pattern: false-positives on `d.c.loring76@gmail.com`, `m.collingwood414@gmail.com`, `c.gambino.11.11@gmail.com`
- Single-word name detection: false-positives on `KRYSTYNA`, `CHARLY`, `SKYLAR`

**Characteristics of these bots:**
- `source = 'Direct'` in CallRail (session tracking doesn't attribute to Google Ads)
- Many have `gclid IS NOT NULL` (bot clicked a Google Ad before submitting)
- Often no phone number, or fake phone numbers
- Same base email pattern across many clients (slight dot variations)

**Single-word bot detection (Added 2026-04-10):** Single-word 12+ char uppercase names with `source = 'Direct'` are also bots. Verified with zero false positives in 90-day data — only 1 Google Ads form ever matched and it was a placeholder (`GETAQUOTETODAY`). Catches 46 additional bot forms across all clients.

**Remaining known gap:** Single-word bots from `source != 'Direct'` are not caught. Real names like `KRYSTYNA`, `CHARLY`, `SKYLAR` would false-positive on a more aggressive single-word rule.

**Impact (last 90 days):** 440 bot forms across 25 clients detected and excluded with zero false positives.

**Implementation:** Filter in `unmatched_forms` CTE in `getHcpFunnel()`. Also mark `form_submissions.is_spam = true` during ETL for forms matching these patterns.

### GHL-Based Spam Detection
**Applied to:** GHL clients on funnel dashboard (spam count KPI + lead exclusion)

**Detection sources (OR logic — any match = spam):**

| Source | Field | Match Pattern |
|--------|-------|---------------|
| Contact lost reason | `ghl_contacts.lost_reason` | ILIKE `%spam%`, `%not a lead%`, `%spoofed%`, `%duplicate%` |
| Opportunity stage | `ghl_opportunities.stage_name` | ILIKE `%spam%`, `%not a lead%` |

**Impact:**
- Leads matching either condition are excluded from funnel stage counts (leads, estimates, etc.)
- Counted separately as `spam` in the funnel KPIs
- Exclusion uses `NOT EXISTS` subqueries against `ghl_contacts` joined to `ghl_opportunities`

**Note:** Some GHL pipelines track spam via contact `lost_reason` (e.g., "Spam/Sales Call-Dead"), while others use opportunity stage names (e.g., "Spam 🗑️", "Not A Lead 🗑️"). Both methods are supported.

**Source files:**
- Gibberish detection: `add-spam-detection.sql`
- GHL spam pattern: `hcp-review-app/server.js` (~line 1327, `spamPattern` constant)

---

## 22. Lead Attribution & Source Detection

### Source Classification Priority

**In mv_funnel_leads (canonical, used by BlueprintOS):**

| Priority | Check | Source |
|----------|-------|--------|
| 1 | Manual `attribution_override = 'google_ads'` | `google_ads` |
| 2 | Webflow match (`callrail_id LIKE 'WF_%'`) | `google_ads` |
| 3 | Call through Google Ads Call Extension tracker (`callrail_trackers.source_type = 'google_ad_extension'`, matched by callrail_id) | `google_ads` |
| 4 | Call through Google Ads Call Extension tracker (matched by phone) | `google_ads` |
| 5 | Call has GCLID or `classified_source = 'google_ads'` (excluding LSA) | `google_ads` |
| 6 | Form has GCLID or `source = 'Google Ads'` (matched by callrail_id, phone, or email) | `google_ads` |
| 7 | GHL contact has GCLID (matched by phone or email) | `google_ads` |
| 8 | `attribution_override = 'lsa'` or call `source_name = 'LSA'` | `lsa` |
| 9 | `attribution_override = 'gbp'` or call `source = 'Google My Business'` | `gbp` |
| 10 | None of the above | `other` |

**In v_lead_revenue / get_dashboard_metrics (risk dashboard) — older logic, does not yet include steps 3-4 (tracker attribution). Unification planned.**

### Multi-Touch Form Matching (Critical)

When checking form submissions for attribution, match by **callrail_id OR phone OR email** — not just callrail_id. This handles the case where a lead submits a Google Ads form but later calls via GBP; the call match sets the HCP callrail_id to the GBP call, hiding the form's gclid. The phone/email fallback finds the original form.

```sql
EXISTS (SELECT 1 FROM form_submissions f
  WHERE f.customer_id = hc.customer_id
    AND (f.callrail_id = hc.callrail_id
         OR normalize_phone(f.customer_phone) = hc.phone_normalized
         OR (hc.email IS NOT NULL AND LOWER(f.customer_email) = LOWER(hc.email)))
    AND (f.gclid IS NOT NULL OR f.source = 'Google Ads'))
```

### GBP Multi-Touch Rules

When a call comes from GBP/GMB, check if the same person has a prior Google Ads interaction (call with gclid OR form with gclid/source). If yes, attribute to `google_ads`. If no, attribute to `google_business_profile`.

### Attribution Override Values
- `confirmed_google_ads` — manually confirmed as GA lead
- `not_google_ads` — excluded from GA metrics
- `google_ads`, `google_business_profile`, `lsa`, `referral`, `direct`, `other` — source override

**Source files:**
- `risk-dashboard/fix_lead_revenue.sql`
- `hcp-sync/pull_callrail_forms.py` (GCLID extraction)
- `hcp-sync/migrations/004_review_overrides.sql`

---


### GHL GCLID Fallback (Added 2026-03-30, implemented 2026-04-03)

For GHL clients, leads may click a Google Ad but contact the business directly (bypassing CallRail tracking). GHL captures the GCLID in contact custom fields ("Google Click ID"). When a GHL contact has a non-null GCLID, attribute to google_ads even if no CallRail call or form exists for that phone number.

- **Match:** By phone_normalized OR email (same logic as form_submissions matching)
- **Signal:** Only `ghl_contacts.gclid IS NOT NULL` — never use `ghl_contacts.source` field (unreliable, can be set manually)
- **ETL:** ghl-sync/pull_ghl_data.py extracts GCLID from custom fields (field name containing "google click" or "gclid") and stores in ghl_contacts.gclid. Also checks `lastAttributionSource.gclid` as fallback.
- **Views:** Added to v_lead_revenue (fix_lead_revenue.sql) and mv_funnel_leads as step 7 in the attribution cascade, after Webflow and before ELSE 'unknown'
- **Scope:** Applies to Google Ads source filter only. Does not affect GBP, LSA, or other source detection.
- **Priority:** Lowest — only fires when CallRail (calls, forms, webflow) has no GA match

### Call Extension Tracker Attribution (Added 2026-04-06)

CallRail assigns each tracking number a `tracker_id`. Some calls through Google Ads Call Extension trackers lose their "Google Ads" source tag in CallRail (tagged as "Direct" instead). We now detect these by matching the call's `tracker_id` against the `callrail_trackers` lookup table.

- **Rule:** If a call came through a tracker where `source_type = 'google_ad_extension'`, attribute to `google_ads` regardless of CallRail's `source` field
- **Table:** `callrail_trackers` — populated from CallRail API (`GET /v3/a/{account_id}/trackers.json`). Contains tracker_id, name, source_type, company_id, tracking_number, status.
- **ETL:** `classify_calls.py` now pulls `tracker_id` on every call and stores it in `calls.tracker_id`. Backfill script: `backfill_tracker_id.py`.
- **Priority:** In mv_funnel_leads, this fires after `attribution_override` and `WF_` checks, but before GCLID/source checks. Two checks: first by callrail_id match, then by phone match (catches calls not linked by callrail_id).
- **Tracker names vary:** "Google Ads Call Extension", "Google Ad Extension", "Google Ads Extension Number", etc. The `source_type = 'google_ad_extension'` field is canonical.
- **Impact:** 86 leads, $121K moved from "other" to google_ads across 7 clients.
- **Scope:** Currently only implemented in `mv_funnel_leads`. Not yet in `v_lead_revenue` or `get_dashboard_metrics()`. Unification planned.

### Post-Touch Revenue Rule (Added 2026-04-06)

For Google Ads leads, only count revenue (invoices, estimates) and funnel activity (inspections, jobs) that occurred **after** the first Google Ads touch. Pre-touch revenue is not attributable to the ad.

- **Column:** `mv_funnel_leads.first_ga_touch_time` — the earliest Google Ads attribution event
- **Computed as:** `LEAST(earliest GA call, earliest GA form, earliest GHL GCLID contact date)`
  - GA call: CallRail call with `source IN ('Google Ads','Google Ads 2')` OR `gclid IS NOT NULL` OR `classified_source = 'google_ads'` OR tracker is `google_ad_extension`
  - GA form: CallRail form with `gclid IS NOT NULL` OR `source = 'Google Ads'`
  - GHL GCLID: `COALESCE(ghl_contacts.kpi_date_created, ghl_contacts.date_added)` where `gclid IS NOT NULL`. The `kpi_date_created` custom field overrides `date_added` for Webflow CSV imports where the default create date is the import date, not the form submission date.
- **Filter:** All revenue columns (est_sent_cents, est_approved_cents, job_cents, invoice_cents, insp_invoice_cents, treat_invoice_cents) and all funnel flags (has_inspection_scheduled, etc.) only count records dated on or after `first_ga_touch_time`
- **Same-day tolerance (Added 2026-04-10):** Comparison uses `::date` (date-only), not full timestamp. This catches "over-the-phone quote" scenarios where the rep creates an HCP estimate during the call (a few seconds/minutes BEFORE the CallRail GCLID timestamp finalizes when the call ends). Without this tolerance, legitimate same-day estimates were excluded due to timestamp precision. Affected 7 leads worth $107K across all clients (initially).
- **Non-GA leads unaffected:** The post-touch filter only applies when `lead_source = 'google_ads'`. Other sources get all-time revenue as before.
- **Impact:** ~$88K removed across portfolio (last 90 days), ~7% correction.
- **Scope:** Currently only in `mv_funnel_leads`. Not yet in `v_lead_revenue` or `get_dashboard_metrics()`. Unification planned.

### Reactivation Protocol — 60-Day Combo Rule (Added 2026-04-08)

When a Google Ads lead already existed in the system before their first GA touch, determine whether to count them as a legitimate GA-attributed lead or exclude them.

**Rule (the "60-day combo"):**

| Scenario | Disposition |
|----------|------------|
| New lead (HCP created within 7 days of GA touch) | **Count** — normal new lead |
| Prior history, last interaction < 60 days before GA touch | **Exclude** — recently active, would have found business anyway |
| Prior history, last interaction 60+ days, NO prior treatment | **Count** — ad reactivated a dormant unconverted prospect |
| Prior history, last interaction 60+ days, HAD prior treatment | **Exclude** — established customer, ad didn't drive the conversion |

**Definitions:**
- **Last interaction:** `GREATEST(hcp_created_at, last job date, last inspection date, last estimate sent date, last CallRail call, last CallRail form)` — all dated before the first GA touch
- **Prior treatment:** Completed treatment job (`status IN ('complete rated','complete unrated')` AND `total_amount_cents >= 100000`) OR treatment invoice (`invoice_type = 'treatment'` AND `amount_cents > 0`) dated before the first GA touch
- **60 days:** Calendar days between last_prior_interaction and first_ga_touch_time

**Columns:**
- `mv_funnel_leads.last_prior_interaction` — timestamp of most recent pre-GA-touch activity
- `mv_funnel_leads.has_prior_treatment` — boolean, true if completed treatment work exists before GA touch
- `mv_funnel_leads.exclude_from_ga_roas` — boolean, true if the lead should be excluded from GA ROAS calculations

**How dashboards should use it:**
- When calculating Google Ads ROAS, filter: `WHERE lead_source = 'google_ads' AND NOT exclude_from_ga_roas`
- The lead still appears in the funnel and lead lists — it's not deleted, just excluded from ROAS
- The post-touch revenue columns still reflect only post-touch amounts regardless of exclusion

**Impact (last 90 days):** 3 leads excluded, $18,792 removed.

**Scope:** Currently only in `mv_funnel_leads`. Not yet in `v_lead_revenue` or `get_dashboard_metrics()`. Unification planned.

**Decision history:** Discussed and approved by team on 2026-04-08 via Slack. Combo approach chosen over pure history-based or pure time-based options.

### Unmatched Repeat Lead Filtering (Added 2026-04-08)

Applies to: `unmatched_calls` and `unmatched_forms` CTEs in `getHcpFunnel()` and `getJobberFunnel()` in BlueprintOS API.

**Problem:** A lead whose first CallRail interaction was before the dashboard date range still appears as a unique phone in the current period. They're not a new lead — they're a returning caller.

**Rule:** Apply the same 60-day combo logic to unmatched CallRail leads:

| Scenario | Disposition |
|----------|------------|
| `first_call = true` and no prior form for this phone | **Count** — genuinely new lead |
| `first_call = false`, no prior call found in DB | **Count** — prior call predates our data, treat as reactivation |
| `first_call = false`, last prior call < 60 days ago | **Exclude** — recently active, not a new lead |
| `first_call = false`, last prior call 60+ days ago, no prior treatment in HCP/Jobber | **Count** — ad reactivated a dormant prospect |
| `first_call = false`, last prior call 60+ days ago, had prior treatment in HCP/Jobber | **Exclude** — returning customer |

**For unmatched forms:** Same logic, but check whether the phone had any prior call or form submission before the current form's date. Use the earliest prior interaction as the comparison point.

**Definitions:**
- **Last prior call:** `MAX(start_time) FROM calls WHERE phone = X AND start_time < current_interaction_date`
- **Prior treatment:** `EXISTS` in `mv_funnel_leads` where `phone_normalized = X AND (has_job_completed OR has_invoice)`. If the phone has no `mv_funnel_leads` record, there's no treatment history.
- **60 days:** Calendar days between last prior interaction and current interaction

**Implementation:** Filter in the `unmatched_calls` and `unmatched_forms` CTEs. For each `first_call = false` phone, run a subquery checking the 60-day combo rule.

**Source file:** `apps/blueprintos-api/index.js` — `getHcpFunnel()`, `getJobberFunnel()`

---

## 23. Manual Override System

### Risk Override
**Column:** `clients.risk_override` (TEXT, nullable)

| Value | Effect |
|-------|--------|
| `'risk'` | Forces client to Risk status regardless of metrics |
| `'flag'` | Forces client to Flag status |
| `'healthy'` | Forces client to Healthy status |
| `NULL` | Normal computed risk |

**Source file:** `risk-dashboard/migrations/get_dashboard_with_risk.sql`

### Review Status
**Column:** `hcp_customers.review_status`

| Value | Meaning |
|-------|---------|
| `pending` | Not yet reviewed (default) |
| `confirmed` | VA approved — looks good |
| `flagged` | VA flagged for manager |
| `overridden` | Manager overrode classification |
| `resolved` | Manager resolved a flagged issue |

---

## 24. Client Portal Rules

**Auth:** Token-based via `clients.client_portal_token` (unique URL token).

**What clients see:**
- Their matched leads with name, phone, date, status, revenue
- Pipeline summary (inspections, estimates, jobs, invoices)

**What clients can do:**
- Flag leads as: `spam`, `out_of_area`, `wrong_service`, or `other`

**Flagged lead impact:**
- Stored in `client_flagged_leads` table
- Leads flagged as `spam`, `out_of_area`, `wrong_service` are **excluded** from all views, counts, and revenue calculations via `NOT EXISTS` filters

**Source file:** `hcp-review-app/server.js` (client portal endpoints, ~line 705+)

---

## 25. Phone Priority

**When HCP customer has multiple phone numbers:**

```
phone_primary = mobile > home > work
```

Take first non-empty in that order.

**Source file:** `hcp-sync/pull_hcp_data.py` (~line 81)

---

## 26. Call Classification

| Field | Values | Source |
|-------|--------|--------|
| `classified_status` | `answered`, `missed`, `abandoned` | AI classifier |
| `classified_source` | `google_ads`, `lsa`, etc. | Source detection |
| `classified_period` | `business_hours`, `after_hours` | Client biz hours config |

**Business hours:** Configured per-client via `clients.biz_hours_start`, `biz_hours_end`, `biz_days` (1=Mon, 5=Fri).

**Source file:** `call-classifier/classify_calls.py`

---

## 27. First Contact Detection

| Field | Table | Source |
|-------|-------|--------|
| `first_call` | `calls` | From CallRail API — TRUE if first call from this phone to this tracking number |
| `first_form` | `form_submissions` | TRUE if first form submission from this phone/email |

### How Repeat Callers Are Handled by Context

| Context | Repeat Caller Handling |
|---------|----------------------|
| **Contacts count** (risk dashboard) | `COUNT(DISTINCT phone)` — same phone calling 5 times = **1 contact** |
| **Quality leads** | Same as contacts — deduped by phone within the period |
| **CPL** | Uses deduped contacts, so repeat callers don't inflate lead count |
| **Answer rate** | Only `first_call = true` + business hours — repeat callers are **excluded** from both numerator and denominator |
| **HCP matching** | `DISTINCT ON (hcp_customer_id) ORDER BY start_time ASC` — earliest call wins, one callrail_id per HCP customer |
| **Lead volume change** | Compares distinct phone counts between periods — repeat callers don't inflate either period |
| **Days since lead** | `MAX(start_time)` — uses most recent call regardless of first/repeat |
| **Form dedup vs calls** | If same phone submitted a form AND called, only counted once in contacts (form excluded via `NOT EXISTS` against call contacts) |

### Key Principle
A person who calls 3 times counts as **1 lead** for contacts/CPL/quality, but their answer rate is only measured on the **first call** (when the client had a chance to make a first impression). Repeat calls are excluded from answer rate because the client already knows who's calling.

**Source files:**
- `first_call` from API: `call-classifier/classify_calls.py` (CallRail API `fields` param)
- Contacts dedup: `risk-dashboard/migration.sql` (lines 82-126)
- Answer rate filter: `risk-dashboard/migration.sql` (lines 174-194)

---

## 28. Large Account Handling

**Threshold:** `LARGE_ACCOUNT_THRESHOLD = 1000` (customers per HCP account)

If account has >1000 customers:
- Jobs/estimates/invoices pulls are date-filtered (last 365 days)
- Prevents API timeout and memory issues

**Source file:** `hcp-sync/pull_hcp_data.py` (line 43)

---

## 29. Guarantee Calculation

```
guarantee = all_time_revenue / program_price
```

**This measures: how much of their agency fees has the client covered with Google Ads revenue.**

- `all_time_revenue`: Sum of ROAS revenue from all CRM sources (HCP + Jobber + GHL) for Google Ads leads
- `program_price`: Total agency fees for the program (from `clients.program_price` column, typically $15K-$42K)

**NOT** `revenue / ad_spend` — that's ROAS, a different metric.

**Dashboard highlighting (stage-aware):**

| Stage | Green | Yellow | Red |
|-------|-------|--------|-----|
| Months 1-2 | - | - | - |
| Months 5-6 | 1x+ | <0.5x | - |
| Months 7-9 | 1x+ | 0.5-0.75x | <0.5x |
| Months 10+ | 1x+ | 0.8-1x | <0.8x |

**Impact on risk scoring:**
- Guarantee < 0.5x at months 7+ → funnel risk trigger (non-negotiable)
- Guarantee meeting lifetime threshold → saves from presentation risk
- ROAS ≥ 3x overrides CPL risk → downgrades to flag (for CRM-connected clients)

**Updated 2026-03-21:** Changed from `program_price / ad_spend` to `all_time_revenue / program_price`.

---
## 30. Date Conventions

| Context | Which Date |
|---------|-----------|
| Lead reporting / lead list | **Lead date** (first CallRail contact or HCP created) |
| Revenue reporting | **Revenue date** (invoice date, job completed date) |
| Pipeline velocity | Mix: lead date for entry, revenue date for completion |
| Risk dashboard date range | Filters by lead date (CallRail contact time) |
| Funnel dashboard | Lead date for counting, current status for funnel stage |

**Rule:** Lead date is the primary view. Revenue date is secondary. Can mix for pipeline velocity analysis.

---

## 31. Display Standards

### Number Formats
- Currency: `$X,XXX` (no cents unless under $10)
- Percentages: `XX%` (whole numbers)
- CPL: `$XXX` with up/down arrow for trend

### Status Labels
- Completed: green badge
- Scheduled/Sent/In Progress: amber badge
- Canceled/Declined/Voided: red badge

### Brand Colors
- Background: `#F5F1E8` (cream), `#ebe7de` (page bg), `#EEEAD9` (subtle bg)
- Text: `#000` (primary), `#8a8279` (muted), `#5a554d` (secondary)
- Accent: `#E85D4D` (coral/red), `#3b8a5a` (green), `#c4890a` (amber)
- Risk: red `#E85D4D`, Flag: amber `#c4890a`, Healthy: green `#3b8a5a`

---

## 32. On Calendar Metric

**Applies to:** All clients with field management software (HCP, Jobber, or GHL). Counts inspections scheduled in the next 14 days.

**Two numbers:** `on_cal_14d` (GA-attributed) / `on_cal_total` (all sources).

### HCP

- Source: `hcp_inspections` table
- Filter: `status IN ('scheduled', 'in_progress', 'needs scheduling')` AND `record_status = 'active'`
- Date: `scheduled_at` between today and today + 13 days
- GA attribution: HCP customer matched to a CallRail call with `classified_source = 'google_ads'`, a form with GCLID, or a Webflow lead (`callrail_id LIKE 'WF_%'`)

### Jobber

Jobber clients may schedule inspections as **requests** (with assessments) or as **jobs**. The calendar metric checks both:

**1. Requests with assessments:**
- Source: `jobber_requests` where `has_assessment = true`
- Date: `assessment_start_at` between today and today + 13 days
- Filter: `assessment_completed_at IS NULL` (not yet done)

**2. Inspection-titled jobs with visits:**
- Source: `jobber_jobs` joined to `jobber_visits`
- Date: `jobber_visits.start_at` between today and today + 13 days
- Filter: `completed_at IS NULL` (not yet done)
- Title keywords: `assessment`, `instascope`, `inspection`, `mold test`, `air quality`, `air test`, `free inspection`, `home inspection`

**Important:** Jobber jobs do NOT have a `scheduled_at` column. Scheduling data comes from `jobber_visits.start_at`, which is synced from the Jobber GraphQL API `visits` field on each job. The `jobber_visits` table must be populated for this metric to work.

**GA attribution (both):** Jobber customer matched to a CallRail call with `classified_source = 'google_ads'` or a form with GCLID.

**Note:** `insp_booked` (inspection booked rate) is a separate metric — it counts whether an inspection job/request *exists* for a lead, not whether it's scheduled on the calendar. It is NOT affected by visit data.

### GHL

- Source: `ghl_appointments` table (synced from GHL `/contacts/{id}/appointments` endpoint)
- Filter: `appointment_type = 'inspection'` AND `deleted = false` AND `status NOT IN ('cancelled')`
- Date: `start_time` between today and today + 13 days
- GA attribution: Appointment contact phone matched to a CallRail call with `classified_source = 'google_ads'`
- Calendar classification: calendar name matched against keywords (see Section 33)

**Source files:**
- `risk-dashboard/migration.sql` → CTEs `on_cal_hcp`, `on_cal_jobber`, `on_cal_ghl`
- `ghl-sync/pull_ghl_appointments.py` → Syncs appointments from GHL contacts endpoint
- `jobber-sync/pull_jobber_data.py` → GraphQL query includes `visits(first: 50) { nodes { startAt endAt title } }`

---

## 33. GHL-as-CRM Integration

**Applies to:** Clients with `field_management_software = 'ghl'` who use GoHighLevel as their primary CRM instead of HCP or Jobber.

**Current clients:** Brawley (Pure Maintenance of East Texas LLC, customer_id: 1714816135)

### Data Sources

| GHL Table | Purpose | Synced Via |
|-----------|---------|------------|
| `ghl_estimates` | Estimates with statuses (draft/sent/accepted/invoiced) | `pull_ghl_estimates.py` |
| `ghl_appointments` | Calendar appointments (inspections, jobs) | `pull_ghl_appointments.py` |
| `ghl_contacts` | Contact enrichment, spam detection (lost_reason) | `pull_ghl_data.py` |
| `ghl_opportunities` | Pipeline stages, spam/abandoned classification | `pull_ghl_data.py` |

### GHL Estimate Status → Funnel Stage Mapping

| GHL Status | Funnel Stage | Revenue? |
|------------|-------------|----------|
| `draft` | Not in funnel | No |
| `sent` | Estimate Sent | Open estimate (pipeline) |
| `accepted` | Estimate Approved | Yes (ROAS revenue) |
| `invoiced` | Job Completed / Invoiced | Yes (ROAS revenue) |

### ROAS Revenue Calculation (GHL)

**Same ROAS waterfall as HCP** (see Section 6), applied per contact:

```
Per-contact ROAS revenue = GREATEST(invoiced_total, accepted_total)
```

- If a contact has both invoiced and accepted estimates, take whichever is higher
- `accepted` estimates count as revenue (approved work, even if not yet invoiced)
- `sent` estimates are pipeline only (open estimates), NOT revenue
- GA attribution: contact phone must match a CallRail call with `classified_source = 'google_ads'`

**Source file:** `risk-dashboard/migration.sql` → CTE `revenue_ghl`

### Calendar/Appointment Classification

Appointments are classified as `inspection`, `job`, or `other` based on the GHL calendar name:

| Calendar Name Keywords | Type |
|----------------------|------|
| inspection, air quality, air test, mold test, assessment, estimate, consult, evaluation, survey, walkthrough | `inspection` |
| treatment, dry fog, dry vapor, remediation, removal, abatement, encapsulation | `job` |
| Everything else (e.g., "Missed Callback") | `other` |

**Appointment data comes from:** `GET /contacts/{contactId}/appointments` (per-contact endpoint, NOT the calendar events endpoint which returns empty for most setups).

### Risk Dashboard Integration

GHL clients are fully integrated into `get_dashboard_metrics()` with three CTEs:

| CTE | What It Provides |
|-----|-----------------|
| `revenue_ghl` | Insp booked, period/all-time/trailing revenue, open estimates (per-contact GREATEST waterfall) |
| `open_est_ghl` | Total open estimate pipeline (sent + accepted, any source) |
| `on_cal_ghl` | Future inspection appointments in next 14 days (GA + all) |

These are summed into the same total columns as HCP/Jobber: `total_insp_booked`, `total_closed_rev`, `total_open_est_rev`, `on_cal_14d`, ROAS, guarantee, and all trailing metrics.

### Client Portal Funnel

GHL clients use `handleGhlFunnel()` in server.js (not the HCP handler). The funnel shows:
- Leads (CallRail quality leads)
- Estimate Sent (GHL estimates with status sent/accepted/invoiced)
- Estimate Approved (accepted/invoiced)
- Invoiced (invoiced only)

No inspection/job stages in the portal funnel — GHL clients typically don't track inspections as separate line items.

### ETL Schedule

| Script | Frequency | What It Syncs |
|--------|-----------|--------------|
| `pull_ghl_data.py` | Every hour (:10, :40) | Contacts, opportunities, pipeline stages |
| `pull_ghl_estimates.py` | Every hour (:10, :40) | Estimates for GHL-only clients |
| `pull_ghl_appointments.py` | Every hour (:10, :40) | Calendar appointments for GHL-only clients |

All three run via `ghl-pull.sh` launcher, scheduled by `com.blueprint.ghl-pull` launchd agent.

### Onboarding a New GHL-as-CRM Client

1. Set `field_management_software = 'ghl'` in clients table
2. Ensure `ghl_api_key` and `ghl_location_id` are populated
3. Run `pull_ghl_estimates.py --client X` to backfill estimates
4. Run `pull_ghl_appointments.py --client X` to sync appointments
5. Generate portal token: `POST /api/admin/generate-portal-token`
6. Everything else (risk dashboard, funnel, cohort) is automatic

**Source files:**
- `ghl-sync/pull_ghl_estimates.py` — Estimate ETL
- `ghl-sync/pull_ghl_appointments.py` — Appointment ETL
- `ghl-sync/pull_ghl_data.py` — Contact/opportunity ETL
- `ghl-sync/migrations/002_ghl_appointments.sql` — Appointment table schema
- `risk-dashboard/migration.sql` — `revenue_ghl`, `open_est_ghl`, `on_cal_ghl` CTEs
- `hcp-review-app/server.js` — `handleGhlFunnel()` function

---

## 34. Slack Alert System

**Replaces:** Google Sheets Apps Script alerts. Now powered by PostgreSQL `get_dashboard_with_risk()`.

### Alert Types

| Alert | Schedule | Destination | Script |
|-------|----------|-------------|--------|
| Weekly Summary Report | Sunday 4pm ET (9pm UK) | `#reports` | `slack-weekly-report.js` |
| Ads Manager Brief | Sunday (per-manager timezone) | Manager channels / DM | `slack-manager-briefs.js` |

### Manager Channel Routing

| Manager | Destination | Timing |
|---------|-------------|--------|
| Martin | DM (U06QCME7K5H) | Sunday 4pm ET (9pm UK) |
| Luke | C08LQL3TPGA | Sunday 4pm ET (9pm UK) |
| Nima | C09P10LJZJ5 | Sunday 11:30am ET (9pm IST) |

### Ads Manager Brief Rules

- **Only shows ads-actionable risk**: Clients with `Ads Risk` or `Both Risk` appear in the Risk section. Clients with `Funnel Risk` only are listed under Healthy (funnel issues are not actionable by ads managers).
- **Prioritized by severity**: Clients sorted by risk type (Both Risk first), then trigger count, then duration. Worst client at top.
- **Fire emoji scale**: Top client gets triple fire if Both Risk or 4+ triggers; double fire for 3+ triggers or Both Risk; single fire otherwise.

### Risk Duration Smoothing

**Applies to:** Slack alerts only (not the risk dashboard heatmap, which shows raw daily snapshots).

**Rule:** When calculating "how long in risk," ignore brief dips of 3 days or fewer. A client must be Healthy for **4+ consecutive days** for the duration counter to reset.

**Why:** Rolling 30-day metrics cause clients to briefly dip in and out of risk as individual days roll off the window. A 1-day dip to Flag or Healthy doesn't mean the underlying issue resolved — showing "5d in risk" when they've actually been struggling for a month is misleading to managers.

**How it works:**
1. Walk backwards through `risk_status_snapshots`
2. Find the most recent stretch of 4+ consecutive Healthy days
3. Duration = days since that stretch ended
4. If no 4+ day Healthy stretch exists, duration = days since first snapshot

**Example:** Client is Risk → Risk → Flag (1 day) → Risk → Risk = reported as continuous risk, not "2d in risk."

### Slack Credentials

- **Bot token** (`xoxb-...`): Dashboard Bot — must be invited to each channel before posting
- **Channel IDs**: Stored in `clients.slack_channel_id` column and `Manager Channels` tab in Dashboard spreadsheet
- **Bot name**: "Dashboard Bot" in Slack workspace

### Source Files

- `risk-dashboard/slack-weekly-report.js` — Weekly summary to #reports
- `risk-dashboard/slack-manager-briefs.js` — Per-manager briefs with severity sorting

---

## Source File Index

| File | What It Contains |
|------|-----------------|
| `hcp-sync/pull_hcp_data.py` | Classification, auto-fixes, matching, exception flags |
| `hcp-sync/migrations/003_hcp_schema.sql` | Schema, normalize_phone, record statuses |
| `hcp-sync/migrations/004_review_overrides.sql` | Review system, attribution overrides |
| `hcp-sync/migrations/006_review_functions.sql` | All review action functions (approve, flag, group, reclassify) |
| `hcp-sync/dashboard_views.sql` | Funnel counting, lead date, client summaries |
| `risk-dashboard/migration.sql` | `compute_risk_status()`, all risk/flag thresholds |
| `risk-dashboard/fix_lead_revenue.sql` | `v_lead_revenue`, ROAS calc, GBP detection |
| `risk-dashboard/fix_pipeline_inference.sql` | `v_lead_pipeline`, stage inference |
| `risk-dashboard/fix_jobber_inspections.sql` | Jobber-specific revenue logic |
| `jobber-sync/pull_jobber_data.py` | Jobber matching, classification |
| `hcp-review-app/server.js` | VA queue filter, client portal, all API endpoints |
| `ghl-sync/pull_ghl_data.py` | GHL contacts, opportunities, pipeline stages |
| `ghl-sync/pull_ghl_estimates.py` | GHL estimates for GHL-as-CRM clients |
| `ghl-sync/pull_ghl_appointments.py` | GHL calendar appointments, inspection/job classification |
| `risk-dashboard/slack-weekly-report.js` | Weekly risk summary to #reports |
| `risk-dashboard/slack-manager-briefs.js` | Per-manager briefs with severity + duration |
| `add-spam-detection.sql` | Gibberish name algorithm |
| `call-classifier/classify_calls.py` | Call fetching, classification, Google Ads upload |

---

## 35. Data Ownership — Dashboard vs Call Classifier

### Two Separate Systems

There are **two independent systems** that operate on the `calls` table. They must never be mixed.

#### 1. Call Classifier (Martin's system)
- **Script:** `call-classifier/classify_calls.py`
- **Purpose:** AI + CallRail transcript analysis for Google Ads conversion uploads
- **Columns it owns (writes to):**
  - `classification` — legitimate, spam, duplicate, low_quality, unknown
  - `classification_reason` — free text explanation
  - `classification_attempts` — retry count
  - `classified_source` — google_ads, other_ads, organic, direct, unknown
  - `classified_status` — new_lead, existing_customer, etc.
  - `classified_period` — first_call, repeat, etc.
  - `classified_at` — timestamp of classification
  - `ai_classification` — raw AI output
  - `lead_score` — numeric score
  - `lead_scored_at` — timestamp of scoring
- **These columns exist solely for the classifier pipeline and Google Ads conversion upload. No dashboard, view, or report should read them.**

#### 2. Dashboard & Reporting System
- **Scripts:** `risk-dashboard/`, `hcp-review-app/`, all views and SQL functions
- **Purpose:** Client-facing metrics, risk scoring, funnel tracking
- **Columns it reads from `calls`:**
  - `source` — raw CallRail source (e.g., 'Google Ads', 'Google Ads 2', 'Google My Business'). **This is the GA attribution field.**
  - `caller_phone`, `start_time`, `duration`, `answered`, `first_call` — call metadata
  - `gclid` — Google click ID (backup GA attribution)
  - `source_name`, `formatted_tracking_source` — CallRail tracking details
  - `customer_name`, `customer_city`, `customer_state` — caller info
  - `voicemail`, `call_type`, `callrail_status` — call details
- **Quality/spam classification comes from GHL CRM data** (ghl_contacts lost_reason, stage names, ghl_opportunities status) — never from the classifier columns.

### The Rule

> **Dashboard queries must NEVER reference `classified_source`, `classification`, `classified_status`, `classified_period`, `classified_at`, `ai_classification`, `lead_score`, or `lead_scored_at`.** Use `is_google_ads_call(source, source_name, gclid)` for GA attribution. Use GHL CRM data for quality classification.

### The Function

All GA attribution goes through one SQL function:

```sql
is_google_ads_call(source, source_name, gclid) → BOOLEAN
```

Returns TRUE when:
- `source IN ('Google Ads', 'Google Ads 2')` AND `source_name` is NOT a GBP/GMB/Main Business Line tracking number
- OR `gclid IS NOT NULL` (multi-touch: person clicked a Google Ad but called via a different number)

The `source_name` exclusion handles mislabeled CallRail tracking numbers (e.g., a GBP number incorrectly assigned to the Google Ads source pool). Known affected clients: Chad Adams and 12 others.

### Why

The call classifier is an independent pipeline that may run on a different schedule, may change its logic, or may stop populating fields. The dashboard cannot have a hidden dependency on it. Raw CallRail fields (`source`, `source_name`, `gclid`) are populated at ingest time and are always reliable. GHL CRM data reflects what clients actually enter about their leads.

---

## 36. Answer Rate Detection

### Why Not CallRail's `answered` Field

CallRail marks a call as `answered = true` whenever something picks up — including voicemail greetings, IVR welcome messages, and auto-attendants. This inflates answer rates. Historically, ~696 voicemail pickups were being counted as answered calls.

### The Intelligence Service

The **CallRail Intelligence Service** (`callrail-intelligence/`) runs on the Mac Mini (port 3130) and analyzes call transcripts to determine if a **human** actually answered. It polls CallRail every 5 minutes.

**Detection algorithm** (in order of priority):

1. `call_type = 'abandoned'` → **abandoned** (95% confidence)
2. Duration < 5s → **abandoned** (90%)
3. **CallRail says not answered** (`answered=false` or `callrail_status=missed/abandoned`) → trust it (90%). CallRail is 99.85% accurate when it says a call was NOT answered. Only override if transcript shows 60s+ two-way conversation.
4. No transcript + duration < 10s → **abandoned** (70%)
5. No transcript + duration 20–44s → **missed** (50% — likely voicemail; old threshold was 20s, raised to 45s)
6. No transcript + duration 45s+ → **answered** (50%)
7. **Long call override**: 60s+ with both `Agent:` and `Caller:` in transcript → **answered** (95%)
8. Voicemail keywords + 1 speaker → **missed** (95%)
9. Voicemail keywords + <60s (even with 2 speakers) → **missed** (85% — caller left a message after beep)
10. Greeting keywords + 1 speaker + <10s → **abandoned** (90%)
11. Greeting keywords + 1 speaker + 10s+ → **missed** (85% — IVR only, no conversation)
12. Greeting keywords + 2+ speakers + 20s+ → **answered** (90%)
13. 2+ speakers + 15s+ → **answered** (85%)
14. 1 speaker + < 15s → **abandoned** (80%)
15. Fallback: CallRail's `answered` boolean (30% confidence)

**Speaker detection**: Counts `Agent:` and `Caller:` labels in transcript text (more reliable than `speaker_percent` which is often missing).

**Voicemail keywords:** "leave a message", "at the tone", "you've reached", "mailbox is full", etc.
**Greeting keywords:** "thank you for calling", "press 1", "call may be recorded", etc.

### Database Columns

| Column | Type | Description |
|--------|------|-------------|
| `ai_answered` | TEXT | `answered`, `missed`, or `abandoned` |
| `ai_answered_reason` | TEXT | Why this determination was made |
| `ai_answered_confidence` | NUMERIC(3,2) | 0.00–1.00 confidence score |

### How the Dashboard Uses It

The risk dashboard answer rate uses:
```sql
COALESCE(ca.ai_answered, CASE WHEN ca.answered THEN 'answered' ELSE 'missed' END) = 'answered'
```

This means: use the AI determination if available, fall back to CallRail's boolean for calls not yet processed.

### Answer Rate Formula

```
answer_rate = biz_hour_answered / biz_hour_calls
```

Both numerator and denominator are filtered to:
- Google Ads calls only (`is_google_ads_call()`)
- First-time callers only (`first_call = true`)
- Business hours only (client's timezone, biz days, biz hours)

### Additional Features

The service also provides:
- **Lead scoring** — Claude Haiku analyzes transcripts to determine if caller is a mold remediation lead
- **CallRail tagging** — Tags calls as `answered`/`missed`/`abandoned` in CallRail's UI
- **GHL feedback endpoint** — `/feedback` receives webhooks when clients mark leads as spam/qualified
- **DB backfill** — `POST /backfill-db` processes historical calls from the database

**Source file:** `callrail-intelligence/server.js`, `callrail-intelligence/lib/answer-detector.js`
