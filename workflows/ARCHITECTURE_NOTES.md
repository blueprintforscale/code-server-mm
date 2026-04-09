# Architecture Notes — Technical Debt & Known Issues

Last updated: 2026-04-08

This document captures known architectural issues, divergences, and technical debt that need to be addressed during the infrastructure review. It complements RULES.md (business logic) and the contractor scope doc (deliverables).

---

## 1. The Attribution Divergence Problem (Critical)

There are **four separate code paths** that compute Google Ads lead attribution. They use different logic, different data sources, and produce different results for edge cases.

| Code Path | Used By | Location | Has Tracker Attribution | Has Post-Touch | Has 60-Day Rule | Has Bot Filter |
|-----------|---------|----------|------------------------|----------------|-----------------|----------------|
| `mv_funnel_leads` (materialized view) | BlueprintOS funnel, reactivation badge | PostgreSQL | Yes | Yes | Yes | N/A (view) |
| `v_lead_revenue` (view) | Risk dashboard drill-downs | PostgreSQL | No | No | No | N/A |
| `get_dashboard_metrics()` (SQL function) | Risk dashboard summary cards | PostgreSQL | No | No | No | N/A |
| Lead-spreadsheet endpoint (inline SQL) | BlueprintOS lead drawer | `blueprintos-api/index.js` | Partial | No | No | Yes |

**Impact:** The same lead can show different attribution, different revenue, and different funnel stage on different dashboards. The risk dashboard shows ~7% higher ROAS than BlueprintOS for affected clients.

**Recommendation:** Consolidate to a single attribution computation. Options:
1. Make `mv_funnel_leads` the canonical source, rewrite others as thin wrappers
2. Compute attribution in ETL, write to a `lead_attribution` table, all views/endpoints read from it
3. Architect a proper attribution service (the contractor should decide)

**View definitions exported to:** `workflows/views/` directory for comparison.

---

## 2. mv_funnel_leads Complexity

The materialized view is a single SQL query with:
- 204 lines of SQL
- 30+ subqueries (EXISTS checks, COALESCE aggregations, LEAST/GREATEST computations)
- 3 CTEs (phone_groups, lead_base, outer select)
- Business logic for: attribution cascade, first GA touch time, last prior interaction, prior treatment detection, 60-day reactivation rule, post-touch revenue filtering, funnel stage computation, spam detection, abandoned detection

**Problems:**
- No version control — the view definition only exists in PostgreSQL. A snapshot is saved in `workflows/views/mv_funnel_leads.sql` but this is a point-in-time export, not a source of truth.
- REFRESH takes increasing time as data grows (~30 seconds currently, will grow with clients)
- Any rule change requires DROP + CREATE of the entire view
- Multiple agents/sessions modifying the view simultaneously risk overwriting each other's changes
- No incremental refresh — the entire 41K+ row view is rebuilt from scratch

**Recommendation:** Break into smaller composable views or move computation to ETL.

---

## 3. Lead-Spreadsheet Endpoint Has Its Own Attribution

The `/clients/:customerId/lead-spreadsheet` endpoint in `blueprintos-api/index.js` has a ~300-line inline SQL query that:
- Computes its own attribution using `is_google_ads_call()` function (doesn't include tracker attribution)
- Returns leads from `hcp_customers` directly (not from `mv_funnel_leads`)
- Has separate unmatched_calls and unmatched_forms CTEs with their own filters
- Recently added: bot spam filter, 60-day repeat caller filter

This means a lead can be `google_ads` in `mv_funnel_leads` (via tracker attribution) but `unmatched` in the lead-spreadsheet endpoint (because it doesn't know about trackers). This is what caused the missing "Reactivated" badge for Darren Richardson.

**Recommendation:** The lead-spreadsheet endpoint should read from `mv_funnel_leads` instead of computing attribution inline.

---

## 4. Phone Group Cross-Reference Bug (Being Fixed)

The `has_prior_treatment` and `last_prior_interaction` computations in `mv_funnel_leads` used `hc.hcp_customer_id` instead of `ANY(pg.all_ids)`. This means they only checked treatment history on the same HCP customer record, missing cases where a customer has multiple HCP records under different phones.

**Example:** Ann-Marie Porter has two HCP records — one from Aug 2025 (other source, had treatment) and one from Jan 2026 (google_ads). The prior treatment on the first record wasn't detected because the view only checked the second record.

**Status:** Fix in progress (changing to `ANY(pg.all_ids)`). 86 leads across all clients are affected.

---

## 5. is_google_ads_call() Function

A SQL function that determines if a CallRail call is from Google Ads:
```sql
is_google_ads_call(source, source_name, gclid) → BOOLEAN
```

Returns TRUE when `source IN ('Google Ads', 'Google Ads 2')` AND `source_name` is NOT a GBP/GMB/Main Business Line tracker, OR `gclid IS NOT NULL`.

**Problem:** This function doesn't know about `callrail_trackers`. Calls through Google Ads Call Extension trackers that CallRail tags as "Direct" are missed by this function but caught by the tracker check in `mv_funnel_leads`. The lead-spreadsheet endpoint uses this function, creating a divergence.

**Recommendation:** Either update the function to check `callrail_trackers`, or stop using it in favor of reading from `mv_funnel_leads`.

---

## 6. No Migration System

Database schema changes are made via ad-hoc SQL commands. There is no sequential migration system (like Flyway, Alembic, or even numbered SQL files).

**Recent changes made without migrations:**
- `calls.tracker_id` column added
- `callrail_trackers` table created
- `ghl_contacts.kpi_date_created` column added
- `mv_funnel_leads` rebuilt 4+ times in one session
- `v_lead_revenue` and `get_dashboard_metrics()` have unknown change history

**Recommendation:** Adopt a migration system. Even a simple numbered SQL file approach (`001_initial.sql`, `002_add_tracker_id.sql`, etc.) would be a major improvement.

---

## 7. No Automated Testing

There are no unit tests, integration tests, or smoke tests for:
- Attribution logic (the most critical business logic)
- ROAS calculations
- Funnel stage computation
- API endpoints
- ETL pipeline correctness

**Impact:** Every change is verified manually by running queries and checking dashboards. Rule changes can silently break edge cases.

**Recommendation:** At minimum, create a set of SQL-based regression tests that verify known leads are attributed correctly. Run them after every view rebuild.

---

## 8. Secrets in Code

- CallRail API key visible in conversation history and ETL scripts
- GitHub personal access token was shared in a conversation (rotated)
- GHL API keys stored in `clients` table (plaintext)
- Stripe keys in `.env` files
- No secret rotation policy

**Recommendation:** Move to a secret manager or at minimum use environment variables consistently.

---

## 9. Data Flow Summary

```
External APIs          ETL Scripts              PostgreSQL                    API/Dashboards
─────────────       ─────────────────        ──────────────               ──────────────────
CallRail       →    classify_calls.py    →   calls table              →   is_google_ads_call()
CallRail       →    classify_calls.py    →   form_submissions         →   lead-spreadsheet endpoint
HousecallPro   →    pull_hcp_data.py     →   hcp_customers/jobs/etc   →   mv_funnel_leads (materialized view)
Jobber         →    pull_jobber_data.py  →   jobber_customers/etc     →   getJobberFunnel() inline SQL
GHL            →    pull_ghl_data.py     →   ghl_contacts/opps        →   spam detection, GCLID fallback
Google Ads     →    google-ads ETL       →   campaign_daily_metrics   →   ad spend for ROAS/CPL
CallRail API   →    backfill_tracker.py  →   callrail_trackers        →   tracker attribution in mv_funnel_leads

                                              mv_funnel_leads          →   BlueprintOS funnel (HCP clients)
                                              v_lead_revenue           →   Risk dashboard drill-downs
                                              get_dashboard_metrics()  →   Risk dashboard summary cards
                                              inline SQL               →   BlueprintOS funnel (Jobber clients)
                                              inline SQL               →   Lead-spreadsheet / drawer
```

---

## 10. Files Reference

| File | Purpose |
|------|---------|
| `workflows/RULES.md` | All business rules (1,500+ lines) |
| `workflows/RISK_THRESHOLDS.md` | Risk dashboard thresholds |
| `workflows/views/mv_funnel_leads.sql` | Materialized view snapshot (point-in-time) |
| `workflows/views/v_lead_revenue.sql` | Risk dashboard revenue view snapshot |
| `workflows/views/get_dashboard_metrics.sql` | Risk dashboard summary function snapshot |
| `workflows/call-classifier/classify_calls.py` | CallRail ETL (calls + forms) |
| `workflows/call-classifier/backfill_tracker_id.py` | Tracker ID backfill utility |
| `workflows/call-classifier/migration_tracker_attribution.sql` | Schema changes from 2026-04-06 |
| `workflows/ghl-sync/pull_ghl_data.py` | GHL ETL (contacts, opportunities) |
| `workflows/hcp-sync/pull_hcp_data.py` | HCP ETL (customers, jobs, invoices, etc.) |
| `workflows/jobber-sync/pull_jobber_data.py` | Jobber ETL |
| `apps/blueprintos-api/index.js` | BlueprintOS API (3,300+ lines, all endpoints) |
| `workflows/risk-dashboard/server.js` | Risk dashboard server |
