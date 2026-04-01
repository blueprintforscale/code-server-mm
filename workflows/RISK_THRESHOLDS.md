# Risk & Flag Scoring Thresholds

> **Reference for the risk dashboard only.** These thresholds determine when a client shows as Risk, Flag, or Healthy.
> Not needed for general data pulls or dashboard building.
> Last updated: 2026-03-22

**Function:** `compute_risk_status()` in `risk-dashboard/migration.sql`

---

## How Status Is Determined

1. **Risk** = any risk trigger fired
   - Ads + Funnel risks = sort priority 1 (Both)
   - Ads risks only = sort priority 2
   - Funnel risks only = sort priority 3
2. **Flag** = ≥3 flag triggers fired (no risk triggers)
   - Sort priority 4
3. **Healthy** = everything else
   - Sort priority 5

**Floor rule:** CPL > $170 always forces at least Flag status.

**Manual override:** `clients.risk_override` can force any status (`'risk'`, `'flag'`, `'healthy'`, or `NULL` for computed).

---

## ROAS ≥ 3x Override

When ROAS ≥ 3x, the following risk triggers **downgrade to flags**:
- CPL risk (>$170)
- Lead count risk

Rationale: client is profitable despite individual metric concerns.

---

## Early Stage (Months 1-2)

| Trigger | Type | Condition |
|---------|------|-----------|
| Zero spend | Risk | $0 spend with budget set |
| Month 1 zero leads | Flag | No leads in first month |
| Month 2 low leads | Flag | ≤10 leads |
| Month 2 high CPL | Flag | CPL > $400 |
| Month 2 low book rate | Flag | < 20% |
| Month 2 stale | Flag | Days since last lead ≥ 14 |
| High spam | Flag | > 11% |
| High abandoned | Flag | > 11% |

---

## Full Risk — Ads (Months 3+)

| Trigger | Type | Condition |
|---------|------|-----------|
| Zero spend | Risk | $0 spend with budget set |
| Very low leads (months 3-5) | Risk | ≤10 leads |
| Low leads (months 6+, budget ≥$3K) | Risk | <20 leads |
| Low leads + low ROAS + low guarantee (months 6+) | Risk | <30 leads AND ROAS <0.9 AND guarantee ≤2 |
| High CPL | Risk | >$170 AND ROAS ≤3x |
| Lead volume drop (CPL >$140) | Risk | >30% drop vs prior period AND CPL >$140 |
| Lead volume drop (CPL ≤$140) | Flag | >30% drop vs prior period BUT CPL ≤$140 (ads still efficient) |
| Stale (months 4+) | Risk | ≥7 days since last lead |
| Stale (months 1-3) | Risk | >10 days since last lead |
| Overspend | Risk | >124% of budget |
| Underspend | Risk | <50% of budget |

---

## Full Risk — Funnel (Months 3+, only if field management connected)

| Trigger | Type | Condition |
|---------|------|-----------|
| Low book rate (free inspection) | Risk | <15% when guarantee ≤3 |
| Low book rate (paid inspection) | Risk | ≤10% when guarantee ≤3 |
| Low guarantee (months 7+) | Risk | <0.5x |
| Low ROAS | Risk | <60% |

---

## Flag Thresholds

| # | Metric | Condition |
|---|--------|-----------|
| 1 | Leads | 11-19 (months 3-5); 20-29 with low ROAS (months 6+) |
| 2 | CPL | $140-170; also <$40 (suspiciously low) |
| 3 | Lead volume change | -10% to -30% drop |
| 4 | Book rate | 15-28% free; 11-19% paid (unless guarantee >3x) |
| 5 | Guarantee | <0.5x at months 5-6 |
| 6 | ROAS | 60-100% |
| 7 | Spam rate | >11% |
| 8 | Abandoned rate | >11% |
| 9 | Overspend | 110-124% of budget |
| 12 | Days since lead | 4-6 days (months 4+); 7-10 days (months 1-3) |
| 13 | On-calendar | 0 or 1 GA inspections in next 14 days (months 3+) |

---


---

## Removed Triggers (2026-03-21)

The following were removed from risk/flag scoring:

| Removed | Was | Reason |
|---------|-----|--------|
| Overspend (>124% budget) | Ads Risk | Budget discrepancies are admin errors, not ad performance |
| Underspend (<50% budget) | Ads Risk | Budget discrepancies are admin errors, not ad performance |
| Moderate overspend (110-124%) | Flag | Same as above |
| Early overspend/underspend | Flag | Same as above |
| $0 spend with budget set | Ads Risk | Moved to flag — setup delays are not ad performance issues |
| Book rate (standalone) | Funnel Risk | Now a flag only; doesn't trigger risk alone |
| ROAS <60% (standalone) | Funnel Risk | Replaced by presentation risk framework |
| Leads + ROAS + guarantee combo | Ads Risk | ROAS/guarantee are funnel metrics, moved out of ads risk |

## Funnel Risk — Presentation Framework (New 2026-03-21)

Funnel risk now uses a **three-story framework** instead of individual metric thresholds.

**Guarantee risk (non-negotiable):**
- Guarantee < 0.5x at months 7+ → always funnel risk

**Presentation risk (months 5+):**

| Months | Lifetime threshold | Ramp-up threshold | Potential threshold |
|--------|-------------------|-------------------|---------------------|
| 5-6 | Guarantee ≥ 1.5x | Trailing 6mo or 3mo ROAS ≥ 2.0x | Trailing potential ROAS ≥ 2.0x |
| 7-9 | Guarantee ≥ 2.0x | Trailing 6mo or 3mo ROAS ≥ 2.5x | Trailing potential ROAS ≥ 2.5x |
| 10-12+ | Guarantee ≥ 2.5x | Trailing 6mo or 3mo ROAS ≥ 3.0x | Trailing potential ROAS ≥ 3.0x |

**Risk fires ONLY when all three stories fail.** Any single story succeeding = no presentation risk.

Potential ROAS = (closed revenue + open estimates) / ad spend for the trailing period.


## Metric Definitions (for risk scoring context)

| Metric | Formula | Notes |
|--------|---------|-------|
| CPL | `ad_spend / quality_leads` | Excludes LSA spend AND LSA leads |
| ROAS | `period_google_ads_revenue / ad_spend` | Excludes LSA spend AND LSA revenue |
| Quality leads | Distinct phones/emails from GA calls + GCLID forms, minus spam | Excludes LSA. Spam matched by phone AND email against GHL. Abandoned-as-spam when rate >20%. |
| Book rate | `inspections_booked / quality_leads` | Only for clients with field management |
| Guarantee | `program_price / ad_spend` | Uses program_price, NOT revenue |
| Lead volume change | `(quality_leads - prior_quality_leads) / prior_quality_leads` | Raw 30-day rolling: current period vs prior same-length period. Not pro-rated. |
| Prior CPL | `prior_ad_spend / prior_quality_leads` | CPL from the prior 30-day period. Used for CPL spike badge on frontend. |
| Spam rate | `spam_contacts / total_contacts` | Actual spam count from GHL matching (phone + email) / total GA contacts |
| Abandoned rate | `abandoned_ga_contacts / total_ga_contacts` | GA contacts matched to GHL abandoned (status or lost_reason). Period-scoped. |
| Days since lead | `today - MAX(last_ga_call, last_ga_form)` | Only GA-attributed leads |

---

## CPL Spike Badge (Frontend Only)

A red "CPL +X%" badge appears next to the client name when:
- CPL increased >30% vs prior period
- Current CPL is ≥$80
- Current CPL is <$140 (above $140 the cell coloring already flags it)

This is a visual indicator only — it does NOT affect risk/flag status or recovery scoring.

---

## Volume Alert Badge (Frontend + Slack Briefs)

A gold "Vol ↓XX%" badge appears next to the client name when:
- Lead volume dropped >30% vs prior 30-day period

This badge appears regardless of the client's risk/flag/healthy status. Clients feel volume drops the most — the phone stops ringing — so this badge ensures visibility even when CPL is healthy and the client isn't in Risk.

**Slack manager briefs:** A dedicated "VOLUME ALERTS" section appears between Flag and Healthy, listing all non-Risk clients with >30% volume drops. This ensures ads managers see volume dips even when CPL is healthy enough to keep the client out of Risk.

**Rationale (added 2026-03-23):** Volume drop >30% with healthy CPL (≤$140) is a momentum problem, not a cost problem. The ads are still efficient — there are just fewer opportunities. This can be seasonal, market-driven, or temporary. Putting these clients in Risk alongside chronic CPL/lead-count issues dilutes the urgency of true Risk clients.

---

## Recovery Arrow & Drilldown Verdict

The recovery arrow (table) and trajectory verdict (drilldown) check whether the **triggered metrics** are improving:

- **Risk clients:** only risk triggers are evaluated
- **Flag clients:** only flag triggers are evaluated
- **Healthy clients:** general direction signals shown (Volume, CPL, ROAS, Booking, Calendar)

**All** triggered metrics must be improving for the green "Recovering" arrow. Otherwise:
- Some improving = "Mixed — X/Y improving" (yellow)
- None improving + days since lead ≥7 = "Stalled" (red)
- None improving = "Needs Attention" (red)

**Trigger-to-signal mapping:**

| Trigger Pattern | Recovery Signal | Good When |
|----------------|----------------|-----------|
| CPL | 14d CPL recent vs prior | CPL going down |
| Lead volume / lead count | 14d volume change | Volume going up |
| Book rate | Trend book_delta | Booking going up |
| ROAS | Trend roas_delta | ROAS going up |
| Calendar | on_cal_14d count | >0 GA inspections |
| Days since lead | days_since_lead | ≤3 days |
| Over/underspend | ad_spend / budget | 50-124% of budget |
| Guarantee | Trend roas_delta | Trending up |

**Not evaluated for recovery:** Spam rate, abandoned rate (already reflected in quality lead count and CPL).

---

## Source File

`risk-dashboard/migration.sql` → `compute_risk_status()` function (line ~513+)
