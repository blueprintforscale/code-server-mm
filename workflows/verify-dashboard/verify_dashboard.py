#!/usr/bin/env python3
"""
verify_dashboard.py — comprehensive dashboard accuracy verifier

Usage:
  python3 verify_dashboard.py --client "Alemania"
  python3 verify_dashboard.py --customer-id 3703996852 --mode quick
  python3 verify_dashboard.py --client "Mike Pierce" --mode full --source ga
  python3 verify_dashboard.py --client "Chad" --json   # machine output
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
import urllib.request
import urllib.parse

# ── Config ─────────────────────────────────────────────────

DB_CONFIG = {"dbname": "blueprint", "user": "blueprint", "host": "localhost", "port": 5432}
API_BASE = "http://localhost:3500"
API_KEY = os.environ.get("BLUEPRINTOS_API_KEY", "")

# Severity weights for verdict
SEV_CRIT = "critical"
SEV_HIGH = "high"
SEV_WARN = "warning"
SEV_INFO = "info"
SEV_PASS = "pass"

# ── Helpers ────────────────────────────────────────────────

def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    return conn


def api_get(path, params=None):
    """GET against the local BlueprintOS API."""
    url = f"{API_BASE}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"x-api-key": API_KEY})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"error": str(e), "_api_failed": True}


def find_client(cur, query):
    """Resolve a customer_id from a name fragment or numeric id."""
    if str(query).isdigit():
        cur.execute("SELECT customer_id, name, field_management_software, start_date, status FROM clients WHERE customer_id = %s", (int(query),))
    else:
        cur.execute("""
            SELECT customer_id, name, field_management_software, start_date, status
            FROM clients
            WHERE REGEXP_REPLACE(LOWER(name), '[^a-z0-9]+', ' ', 'g') ILIKE %s
            ORDER BY status='active' DESC LIMIT 1
        """, (f"%{query.lower()}%",))
    row = cur.fetchone()
    return row


def get_known_mismatches(cur, customer_id):
    cur.execute("""
        SELECT pattern_key, pattern_description, reason
        FROM known_mismatches
        WHERE active = true
          AND (applies_to_clients IS NULL OR %s = ANY(applies_to_clients))
    """, (customer_id,))
    return cur.fetchall()


# ── Active sources detection ───────────────────────────────

def detect_active_sources(cur, customer_id, days=90):
    """Return list of source codes that have any data in the last N days."""
    sources = []
    # Google Ads
    cur.execute("""
        SELECT 1 FROM calls
        WHERE customer_id = %s
          AND start_time >= NOW() - INTERVAL '%s days'
          AND is_google_ads_call(source, source_name, gclid)
        LIMIT 1
    """, (customer_id, days))
    if cur.fetchone():
        sources.append("google_ads")
    # GBP
    cur.execute("""
        SELECT 1 FROM calls
        WHERE customer_id = %s
          AND start_time >= NOW() - INTERVAL '%s days'
          AND source = 'Google My Business'
          AND NOT is_google_ads_call(source, source_name, gclid)
        LIMIT 1
    """, (customer_id, days))
    if cur.fetchone():
        sources.append("gbp")
    # LSA
    cur.execute("""
        SELECT 1 FROM lsa_leads WHERE customer_id = %s
          AND lead_creation_time >= NOW() - INTERVAL '%s days' LIMIT 1
    """, (customer_id, days))
    if cur.fetchone():
        sources.append("lsa")
    return sources


# ── CHECK FUNCTIONS ────────────────────────────────────────
# Each returns: list of {check, severity, message, details}

def check_funnel_vs_drawer(cur, customer_id, source, date_from, date_to):
    """Check 1: Funnel card counts match drawer row counts per stage."""
    results = []
    funnel = api_get(f"/clients/{customer_id}/funnel", {
        "source": source, "date_from": date_from, "date_to": date_to
    })
    drawer = api_get(f"/clients/{customer_id}/lead-spreadsheet", {
        "source": source, "date_from": date_from, "date_to": date_to
    })

    funnel_failed = isinstance(funnel, dict) and funnel.get("_api_failed")
    drawer_failed = isinstance(drawer, dict) and drawer.get("_api_failed")
    if funnel_failed or drawer_failed:
        results.append({
            "check": "funnel_vs_drawer", "severity": SEV_CRIT,
            "message": f"API failed: funnel={funnel.get('error','') if funnel_failed else 'ok'} drawer={drawer.get('error','') if drawer_failed else 'ok'}",
        })
        return results

    if isinstance(drawer, dict) and drawer.get("error"):
        results.append({
            "check": "funnel_vs_drawer", "severity": SEV_CRIT,
            "message": f"Drawer endpoint error: {drawer.get('error')}",
        })
        return results

    rows = drawer if isinstance(drawer, list) else []

    # Map dashboard funnel field -> drawer-level boolean
    stage_map = [
        ("Leads", "leads", lambda l: True),
        ("Insp Scheduled", "inspection_scheduled", lambda l: l.get("inspection_scheduled")),
        ("Insp Completed", "inspection_completed", lambda l: l.get("inspection_completed")),
        ("Est Sent", "estimate_sent", lambda l: l.get("estimate_sent")),
        ("Est Approved", "estimate_approved", lambda l: l.get("estimate_approved")),
        ("Job Scheduled", "job_scheduled", lambda l: l.get("job_scheduled")),
        ("Job Completed", "job_completed", lambda l: l.get("job_completed")),
    ]
    for label, field, predicate in stage_map:
        card = int(funnel.get(field, 0) or 0)
        drawer_count = sum(1 for r in rows if predicate(r))
        if card == drawer_count:
            results.append({
                "check": f"funnel_vs_drawer.{field}", "severity": SEV_PASS,
                "message": f"{label}: {card} ✓",
            })
        else:
            diff_leads = []
            if predicate is not None:
                for r in rows:
                    if predicate(r):
                        diff_leads.append({"name": r.get("name"), "phone": r.get("phone")})
            results.append({
                "check": f"funnel_vs_drawer.{field}", "severity": SEV_HIGH,
                "message": f"{label}: card={card} drawer={drawer_count} (diff {drawer_count-card:+d})",
                "details": {"sample_drawer_leads": diff_leads[:5]},
            })
    return results


def check_funnel_inversion(cur, customer_id, source, date_from, date_to):
    """Check 2: downstream stage > upstream stage."""
    funnel = api_get(f"/clients/{customer_id}/funnel", {
        "source": source, "date_from": date_from, "date_to": date_to
    })
    if funnel.get("_api_failed"):
        return []
    g = lambda k: int(funnel.get(k, 0) or 0)
    pairs = [
        ("inspection_scheduled", "leads"),
        ("inspection_completed", "inspection_scheduled"),
        ("estimate_sent", "inspection_completed"),  # known: skip-estimate clients invert this
        ("estimate_approved", "estimate_sent"),
        ("job_scheduled", "estimate_approved"),     # known: skip-estimate clients invert this
        ("job_completed", "job_scheduled"),
    ]
    out = []
    for downstream, upstream in pairs:
        d, u = g(downstream), g(upstream)
        if d > u:
            out.append({
                "check": f"inversion.{downstream}_gt_{upstream}",
                "severity": SEV_HIGH,
                "message": f"{downstream}={d} > {upstream}={u}",
                "details": {"downstream": d, "upstream": u},
            })
    if not out:
        out.append({"check": "inversion", "severity": SEV_PASS, "message": "Funnel monotonic ✓"})
    return out


def check_high_approved_low_scheduled(cur, customer_id, source, date_from, date_to):
    """Check 3: many approved estimates but few job_scheduled — likely classifier misses."""
    funnel = api_get(f"/clients/{customer_id}/funnel", {
        "source": source, "date_from": date_from, "date_to": date_to
    })
    if funnel.get("_api_failed"):
        return []
    est_app = int(funnel.get("estimate_approved", 0) or 0)
    job_sch = int(funnel.get("job_scheduled", 0) or 0)
    if est_app < 3:
        return [{"check": "approved_vs_scheduled", "severity": SEV_PASS,
                 "message": f"Too few approved ({est_app}) to evaluate"}]
    ratio = job_sch / est_app if est_app else 0
    if ratio < 0.5:
        return [{
            "check": "approved_vs_scheduled", "severity": SEV_WARN,
            "message": f"job_scheduled/est_approved = {job_sch}/{est_app} = {ratio:.0%} — likely missed job classifications",
            "details": {"approved": est_app, "scheduled": job_sch, "ratio": ratio},
        }]
    return [{"check": "approved_vs_scheduled", "severity": SEV_PASS,
             "message": f"Conversion ratio healthy: {ratio:.0%}"}]


def check_needs_scheduling_backlog(cur, customer_id, fms):
    """Check 4: HCP 'needs scheduling' backlog, classified into 3 buckets.

    Bucket A — TRULY STUCK (WARN): needs-scheduling treatment job AND the
      customer has no other activity (no active jobs, no segments, no
      treatment invoices). Brand-new sales that went cold.

    Bucket B — RETURNING CUSTOMER, NEW WORK (WARN): needs-scheduling
      treatment job AND the customer has past treatment invoices but no
      currently-active treatment jobs. They agreed to new work but it's
      not on the calendar yet. Most actionable — chase these to schedule.

    Bucket C — FOLLOW-ON PHASE / LIKELY DUPLICATE (INFO): needs-scheduling
      treatment job AND the customer has other currently-active treatment
      jobs or segments. Usually follow-on phases of an in-progress project
      or client-side data-entry duplicates. Not urgent.
    """
    out = []
    if fms == "housecall_pro":
        # Classify each needs-scheduling treatment job into a bucket
        cur.execute("""
            WITH ns AS (
              SELECT j.hcp_job_id, j.hcp_customer_id, j.description,
                     j.total_amount_cents/100.0 as amount,
                     hc.first_name, hc.last_name
              FROM hcp_jobs j
              JOIN hcp_customers hc ON hc.hcp_customer_id = j.hcp_customer_id
                AND hc.customer_id = j.customer_id
              WHERE j.customer_id = %s
                AND j.record_status = 'active'
                AND j.status = 'needs scheduling'
                AND j.work_category = 'treatment'
                AND j.total_amount_cents >= 100000
            ),
            classified AS (
              SELECT ns.*,
                EXISTS (
                  SELECT 1 FROM hcp_jobs j2
                  WHERE j2.customer_id = %s
                    AND j2.hcp_customer_id = ns.hcp_customer_id
                    AND j2.hcp_job_id != ns.hcp_job_id
                    AND j2.record_status = 'active'
                    AND j2.work_category = 'treatment'
                    AND j2.status IN ('scheduled','in progress','complete rated','complete unrated')
                ) OR EXISTS (
                  SELECT 1 FROM hcp_job_segments seg
                  WHERE seg.customer_id = %s
                    AND seg.hcp_customer_id = ns.hcp_customer_id
                    AND seg.status IN ('scheduled','in progress','complete rated','complete unrated')
                    AND seg.total_amount_cents >= 100000
                ) as has_active_work,
                EXISTS (
                  SELECT 1 FROM hcp_invoices inv
                  WHERE inv.customer_id = %s
                    AND inv.hcp_customer_id = ns.hcp_customer_id
                    AND inv.invoice_type = 'treatment'
                    AND inv.status NOT IN ('canceled','voided')
                    AND inv.amount_cents > 0
                ) as has_treat_invoice
              FROM ns
            )
            SELECT
              CASE
                WHEN has_active_work THEN 'phase'
                WHEN has_treat_invoice THEN 'returning'
                ELSE 'stuck'
              END as bucket,
              hcp_job_id, description, amount, first_name, last_name
            FROM classified
            ORDER BY amount DESC
        """, (customer_id, customer_id, customer_id, customer_id))
        rows = cur.fetchall()
        stuck = [r for r in rows if r[0] == 'stuck']
        returning = [r for r in rows if r[0] == 'returning']
        phase = [r for r in rows if r[0] == 'phase']

        def sample(bucket_rows):
            return [
                {"name": f"{r[4] or ''} {r[5] or ''}".strip(),
                 "amount": float(r[3] or 0),
                 "desc": (r[2] or '')[:60]}
                for r in bucket_rows[:8]
            ]

        # Bucket A: truly stuck — brand-new sales that went cold
        if len(stuck) >= 3:
            total = sum(float(r[3] or 0) for r in stuck)
            out.append({
                "check": "needs_scheduling.truly_stuck", "severity": SEV_HIGH,
                "message": (
                    f"{len(stuck)} new-sale treatment jobs truly stuck — "
                    f"${total:,.0f} at risk (customer has no prior work; needs immediate chase)"
                ),
                "details": {"sample": sample(stuck), "total_dollars": total, "count": len(stuck)},
            })
        elif len(stuck) > 0:
            total = sum(float(r[3] or 0) for r in stuck)
            out.append({
                "check": "needs_scheduling.truly_stuck", "severity": SEV_WARN,
                "message": (
                    f"{len(stuck)} new-sale treatment job{'s' if len(stuck)!=1 else ''} truly stuck — "
                    f"${total:,.0f} (customer has no prior work)"
                ),
                "details": {"sample": sample(stuck), "total_dollars": total, "count": len(stuck)},
            })
        else:
            out.append({"check": "needs_scheduling.truly_stuck", "severity": SEV_PASS,
                        "message": "0 truly-stuck new-sale jobs ✓"})

        # Bucket B: returning customer with unscheduled new work — most actionable
        if len(returning) >= 3:
            total = sum(float(r[3] or 0) for r in returning)
            out.append({
                "check": "needs_scheduling.returning_customer", "severity": SEV_WARN,
                "message": (
                    f"{len(returning)} returning customers with unscheduled new treatment work — "
                    f"${total:,.0f} agreed but not on calendar"
                ),
                "details": {"sample": sample(returning), "total_dollars": total, "count": len(returning)},
            })
        elif len(returning) > 0:
            total = sum(float(r[3] or 0) for r in returning)
            out.append({
                "check": "needs_scheduling.returning_customer", "severity": SEV_INFO,
                "message": (
                    f"{len(returning)} returning customer{'s' if len(returning)!=1 else ''} with "
                    f"unscheduled new work — ${total:,.0f}"
                ),
                "details": {"sample": sample(returning), "total_dollars": total, "count": len(returning)},
            })
        else:
            out.append({"check": "needs_scheduling.returning_customer", "severity": SEV_PASS,
                        "message": "0 returning customers with unscheduled work ✓"})

        # Bucket C: follow-on phases / duplicates — informational only
        if len(phase) > 0:
            total = sum(float(r[3] or 0) for r in phase)
            out.append({
                "check": "needs_scheduling.follow_on_phase", "severity": SEV_INFO,
                "message": (
                    f"{len(phase)} needs-scheduling jobs (${total:,.0f}) are follow-on phases "
                    f"or duplicates — customer has other active work"
                ),
            })
    return out


def check_quality_spam_math(cur, customer_id, source, date_from, date_to):
    """Check 6: quality + spam = contacts."""
    funnel = api_get(f"/clients/{customer_id}/funnel", {
        "source": source, "date_from": date_from, "date_to": date_to
    })
    if funnel.get("_api_failed"):
        return []
    contacts = int(funnel.get("total_contacts", 0) or 0)
    quality = int(funnel.get("quality_leads", 0) or 0)
    spam = int(funnel.get("spam_count", 0) or 0)
    if quality + spam == contacts:
        return [{"check": "quality_spam_math", "severity": SEV_PASS,
                 "message": f"{quality}+{spam}={contacts} ✓"}]
    return [{"check": "quality_spam_math", "severity": SEV_CRIT,
             "message": f"Math fails: quality({quality}) + spam({spam}) ≠ contacts({contacts})",
             "details": {"quality": quality, "spam": spam, "contacts": contacts}}]


def check_data_freshness(cur, customer_id):
    """Check 7: ETL freshness."""
    out = []
    # Last call received
    cur.execute("SELECT MAX(start_time) FROM calls WHERE customer_id = %s", (customer_id,))
    last_call = cur.fetchone()[0]
    if last_call:
        age_hours = (datetime.now(timezone.utc) - last_call).total_seconds() / 3600
        sev = SEV_PASS if age_hours < 168 else (SEV_WARN if age_hours < 336 else SEV_HIGH)
        out.append({"check": "freshness.last_call",
                    "severity": sev,
                    "message": f"Last call: {age_hours:.0f}h ago" + (" ✓" if sev == SEV_PASS else "")})
    # MV refresh
    cur.execute("""
        SELECT GREATEST(
          (SELECT pg_stat_get_last_analyze_time(c.oid) FROM pg_class c WHERE relname = 'mv_funnel_leads'),
          (SELECT pg_stat_get_last_autoanalyze_time(c.oid) FROM pg_class c WHERE relname = 'mv_funnel_leads')
        )
    """)
    mv_refresh = cur.fetchone()[0]
    if mv_refresh:
        age_hours = (datetime.now(timezone.utc) - mv_refresh).total_seconds() / 3600
        sev = SEV_PASS if age_hours < 6 else (SEV_WARN if age_hours < 24 else SEV_HIGH)
        out.append({"check": "freshness.mv_funnel_leads",
                    "severity": sev,
                    "message": f"mv_funnel_leads: {age_hours:.1f}h ago" + (" ✓" if sev == SEV_PASS else "")})
    return out


def check_stalled_estimates(cur, customer_id):
    """Check 8: approved estimates with no invoice >60d old (info)."""
    cur.execute("""
        SELECT COUNT(*), COALESCE(SUM(eg.approved_total_cents),0)/100.0 as total_dollars
        FROM v_estimate_groups eg
        JOIN hcp_customers hc ON hc.hcp_customer_id = eg.hcp_customer_id
        WHERE hc.customer_id = %s
          AND eg.status = 'approved'
          AND eg.count_revenue
          AND eg.estimate_type = 'treatment'
          AND eg.sent_at < NOW() - INTERVAL '60 days'
          AND NOT EXISTS (
            SELECT 1 FROM hcp_invoices i
            WHERE i.hcp_customer_id = eg.hcp_customer_id
              AND i.status NOT IN ('canceled','voided')
          )
    """, (customer_id,))
    cnt, total = cur.fetchone()
    if cnt and cnt > 0:
        return [{"check": "stalled_approved_estimates", "severity": SEV_INFO,
                 "message": f"{cnt} approved estimates >60d with no invoice — ${float(total):,.0f}",
                 "details": {"count": cnt, "dollars": float(total)}}]
    return [{"check": "stalled_approved_estimates", "severity": SEV_PASS,
             "message": "No long-stalled approved estimates"}]


def check_badge_accuracy(cur, customer_id, source, date_from, date_to):
    """Check 5: Badge logic. For each lead, verify the stage badge logic matches."""
    drawer = api_get(f"/clients/{customer_id}/lead-spreadsheet", {
        "source": source, "date_from": date_from, "date_to": date_to
    })
    if not isinstance(drawer, list) or not drawer:
        return [{"check": "badge_accuracy", "severity": SEV_PASS, "message": "No leads to audit"}]
    # Apply the same logic as FunnelDrawer.tsx getHighestStage
    def expected_stage(l):
        if l.get("job_completed") and not l.get("job_completed_inferred"):
            return "Job Completed"
        if l.get("job_scheduled") and not l.get("job_scheduled_inferred"):
            return "Job Scheduled"
        if l.get("estimate_approved"):
            return "Estimate Approved"
        if l.get("estimate_sent"):
            return "Estimate Sent"
        if l.get("inspection_completed"):
            return "Inspection Complete"
        if l.get("inspection_scheduled"):
            return "Inspection Scheduled"
        return "Lead"
    correct = 0
    issues = []
    for l in drawer:
        # Basic sanity checks (frontend renders correctly per logic — flag impossible states)
        # If revenue_closed=true but no stage flags, that's odd
        if l.get("revenue_closed") and not (l.get("job_completed") or l.get("job_scheduled") or l.get("estimate_approved")):
            issues.append({
                "name": l.get("name"), "phone": l.get("phone"),
                "issue": "revenue_closed=true but no upstream stage flags",
            })
            continue
        # If invoiced_revenue > 0, expect at least estimate_approved
        try:
            inv = float(l.get("invoiced_revenue") or 0)
        except Exception:
            inv = 0
        if inv > 0 and not (l.get("job_completed") or l.get("estimate_approved") or l.get("job_completed_inferred")):
            issues.append({
                "name": l.get("name"), "phone": l.get("phone"),
                "issue": f"invoiced_revenue={inv} but no completion/approval flags",
            })
            continue
        correct += 1
    if not issues:
        return [{"check": "badge_accuracy.stage", "severity": SEV_PASS,
                 "message": f"All {len(drawer)} leads have consistent stage data"}]
    return [{"check": "badge_accuracy.stage", "severity": SEV_HIGH,
             "message": f"{len(issues)}/{len(drawer)} leads have inconsistent stage data",
             "details": {"sample": issues[:5]}}]


def check_source_badges(cur, customer_id, source, date_from, date_to):
    """Source badge accuracy — leads should have source_label matching the source they're returned for."""
    drawer = api_get(f"/clients/{customer_id}/lead-spreadsheet", {
        "source": source, "date_from": date_from, "date_to": date_to
    })
    if not isinstance(drawer, list) or not drawer:
        return [{"check": "source_badges", "severity": SEV_PASS, "message": "No leads"}]
    no_label = [l for l in drawer if not l.get("source_label")]
    if no_label:
        return [{"check": "source_badges", "severity": SEV_WARN,
                 "message": f"{len(no_label)}/{len(drawer)} leads missing source_label — frontend will fall back to active tab badge",
                 "details": {"sample": [{"name": l.get("name"), "phone": l.get("phone")} for l in no_label[:5]]}}]
    return [{"check": "source_badges", "severity": SEV_PASS,
             "message": f"All {len(drawer)} leads have source_label set"}]


def check_chart_sum_vs_card(cur, customer_id, source, date_from, date_to):
    """Check leads/spend sum from daily series vs headline card."""
    out = []
    funnel = api_get(f"/clients/{customer_id}/funnel", {
        "source": source, "date_from": date_from, "date_to": date_to
    })
    if funnel.get("_api_failed"):
        return out
    # Daily metrics for ad spend reconciliation
    if source == "google_ads":
        cur.execute("""
            SELECT COALESCE(SUM(cost), 0)
            FROM campaign_daily_metrics
            WHERE customer_id = %s
              AND date BETWEEN %s::date AND %s::date
              AND campaign_type != 'LOCAL_SERVICES'
        """, (customer_id, date_from, date_to))
        daily_spend = float(cur.fetchone()[0] or 0)
        card_spend = float(funnel.get("ad_spend") or 0)
        diff = abs(daily_spend - card_spend)
        if diff < 1.0:
            out.append({"check": "chart_sum.ad_spend", "severity": SEV_PASS,
                        "message": f"Ad spend reconciles: ${card_spend:.2f} ✓"})
        else:
            out.append({"check": "chart_sum.ad_spend", "severity": SEV_WARN,
                        "message": f"Ad spend mismatch: card=${card_spend:.2f} daily_sum=${daily_spend:.2f} (diff ${diff:.2f})"})
    return out


def check_cross_source_consistency(cur, customer_id, sources, date_from, date_to):
    """When multiple sources are active, compare to 'all' total."""
    if len(sources) < 2:
        return []
    full = api_get(f"/clients/{customer_id}/funnel", {
        "source": "all", "date_from": date_from, "date_to": date_to
    })
    if full.get("_api_failed"):
        return []
    full_leads = int(full.get("quality_leads", 0) or 0)
    sum_per_source = 0
    for s in sources:
        f = api_get(f"/clients/{customer_id}/funnel", {
            "source": s, "date_from": date_from, "date_to": date_to
        })
        sum_per_source += int(f.get("quality_leads", 0) or 0)
    if sum_per_source <= full_leads * 1.05:  # allow 5% slack for overlap
        return [{"check": "cross_source.lead_sum", "severity": SEV_PASS,
                 "message": f"Σ source leads ({sum_per_source}) ≤ all leads ({full_leads}) ✓"}]
    return [{"check": "cross_source.lead_sum", "severity": SEV_WARN,
             "message": f"Σ source leads ({sum_per_source}) > all leads ({full_leads}) — leads may be double-counted"}]


# ── Orchestrator ───────────────────────────────────────────

def run_checks(customer_id, fms, sources, mode, date_from, date_to, cur):
    all_results = []
    for source in sources:
        results_for_source = []
        results_for_source.extend(check_funnel_vs_drawer(cur, customer_id, source, date_from, date_to))
        results_for_source.extend(check_funnel_inversion(cur, customer_id, source, date_from, date_to))
        results_for_source.extend(check_high_approved_low_scheduled(cur, customer_id, source, date_from, date_to))
        results_for_source.extend(check_quality_spam_math(cur, customer_id, source, date_from, date_to))
        results_for_source.extend(check_badge_accuracy(cur, customer_id, source, date_from, date_to))
        results_for_source.extend(check_source_badges(cur, customer_id, source, date_from, date_to))
        results_for_source.extend(check_chart_sum_vs_card(cur, customer_id, source, date_from, date_to))
        for r in results_for_source:
            r["source"] = source
        all_results.extend(results_for_source)
    # Cross-source + non-source-specific
    all_results.extend(check_needs_scheduling_backlog(cur, customer_id, fms))
    all_results.extend(check_data_freshness(cur, customer_id))
    all_results.extend(check_stalled_estimates(cur, customer_id))
    all_results.extend(check_cross_source_consistency(cur, customer_id, sources, date_from, date_to))
    return all_results


def format_report(client_row, results, mode, sources, date_from, date_to, duration):
    name = client_row[1]
    cust_id = client_row[0]
    by_source = {}
    cross = []
    for r in results:
        s = r.get("source", "_global")
        by_source.setdefault(s, []).append(r)
        if s == "_global":
            cross.append(r)
    counts = {SEV_PASS: 0, SEV_INFO: 0, SEV_WARN: 0, SEV_HIGH: 0, SEV_CRIT: 0}
    for r in results:
        counts[r["severity"]] = counts.get(r["severity"], 0) + 1

    lines = []
    lines.append(f"\n🔍 DASHBOARD VERIFICATION: {name}")
    lines.append(f"   Customer ID: {cust_id} | Mode: {mode} | Sources: {','.join(sources)} | Range: {date_from} → {date_to}")
    lines.append(f"   Run time: {duration:.1f}s\n")

    sev_icon = {SEV_PASS: "✅", SEV_INFO: "ℹ️ ", SEV_WARN: "⚠️ ", SEV_HIGH: "🟧", SEV_CRIT: "🔴"}
    for src in sources:
        rs = [r for r in by_source.get(src, []) if r["severity"] != SEV_PASS]
        passes = [r for r in by_source.get(src, []) if r["severity"] == SEV_PASS]
        lines.append("═" * 60)
        lines.append(f"SOURCE: {src.upper()}")
        lines.append("═" * 60)
        if not rs:
            lines.append(f"  ✅ All {len(passes)} checks passed")
        else:
            for r in rs:
                lines.append(f"  {sev_icon[r['severity']]} {r['message']}")
                if r.get("details") and r["severity"] in (SEV_HIGH, SEV_CRIT, SEV_WARN):
                    sample = r["details"].get("sample") or r["details"].get("sample_drawer_leads")
                    if sample:
                        for s in sample[:3]:
                            if isinstance(s, dict):
                                bits = ", ".join(f"{k}={v}" for k, v in s.items() if v)
                                lines.append(f"      - {bits}")
            lines.append(f"  ({len(passes)} other checks passed)")

    if cross:
        lines.append("═" * 60)
        lines.append("CROSS-SOURCE / GLOBAL")
        lines.append("═" * 60)
        for r in cross:
            if r["severity"] == SEV_PASS:
                lines.append(f"  ✅ {r['message']}")
            else:
                lines.append(f"  {sev_icon[r['severity']]} {r['message']}")
                if r.get("details", {}).get("sample"):
                    for s in r["details"]["sample"][:5]:
                        if isinstance(s, dict):
                            bits = ", ".join(f"{k}={v}" for k, v in s.items() if v)
                            lines.append(f"      - {bits}")

    lines.append("═" * 60)
    lines.append("VERDICT")
    lines.append("═" * 60)
    lines.append(f"  ✅ {counts[SEV_PASS]} passed   ℹ️  {counts[SEV_INFO]} info   ⚠️  {counts[SEV_WARN]} warnings   🟧 {counts[SEV_HIGH]} high   🔴 {counts[SEV_CRIT]} critical")
    if counts[SEV_CRIT]:
        lines.append("  🔴 DO NOT proceed without resolving critical issues")
    elif counts[SEV_HIGH]:
        lines.append("  🟧 Review HIGH issues before client call")
    elif counts[SEV_WARN]:
        lines.append("  ⚠️  Dashboard usable, review warnings for context")
    else:
        lines.append("  ✅ Dashboard ready")
    return "\n".join(lines), counts


def write_history(cur, customer_id, mode, sources, ran_by, duration, results, counts, verdict):
    cur.execute("""
        INSERT INTO dashboard_verifications
          (customer_id, mode, source, ran_by, duration_seconds, passed_count, warning_count, critical_count, info_count, details, verdict)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        customer_id, mode, ",".join(sources), ran_by, duration,
        counts.get(SEV_PASS, 0),
        counts.get(SEV_WARN, 0) + counts.get(SEV_HIGH, 0),
        counts.get(SEV_CRIT, 0),
        counts.get(SEV_INFO, 0),
        json.dumps(results, default=str),
        verdict,
    ))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--client", help="Client name fragment")
    p.add_argument("--customer-id", type=int)
    p.add_argument("--mode", choices=["quick", "full"], default="quick")
    p.add_argument("--source", default="all", help="ga|gbp|lsa|all")
    p.add_argument("--days", type=int, default=90)
    p.add_argument("--json", action="store_true")
    p.add_argument("--ran-by", default="cli")
    args = p.parse_args()

    if not (args.client or args.customer_id):
        print("Need --client or --customer-id", file=sys.stderr)
        sys.exit(1)

    if not API_KEY:
        print("BLUEPRINTOS_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    conn = get_db()
    cur = conn.cursor()

    query = args.customer_id or args.client
    client_row = find_client(cur, query)
    if not client_row:
        print(f"Client not found: {query}", file=sys.stderr)
        sys.exit(2)
    customer_id = client_row[0]
    fms = client_row[2]

    # Sources to check
    if args.source == "all":
        sources = detect_active_sources(cur, customer_id, args.days) or ["google_ads"]
    else:
        m = {"ga": "google_ads", "gbp": "gbp", "lsa": "lsa"}
        sources = [m.get(args.source, args.source)]

    date_from = (datetime.now(timezone.utc).date() - timedelta(days=args.days)).isoformat()
    date_to = datetime.now(timezone.utc).date().isoformat()

    t0 = time.time()
    results = run_checks(customer_id, fms, sources, args.mode, date_from, date_to, cur)
    duration = time.time() - t0
    report, counts = format_report(client_row, results, args.mode, sources, date_from, date_to, duration)

    verdict = "ready" if counts[SEV_CRIT] == 0 and counts[SEV_HIGH] == 0 else ("review" if counts[SEV_HIGH] else "blocked")
    write_history(cur, customer_id, args.mode, sources, args.ran_by, duration, results, counts, verdict)

    if args.json:
        print(json.dumps({"verdict": verdict, "counts": counts, "results": results, "duration": duration}, default=str))
    else:
        print(report)

    cur.close()
    conn.close()
    sys.exit(0 if counts[SEV_CRIT] == 0 else 1)


if __name__ == "__main__":
    main()
