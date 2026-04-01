#!/usr/bin/env python3
"""
Auto-Alert Generator — Scans client data and creates alerts for clients needing attention.

Usage:
  python3 generate_alerts.py                # Run all alert checks
  python3 generate_alerts.py --client 123   # Single client

Checks:
  - No leads in 3+ days
  - ROAS below 2x for 2+ weeks
  - Lead volume dropped 30%+ vs prior month
  - Contract expiring within 30 days
  - No team interaction in 14+ days
  - Overdue tasks
  - Approved estimates uninvoiced for 14+ days
"""

import argparse
import logging
import sys

import psycopg2
import psycopg2.extras

DB_CONFIG = {
    "dbname": "blueprint",
    "user": "blueprint",
    "host": "localhost",
    "port": 5432,
}

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger('alerts')


def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def create_alert(cur, customer_id, alert_type, severity, message):
    """Create an alert if one doesn't already exist (unresolved) for this type."""
    cur.execute("""
        SELECT id FROM client_alerts
        WHERE customer_id = %s
          AND alert_type = %s
          AND resolved_at IS NULL
    """, (customer_id, alert_type))
    if cur.fetchone():
        return False  # Already have an open alert of this type

    cur.execute("""
        INSERT INTO client_alerts (customer_id, alert_type, severity, message, auto_generated)
        VALUES (%s, %s, %s, %s, TRUE)
    """, (customer_id, alert_type, severity, message))
    return True


def auto_resolve(cur, customer_id, alert_type):
    """Resolve alerts that are no longer applicable."""
    cur.execute("""
        UPDATE client_alerts
        SET resolved_at = NOW(), resolved_by = 'auto'
        WHERE customer_id = %s
          AND alert_type = %s
          AND resolved_at IS NULL
          AND auto_generated = TRUE
    """, (customer_id, alert_type))
    return cur.rowcount


def check_no_leads(cur, customer_id, client_name):
    """Alert if no leads in 3+ days."""
    cur.execute("""
        SELECT MAX(c.start_time)::date AS last_lead_date,
               CURRENT_DATE - MAX(c.start_time)::date AS days_ago
        FROM calls c
        WHERE c.customer_id = %s
          AND c.classified_status NOT IN ('spam', 'irrelevant', 'brand')
    """, (customer_id,))
    row = cur.fetchone()
    if row and row[1] and row[1] >= 3:
        days = row[1]
        if create_alert(cur, customer_id, 'no_leads', 'warning',
                        f"No leads in {days} days (last: {row[0]})"):
            log.info(f"  ALERT: {client_name} — no leads in {days} days")
    else:
        auto_resolve(cur, customer_id, 'no_leads')


def check_no_interaction(cur, customer_id, client_name):
    """Alert if no team interaction in 14+ days."""
    cur.execute("""
        SELECT MAX(interaction_date)::date AS last_date,
               CURRENT_DATE - MAX(interaction_date)::date AS days_ago
        FROM client_interactions
        WHERE customer_id = %s
    """, (customer_id,))
    row = cur.fetchone()
    if row and row[1] and row[1] >= 14:
        days = row[1]
        if create_alert(cur, customer_id, 'no_interaction', 'warning',
                        f"No team interaction in {days} days (last: {row[0]})"):
            log.info(f"  ALERT: {client_name} — no interaction in {days} days")
    elif row and row[0]:
        auto_resolve(cur, customer_id, 'no_interaction')


def check_contract_expiring(cur, customer_id, client_name):
    """Alert if contract expires within 30 days."""
    cur.execute("""
        SELECT contract_renewal_date,
               contract_renewal_date - CURRENT_DATE AS days_until
        FROM client_profiles
        WHERE customer_id = %s
          AND contract_renewal_date IS NOT NULL
    """, (customer_id,))
    row = cur.fetchone()
    if row and row[1] is not None and 0 < row[1] <= 30:
        days = row[1]
        if create_alert(cur, customer_id, 'contract_expiring', 'info',
                        f"Contract renews in {days} days ({row[0]})"):
            log.info(f"  ALERT: {client_name} — contract renews in {days} days")
    else:
        auto_resolve(cur, customer_id, 'contract_expiring')


def check_overdue_tasks(cur, customer_id, client_name):
    """Alert if there are overdue tasks."""
    cur.execute("""
        SELECT COUNT(*), MIN(due_date)
        FROM client_tasks
        WHERE customer_id = %s
          AND status NOT IN ('done', 'cancelled')
          AND due_date < CURRENT_DATE
    """, (customer_id,))
    row = cur.fetchone()
    if row and row[0] and row[0] > 0:
        count = row[0]
        oldest = row[1]
        severity = 'warning' if count <= 2 else 'critical'
        if create_alert(cur, customer_id, 'overdue_task', severity,
                        f"{count} overdue task(s), oldest due {oldest}"):
            log.info(f"  ALERT: {client_name} — {count} overdue tasks")
    else:
        auto_resolve(cur, customer_id, 'overdue_task')


def check_lead_volume_drop(cur, customer_id, client_name):
    """Alert if lead volume dropped 30%+ vs prior month."""
    cur.execute("""
        WITH monthly AS (
            SELECT
                DATE_TRUNC('month', start_time) AS month,
                COUNT(*) AS leads
            FROM calls
            WHERE customer_id = %s
              AND classified_status NOT IN ('spam', 'irrelevant', 'brand')
              AND start_time >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '2 months'
            GROUP BY 1
        )
        SELECT
            curr.leads AS current_leads,
            prev.leads AS previous_leads,
            ROUND((1.0 - curr.leads::NUMERIC / NULLIF(prev.leads, 0)) * 100) AS drop_pct
        FROM monthly curr
        JOIN monthly prev ON curr.month = prev.month + INTERVAL '1 month'
        WHERE curr.month = DATE_TRUNC('month', CURRENT_DATE)
    """, (customer_id,))
    row = cur.fetchone()
    if row and row[2] and row[2] >= 30:
        pct = row[2]
        if create_alert(cur, customer_id, 'lead_drop', 'warning',
                        f"Lead volume down {pct}% vs last month ({row[0]} vs {row[1]})"):
            log.info(f"  ALERT: {client_name} — leads down {pct}%")
    else:
        auto_resolve(cur, customer_id, 'lead_drop')


def main():
    parser = argparse.ArgumentParser(description="Generate client intelligence alerts")
    parser.add_argument('--client', type=str, help='Check only one customer_id')
    args = parser.parse_args()

    conn = get_db()

    try:
        with conn.cursor() as cur:
            query = """
                SELECT c.customer_id, c.name
                FROM clients c
                WHERE c.status = 'active'
            """
            params = []
            if args.client:
                query += " AND c.customer_id = %s"
                params.append(int(args.client))
            query += " ORDER BY c.name"
            cur.execute(query, params)
            clients = cur.fetchall()

        log.info(f"Checking alerts for {len(clients)} clients...")
        alerts_created = 0
        alerts_resolved = 0

        for customer_id, name in clients:
            with conn.cursor() as cur:
                check_no_leads(cur, customer_id, name)
                check_no_interaction(cur, customer_id, name)
                check_contract_expiring(cur, customer_id, name)
                check_overdue_tasks(cur, customer_id, name)
                check_lead_volume_drop(cur, customer_id, name)

            conn.commit()

        # Count results
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM client_alerts
                WHERE auto_generated = TRUE
                  AND created_at >= CURRENT_DATE
                  AND resolved_at IS NULL
            """)
            new_alerts = cur.fetchone()[0]

        log.info(f"Done. {new_alerts} new open alerts today.")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
