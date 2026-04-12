#!/bin/bash
# Daily risk status snapshot — run after ETL completes
PSQL="/opt/homebrew/opt/postgresql@17/bin/psql -U blueprint blueprint"

$PSQL -c "
INSERT INTO risk_status_snapshots (customer_id, snapshot_date, status, risk_type, risk_triggers, flag_triggers, flag_count)
SELECT customer_id, CURRENT_DATE, status, risk_type, risk_triggers, flag_triggers, flag_count
FROM get_dashboard_with_risk()
ON CONFLICT (customer_id, snapshot_date) DO UPDATE SET
  status = EXCLUDED.status,
  risk_type = EXCLUDED.risk_type,
  risk_triggers = EXCLUDED.risk_triggers,
  flag_triggers = EXCLUDED.flag_triggers,
  flag_count = EXCLUDED.flag_count;
"

# Step 2: Update confirmed statuses (3-day confirmation + hysteresis)
$PSQL -c "SELECT update_confirmed_statuses();"

# Step 3: Write confirmed/pending status into today's snapshot for audit
$PSQL -c "
UPDATE risk_status_snapshots s
SET confirmed_status = ccs.confirmed_status,
    pending_status = ccs.pending_status
FROM client_confirmed_status ccs
WHERE s.customer_id = ccs.customer_id
  AND s.snapshot_date = CURRENT_DATE;
"

echo "$(date): Risk snapshot + confirmed status update completed for $(date +%Y-%m-%d)" >> /Users/bp/projects/workflows/risk-dashboard/snapshot.log
