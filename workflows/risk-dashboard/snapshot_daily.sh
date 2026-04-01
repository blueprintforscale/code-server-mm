#!/bin/bash
# Daily risk status snapshot — run after ETL completes
PSQL="/opt/homebrew/opt/postgresql@17/bin/psql -U blueprint blueprint"

# CRM-adjusted snapshot (with GHL spam/abandoned)
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

# Raw snapshot (without CRM spam — ads manager view)
$PSQL -c "
UPDATE risk_status_snapshots s SET
  status_raw = r.status,
  risk_type_raw = r.risk_type,
  risk_triggers_raw = r.risk_triggers,
  flag_triggers_raw = r.flag_triggers,
  flag_count_raw = r.flag_count
FROM get_dashboard_with_risk_raw() r
WHERE s.customer_id = r.customer_id
  AND s.snapshot_date = CURRENT_DATE;
"

echo "$(date): Risk snapshot completed for $(date +%Y-%m-%d)" >> /Users/bp/projects/workflows/risk-dashboard/snapshot.log
