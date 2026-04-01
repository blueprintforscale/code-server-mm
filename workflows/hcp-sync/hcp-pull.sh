#!/bin/bash
source /Users/bp/projects/cron/guard.sh hcp-pull 1200
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$SCRIPT_DIR/hcp-pull.log"
PSQL="/opt/homebrew/opt/postgresql@17/bin/psql -U blueprint blueprint"
echo "=== HCP Pull $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$LOG_FILE"
/usr/bin/python3 "$SCRIPT_DIR/pull_hcp_data.py" >> "$LOG_FILE" 2>&1
echo "[MV] Refreshing mv_lead_revenue + mv_funnel_leads..." >> "$LOG_FILE"
$PSQL -c "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_lead_revenue" >> "$LOG_FILE" 2>&1
$PSQL -c "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_funnel_leads" >> "$LOG_FILE" 2>&1
echo "=== Done $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"
tail -2000 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
