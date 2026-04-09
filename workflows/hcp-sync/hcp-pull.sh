#!/bin/bash
source /Users/bp/projects/cron/guard.sh hcp-pull 7200
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$SCRIPT_DIR/hcp-pull.log"
PSQL="/opt/homebrew/opt/postgresql@17/bin/psql -U blueprint blueprint"
SLACK_TOKEN="${SLACK_BOT_TOKEN:-}"
SLACK_CHANNEL="C09AZ1MCLN7"

slack_alert() {
    local msg="$1"
    curl -s -X POST https://slack.com/api/chat.postMessage \
        -H "Authorization: Bearer $SLACK_TOKEN" \
        -H "Content-Type: application/json" \
        -d "{\"channel\": \"$SLACK_CHANNEL\", \"text\": \"$msg\"}" > /dev/null 2>&1
}

START_TIME=$(date +%s)
echo "=== HCP Pull $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$LOG_FILE"

# Run ETL
/usr/bin/python3 "$SCRIPT_DIR/pull_hcp_data.py" >> "$LOG_FILE" 2>&1
ETL_EXIT=$?

# Run inspection completion inference before view refresh
echo "[INFER] Running infer_inspection_completions()..." >> "$LOG_FILE"
$PSQL -c "SELECT infer_inspection_completions()" >> "$LOG_FILE" 2>&1

# Refresh materialized views
echo "[MV] Refreshing mv_lead_revenue + mv_funnel_leads..." >> "$LOG_FILE"
$PSQL -c "REFRESH MATERIALIZED VIEW mv_lead_revenue" >> "$LOG_FILE" 2>&1
MV1_EXIT=$?
$PSQL -c "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_funnel_leads" >> "$LOG_FILE" 2>&1
MV2_EXIT=$?

END_TIME=$(date +%s)
DURATION=$(( (END_TIME - START_TIME) / 60 ))

echo "=== Done $(date '+%Y-%m-%d %H:%M:%S') (${DURATION}m) ===" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

# Alert on errors
if [ $ETL_EXIT -ne 0 ]; then
    slack_alert ":rotating_light: HCP ETL failed (exit code $ETL_EXIT) after ${DURATION}m. Check hcp-pull.log."
elif [ $MV1_EXIT -ne 0 ] || [ $MV2_EXIT -ne 0 ]; then
    slack_alert ":warning: HCP ETL completed but materialized view refresh failed after ${DURATION}m. Check hcp-pull.log."
elif [ $DURATION -gt 180 ]; then
    slack_alert ":clock3: HCP ETL completed in ${DURATION}m (>3h, slower than expected). All clients synced."
fi

# Trim log
tail -2000 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
