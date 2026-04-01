#!/bin/bash
# Google Ads full pull (with search terms) — daily 6am ET
# Runs ETL pipeline via Claude Code CLI (subscription, no API charges)
# Schedule: 0 3 * * * (3am PST = 6am ET)

export PATH="/Users/bp/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
unset CLAUDECODE
source /Users/bp/projects/cron/guard.sh google-ads-full-pull 2700

LOGFILE="/Users/bp/projects/cron/logs/google-ads-full-pull.log"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

echo "[$TIMESTAMP] Starting Google Ads full pull (with search terms)" >> "$LOGFILE"

cd /Users/bp/projects

claude -p --dangerously-skip-permissions \
  "Run the full Google Ads ETL pipeline including search terms: /Users/bp/data-pipeline/.venv/bin/python3 /Users/bp/data-pipeline/scripts/pull_google_ads.py --days 7

If the pipeline succeeds with no errors, output ONLY: SUCCESS - no further action needed.

If there are any errors or failures, post a message to the Slack channel C09AZ1MCLN7 (client_notifications) using the Slack MCP tool summarizing what went wrong, including the error output. Start the Slack message with '🔴 Google Ads Full Pull Failed' followed by the error details." \
  >> "$LOGFILE" 2>&1

EXIT_CODE=$?
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
echo "[$TIMESTAMP] Finished with exit code $EXIT_CODE" >> "$LOGFILE"
echo "---" >> "$LOGFILE"
