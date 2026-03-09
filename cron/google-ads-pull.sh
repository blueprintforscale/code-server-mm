#!/bin/bash
# Google Ads quick pull — every 30 min
# Runs ETL pipeline via Claude Code CLI (subscription, no API charges)
# Schedule: */30 * * * *

export PATH="/Users/bp/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
unset CLAUDECODE

LOGFILE="/Users/bp/projects/cron/logs/google-ads-pull.log"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

echo "[$TIMESTAMP] Starting Google Ads quick pull" >> "$LOGFILE"

cd /Users/bp/projects

claude -p --dangerously-skip-permissions \
  "Run the Google Ads ETL pipeline: /Users/bp/data-pipeline/.venv/bin/python3 /Users/bp/data-pipeline/scripts/pull_google_ads.py --days 1 --skip-search-terms

If the pipeline succeeds with no errors, output ONLY: SUCCESS - no further action needed.

If there are any errors or failures, post a message to the Slack #general channel using the Slack MCP tool summarizing what went wrong, including the error output. Start the Slack message with '🔴 Google Ads Quick Pull Failed' followed by the error details." \
  >> "$LOGFILE" 2>&1

EXIT_CODE=$?
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
echo "[$TIMESTAMP] Finished with exit code $EXIT_CODE" >> "$LOGFILE"
echo "---" >> "$LOGFILE"
