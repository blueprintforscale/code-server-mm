#!/bin/bash
# GCLID → Campaign map pull — daily at 3:30am PST (after google-ads-full-pull)
# Pulls click_view data to map GCLIDs to campaigns for all clients
# Schedule: daily 3:30am PST

export PATH="/Users/bp/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

LOGFILE="/Users/bp/projects/cron/logs/gclid-campaign-pull.log"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
source /Users/bp/projects/cron/guard.sh gclid-campaign-pull 900

echo "[$TIMESTAMP] Starting GCLID campaign map pull" >> "$LOGFILE"

/Users/bp/data-pipeline/.venv/bin/python3 /Users/bp/data-pipeline/scripts/pull_gclid_campaigns.py --days 7 >> "$LOGFILE" 2>&1

EXIT_CODE=$?
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
echo "[$TIMESTAMP] Finished with exit code $EXIT_CODE" >> "$LOGFILE"
echo "---" >> "$LOGFILE"
