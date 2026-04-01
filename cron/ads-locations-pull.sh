#!/bin/bash
# Google Ads Location Targeting pull — weekly (Sunday 2am)
# Pulls location criteria for active US clients, auto-geocodes new locations.

export PATH="/Users/bp/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

LOGFILE="/Users/bp/projects/cron/logs/ads-locations-pull.log"
TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")
source /Users/bp/projects/cron/guard.sh ads-locations-pull 900

echo "[$TIMESTAMP] Starting ads location targeting pull" >> "$LOGFILE"

/Users/bp/data-pipeline/.venv/bin/python3 \
  /Users/bp/projects/workflows/client-intelligence/pull_ads_locations.py \
  >> "$LOGFILE" 2>&1

EXIT_CODE=$?
TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")
echo "[$TIMESTAMP] Finished with exit code $EXIT_CODE" >> "$LOGFILE"
echo "---" >> "$LOGFILE"
