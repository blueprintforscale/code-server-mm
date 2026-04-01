#!/bin/bash
source /Users/bp/projects/cron/guard.sh hcp-followups 1800
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$SCRIPT_DIR/hcp-followups.log"
source /Users/bp/projects/.env.secrets
echo "=== Follow-up Extract $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$LOG_FILE"
/usr/bin/python3 "$SCRIPT_DIR/extract_followups.py" >> "$LOG_FILE" 2>&1
echo "=== Done $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"
tail -1000 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
