#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$SCRIPT_DIR/callrail-forms.log"
echo "=== Form Pull $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$LOG_FILE"
/usr/bin/python3 "$SCRIPT_DIR/pull_callrail_forms.py" >> "$LOG_FILE" 2>&1
echo "=== Done $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"
tail -500 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
