#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$SCRIPT_DIR/lsa-pull.log"
PYTHON="/Users/bp/projects/workflows/call-classifier/.venv/bin/python3"
echo "=== LSA Pull $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$LOG_FILE"
$PYTHON "$SCRIPT_DIR/pull_lsa_leads.py" >> "$LOG_FILE" 2>&1
echo "=== Done $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"
tail -1000 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
