#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$SCRIPT_DIR/fireflies-pull.log"

echo "=== Fireflies Pull $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$LOG_FILE"

if [ -f "$SCRIPT_DIR/.env" ]; then
    export $(grep -v '^#' "$SCRIPT_DIR/.env" | xargs)
fi

/usr/bin/python3 "$SCRIPT_DIR/pull_fireflies_data.py" >> "$LOG_FILE" 2>&1

echo "--- Posting summaries $(date '+%Y-%m-%d %H:%M:%S') ---" >> "$LOG_FILE"
/usr/bin/python3 "$SCRIPT_DIR/post_call_summaries.py" --days 3 --limit 20 >> "$LOG_FILE" 2>&1
echo "=== Done $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$LOG_FILE"

tail -2000 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
