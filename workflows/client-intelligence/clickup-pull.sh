#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$SCRIPT_DIR/clickup-pull.log"

echo "=== ClickUp Pull $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$LOG_FILE"

# Source environment variables
if [ -f "$SCRIPT_DIR/.env" ]; then
    export $(grep -v '^#' "$SCRIPT_DIR/.env" | xargs)
fi

/usr/bin/python3 "$SCRIPT_DIR/pull_clickup_data.py" >> "$LOG_FILE" 2>&1
echo "=== Done $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$LOG_FILE"

# Rotate logs (keep last 2000 lines)
tail -2000 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
