#!/bin/bash
# guard.sh — Timeout + lock file wrapper for cron jobs
# Usage: source guard.sh <job-name> <timeout-seconds>
#   e.g.: source guard.sh google-ads-pull 1200
#
# Provides:
#   - LOCKFILE-based single-instance enforcement (skip if already running)
#   - Self-kill after TIMEOUT seconds (prevents zombie jobs)
#   - Slack alert on timeout
#   - Cleanup on exit (removes lock, kills watchdog)

GUARD_JOB="$1"
GUARD_TIMEOUT="${2:-1200}"
GUARD_LOCKDIR="/tmp/cron-locks"
mkdir -p "$GUARD_LOCKDIR"
GUARD_LOCKFILE="$GUARD_LOCKDIR/${GUARD_JOB}.lock"

# Slack alert function
GUARD_SLACK_TOKEN="xoxb-6594692085893-10476528834625-otlCrGN5kiu31kQYWDvrCAwC"
GUARD_SLACK_CHANNEL="C09AZ1MCLN7"
guard_slack() {
    curl -s -X POST https://slack.com/api/chat.postMessage         -H "Authorization: Bearer $GUARD_SLACK_TOKEN"         -H "Content-Type: application/json"         -d "{\"channel\": \"$GUARD_SLACK_CHANNEL\", \"text\": \"$1\"}" > /dev/null 2>&1
}

# --- Single-instance lock ---
if [ -f "$GUARD_LOCKFILE" ]; then
    GUARD_OLD_PID=$(cat "$GUARD_LOCKFILE" 2>/dev/null)
    if kill -0 "$GUARD_OLD_PID" 2>/dev/null; then
        echo "[$(date "+%Y-%m-%d %H:%M:%S")] SKIPPED: $GUARD_JOB already running (PID $GUARD_OLD_PID)"
        exit 0
    else
        echo "[$(date "+%Y-%m-%d %H:%M:%S")] Stale lock found (PID $GUARD_OLD_PID dead), removing"
        rm -f "$GUARD_LOCKFILE"
    fi
fi
echo $$ > "$GUARD_LOCKFILE"

# --- Timeout watchdog ---
(
    sleep "$GUARD_TIMEOUT"
    if kill -0 $$ 2>/dev/null; then
        TIMEOUT_MIN=$(( GUARD_TIMEOUT / 60 ))
        echo "[$(date "+%Y-%m-%d %H:%M:%S")] TIMEOUT: $GUARD_JOB exceeded ${GUARD_TIMEOUT}s — killing PID $$ and children"
        guard_slack ":rotating_light: $GUARD_JOB TIMEOUT after ${TIMEOUT_MIN}m — process killed. Check logs."
        pkill -P $$ 2>/dev/null
        sleep 2
        kill -9 $$ 2>/dev/null
    fi
) &
GUARD_WATCHDOG_PID=$!

# --- Cleanup on exit ---
guard_cleanup() {
    rm -f "$GUARD_LOCKFILE"
    kill "$GUARD_WATCHDOG_PID" 2>/dev/null
}
trap guard_cleanup EXIT
