#!/bin/bash
# guard.sh — Timeout + lock file wrapper for cron jobs
# Usage: source guard.sh <job-name> <timeout-seconds>
#   e.g.: source guard.sh google-ads-pull 1200
#
# Provides:
#   - LOCKFILE-based single-instance enforcement (skip if already running)
#   - Self-kill after TIMEOUT seconds (prevents zombie jobs)
#   - Cleanup on exit (removes lock, kills watchdog)

GUARD_JOB="$1"
GUARD_TIMEOUT="${2:-1200}"
GUARD_LOCKDIR="/tmp/cron-locks"
mkdir -p "$GUARD_LOCKDIR"
GUARD_LOCKFILE="$GUARD_LOCKDIR/${GUARD_JOB}.lock"

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
        echo "[$(date "+%Y-%m-%d %H:%M:%S")] TIMEOUT: $GUARD_JOB exceeded ${GUARD_TIMEOUT}s — killing PID $$ and children"
        # Kill the entire process group
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
