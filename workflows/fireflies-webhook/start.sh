#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ -f "$SCRIPT_DIR/../client-intelligence/.env" ]; then
    export $(grep -v '^#' "$SCRIPT_DIR/../client-intelligence/.env" | xargs)
fi

export PORT=3120
export PATH=/opt/homebrew/bin:$PATH

exec /opt/homebrew/bin/node "$SCRIPT_DIR/server.js"
