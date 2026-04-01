#!/bin/bash
cd /Users/bp/projects/workflows/sheet-sync
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

echo ""
echo "═══ Sheet Sync — $(date) ═══"
python3 pull_sheet_data.py 2>&1
echo "═══ Done — $(date) ═══"
