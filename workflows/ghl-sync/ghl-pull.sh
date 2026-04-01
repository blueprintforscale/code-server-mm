#!/bin/bash
cd /Users/bp/projects/workflows/ghl-sync
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
echo ""
echo "═══ GHL Pull — $(date) ═══"
python3 pull_ghl_data.py 2>&1
echo "═══ GHL Estimates — $(date) ═══"
python3 pull_ghl_estimates.py 2>&1
echo "═══ GHL Appointments — $(date) ═══"
python3 pull_ghl_appointments.py 2>&1
echo "═══ GHL Transactions — $(date) ═══"
python3 pull_ghl_transactions.py 2>&1
echo "═══ Done — $(date) ═══"
