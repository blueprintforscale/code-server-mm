#!/bin/bash
# Jobber ETL — pull customers, quotes, jobs, invoices for all Jobber clients
cd /Users/bp/projects/workflows/jobber-sync
source /Users/bp/projects/cron/guard.sh jobber-pull 1200
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
export JOBBER_CLIENT_ID="e46bcd1e-04a1-4770-bd86-88cf4abd9f35"
export JOBBER_CLIENT_SECRET="204ba95f7ec0f79a236112e4e475c1d22e8910689cd085573eade9d9009c2f62"

echo ""
echo "═══ Jobber Pull — $(date) ═══"
python3 pull_jobber_data.py 2>&1
echo "═══ Done — $(date) ═══"
