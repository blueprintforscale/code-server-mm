#!/bin/bash
# Lead authenticity verifier — every 30 min (offset :15/:45)
# Fetches calls/forms from CallRail, classifies spam vs legitimate, uploads to Google Ads
# Runs via Claude Code CLI (subscription, no API charges)
# Schedule: 15,45 * * * *

export PATH="/Users/bp/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
unset CLAUDECODE
source /Users/bp/projects/cron/guard.sh lead-verifier 1200

LOGFILE="/Users/bp/projects/cron/logs/lead-verifier.log"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

echo "[$TIMESTAMP] Starting lead authenticity verifier" >> "$LOGFILE"

cd /Users/bp/projects

claude -p --dangerously-skip-permissions \
  "Run this pipeline. ALWAYS run ALL steps in order. The variable P=/Users/bp/projects/workflows/call-classifier/.venv/bin/python3 and S=/Users/bp/projects/workflows/call-classifier/classify_calls.py.

Step 1: Run \`\$P \$S fetch\` (fetches new calls from CallRail)
Step 2: Run \`\$P \$S fetch-forms\` (fetches new form submissions from CallRail)
Step 3: Run \`\$P \$S pending\` to get unclassified calls. If the JSON array is non-empty, classify each call as spam or legitimate based on the transcript (legitimate = mold remediation inquiries, quotes, appointments, follow-ups; spam = robocalls, solicitations, wrong numbers, hangups). Then run \`\$P \$S classify-batch --data '<JSON>'\` with [{\"id\": N, \"classification\": \"spam\"|\"legitimate\", \"reason\": \"...\"}].
Step 4: Run \`\$P \$S pending-forms\` to get unclassified forms. If non-empty, classify each (legitimate = real people inquiring about mold services; spam = gibberish names, fake emails, bot submissions). Then run \`\$P \$S classify-forms --data '<JSON>'\` with same format.
Step 5: Run \`\$P \$S upload\`
Step 6: Run \`\$P \$S summary\`

If all steps complete without errors, output ONLY: SUCCESS - no further action needed.

If any step returns an actual error, post a message to the Slack channel C09AZ1MCLN7 (client_notifications) using the Slack MCP tool with the error details. Start the Slack message with '🔴 Lead Verifier Failed' followed by which step failed and the error output." \
  >> "$LOGFILE" 2>&1

EXIT_CODE=$?
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
echo "[$TIMESTAMP] Finished with exit code $EXIT_CODE" >> "$LOGFILE"
echo "---" >> "$LOGFILE"
