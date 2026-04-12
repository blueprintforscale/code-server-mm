#!/bin/bash
cd "$(dirname "$0")"
export $(grep -v '^#' .env | xargs)
/usr/bin/python3 pull_ghl_conversations.py >> ghl-conversations-pull.log 2>&1
tail -1000 ghl-conversations-pull.log > ghl-conversations-pull.log.tmp && mv ghl-conversations-pull.log.tmp ghl-conversations-pull.log

# Bridge meaningful GHL outbound calls into client_interactions for contact tracking
/opt/homebrew/opt/postgresql@17/bin/psql -U blueprint blueprint -c "
INSERT INTO client_interactions (customer_id, interaction_type, interaction_date, source, summary, logged_by, source_id)
SELECT cm.customer_id, 'call', cm.message_date, 'ghl_call',
  'GHL outbound call to ' || cm.contact_name || ' (' || ROUND(cm.duration/60.0, 0) || ' min)',
  'GHL', cm.source_id
FROM crm_messages cm
WHERE cm.channel = 'call' AND cm.direction = 'outbound' AND cm.duration > 60
  AND cm.customer_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM client_interactions ci
    WHERE ci.source_id = cm.source_id AND ci.source = 'ghl_call'
  )
" >> ghl-conversations-pull.log 2>&1
