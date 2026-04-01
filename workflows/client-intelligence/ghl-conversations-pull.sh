#!/bin/bash
cd "$(dirname "$0")"
export $(grep -v '^#' .env | xargs)
/usr/bin/python3 pull_ghl_conversations.py >> ghl-conversations-pull.log 2>&1
tail -1000 ghl-conversations-pull.log > ghl-conversations-pull.log.tmp && mv ghl-conversations-pull.log.tmp ghl-conversations-pull.log
