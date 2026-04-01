#!/bin/bash
cd "$(dirname "$0")"
export $(grep -v '^#' .env | xargs)
/usr/bin/python3 pull_ghl_client_accounts.py >> ghl-accounts-pull.log 2>&1
tail -1000 ghl-accounts-pull.log > ghl-accounts-pull.log.tmp && mv ghl-accounts-pull.log.tmp ghl-accounts-pull.log
