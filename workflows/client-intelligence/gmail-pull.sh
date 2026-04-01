#!/bin/bash
cd "$(dirname "$0")"
export $(grep -v '^#' .env | xargs)
/usr/bin/python3 pull_gmail_data.py >> gmail-pull.log 2>&1
tail -1000 gmail-pull.log > gmail-pull.log.tmp && mv gmail-pull.log.tmp gmail-pull.log
