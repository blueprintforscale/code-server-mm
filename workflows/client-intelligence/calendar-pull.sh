#!/bin/bash
cd "$(dirname "$0")"
export $(grep -v '^#' .env | xargs)
/usr/bin/python3 pull_calendar_data.py >> calendar-pull.log 2>&1
tail -1000 calendar-pull.log > calendar-pull.log.tmp && mv calendar-pull.log.tmp calendar-pull.log
