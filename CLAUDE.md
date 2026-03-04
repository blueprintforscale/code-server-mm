# Blueprint Code Server

## What This Is

This is the central monorepo for Blueprint's always-on Mac Mini server (`Blueprints-Mac-mini.local`). It hosts all business apps, workflows, cronjobs, and internal/external tools. Claude Code is the primary builder and operator — it builds apps, manages infrastructure, and runs scheduled jobs.

## Infrastructure

- **Machine**: Mac Mini (Apple Silicon, arm64), macOS 15.5, user `bp`
- **Database**: PostgreSQL 17 (Homebrew), running as a launch agent
  - Host: `localhost`, default port `5432`
  - Main database: `blueprint` (owner: `blueprint`)
  - CLI: `psql -U blueprint blueprint`
- **OpenClaw**: v2026.2.15, gateway on port 18789 (launch agent, always-on)
  - Config: `~/.openclaw/openclaw.json`
  - Agents: main, martin (Huginn), josh, susie — with Telegram bindings
  - Manages cronjobs (not system crontab) via `~/.openclaw/cron/jobs.json`
- **Data Pipeline**: `/Users/bp/data-pipeline/` (separate from this repo)
  - `pull_google_ads.py` — ETL from Google Ads to PostgreSQL
  - Runs via OpenClaw cron: every 30min (quick pull) + daily 6am ET (full with search terms)
  - Errors reported to Telegram
- **GitHub**: `blueprintforscale` account, authenticated via `gh` CLI
- **Runtime**: Node.js (via npm), Python 3.9

## Project Structure

```
/Users/bp/projects/
├── CLAUDE.md          # This file — project context for Claude Code
├── apps/              # Web apps, APIs, dashboards
├── workflows/         # Automation scripts and data pipelines
├── cron/              # Cronjob scripts and schedule definitions
├── lib/               # Shared utilities and database helpers
└── scripts/           # One-off and maintenance scripts
```

## Conventions

- **Language**: Use TypeScript (Node.js) for apps and APIs. Use Python for data scripts and quick automations where it makes more sense.
- **Database access**: Always use parameterized queries. Never interpolate user input into SQL.
- **Secrets**: Store in environment variables or `.env` files. Never commit secrets. `.env` is in `.gitignore`.
- **Cronjobs**: Define cron scripts in `cron/`. Document the schedule in a comment at the top of each script. Register them in the system crontab.
- **Error handling**: Cronjobs and workflows must log errors. Never fail silently.
- **Dependencies**: Use `package.json` at the root for Node.js deps. Use `requirements.txt` for Python deps.

## Database Guidelines

- The `blueprint` database holds client data and internal app data.
- Use migrations for schema changes (store in `lib/migrations/`).
- Always back up before destructive schema changes.
- Use connection pooling for apps that maintain persistent connections.

## Cronjob Guidelines

- Cronjobs are managed by OpenClaw, not system crontab. Jobs defined in `~/.openclaw/cron/jobs.json`.
- Each cron script should be self-contained and idempotent where possible.
- Log output with timestamps. OpenClaw handles run tracking and error reporting.
- Use `#!/usr/bin/env node` or `#!/usr/bin/env python3` shebangs.
- Test scripts manually before registering them as OpenClaw cron jobs.

## Deployment

This is a single-server setup. There is no CI/CD pipeline — Claude Code deploys directly. Apps run as persistent processes managed via `pm2` or similar. The server is always on.
