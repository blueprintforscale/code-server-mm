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
  - **Cron jobs are disabled** in `~/.openclaw/cron/jobs.json` — migrated to launchd
- **Data Pipeline**: `/Users/bp/data-pipeline/` (separate from this repo)
  - `pull_google_ads.py` — ETL from Google Ads to PostgreSQL
- **GitHub**: `blueprintforscale/code-server-mm`, authenticated via `gh` CLI
- **Runtime**: Node.js 25.x (Homebrew), Python 3.12 (Homebrew)

## MCP Servers

Configured in `/Users/bp/projects/.mcp.json` (gitignored — contains secrets):

- **Slack** (`slack-mcp-server`): Posts error alerts to Slack #client_notifications (C09AZ1MCLN7). Uses xoxp token.
- **Google Ads** (`google-ads-mcp` v0.6.2): Read-only GAQL queries against Google Ads API.
  - Venv: `.mcp-servers/google_ads_mcp/.venv/`
  - Credentials: `.mcp-servers/google_ads_mcp/google-ads.yaml` (OAuth2 + MCC login_customer_id)
  - MCC ID: `2985235474` (Pure Maintenance Growth Consulting)

## Project Structure

```
/Users/bp/projects/
├── CLAUDE.md                    # This file — project context for Claude Code
├── .mcp.json                    # MCP server config (gitignored, has secrets)
├── .mcp-servers/                # MCP server venvs and credentials (gitignored)
│   └── google_ads_mcp/
│       ├── .venv/               # Python 3.12 venv with google-ads-mcp
│       └── google-ads.yaml      # Google Ads OAuth2 credentials
├── apps/                        # Web apps, APIs, dashboards
├── workflows/                   # Automation scripts and data pipelines
│   └── call-classifier/         # Lead qualifier pipeline (see below)
├── cron/                        # Cron wrapper scripts (invoke claude -p)
│   ├── google-ads-pull.sh       # Every 30 min (:00/:30)
│   ├── google-ads-full-pull.sh  # Daily 3am PST (6am ET)
│   ├── lead-verifier.sh         # Every 30 min (:15/:45)
│   └── logs/                    # Cron output logs (gitignored)
├── lib/                         # Shared utilities and database helpers
└── scripts/                     # One-off and maintenance scripts
```

## Cronjobs (launchd)

All cron jobs run via **launchd** (not system crontab, not OpenClaw). Each wrapper script invokes `claude -p --dangerously-skip-permissions` so jobs run through the Anthropic subscription (no API charges). Scripts must `unset CLAUDECODE` to avoid nested-session errors.

Plist files in `~/Library/LaunchAgents/`:

| Job | Plist | Schedule | What it does |
|-----|-------|----------|-------------|
| Google Ads quick pull | `com.blueprint.google-ads-pull.plist` | :00, :30 | Runs data-pipeline ETL (skip search terms) |
| Google Ads full pull | `com.blueprint.google-ads-full-pull.plist` | 3:00 AM PST | Runs data-pipeline ETL (with search terms) |
| Lead verifier | `com.blueprint.lead-verifier.plist` | :15, :45 | 6-step pipeline: fetch→classify→upload |

**Error reporting**: On failure, `claude -p` posts to Slack #client_notifications (C09AZ1MCLN7) via Slack MCP. On success, no message (silent success).

**Logs**: `cron/logs/*.log` (script-level) and `cron/logs/*.launchd.log` (launchd stdout/stderr).

**Managing jobs**:
```bash
launchctl list | grep com.blueprint          # Check status
launchctl unload ~/Library/LaunchAgents/...  # Stop a job
launchctl load ~/Library/LaunchAgents/...    # Start a job
```

## Lead Qualifier Pipeline

Located at `workflows/call-classifier/`. Python 3.12 venv.

**What it does**:
1. Fetches **ALL calls** from CallRail (every source, every status) into the `calls` table
2. Fetches Google Ads **form submissions** into the `form_submissions` table
3. Classifies **all calls and forms** as spam or legitimate (Claude does the classification)
4. Uploads legitimate leads to Google Ads as **Enhanced Conversions** (via GCLID or hashed phone/email) — calls to "Qualified Call [AI]", forms to "Qualified Form [AI]". Only uploads leads from the last **90 days** (Google's click-through conversion window).

**Script**: `classify_calls.py` with subcommands: `fetch`, `fetch-forms`, `pending`, `pending-forms`, `classify-batch`, `classify-forms`, `upload`, `summary`, `log-run`

**Active clients**: 31 clients with `callrail_company_id` set in `clients` table. All actively spending Google Ads accounts have two conversion actions: "Qualified Call [AI]" (for calls) and "Qualified Form [AI]" (for forms), both as secondary conversions.

**Multi-account CallRail support**: Most clients use the default API key (from `.env`) and account ID (`465371377`). Clients on separate CallRail accounts have per-client credentials stored in `callrail_api_key` and `callrail_account_id` columns:
- Mold Cure (`1338532896`) — separate CallRail account

**Adding a new client**:
1. Set `callrail_company_id` in the `clients` table (find via CallRail API `/companies.json`)
2. If on a different CallRail account, also set `callrail_account_id` and `callrail_api_key`
3. Create two conversion actions in their Google Ads account (type: `UPLOAD_CLICKS`, category: `QUALIFIED_LEAD`): "Qualified Call [AI]" and "Qualified Form [AI]", both as secondary
4. The next cron run will automatically start fetching and classifying

**Key database tables**:
- `calls` — all CallRail calls (all sources). Columns include: `source`, `medium`, `duration`, `transcript`, `first_call`, `callrail_status` (answered/missed/abandoned), `call_type`, `classification`, `uploaded_to_gads`
- `form_submissions` — all CallRail form submissions (all sources)
- `call_pipeline_log` — run history for the pipeline
- `clients` — client config with `callrail_company_id`, `callrail_account_id` (optional), `callrail_api_key` (optional), and `conversion_value`

**Credentials**: Google Ads yaml at `.mcp-servers/google_ads_mcp/google-ads.yaml`. Default CallRail API key in `.env`. Per-client CallRail keys in `clients` table.

## Conventions

- **Language**: Use TypeScript (Node.js) for apps and APIs. Use Python for data scripts and quick automations where it makes more sense.
- **Database access**: Always use parameterized queries. Never interpolate user input into SQL.
- **Secrets**: Store in environment variables or `.env` files. Never commit secrets. `.env`, `.mcp.json`, and `google-ads.yaml` are in `.gitignore`.
- **Cronjobs**: Define cron scripts in `cron/`. Document the schedule in a comment at the top of each script. Register as launchd agents.
- **Error handling**: Cronjobs and workflows must log errors and report to Slack. Never fail silently.
- **Dependencies**: Use `package.json` at the root for Node.js deps. Use `requirements.txt` per workflow for Python deps.
- **Python venvs**: Use Python 3.12 (`/opt/homebrew/bin/python3.12`). Each workflow has its own `.venv/`.


## Critical: Do Not Modify These Sections

**BlueprintOS API (`apps/blueprintos-api/index.js`):**

- **Do NOT re-enable `get_dashboard_metrics()` override** in the funnel endpoint (line ~210). It is intentionally disabled (`if (false && ...)`). The funnel endpoint (`getHcpFunnel`) is now the single source of truth for lead counts. `get_dashboard_metrics()` does not handle bot detection, repeat caller filtering, attribution overrides, or the reactivation protocol. Re-enabling it causes funnel/drawer count mismatches.

- **Do NOT remove the lead-spreadsheet post-filter** (the `quality_phones` section near the end of the `/lead-spreadsheet` handler). This ensures the drawer count matches the funnel count exactly. It uses `getHcpFunnel`'s `quality_phones` array as the authoritative phone list, with a fallback query for missing phones.

- **Do NOT remove the `quality_phones` subquery** from `getHcpFunnel`'s final SELECT. The lead-spreadsheet post-filter depends on it.

These were added on 2026-04-08 after extensive data accuracy work. See `workflows/RULES.md` (Sections 15, 21, 22) and the guard comments in the code for full context.
## Database Guidelines

- The `blueprint` database holds client data and internal app data.
- Use migrations for schema changes (store in `lib/migrations/`).
- Always back up before destructive schema changes.
- Use connection pooling for apps that maintain persistent connections.

## Deployment

This is a single-server setup. There is no CI/CD pipeline — Claude Code deploys directly. Apps run as persistent processes managed via `pm2` or similar. The server is always on.

