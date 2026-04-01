# Blueprint for Scale — Project Instructions

## Business Rules (CRITICAL)
Before building or modifying ANY dashboard, view, pipeline, or data pull, read the rules files from the **Mac Mini** (source of truth):

```bash
ssh mac-mini "cat ~/projects/workflows/RULES.md"
ssh mac-mini "cat ~/projects/workflows/RISK_THRESHOLDS.md"
```

- **`RULES.md`** — All business logic: ROAS calculations, classification rules, matching logic, funnel counting, LSA exclusion, VA review actions, and 30 total rules.
- **`RISK_THRESHOLDS.md`** — Risk/flag scoring thresholds (CPL limits, lead volume triggers, etc.). Only needed when working on the risk dashboard.

Local copies exist at `~/Documents/CallrailAntigravity/` but may be stale. **Always read from Mac Mini.** When updating rules, update the Mac Mini version first, then sync local:
```bash
scp mac-mini:~/projects/workflows/RULES.md ~/Documents/CallrailAntigravity/RULES.md
scp mac-mini:~/projects/workflows/RISK_THRESHOLDS.md ~/Documents/CallrailAntigravity/RISK_THRESHOLDS.md
```

Do NOT re-derive rules from scratch. If a rule isn't in RULES.md, ask before assuming.

## Infrastructure

### Mac Mini Server
- SSH: `ssh mac-mini` (user: bp, via Tailscale at 100.98.109.121)
- PostgreSQL 17: `/opt/homebrew/opt/postgresql@17/bin/psql -U blueprint blueprint`
- Node.js: `/opt/homebrew/bin/node`

### Key Directories
- Local project: `~/Documents/CallrailAntigravity/`
- Mac Mini workflows: `~/projects/workflows/` (hcp-sync, jobber-sync, hcp-review-app, risk-dashboard, ghl-sync)
- Mac Mini CallRail pipeline: `~/projects/workflows/call-classifier/`

### Deploying
- `scp local_file mac-mini:~/remote/path/`
- HTML files to review app: no restart needed (static)
- server.js changes: `launchctl unload/load ~/Library/LaunchAgents/com.blueprint.hcp-review-app.plist`

## Conventions
- Client `customer_id` = Google Ads customer ID (BIGINT, no dashes)
- Phone normalization: `normalize_phone()` — strip non-digits, take last 10
- Amounts in HCP/Jobber are in cents — divide by 100 for dollars
- HCP IDs: customers=`cus_`, estimates=`csr_`, jobs=`job_`, invoices=`invoice_`

## Client Lookup
```sql
SELECT customer_id, name, field_management_software, hcp_api_key,
       callrail_company_id, start_date, budget, status, ads_manager
FROM clients WHERE name ILIKE '%search_term%';
```
