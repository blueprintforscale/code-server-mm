#!/usr/bin/env node
/**
 * Blueprint Bulletin — Weekly changelog posted to Slack by Blueprint Brain
 * Runs Sunday evenings. Gathers git commits, sends to Claude, posts digest.
 */

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });
const { execSync } = require('child_process');
const https = require('https');

// ── Config ───────────────────────────────────────────────────

const SLACK_BOT_TOKEN = process.env.SLACK_BOT_TOKEN;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const CHANNEL = process.env.BULLETIN_CHANNEL || process.argv[2] || '';
const DAYS = parseInt(process.env.BULLETIN_DAYS || '7', 10);
const DRY_RUN = process.argv.includes('--dry-run');

const REPOS = [
  { name: 'BlueprintOS (Dashboard + Client App)', path: '/Users/bp/blueprintos' },
  { name: 'Blueprint Systems (Backend + ETL + Pipelines)', path: '/Users/bp/projects' },
];

if (!DRY_RUN && (!SLACK_BOT_TOKEN || !CHANNEL)) {
  console.error('Missing SLACK_BOT_TOKEN or BULLETIN_CHANNEL. Use --dry-run to preview.');
  process.exit(1);
}
if (!ANTHROPIC_API_KEY) {
  console.error('Missing ANTHROPIC_API_KEY');
  process.exit(1);
}

// ── Git log collection ───────────────────────────────────────

function getCommits(repoPath, repoName, days) {
  try {
    const raw = execSync(
      `git -C "${repoPath}" log --since="${days} days ago" --all --no-merges --format="%h|%an|%ad|%s" --date=short 2>/dev/null`,
      { encoding: 'utf8', timeout: 15000 }
    ).trim();
    if (!raw) return [];
    return raw.split('\n').map(line => {
      const [hash, author, date, ...msgParts] = line.split('|');
      return { repo: repoName, hash, author, date, message: msgParts.join('|') };
    });
  } catch (e) {
    console.error(`Warning: Could not read ${repoName} at ${repoPath}: ${e.message}`);
    return [];
  }
}

// ── Claude API call ──────────────────────────────────────────

function callClaude(commits) {
  const commitText = commits.map(c =>
    `[${c.repo}] ${c.date} ${c.hash} ${c.message}`
  ).join('\n');

  const today = new Date();
  const weekAgo = new Date(today - DAYS * 86400000);
  const fmt = { month: 'short', day: 'numeric' };
  const weekLabel = `${weekAgo.toLocaleDateString('en-US', fmt)} – ${today.toLocaleDateString('en-US', { ...fmt, year: 'numeric' })}`;

  const prompt = `You are writing the "Blueprint Bulletin" — a weekly Slack changelog for Blueprint for Scale, a Google Ads agency for mold remediation companies.

Below are the git commits from the past ${DAYS} days across our repos. Categorize and summarize them into a clean, scannable Slack message.

FORMAT RULES:
- Use Slack mrkdwn (not markdown): *bold*, _italic_, \`code\`, ~strikethrough~
- Use emoji sparingly but effectively (🚀 for launches, 🐛 for bug fixes, 📊 for dashboards/metrics, 🔧 for infrastructure, 🛡️ for spam/quality)
- Group into these sections (skip any section with nothing in it):
  *🚀 Launches* — new features or pages that are now live
  *📊 Improvements* — enhancements to existing features  
  *🐛 Bug Fixes* — things that were broken and are now fixed
  *🔧 Infrastructure* — backend, ETL, database, pipeline changes
  *🛡️ Spam & Lead Quality* — anything about spam detection, bot filtering, lead classification
- Each item MUST start with a bullet point (use • character). One bullet per line, plain language, no commit hashes. Example format:
  • Multi-business rollup dashboards now live
  • Campaign isolation on trends chart
- Combine related commits into single bullets (e.g. 5 commits tuning spam detection = 1 bullet)
- Write for account managers and agency leadership, not engineers — explain what changed in terms of what they'll see or what it means for clients
- Do NOT include a contributors section
- Keep the whole message under 2000 characters
- Start with: 📋 *Blueprint Bulletin* — ${weekLabel}
- Add a one-line subtitle under the title in italics summarizing the week's theme
- End with: ───────────────────── then on the next line: _Powered by Blueprint Bulletin_ :bb: then on the next line add this exact blurb in italics: _Blueprint Bulletin is an automated weekly digest of all changes made to Blueprint systems — dashboards, pipelines, integrations, and tools running on the Mac Mini server. Compiled by BB every Sunday evening._


COMMITS:
${commitText}`;

  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 2000,
      messages: [{ role: 'user', content: prompt }],
    });

    const req = https.request({
      hostname: 'api.anthropic.com',
      path: '/v1/messages',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
      },
    }, res => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          if (parsed.content && parsed.content[0]) {
            resolve(parsed.content[0].text);
          } else {
            reject(new Error(`Claude API error: ${data.slice(0, 500)}`));
          }
        } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

// ── Slack post ───────────────────────────────────────────────

function postToSlack(channel, text) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      channel,
      text,
      unfurl_links: false,
      unfurl_media: false,
    });

    const req = https.request({
      hostname: 'slack.com',
      path: '/api/chat.postMessage',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': `Bearer ${SLACK_BOT_TOKEN}`,
      },
    }, res => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          if (parsed.ok) {
            resolve(parsed);
          } else {
            reject(new Error(`Slack error: ${parsed.error}`));
          }
        } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

// ── Main ─────────────────────────────────────────────────────

async function main() {
  // Pull latest commits from remote before scanning
try { execSync("git -C /Users/bp/blueprintos pull --ff-only 2>/dev/null", { timeout: 30000 }); } catch(e) {}
try { execSync("git -C /Users/bp/projects pull --ff-only 2>/dev/null", { timeout: 30000 }); } catch(e) {}

console.log(`Blueprint Bulletin — gathering ${DAYS} days of commits...`);

  const allCommits = [];
  for (const repo of REPOS) {
    const commits = getCommits(repo.path, repo.name, DAYS);
    console.log(`  ${repo.name}: ${commits.length} commits`);
    allCommits.push(...commits);
  }

  if (allCommits.length === 0) {
    console.log('No commits found in the past week. Skipping bulletin.');
    return;
  }

  allCommits.sort((a, b) => b.date.localeCompare(a.date));
  console.log(`Total: ${allCommits.length} commits. Sending to Claude for summary...`);
  const bulletin = await callClaude(allCommits);

  if (DRY_RUN) {
    console.log('\n--- DRY RUN (would post to Slack) ---\n');
    console.log(bulletin);
    return;
  }

  console.log('Posting to Slack...');
  await postToSlack(CHANNEL, bulletin);
  console.log('Blueprint Bulletin posted successfully!');
}

main().catch(err => {
  console.error('Bulletin failed:', err.message);
  process.exit(1);
});
