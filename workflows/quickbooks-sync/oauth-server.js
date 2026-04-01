/**
 * QuickBooks OAuth2 Authorization Server
 *
 * Run this temporarily to get the initial OAuth tokens.
 * After authorization, tokens are saved to the database.
 *
 * Usage: node oauth-server.js
 * Then visit: http://localhost:3457/connect
 */

const http = require('http');
const https = require('https');
const { URL } = require('url');
const { Pool } = require('pg');

const CLIENT_ID = 'ABwh359LA5XLdzGjzm75AEc8khpi7bIrglDk19LNH8dDAwb9Fd';
const CLIENT_SECRET = '3axIgh66ScbYFGG8k3iJ4STbRIIOIrK80ZOsQJGY';
const REDIRECT_URI = 'http://localhost:3457/callback';
const SCOPES = 'com.intuit.quickbooks.accounting';

// Intuit OAuth endpoints
const AUTH_URL = 'https://appcenter.intuit.com/connect/oauth2';
const TOKEN_URL = 'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer';

const pool = new Pool({ connectionString: 'postgresql://blueprint@localhost/blueprint', ssl: false });

function makeRequest(url, options, postData) {
  return new Promise((resolve, reject) => {
    const req = https.request(url, options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, data: JSON.parse(data) }); }
        catch (e) { resolve({ status: res.statusCode, data }); }
      });
    });
    req.on('error', reject);
    if (postData) req.write(postData);
    req.end();
  });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost:3457');

  if (url.pathname === '/connect') {
    // Step 1: Redirect to Intuit authorization
    const authUrl = `${AUTH_URL}?client_id=${CLIENT_ID}&redirect_uri=${encodeURIComponent(REDIRECT_URI)}&response_type=code&scope=${encodeURIComponent(SCOPES)}&state=blueprint`;
    res.writeHead(302, { Location: authUrl });
    res.end();
    console.log('Redirecting to Intuit authorization...');
    return;
  }

  if (url.pathname === '/callback') {
    // Step 2: Exchange auth code for tokens
    const code = url.searchParams.get('code');
    const realmId = url.searchParams.get('realmId');
    const error = url.searchParams.get('error');

    if (error) {
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(`<h2>Authorization Failed</h2><p>${error}</p>`);
      return;
    }

    console.log(`Got auth code, realmId: ${realmId}`);

    // Exchange code for tokens
    const postData = new URLSearchParams({
      grant_type: 'authorization_code',
      code: code,
      redirect_uri: REDIRECT_URI,
    }).toString();

    const basicAuth = Buffer.from(`${CLIENT_ID}:${CLIENT_SECRET}`).toString('base64');

    try {
      const result = await makeRequest(TOKEN_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Authorization': `Basic ${basicAuth}`,
          'Accept': 'application/json',
        },
      }, postData);

      if (result.status === 200 && result.data.access_token) {
        const { access_token, refresh_token, expires_in } = result.data;
        const expires_at = new Date(Date.now() + expires_in * 1000);

        // Save to database
        await pool.query(`
          CREATE TABLE IF NOT EXISTS quickbooks_tokens (
            id SERIAL PRIMARY KEY,
            realm_id TEXT NOT NULL,
            access_token TEXT NOT NULL,
            refresh_token TEXT NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL,
            client_id TEXT,
            updated_at TIMESTAMPTZ DEFAULT NOW()
          )
        `);

        await pool.query(`
          INSERT INTO quickbooks_tokens (realm_id, access_token, refresh_token, expires_at, client_id)
          VALUES ($1, $2, $3, $4, $5)
          ON CONFLICT (realm_id) DO UPDATE SET
            access_token = EXCLUDED.access_token,
            refresh_token = EXCLUDED.refresh_token,
            expires_at = EXCLUDED.expires_at,
            updated_at = NOW()
        `, [realmId, access_token, refresh_token, expires_at, CLIENT_ID]).catch(async () => {
          // If no unique constraint yet, just insert
          await pool.query(`DELETE FROM quickbooks_tokens WHERE realm_id = $1`, [realmId]);
          await pool.query(`
            INSERT INTO quickbooks_tokens (realm_id, access_token, refresh_token, expires_at, client_id)
            VALUES ($1, $2, $3, $4, $5)
          `, [realmId, access_token, refresh_token, expires_at, CLIENT_ID]);
        });

        console.log(`\nTokens saved successfully!`);
        console.log(`  Realm ID: ${realmId}`);
        console.log(`  Expires: ${expires_at.toISOString()}`);
        console.log(`\nYou can close this server now (Ctrl+C).`);

        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end(`
          <html><body style="font-family:system-ui;padding:3rem;text-align:center">
            <h2 style="color:#16a34a">QuickBooks Connected!</h2>
            <p>Realm ID: <strong>${realmId}</strong></p>
            <p>Tokens saved to database. You can close this page.</p>
          </body></html>
        `);
      } else {
        console.error('Token exchange failed:', result);
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end(`<h2>Token Exchange Failed</h2><pre>${JSON.stringify(result.data, null, 2)}</pre>`);
      }
    } catch (err) {
      console.error('Error:', err);
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('Error: ' + err.message);
    }
    return;
  }

  res.writeHead(200, { 'Content-Type': 'text/html' });
  res.end(`
    <html><body style="font-family:system-ui;padding:3rem;text-align:center">
      <h2>QuickBooks OAuth Setup</h2>
      <p><a href="/connect" style="padding:1rem 2rem;background:#000;color:#fff;border-radius:8px;text-decoration:none;font-weight:600">Connect QuickBooks</a></p>
    </body></html>
  `);
});

server.listen(3457, () => {
  console.log('\nQuickBooks OAuth server running at http://localhost:3457');
  console.log('Visit http://localhost:3457/connect to authorize\n');
});
