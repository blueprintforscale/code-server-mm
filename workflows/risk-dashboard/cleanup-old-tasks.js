#!/usr/bin/env node
require('dotenv').config({ path: __dirname + '/.env' });
const https = require('https');
const { Pool } = require('pg');
const pool = new Pool({ user: 'blueprint', database: 'blueprint', host: 'localhost', port: 5432, ssl: false });
const CLICKUP_API_KEY = 'pk_50313409_WWF9IOF9PJP60BYRC3LSAME866GPBYDP';

function clickupRequest(method, path) {
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: 'api.clickup.com', path: '/api/v2' + path, method,
      headers: { 'Authorization': CLICKUP_API_KEY }
    }, res => {
      let body = ''; res.on('data', d => body += d);
      res.on('end', () => { try { resolve(JSON.parse(body)); } catch(e) { resolve({}); } });
    });
    req.on('error', reject); req.end();
  });
}

(async () => {
  const { rows } = await pool.query(
    "SELECT clickup_list_id FROM clients WHERE status = 'active' AND clickup_list_id IS NOT NULL GROUP BY clickup_list_id"
  );
  let deleted = 0;
  for (const r of rows) {
    const data = await clickupRequest('GET', '/list/' + r.clickup_list_id + '/task?include_closed=false');
    if (!data.tasks) continue;
    for (const t of data.tasks) {
      // Delete old-type touchpoint tasks (custom_item_id=0) that we created
      const isOurTask = t.name.startsWith('\u{1F4DE}') || t.name.startsWith('Weekly touchpoint');
      if (isOurTask && (t.custom_item_id === 0 || t.custom_item_id === 1001)) {
        await clickupRequest('DELETE', '/task/' + t.id);
        deleted++;
        console.log('  Deleted: ' + t.name.substring(0, 60));
        await new Promise(r => setTimeout(r, 200));
      }
    }
    await new Promise(r => setTimeout(r, 300));
  }
  console.log('\nTotal deleted: ' + deleted);
  await pool.end();
})();
