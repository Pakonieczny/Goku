// The Shopify status chip must say "paused" and when the queue resumes when the daily budget blocks the queue.
const P = require('child_process').execSync('readlink -f $(npm root -g)/playwright').toString().trim();
const { chromium } = require(P);
const fs = require('fs'), path = require('path');
const { Store } = require('./store.js');
const FAKE = fs.readFileSync(path.join(__dirname, 'fakefb.js'), 'utf8');
const URL = 'file://' + path.resolve(__dirname, '../../Index.html');
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
(async () => {
  const store = new Store();
  const browser = await chromium.launch({ args: ['--no-sandbox'] });
  const c = await browser.newContext();
  await c.exposeBinding('__fs', async (src, op, json) => JSON.stringify(store.op(op, JSON.parse(json))));
  await c.addInitScript(FAKE);
  await c.route(/^(?!file:)/, r => r.abort());
  const pg = await c.newPage({ viewport: { width: 1400, height: 900 } });
  await pg.goto(URL); await sleep(3500);
  const out = await pg.evaluate(async () => {
    document.body.classList.remove('lg-locked');
    const results = [];
    const run = async (body) => {
      window.fetch = async (u) => ({ ok: true, json: async () => body });
      await window.refreshShopifyStatus();
      const el = document.getElementById('shopifyQueueStatus');
      return { text: el.textContent, warn: el.classList.contains('warn') };
    };
    results.push(await run({ queued: 56, uploading: 0, failed: 0, done: 10, budget: { used: 928, ceiling: 950, remaining: 22, paused: true, resetsInMs: 5 * 3600000 + 12 * 60000, nextNeeds: 32 }, recent: [] }));
    results.push(await run({ queued: 3, uploading: 0, failed: 0, done: 10, budget: { used: 100, ceiling: 950, remaining: 850, paused: false, resetsInMs: 3600000 }, recent: [] }));
    return results;
  });
  console.log(JSON.stringify(out, null, 1));
  await browser.close();
})();
