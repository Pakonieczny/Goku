// A tab running an old build reloads itself once it is idle (never while busy), and never loops.
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
  const pg = await c.newPage();
  await pg.goto(URL); await sleep(2500);
  const out = await pg.evaluate(async () => {
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));
    window.__reloads = 0; window.__reloadPage = () => { window.__reloads++; };
    const r = {};
    window.APP_BUILD_URL = 'data:application/json,' + encodeURIComponent(JSON.stringify({ build: window.APP_BUILD }));
    r.sameBuild = await window.__buildCheckNow();                                   // up to date: nothing happens
    window.APP_BUILD_URL = 'data:application/json,' + encodeURIComponent(JSON.stringify({ build: 'NEWER' }));
    window.isProcessingSet = true;                                                    // busy publishing a listing
    r.busy = await window.__buildCheckNow();
    await sleep(6500);
    r.reloadsWhileBusy = window.__reloads;
    window.isProcessingSet = false;                                                   // idle now
    await sleep(5500);
    r.reloadsWhenIdle = window.__reloads; r.staleFlag = window.__staleBuild;
    return r;
  });
  console.log(JSON.stringify(out));
  await browser.close();
})();
