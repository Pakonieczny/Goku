// Restart of an interrupted set: must start straight from the ledger, without listing hundreds of storage folders first.
const P = require('child_process').execSync('readlink -f $(npm root -g)/playwright').toString().trim();
const { chromium } = require(P);
const fs = require('fs'), path = require('path');
const { Store } = require('./store.js');
const FAKE = fs.readFileSync(path.join(__dirname, 'fakefb.js'), 'utf8');
const URL = 'file://' + path.resolve(__dirname, '../../Index.html');
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
(async () => {
  const store = new Store();
  const NAME = 'Beady_Necklace_Set_527';
  store.put('ListingGen_Config/ledger~' + NAME, { kind: 'ledger', setName: NAME, category: 'Beady_Necklace', description: 'Easter Bunny', state: 'claimed', active: true, rev: 3, approvedAt: Date.now() - 3600000, approvedBy: 'x',
    claim: { owner: 'cOLD', page: 'pOLD', label: 'Old computer', token: 'tok1', leaseUntilMs: Date.now() - 5000, stage: 'preparing', startedMs: Date.now() - 600000 }, attempts: 1, meta: { listingId: '123456789' }, updatedMs: Date.now() - 600000, updatedBy: 'cOLD' }, false);
  const browser = await chromium.launch({ args: ['--no-sandbox'] });
  const c = await browser.newContext();
  await c.exposeBinding('__fs', async (src, op, json) => JSON.stringify(store.op(op, JSON.parse(json))));
  await c.addInitScript(FAKE);
  await c.addInitScript((NAME) => {
    const base = 'listing-generator-1/Generated_Listing_Sets/';
    const many = []; for (let i = 1; i <= 436; i++) many.push('Beady_Necklace_Set_' + i);
    window.__storage[base + 'Completed_Listing_Sets'] = { prefixes: many, items: [] };
    window.__storage[base + 'Approved_Listing_Sets'] = { prefixes: Array.from({ length: 3000 }, (_, i) => 'Done_Set_' + i), items: [] };
    window.__storage[base + 'Completed_Listing_Sets/' + NAME] = { prefixes: [], items: ['Slot_1.png', 'Slot_2.png', 'Slot_3.png', 'Slot_4.png', 'Slot_5.png', 'Slot_6.png', 'Slot_7.png', 'Slot_8.png', 'manifest.json'] };
  }, NAME);
  await c.route(/^(?!file:)/, r => r.abort());
  const pg = await c.newPage({ viewport: { width: 1300, height: 900 } });
  pg.on('pageerror', e => { if (!/firebase is not defined|before initialization|M is not defined/.test(e.message)) console.log('pageerror:', e.message.slice(0, 140)); });
  await pg.goto(URL);
  await sleep(3500);
  await pg.mouse.click(600, 400);
  const out = await pg.evaluate(async () => {
    document.body.classList.remove('lg-locked');
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));
    for (let i = 0; i < 40 && !(window.ListingLedger && window.PipeGuard); i++) await sleep(250);
    await sleep(2500);                                       // ledger snapshot arrives
    window.__started = null;
    window.runFullAutomationSequence = async (ref, name, claim) => { window.__started = { name, at: performance.now(), ok: !!(claim && claim.ok) }; };
    window.PipeGuard.hasWorkWaiting = window.PipeGuard.hasWorkWaiting;
    const waiting = window.PipeGuard.waitingCount(), next = window.PipeGuard.nextResumable();
    window.__listCalls.length = 0;
    window.autoProcessingEnabled = true;
    const t0 = performance.now();
    window.__scanAt(50);
    for (let i = 0; i < 60 && !window.__started; i++) await sleep(100);
    return { waiting, next, startedAfterMs: window.__started ? Math.round(window.__started.at - t0) : null, started: window.__started && window.__started.name, listCalls: window.__listCalls.slice() };
  });
  console.log(JSON.stringify(out, null, 1));
  await browser.close();
})();
