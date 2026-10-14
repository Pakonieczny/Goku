// A hung storage listing must never leave the scan sitting there.
const P = require('child_process').execSync('readlink -f $(npm root -g)/playwright').toString().trim();
const { chromium } = require(P);
const fs = require('fs'), path = require('path');
const { Store } = require('./store.js');
const FAKE = fs.readFileSync(path.join(__dirname, 'fakefb.js'), 'utf8');
const URL = 'file://' + path.resolve(__dirname, '../../Index.html');
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const which = process.argv[2] || 'approved';
(async () => {
  const store = new Store();
  const NAME = 'Beady_Necklace_Set_3';
  if (which === 'completed') store.put('ListingGen_Config/ledger~' + NAME, { kind: 'ledger', setName: NAME, category: 'Beady_Necklace', description: 'x', state: 'queued', active: true, rev: 1, approvedAt: Date.now() - 3600000, approvedBy: 'x', attempts: 0, meta: {}, updatedMs: Date.now(), updatedBy: 'x' }, false);
  const browser = await chromium.launch({ args: ['--no-sandbox'] });
  const c = await browser.newContext();
  await c.exposeBinding('__fs', async (src, op, json) => JSON.stringify(store.op(op, JSON.parse(json))));
  await c.addInitScript(FAKE);
  await c.addInitScript(([which, NAME]) => {
    const base = 'listing-generator-1/Generated_Listing_Sets/';
    const imgs = ['Slot_1.png', 'Slot_2.png', 'Slot_3.png', 'Slot_4.png', 'Slot_5.png', 'Slot_6.png', 'Slot_7.png', 'Slot_8.png', 'manifest.json'];
    window.__storage[base + 'Completed_Listing_Sets'] = { prefixes: ['Beady_Necklace_Set_1', 'Beady_Necklace_Set_2', NAME], items: [] };
    window.__storage[base + 'Completed_Listing_Sets/' + NAME] = { prefixes: [], items: imgs };
    window.__hang = {}; window.__hang[base + (which === 'approved' ? 'Approved_Listing_Sets' : 'Completed_Listing_Sets')] = true;
  }, [which, NAME]);
  await c.route(/^(?!file:)/, r => r.abort());
  const pg = await c.newPage({ viewport: { width: 1300, height: 900 } });
  pg.on('pageerror', e => { if (!/firebase is not defined|before initialization|M is not defined/.test(e.message)) console.log('pageerror:', e.message.slice(0, 140)); });
  await pg.goto(URL); await sleep(3500); await pg.mouse.click(600, 400);
  const out = await pg.evaluate(async (which) => {
    document.body.classList.remove('lg-locked');
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));
    for (let i = 0; i < 40 && !(window.ListingLedger && window.PipeGuard); i++) await sleep(250);
    await sleep(2000);
    document.getElementById('queueBeadyNecklace').value = '1111111111';
    window.__started = null; window.isProcessingSet = false; window.__isScanningFB = false;
    window.runFullAutomationSequence = async (ref, name) => { window.__started = { name, at: performance.now() }; };
    window.autoProcessingEnabled = true;
    const t0 = performance.now(); window.__scanAt(50);
    const limit = which === 'completed' ? 450 : 80;
    for (let i = 0; i < limit && !window.__started; i++) await sleep(100);
    return { hungListing: which, startedAfterMs: window.__started ? Math.round(window.__started.at - t0) : 'NEVER', started: window.__started && window.__started.name };
  }, which);
  console.log(JSON.stringify(out));
  await browser.close();
})();
