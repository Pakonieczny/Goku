// A slow/failing background archive and a folder that answers HTTP 500 must not stop the scan from starting the next ready set.
const P = require('child_process').execSync('readlink -f $(npm root -g)/playwright').toString().trim();
const { chromium } = require(P);
const fs = require('fs'), path = require('path');
const { Store } = require('./store.js');
const FAKE = fs.readFileSync(path.join(__dirname, 'fakefb.js'), 'utf8');
const URL = 'file://' + path.resolve(__dirname, '../../Index.html');
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
(async () => {
  const store = new Store();
  store.put('ListingGen_Config/ledger~Beady_Necklace_Set_527', { kind: 'ledger', setName: 'Beady_Necklace_Set_527', category: 'Beady_Necklace', description: 'x', state: 'uploading', active: true, rev: 2, approvedAt: Date.now() - 3600000, approvedBy: 'x',
    claim: { owner: 'cOLD', page: 'pOLD', label: 'Old', token: 't', leaseUntilMs: Date.now() - 5000, stage: 'archiving' }, attempts: 1, meta: { etsyDone: true, statsDone: true, listingId: '123' }, updatedMs: Date.now() - 600000, updatedBy: 'cOLD' }, false);
  const browser = await chromium.launch({ args: ['--no-sandbox'] });
  const c = await browser.newContext();
  await c.exposeBinding('__fs', async (src, op, json) => JSON.stringify(store.op(op, JSON.parse(json))));
  await c.addInitScript(FAKE);
  await c.addInitScript(() => {
    const base = 'listing-generator-1/Generated_Listing_Sets/';
    const imgs = ['Slot_1.png', 'Slot_2.png', 'Slot_3.png', 'Slot_4.png', 'Slot_5.png', 'Slot_6.png', 'Slot_7.png', 'Slot_8.png', 'manifest.json'];
    window.__storage[base + 'Completed_Listing_Sets'] = { prefixes: ['Beady_Necklace_Set_527', 'Beady_Necklace_Set_293', 'Beady_Necklace_Set_528'], items: [] };
    window.__storage[base + 'Completed_Listing_Sets/Beady_Necklace_Set_527'] = { prefixes: [], items: [] };
    window.__storage[base + 'Completed_Listing_Sets/Beady_Necklace_Set_528'] = { prefixes: [], items: imgs };
    window.__fail = {}; window.__fail[base + 'Completed_Listing_Sets/Beady_Necklace_Set_293'] = true;      // HTTP 500 on this folder
    window.__storage[base + 'Approved_Listing_Sets'] = { prefixes: ['Beady_Necklace_Set_527'], items: [] };  // 527 is half-archived
  });
  await c.route(/^(?!file:)/, r => r.abort());
  const pg = await c.newPage({ viewport: { width: 1300, height: 900 } });
  pg.on('pageerror', e => { if (!/firebase is not defined|before initialization|M is not defined/.test(e.message)) console.log('pageerror:', e.message.slice(0, 140)); });
  await pg.goto(URL); await sleep(3500); await pg.mouse.click(600, 400);
  const out = await pg.evaluate(async () => {
    document.body.classList.remove('lg-locked');
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));
    for (let i = 0; i < 40 && !(window.ListingLedger && window.PipeGuard); i++) await sleep(250);
    await sleep(2000);
    document.getElementById('queueBeadyNecklace').value = '1111111111';
    window.__started = null; window.isProcessingSet = false; window.__isScanningFB = false;
    let reconciles = 0;
    window.PipeGuard.reconcileArchived = () => { reconciles++; return new Promise((r) => setTimeout(r, 120000)); };      // an archive that takes minutes
    window.__processedCache = { at: Date.now(), names: ['Beady_Necklace_Set_527'] };
    window.runFullAutomationSequence = async (ref, name) => { window.__started = { name, at: performance.now() }; };
    window.autoProcessingEnabled = true;
    const t0 = performance.now(); window.__scanAt(50);
    for (let i = 0; i < 80 && !window.__started; i++) await sleep(100);
    const first = { startedAfterMs: window.__started ? Math.round(window.__started.at - t0) : 'NEVER', started: window.__started && window.__started.name, reconcilesStarted: reconciles };
    window.__started = null; window.isProcessingSet = false; window.__isScanningFB = false; window.__scanAt(50); await sleep(1500);
    return { first, reconcilesAfterSecondScan: reconciles };
  });
  console.log(JSON.stringify(out));
  await browser.close();
})();
