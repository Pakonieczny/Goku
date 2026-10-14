// Normal scan (nothing interrupted): picks the first READY folder, lists the finished-sets pile only once a minute.
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
  await c.addInitScript(() => {
    const base = 'listing-generator-1/Generated_Listing_Sets/';
    const many = []; for (let i = 1; i <= 436; i++) many.push('Beady_Necklace_Set_' + i);
    window.__storage[base + 'Completed_Listing_Sets'] = { prefixes: many, items: [] };
    window.__storage[base + 'Approved_Listing_Sets'] = { prefixes: Array.from({ length: 3000 }, (_, i) => 'Done_Set_' + i), items: [] };
    const imgs = ['Slot_1.png', 'Slot_2.png', 'Slot_3.png', 'Slot_4.png', 'Slot_5.png', 'Slot_6.png', 'Slot_7.png', 'Slot_8.png', 'manifest.json'];
    window.__storage[base + 'Completed_Listing_Sets/Beady_Necklace_Set_1'] = { prefixes: [], items: ['Slot_1.png'] };   // incomplete
    window.__storage[base + 'Completed_Listing_Sets/Beady_Necklace_Set_2'] = { prefixes: [], items: [] };               // empty
    window.__storage[base + 'Completed_Listing_Sets/Beady_Necklace_Set_3'] = { prefixes: [], items: imgs };             // ready
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
    document.getElementById('queueBeadyNecklace').value = '1111111111, 2222222222';
    window.__started = null;
    window.runFullAutomationSequence = async (ref, name) => { window.__started = { name, at: performance.now() }; };
    window.__listCalls.length = 0;
    window.autoProcessingEnabled = true;
    const t0 = performance.now(); window.__scanAt(50);
    for (let i = 0; i < 80 && !window.__started; i++) await sleep(100);
    const first = { picked: window.__started && window.__started.name, ms: window.__started ? Math.round(window.__started.at - t0) : null, approvedListings: window.__listCalls.filter(p => /Approved_Listing_Sets$/.test(p)).length, completedListings: window.__listCalls.filter(p => /Completed_Listing_Sets$/.test(p)).length, folderChecks: window.__listCalls.filter(p => /Set_\d+$/.test(p)).length };
    // second scan within a minute: finished-sets pile must not be listed again
    window.isProcessingSet = false; window.__isScanningFB = false; window.__started = null; window.__listCalls.length = 0;
    window.__scanAt(50);
    for (let i = 0; i < 80 && !window.__started; i++) await sleep(100);
    return { first, secondApprovedListings: window.__listCalls.filter(p => /Approved_Listing_Sets$/.test(p)).length };
  });
  console.log(JSON.stringify(out, null, 1));
  await browser.close();
})();
