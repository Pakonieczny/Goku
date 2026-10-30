// Reliability fixes of the 2026-10-30 review:
//   H1  a computer WITHOUT Etsy never takes the pipeline over by itself (it used to, and then sat on "connect to Etsy here")
//   H2  a controller that cannot publish (no Etsy) says so (ctl.need) and an Etsy-connected computer takes over
//   H3  circuit breaker: a systemic stop / three failing sets in a row pause the pipeline; the scanner then picks nothing
//   H4  "Retry now": a failed set goes back to the queue (ledger), and its page-local park is lifted
//   H5  the Health & Recovery panel names the problem
// Run: node tests/sim/health.js [h1|h2|h3|h4|h5|all]
const P = require('child_process').execSync('readlink -f $(npm root -g)/playwright').toString().trim();
const { chromium } = require(P);
const fs = require('fs'), path = require('path');
const { Store } = require('./store.js');
const FAKE = fs.readFileSync(path.join(__dirname, 'fakefb.js'), 'utf8');
const URL = 'file://' + path.resolve(__dirname, '../../Index.html');
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const which = process.argv[2] || 'all';
let failures = 0;
const check = (ok, what) => { console.log((ok ? 'ok   ' : 'FAIL ') + what); if (!ok) failures++; };

(async () => {
  const browser = await chromium.launch({ args: ['--no-sandbox'] });
  async function world() {
    const store = new Store(), dead = new Set();
    async function computer(o) {
      const c = await browser.newContext();
      await c.exposeBinding('__fs', async (src, op, json) => { if (dead.has(src.page)) return new Promise(() => {}); return JSON.stringify(store.op(op, JSON.parse(json))); });
      if (!o || o.etsy !== false) await c.addInitScript(() => { try { localStorage.setItem('etsyRefreshToken', 'sim-refresh'); } catch (_) {} });
      await c.addInitScript(FAKE);
      if (o && o.init) await c.addInitScript(o.init);
      await c.route(/^(?!file:)/, r => r.abort());
      return c;
    }
    return { store, dead, computer };
  }
  async function open(c, name) {
    const pg = await c.newPage({ viewport: { width: 1300, height: 900 } });
    pg.on('pageerror', e => { if (!/firebase is not defined|before initialization|M is not defined/.test(e.message)) console.log(`[${name}] pageerror:`, e.message.slice(0, 160)); });
    await pg.goto(URL);
    await pg.evaluate(() => { document.body.classList.remove('lg-locked'); });
    return pg;
  }
  const role = (pg) => pg.evaluate(() => window.LiveMirror ? window.LiveMirror.state() : 'none');
  async function until(pg, pred, ms) { const t0 = Date.now(); while (Date.now() - t0 < ms) { if (await pg.evaluate(pred).catch(() => false)) return Date.now() - t0; await sleep(200); } return -1; }
  const rec = (pg) => pg.evaluate(() => { if (window.PipeGuard) { window.PipeGuard.hasWorkWaiting = () => true; window.PipeGuard.restoreAuto = async () => { window.__restoreAuto = (window.__restoreAuto || 0) + 1; return true; }; } });
  async function runningController(w, o) {
    const cA = await w.computer(o);
    const pa = await open(cA, 'A'); await sleep(2000); await pa.mouse.click(600, 400);
    await until(pa, () => window.LiveMirror.state() === 'controller', 8000);
    await pa.evaluate(() => { window.autoProcessingEnabled = true; window.isProcessingSet = true; try { localStorage.setItem('lgAutoIntent.v1', JSON.stringify({ on: true, at: Date.now() })); } catch (_) {} });
    await sleep(3000);
    return { cA, pa };
  }

  if (which === 'h1' || which === 'all') {
    console.log('\n=== H1: the working computer dies; the open one has no Etsy connection ===');
    const w = await world(); const { cA, pa } = await runningController(w);
    const cB = await w.computer({ etsy: false });
    const pb = await open(cB, 'B'); await until(pb, () => window.LiveMirror.state() === 'follower', 10000); await rec(pb);
    w.dead.add(pa);
    const t = await until(pb, () => window.LiveMirror.state() === 'controller', 70000);
    check(t < 0, `without Etsy, B does not take over by itself (${t < 0 ? 'stayed ' + await role(pb) : 'took over after ' + t + ' ms'})`);
    await pb.evaluate(() => localStorage.setItem('etsyRefreshToken', 'now-connected'));
    const t2 = await until(pb, () => window.LiveMirror.state() === 'controller', 30000);
    check(t2 >= 0, `once connected to Etsy, B takes over (${t2 >= 0 ? 'after ' + t2 + ' ms' : 'never'})`);
    await cA.close(); await cB.close();
  }

  if (which === 'h2' || which === 'all') {
    console.log('\n=== H2: the controller cannot publish (no Etsy) and says so; a connected computer relieves it ===');
    const w = await world();
    const { cA, pa } = await runningController(w, { etsy: false });
    const cB = await w.computer();
    const pb = await open(cB, 'B'); await until(pb, () => window.LiveMirror.state() === 'follower', 10000); await rec(pb);
    await sleep(3000);
    check(await role(pb) === 'follower', 'B stays a follower while A is alive and has not asked for help');
    await pa.evaluate(() => { window.LiveMirror.publish('ctl.need', { on: true, why: 'etsy', at: Date.now(), by: window.LiveMirror.terminal.id }); });
    const t = await until(pb, () => window.LiveMirror.state() === 'controller', 30000);
    check(t >= 0, `B takes the pipeline over from the Etsy-less controller (${t >= 0 ? 'after ' + t + ' ms' : 'never'})`);
    const tA = await until(pa, () => window.LiveMirror.state() === 'follower', 15000);
    check(tA >= 0, `A becomes a follower (${tA >= 0 ? tA + ' ms later' : 'still ' + await role(pa)})`);
    await sleep(4000);
    check(await role(pb) === 'controller' && await role(pa) === 'follower', `roles settle: A ${await role(pa)}, B ${await role(pb)}`);
    await cA.close(); await cB.close();
  }

  if (which === 'h3' || which === 'all') {
    console.log('\n=== H3: circuit breaker ===');
    const w = await world(); const c = await w.computer();
    const pg = await open(c, 'solo'); await sleep(3000);
    const r = await pg.evaluate(async () => {
      const sleep = (ms) => new Promise(r => setTimeout(r, ms));
      const G = window.PipeGuard, out = {};
      const runFail = async (name, msg, o) => { const h = G.begin(name, { local: true }); h.note(msg, o); await h.finish(); };
      await runFail('Beady_Necklace_Set_1', 'listing-ID queue unreachable — nothing was written to Etsy', { noFault: true, cooldownMs: 120000 });
      out.systemic = !!G.paused(); out.systemicWhy = G.paused() && G.paused().why;
      G.resumeNow(); out.afterResume = !!G.paused();
      await runFail('Beady_Necklace_Set_2', 'no unused listing IDs left in the queue', { noFault: true, cooldownMs: 600000 });
      out.categoryEmptyPauses = !!G.paused();
      await runFail('Beady_Necklace_Set_3', 'upload FAILED', { cooldownMs: 600000 });
      await runFail('Beady_Necklace_Set_4', 'upload FAILED', { cooldownMs: 600000 });
      out.twoFaults = !!G.paused();
      await runFail('Beady_Necklace_Set_5', 'generation failed (title)', { cooldownMs: 600000 });
      out.threeFaults = !!G.paused(); out.threeWhy = G.paused() && G.paused().why;
      // the scanner picks nothing while paused, and at once after Resume
      window.__started = null;
      window.runFullAutomationSequence = async (ref, name) => { window.__started = name; };
      const base = 'listing-generator-1/Generated_Listing_Sets/';
      window.__storage[base + 'Completed_Listing_Sets'] = { prefixes: ['Beady_Necklace_Set_9'], items: [] };
      window.__storage[base + 'Completed_Listing_Sets/Beady_Necklace_Set_9'] = { prefixes: [], items: ['1.png', '2.png', '3.png', '4.png', '5.png', '6.png', '7.png', '8.png'] };
      const q = document.getElementById('queueBeadyNecklace'); if (q) q.value = '1234567890';
      window.autoProcessingEnabled = true;
      window.__scanAt(50); await sleep(1500);
      out.pickedWhilePaused = window.__started;
      out.statusWhilePaused = (document.getElementById('fbText') || {}).textContent;
      G.resumeNow(); await sleep(1500);
      out.pickedAfterResume = window.__started;
      return out;
    });
    check(r.systemic, 'an unreachable listing-ID queue pauses the pipeline: ' + r.systemicWhy);
    check(!r.afterResume, 'Resume now lifts the pause');
    check(!r.categoryEmptyPauses, 'one empty category does not pause everything');
    check(!r.twoFaults, 'two failing sets do not pause yet');
    check(r.threeFaults, 'three different failing sets in a row pause: ' + r.threeWhy);
    check(!r.pickedWhilePaused && /Paused/.test(r.statusWhilePaused || ''), 'the scanner picks nothing while paused and says why: ' + r.statusWhilePaused);
    check(r.pickedAfterResume === 'Beady_Necklace_Set_9', 'after Resume the ready set starts at once (' + r.pickedAfterResume + ')');
    await c.close();
  }

  if (which === 'h4' || which === 'all') {
    console.log('\n=== H4: Retry now ===');
    const w = await world();
    const NAME = 'Beady_Necklace_Set_77';
    w.store.put('ListingGen_Config/ledger~' + NAME, { kind: 'ledger', setName: NAME, category: 'Beady_Necklace', description: 'Fox', state: 'failed', active: true, rev: 6, approvedAt: Date.now() - 3600000, approvedBy: 'x',
      claim: null, attempts: 5, lastError: 'upload FAILED', retryAt: Date.now() + 3600000, meta: { listingId: '4444444444', seq: 3 }, updatedMs: Date.now() - 60000, updatedBy: 'x' }, false);
    const c = await w.computer();
    const pg = await open(c, 'solo'); await sleep(3500);
    const r = await pg.evaluate(async (NAME) => {
      const sleep = (ms) => new Promise(r => setTimeout(r, ms));
      const L = window.ListingLedger, G = window.PipeGuard, out = {};
      out.before = G.eligibility(NAME);
      window.__manuallySkippedSets.add(NAME, 'failed-twice');
      const res = await L.retryNow([NAME, 'Nope_Set_1']);
      await sleep(500);
      const e = L.get(NAME);
      out.res = res; out.after = { state: e.state, attempts: e.attempts, retryAt: e.retryAt, listingId: G.effectiveMeta(NAME, e).listingId };
      out.eligible = G.eligibility(NAME); out.parked = window.__manuallySkippedSets.has(NAME);
      return out;
    }, NAME);
    check(!r.before.ok, 'before: the failed set is not eligible (' + r.before.why + ')');
    check(r.res.retried.length === 1 && r.res.skipped.length === 1, 'retryNow retried the set and left the unknown name alone');
    check(r.after.state === 'queued' && !r.after.attempts && !r.after.retryAt, 'after: queued, no attempts, no cooldown');
    check(r.after.listingId === '4444444444', 'its reserved listing ID is kept (reused when it runs)');
    check(r.eligible.ok && !r.parked, 'it is eligible again and no longer parked on this page');
    const stored = JSON.parse(JSON.stringify((w.store.docs.get('ListingGen_Config/ledger~' + NAME) || {}).d || {}));
    check(stored.state === 'queued', 'the shared ledger document says queued');
    await c.close();
  }

  if (which === 'h5' || which === 'all') {
    console.log('\n=== H5: Health & Recovery panel ===');
    const w = await world();
    for (let i = 1; i <= 4; i++) {
      const n = 'Beady_Necklace_Set_' + (500 + i);
      w.store.put('ListingGen_Config/ledger~' + n, { kind: 'ledger', setName: n, category: 'Beady_Necklace', description: 'd', state: 'queued', active: true, rev: 1, approvedAt: Date.now() - 1000 * i, approvedBy: 'x',
        claim: null, attempts: i === 4 ? 1 : 0, lastError: i === 4 ? 'upload FAILED' : '', retryAt: i === 4 ? Date.now() + 600000 : 0, meta: i === 4 ? { listingId: '5555555555', seq: 1 } : {}, updatedMs: Date.now(), updatedBy: 'x' }, false);
    }
    const c = await w.computer({ etsy: false });
    const pg = await open(c, 'solo'); await sleep(3500);
    const r = await pg.evaluate(async () => {
      const sleep = (ms) => new Promise(r => setTimeout(r, ms));
      document.getElementById('dNavHealth').click(); await sleep(300);
      const body = document.getElementById('hrBody');
      const s = window.HealthPanel.snapshot();
      return { open: !!body, text: body ? body.innerText : '', ready: s.queue.ready.length, cooling: s.queue.cooling.length, earmarked: s.earmarkedIds,
        badge: getComputedStyle(document.getElementById('dNavHealthBadge')).display !== 'none' };
    });
    check(r.open, 'the panel opens from the rail');
    check(r.ready === 3 && r.cooling === 1, `queue counts: ${r.ready} ready, ${r.cooling} cooling`);
    check(/Auto-Pilot is off on every computer/.test(r.text), 'names the problem: approved sets waiting, Auto-Pilot off');
    check(/held back/.test(r.text) && /Retry waiting sets now/.test(r.text), 'offers Retry for the held-back set');
    check(r.earmarked === 1 && /reserved by waiting sets/.test(r.text), 'explains the reserved listing ID');
    check(r.badge, 'the rail badge shows that something needs attention');
    await c.close();
  }

  if (which === 'h6' || which === 'all') {
    console.log('\n=== H6: Start Auto pressed on a follower; the working computer cannot start (no Etsy there) ===');
    const w = await world();
    const cA = await w.computer({ etsy: false });
    const pa = await open(cA, 'A'); await sleep(2000); await pa.mouse.click(600, 400);
    await until(pa, () => window.LiveMirror.state() === 'controller', 8000);
    const cB = await w.computer();
    const pb = await open(cB, 'B'); await until(pb, () => window.LiveMirror.state() === 'follower', 10000);
    await pb.evaluate(() => { window.accessToken = 'sim-token'; window.__pickerOpened = 0; window.AutoScopePicker.open = async () => { window.__pickerOpened++; return { scope: null, batch: 0 }; }; });
    await pb.evaluate(() => document.getElementById('toggleAuto').click());
    const t = await until(pb, () => window.LiveMirror.state() === 'controller' && window.autoProcessingEnabled === true, 45000);
    check(t >= 0, `B takes over and Auto-Pilot runs on B (${t >= 0 ? 'after ' + t + ' ms' : 'never; B is ' + await role(pb) + ', auto ' + await pb.evaluate(() => window.autoProcessingEnabled)})`);
    check(await pb.evaluate(() => window.__pickerOpened) === 1, 'the source picker opened exactly once (no second dialog)');
    await until(pa, () => window.LiveMirror.state() === 'follower', 15000);
    check(await role(pa) === 'follower', 'A became a follower');
    await cA.close(); await cB.close();
  }

  await browser.close();
  console.log(failures ? `\n${failures} check(s) FAILED` : '\nall checks passed');
  process.exit(failures ? 1 : 0);
})();
