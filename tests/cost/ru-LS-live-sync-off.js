// ru-LS: the multi-computer live connection (LiveMirror, ListingGen_Config live~*) can be switched off with ONE constant,
// LIVE_SYNC_ENABLED in Index.html. With it false this page must
//   - attach no Firestore listener on a live~* document (ctrl / state / img / presence / mail) and read/write no live~* doc at all,
//   - boot without page errors, say "Live sync switched off · this computer works on its own" in the status chip,
//   - keep every LiveMirror method callable with a safe answer (nothing throws, nothing waits forever),
//   - still work as a single computer (the scan starts a set, the ledger enqueue/claim/complete round trip hits Firestore as before).
// A CONTROL run of the same harness against a copy of Index.html with the constant flipped to true must see live~* listeners and
// writes, so a green "zero" above means the detector works and not that it is blind.
// Run:  NODE_PATH=$(npm root -g) PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers node tests/cost/ru-LS-live-sync-off.js
const P = require('child_process').execSync('readlink -f $(npm root -g)/playwright').toString().trim();
const { chromium } = require(P);
const fs = require('fs'), os = require('os'), path = require('path');
const { Store } = require('../sim/store.js');
const FAKE = fs.readFileSync(path.join(__dirname, '../sim/fakefb.js'), 'utf8');
const ROOT = path.resolve(__dirname, '../..');
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const FLAG_RE = /const LIVE_SYNC_ENABLED = (true|false);/;

// Count what the page does to live~* documents (Node side sees every Firestore operation of the fake) and which listeners it attaches (in page).
const PATCH_SNAPSHOTS = () => {
  window.__snaps = [];
  const fs_ = window.firebase.firestore();
  const DR = Object.getPrototypeOf(fs_.doc('a/b'));                                  // DocRef
  const Q = Object.getPrototypeOf(Object.getPrototypeOf(fs_.collection('a')));       // Query (CollRef extends it)
  const od = DR.onSnapshot, oq = Q.onSnapshot;
  DR.onSnapshot = function (...a) { window.__snaps.push({ kind: 'doc', path: this.path }); return od.apply(this, a); };
  Q.onSnapshot = function (...a) { window.__snaps.push({ kind: 'query', path: this.path, f: this.f }); return oq.apply(this, a); };
};
const isLiveId = (p) => String(p || '').split('/').pop().startsWith('live~');
const opTouchesLive = (e) => {
  const p = e.p;
  if (e.op === 'commit') return Object.keys(p.reads || {}).some(isLiveId) || (p.writes || []).some(w => isLiveId(w.path));
  if (e.op === 'query') return (p.f || []).some(f => f.field === '__name__' && String(f.val).startsWith('live~'));
  return isLiveId(p.path);
};
const snapTouchesLive = (s) => isLiveId(s.path) || (s.f || []).some(f => f.field === '__name__' && String(f.val).startsWith('live~'));

async function scenario(browser, indexPath, { exerciseApi }) {
  const URL = 'file://' + indexPath;
  const store = new Store(), ops = [], errors = [], ignored = [];
  const NAME = 'Beady_Necklace_Set_3';
  const c = await browser.newContext();
  await c.exposeBinding('__fs', async (src, op, json) => { const p = JSON.parse(json); if (op !== 'changes') ops.push({ op, p }); return JSON.stringify(store.op(op, p)); });
  await c.addInitScript(FAKE);
  await c.addInitScript(PATCH_SNAPSHOTS);
  await c.addInitScript(() => {
    const base = 'listing-generator-1/Generated_Listing_Sets/';
    const imgs = ['Slot_1.png', 'Slot_2.png', 'Slot_3.png', 'Slot_4.png', 'Slot_5.png', 'Slot_6.png', 'Slot_7.png', 'Slot_8.png', 'manifest.json'];
    window.__storage[base + 'Completed_Listing_Sets'] = { prefixes: ['Beady_Necklace_Set_1', 'Beady_Necklace_Set_2', 'Beady_Necklace_Set_3'], items: [] };
    window.__storage[base + 'Completed_Listing_Sets/Beady_Necklace_Set_3'] = { prefixes: [], items: imgs };
  });
  await c.route(/^(?!file:)/, r => r.abort());
  const pg = await c.newPage({ viewport: { width: 1300, height: 900 } });
  pg.on('pageerror', e => { (/firebase is not defined|before initialization|M is not defined/.test(e.message) ? ignored : errors).push(e.message.slice(0, 160)); });   // same benign set the other sim tests ignore (CDN scripts are blocked offline)
  await pg.goto(URL); await sleep(3500);
  await pg.mouse.click(600, 400);                                                    // a trusted click: with live sync ON this is "I am working here" and claims live~ctrl
  const out = await pg.evaluate(async ({ exerciseApi, NAME }) => {
    document.body.classList.remove('lg-locked');
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));
    const T = (p, ms) => Promise.race([Promise.resolve(p), sleep(ms || 3000).then(() => 'TIMEOUT')]);
    for (let i = 0; i < 40 && !(window.ListingLedger && window.PipeGuard); i++) await sleep(250);
    const r = { role: window.LiveMirror && window.LiveMirror.state() };
    if (exerciseApi) {
      const L = window.LiveMirror, a = {}, thrown = [];
      const t = (name, fn) => { try { a[name] = fn(); } catch (e) { thrown.push(name + ': ' + (e && e.message)); } };
      const ta = async (name, fn) => { try { a[name] = await T(fn()); } catch (e) { thrown.push(name + ': ' + (e && e.message)); } };
      let changed = 0;
      a.ready = await T(L.ready);
      t('register', () => L.register('ws.ru-test', { read: () => 1, apply: () => {} }));
      t('touch', () => L.touch('ws.ru-test')); t('touchAll', () => L.touchAll());
      t('publish', () => L.publish('ws.ru-test', 5)); t('remove', () => L.remove('ws.ru-test'));
      t('get', () => L.get('ws.ru-test') === undefined); t('getMeta', () => L.getMeta('ws.ru-test') === null);
      t('onChange', () => typeof L.onChange('ws', () => { changed++; })); t('onLost', () => typeof L.onLost(() => {})); t('onError', () => typeof L.onError(() => {}));
      t('onViewers', () => typeof L.onViewers(() => {})); t('onRole', () => typeof L.onRole(() => {}));
      await ta('edit', () => L.edit('ws.ru-test', 3));
      t('controller', () => L.controller()); t('viewers', () => L.viewers().length);
      t('info', () => { const i = L.info(); return { role: i.role, denied: i.denied, viewers: i.viewers }; });
      await ta('command', () => L.command('stop', {})); t('onCommand', () => typeof L.onCommand('ru-test', () => {}));
      await ta('takeOver', () => L.takeOver({ reason: 'user' })); await ta('yield', () => L.yield()); await ta('setMode', () => L.setMode('auto'));
      t('activity', () => L.activity('control', 'x')); t('activityTail', () => Array.isArray(L.activityTail()));
      await ta('imgPut', () => L.img.put('1', 'data:image/jpeg;base64,AAAA', { ver: 1 })); t('imgGet', () => L.img.get('1'));
      t('imgWatch', () => { const un = L.img.watch('1', () => {}); const ok = typeof un === 'function'; un(); return ok; }); t('imgSlots', () => L.img.slots().length);
      t('stats', () => { const s = L.stats(); return { writes: s.writes, reads: s.reads }; }); t('config', () => typeof L.config({}));
      t('bar', () => !!L.bar && L.bar.id); t('applying', () => L.applying()); t('terminal', () => !!L.terminal.id);
      await sleep(300);
      a.changeFired = changed; a.thrown = thrown; r.api = a;
      // the page's own consumers of LiveMirror: the pipeline gate, and a clean (not "waiting forever") answer to "may I start work?"
      r.mayRunScan = window.PipeControl ? window.PipeControl.mayRun('scan') : 'no PipeControl';
      // single-computer flow 1: the scan picks the ready folder and starts its sequence (scan.js)
      document.getElementById('queueBeadyNecklace').value = '1111111111, 2222222222';
      window.__started = null; window.isProcessingSet = false; window.__isScanningFB = false;
      window.runFullAutomationSequence = async (ref, name) => { window.__started = { name }; };
      window.autoProcessingEnabled = true; window.__scanAt(50);
      for (let i = 0; i < 80 && !window.__started; i++) await sleep(100);
      r.scanStarted = window.__started && window.__started.name;
      // single-computer flow 2: the job ledger (ListingGen_Config ledger~, NOT part of live sync) enqueue -> claim -> complete
      const LG = window.ListingLedger, set = 'Beady_Necklace_Set_9001';
      const en = await T(LG.enqueue(set, { category: 'Beady_Necklace', description: 'ru-LS test' }));
      const cl = await T(LG.claim(set, { stage: 'preparing' }));
      const co = cl && cl.token ? await T(LG.complete(set, cl.token, { listingId: '999000111', etsyUrl: 'https://example.invalid/l/1' })) : null;
      await T(LG.flush(), 5000);
      r.ledger = { enqueue: en && en.ok, claim: cl && cl.ok, complete: co && co.ok };
    }
    await sleep(10000);                                                              // idle: every timer of the module (3 s label, 8 s ready, 2 s sweep, presence, lease) would have fired
    const b = document.getElementById('lmBar');
    r.bar = b ? { hidden: b.hidden, shown: b.offsetHeight > 0, text: b.textContent.replace(/\s+/g, ' ').trim(), parent: b.parentNode && (b.parentNode.id || b.parentNode.tagName) } : null;
    r.role2 = window.LiveMirror && window.LiveMirror.state();
    r.snaps = window.__snaps;
    return r;
  }, { exerciseApi, NAME });
  await c.close();
  const ledgerDocs = [...store.docs.keys()].filter(k => /\/ledger~Beady_Necklace_Set_9001$/.test(k));
  return { out, ops, errors, ignored, liveOps: ops.filter(opTouchesLive), liveSnaps: out.snaps.filter(snapTouchesLive), otherSnaps: out.snaps.filter(s => !snapTouchesLive(s)), ledgerDocs, liveDocs: [...store.docs.keys()].filter(isLiveId) };
}

let failed = 0;
const check = (ok, what, extra) => { if (!ok) failed++; console.log((ok ? 'PASS  ' : 'FAIL  ') + what + (extra !== undefined && !ok ? '   -> ' + JSON.stringify(extra) : '')); };

(async () => {
  const index = path.join(ROOT, 'Index.html'), html = fs.readFileSync(index, 'utf8');
  const m = FLAG_RE.exec(html);
  check(!!m, 'Index.html declares LIVE_SYNC_ENABLED');
  if (!m) { process.exit(1); }
  const flagOn = m[1] === 'true';
  const browser = await chromium.launch({ args: ['--no-sandbox'] });
  console.log('LIVE_SYNC_ENABLED in Index.html = ' + m[1]);

  console.log('\n--- A. Index.html as it is (' + (flagOn ? 'live sync ON: only the control expectations apply' : 'live sync OFF') + ') ---');
  const A = await scenario(browser, index, { exerciseApi: !flagOn });
  check(A.errors.length === 0, 'page boots without page errors (ignoring the ' + A.ignored.length + ' offline-CDN ones the other sim tests ignore)', A.errors);
  check(A.otherSnaps.length > 0, 'harness sees the other listeners (' + A.otherSnaps.length + ' non-live onSnapshot attachments: ledger, queues, stats ...)');
  if (!flagOn) {
    check(A.out.role === 'denied' && A.out.role2 === 'denied', 'LiveMirror.state() is the existing "denied" (works on its own) role', [A.out.role, A.out.role2]);
    check(A.liveSnaps.length === 0, 'ZERO onSnapshot listeners on live~* (ctrl/state/img/presence/mail)', A.liveSnaps);
    check(A.liveOps.length === 0, 'ZERO Firestore reads/writes/deletes/transactions/queries on live~* docs (' + A.ops.length + ' ops in total, none live)', A.liveOps.slice(0, 5));
    check(A.liveDocs.length === 0, 'no live~* document was created in the database', A.liveDocs);
    check(A.out.bar && A.out.bar.shown && !A.out.bar.hidden && /Live sync switched off · this computer works on its own/.test(A.out.bar.text), 'status chip reads "Live sync switched off · this computer works on its own" and is visible', A.out.bar);
    const a = A.out.api || {};
    check(a.thrown && a.thrown.length === 0, 'no LiveMirror method throws', a.thrown);
    check(!Object.keys(a).some(k => a[k] === 'TIMEOUT'), 'no LiveMirror method waits forever (ready, edit, command, takeOver, yield, setMode, img.put all answer)', Object.keys(a).filter(k => a[k] === 'TIMEOUT'));
    check(a.ready === 'denied', 'ready resolves with "denied"', a.ready);
    check(a.register === true && a.touch === false && a.touchAll === false && a.publish === false && a.remove === false, 'register ok; touch/touchAll/publish/remove return false', [a.register, a.touch, a.touchAll, a.publish, a.remove]);
    check(a.get === true && a.getMeta === true && a.controller === null && a.viewers === 0 && a.changeFired === 0, 'get/getMeta empty, controller null, no viewers, onChange never fires', [a.get, a.getMeta, a.controller, a.viewers, a.changeFired]);
    check(a.edit && a.edit.ok === false && a.edit.reason === 'denied', 'edit -> {ok:false, reason:"denied"}', a.edit);
    check(a.command && a.command.ok === false && a.takeOver && a.takeOver.ok === false && a.takeOver.reason === 'denied' && a.yield && a.yield.ok === true && a.setMode && a.setMode.ok === false, 'command / takeOver / setMode refuse, yield is a no-op', [a.command, a.takeOver, a.yield, a.setMode]);
    check(a.imgPut && a.imgPut.ok === false && a.imgGet === null && a.imgWatch === true && a.imgSlots === 0, 'img.put refused, img.get null, img.watch returns an unsubscribe and attaches nothing', [a.imgPut, a.imgGet, a.imgWatch, a.imgSlots]);
    check(a.info && a.info.role === 'denied' && a.info.denied === true && a.info.viewers === 0 && a.stats && a.stats.writes === 0 && a.stats.reads === 0, 'info()/stats() answer: role denied, 0 reads, 0 writes', [a.info, a.stats]);
    check(A.out.mayRunScan === true && A.out.scanStarted === 'Beady_Necklace_Set_3', 'single-computer pipeline: PipeControl.mayRun("scan") is true and the scan starts the ready set', [A.out.mayRunScan, A.out.scanStarted]);
    check(A.out.ledger && A.out.ledger.enqueue && A.out.ledger.claim && A.out.ledger.complete && A.ledgerDocs.length === 1, 'job ledger (not live sync) still enqueues, claims and completes through Firestore', [A.out.ledger, A.ledgerDocs]);
  } else {
    check(A.liveSnaps.length > 0 && A.liveOps.length > 0, 'live sync ON: live~* listeners and operations are present', [A.liveSnaps.length, A.liveOps.length]);
  }

  if (!flagOn) {
    console.log('\n--- B. CONTROL: same harness, copy of Index.html with LIVE_SYNC_ENABLED = true (the detector must see live~* traffic) ---');
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ru-LS-'));
    try {
      fs.writeFileSync(path.join(tmp, 'Index.html'), html.replace(FLAG_RE, 'const LIVE_SYNC_ENABLED = true;'));
      fs.cpSync(path.join(ROOT, 'assets'), path.join(tmp, 'assets'), { recursive: true });
      const B = await scenario(browser, path.join(tmp, 'Index.html'), { exerciseApi: false });
      const kinds = [...new Set(B.liveSnaps.map(s => s.kind + ':' + String(s.path).split('/').pop()))];
      check(B.liveSnaps.length >= 3, 'control: ' + B.liveSnaps.length + ' live~* listeners attached (' + kinds.join(', ') + ')', B.liveSnaps);
      check(B.liveSnaps.some(s => s.kind === 'query' && (s.f || []).some(f => String(f.val).startsWith('live~presence~'))), 'control: the presence prefix-range listener is attached');
      check(B.liveOps.length > 0 && B.liveDocs.length > 0, 'control: ' + B.liveOps.length + ' operations on live~* docs, docs created: ' + B.liveDocs.join(', '), [B.liveOps.length, B.liveDocs]);
    } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
  }
  await browser.close();
  console.log('\n' + (failed ? failed + ' CHECK(S) FAILED' : 'ALL CHECKS PASSED'));
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error(e); process.exit(2); });
