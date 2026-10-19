// Shared helpers for the ru-G1-* cost tests: the Firestore fake of tests/sim (store.js + fakefb.js) with read/write METERING.
//   reads  = what Firestore would bill: a listener's first snapshot = every doc in it, later snapshots = docs added/changed/removed;
//            get() = docs returned; transaction get = 1 per doc.      (counted in the page, per tab, by doc-path class)
//   writes = every set/update/delete/commit-write that reaches the store (counted in node, all tabs together)
// Needs Playwright installed globally (NODE_PATH=$(npm root -g)), like tests/sim/*.js.
const P = require('child_process').execSync('readlink -f $(npm root -g)/playwright').toString().trim();
const { chromium } = require(P);
const fs = require('fs'), path = require('path');
const { Store } = require('../sim/store.js');
const FAKE = fs.readFileSync(path.join(__dirname, '../sim/fakefb.js'), 'utf8');
const INDEX = process.env.RU_INDEX ? path.resolve(process.env.RU_INDEX) : path.resolve(__dirname, '../../Index.html');   // RU_INDEX = another copy of the page (e.g. the version before a fix)
const URL = 'http://app.test/Index.html';          // served from INDEX by a route: fetch() cannot reach file: URLs, and the Netlify function calls must be seen
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const klass = (p) => String(p).replace(/^ListingGen_Config\/live~presence~.*/, 'cfg/live~presence~*').replace(/^ListingGen_Config\/live~img~.*/, 'cfg/live~img~*')
  .replace(/^ListingGen_Config\/ledger~.*/, 'cfg/ledger~*').replace(/^ListingGen_Config\/idclaim~.*/, 'cfg/idclaim~*').replace(/^ListingGen_Config\//, 'cfg/')
  .replace(/^ListingGen_Stats\/.*/, 'stats/*').replace(/^ListingGen_Review\/.*/, 'review/*');

class MeterStore extends Store {
  constructor() { super(); this.w = {}; this.wb = {}; this.on = true; }
  op(op, p) {
    if (this.on) {
      const bump = (path, d) => { const k = klass(path); this.w[k] = (this.w[k] || 0) + 1; this.wb[k] = (this.wb[k] || 0) + (d ? JSON.stringify(d).length : 0); };
      if (op === 'set' || op === 'update' || op === 'delete') bump(p.path, p.data);
      if (op === 'commit') { const r = super.op(op, p); if (r.ok) p.writes.forEach(w => bump(w.path, w.data)); return r; }
    }
    return super.op(op, p);
  }
  seed(path, data) { this.put(path, data, false); }
}

// Page side: wraps onSnapshot / get / runTransaction of the fake and counts billable reads into window.__rd (class -> n).
const METER = `(function () {
  window.__rd = {}; window.__rb = {};
  const klass = ${klass.toString()};
  const bytes = (d) => (d == null ? 0 : JSON.stringify(d).length);
  const add = (k, n, b) => { if (n) window.__rd[k] = (window.__rd[k] || 0) + n; if (b) window.__rb[k] = (window.__rb[k] || 0) + b; };
  const fs = firebase.firestore();
  const D = Object.getPrototypeOf(fs.doc('a/b')), Q = Object.getPrototypeOf(Object.getPrototypeOf(fs.collection('x')));
  const wrap = (proto, isQuery) => {
    const orig = proto.onSnapshot;
    proto.onSnapshot = function (...a) {
      const i = a.findIndex(x => typeof x === 'function'); const next = a[i]; const me = this; const seen = new Map();
      a[i] = function (snap) {
        if (!isQuery) add('L:' + klass(me.path), 1, bytes(snap._d));
        else {
          const now = new Map();
          snap.docs.forEach(d => { now.set(d.ref.path, d._v); if (seen.get(d.ref.path) !== d._v) add('L:' + klass(d.ref.path), 1, bytes(d._d)); });
          seen.forEach((v, k) => { if (!now.has(k)) add('L:' + klass(k), 1, 0); });
          seen.clear(); now.forEach((v, k) => seen.set(k, v));
        }
        return next.apply(this, arguments);
      };
      return orig.apply(this, a);
    };
  };
  wrap(D, false); wrap(Q, true);
  const dget = D.get, qget = Q.get;
  D.get = async function () { const r = await dget.apply(this, arguments); add('G:' + klass(this.path), 1, bytes(r._d)); return r; };
  Q.get = async function () { const r = await qget.apply(this, arguments); r.docs.forEach(d => add('G:' + klass(d.ref.path), 1, bytes(d._d))); if (!r.docs.length) add('G:' + klass(this.path + '/'), 1, 0); return r; };
  const rt = fs.runTransaction;
  fs.runTransaction = function (fn) { return rt.call(fs, (tx) => { const g = tx.get; tx.get = async (ref) => { const r = await g.call(tx, ref); add('T:' + klass(ref.path), 1, bytes(r._d)); return r; }; return fn(tx); }); };
})();`;

async function world() {
  const store = new MeterStore();
  const browser = await chromium.launch({ args: ['--no-sandbox'] });
  const calls = [];                         // every Netlify function call: { tab, url, t }
  async function computer(name, stub) {
    const c = await browser.newContext();
    await c.exposeBinding('__fs', async (src, op, json) => JSON.stringify(store.op(op, JSON.parse(json))));
    await c.addInitScript(FAKE);
    await c.addInitScript(METER);
    // one router: the page itself from disk, Netlify functions counted and answered by stub(url), everything else (CDNs) refused
    await c.route('**/*', (r) => {
      const u = new globalThis.URL(r.request().url());
      if (u.host !== 'app.test') return r.abort();
      if (u.pathname === '/Index.html') return r.fulfill({ status: 200, contentType: 'text/html', body: fs.readFileSync(INDEX, 'utf8') });
      if (/^\/\.netlify\/functions\//.test(u.pathname)) {
        const rel = u.pathname + u.search;
        calls.push({ tab: name, url: rel, t: Date.now(), method: r.request().method() });
        let body = {}; try { body = (stub && stub(rel, r.request())) || {}; } catch (_) {}
        return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) });
      }
      return r.fulfill({ status: 404, body: '' });
    });
    return c;
  }
  async function open(c, opts) {
    const pg = await c.newPage({ viewport: { width: 1300, height: 900 } });
    pg.on('pageerror', e => { if (!/firebase is not defined|before initialization|M is not defined/.test(e.message)) console.log(`[${c.__name}] pageerror:`, e.message.slice(0, 160)); });
    await pg.goto(URL);
    await pg.evaluate(() => { document.body.classList.remove('lg-locked'); });
    return pg;
  }
  // a page on a virtual clock (native timers, no worker shim): advance(pg, ms) fast-forwards hours of polling in seconds
  async function openFast(c, o) {
    o = o || {};
    const pg = await c.newPage({ viewport: { width: 1300, height: 900 } });
    pg.on('pageerror', e => { if (!/firebase is not defined|before initialization|M is not defined/.test(e.message)) console.log(`[${c.__name}] pageerror:`, e.message.slice(0, 160)); });
    await pg.addInitScript('window.__lgTimerShim = true;');
    await pg.clock.install({ time: Date.now() });
    await pg.goto(URL);
    await pg.evaluate(() => { document.body.classList.remove('lg-locked'); });
    return pg;
  }
  return { store, browser, computer, open, openFast, calls, close: () => browser.close() };
}
const reads = (pg) => pg.evaluate(() => Object.assign({}, window.__rd));
const bytesOf = (pg) => pg.evaluate(() => Object.assign({}, window.__rb));
const sum = (o, pre) => Object.keys(o).filter(k => !pre || k.startsWith(pre)).reduce((n, k) => n + o[k], 0);
const diff = (a, b) => { const o = {}; Object.keys(b).forEach(k => { const d = (b[k] || 0) - (a[k] || 0); if (d) o[k] = d; }); return o; };
async function until(pg, pred, ms, arg) { const t0 = Date.now(); while (Date.now() - t0 < ms) { if (await pg.evaluate(pred, arg).catch(() => false)) return Date.now() - t0; await sleep(150); } return -1; }
const advance = async (pg, ms, step) => { step = step || 1000; for (let t = 0; t < ms; t += step) { await pg.clock.runFor(Math.min(step, ms - t)); await sleep(25); } };
const setHidden = (pg, h) => pg.evaluate((h) => { Object.defineProperty(document, 'hidden', { configurable: true, get: () => h }); Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => (h ? 'hidden' : 'visible') }); document.dispatchEvent(new Event('visibilitychange')); }, h);
module.exports = { bytesOf, advance, setHidden, world, reads, sum, diff, until, sleep, URL, klass };
