// Copied unchanged from GoKu_Shipping tests/cost/meter.cjs so the Goku cost tests run on their own (CommonJS, no dependencies).
// Firestore / Storage cost meter for the offline tests (Firebase cost emergency, FC1).
//
// WHAT IT DOES  Counts reads, document bytes, writes, deletes and aggregation (count()) reads, per OPERATION NAME and per CALLER
// (file:line of the netlify/functions code that issued the call), for any Admin-SDK-shaped Firestore. It ships its own full
// in-memory Firestore (createDb) AND can wrap the little fake that a test already has (wrap). Nothing here touches a network,
// the real Firestore or any secret.
//
// BILLING RULES IT FOLLOWS (Firestore Standard, same as the invoice)
//   doc get / getAll ref / tx.get ........ 1 read per document asked for, a missing document is still 1 read
//   query get ............................ 1 read per document returned, an empty result is 1 read; offset(n) skipped docs ARE billed
//   select(...fields) .................... same read count, but only the selected fields count as bytes (this is what a field mask saves)
//   count() .............................. 1 read per 1000 index entries matched (min 1), 0 document bytes
//   onSnapshot ........................... 1 read per document in the first answer, then 1 per document changed in each later answer
//   set / update / create / add .......... 1 write each (a batch or a transaction counts each queued operation once)
//   delete ............................... 1 delete
//   bytes ................................ Buffer.byteLength(JSON.stringify(data)) per document read (what crosses the wire and is billed as egress)
//
// USAGE (copy this block into a test, run it with plain node, e.g. node tests/cost/my-area-cost.cjs)
//   const meter = require('../cost/meter.cjs');
//   const m = meter.create();                          // new empty meter + in-memory Firestore: m.db
//   m.db.seed({ 'orders/o1': { total: 12 }, 'orders/o2': { total: 5 } });   // plain objects, no cost counted
//   m.install();                                       // require('.../firebaseAdmin') now returns this fake admin (Module._load hook)
//   const fn = require('../../netlify/functions/someFunction.js');         // require it AFTER install()
//   await m.op('someFunction.poll', () => fn.handler(event));              // everything inside is booked under that name
//   m.uninstall();
//   m.print();                                         // table: per op, per caller, per collection, with USD at current prices
//   const r = m.report();                              // { total, byOp, byCaller, byCollection, cost }  (plain JSON)
//   // before/after in one test: const a = m.snapshot(); ...call...; const d = m.since(a);  d.reads, d.bytes, d.writes ...
//   // per hour for a page that calls every 3 s:  meter.perHour(d, 3600 / 3)  ->  { reads, bytes, writes, deletes, usd }
//   // own fake already in the test?  const db = meter.wrap(myFakeDb, m);  (reads/writes through db are then counted)
//   // assertions:  meter.assertMax(d, { reads: 10, bytes: 40000 }, 'library poll when nothing changed')
//
// PRICES  meter.PRICES holds the USD list prices (Firestore multi-region nam5/nam7, Standard) used by cost(); see
// /mnt/project-files/plans/firebase-cost/cost-model.md for where each number comes from and which ones are not certain.
'use strict';
const path = require('path'), Module = require('module');
const { AsyncLocalStorage } = require('async_hooks');
const ROOT = path.join(__dirname, '..', '..');
const GIB = 1024 * 1024 * 1024;

const PRICES = Object.freeze({
  readPer100k: 0.06, writePer100k: 0.18, deletePer100k: 0.02,     // Firestore multi-region (nam5 / nam7), USD
  egressPerGiB: 0.12,                                              // internet egress after the free 10 GiB / month (Firestore and Storage), USD
  storedPerGiBMonth: 0.18,                                         // Firestore stored data, multi-region, USD
  freeReadsPerDay: 50000, freeWritesPerDay: 20000, freeDeletesPerDay: 20000, freeEgressGiBPerMonth: 10
});

/* ───────────────────────────── sizes ───────────────────────────── */
const sizeOf = v => { try { return Buffer.byteLength(JSON.stringify(v, (k, x) => (x && typeof x.toMillis === 'function' ? x.toMillis() : x)) || '', 'utf8'); } catch (e) { return 0; } };

/* ───────────────────────────── counters ───────────────────────────── */
const zero = () => ({ reads: 0, bytes: 0, writes: 0, deletes: 0, aggs: 0, calls: 0 });
const add = (t, k, n) => { t[k] += n; };
const clone = o => JSON.parse(JSON.stringify(o));
function cost(t, prices) {
  const P = Object.assign({}, PRICES, prices || {});
  const reads = (t.reads + t.aggs) / 1e5 * P.readPer100k, writes = t.writes / 1e5 * P.writePer100k, deletes = t.deletes / 1e5 * P.deletePer100k;
  const egress = t.bytes / GIB * P.egressPerGiB;
  return { reads, writes, deletes, egress, total: reads + writes + deletes + egress };
}

function callerOf() {
  const lim = Error.stackTraceLimit; Error.stackTraceLimit = 40;
  const st = (new Error().stack || '').split('\n').slice(1); Error.stackTraceLimit = lim;
  let first = '';
  for (const line of st) {
    const m = /\(?((?:\/|file:\/\/)[^():]+):(\d+):\d+\)?\s*$/.exec(line); if (!m) continue;
    const f = m[1].replace(/^file:\/\//, '');
    if (f === __filename || f.startsWith('node:') || f.includes('node_modules') || f.includes('/node:internal')) continue;
    const rel = path.relative(ROOT, f) + ':' + m[2];
    if (/netlify\/functions\/|^lib\/|^[^/]*\.js:/.test(rel)) return rel;      // the first frame inside the product code wins
    if (!first) first = rel;                                                   // else the first frame outside the meter (the test itself)
  }
  return first || '(unknown)';
}

/* ───────────────────────────── the meter ───────────────────────────── */
function create(opts) {
  opts = opts || {};
  const als = new AsyncLocalStorage();
  const M = { _als: als, total: zero(), byOp: {}, byCaller: {}, byCollection: {}, storage: { downloads: 0, downloadBytes: 0, uploads: 0, uploadBytes: 0, deletes: 0, metadata: 0, lists: 0 }, log: [] };
  const bucket = (map, key) => map[key] || (map[key] = zero());
  const book = (kind, n, bytes, ctx) => {
    const op = (ctx && ctx.op) || (als.getStore() || {}).op || '(no op)';
    const caller = (ctx && ctx.caller) || '(unknown)', col = (ctx && ctx.col) || '(unknown)';
    for (const t of [M.total, bucket(M.byOp, op), bucket(M.byCaller, caller), bucket(M.byCollection, col)]) {
      add(t, 'calls', 1);
      if (kind === 'agg') add(t, 'aggs', n); else add(t, kind, n);
      if (bytes) add(t, 'bytes', bytes);
    }
    if (opts.log) M.log.push({ op, caller, col, kind, n, bytes });
  };
  M.book = book;
  M.op = (name, fn) => als.run({ op: name }, fn);
  M.reset = () => { M.total = zero(); M.byOp = {}; M.byCaller = {}; M.byCollection = {}; M.log = []; M.storage = { downloads: 0, downloadBytes: 0, uploads: 0, uploadBytes: 0, deletes: 0, metadata: 0, lists: 0 }; };
  M.snapshot = () => ({ total: clone(M.total), storage: clone(M.storage) });
  M.since = a => { const d = zero(); for (const k of Object.keys(d)) d[k] = M.total[k] - a.total[k]; d.cost = cost(d); d.storage = {}; for (const k of Object.keys(M.storage)) d.storage[k] = M.storage[k] - a.storage[k]; return d; };
  M.report = () => ({ total: clone(M.total), cost: cost(M.total), byOp: clone(M.byOp), byCaller: clone(M.byCaller), byCollection: clone(M.byCollection), storage: clone(M.storage) });
  M.print = (stream) => {
    const w = stream || (s => process.stdout.write(s + '\n'));
    const row = (name, t) => `${String(name).padEnd(52).slice(0, 52)} ${String(t.reads + t.aggs).padStart(9)} ${String(t.bytes).padStart(12)} ${String(t.writes).padStart(7)} ${String(t.deletes).padStart(7)} ${('$' + cost(t).total.toFixed(5)).padStart(11)}`;
    const head = (h) => { w(''); w(h); w(`${'name'.padEnd(52)} ${'reads'.padStart(9)} ${'bytes'.padStart(12)} ${'writes'.padStart(7)} ${'deletes'.padStart(7)} ${'USD'.padStart(11)}`); };
    head('TOTAL'); w(row('all', M.total));
    for (const [h, map] of [['BY OPERATION', M.byOp], ['BY CALLER (file:line)', M.byCaller], ['BY COLLECTION', M.byCollection]]) {
      head(h); for (const [k, t] of Object.entries(map).sort((a, b) => cost(b[1]).total - cost(a[1]).total)) w(row(k, t));
    }
    const s = M.storage; if (s.downloads || s.uploads || s.deletes || s.metadata || s.lists) { w(''); w(`STORAGE downloads ${s.downloads} (${s.downloadBytes} B), uploads ${s.uploads} (${s.uploadBytes} B), deletes ${s.deletes}, metadata ${s.metadata}, lists ${s.lists}`); }
  };
  M.db = createDb(M);
  M.install = (extra) => install(M, extra);
  M.uninstall = () => { if (M._restore) { M._restore(); M._restore = null; } };
  return M;
}

/* ───────────────────────────── helpers for projections and asserts ───────────────────────────── */
// d = a snapshot delta (m.since) or a report total; times = how many times per hour that same unit of work happens.
function perHour(d, times) {
  const o = { reads: (d.reads + (d.aggs || 0)) * times, bytes: d.bytes * times, writes: d.writes * times, deletes: d.deletes * times };
  o.usd = cost({ reads: o.reads, aggs: 0, bytes: o.bytes, writes: o.writes, deletes: o.deletes }).total; return o;
}
function assertMax(d, max, label) {
  const bad = [];
  for (const [k, v] of Object.entries(max)) { const got = k === 'reads' ? d.reads + (d.aggs || 0) : d[k]; if (got > v) bad.push(`${k} ${got} > ${v}`); }
  if (bad.length) throw new Error(`cost budget exceeded${label ? ' (' + label + ')' : ''}: ${bad.join(', ')}`);
}

/* ═════════════════════ generic wrapper: instrument ANY Admin-shaped Firestore (the fakes tests already have) ═════════════════════ */
const WRITE = new Set(['set', 'update', 'create', 'add']);
function wrap(db, M, label) {
  const raw = new WeakMap();                                 // proxy -> target, so proxies handed back in as arguments are unwrapped
  const ctxOfRaw = new WeakMap();                            // target -> the chain context it was created with (collection path, select, offset)
  const alsOf = () => (M._als ? M._als.getStore() : null);
  const unwrap = a => (a && typeof a === 'object' && raw.has(a) ? raw.get(a) : a);
  const pathOf = (ctx, extra) => (extra ? (ctx.col ? ctx.col + '/' + extra : extra) : ctx.col);
  const colOf = p => String(p || '(unknown)').split('/').filter((_, i) => i % 2 === 0).join('/');      // orders/o1/sheets/s1 -> orders/sheets
  const readSnap = (snap, ctx, caller) => {
    if (!snap || typeof snap !== 'object') return;
    if (Array.isArray(snap.docs)) {                                                  // QuerySnapshot
      let bytes = 0; for (const d of snap.docs) bytes += sizeOf(typeof d.data === 'function' ? d.data() : d);
      M.book('reads', Math.max(1, snap.docs.length) + (snap._skipped != null ? snap._skipped : (ctx.offset || 0)), bytes, { op: ctx.op, caller, col: colOf(ctx.col) }); return;
    }
    if (typeof snap.exists !== 'undefined' && typeof snap.data === 'function') {      // DocumentSnapshot
      M.book('reads', 1, snap.exists ? sizeOf(snap.data()) : 0, { op: ctx.op, caller, col: colOf(ctx.col || (snap.ref && snap.ref.path)) }); return;
    }
    if (typeof snap.data === 'function') {                                            // AggregateQuerySnapshot
      const d = snap.data() || {}; const n = Number(d.count) || 0;
      M.book('agg', Math.max(1, Math.ceil(n / 1000)), 0, { op: ctx.op, caller, col: colOf(ctx.col) });
    }
  };
  const mk = (target, ctx) => {
    const p = new Proxy(target, {
      get(t, prop) {
        const v = Reflect.get(t, prop, t);
        if (typeof v !== 'function' || typeof prop === 'symbol') return v;
        return function (...args) {
          const argCtx = args.map(a => (a && typeof a === 'object' && raw.has(a) ? ctxOfRaw.get(raw.get(a)) : null));
          args = args.map(unwrap);
          const c = Object.assign({}, ctx);
          if (ctx.kind === 'tx' && prop === 'get' && argCtx[0]) Object.assign(c, { col: argCtx[0].col, select: argCtx[0].select, offset: argCtx[0].offset });
          if ((prop === 'collection' || prop === 'doc' || prop === 'collectionGroup') && typeof args[0] === 'string') c.col = prop === 'collectionGroup' ? '*/' + args[0] : pathOf(ctx, args[0]);
          if (prop === 'offset') c.offset = Number(args[0]) || 0;
          if (prop === 'select') c.select = args.map(String);
          const caller = ['get', 'getAll', 'onSnapshot', 'count', 'stream', 'listDocuments', 'listCollections', 'set', 'update', 'create', 'add', 'delete', 'commit', 'runTransaction', 'batch'].includes(prop) ? callerOf() : null;
          const opName = (alsOf() || {}).op;
          const cc = Object.assign({ op: opName }, c);
          // reads and writes -----------------------------------------------------------------
          if (prop === 'runTransaction') {
            const fn = args[0];
            return v.call(t, (tx, ...r) => fn(mk(tx, { kind: 'tx', col: '' , op: opName }), ...r), ...args.slice(1));
          }
          if (prop === 'get' || prop === 'getAll') {
            const res = v.apply(t, args);
            const lastArg = args[args.length - 1], gaMask = prop === 'getAll' && lastArg && Array.isArray(lastArg.fieldMask) ? lastArg.fieldMask : null;
            const done = r => { if (prop === 'getAll') (Array.isArray(r) ? r : []).forEach((s, i) => readSnap(gaMask && s && s.exists ? { exists: true, data: () => pickFields(s.data(), gaMask), ref: s.ref } : s, Object.assign({}, cc, { col: s && s.ref && s.ref.path ? s.ref.path : ((argCtx[i] && argCtx[i].col) || cc.col) }), caller)); else readSnap(maskSnap(r, c), cc, caller); return r; };
            return res && typeof res.then === 'function' ? res.then(done) : done(res);
          }
          if (prop === 'onSnapshot') {
            let first = true; const cb = args[0];
            if (typeof cb === 'function') args[0] = function (snap, ...r) { try { const changes = snap && typeof snap.docChanges === 'function' ? snap.docChanges() : null; if (first || !changes) readSnap(snap, cc, caller); else { for (const ch of changes) M.book('reads', 1, ch.type === 'removed' ? 0 : sizeOf(ch.doc.data()), { op: cc.op, caller, col: colOf(cc.col) }); } } catch (e) { /* metering never breaks the code under test */ } first = false; return cb.call(this, snap, ...r); };
            return v.apply(t, args);
          }
          if (WRITE.has(prop) && ctx.kind !== 'query') {
            M.book('writes', 1, 0, { op: opName, caller, col: colOf(prop === 'add' ? cc.col : (cc.kind === 'tx' || cc.kind === 'batch' ? (args[0] && args[0].path) : cc.col)) });
            const res = v.apply(t, args); return res;
          }
          if (prop === 'delete') {
            M.book('deletes', 1, 0, { op: opName, caller, col: colOf(cc.kind === 'tx' || cc.kind === 'batch' ? (args[0] && args[0].path) : cc.col) });
            return v.apply(t, args);
          }
          if (prop === 'listDocuments' || prop === 'listCollections') {
            const res = v.apply(t, args); const done = r => { M.book('reads', Math.max(1, Array.isArray(r) ? r.length : 1), 0, { op: opName, caller, col: colOf(cc.col) }); return r; };
            return res && typeof res.then === 'function' ? res.then(done) : done(res);
          }
          if (prop === 'count') { const agg = v.apply(t, args); return mk(agg, Object.assign({}, c, { kind: 'agg' })); }
          if (prop === 'batch') return mk(v.apply(t, args), { kind: 'batch', col: '', op: opName });
          // everything else that returns an object keeps the chain (collection, doc, where, orderBy, limit, select, startAfter ...)
          const res = v.apply(t, args);
          if (res && typeof res === 'object' && typeof res.then !== 'function' && !Array.isArray(res)) {
            const kind = prop === 'doc' ? 'doc' : (ctx.kind === 'doc' && prop === 'collection') ? 'collection' : prop === 'collection' ? 'collection' : ctx.kind === 'tx' || ctx.kind === 'batch' ? ctx.kind : 'query';
            return mk(res, Object.assign({}, c, { kind }));
          }
          return res;
        };
      }
    });
    raw.set(p, target); ctxOfRaw.set(target, ctx);
    return p;
  };
  function maskSnap(r, c) {      // a select() field mask: only the selected fields count as bytes (the real server sends only those)
    if (!c.select || !c.select.length || !r) return r;
    const pick = o => { const out = {}; for (const f of c.select) { const parts = f.split('.'); let s = o, ok = true; for (const k of parts) { if (s && typeof s === 'object' && k in s) s = s[k]; else { ok = false; break; } } if (ok) { let d = out; parts.forEach((k, i) => { if (i === parts.length - 1) d[k] = s; else d = d[k] = d[k] || {}; }); } } return out; };
    if (Array.isArray(r.docs)) return { docs: r.docs.map(d => ({ data: () => pick(d.data()) })) };
    return r;
  }
  return mk(db, { kind: 'db', col: '', label });
}

/* ═════════════════════ in-memory Firestore (Admin SDK subset the functions use) ═════════════════════ */
class Timestamp {
  constructor(s, n) { this.seconds = s; this.nanoseconds = n || 0; }
  static now() { return Timestamp.fromMillis(Date.now()); }
  static fromMillis(ms) { return new Timestamp(Math.floor(ms / 1000), (ms % 1000) * 1e6); }
  static fromDate(d) { return Timestamp.fromMillis(d.getTime()); }
  toMillis() { return this.seconds * 1000 + Math.floor(this.nanoseconds / 1e6); }
  toDate() { return new Date(this.toMillis()); }
  isEqual(o) { return o instanceof Timestamp && o.seconds === this.seconds && o.nanoseconds === this.nanoseconds; }
  toJSON() { return { seconds: this.seconds, nanoseconds: this.nanoseconds }; }
}
const S = (tag, x) => ({ __sentinel: tag, x });
const FieldValue = { serverTimestamp: () => S('ts'), increment: n => S('inc', n), delete: () => S('del'), arrayUnion: (...a) => S('au', a), arrayRemove: (...a) => S('ar', a) };
const FieldPath = { documentId: () => ({ __docId: true }) };
const eqv = (a, b) => (a instanceof Timestamp && b instanceof Timestamp) ? a.toMillis() === b.toMillis() : JSON.stringify(a) === JSON.stringify(b);
const cmpv = (a, b) => { const va = a instanceof Timestamp ? a.toMillis() : a, vb = b instanceof Timestamp ? b.toMillis() : b; return va < vb ? -1 : va > vb ? 1 : 0; };
const getPath = (o, f) => { let s = o; for (const k of String(f).split('.')) { if (s && typeof s === 'object' && k in s) s = s[k]; else return undefined; } return s; };
function setPath(o, f, v) { const ks = String(f).split('.'); let s = o; ks.forEach((k, i) => { if (i === ks.length - 1) { if (v && v.__sentinel === 'del') delete s[k]; else s[k] = v; } else { if (!s[k] || typeof s[k] !== 'object') s[k] = {}; s = s[k]; } }); }
const pickFields = (o, fields) => { const out = {}; for (const f of fields) { const v = getPath(o, f); if (v !== undefined) setPath(out, f, deepClone(v)); } return out; };
const deepClone = v => v instanceof Timestamp ? new Timestamp(v.seconds, v.nanoseconds) : Array.isArray(v) ? v.map(deepClone) : v && typeof v === 'object' && !v.__sentinel ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, deepClone(x)])) : v;
const isMapV = v => v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Timestamp) && !v.__sentinel;

function applyWrite(prev, patch, o, now) {
  const resolve = (cur, v) => {
    if (v && v.__sentinel) {
      switch (v.__sentinel) {
        case 'ts': return Timestamp.fromMillis(now());
        case 'inc': return (Number(cur) || 0) + v.x;
        case 'au': { const a = Array.isArray(cur) ? cur.slice() : []; for (const x of v.x) if (!a.some(y => eqv(x, y))) a.push(x); return a; }
        case 'ar': return (Array.isArray(cur) ? cur : []).filter(y => !v.x.some(x => eqv(x, y)));
        default: return v;
      }
    }
    return deepClone(v);
  };
  const walk = (cur, p, merge) => {
    const out = merge && isMapV(cur) ? deepClone(cur) : {};
    for (const [k, v] of Object.entries(p)) {
      if (v && v.__sentinel === 'del') delete out[k];
      else if (isMapV(v)) out[k] = walk(isMapV(out[k]) ? out[k] : null, v, merge);
      else out[k] = resolve(out[k], v);
    }
    return out;
  };
  if (o && o.__update) {       // update(): dotted paths, fields replaced at their path
    const out = deepClone(prev || {});
    for (const [f, v] of Object.entries(patch)) { if (f.includes('.')) { const cur = getPath(out, f); setPath(out, f, v && v.__sentinel && v.__sentinel !== 'del' ? resolve(cur, v) : deepClone(v)); } else if (v && v.__sentinel === 'del') delete out[f]; else out[f] = resolve(out[f], v); }
    return out;
  }
  if (o && o.mergeFields) { const out = deepClone(prev || {}); for (const f of o.mergeFields) { const v = getPath(patch, f); setPath(out, f, v && v.__sentinel ? resolve(getPath(out, f), v) : deepClone(v)); } return out; }
  return walk(prev, patch, !!(o && o.merge));
}

function createDb(M) {
  const docs = new Map();                                        // 'orders/o1' -> data
  const times = new Map();                                       // 'orders/o1' -> { create, update } ms
  const listeners = new Set();
  const now = () => Date.now();
  let idn = 0;
  const autoId = () => 'auto' + String(++idn).padStart(6, '0') + Math.random().toString(36).slice(2, 8);
  const depth = p => p.split('/').length;
  const colOfPath = p => p.split('/').slice(0, -1).join('/');
  const idOf = p => p.split('/').pop();

  function notify(p) { for (const l of [...listeners]) l(p); }
  function put(p, data) { const had = docs.has(p); docs.set(p, data); const t = times.get(p) || { create: now() }; t.update = now(); times.set(p, t); notify(p); return had; }
  function drop(p) { if (docs.delete(p)) { times.delete(p); notify(p); } }

  const snap = (p, withData = true) => {
    const d = docs.get(p), exists = docs.has(p), t = times.get(p);
    return { id: idOf(p), exists, ref: docRef(p), data: () => (exists && withData ? deepClone(d) : undefined), get: f => (exists ? getPath(d, f) : undefined), createTime: t && Timestamp.fromMillis(t.create), updateTime: t && Timestamp.fromMillis(t.update), readTime: Timestamp.now() };
  };

  // getAll(...refs, { fieldMask: [...] }): only the masked fields come back (and cross the wire)
  function getAllMasked(refs) {
    let mask = null; if (refs.length && refs[refs.length - 1] && refs[refs.length - 1].fieldMask) mask = refs.pop().fieldMask;
    return refs.map(r => { const s = snap(r.path); if (mask && s.exists) { const full = docs.get(r.path); s.data = () => pickFields(full, mask); } return s; });
  }

  /* queries ---------------------------------------------------------------------------- */
  function Query(colPath, group, st) {
    st = Object.assign({ filters: [], order: [], lim: null, limLast: false, off: 0, start: null, end: null, sel: null }, st);
    const q = {
      _colPath: colPath, _st: st,
      where(f, op, v) { return Query(colPath, group, Object.assign({}, st, { filters: st.filters.concat([[f, op, v]]) })); },
      orderBy(f, dir) { return Query(colPath, group, Object.assign({}, st, { order: st.order.concat([[f, dir || 'asc']]) })); },
      limit(n) { return Query(colPath, group, Object.assign({}, st, { lim: n, limLast: false })); },
      limitToLast(n) { return Query(colPath, group, Object.assign({}, st, { lim: n, limLast: true })); },
      offset(n) { return Query(colPath, group, Object.assign({}, st, { off: n })); },
      select(...f) { return Query(colPath, group, Object.assign({}, st, { sel: f })); },
      startAt(...v) { return Query(colPath, group, Object.assign({}, st, { start: { v, incl: true } })); },
      startAfter(...v) { return Query(colPath, group, Object.assign({}, st, { start: { v, incl: false } })); },
      endAt(...v) { return Query(colPath, group, Object.assign({}, st, { end: { v, incl: true } })); },
      endBefore(...v) { return Query(colPath, group, Object.assign({}, st, { end: { v, incl: false } })); },
      _run() {
        if (M.queryGuard) M.queryGuard({ col: colPath, group, filters: st.filters, order: st.order });
        let list = [...docs.keys()].filter(p => group ? idOf(colOfPath(p)) === colPath && depth(p) % 2 === 0 : colOfPath(p) === colPath);
        const val = (p, f) => (f && f.__docId) ? idOf(p) : getPath(docs.get(p), f);
        for (const [f, op, v] of st.filters) list = list.filter(p => {
          const x = val(p, f); if (x === undefined && op !== '!=' && op !== 'not-in') return false;
          switch (op) {
            case '==': return x !== undefined && eqv(x, v);
            case '!=': return x !== undefined && !eqv(x, v);
            case '<': return cmpv(x, v) < 0; case '<=': return cmpv(x, v) <= 0; case '>': return cmpv(x, v) > 0; case '>=': return cmpv(x, v) >= 0;
            case 'in': return v.some(y => eqv(x, y)); case 'not-in': return x !== undefined && !v.some(y => eqv(x, y));
            case 'array-contains': return Array.isArray(x) && x.some(y => eqv(y, v));
            case 'array-contains-any': return Array.isArray(x) && x.some(y => v.some(z => eqv(y, z)));
            default: throw new Error('meter fake: unsupported operator ' + op);
          }
        });
        const ord = st.order.length ? st.order : [[{ __docId: true }, 'asc']];
        if (st.order.length) list = list.filter(p => st.order.every(([f]) => val(p, f) !== undefined));
        const sortFn = (a, b) => { for (const [f, dir] of ord) { const c = cmpv(val(a, f), val(b, f)); if (c) return dir === 'desc' ? -c : c; } return 0; };
        list.sort(sortFn);
        const vals = (s) => s.v.map(x => (x && x.ref ? idOf(x.ref.path) : x));
        if (st.start) list = list.filter(p => { const c = ord.reduce((r, [f, dir], i) => r || (st.start.v[i] === undefined ? 0 : (dir === 'desc' ? -1 : 1) * cmpv(val(p, f), vals(st.start)[i])), 0); return st.start.incl ? c >= 0 : c > 0; });
        if (st.end) list = list.filter(p => { const c = ord.reduce((r, [f, dir], i) => r || (st.end.v[i] === undefined ? 0 : (dir === 'desc' ? -1 : 1) * cmpv(val(p, f), vals(st.end)[i])), 0); return st.end.incl ? c <= 0 : c < 0; });
        const skipped = Math.min(st.off, list.length); list = list.slice(st.off);
        if (st.lim != null) list = st.limLast ? list.slice(-st.lim) : list.slice(0, st.lim);
        return { list, skipped };
      },
      async get() {
        const { list, skipped } = q._run();
        const ds = list.map(p => { const s = snap(p); if (st.sel) { const full = deepClone(docs.get(p)), out = {}; for (const f of st.sel) { const v = getPath(full, f); if (v !== undefined) setPath(out, f, v); } s.data = () => deepClone(out); } return s; });
        // offset(n): the skipped documents are billed as reads although they are not returned
        return Object.assign({}, { docs: ds, size: ds.length, empty: !ds.length, forEach: fn => ds.forEach(fn), docChanges: () => ds.map(d => ({ type: 'added', doc: d })), readTime: Timestamp.now(), _skipped: skipped });
      },
      count() { return { async get() { const { list } = q._run(); return { data: () => ({ count: list.length }) }; } }; },
      stream() { const { Readable } = require('stream'); const { list } = q._run(); return Readable.from(list.map(p => snap(p))); },
      onSnapshot(cb, errCb) {
        let prev = new Map(), first = true;
        const fire = async () => {
          const { list } = q._run(); const next = new Map(list.map(p => [p, JSON.stringify(docs.get(p))])); const changes = [];
          for (const [p, s] of next) { if (!prev.has(p)) changes.push({ type: 'added', doc: snap(p) }); else if (prev.get(p) !== s) changes.push({ type: 'modified', doc: snap(p) }); }
          for (const p of prev.keys()) if (!next.has(p)) changes.push({ type: 'removed', doc: snap(p, false) });
          const ds = list.map(p => snap(p)); prev = next;
          if (first || changes.length) { const ch = first ? ds.map(d => ({ type: 'added', doc: d })) : changes; first = false; cb({ docs: ds, size: ds.length, empty: !ds.length, forEach: f => ds.forEach(f), docChanges: () => ch }); }
        };
        const l = () => { Promise.resolve().then(fire).catch(e => errCb && errCb(e)); }; listeners.add(l); l();
        return () => listeners.delete(l);
      }
    };
    return q;
  }

  /* references --------------------------------------------------------------------------- */
  function collRef(cp) {
    const q = Query(cp, false);
    return Object.assign(q, {
      id: idOf(cp), path: cp, get parent() { return cp.includes('/') ? docRef(colOfPath(cp)) : null; },
      doc: id => docRef(cp + '/' + (id || autoId())),
      async add(data) { const r = docRef(cp + '/' + autoId()); await r.create(data); return r; },
      async listDocuments() { const ids = new Set(); for (const p of docs.keys()) if (p.startsWith(cp + '/') && depth(p) >= depth(cp) + 1) ids.add(p.split('/').slice(0, depth(cp) + 1).join('/')); return [...ids].map(docRef); }
    });
  }
  function docRef(p) {
    return {
      id: idOf(p), path: p, get parent() { return collRef(colOfPath(p)); }, firestore: db,
      collection: n => collRef(p + '/' + n),
      async get() { return snap(p); },
      async set(data, o) { put(p, applyWrite(docs.get(p), data, o, now)); return { writeTime: Timestamp.now() }; },
      async create(data) { if (docs.has(p)) { const e = new Error('6 ALREADY_EXISTS: Document already exists: ' + p); e.code = 6; throw e; } put(p, applyWrite(null, data, null, now)); return { writeTime: Timestamp.now() }; },
      async update(...a) {
        if (!docs.has(p)) { const e = new Error('5 NOT_FOUND: No document to update: ' + p); e.code = 5; throw e; }
        let patch = a[0]; if (typeof patch === 'string') { patch = {}; for (let i = 0; i < a.length; i += 2) patch[a[i]] = a[i + 1]; }
        put(p, applyWrite(docs.get(p), patch, { __update: true }, now)); return { writeTime: Timestamp.now() };
      },
      async delete() { drop(p); return { writeTime: Timestamp.now() }; },
      onSnapshot(cb, errCb) {
        let prev = null, first = true;
        const l = () => Promise.resolve().then(() => { const cur = JSON.stringify(docs.get(p)); if (first || cur !== prev) { prev = cur; const s = snap(p); const ch = [{ type: first ? 'added' : (docs.has(p) ? 'modified' : 'removed'), doc: s }]; first = false; cb(Object.assign(s, { docChanges: () => ch })); } }).catch(e => errCb && errCb(e));
        listeners.add(l); l(); return () => listeners.delete(l);
      },
      async listCollections() { const ids = new Set(); for (const q of docs.keys()) if (q.startsWith(p + '/')) ids.add(q.split('/')[depth(p)]); return [...ids].map(n => collRef(p + '/' + n)); }
    };
  }

  /* writes in a transaction / batch ----------------------------------------------------------- */
  const pending = (ops) => {
    const o = {
      set: (r, d, op) => { ops.push(() => put(r.path, applyWrite(docs.get(r.path), d, op, now))); return o; },
      create: (r, d) => { ops.push(() => { if (docs.has(r.path)) throw Object.assign(new Error('6 ALREADY_EXISTS: ' + r.path), { code: 6 }); put(r.path, applyWrite(null, d, null, now)); }); return o; },
      update: (r, ...a) => { ops.push(() => { if (!docs.has(r.path)) throw Object.assign(new Error('5 NOT_FOUND: ' + r.path), { code: 5 }); let patch = a[0]; if (typeof patch === 'string') { patch = {}; for (let i = 0; i < a.length; i += 2) patch[a[i]] = a[i + 1]; } put(r.path, applyWrite(docs.get(r.path), patch, { __update: true }, now)); }); return o; },
      delete: r => { ops.push(() => drop(r.path)); return o; }
    };
    return o;
  };

  const db = {
    collection: n => collRef(n),
    doc: p => docRef(p),
    collectionGroup: id => Query(id, true),
    async getAll(...refs) { return getAllMasked(refs); },
    batch() { const ops = []; const b = pending(ops); b.commit = async () => { const n = ops.length; ops.splice(0).forEach(f => f()); return Array(n).fill({ writeTime: Timestamp.now() }); }; return b; },
    async runTransaction(fn) {
      const ops = [];
      const tx = Object.assign({}, pending(ops), {
        get: async x => (x && x._run ? x.get() : snap(x.path)),
        getAll: async (...refs) => getAllMasked(refs)
      });
      const out = await fn(tx); ops.splice(0).forEach(f => f()); return out;
    },
    settings() { }, terminate: async () => { },
    async listCollections() { const ids = new Set(); for (const p of docs.keys()) ids.add(p.split('/')[0]); return [...ids].map(collRef); },
    /* test helpers, never billed */
    seed(map) { for (const [p, d] of Object.entries(map || {})) docs.set(p, deepClone(d)), times.set(p, { create: now(), update: now() }); return db; },
    dump() { return Object.fromEntries([...docs].map(([p, d]) => [p, deepClone(d)])); },
    has: p => docs.has(p), size: () => docs.size, docs
  };
  // the metered face: every call through it is booked on M; db.seed/dump/has/docs stay on the raw side
  M._raw = db; M._als = M._als || new AsyncLocalStorage();
  return wrapRoot(db, M);
}
function wrapRoot(db, M) {
  M._als = M._als || new AsyncLocalStorage();
  const w = wrap(db, M, 'in-memory');
  return new Proxy(w, { get(t, p) { if (['seed', 'dump', 'has', 'size', 'docs'].includes(p)) return db[p]; return t[p]; } });
}

/* A production Firestore refuses a query that needs a composite index the project does not have; the fallback paths in the code
   (catch, then scan the whole collection) are then what actually runs and what the invoice shows. Switch it on with
   m.queryGuard = meter.missingCompositeIndex(['EtsyMail_Threads|status,salesCompletedAt']) : a filter plus an orderBy on another
   field, or an inequality (!=, <, >, not-in) plus an orderBy on another field, throws 9 FAILED_PRECONDITION unless listed. */
function missingCompositeIndex(allowed) {
  const ok = new Set(allowed || []);
  return q => {
    const ofields = q.order.map(([f]) => String(f)).filter(f => f !== '[object Object]');
    const ffields = q.filters.map(([f]) => String(f));
    const ineq = q.filters.filter(([, op]) => ['!=', '<', '<=', '>', '>=', 'not-in'].includes(op)).map(([f]) => String(f));
    const needs = (q.filters.length && ofields.some(f => !ffields.includes(f))) || ineq.some(f => ofields.length && ofields[0] !== f);
    if (!needs) return;
    const key = q.col + '|' + [...new Set(ffields.concat(ofields))].join(',');
    if (ok.has(key)) return;
    const e = new Error('9 FAILED_PRECONDITION: The query requires an index (' + key + ')'); e.code = 9; throw e;
  };
}

/* ═════════════════════ in-memory Storage bucket (counts bytes in and out) ═════════════════════ */
function createStorage(M) {
  const files = new Map();
  const file = name => ({
    name,
    async download() { const b = files.get(name); if (!b) throw Object.assign(new Error('No such object: ' + name), { code: 404 }); M.storage.downloads++; M.storage.downloadBytes += b.length; return [b]; },
    createReadStream() { const { Readable } = require('stream'); const b = files.get(name) || Buffer.alloc(0); M.storage.downloads++; M.storage.downloadBytes += b.length; return Readable.from([b]); },
    async save(data) { const b = Buffer.isBuffer(data) ? data : Buffer.from(String(data)); files.set(name, b); M.storage.uploads++; M.storage.uploadBytes += b.length; },
    async exists() { M.storage.metadata++; return [files.has(name)]; },
    async getMetadata() { M.storage.metadata++; const b = files.get(name); return [{ name, size: String(b ? b.length : 0) }]; },
    async delete() { M.storage.deletes++; files.delete(name); },
    async getSignedUrl() { M.storage.metadata++; return ['https://storage.example/signed/' + encodeURIComponent(name)]; },
    async makePublic() { }, async setMetadata() { M.storage.metadata++; }, publicUrl: () => 'https://storage.example/' + name
  });
  const bucket = { name: 'test-bucket', file, async getFiles(o) { M.storage.lists++; const pre = (o && o.prefix) || ''; return [[...files.keys()].filter(n => n.startsWith(pre)).map(file)]; }, async upload() { } };
  return { bucket: () => bucket, files };
}

/* ═════════════════════ install(): make require('./firebaseAdmin') return this fake ═════════════════════ */
function install(M, extra) {
  const store = createStorage(M); M._storage = store;
  const admin = Object.assign({
    apps: [{}],
    firestore: Object.assign(() => M.db, { FieldValue, Timestamp, FieldPath }),
    storage: () => ({ bucket: store.bucket }),
    auth: () => ({ verifyIdToken: async () => { throw new Error('meter fake: auth not available'); } }),
    initializeApp() { }, credential: { cert: () => ({}) },
    CORS_ORIGINS: [], CORS_CONFIG: [], applyBucketCors: async () => { }, DEFAULT_BUCKET: 'test-bucket'
  }, extra || {});
  const realLoad = Module._load;
  Module._load = function (req, ...rest) {
    if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return admin;
    if (req === 'firebase-admin' || req === 'firebase-admin/firestore') return req === 'firebase-admin' ? admin : { FieldValue, Timestamp, FieldPath, getFirestore: () => M.db };
    return realLoad.call(this, req, ...rest);
  };
  M._restore = () => { Module._load = realLoad; };
  return admin;
}

module.exports = { create, missingCompositeIndex, wrap: (db, M, label) => { M._als = M._als || new AsyncLocalStorage(); return wrap(db, M, label); }, createDb, perHour, assertMax, cost, sizeOf, PRICES, Timestamp, FieldValue, FieldPath, GIB };
