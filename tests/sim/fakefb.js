// In-page fake of firebase (app/auth/firestore/storage) backed by a Node-side store via window.__fs(op, payload).
(function () {
  const call = (op, p) => window.__fs(op, JSON.stringify(p || {})).then(r => JSON.parse(r));
  class Timestamp { constructor(ms) { this._ms = ms; this.seconds = Math.floor(ms / 1000); this.nanoseconds = (ms % 1000) * 1e6; } toMillis() { return this._ms; } toDate() { return new Date(this._ms); } static fromMillis(ms) { return new Timestamp(ms); } static now() { return new Timestamp(Date.now()); } }
  const rev = (o) => { // JSON from node -> app values (Timestamps)
    if (o && typeof o === 'object') { if (o.__ts != null) return new Timestamp(o.__ts); if (Array.isArray(o)) return o.map(rev); const r = {}; for (const k in o) r[k] = rev(o[k]); return r; } return o; };
  const SV = { __sv: 'serverTimestamp' }, DEL = { __sv: 'delete' };
  const FieldValue = { serverTimestamp: () => SV, delete: () => DEL, increment: (n) => ({ __sv: 'increment', n }), arrayUnion: (...v) => ({ __sv: 'arrayUnion', v }), arrayRemove: (...v) => ({ __sv: 'arrayRemove', v }) };
  const enc = (o) => { if (o instanceof Timestamp) return { __ts: o._ms }; if (o && typeof o === 'object') { if (o.__sv) return o; if (Array.isArray(o)) return o.map(enc); const r = {}; for (const k in o) r[k] = enc(o[k]); return r; } return o; };
  const DOCID = { __docId: true };
  class DocSnap { constructor(path, d, v) { this.id = path.split('/').pop(); this.ref = new DocRef(path); this._d = d; this.exists = d != null; this.metadata = { fromCache: false, hasPendingWrites: false }; this._v = v; } data() { return this._d == null ? undefined : rev(this._d); } get(f) { const d = this.data(); return d ? d[f] : undefined; } }
  class QSnap { constructor(docs) { this.docs = docs; this.size = docs.length; this.empty = !docs.length; this.metadata = { fromCache: false, hasPendingWrites: false }; } forEach(f) { this.docs.forEach(f); } docChanges() { return this.docs.map(d => ({ type: 'added', doc: d })); } }
  const subs = new Set();
  let seen = 0;
  async function pump() { try { const r = await call('changes', { since: seen }); if (r.v === seen) return; seen = r.v; for (const s of Array.from(subs)) s.refresh(); } catch (_) {} }
  setInterval(pump, 120);
  class DocRef {
    constructor(path) { this.path = path; this.id = path.split('/').pop(); this.parent = { path: path.split('/').slice(0, -1).join('/') }; }
    async get() { const r = await call('get', { path: this.path }); return new DocSnap(this.path, r.d, r.v); }
    async set(data, opts) { await call('set', { path: this.path, data: enc(data), merge: !!(opts && opts.merge) }); }
    async update(data) { const r = await call('update', { path: this.path, data: enc(data) }); if (r.err) { const e = new Error('not-found'); e.code = 'not-found'; throw e; } }
    async delete() { await call('delete', { path: this.path }); }
    onSnapshot(...a) { const next = typeof a[0] === 'function' ? a[0] : a[1], err = typeof a[0] === 'function' ? a[1] : a[2]; let last = null; const me = this; const sub = { async refresh() { try { const r = await call('get', { path: me.path }); const sig = JSON.stringify([r.d, r.v]); if (sig !== last) { last = sig; next(new DocSnap(me.path, r.d, r.v)); } } catch (e) { err && err(e); } } }; subs.add(sub); sub.refresh(); return () => subs.delete(sub); }
    collection(n) { return new CollRef(this.path + '/' + n); }
  }
  class Query {
    constructor(path, f, o, l) { this.path = path; this.f = f || []; this.o = o || []; this.l = l || 0; }
    where(field, op, val) { return new Query(this.path, this.f.concat([{ field: field === DOCID ? '__name__' : field, op, val: enc(val) }]), this.o, this.l); }
    orderBy(field, dir) { return new Query(this.path, this.f, this.o.concat([{ field, dir: dir || 'asc' }]), this.l); }
    limit(n) { return new Query(this.path, this.f, this.o, n); }
    startAfter() { return this; }
    async get() { const r = await call('query', { path: this.path, f: this.f, o: this.o, l: this.l }); return new QSnap(r.docs.map(x => new DocSnap(this.path + '/' + x.id, x.d, x.v))); }
    onSnapshot(...a) { const next = typeof a[0] === 'function' ? a[0] : a[1], err = typeof a[0] === 'function' ? a[1] : a[2]; let last = null; const me = this; const sub = { async refresh() { try { const r = await call('query', { path: me.path, f: me.f, o: me.o, l: me.l }); const sig = JSON.stringify(r.docs); if (sig !== last) { last = sig; next(new QSnap(r.docs.map(x => new DocSnap(me.path + '/' + x.id, x.d, x.v)))); } } catch (e) { err && err(e); } } }; subs.add(sub); sub.refresh(); return () => subs.delete(sub); }
  }
  class CollRef extends Query { doc(id) { return new DocRef(this.path + '/' + (id || ('auto' + Math.random().toString(36).slice(2, 10)))); } async add(d) { const r = this.doc(); await r.set(d); return r; } }
  const fs = {
    collection: (n) => new CollRef(n),
    doc: (p) => new DocRef(p),
    batch() { const ops = []; return { set(r, d, o) { ops.push(['set', r, d, o]); return this; }, update(r, d) { ops.push(['update', r, d]); return this; }, delete(r) { ops.push(['delete', r]); return this; }, async commit() { for (const o of ops) { if (o[0] === 'set') await o[1].set(o[2], o[3]); else if (o[0] === 'update') await o[1].update(o[2]); else await o[1].delete(); } } }; },
    async runTransaction(fn) {
      for (let attempt = 0; attempt < 6; attempt++) {
        const reads = {}, writes = [];
        const tx = {
          async get(ref) { const r = await call('get', { path: ref.path }); reads[ref.path] = r.v; return new DocSnap(ref.path, r.d, r.v); },
          set(ref, d, o) { writes.push({ op: 'set', path: ref.path, data: enc(d), merge: !!(o && o.merge) }); return tx; },
          update(ref, d) { writes.push({ op: 'update', path: ref.path, data: enc(d) }); return tx; },
          delete(ref) { writes.push({ op: 'delete', path: ref.path }); return tx; }
        };
        const out = await fn(tx);
        const r = await call('commit', { reads, writes });
        if (r.ok) return out;
      }
      const e = new Error('aborted'); e.code = 'aborted'; throw e;
    },
    enablePersistence() { return Promise.resolve(); }, settings() {}, clearPersistence() { return Promise.resolve(); }
  };
  const fsFn = () => fs; fsFn.FieldValue = FieldValue; fsFn.Timestamp = Timestamp; fsFn.FieldPath = { documentId: () => DOCID };
  const user = { uid: 'u-sim', isAnonymous: true };
  const auth = { currentUser: user, onAuthStateChanged(cb) { setTimeout(() => cb(user), 20); return () => {}; }, signInAnonymously() { return Promise.resolve({ user }); }, onIdTokenChanged(cb) { setTimeout(() => cb(user), 20); return () => {}; } };
  // Storage: window.__storage maps a folder path -> { prefixes: [names], items: [names] }; every listAll is logged in window.__listCalls.
  window.__storage = window.__storage || {}; window.__listCalls = window.__listCalls || [];
  const sref = (path) => { path = path || ''; return { name: path.split('/').pop(), fullPath: path, child: (n) => sref(path ? path + '/' + n : n), put: () => Promise.resolve({ ref: sref(path) }), getDownloadURL: () => Promise.resolve('about:blank'),
    listAll: () => { window.__listCalls.push(path); const e = window.__storage[path] || { prefixes: [], items: [] }; return Promise.resolve({ prefixes: e.prefixes.map(n => sref(path + '/' + n)), items: e.items.map(n => sref(path + '/' + n)) }); }, delete: () => Promise.resolve() }; };
  window.firebase = { initializeApp() {}, apps: [1], auth: () => auth, firestore: fsFn, storage: () => ({ ref: (p) => sref(p) }), app: () => ({}) };
  window.firebase.firestore.FieldValue = FieldValue; window.firebase.firestore.Timestamp = Timestamp; window.firebase.firestore.FieldPath = fsFn.FieldPath;
  window.firebase.auth.Auth = { Persistence: { LOCAL: 'local', SESSION: 'session', NONE: 'none' } };
})();
