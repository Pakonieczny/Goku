// Node-side shared store with atomic commits (serialised by the event loop).
class Store {
  constructor() { this.docs = new Map(); this.version = 0; this.dead = new Set(); }
  ts() { return { __ts: Date.now() }; }
  resolve(data, prev) { const out = {}; for (const k in data) { const v = data[k]; if (v && v.__sv) { if (v.__sv === 'serverTimestamp') out[k] = this.ts(); else if (v.__sv === 'delete') { /* omit */ } else if (v.__sv === 'increment') out[k] = (prev && typeof prev[k] === 'number' ? prev[k] : 0) + v.n; else if (v.__sv === 'arrayUnion') { const a = (prev && Array.isArray(prev[k]) ? prev[k] : []).slice(); v.v.forEach(x => { if (!a.some(y => JSON.stringify(y) === JSON.stringify(x))) a.push(x); }); out[k] = a; } else out[k] = prev && prev[k]; } else out[k] = v; } return out; }
  put(path, data, merge) { const cur = this.docs.get(path); const prev = cur ? cur.d : null; let nd; if (merge && prev) { nd = Object.assign({}, prev); const res = this.resolve(data, prev); for (const k in data) { if (data[k] && data[k].__sv === 'delete') delete nd[k]; else nd[k] = res[k]; } } else nd = this.resolve(data, prev); this.docs.set(path, { d: nd, v: (cur ? cur.v : 0) + 1 }); this.version++; }
  op(op, p) {
    if (op === 'get') { const c = this.docs.get(p.path); return { d: c ? c.d : null, v: c ? c.v : 0 }; }
    if (op === 'set') { this.put(p.path, p.data, p.merge); return {}; }
    if (op === 'update') { const c = this.docs.get(p.path); if (!c) return { err: 'nf' }; const nd = JSON.parse(JSON.stringify(c.d)); for (const k in p.data) { const v = p.data[k]; const parts = k.split('.'); let o = nd; for (let i = 0; i < parts.length - 1; i++) { if (!o[parts[i]] || typeof o[parts[i]] !== 'object') o[parts[i]] = {}; o = o[parts[i]]; } const last = parts[parts.length - 1]; if (v && v.__sv) { if (v.__sv === 'delete') delete o[last]; else if (v.__sv === 'serverTimestamp') o[last] = this.ts(); else if (v.__sv === 'increment') o[last] = (typeof o[last] === 'number' ? o[last] : 0) + v.n; else o[last] = v; } else o[last] = v; } this.docs.set(p.path, { d: nd, v: c.v + 1 }); this.version++; return {}; }
    if (op === 'delete') { if (this.docs.delete(p.path)) this.version++; return {}; }
    if (op === 'commit') { for (const path in p.reads) { const c = this.docs.get(path); if ((c ? c.v : 0) !== p.reads[path]) return { ok: false }; } for (const w of p.writes) { if (w.op === 'delete') { this.docs.delete(w.path); this.version++; } else if (w.op === 'update') this.op('update', w); else this.put(w.path, w.data, w.merge); } return { ok: true }; }
    if (op === 'changes') return { v: this.version };
    if (op === 'query') {
      const base = p.path + '/'; let rows = [];
      for (const [path, c] of this.docs) { if (path.startsWith(base) && !path.slice(base.length).includes('/')) rows.push({ id: path.slice(base.length), d: c.d, v: c.v }); }
      rows = rows.filter(r => p.f.every(f => { const val = f.field === '__name__' ? r.id : (f.val && f.val.__ts != null ? null : r.d[f.field]); const fv = f.val && f.val.__ts != null ? f.val.__ts : f.val; let x = f.field === '__name__' ? r.id : r.d[f.field]; if (x && x.__ts != null) x = x.__ts; switch (f.op) { case '==': return x === fv; case '>=': return x >= fv; case '>': return x > fv; case '<': return x < fv; case '<=': return x <= fv; case 'in': return Array.isArray(fv) && fv.includes(x); default: return true; } }));
      p.o.forEach(o => rows.sort((a, b) => { let x = a.d[o.field], y = b.d[o.field]; if (x && x.__ts != null) x = x.__ts; if (y && y.__ts != null) y = y.__ts; return (x > y ? 1 : x < y ? -1 : 0) * (o.dir === 'desc' ? -1 : 1); }));
      if (p.l) rows = rows.slice(0, p.l);
      return { docs: rows };
    }
    return {};
  }
}
module.exports = { Store };
