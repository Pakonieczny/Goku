// Firestore cost of the Shopify queue endpoint: ?op=status polling (every open tab, 6-60 s) and the budget-blocked drain kick.
// Real shopifyListingUpload.js against the in-memory metered Firestore (tests/cost/_meter.js); no network, no Shopify.
//   node tests/cost/ru-G2-shopify-status.js
"use strict";
const assert = require("assert");
const path = require("path");
const meter = require("./_meter.js");

const FN = path.join(__dirname, "../../netlify/functions/shopifyListingUpload.js");
const m = meter.create();
const realNow = Date.now.bind(Date);
let skew = 0;
Date.now = () => realNow() + skew;           // every module (and the fake Firestore) sees the same movable clock
const advance = (ms) => { skew += ms; };
m.install();
const realFetch = global.fetch;
global.fetch = async (u) => { throw new Error("no network in tests: " + u); };   // nothing here may reach Shopify (or anything else)
const realWarn = console.warn; console.warn = () => {};                              // the failing fake-Shopify calls warn once per job
delete process.env.URL; delete process.env.DEPLOY_PRIME_URL; delete process.env.DEPLOY_URL;   // no background-drain trigger in tests
delete process.env.SHOPIFY_STORE;
const H = 3600 * 1000;

let failingRev = false;                      // lets a test make the queueRev read fail (fail-open path)
function loadFresh() { delete require.cache[FN]; return require(FN); }
function seedQueue(opts) {
  const o = Object.assign({ done: 3000, queued: 56, failed: 3, budgetUsed: 940 }, opts);
  const seed = {}, now = Date.now();
  const doc = (status, i, extra) => Object.assign({
    status, createdAtMs: now - 1e7 - i * 1000, variantCount: 40, tier: 1, priceMin: 5, priceMax: 9, handle: "h" + i,
    payload: { title: "Charm " + i, category: "Beady_Necklace", descriptionHtml: "x".repeat(3000), images: [{ src: "https://x/" + i }] },
    matrix: { variants: new Array(40).fill({ options: { metal: "Silver", engrave: "No" }, price: 5, sku: "SKU-123456" }) }
  }, extra || {});
  for (let i = 0; i < o.done; i++) seed["Shopify_Upload_Queue/done" + i] = doc("DONE", i + 1000);
  for (let i = 0; i < o.queued; i++) seed["Shopify_Upload_Queue/q" + i] = doc("QUEUED", i, { createdAtMs: now - 1e5 + i, handle: null });
  for (let i = 0; i < o.failed; i++) seed["Shopify_Upload_Queue/f" + i] = doc("FAILED", i + 500, { error: "boom" });
  seed["Shopify_Upload_Meta/budget"] = { entries: [{ t: now - 3600e3, n: o.budgetUsed, j: "x" }], used: o.budgetUsed };
  return seed;
}
const get = (fn, q) => fn.handler({ httpMethod: "GET", queryStringParameters: q, headers: {} });
const post = (fn, body) => fn.handler({ httpMethod: "POST", queryStringParameters: {}, headers: {}, body: JSON.stringify(body) });
async function measure(name, fn) {
  const a = m.snapshot();
  const r = await m.op(name, fn);
  const d = m.since(a);
  return { r, reads: d.reads + d.aggs, aggs: d.aggs, writes: d.writes };
}
const body = (r) => JSON.parse(r.body);

const queue = [];
const t = (name, fn) => queue.push([name, fn]);

t("status: cold answer is the full read; the next polls cost one read (the queueRev doc), not ~150", async () => {
  m.db.docs.clear(); m.db.seed(seedQueue()); skew += 10 * H;
  const fn = loadFresh();
  const cold = await measure("status.cold", () => get(fn, { op: "status", recent: "150" }));
  console.log(`  cold  recent=150: ${cold.reads} reads (${cold.aggs} of them count())`);
  assert(cold.reads >= 150, "cold read is the real thing");
  const first = body(cold.r);
  assert.strictEqual(first.recent.length, 150); assert.strictEqual(first.queued, 56); assert.strictEqual(first.done, 3000); assert.strictEqual(first.failed, 3);

  advance(6000);                                               // the 6 s watcher poll
  const warm = await measure("status.warm150", () => get(fn, { op: "status", recent: "150" }));
  console.log(`  warm  recent=150: ${warm.reads} read, ${warm.writes} writes`);
  assert(warm.reads <= 1 && warm.writes === 0);
  assert.deepStrictEqual(body(warm.r).recent, first.recent, "same answer");
  assert.deepStrictEqual({ ...body(warm.r), recent: null }, { ...first, recent: null });

  advance(6000);                                               // the 60 s chip refresh asks for fewer rows: served from the longer cached list
  const w60 = await measure("status.warm60", () => get(fn, { op: "status", recent: "60" }));
  console.log(`  warm  recent=60 : ${w60.reads} read`);
  assert(w60.reads <= 1);
  assert.deepStrictEqual(body(w60.r).recent, first.recent.slice(0, 60));

  const same = await measure("status.burst", async () => { const rs = []; for (let i = 0; i < 20; i++) rs.push(get(fn, { op: "status", recent: "150" })); return Promise.all(rs); });
  console.log(`  20 simultaneous polls inside the memo window: ${same.reads} reads`);
  assert(same.reads <= 1, "tabs polling together share one answer");
});

t("status: two hours of 6 s watcher polls + 60 s chip refreshes from 3 tabs with nothing changing", async () => {
  m.db.docs.clear(); m.db.seed(seedQueue()); skew += 10 * H;
  const fn = loadFresh();
  await get(fn, { op: "status", recent: "150" });
  const a = m.snapshot();
  let polls = 0, old = 0;                                       // old: measured cost of one uncached poll with this data (151 + 6 count reads, 61 + 6)
  await m.op("status.2h", async () => {
    for (let s = 0; s < 2 * 3600; s += 6) {
      advance(6000);
      for (let tab = 0; tab < 3; tab++) { await get(fn, { op: "status", recent: "150" }); polls++; old += 157; }
      if (s % 60 === 0) for (let tab = 0; tab < 3; tab++) { await get(fn, { op: "status", recent: "60" }); polls++; old += 67; }
    }
  });
  const d = m.since(a), reads = d.reads + d.aggs;
  console.log(`  ${polls} polls in 2 h: ${reads} reads now vs ~${old} before (157 / 67 per poll) = ${(100 * (1 - reads / old)).toFixed(1)}% fewer; per day ${(reads * 12).toFixed(0)} vs ${(old * 12).toFixed(0)}`);
  assert(reads < old / 20, `reads ${reads}`);
  assert.strictEqual(d.writes, 0);
});

t("status: an enqueue, a retry and a failed drain are visible to ANOTHER instance on its next poll", async () => {
  m.db.docs.clear(); m.db.seed(seedQueue({ queued: 2, failed: 1, budgetUsed: 0, done: 5 })); skew += 10 * H;
  const A = loadFresh(), B = loadFresh();                       // two warm instances of the same function
  const first = body(await get(A, { op: "status", recent: "60" }));
  assert.strictEqual(first.queued, 2);
  const enq = await post(B, { op: "enqueue", requestId: "req-aaaaaaaaaaaaaaaa", payload: { category: "Beady_Necklace", title: "Brand new owl charm", images: [{ src: "https://x/1.jpg" }] } });
  assert.strictEqual(enq.statusCode, 200, enq.body);
  advance(2500);
  const afterEnq = body(await get(A, { op: "status", recent: "60" }));
  assert.strictEqual(afterEnq.queued, 3, "the enqueue done on B shows up on A");
  assert(afterEnq.recent.some((j) => j.title === "Brand new owl charm"));
  // a drain on B (no Shopify credentials here, so every job fails before creation) must also show up
  advance(2500);
  const out = await B._drain.drain();
  assert(out.failed.length >= 1, JSON.stringify(out));
  advance(2500);
  const afterDrain = body(await get(A, { op: "status", recent: "60" }));
  assert.strictEqual(afterDrain.queued, 0); assert.strictEqual(afterDrain.failed, 1 + out.failed.length);
  assert(afterDrain.recent.every((j) => j.status !== "QUEUED"));
  // retry of one job
  advance(2500);
  const jobId = "req-aaaaaaaaaaaaaaaa";                         // retryJob only accepts request-style ids
  assert(out.failed.some((f) => f.jobId === jobId));
  assert.strictEqual((await post(B, { op: "retryJob", jobId })).statusCode, 200);
  advance(2500);
  const afterRetry = body(await get(A, { op: "status", recent: "60" }));
  assert.strictEqual(afterRetry.queued, 1); assert.strictEqual(afterRetry.recent.find((j) => j.id === jobId).status, "QUEUED");
});

t("status: ?fresh=1 sees another instance's write inside the 2 s memo (explicit refresh after a click)", async () => {
  m.db.docs.clear(); m.db.seed(seedQueue({ queued: 0, failed: 2, done: 5, budgetUsed: 0 })); skew += 10 * H;
  m.db.seed({ "Shopify_Upload_Queue/req-failed-0000000001": { status: "FAILED", createdAtMs: Date.now() - 5000, variantCount: 40, payload: { title: "x", category: "Beady_Necklace" } } });
  const A = loadFresh(), B = loadFresh();
  assert.strictEqual(body(await get(A, { op: "status" })).failed, 3);
  advance(500);
  assert.strictEqual((await post(B, { op: "retryFailed" })).statusCode, 200);
  advance(500);                                                   // A's rev memo is still warm: a plain poll may be 2 s behind
  const f = await measure("status.fresh", () => get(A, { op: "status", fresh: "1" }));
  assert.strictEqual(body(f.r).failed, 0); assert.strictEqual(body(f.r).queued, 3);
  assert(f.reads <= 20, "one rev read plus the refreshed counts: " + f.reads);
});

t("status: an edit nothing bumped for (a hand edit in the console) still shows up within the max age", async () => {
  m.db.docs.clear(); m.db.seed(seedQueue({ queued: 2, failed: 0, budgetUsed: 0, done: 5 })); skew += 10 * H;
  const fn = loadFresh();
  assert.strictEqual(body(await get(fn, { op: "status", recent: "60" })).queued, 2);
  m.db.seed({ "Shopify_Upload_Queue/q0": { ...m.db.dump()["Shopify_Upload_Queue/q0"], status: "FAILED" } });   // no bump
  advance(10000);
  assert.strictEqual(body(await get(fn, { op: "status", recent: "60" })).queued, 2, "cached while the rev is unchanged");
  advance(61 * 1000);
  assert.strictEqual(body(await get(fn, { op: "status", recent: "60" })).queued, 1, "counts re-read after a minute");
  advance(5 * 60 * 1000);
  assert.strictEqual(body(await get(fn, { op: "status", recent: "60" })).recent.find((j) => j.id === "q0").status, "FAILED", "list re-read after 5 minutes");
});

t("status: fail-open - when the queueRev read fails every poll is the old full read and still answers", async () => {
  m.db.docs.clear(); m.db.seed(seedQueue({ queued: 3, failed: 0, done: 5, budgetUsed: 0 })); skew += 10 * H;
  const fn = loadFresh();
  failingRev = true;
  try {
    const a = await measure("status.failopen1", () => get(fn, { op: "status", recent: "60" }));
    advance(6000);
    const b = await measure("status.failopen2", () => get(fn, { op: "status", recent: "60" }));
    assert.strictEqual(a.r.statusCode, 200); assert.strictEqual(body(b.r).queued, 3);
    assert(b.reads >= 8, "no cache served without a rev: " + b.reads);
  } finally { failingRev = false; }
});

t("drain: once blocked by the daily budget, further kicks cost one read and no writes until something changes", async () => {
  m.db.docs.clear(); m.db.seed(seedQueue({ queued: 56, failed: 0, done: 20, budgetUsed: 940 })); skew += 10 * H;
  m.db.seed({ "Shopify_Upload_Meta/budget": { entries: [{ t: Date.now() - H, n: 940, j: "x" }], used: 940 } });
  const fn = loadFresh();
  const first = await measure("drain.first", () => fn._drain.drain());
  console.log(`  first blocked drain: ${first.reads} reads, ${first.writes} writes -> stoppedFor ${first.r.stoppedFor}`);
  assert.strictEqual(first.r.stoppedFor, "budget"); assert(!first.r.cachedBlock); assert(first.reads >= 56);
  for (let i = 0; i < 5; i++) {
    advance(45 * 1000);                                          // a tab's watchdog / safety-net kick
    const k = await measure("drain.kick", () => fn._drain.drain());
    assert.strictEqual(k.r.cachedBlock, true); assert.strictEqual(k.r.stoppedFor, "budget"); assert.strictEqual(k.r.moreQueued, true);
    assert(k.reads <= 1 && k.writes === 0, `${k.reads} reads ${k.writes} writes`);
  }
  console.log("  later blocked kicks: 1 read, 0 writes each");
  advance(6 * 60 * 1000);                                        // the memo is only good for 5 minutes: a full pass again
  const again = await measure("drain.afterMemo", () => fn._drain.drain());
  assert(!again.r.cachedBlock && again.reads >= 56);
});

t("drain: a new job, or budget that frees up, ends the shortcut", async () => {
  m.db.docs.clear(); m.db.seed(seedQueue({ queued: 3, failed: 0, done: 5, budgetUsed: 940 })); skew += 10 * H;
  m.db.seed({ "Shopify_Upload_Meta/budget": { entries: [{ t: Date.now() - H, n: 940, j: "x" }], used: 940 } });
  const A = loadFresh(), B = loadFresh();
  assert.strictEqual((await A._drain.drain()).stoppedFor, "budget");
  advance(30000);
  assert.strictEqual((await A._drain.drain()).cachedBlock, true);
  // a job that costs nothing (its product already exists) arrives through another instance: it must be picked up
  await post(B, { op: "enqueue", requestId: "req-bbbbbbbbbbbbbbbb", payload: { category: "Beady_Necklace", title: "Another charm", images: [{ src: "https://x/2.jpg" }] } });
  advance(30000);
  const afterNew = await A._drain.drain();
  assert(!afterNew.cachedBlock, "a changed queue is looked at again");
  // budget frees up as the 24 h window rolls on: no cached shortcut even though nothing was written
  m.db.docs.clear(); m.db.seed(seedQueue({ queued: 3, failed: 0, done: 5, budgetUsed: 940 }));
  m.db.seed({ "Shopify_Upload_Meta/budget": { entries: [{ t: Date.now() - 23.95 * H, n: 940, j: "x" }], used: 940 } });
  const C = loadFresh();
  assert.strictEqual((await C._drain.drain()).stoppedFor, "budget");
  advance(30000);
  assert.strictEqual((await C._drain.drain()).cachedBlock, true, "still inside the window");
  advance(10 * 60 * 1000);                                       // the old reservation is now more than 24 h old
  const freed = await C._drain.drain();
  assert(!freed.cachedBlock && freed.stoppedFor !== "budget", "budget freed: " + JSON.stringify(freed));
});

t("status: after ANY queue operation done on another instance, one poll later the answer equals the database (randomised)", async () => {
  m.db.docs.clear(); m.db.seed(seedQueue({ queued: 0, failed: 0, done: 30, budgetUsed: 0 })); skew += 10 * H;
  const A = loadFresh(), B = loadFresh();
  let seq = 0, seed = 12345;
  const rnd = (n) => { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed % n; };
  const truth = () => {
    const c = { QUEUED: 0, UPLOADING: 0, FAILED: 0, DONE: 0 };
    for (const [pth, d] of Object.entries(m.db.dump())) if (pth.startsWith("Shopify_Upload_Queue/")) c[d.status]++;
    return c;
  };
  const ids = () => Object.keys(m.db.dump()).filter((k) => k.startsWith("Shopify_Upload_Queue/req-")).map((k) => k.split("/")[1]);
  for (let i = 0; i < 120; i++) {
    const op = rnd(6);
    if (op <= 1) await post(B, { op: "enqueue", requestId: "req-" + String(++seq).padStart(16, "0"), payload: { category: "Beady_Necklace", title: "Owl charm " + seq, images: [{ src: "https://x/" + seq }] } });
    else if (op === 2) await B._drain.drain();                    // every job fails before creation (no Shopify credentials): claim, reserve, refund, FAILED
    else if (op === 3) await post(B, { op: "retryFailed" });
    else if (op === 4 && ids().length) await post(B, { op: "retryJob", jobId: ids()[rnd(ids().length)] });
    else if (op === 5 && ids().length) await post(B, { op: "markJobDone", jobId: ids()[rnd(ids().length)] });
    advance(2500);
    const j = body(await get(A, { op: "status", recent: "60" })), tr = truth();
    assert.deepStrictEqual({ q: j.queued, u: j.uploading, f: j.failed, d: j.done }, { q: tr.QUEUED, u: tr.UPLOADING, f: tr.FAILED, d: tr.DONE }, "step " + i + " op " + op);
    const listed = {}; j.recent.forEach((r) => { listed[r.id] = r.status; });
    for (const [pth, d] of Object.entries(m.db.dump())) {
      const id = pth.split("/")[1];
      if (pth.startsWith("Shopify_Upload_Queue/req-") && id in listed) assert.strictEqual(listed[id], d.status, "step " + i + " job " + id);
    }
  }
});

t("source: every write that changes a queue job or the budget is followed by a queueRev bump", () => {
  const src = require("fs").readFileSync(FN, "utf8");
  for (const needle of [
    'status: "UPLOADING"', 'status: "DONE"', 'status: "FAILED"', 'status: "QUEUED", error: null', "recoveredProductAtMs", "productId: product.id, handle: product.handle",
    "marked done manually", "const saved = await db.runTransaction", "const res = await db.runTransaction"]) {
    const i = src.indexOf(needle); assert(i > 0, "missing " + needle);
    assert(/bumpQueueRev\(\)/.test(src.slice(i, i + 1600)), "no bump after: " + needle);
  }
});

(async () => {
  // the queueRev read can be made to fail for the fail-open test
  const origDb = m.db;
  const admin = require("module")._load("./firebaseAdmin", null, false);
  const failing = new Proxy(origDb, { get(tg, p) {
    if (p === "collection") return (c) => { const col = tg.collection(c); if (c !== "Shopify_Upload_Meta") return col; return new Proxy(col, { get(ct, cp) { if (cp === "doc") return (id) => { const d = ct.doc(id); if (id !== "queueRev") return d; return new Proxy(d, { get(dt, dp) { if (dp === "get") return () => (failingRev ? Promise.reject(new Error("deadline exceeded")) : dt.get()); const v = dt[dp]; return typeof v === "function" ? v.bind(dt) : v; } }); }; const v = ct[cp]; return typeof v === "function" ? v.bind(ct) : v; } }); };
    const v = tg[p]; return typeof v === "function" ? v.bind(tg) : v;
  } });
  admin.firestore = Object.assign(() => failing, admin.firestore);
  let passed = 0;
  for (const [name, fn] of queue) {
    try { await fn(); passed++; console.log("ok   " + name); } catch (e) { console.log("FAIL " + name + "\n     " + (e && e.stack || e)); process.exitCode = 1; }
  }
  console.log(`\n${passed}/${queue.length} passed`);
  Date.now = realNow; global.fetch = realFetch; console.warn = realWarn; m.uninstall();
})();
