// Firestore cost of the Etsy API usage widget endpoint (Index.html asks every 15 s from every open tab).
// Real etsyApiUsage.js against the in-memory metered Firestore (tests/cost/_meter.js); no network.
//   node tests/cost/ru-G2-etsy-usage.js
"use strict";
const assert = require("assert");
const path = require("path");
const meter = require("./_meter.js");

const FN = path.join(__dirname, "../../netlify/functions/etsyApiUsage.js");
const m = meter.create();
const realNow = Date.now.bind(Date);
let skew = 0;
Date.now = () => realNow() + skew;
m.install();
const realLoad = require("module")._load;                     // etsyRateLimiter needs node-fetch; nothing here may reach a network
require("module")._load = function (req, ...rest) { return req === "node-fetch" ? async () => { throw new Error("no network in tests"); } : realLoad.call(this, req, ...rest); };
const { torontoDayKey } = require("../../netlify/functions/etsyRateLimiter.js");
const day = torontoDayKey();

function load() { delete require.cache[FN]; return require(FN); }
const poll = (fn) => fn.handler({ httpMethod: "GET", headers: {} });
async function measure(name, f) { const a = m.snapshot(); const r = await m.op(name, f); const d = m.since(a); return { r, reads: d.reads + d.aggs, writes: d.writes }; }
const body = (r) => JSON.parse(r.body);
const queue = [];
const t = (name, fn) => queue.push([name, fn]);

t("one poll still costs the same two reads, and the answer is unchanged", async () => {
  m.db.seed({ ["EtsyApi_Usage/" + day]: { count: 120, count_since: realNow() - 5e6, updated_at: realNow(), max_qps: 2, etsy_limit_per_day: 5000, etsy_remaining_today: 4000, etsy_reported_at: realNow() - 1000 } });
  const fn = load();
  const one = await measure("usage.one", () => poll(fn));
  assert.strictEqual(one.reads, 2);
  const j = body(one.r);
  assert.strictEqual(j.ok, true); assert.strictEqual(j.verified, true); assert.strictEqual(j.count, 120); assert.strictEqual(j.etsy_remaining_today, 4000);
  assert.strictEqual(j.etsy_key_source, "listing-generator"); assert.strictEqual(j.budget, 2500);
});

t("10 tabs x 15 s polling for an hour: reads fall from ~4,800 pairs to one pair per 10 s", async () => {
  const fn = load();
  const a = m.snapshot(); let polls = 0;
  await m.op("usage.hour", async () => {
    for (let s = 0; s < 3600; s += 15) { skew += 15000; for (let tab = 0; tab < 10; tab++) { await poll(fn); polls++; } }
  });
  const d = m.since(a), reads = d.reads + d.aggs;
  console.log(`  ${polls} polls in 1 h: ${reads} reads now vs ${polls * 2} before; per day ${reads * 24} vs ${polls * 2 * 24}`);
  assert(reads <= 2 * 3600 / 15 + 2, "at most one pair per 15 s window (the polls are 15 s apart): " + reads);
  assert(reads <= polls * 2 / 9, `reads ${reads} of ${polls * 2}`);
});

t("simultaneous polls share one read pair; a new count shows within the 10 s cache; server_time is always current", async () => {
  const fn = load();
  skew += 60000;
  const burst = await measure("usage.burst", async () => { const rs = []; for (let i = 0; i < 25; i++) rs.push(poll(fn)); return Promise.all(rs); });
  assert.strictEqual(burst.reads, 2, "25 simultaneous polls = one pair");
  const first = body(burst.r[0]);
  m.db.seed({ ["EtsyApi_Usage/" + day]: { count: 121, count_since: realNow() - 5e6, updated_at: realNow(), max_qps: 2, etsy_limit_per_day: 5000, etsy_remaining_today: 3999, etsy_reported_at: realNow() } });
  skew += 4000;
  const cached = body(await poll(fn));
  assert.strictEqual(cached.count, first.count, "inside the cache window the previous reading is served");
  assert(cached.server_time >= first.server_time + 4000, "server_time is not cached");
  skew += 7000;
  const fresh = await measure("usage.fresh", () => poll(fn));
  assert.strictEqual(body(fresh.r).count, 121); assert.strictEqual(fresh.reads, 2);
});

t("a failed read is not cached: the next poll tries again and the error reaches the caller as before", async () => {
  const fn = load();
  skew += 60000;
  const admin = require("module")._load("./firebaseAdmin", null, false);
  const realFs = admin.firestore;
  const broken = { collection: () => ({ doc: () => ({ get: () => Promise.reject(new Error("deadline exceeded")) }) }) };
  admin.firestore = Object.assign(() => broken, realFs);
  const spy = console.error; console.error = () => {};
  try {
    const bad = await poll(fn);
    assert.strictEqual(bad.statusCode, 500); assert.strictEqual(body(bad).ok, false);
  } finally { console.error = spy; admin.firestore = realFs; }
  const ok = await poll(fn);
  assert.strictEqual(ok.statusCode, 200);
});

(async () => {
  let passed = 0;
  for (const [name, fn] of queue) {
    try { await fn(); passed++; console.log("ok   " + name); } catch (e) { console.log("FAIL " + name + "\n     " + (e && e.stack || e)); process.exitCode = 1; }
  }
  console.log(`\n${passed}/${queue.length} passed`);
  Date.now = realNow; require("module")._load = realLoad; m.uninstall();
})();
