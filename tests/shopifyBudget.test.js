// Offline tests for the Shopify rolling 24 h variant budget. No Shopify, no real Firestore.
//   node tests/shopifyBudget.test.js
const assert = require("assert");
const fs = require("fs");
const path = require("path");
const Module = require("module");
const K = require("../netlify/functions/_shopifyBudget.js");
const src = fs.readFileSync(path.join(__dirname, "..", "netlify/functions/shopifyListingUpload.js"), "utf8");
const html = fs.readFileSync(path.join(__dirname, "..", "Index.html"), "utf8");
let passed = 0;
const queue = [];
const t = (name, fn) => queue.push([name, fn]);
const H = 3600 * 1000;
const job = (id, variantCount, extra) => ({ id, data: Object.assign({ variantCount }, extra || {}) });

// ---------- pure helpers ----------
t("rolling window: a reservation counts for exactly 24 h", () => {
  const now = 1e12, e = [{ t: now - 23 * H, n: 500, j: "a" }, { t: now - 25 * H, n: 900, j: "old" }];
  assert.strictEqual(K.usedIn(e, now), 500);                       // the 25 h old one no longer counts
  assert.strictEqual(K.usedIn(e, now + 2 * H), 0);                 // 23 h + 2 h: now it has aged out too
});
t("msUntilFits: when enough old reservations have aged out", () => {
  const now = 1e12;
  const e = [{ t: now - 20 * H, n: 400, j: "a" }, { t: now - 10 * H, n: 528, j: "b" }];     // 928 used, 22 left of 950
  assert.strictEqual(K.msUntilFits(e, 22, 950, now), 0);
  assert.strictEqual(K.msUntilFits(e, 32, 950, now), 4 * H);        // the 400 frees in 4 h, then 422 are free
  assert.strictEqual(K.msUntilFits(e, 420, 950, now), 4 * H);        // 22 + 400 = 422 free once the first ages out
  assert.strictEqual(K.msUntilFits(e, 500, 950, now), 14 * H);       // needs the second one too
  assert.strictEqual(K.msUntilFits(e, 900, 950, now), 14 * H);      // needs the second one to age out as well
});
t("a big job at the head no longer strands the allowance", () => {
  const jobs = [job("a", 40), job("b", 32), job("c", 10)];
  assert.strictEqual(K.pickFit(jobs, 22, new Set()).id, "c");
  assert.strictEqual(K.pickFit(jobs, 35, new Set()).id, "b");
  assert.strictEqual(K.pickFit(jobs, 100, new Set()).id, "a");
  assert.strictEqual(K.pickFit(jobs, 5, new Set()), null);
});
t("jobs already paid for (product exists / reservation still in the window) always fit", () => {
  assert.strictEqual(K.pickFit([job("p", 40, { productId: "gid://x" })], 0, new Set()).id, "p");
  assert.strictEqual(K.pickFit([job("r", 40)], 0, new Set(["r"])).id, "r");
  assert.strictEqual(K.pickFit([job("n", 40)], 0, new Set(["other"])), null);
});
t("old calendar-day counter is carried over once, then ages out", () => {
  const now = Date.now();
  const mig = K.entriesFromDoc({ dateKey: K.torontoDateKey(new Date(now)), used: 400 }, now);
  assert.strictEqual(K.usedIn(mig, now), 400);
  assert.deepStrictEqual(K.entriesFromDoc({ dateKey: "2001-01-01", used: 900 }, now), []);       // another day: gone
  assert.deepStrictEqual(K.entriesFromDoc(null, now), []);
});

// ---------- reservation / refund against an in-memory Firestore ----------
function fakeDb() {
  const docs = new Map();
  const ref = (path) => ({ path, id: path.split("/").pop(), async get() { return snap(path); } });
  const snap = (path) => ({ exists: docs.has(path), id: path.split("/").pop(), data: () => JSON.parse(JSON.stringify(docs.get(path))) });
  const db = {
    docs,
    collection: (c) => ({ doc: (id) => ref(c + "/" + id) }),
    async runTransaction(fn) {
      const writes = [];
      const tx = {
        get: async (r) => snap(r.path),
        set: (r, data, opts) => { writes.push([r.path, data, !!(opts && opts.merge)]); }
      };
      const out = await fn(tx);
      for (const [p, d, merge] of writes) docs.set(p, merge ? Object.assign({}, docs.get(p) || {}, d) : JSON.parse(JSON.stringify(d)));
      return out;
    }
  };
  return db;
}
function loadShopify(db) {
  const orig = Module._load;
  Module._load = function (req, parent, isMain) {
    if (/firebaseAdmin$/.test(req)) return { firestore: () => db };
    return orig.apply(this, arguments);
  };
  try { delete require.cache[require.resolve("../netlify/functions/shopifyListingUpload.js")]; return require("../netlify/functions/shopifyListingUpload.js")._drain; }
  finally { Module._load = orig; }
}
t("reserve: charges once, refuses over the ceiling, refunds failed pre-create jobs, never double-charges a resumed job", async () => {
  const db = fakeDb(); const S = loadShopify(db);
  const jobRef = (id) => db.collection("Shopify_Upload_Queue").doc(id);
  const seedJob = (id, n) => db.docs.set("Shopify_Upload_Queue/" + id, { variantCount: n, status: "UPLOADING" });
  for (const id of ["j1", "j2", "j3"]) seedJob(id, 400);
  assert.strictEqual((await S.reserveBudgetForJob(jobRef("j1"), 400)).granted, true);
  assert.strictEqual((await S.reserveBudgetForJob(jobRef("j2"), 400)).granted, true);
  assert.strictEqual((await S.reserveBudgetForJob(jobRef("j3"), 400)).granted, false, "1200 > 950 ceiling");
  const again = await S.reserveBudgetForJob(jobRef("j1"), 400);
  assert.strictEqual(again.alreadyReserved, true);                       // resumed job: not charged twice
  let snap = await S.budgetSnapshot();
  assert.strictEqual(snap.used, 800); assert.strictEqual(snap.remaining, 150);
  await S.refundBudgetForJob(jobRef("j2"));                              // failed before Shopify created a product
  snap = await S.budgetSnapshot();
  assert.strictEqual(snap.used, 400);
  assert.strictEqual((await S.reserveBudgetForJob(jobRef("j3"), 400)).granted, true);
  const pub = S.publicBudget(snap);
  assert(!("entries" in pub) && !("reservedIds" in pub));
});
t("reserve: a reservation older than 24 h no longer counts (the queue keeps draining all day)", async () => {
  const db = fakeDb(); const S = loadShopify(db);
  db.docs.set("Shopify_Upload_Meta/budget", { entries: [{ t: Date.now() - 25 * H, n: 940, j: "old" }, { t: Date.now() - 1 * H, n: 10, j: "recent" }], used: 950 });
  db.docs.set("Shopify_Upload_Queue/new", { variantCount: 40, status: "UPLOADING" });
  const r = await S.reserveBudgetForJob(db.collection("Shopify_Upload_Queue").doc("new"), 40);
  assert.strictEqual(r.granted, true);
  assert.strictEqual((await S.budgetSnapshot()).used, 50);
});

// ---------- wiring ----------
t("drain and status use the rolling budget", () => {
  assert(/budgetKit\.pickFit\(sorted\.map/.test(src) && /bud\.reservedIds/.test(src));
  assert(/if \(!fit\) \{ out\.stoppedFor = "budget"; break; \}/.test(src));
  assert(/msUntilFits\(budget\.entries/.test(src) && /paused: false, resetsInMs: 0, nextNeeds: 0/.test(src));
});
t("client chip says paused and when it resumes", () => {
  assert(/paused \? " · paused" : ""/.test(html) && /resumes in \$\{resumes\}/.test(html));
  assert(/Variant budget \(rolling 24 h\)/.test(html) || /resets midnight Toronto|rolling 24 h/.test(html));
});
t("a real schedule exists for the backup drain", () => {
  const f = fs.readFileSync(path.join(__dirname, "..", "netlify/functions/shopifyDrainSchedule.mjs"), "utf8");
  assert(/export const config = \{ schedule: "\*\/15 \* \* \* \*" \}/.test(f) && /shopifyDrain-background/.test(f));
});

(async () => {
  for (const [name, fn] of queue) {
    try { await fn(); passed++; console.log("ok   " + name); } catch (e) { console.log("FAIL " + name + "\n     " + (e && e.message)); process.exitCode = 1; }
  }
  console.log(`\n${passed} passed`);
})();
