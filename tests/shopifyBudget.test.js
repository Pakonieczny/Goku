// Offline tests for the Shopify daily-budget helpers and their wiring. No Shopify, no Firestore.
//   node tests/shopifyBudget.test.js
const assert = require("assert");
const fs = require("fs");
const path = require("path");
const K = require("../netlify/functions/_shopifyBudget.js");
const src = fs.readFileSync(path.join(__dirname, "..", "netlify/functions/shopifyListingUpload.js"), "utf8");
const html = fs.readFileSync(path.join(__dirname, "..", "Index.html"), "utf8");
let passed = 0;
const t = (name, fn) => { try { fn(); passed++; console.log("ok   " + name); } catch (e) { console.log("FAIL " + name + "\n     " + e.message); process.exitCode = 1; } };
const job = (id, variantCount, extra) => ({ id, data: Object.assign({ variantCount }, extra || {}) });
const TODAY = "2026-10-14";

t("a big job at the head no longer strands the day's allowance", () => {
  const jobs = [job("a", 40), job("b", 32), job("c", 10)];
  assert.strictEqual(K.pickFit(jobs, 22, TODAY).id, "c");
  assert.strictEqual(K.pickFit(jobs, 35, TODAY).id, "b");
  assert.strictEqual(K.pickFit(jobs, 100, TODAY).id, "a");          // oldest first when everything fits
  assert.strictEqual(K.pickFit(jobs, 5, TODAY), null);              // nothing fits: paused until tomorrow
});

t("jobs that already cost their budget (product exists / reserved today) always fit", () => {
  assert.strictEqual(K.pickFit([job("p", 40, { productId: "gid://x" })], 0, TODAY).id, "p");
  assert.strictEqual(K.pickFit([job("r", 40, { budgetReservedDay: TODAY })], 0, TODAY).id, "r");
  assert.strictEqual(K.pickFit([job("old", 40, { budgetReservedDay: "2026-10-13" })], 0, TODAY), null);   // yesterday's reservation does not count
});

t("smallestNeed ignores jobs that cost nothing more", () => {
  assert.strictEqual(K.smallestNeed([job("a", 40), job("b", 32), job("c", 10, { productId: "x" })], TODAY), 32);
  assert.strictEqual(K.smallestNeed([], TODAY), 0);
});

t("Toronto day boundary", () => {
  assert.strictEqual(K.torontoDateKey(new Date("2026-10-14T03:30:00Z")), "2026-10-13");     // 23:30 EDT the day before
  assert.strictEqual(K.torontoDateKey(new Date("2026-10-14T05:00:00Z")), "2026-10-14");     // 01:00 EDT
  assert.strictEqual(K.msUntilTorontoMidnight(new Date("2026-10-14T16:00:00Z")), 12 * 3600 * 1000);   // noon EDT
  assert.strictEqual(K.msUntilTorontoMidnight(new Date("2026-10-14T03:59:30Z")), 30 * 1000);          // 23:59:30 EDT
});

t("drain and status use the helpers", () => {
  assert(/budgetKit\.pickFit\(sorted\.map/.test(src), "drain picks a job that fits");
  assert(/if \(!fit\) \{ out\.stoppedFor = "budget"; break; \}/.test(src));
  assert(/budget\.paused = /.test(src) && /resetsInMs/.test(src) && /nextNeeds/.test(src), "status reports paused + reset time");
});

t("client chip says paused and when it resumes", () => {
  assert(/paused \? " · paused" : ""/.test(html) && /resumes in \$\{resumes\}/.test(html));
});

t("a real schedule exists for the backup drain", () => {
  const f = fs.readFileSync(path.join(__dirname, "..", "netlify/functions/shopifyDrainSchedule.mjs"), "utf8");
  assert(/export const config = \{ schedule: "\*\/15 \* \* \* \*" \}/.test(f));
  assert(/shopifyDrain-background/.test(f));
});

console.log(`\n${passed} passed`);
