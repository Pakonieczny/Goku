// netlify/functions/etsyRateLimiter.js
//
// One distributed gate for every Etsy API request made by the Listing
// Generator. Firestore provides cluster-wide pacing and accounting across
// concurrent Netlify instances. The in-memory scheduler remains a safe
// fallback when Firestore is temporarily unavailable.

const fetch = require("node-fetch");

// Etsy app limits and the operator's 50% safety allocation.
const ETSY_QPS_LIMIT = 5;
const ETSY_DAILY_LIMIT = 5000;
const INTERNAL_FRACTION = 0.5;
const RATE_PER_SEC = ETSY_QPS_LIMIT * INTERNAL_FRACTION; // 2.5/s
const DAILY_BUDGET = Math.floor(ETSY_DAILY_LIMIT * INTERNAL_FRACTION); // 2,500/day
const BURST = 2;
const STATE_RETRIES = 5;
// The distributed gate already keeps traffic below Etsy's per-second limit.
// One retry is enough for a genuine edge-window 429; five attempts multiplied
// a single request into five quota charges during outages.
const ETSY_429_MAX_ATTEMPTS = 2;
const JITTER_MS = 50;
const USAGE_COLLECTION = "ListingGenerator_ApiUsage";
const RATE_BUCKET = "etsy-listing-generator-global";

let db = null;
let useFirestore = false;
try {
  const admin = require("./firebaseAdmin");
  db = admin.firestore();
  useFirestore = true;
} catch (error) {
  console.warn("etsyRateLimiter: Firestore unavailable; using per-instance pacing only.", error.message);
}

const memState = { nextFreeMs: 0 };
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const jitter = () => Math.floor(Math.random() * JITTER_MS);

function backoff(attempt) {
  return Math.min(2000, 250 * Math.pow(2, attempt)) + jitter();
}

function parseRetryAfter(value) {
  if (!value) return null;
  const seconds = Number(value);
  if (Number.isFinite(seconds)) return Math.ceil(seconds * 1000);
  const when = Date.parse(value);
  return Number.isFinite(when) ? Math.max(0, when - Date.now()) : null;
}

// The app counter rolls into the next Toronto date one minute early, matching
// the verified Etsy Pricing console supplied with this project.
function torontoDayKey() {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Toronto",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date(Date.now() + 60000));
}

async function takeToken(bucket = RATE_BUCKET) {
  if (!useFirestore) {
    const now = Date.now();
    const slot = Math.max(now, memState.nextFreeMs);
    memState.nextFreeMs = slot + 515; // strict 2/s fallback
    if (slot > now) await sleep(slot - now);
    return;
  }

  const ref = db.collection("rate_limits").doc(bucket);
  for (let attempt = 1; ; attempt++) {
    const now = Date.now();
    try {
      await db.runTransaction(async (tx) => {
        const snap = await tx.get(ref);
        const data = snap.exists ? snap.data() : { tokens: BURST, lastMs: now };
        const last = Number(data.lastMs || now);
        const stored = Number.isFinite(data.tokens) ? Number(data.tokens) : BURST;
        const refill = (Math.max(0, now - last) / 1000) * RATE_PER_SEC;
        let tokens = Math.min(BURST, stored + refill);

        if (tokens < 1) {
          const error = new Error("rate-limit-wait");
          error.waitMs = Math.ceil(((1 - tokens) / RATE_PER_SEC) * 1000) + jitter();
          throw error;
        }

        tokens -= 1;
        tx.set(ref, { tokens, lastMs: now }, { merge: true });
      });
      return;
    } catch (error) {
      if (error && error.message === "rate-limit-wait") {
        await sleep(error.waitMs);
      } else {
        if (attempt >= STATE_RETRIES) throw error;
        await sleep(backoff(attempt));
      }
    }
  }
}

// Etsy meters requests in one-second windows. A transactional epoch-second
// counter makes the observed QPS and the hard cap accurate across all
// concurrent function instances.
const PER_SECOND_CAP = Math.floor(RATE_PER_SEC);
async function chargeDailyBudget() {
  if (!useFirestore) return;

  const ref = db.collection(USAGE_COLLECTION).doc(torontoDayKey());
  for (let attempt = 0; attempt < 40; attempt++) {
    let waitMs = 0;
    await db.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      const data = snap.exists ? snap.data() : {};
      const used = Number(data.count || 0);

      if (used >= DAILY_BUDGET) {
        const error = new Error(
          `DAILY_BUDGET_EXHAUSTED: this app has spent its ${DAILY_BUDGET}-call ` +
          `daily allocation. Calls resume after the 11:59 PM Toronto reset.`
        );
        error.code = "DAILY_BUDGET_EXHAUSTED";
        throw error;
      }

      const headerIsFresh =
        data.etsy_reported_at &&
        Date.now() - Number(data.etsy_reported_at) < 30 * 60 * 1000;
      if (
        headerIsFresh &&
        data.etsy_remaining_today != null &&
        Number(data.etsy_remaining_today) <= 100
      ) {
        const error = new Error(
          `DAILY_BUDGET_EXHAUSTED: Etsy reports only ${data.etsy_remaining_today} ` +
          "calls left on the whole API key. The final 100 calls are reserved."
        );
        error.code = "DAILY_BUDGET_EXHAUSTED";
        throw error;
      }

      const nowMs = Date.now();
      const nowSec = Math.floor(nowMs / 1000);
      const sameSecond = Number(data.sec_key) === nowSec;
      const usedThisSecond = sameSecond ? Number(data.sec_count || 0) : 0;

      if (usedThisSecond >= PER_SECOND_CAP) {
        waitMs = 1000 - (nowMs % 1000) + 5 + jitter();
        return;
      }

      tx.set(
        ref,
        {
          count: used + 1,
          count_since: data.count_since || nowMs,
          sec_key: nowSec,
          sec_count: usedThisSecond + 1,
          max_qps: Math.max(Number(data.max_qps || 0), usedThisSecond + 1),
          updated_at: nowMs,
        },
        { merge: true }
      );
    });

    if (!waitMs) return;
    await sleep(waitMs);
  }

  const error = new Error("Rate gate contention: Etsy call slot could not be reserved.");
  error.code = "RATE_GATE_CONTENTION";
  throw error;
}

// Etsy's response headers are the authoritative whole-key daily meter. Await
// persistence so a serverless invocation cannot freeze before the reading is
// recorded.
async function captureEtsyHeaders(response) {
  if (!useFirestore || !response || !response.headers) return;
  try {
    const limit = response.headers.get("x-limit-per-day");
    const remaining = response.headers.get("x-remaining-today");
    if (limit == null && remaining == null) return;

    const patch = { etsy_reported_at: Date.now() };
    const parsedLimit = Number(limit);
    const parsedRemaining = Number(remaining);
    if (limit != null && Number.isFinite(parsedLimit)) {
      patch.etsy_limit_per_day = parsedLimit;
    }
    if (remaining != null && Number.isFinite(parsedRemaining)) {
      patch.etsy_remaining_today = parsedRemaining;
    }
    await db.collection(USAGE_COLLECTION).doc(torontoDayKey()).set(patch, { merge: true });
  } catch (error) {
    console.warn("etsyRateLimiter: could not persist Etsy rate headers.", error.message);
  }
}

async function etsyFetch(url, init = {}, options = {}) {
  const bucket = options.bucket || RATE_BUCKET;
  const retries = options.retries == null ? ETSY_429_MAX_ATTEMPTS : Number(options.retries);

  for (let attempt = 1; ; attempt++) {
    await takeToken(bucket);
    await chargeDailyBudget(); // each real attempt, including a 429 retry

    const response = await fetch(url, init);
    await captureEtsyHeaders(response);

    if (response.status !== 429 || attempt >= retries) return response;

    // A daily-limit 429 cannot recover before reset. Never spend another call
    // retrying it. clone() preserves the original body for the caller.
    try {
      const detail = await response.clone().text();
      if (/exceeded daily rate limit|daily rate limit/i.test(detail)) return response;
    } catch (_) {}

    const retryAfter = parseRetryAfter(response.headers.get("retry-after"));
    await sleep(retryAfter != null ? retryAfter + jitter() : backoff(attempt));
  }
}

module.exports = {
  etsyFetch,
  takeToken,
  torontoDayKey,
  USAGE_COLLECTION,
  DAILY_BUDGET,
  RATE_PER_SEC,
};
