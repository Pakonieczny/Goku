// netlify/functions/_shared/etsyRateLimiter.js
// Distributed token-bucket limiter + resilient Etsy fetch with 429 handling.
// Uses Firestore if FIREBASE_SERVICE_ACCOUNT is provided; otherwise falls back to in-memory
// (in-memory helps single instance, Firestore makes it safe across many instances).

const fetch = require("node-fetch");

// ---- Config
const RATE_PER_SEC = 5;      // Etsy hard cap
const BURST        = 5;      // bucket capacity
const MAX_RETRIES  = 5;      // on 429 / contention
const JITTER_MS    = 50;     // random jitter to avoid thundering herd

// ---- Firestore (optional but recommended)
let useFirestore = false;
let db = null;

function buildSvcFromSplitEnv() {
  const {
    FIREBASE_PROJECT_ID,
    FIREBASE_CLIENT_EMAIL,
    FIREBASE_PRIVATE_KEY,
  } = process.env;

  // Minimum needed: project_id, client_email, private_key
  if (!FIREBASE_PROJECT_ID || !FIREBASE_CLIENT_EMAIL || !FIREBASE_PRIVATE_KEY) return null;

  // Fix escaped newlines in Netlify envs
  const pk = FIREBASE_PRIVATE_KEY.includes("\\n")
    ? FIREBASE_PRIVATE_KEY.replace(/\\n/g, "\n")
    : FIREBASE_PRIVATE_KEY;

  return {
    type: "service_account",
    project_id: FIREBASE_PROJECT_ID,
    private_key: pk,
    client_email: FIREBASE_CLIENT_EMAIL,
  };
}

try {
  let svc = null;

  if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    // Option A: single JSON var
    svc = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  } else {
    // Option B: split vars like in your screenshot
    svc = buildSvcFromSplitEnv();
  }

  if (svc) {
    const admin = require("firebase-admin");
    if (!admin.apps.length) {
      admin.initializeApp({ credential: admin.credential.cert(svc), projectId: svc.project_id });
    }
    db = admin.firestore();
    useFirestore = true;
  }
} catch (_) {
  useFirestore = false; // falls back to in-memory token-bucket
}

// ---- In-memory fallback (per instance)
const memState = { tokens: BURST, lastMs: Date.now() };

// Helpers
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const jitter = () => Math.floor(Math.random() * JITTER_MS);

function backoff(attempt) {
  // 200ms per token at 5 rps; exponential with cap
  const base = 200;
  const ms = Math.min(2000, base * Math.pow(2, attempt));
  return ms + jitter();
}

function parseRetryAfter(h) {
  if (!h) return null;
  const s = Number(h);
  if (Number.isFinite(s)) return Math.ceil(s * 1000);
  const when = Date.parse(h);
  return Number.isFinite(when) ? Math.max(0, when - Date.now()) : null;
}

// ---- Token bucket (Firestore-backed if available)
async function takeToken(bucket = "etsy-global") {
  const now = Date.now();

  if (!useFirestore) {
    // In-memory (per VM) — good safety net; Firestore is the robust path
    const elapsed = now - memState.lastMs;
    const refill  = (elapsed / 1000) * RATE_PER_SEC;
    memState.tokens = Math.min(BURST, memState.tokens + refill);
    memState.lastMs = now;

    if (memState.tokens >= 1) {
      memState.tokens -= 1;
      return;
    }
    const need = 1 - memState.tokens;
    const waitMs = Math.ceil((need / RATE_PER_SEC) * 1000) + jitter();
    await sleep(waitMs);
    return;
  }

  // Firestore transaction to make it safe across instances
  const ref = db.collection("rate_limits").doc(bucket);
  let attempt = 0;

  while (true) {
    attempt++;
    try {
      await db.runTransaction(async tx => {
        const snap = await tx.get(ref);
        const data = snap.exists ? snap.data() : { tokens: BURST, lastMs: now };
        const last = Number(data.lastMs || now);
        const tokensStored = Number.isFinite(data.tokens) ? data.tokens : BURST;

        const elapsed = Math.max(0, now - last);
        const refill  = (elapsed / 1000) * RATE_PER_SEC;
        let tokens    = Math.min(BURST, tokensStored + refill);

        if (tokens < 1) {
          // Not enough tokens → throw with recommended retry delay
          const need   = 1 - tokens;
          const waitMs = Math.ceil((need / RATE_PER_SEC) * 1000) + jitter();
          const err    = new Error("rate-limit-wait");
          err.waitMs   = waitMs;
          throw err;
        }

        tokens -= 1;
        tx.set(ref, { tokens, lastMs: now }, { merge: true });
      });
      return; // success
    } catch (e) {
      if (e && e.message === "rate-limit-wait") {
        await sleep(e.waitMs);
      } else {
        // Transaction contention/backoff
        if (attempt >= MAX_RETRIES) throw e;
        await sleep(backoff(attempt));
      }
    }
  }
}

// ---- Public Etsy fetch with gating + 429 retry
//
// METERING (added for verified API-usage parity): every call through here is
// recorded and every response has its Etsy rate-limit headers captured, so
// etsyApiUsage can serve the App/Key/QPS widget without ever estimating.
// This is deliberately fire-and-forget and fully guarded — a metering fault
// must never break, delay, or alter an Etsy call. If the usage module can't
// even be required (missing file, Firestore down at init), we degrade to a
// no-op and Etsy traffic is completely unaffected.
let _usage = null;
try {
  _usage = require("./_etsyApiUsage");
} catch (_) {
  _usage = null;
}
function _meter(appId, res) {
  if (!_usage) return;
  try { _usage.recordCall(appId, res); } catch (_) {}
}

async function etsyFetch(url, init = {}, opts = {}) {
  const {
    bucket = "etsy-global",
    retries = MAX_RETRIES,
    // Which console to attribute this call to. Callers may override per
    // request; the env default keeps existing call sites working untouched.
    appId = process.env.ETSY_APP_ID || "pricing-console",
  } = opts;

  let attempt = 0;
  while (true) {
    attempt++;
    await takeToken(bucket);

    let res;
    try {
      res = await fetch(url, init);
    } catch (err) {
      // Network failure still consumed a token and may have reached Etsy —
      // count the attempt so the budget can't silently under-report.
      _meter(appId, null);
      throw err;
    }

    // Count every attempt, and harvest the rate-limit headers from every
    // response including 429s (those carry the most important reading).
    _meter(appId, res);

    if (res.status !== 429) return res;

    // 429 → respect Retry-After if present, else exponential backoff
    const ra = parseRetryAfter(res.headers.get("retry-after"));
    const waitMs = ra != null ? ra + jitter() : backoff(attempt);
    if (attempt >= retries) return res; // propagate the 429 payload to caller
    await sleep(waitMs);
  }
}

/*  ═══ BACKWARD-COMPATIBILITY EXPORTS ═══════════════════════════════════
 *
 *  REGRESSION FIX. This site's original etsyRateLimiter.js exported FIVE
 *  things. The version that replaced it (copied from the Etsy Pricing Console,
 *  which has a different API surface) exported only two, so etsyApiUsage.js —
 *  which does
 *
 *      const { torontoDayKey, USAGE_COLLECTION, DAILY_BUDGET, RATE_PER_SEC }
 *        = require("./etsyRateLimiter");
 *
 *  — got `undefined` for all four. torontoDayKey() then threw on the first
 *  line of the handler, the endpoint returned 500, and the API-usage widget
 *  showed "Unavailable / — QPS". Nothing else was affected: the other twelve
 *  consumers only need etsyFetch, which never went missing.
 *
 *  All four are restored below, and the metering path now ALSO mirrors its
 *  per-day totals into USAGE_COLLECTION in the exact shape etsyApiUsage.js
 *  reads, so the widget has data to display again.
 */

// Toronto day key, e.g. "2026-07-27". Matches the "resets 11:59 PM Toronto"
// wording the widget shows the operator.
const _DAY_FMT = new Intl.DateTimeFormat("en-CA", {
  timeZone: "America/Toronto", year: "numeric", month: "2-digit", day: "2-digit",
});
function torontoDayKey(d = new Date()) { return _DAY_FMT.format(d); }

/*  The Firestore collection etsyApiUsage.js reads its per-day counters from.
 *
 *  ⚠ IF YOU KNOW THE NAME YOUR ORIGINAL FILE USED, SET IT — either by editing
 *  the default below or via the ETSY_USAGE_COLLECTION env var. The name could
 *  not be recovered from the deployed bundle, so this default starts a FRESH
 *  daily counter; any figures already banked under the old name are not lost,
 *  just not read. Etsy's own key figures (limit/remaining) come from live
 *  response headers, so those are correct immediately either way.
 */
const USAGE_COLLECTION = process.env.ETSY_USAGE_COLLECTION || "EtsyApi_Usage";

// The app's own daily call budget, and the per-second cap. RATE_PER_SEC is the
// same constant the limiter enforces above, so the widget cannot drift from
// actual behaviour.
const DAILY_BUDGET = Number(process.env.ETSY_APP_DAILY_BUDGET || 2500);

module.exports = {
  etsyFetch,
  takeToken,
  torontoDayKey,
  USAGE_COLLECTION,
  DAILY_BUDGET,
  RATE_PER_SEC,
};