/*  netlify/functions/_etsyApiUsage.js
 *
 *  Verified Etsy API usage store — the data layer behind etsyApiUsage.js
 *  and etsyApiProbe.js, and the source for the "App / Key / QPS" widget in
 *  both the Listing Generator (Index.html) and the Pricing Console
 *  (etsy-pricing.html).
 *
 *  ═══ WHY THIS EXISTS ════════════════════════════════════════════════════
 *
 *  Before this module, this site had two partial pieces and one hole:
 *
 *    _etsyApiMeter.js   counts EtsyMail's calls per instrumentation site,
 *                       but is not wired into etsyRateLimiter and does not
 *                       capture Etsy's own rate-limit headers.
 *    etsyRateLimiter.js gates every Etsy call from the pricing/listing
 *                       proxies at 5 QPS, but counts nothing at all.
 *    (hole)             NOTHING recorded Etsy's authoritative
 *                       x-limit-per-day / x-remaining-today headers, so the
 *                       whole-key "Key" row had no possible source.
 *
 *  This module fills the hole and is the single source of truth for the
 *  verified usage contract.
 *
 *  ═══ TWO INDEPENDENT MEASUREMENTS ══════════════════════════════════════
 *
 *  App  — OUR transactional count, per app id, reset at midnight Toronto.
 *         This is what each console budgets against (2,500/day = 50% of
 *         Etsy's 5,000, the rest reserved for other apps on the key).
 *
 *  Key  — Etsy's OWN meter for the whole API key, read only from response
 *         headers Etsy sends back. Never estimated, never derived from our
 *         own count. Resets at midnight UTC (Etsy's boundary, not ours).
 *
 *  Keeping these separate is the entire point: if our count and Etsy's
 *  headers ever disagree, the disagreement is visible rather than papered
 *  over by showing a single blended number.
 *
 *  ═══ FIRESTORE DOC SHAPE ════════════════════════════════════════════════
 *
 *  EtsyApi_Config/usage {
 *    day               : "2026-07-25"        (America/Toronto YYYY-MM-DD)
 *    apps: {
 *      "pricing-console"  : { count: 412, since: <ms> },
 *      "listing-generator": { count: 233, since: <ms> }
 *    }
 *    etsy: {                                  (verbatim from Etsy headers)
 *      limit_per_day   : 5000,
 *      remaining_today : 3120,
 *      reported_at     : <ms>
 *    }
 *    qps               : { max: 2.1, day: "2026-07-25" }
 *    updatedAt         : <Timestamp>
 *  }
 *
 *  ═══ FAILURE ISOLATION (non-negotiable) ════════════════════════════════
 *
 *  Recording must NEVER break or delay a real Etsy call. Every write is
 *  fire-and-forget, wrapped in try/catch, and buffered in memory so a burst
 *  of calls becomes one Firestore write. If Firestore is unreachable the
 *  meter degrades silently and Etsy traffic continues untouched. Nothing in
 *  this file may throw into a caller.
 */

"use strict";

/*  Firestore handle — SELF-CONTAINED.
 *
 *  This used to be `require("./firebaseAdmin")`. That file is NOT part of the
 *  Listing Generator deployment bundle, so on that site the require failed:
 *  either Netlify's bundler could not resolve it and the WHOLE function build
 *  broke, or the module threw at load and etsyRateLimiter's guarded require
 *  swallowed it, leaving the meter permanently and silently dead.
 *
 *  It now initialises from env directly, using exactly the same credential
 *  resolution etsyRateLimiter.js already uses (FIREBASE_SERVICE_ACCOUNT, else
 *  the split FIREBASE_* vars). If credentials are absent it degrades to a
 *  no-op meter rather than throwing — recording must never break Etsy traffic.
 */
let admin = null, db = null, FV = null;
try {
  let svc = null;
  if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    svc = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  } else {
    const { FIREBASE_PROJECT_ID, FIREBASE_CLIENT_EMAIL, FIREBASE_PRIVATE_KEY } = process.env;
    if (FIREBASE_PROJECT_ID && FIREBASE_CLIENT_EMAIL && FIREBASE_PRIVATE_KEY) {
      svc = {
        type: "service_account",
        project_id: FIREBASE_PROJECT_ID,
        client_email: FIREBASE_CLIENT_EMAIL,
        private_key: FIREBASE_PRIVATE_KEY.includes("\\n")
          ? FIREBASE_PRIVATE_KEY.replace(/\\n/g, "\n")
          : FIREBASE_PRIVATE_KEY,
      };
    }
  }
  if (svc) {
    admin = require("firebase-admin");
    if (!admin.apps.length) {
      admin.initializeApp({ credential: admin.credential.cert(svc), projectId: svc.project_id });
    }
    db = admin.firestore();
    FV = admin.firestore.FieldValue;
  } else {
    console.warn("[etsyApiUsage] No Firebase credentials in env — API usage metering is DISABLED (Etsy traffic unaffected).");
  }
} catch (e) {
  db = null; FV = null;
  console.warn("[etsyApiUsage] Firestore init failed — metering DISABLED (Etsy traffic unaffected):", e && e.message);
}

const COLL = "EtsyApi_Config";
const DOC_ID = "usage";
const HISTORY_PREFIX = "usageHistory_";

// Policy: this system may consume at most 50% of Etsy's per-key quota so
// the remainder stays available to the other apps sharing the key.
const ETSY_DAILY_LIMIT_DEFAULT = 5000;
const APP_DAILY_BUDGET = Number(process.env.ETSY_APP_DAILY_BUDGET || 2500);
const QPS_CAP = Number(process.env.ETSY_APP_QPS_CAP || 2.5);

const FLUSH_INTERVAL_MS = 2000;
const FLUSH_MAX_PENDING = 25;

// Day key in Toronto time — matches the "resets 11:59 PM Toronto" wording
// the widget shows the operator.
const DAY_FMT = new Intl.DateTimeFormat("sv-SE", {
  timeZone: "America/Toronto",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});
function todayKey() {
  return DAY_FMT.format(new Date());
}

// ─── Process-local buffers (reset on cold start; Firestore is truth) ────
const _pendingCounts = new Map(); // appId → pending increment
let _pendingHeaders = null;       // newest observed Etsy headers
let _pendingCount = 0;
let _flushTimer = null;
let _flushInFlight = null;

// Peak-QPS tracking: bucket attempt timestamps per whole second.
const _qpsBuckets = new Map();    // secondEpoch → count
let _knownMaxQps = 0;             // last value we believe is stored

function _noteQps(nowMs) {
  const sec = Math.floor(nowMs / 1000);
  _qpsBuckets.set(sec, (_qpsBuckets.get(sec) || 0) + 1);
  // Drop buckets older than 5 seconds; we only need the running peak.
  for (const k of _qpsBuckets.keys()) {
    if (k < sec - 5) _qpsBuckets.delete(k);
  }
  let peak = 0;
  for (const v of _qpsBuckets.values()) peak = Math.max(peak, v);
  return peak;
}

function _scheduleFlush() {
  if (_flushTimer) return;
  _flushTimer = setTimeout(() => {
    _flushTimer = null;
    _flush().catch(() => {});
  }, FLUSH_INTERVAL_MS);
  // Never hold the Lambda open just to flush counters.
  if (typeof _flushTimer.unref === "function") _flushTimer.unref();
}

async function _flush() {
  if (_flushInFlight) return _flushInFlight;
  if (_pendingCounts.size === 0 && !_pendingHeaders && _knownMaxQps === 0) return;
  // No Firestore handle (missing/invalid credentials) -> drop the buffer and
  // carry on. Metering is best-effort by design; it must never throw upward.
  if (!db) { _pendingCounts.clear(); _pendingHeaders = null; _pendingCount = 0; return; }

  const counts = new Map(_pendingCounts);
  const headers = _pendingHeaders;
  const maxQps = _knownMaxQps;
  _pendingCounts.clear();
  _pendingHeaders = null;
  _pendingCount = 0;

  _flushInFlight = (async () => {
    try {
      const ref = db.collection(COLL).doc(DOC_ID);
      const day = todayKey();
      const now = Date.now();

      await db.runTransaction(async (tx) => {
        const snap = await tx.get(ref);
        const data = snap.exists ? snap.data() || {} : {};
        const storedDay = data.day || null;

        // Day rollover — archive yesterday, then start clean. The Etsy
        // header block is NOT reset here: it has its own UTC reset cadence
        // and its reported_at timestamp tells the UI how fresh it is.
        let apps = data.apps || {};
        if (storedDay && storedDay !== day) {
          try {
            tx.set(
              db.collection(COLL).doc(HISTORY_PREFIX + storedDay),
              { ...data, archivedAt: FV.serverTimestamp() }
            );
          } catch (_) {}
          apps = {};
        }

        for (const [appId, inc] of counts.entries()) {
          const prev = apps[appId] || { count: 0, since: now };
          apps[appId] = {
            count: Number(prev.count || 0) + inc,
            since: Number(prev.since || now),
          };
        }

        const patch = { day, apps, updatedAt: FV.serverTimestamp() };

        if (headers) patch.etsy = headers;

        // Peak QPS is a high-water mark for the day. No atomic max operator
        // exists, so only write when we actually beat the stored value.
        const storedQps =
          data.qps && data.qps.day === day ? Number(data.qps.max || 0) : 0;
        if (maxQps > storedQps) patch.qps = { max: maxQps, day };
        else if (!data.qps || data.qps.day !== day) patch.qps = { max: storedQps, day };

        tx.set(ref, patch, { merge: true });
      });
    } catch (_) {
      // Swallow: metering must never surface as an API failure.
    } finally {
      _flushInFlight = null;
    }
  })();

  return _flushInFlight;
}

/**
 * Record one Etsy API call. Fire-and-forget — never await this in a hot
 * path and never let it throw.
 *
 * @param {string} appId  which console made the call (e.g. "pricing-console")
 * @param {object} res    the fetch Response (optional) — its rate-limit
 *                        headers are captured verbatim when present
 */
function recordCall(appId, res) {
  try {
    const app = String(appId || "unknown");
    _pendingCounts.set(app, (_pendingCounts.get(app) || 0) + 1);
    _pendingCount++;

    const peak = _noteQps(Date.now());
    if (peak > _knownMaxQps) _knownMaxQps = peak;

    captureHeaders(res);

    if (_pendingCount >= FLUSH_MAX_PENDING) _flush().catch(() => {});
    else _scheduleFlush();
  } catch (_) {
    // never propagate
  }
}

/**
 * Capture Etsy's authoritative rate-limit headers from any response.
 * Safe to call with anything; non-Etsy or header-less responses are ignored.
 */
function captureHeaders(res) {
  try {
    if (!res || !res.headers || typeof res.headers.get !== "function") return;
    const limit = Number(res.headers.get("x-limit-per-day"));
    const remaining = Number(res.headers.get("x-remaining-today"));
    if (!Number.isFinite(limit) || !Number.isFinite(remaining) || limit <= 0) return;
    _pendingHeaders = {
      limit_per_day: limit,
      remaining_today: remaining,
      reported_at: Date.now(),
    };
    _scheduleFlush();
  } catch (_) {}
}

/**
 * Read the verified usage snapshot for one app.
 * Returns the exact contract both consoles expect. `verified` is true only
 * when this really came from the store — a read failure returns
 * verified:false so the UI shows "Unavailable" rather than a fabricated 0.
 */
async function readUsage(appId) {
  const app = String(appId || "unknown");
  const now = Date.now();
  if (!db) {
    return { ok: false, unavailable: true, app,
      error: "API usage metering is not configured on this site (no Firebase credentials in env)." };
  }
  try {
    // Push anything buffered so a caller reading right after a burst sees it.
    await _flush().catch(() => {});

    const snap = await db.collection(COLL).doc(DOC_ID).get();
    const data = snap.exists ? snap.data() || {} : {};
    const day = todayKey();
    const sameDay = data.day === day;

    const appRow = (sameDay && data.apps && data.apps[app]) || null;
    const etsy = data.etsy || {};
    const qps = sameDay && data.qps ? data.qps : null;

    return {
      ok: true,
      verified: true,
      app: app,
      day,
      count: appRow ? Number(appRow.count || 0) : 0,
      count_since: appRow ? Number(appRow.since || 0) || null : null,
      budget: APP_DAILY_BUDGET,
      max_qps: qps ? Number(qps.max || 0) : 0,
      qps_cap: QPS_CAP,
      etsy_limit_per_day:
        etsy.limit_per_day == null ? null : Number(etsy.limit_per_day),
      etsy_remaining_today:
        etsy.remaining_today == null ? null : Number(etsy.remaining_today),
      etsy_reported_at:
        etsy.reported_at == null ? null : Number(etsy.reported_at),
      server_time: now,
    };
  } catch (e) {
    return {
      ok: false,
      verified: false,
      error: "Usage store unavailable: " + (e && e.message ? e.message : e),
      server_time: now,
    };
  }
}

async function flushNow() {
  try { await _flush(); } catch (_) {}
}

module.exports = {
  recordCall,
  captureHeaders,
  readUsage,
  flushNow,
  APP_DAILY_BUDGET,
  QPS_CAP,
  ETSY_DAILY_LIMIT_DEFAULT,
};
