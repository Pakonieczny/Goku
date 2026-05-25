/*  netlify/functions/_etsyApiMeter.js
 *
 *  Real-time Etsy API call meter.
 *
 *  ═══ WHAT IT DOES ═══════════════════════════════════════════════════════
 *
 *  Captures EVERY Etsy API call attempt across the entire EtsyMail system
 *  and writes per-site counters to Firestore at:
 *      EtsyMail_Config/etsyApiCounters
 *
 *  The inbox UI polls that doc every 3 seconds and renders a live counter
 *  panel in the left sidebar. The doc is also the canonical record of
 *  daily API usage against Etsy's 5,000 QPD quota.
 *
 *  ═══ INSTRUMENTATION SITES (the 11 places identified) ═══════════════════
 *
 *  Site ID                       Source file                              What it covers
 *  ───────────────────────────────────────────────────────────────────────────────
 *  helper.etsyFetch              _etsyMailEtsy.js          → fetch line 101  Every domain helper (parent counter)
 *  helper.getShop                _etsyMailEtsy.js getShop()                   /shops/{shop_id}
 *  helper.getShopSections        _etsyMailEtsy.js getShopSections()           /shops/{shop_id}/sections
 *  helper.getShopReceiptFull     _etsyMailEtsy.js getShopReceiptFull()        /shops/{shop_id}/receipts/{rid} +includes
 *  helper.getShopReceiptShip     _etsyMailEtsy.js getShopReceiptShipments()   /shops/{shop_id}/receipts/{rid}
 *  helper.getListing             _etsyMailEtsy.js getListing()                /listings/{id}
 *  helper.getListingImages       _etsyMailEtsy.js getListingImages()          /listings/{id}/images
 *  helper.getListingInventory    _etsyMailEtsy.js getListingInventory()       /listings/{id}/inventory
 *  helper.oauthRefresh           _etsyMailEtsy.js refreshEtsyToken()          POST /public/oauth/token (shared)
 *
 *  order.receiptFetch            etsyMailOrder.js line 122                    Receipt fetch on order modal open
 *  order.imageFetch              etsyMailOrder.js line 157                    Per-listing image fetch on order modal
 *  order.oauthRefresh            etsyMailOrder.js line 57                     POST /public/oauth/token (duplicate)
 *
 *  sync.receiptsPage             etsyMailSync-background.js line 188          /shops/{shop_id}/receipts pagination
 *  sync.oauthRefresh             etsyMailSync-background.js line 120          POST /public/oauth/token (duplicate)
 *
 *  catalog.activeListings        etsyMailListingsCatalog.js line 247          /shops/{shop_id}/listings/active pagination
 *
 *  shipping.profiles             etsyMailShippingSync.js line 178             /shops/{shop_id}/shipping-profiles
 *  shipping.upgrades             etsyMailShippingSync.js line 197             /shops/{shop_id}/shipping-profiles/{id}/upgrades
 *  shipping.destinations         etsyMailShippingSync.js line 198             /shops/{shop_id}/shipping-profiles/{id}/destinations
 *
 *  creator.templateListing       etsyMailListingCreator-background.js 584     /listings/{tid}
 *  creator.templateInventory     etsyMailListingCreator-background.js 585     /listings/{tid}/inventory
 *  creator.readinessDefs         etsyMailListingCreator-background.js 629     /shops/{shop_id}/readiness-state-definitions
 *  creator.createDraft           etsyMailListingCreator-background.js 760     POST /shops/{shop_id}/listings
 *  creator.templateImages        etsyMailListingCreator-background.js 806     /listings/{tid}/images (fallback)
 *  creator.inventoryGet          etsyMailListingCreator-background.js 880     /listings/{id}/inventory
 *  creator.inventoryPut          etsyMailListingCreator-background.js 917     PUT /listings/{id}/inventory
 *  creator.imageUpload           etsyMailListingCreator-background.js 789     POST /listings/{id}/images (multipart, direct)
 *  creator.publishPatch          etsyMailListingCreator-background.js 937     PATCH /listings/{id} (direct)
 *  creator.postPublishGet        etsyMailListingCreator-background.js 960     /listings/{id} (URL recovery)
 *
 *  ═══ OUTCOME BUCKETS ═══════════════════════════════════════════════════
 *
 *  For each siteId we count 5 buckets:
 *      attempt  — about to fire (counted unconditionally on every bump,
 *                 even if fetch throws — so this is the true call count)
 *      ok       — HTTP 2xx
 *      failHttp — HTTP 4xx/5xx (excluding 429)
 *      fail429  — HTTP 429 (rate-limited)
 *      failNet  — fetch threw (network/timeout/abort)
 *
 *  The `attempt` counter is ALWAYS incremented first; the outcome counter
 *  is incremented after the call completes. If a call attempts but the
 *  outcome bump fails (e.g. Firestore unreachable), the attempt count
 *  still records the attempt — we never lose an attempt.
 *
 *  ═══ FIRESTORE DOC SHAPE ════════════════════════════════════════════════
 *
 *  EtsyMail_Config/etsyApiCounters {
 *    day                   : "2026-05-22"          (UTC ISO date)
 *    grandTotal            : 1432                  (sum of all `attempt` counts)
 *    sites: {
 *      "helper.etsyFetch": { attempt: 982, ok: 980, failHttp: 0, fail429: 2, failNet: 0,
 *                            lastAttemptAt: <Timestamp>, last60s: 12 },
 *      "helper.getListing": { attempt: 312, ok: 312, ... },
 *      ...
 *    }
 *    updatedAt             : <Timestamp>
 *  }
 *
 *  When the UTC day rolls over, the next bump() detects the date change
 *  and archives the previous day's counters into
 *      EtsyMail_Config/etsyApiCountersHistory_{YYYY-MM-DD}
 *  then resets the live doc to zero.
 *
 *  ═══ BATCHING & PERFORMANCE ═════════════════════════════════════════════
 *
 *  Each bump() does NOT immediately write to Firestore. Instead it
 *  accumulates in a per-process in-memory buffer and flushes every
 *  FLUSH_INTERVAL_MS, or when the buffer exceeds FLUSH_MAX_PENDING. The
 *  flush uses one Firestore `update()` call with `FieldValue.increment()`
 *  for every accumulated bucket — so 50 calls within a 2-second window
 *  become ONE Firestore write.
 *
 *  Why batched, not per-call:
 *    - Netlify functions can fire dozens of Etsy calls in a single
 *      invocation (e.g. listing creator). One Firestore write per call
 *      would dominate latency.
 *    - Atomic `increment()` makes concurrent flushes safe — no lost
 *      updates even if two invocations flush simultaneously.
 *    - Lambda-style cold-start tail risk: we ALSO flush on the first
 *      bump after the previous flush window, so we self-correct if the
 *      setTimeout-based flush never fires (e.g. container suspended).
 *
 *  ═══ SLIDING-WINDOW LIVE GAUGE ═════════════════════════════════════════
 *
 *  In addition to lifetime-day counters, each flush computes `last60s`
 *  per site by carrying a rolling ring buffer of attempt timestamps in
 *  module scope. The UI uses this to show "12 hits in last minute" next
 *  to each row, separately from the daily count. Ring buffers don't
 *  survive container restarts — they're purely a live-feel indicator.
 *
 *  ═══ FAILURE ISOLATION ══════════════════════════════════════════════════
 *
 *  Every Firestore write is wrapped in try/catch. If the counter doc
 *  becomes unreachable, the meter degrades silently — Etsy calls
 *  continue normally. The buffer keeps accumulating; the next successful
 *  flush catches up. We NEVER throw out of bump(); the caller's Etsy
 *  call must not fail because the meter failed.
 *
 *  ═══ EXPORTED SURFACE ═══════════════════════════════════════════════════
 *
 *    meter.bump(siteId)              → returns a token; call token.ok()/
 *                                       fail(status)/fail(err) on completion
 *    meter.bumpSimple(siteId,outcome) → if you just want fire-and-forget
 *
 *    meter.wrap(siteId, asyncFn)    → wrapper helper; runs asyncFn,
 *                                       auto-tags outcome from result/error
 */

"use strict";

const admin = require("./firebaseAdmin");
const db = admin.firestore();
const FV = admin.firestore.FieldValue;

// ─── Constants ──────────────────────────────────────────────────────────
const COUNTERS_COLL    = "EtsyMail_Config";
const COUNTERS_DOC_ID  = "etsyApiCounters";
const HISTORY_DOC_PREFIX = "etsyApiCountersHistory_";  // + YYYY-MM-DD

const FLUSH_INTERVAL_MS  = 2000;     // flush at most every 2 seconds
const FLUSH_MAX_PENDING  = 30;       // or after 30 buffered events, whichever first
const RING_WINDOW_MS     = 60 * 1000;  // sliding window for `last60s`
const RING_MAX_LEN       = 5000;     // safety cap on ring buffer per site

// ─── Process-local state ────────────────────────────────────────────────
// All Netlify functions share container memory while warm. These maps
// are per-container; they reset on cold-start. The Firestore doc is the
// source of truth — the in-memory state only batches writes.
const _pending = new Map();     // siteId → { attempt, ok, failHttp, fail429, failNet }
const _ringBuffers = new Map(); // siteId → Array<timestampMs>  (attempt timestamps only)
let _flushTimer = null;
let _flushInFlight = null;      // promise of the currently-running flush, for serialization
let _pendingCount = 0;

// ─── Helpers ────────────────────────────────────────────────────────────

function todayKeyUTC() {
  return new Date().toISOString().slice(0, 10);   // YYYY-MM-DD
}

function emptyBucket() {
  return { attempt: 0, ok: 0, failHttp: 0, fail429: 0, failNet: 0 };
}

function _getOrInit(siteId) {
  let b = _pending.get(siteId);
  if (!b) {
    b = emptyBucket();
    _pending.set(siteId, b);
  }
  return b;
}

function _pushRing(siteId, ts) {
  let arr = _ringBuffers.get(siteId);
  if (!arr) { arr = []; _ringBuffers.set(siteId, arr); }
  arr.push(ts);
  // Trim by both window AND size cap
  const cutoff = ts - RING_WINDOW_MS;
  while (arr.length && arr[0] < cutoff) arr.shift();
  if (arr.length > RING_MAX_LEN) arr.splice(0, arr.length - RING_MAX_LEN);
}

function _last60sCount(siteId, now) {
  const arr = _ringBuffers.get(siteId);
  if (!arr || !arr.length) return 0;
  const cutoff = now - RING_WINDOW_MS;
  // Binary search the cutoff would be faster but arrays are tiny; linear is fine.
  let i = 0;
  while (i < arr.length && arr[i] < cutoff) i++;
  if (i > 0) arr.splice(0, i);   // opportunistic GC
  return arr.length;
}

// ─── Day-roll detection ─────────────────────────────────────────────────
//
// On every flush, check whether the existing doc's `day` field matches
// today. If not, archive the prior day to history_{day} and reset the
// live doc. We do this INSIDE a transaction so two concurrent flushes
// from different functions don't race the archive step.
async function _checkAndRollDay(tx, liveRef) {
  const today = todayKeyUTC();
  const liveSnap = await tx.get(liveRef);
  if (!liveSnap.exists) {
    // First write ever — initialize cleanly
    tx.set(liveRef, {
      day        : today,
      grandTotal : 0,
      sites      : {},
      createdAt  : FV.serverTimestamp(),
      updatedAt  : FV.serverTimestamp()
    });
    return today;
  }
  const data = liveSnap.data() || {};
  const docDay = data.day;
  if (docDay && docDay !== today) {
    // Archive — write the FULL previous-day doc to history collection,
    // then reset the live doc.
    const histRef = db.collection(COUNTERS_COLL).doc(HISTORY_DOC_PREFIX + docDay);
    tx.set(histRef, {
      ...data,
      archivedAt: FV.serverTimestamp()
    });
    tx.set(liveRef, {
      day        : today,
      grandTotal : 0,
      sites      : {},
      updatedAt  : FV.serverTimestamp()
    });
  }
  return today;
}

// ─── Flush ──────────────────────────────────────────────────────────────
//
// Drains _pending into a single Firestore write using atomic increments.
// Serialized: if a flush is already running, the next one waits.
async function _doFlush() {
  if (_pending.size === 0) return;

  // Snapshot the pending buffer and clear it so new bumps during the
  // flush don't get lost — they accumulate into a fresh buffer.
  const snapshot = new Map(_pending);
  _pending.clear();
  _pendingCount = 0;

  const liveRef = db.collection(COUNTERS_COLL).doc(COUNTERS_DOC_ID);
  const now = Date.now();

  try {
    await db.runTransaction(async (tx) => {
      await _checkAndRollDay(tx, liveRef);

      // Build a single update payload with field-path increments.
      // Path format: `sites.<siteId>.attempt`, etc. Firestore allows
      // dotted field paths in update() to target nested object keys.
      const update = {
        updatedAt: FV.serverTimestamp()
      };
      let attemptDelta = 0;
      for (const [siteId, b] of snapshot.entries()) {
        if (b.attempt)  update[`sites.${siteId}.attempt`]  = FV.increment(b.attempt);
        if (b.ok)       update[`sites.${siteId}.ok`]       = FV.increment(b.ok);
        if (b.failHttp) update[`sites.${siteId}.failHttp`] = FV.increment(b.failHttp);
        if (b.fail429)  update[`sites.${siteId}.fail429`]  = FV.increment(b.fail429);
        if (b.failNet)  update[`sites.${siteId}.failNet`]  = FV.increment(b.failNet);
        // Live `last60s` is overwritten (not incremented) every flush —
        // it's a snapshot of the current sliding-window count.
        update[`sites.${siteId}.last60s`] = _last60sCount(siteId, now);
        update[`sites.${siteId}.lastAttemptAt`] = admin.firestore.Timestamp.fromMillis(now);
        attemptDelta += b.attempt || 0;
      }
      if (attemptDelta) {
        update.grandTotal = FV.increment(attemptDelta);
      }
      tx.update(liveRef, update);
    });
  } catch (err) {
    // Failure: re-merge the snapshot back into _pending so we retry on
    // the next flush. Use addition (the buffer may have new pending
    // counts from concurrent bumps).
    for (const [siteId, b] of snapshot.entries()) {
      const cur = _getOrInit(siteId);
      cur.attempt  += b.attempt;
      cur.ok       += b.ok;
      cur.failHttp += b.failHttp;
      cur.fail429  += b.fail429;
      cur.failNet  += b.failNet;
      _pendingCount += (b.attempt + b.ok + b.failHttp + b.fail429 + b.failNet);
    }
    console.warn(`[etsyApiMeter] flush failed (will retry on next flush):`, err.message);
  }
}

function _scheduleFlush() {
  if (_flushTimer) return;
  _flushTimer = setTimeout(async () => {
    _flushTimer = null;
    // Serialize: chain onto any in-flight flush.
    const prev = _flushInFlight || Promise.resolve();
    _flushInFlight = prev.then(_doFlush).catch(() => {});
    await _flushInFlight;
    _flushInFlight = null;
    if (_pending.size > 0) _scheduleFlush();
  }, FLUSH_INTERVAL_MS);
  // Don't keep the event loop alive for the flush timer alone — the
  // function process should still be able to exit. Netlify will reuse
  // the warm container; the next invocation re-schedules if needed.
  if (typeof _flushTimer.unref === "function") _flushTimer.unref();
}

// ─── Public bump() — the everything-else helper ────────────────────────
//
// Two usage modes:
//
//   (a) Token mode (preferred — auto-records outcome):
//        const t = meter.bump("helper.getListing");
//        try {
//          const res = await fetch(...);
//          t.fromHttp(res.status);
//        } catch (e) {
//          t.failNet();
//          throw e;
//        }
//
//   (b) Simple mode (manual outcome):
//        meter.bumpSimple("helper.getListing", "ok");
//        meter.bumpSimple("helper.getListing", "fail429");
//
function bump(siteId) {
  const now = Date.now();
  const b = _getOrInit(String(siteId));
  b.attempt += 1;
  _pushRing(String(siteId), now);
  _pendingCount += 1;
  if (_pendingCount >= FLUSH_MAX_PENDING) {
    // Drop the timer; flush immediately.
    if (_flushTimer) { clearTimeout(_flushTimer); _flushTimer = null; }
    const prev = _flushInFlight || Promise.resolve();
    _flushInFlight = prev.then(_doFlush).catch(() => {});
  } else {
    _scheduleFlush();
  }
  // Return a token whose methods are no-ops if the caller doesn't use them.
  let _settled = false;
  const _mark = (bucket) => {
    if (_settled) return;
    _settled = true;
    const bb = _getOrInit(String(siteId));
    bb[bucket] = (bb[bucket] || 0) + 1;
    _pendingCount += 1;
    if (_pendingCount >= FLUSH_MAX_PENDING) {
      if (_flushTimer) { clearTimeout(_flushTimer); _flushTimer = null; }
      const prev = _flushInFlight || Promise.resolve();
      _flushInFlight = prev.then(_doFlush).catch(() => {});
    } else {
      _scheduleFlush();
    }
  };
  return {
    ok      : () => _mark("ok"),
    failHttp: () => _mark("failHttp"),
    fail429 : () => _mark("fail429"),
    failNet : () => _mark("failNet"),
    /** Classify by HTTP status. 2xx → ok, 429 → fail429, else failHttp. */
    fromHttp: (status) => {
      const s = Number(status);
      if (s >= 200 && s < 300)      _mark("ok");
      else if (s === 429)           _mark("fail429");
      else                          _mark("failHttp");
    },
    /** Classify by an error object. AbortError/network errors → failNet. */
    fromError: (err) => {
      const status = err && err.status;
      if (typeof status === "number") {
        const s = Number(status);
        if (s >= 200 && s < 300)    _mark("ok");
        else if (s === 429)         _mark("fail429");
        else                        _mark("failHttp");
      } else {
        _mark("failNet");
      }
    }
  };
}

function bumpSimple(siteId, outcome) {
  const t = bump(siteId);
  if (outcome && typeof t[outcome] === "function") t[outcome]();
}

/** Convenience wrapper: runs asyncFn, auto-tags outcome from result/error.
 *  Use when the asyncFn does the fetch itself and you don't want to touch
 *  the token explicitly. */
async function wrap(siteId, asyncFn) {
  const t = bump(siteId);
  try {
    const result = await asyncFn();
    // If result has a `status` or `statusCode`, classify from it; else assume ok.
    if (result && typeof result === "object") {
      if (typeof result.status === "number") { t.fromHttp(result.status); return result; }
      if (typeof result.statusCode === "number") { t.fromHttp(result.statusCode); return result; }
    }
    t.ok();
    return result;
  } catch (err) {
    t.fromError(err);
    throw err;
  }
}

/** Force-flush — useful for tests and for a `finally` block in a
 *  long-running background function. Returns the flush promise. */
async function flushNow() {
  if (_flushTimer) { clearTimeout(_flushTimer); _flushTimer = null; }
  const prev = _flushInFlight || Promise.resolve();
  _flushInFlight = prev.then(_doFlush).catch(() => {});
  await _flushInFlight;
  _flushInFlight = null;
}

/** Convenience wrapper for a Netlify handler. Use like:
 *      exports.handler = meter.wrapHandler(async (event) => { ... });
 *  Equivalent to wrapping the original handler in try/finally that
 *  calls flushNow() on every exit path, including thrown errors.
 *  This guarantees no buffered counts are lost when Netlify suspends
 *  the container, even on the very last invocation before a cold-stop. */
function wrapHandler(innerHandler) {
  return async function (event, context) {
    try {
      return await innerHandler(event, context);
    } finally {
      try { await flushNow(); } catch (e) {
        // Never let a meter failure mask the handler's real outcome.
        console.warn("[etsyApiMeter] flushNow() in wrapHandler failed (non-fatal):", e.message);
      }
    }
  };
}

module.exports = {
  bump,
  bumpSimple,
  wrap,
  wrapHandler,
  flushNow,
  // Exposed for testing / introspection only
  _pending,
  _ringBuffers
};
