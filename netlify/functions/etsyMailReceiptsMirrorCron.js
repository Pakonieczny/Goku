/*  netlify/functions/etsyMailReceiptsMirrorCron.js
 *
 *  ═══ WHAT THIS DOES ═══════════════════════════════════════════════════
 *
 *  Scheduled function that runs every 3 minutes (cron cadence) but
 *  only does real Etsy work once every 7-10 minutes (randomized).
 *  Each successful run schedules the next eligibility window via
 *  `nextEligibleAtMs` in EtsyMail_Config/receiptsMirrorState; ticks
 *  inside that window skip without hitting Etsy.
 *
 *  Why randomized: Etsy's anti-abuse heuristics can throttle clients
 *  that hit the API on a precise deterministic schedule even when the
 *  raw QPD count is well under the daily cap. A random 7-10 min
 *  spacing keeps us under the 5,000 QPD ceiling by ~3x compared to
 *  the prior 3-min cadence AND removes the rhythmic signature.
 *
 *  When the function does run, it pulls receipts that have been
 *  created or modified on Etsy since the last successful run and
 *  mirrors them into Firestore at EtsyMail_Receipts/{receipt_id}.
 *
 *  This is the operational heartbeat of the receipts-mirror architecture.
 *  Once running, EtsyMail_Customers/{buyerUserId} aggregations no longer
 *  call Etsy at all — they query the Firestore mirror.
 *
 *  ═══ WHY THIS EXISTS ══════════════════════════════════════════════════
 *
 *  Etsy's getShopReceipts endpoint does NOT support a working
 *  `buyer_user_id` filter. Asking "give me receipts for buyer X" returns
 *  the entire shop's receipts. At 3,000 orders/month this means each
 *  buyer-sync invocation would paginate hundreds of pages of unrelated
 *  receipts to find the few belonging to that buyer — burning 5,000
 *  QPD daily quota in hours.
 *
 *  The mirror inverts the model: sync ALL recently-changed receipts
 *  every 3 min, query Firestore by buyer_user_id on demand. Etsy is hit
 *  predictably (~480 calls/day, <10% of quota) regardless of how many
 *  buyer-sync requests we make.
 *
 *  ═══ SCHEDULE ═════════════════════════════════════════════════════════
 *
 *  netlify.toml:
 *    [functions."etsyMailReceiptsMirrorCron"]
 *      schedule = "*\/3 * * * *"
 *
 *  Runtime envelope: 30 seconds (Netlify scheduled-function cap).
 *  In a normal 3-min window we expect ≤1 page of changes (≤100 receipts).
 *  Each page costs ~500ms to fetch + ~1s to batch-write. 30s is plenty
 *  even with 5-10 pages of catch-up after a transient failure.
 *
 *  ═══ STATE ════════════════════════════════════════════════════════════
 *
 *  EtsyMail_Config/receiptsMirrorState (single doc):
 *    {
 *      enabled                : true,         // master kill-switch
 *      lastSyncTimestamp      : <unix sec>,   // min_last_modified for next run
 *      lastSyncCompletedAt    : <ts>,         // when last successful run finished
 *      lastSyncCallCount      : <int>,        // Etsy calls made last run
 *      lastSyncReceiptsCount  : <int>,        // receipts written last run
 *      lastSyncOutcome        : "ok"|"error"|"skipped"|"rate_limited",
 *      lastSyncErrorMsg       : <string|null>,
 *      backfillProgress       : {
 *        status               : "idle"|"running"|"complete"|"error",
 *        startedAt            : <ts>,
 *        completedAt          : <ts|null>,
 *        totalPagesEstimate   : <int>,
 *        pagesProcessed       : <int>,
 *        receiptsProcessed    : <int>,
 *        currentOffset        : <int>,
 *        windowMinCreated     : <unix sec>,   // 24 months ago at start
 *        windowMaxCreated     : <unix sec>,
 *        errorMsg             : <string|null>
 *      }
 *    }
 *
 *  ═══ RECEIPT DOC SHAPE ════════════════════════════════════════════════
 *
 *  EtsyMail_Receipts/{receipt_id} (one doc per Etsy receipt):
 *    {
 *      receipt_id          : "123456789",
 *      buyer_user_id       : "10408187",     // INDEXED
 *      created_timestamp   : 1716400000,     // INDEXED (sec)
 *      updated_timestamp   : 1716500000,     // (sec) — drives incremental
 *      status              : "Open",
 *      is_paid             : true,
 *      is_shipped          : true,
 *      grandtotal_amount   : 45.00,          // decimal dollars
 *      grandtotal_currency : "USD",
 *      buyer_name          : "Jane Smith",
 *      raw                 : { ...full Etsy receipt JSON... },
 *      mirrorWrittenAt     : <ts>            // when WE wrote it
 *    }
 *
 *  Firestore composite index required for the buyer-lookup query:
 *    Collection: EtsyMail_Receipts
 *    Fields: buyer_user_id (asc), created_timestamp (desc)
 *
 *  Without the index, queries fail with a clickable auto-create link
 *  in the error message. First query failure is the easiest path to
 *  creating the index — Firebase shows the exact link in the response.
 *
 *  ═══ FAILURE BEHAVIOR ═════════════════════════════════════════════════
 *
 *  - Network/timeout fetching Etsy:
 *      lastSyncTimestamp is NOT advanced. Next run retries the same
 *      window. Diagnostic log records the error.
 *  - Etsy returns 429 (rate-limited):
 *      Skip this run entirely. lastSyncTimestamp unchanged. Next run
 *      (3 min later) retries. No retry-after sleeping — we just defer.
 *  - Etsy returns daily rate limit:
 *      Records to syncState.etsyDailyLimitResetAt as before. Subsequent
 *      runs short-circuit until the reset time passes.
 *  - Partial success (some pages written, then failure):
 *      Already-written receipts stay. lastSyncTimestamp advances only
 *      to the max updated_timestamp of successfully-processed receipts.
 *      Next run picks up from there. The Firestore set+merge per
 *      receipt is idempotent: re-fetching and re-writing the same
 *      receipt is harmless.
 *  - Firestore write fails:
 *      Logged, batch fails, lastSyncTimestamp NOT advanced. Next run
 *      retries the same window.
 */

"use strict";

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");
const meter = require("./_etsyApiMeter");
// v5.1 — Use the shared OAuth helper from _etsyMailEtsy.js for token
// management. Cuts redundant token-refresh calls and adds an in-memory
// cache layer that survives across invocations within the same warm
// Node process. Was previously duplicated inline here with a field-name
// mismatch (`expires_at_ms` vs `expires_at`) that caused the two paths
// to invalidate each other's cached tokens.
const { getValidEtsyAccessToken } = require("./_etsyMailEtsy");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

// ─── Config ────────────────────────────────────────────────────────────────
const SHOP_ID       = process.env.SHOP_ID;
const CLIENT_ID     = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET || process.env.ETSY_SHARED_SECRET;

const OAUTH_DOC_PATH    = "config/etsyOauth";
const MIRROR_STATE_PATH = "EtsyMail_Config/receiptsMirrorState";
const SYNC_STATE_PATH   = "EtsyMail_Config/syncState";

const TOKEN_REFRESH_BUFFER_MS = 2 * 60 * 1000;
const PAGE_SIZE       = 100;
const MAX_PAGES       = 25;  // safety cap per cron run — 30s budget allows ~10 pages typically
const FETCH_TIMEOUT_MS = 20 * 1000;
const RECENT_RECEIPTS_CAP = 10;
const CUSTOMER_REBUILD_LIMIT_PER_RUN = 100;

// ─── Money helper ──────────────────────────────────────────────────────────
function moneyAmt(m) {
  if (!m || typeof m.amount !== "number" || typeof m.divisor !== "number") return null;
  return m.amount / m.divisor;
}

// ─── Fetch one page of receipts ────────────────────────────────────────────
//
// Unlike sync-background's getReceiptsPage, this one does NOT recursively
// retry on 429 — scheduled-function budget is too tight for sleeping.
// Returns a structured outcome object so the caller can decide what to do.
async function fetchOnePage(accessToken, params) {
  const qs = new URLSearchParams(Object.fromEntries(
    Object.entries(params).filter(([_, v]) => v != null && v !== "")
  )).toString();
  const url = `https://api.etsy.com/v3/application/shops/${SHOP_ID}/receipts?${qs}`;

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

  const _meterToken = meter.bump("mirror.receiptsPage");

  let res;
  try {
    res = await fetch(url, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "x-api-key": `${CLIENT_ID}:${CLIENT_SECRET}`,
        "Content-Type": "application/json"
      },
      signal: controller.signal
    });
  } catch (err) {
    clearTimeout(timeoutId);
    _meterToken.failNet();
    if (err.name === "AbortError") {
      return { ok: false, kind: "timeout", message: "Etsy fetch timeout" };
    }
    return { ok: false, kind: "network", message: err.message };
  }
  clearTimeout(timeoutId);
  _meterToken.fromHttp(res.status);

  if (res.status === 429) {
    const retryAfter = parseInt(res.headers.get("retry-after") || "5", 10);
    const bodyText = await res.text().catch(() => "");
    const isDaily = /daily|day/i.test(bodyText) || retryAfter > 3600;
    if (isDaily) {
      // Persist the daily-lock signal so the buyer-mode handler in
      // sync-background can short-circuit until reset.
      try {
        await db.doc(SYNC_STATE_PATH).set({
          etsyDailyLimitHitAt   : FV.serverTimestamp(),
          etsyDailyLimitResetAt : admin.firestore.Timestamp.fromMillis(Date.now() + retryAfter * 1000),
          etsyDailyLimitDetail  : bodyText.slice(0, 300)
        }, { merge: true });
      } catch {}
      return { ok: false, kind: "daily_rate_limit", retryAfter, message: bodyText.slice(0, 200) };
    }
    return { ok: false, kind: "rate_limited", retryAfter, message: bodyText.slice(0, 200) };
  }

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    return { ok: false, kind: "http_error", status: res.status, message: text.slice(0, 300) };
  }

  const json = await res.json();
  return { ok: true, data: json };
}

// ─── Receipt → mirror doc transform ────────────────────────────────────────
function receiptToMirrorDoc(r) {
  const receiptId = String(r.receipt_id);
  const buyerUserId = r.buyer_user_id ? String(r.buyer_user_id) : null;
  return {
    receipt_id          : receiptId,
    receiptId           : receiptId,
    etsyOrderId         : receiptId,
    buyer_user_id       : buyerUserId,
    buyerUserId         : buyerUserId,
    created_timestamp   : r.created_timestamp || null,
    updated_timestamp   : r.updated_timestamp || null,
    status              : r.status || null,
    is_paid             : !!r.is_paid,
    is_shipped          : !!r.is_shipped,
    grandtotal_amount   : moneyAmt(r.grandtotal),
    grandtotal_currency : r.grandtotal ? r.grandtotal.currency_code : null,
    buyer_name          : r.name || null,
    raw                 : r,
    mirrorWrittenAt     : FV.serverTimestamp()
  };
}

// ─── Batch-write receipts to Firestore ─────────────────────────────────────
//
// Firestore batch write cap: 500 ops per batch. We use 100-doc batches
// (matching PAGE_SIZE) so each Etsy page maps to exactly one batch write.
async function writeReceiptBatch(receipts) {
  if (!receipts.length) return 0;
  const batch = db.batch();
  for (const r of receipts) {
    if (!r.receipt_id) continue;
    const doc = receiptToMirrorDoc(r);
    batch.set(db.collection("EtsyMail_Receipts").doc(String(r.receipt_id)), doc, { merge: true });
  }
  await batch.commit();
  return receipts.length;
}

function mirrorToSummary(m) {
  return {
    receiptId  : String(m.receipt_id),
    orderedAt  : m.created_timestamp ? m.created_timestamp * 1000 : null,
    updatedAt  : m.updated_timestamp ? m.updated_timestamp * 1000 : null,
    grandTotal : m.grandtotal_amount,
    currency   : m.grandtotal_currency,
    status     : m.status || null,
    isPaid     : !!m.is_paid,
    isShipped  : !!m.is_shipped,
    buyerUserId: m.buyer_user_id || null,
    buyerName  : m.buyer_name || null
  };
}

function isMissingFirestoreIndexError(err) {
  const msg = String((err && (err.message || err.details)) || err || "");
  return /FAILED_PRECONDITION/i.test(msg) && /requires an index/i.test(msg);
}

async function queryBuyerMirrorReceipts(buyerUserId) {
  const buyerId = String(buyerUserId);
  try {
    const snap = await db.collection("EtsyMail_Receipts")
      .where("buyer_user_id", "==", buyerId)
      .orderBy("created_timestamp", "desc")
      .limit(1000)
      .get();
    return {
      docs: snap.docs.map(d => d.data()),
      queryMode: "buyer_user_id_created_timestamp_index",
      missingIndexFallback: false,
      missingIndexErrorMsg: null
    };
  } catch (err) {
    if (!isMissingFirestoreIndexError(err)) throw err;

    const fallbackSnap = await db.collection("EtsyMail_Receipts")
      .where("buyer_user_id", "==", buyerId)
      .limit(1000)
      .get();

    const docs = fallbackSnap.docs
      .map(d => d.data())
      .sort((a, b) => Number(b.created_timestamp || 0) - Number(a.created_timestamp || 0));

    return {
      docs,
      queryMode: "buyer_user_id_single_field_fallback_sorted_in_memory",
      missingIndexFallback: true,
      missingIndexErrorMsg: String(err.message || err).slice(0, 700)
    };
  }
}

async function rebuildCustomerFromMirror(buyerUserId) {
  const queryResult = await queryBuyerMirrorReceipts(buyerUserId);

  const summaries = [];
  let displayName = null;
  let currency = null;
  let totalSpent = 0;
  let firstMs = null;
  let lastMs = null;

  for (const m of queryResult.docs) {
    const s = mirrorToSummary(m);
    summaries.push(s);
    if (s.buyerName && !displayName) displayName = s.buyerName;
    if (s.currency && !currency) currency = s.currency;
    if (typeof s.grandTotal === "number") totalSpent += s.grandTotal;
    if (s.orderedAt) {
      if (firstMs === null || s.orderedAt < firstMs) firstMs = s.orderedAt;
      if (lastMs  === null || s.orderedAt > lastMs ) lastMs  = s.orderedAt;
    }
  }

  const orderCount = summaries.length;
  const customerRef = db.collection("EtsyMail_Customers").doc(String(buyerUserId));
  const existingSnap = await customerRef.get();
  const existing = existingSnap.exists ? existingSnap.data() : null;

  const recentReceipts = summaries.slice(0, RECENT_RECEIPTS_CAP).map(s => ({
    receiptId : s.receiptId,
    orderedAt : s.orderedAt ? admin.firestore.Timestamp.fromMillis(s.orderedAt) : null,
    grandTotal: s.grandTotal,
    currency  : s.currency,
    status    : s.status,
    isPaid    : s.isPaid,
    isShipped : s.isShipped
  }));

  await customerRef.set({
    buyerUserId  : String(buyerUserId),
    displayName  : displayName || (existing && existing.displayName) || "Unknown",
    currency     : currency || (existing && existing.currency) || "USD",
    orderCount,
    totalSpent   : Math.round(totalSpent * 100) / 100,
    firstOrderAt : firstMs ? admin.firestore.Timestamp.fromMillis(firstMs) : null,
    lastOrderAt  : lastMs  ? admin.firestore.Timestamp.fromMillis(lastMs)  : null,
    isRepeatBuyer: orderCount >= 2,
    recentReceipts,
    updatedAt    : FV.serverTimestamp(),
    syncSource   : "mirror-cron",
    mirrorCustomerRebuiltAt: FV.serverTimestamp()
  }, { merge: true });

  return {
    buyerUserId: String(buyerUserId),
    receiptsProcessed: orderCount,
    queryMode: queryResult.queryMode,
    missingIndexFallback: !!queryResult.missingIndexFallback
  };
}

async function rebuildCustomersForChangedBuyers(buyerIds) {
  const ids = Array.from(new Set(Array.from(buyerIds || []).filter(Boolean).map(String)));
  const selected = ids.slice(0, CUSTOMER_REBUILD_LIMIT_PER_RUN);
  const skipped = Math.max(0, ids.length - selected.length);

  const result = {
    attempted: selected.length,
    updated: 0,
    skipped,
    errors: [],
    missingIndexFallbackCount: 0
  };

  for (const buyerId of selected) {
    try {
      const out = await rebuildCustomerFromMirror(buyerId);
      result.updated += 1;
      if (out.missingIndexFallback) result.missingIndexFallbackCount += 1;
    } catch (err) {
      result.errors.push({ buyerUserId: buyerId, error: String(err.message || err).slice(0, 500) });
    }
  }

  return result;
}

// ─── Diagnostic log helper ─────────────────────────────────────────────────
async function writeDiagLog(invocationId, payload) {
  try {
    await db.collection("EtsyMail_DiagnosticLog").doc(invocationId).set(payload, { merge: true });
  } catch (e) {
    console.warn("[mirror-cron] diagnostic write failed:", e.message);
  }
}

// ─── Main handler ──────────────────────────────────────────────────────────
//
// Wrapped with meter.wrapHandler so any meter bumps made during the
// invocation get flushed to Firestore before the container suspends.
// Without the wrapper the counts can be lost on cold-stop.
exports.handler = meter.wrapHandler(async () => {
  const invocationStartMs = Date.now();
  const invocationId = `mirror_${invocationStartMs}_${Math.random().toString(36).slice(2, 9)}`;

  // Diagnostic: record invocation start
  await writeDiagLog(invocationId, {
    invocationId,
    function     : "etsyMailReceiptsMirrorCron",
    phase        : "start",
    createdAt    : FV.serverTimestamp(),
    invocationStartMs
  });

  // Env check
  if (!SHOP_ID || !CLIENT_ID || !CLIENT_SECRET) {
    console.error("[mirror-cron] Missing env vars SHOP_ID/CLIENT_ID/CLIENT_SECRET");
    await writeDiagLog(invocationId, { phase: "end", outcome: "error", errorMsg: "missing env vars" });
    return { statusCode: 500, body: "Missing env vars" };
  }

  // ─── Gate 1: kill-switch ──────────────────────────────────────────
  let mirrorCfg = null;
  try {
    const snap = await db.doc(MIRROR_STATE_PATH).get();
    mirrorCfg = snap.exists ? snap.data() : null;
  } catch (e) {
    console.warn("[mirror-cron] state read failed (continuing with defaults):", e.message);
  }

  // Default-on: a missing doc means we have never run. We initialize and proceed.
  const enabled = mirrorCfg ? mirrorCfg.enabled !== false : true;
  if (!enabled) {
    console.log("[mirror-cron] disabled via receiptsMirrorState.enabled=false — skipping");
    await writeDiagLog(invocationId, { phase: "end", outcome: "skipped", reason: "disabled" });
    return { statusCode: 200, body: JSON.stringify({ skipped: true, reason: "disabled" }) };
  }

  // ─── Gate 1.5: randomized 7-10 min eligibility window ─────────────
  //
  // The Netlify cron fires every 3 min, but we don't want to hit Etsy
  // that often. Etsy can throttle for "too-frequent identical requests"
  // even when we're well under the 5,000 QPD daily cap. By introducing
  // a randomized 7-10 min minimum interval between runs, we (a) cut
  // the request volume to roughly 1/3 of the prior pace, and (b) avoid
  // the deterministic-cadence pattern that anti-spam heuristics catch.
  //
  // Logic: each successful run writes `nextEligibleAtMs` = now + 7..10
  // min (uniform random). Subsequent cron ticks check this before
  // doing any Etsy work and skip if we're still inside the window.
  // Failed runs do NOT bump nextEligibleAtMs, so a transient error
  // gets retried on the next tick instead of waiting another 7-10 min.
  const nextEligibleAtMs = mirrorCfg && typeof mirrorCfg.nextEligibleAtMs === "number"
    ? mirrorCfg.nextEligibleAtMs
    : 0;
  if (nextEligibleAtMs && Date.now() < nextEligibleAtMs) {
    const waitMs = nextEligibleAtMs - Date.now();
    const waitSec = Math.ceil(waitMs / 1000);
    console.log(`[mirror-cron] inside randomized interval window — ${waitSec}s remaining, skipping`);
    await writeDiagLog(invocationId, { phase: "end", outcome: "skipped", reason: "inside_jitter_window", waitSec });
    return { statusCode: 200, body: JSON.stringify({ skipped: true, reason: "inside_jitter_window", waitSec }) };
  }

  // ─── Gate 2: daily rate-limit short-circuit ───────────────────────
  try {
    const syncSnap = await db.doc(SYNC_STATE_PATH).get();
    if (syncSnap.exists) {
      const ss = syncSnap.data();
      const resetMs = ss.etsyDailyLimitResetAt && ss.etsyDailyLimitResetAt.toMillis
        ? ss.etsyDailyLimitResetAt.toMillis() : 0;
      if (resetMs > Date.now()) {
        const waitMin = Math.ceil((resetMs - Date.now()) / 60000);
        console.log(`[mirror-cron] Etsy daily limit active for ${waitMin}m — skipping`);
        await writeDiagLog(invocationId, { phase: "end", outcome: "skipped", reason: "daily_rate_limit" });
        return { statusCode: 200, body: JSON.stringify({ skipped: true, reason: "daily_rate_limit", waitMinutes: waitMin }) };
      }
    }
  } catch (e) {
    console.warn("[mirror-cron] syncState read failed (continuing):", e.message);
  }

  // ─── Gate 3: don't compete with backfill ──────────────────────────
  // If a backfill is actively running, skip this tick. Backfill makes
  // many Etsy calls per chunk; running incremental sync alongside it
  // would just compound the rate-limit risk.
  //
  // BUT also: if backfill claims to be running but has stalled (no
  // progress in STALL_THRESHOLD_MS), re-fire a chunk to restart the
  // chain. This handles the case where the chunk's fire-and-forget
  // self-trigger failed silently (network blip, container scheduling
  // issue, etc.) and the chain is now broken with status stuck at
  // "running" forever.
  const backfillStatus = mirrorCfg && mirrorCfg.backfillProgress && mirrorCfg.backfillProgress.status;
  if (backfillStatus === "running") {
    // Detect stall: lastChunkAt should be within STALL_THRESHOLD_MS.
    // If not (or if the field doesn't exist), this chunk hasn't moved
    // recently — re-fire one chunk to restart the chain.
    const STALL_THRESHOLD_MS = 5 * 60 * 1000;  // 5 minutes without progress = stalled
    const lastChunkAtMs = (function () {
      const p = mirrorCfg.backfillProgress || {};
      const t = p.lastChunkAt;
      if (!t) return 0;
      if (typeof t === "number") return t;
      if (typeof t.toMillis === "function") return t.toMillis();
      if (typeof t._seconds === "number") return t._seconds * 1000 + Math.floor((t._nanoseconds || 0) / 1e6);
      if (typeof t.seconds === "number") return t.seconds * 1000 + Math.floor((t.nanoseconds || 0) / 1e6);
      return 0;
    })();
    const stalled = lastChunkAtMs === 0 || (Date.now() - lastChunkAtMs) > STALL_THRESHOLD_MS;

    if (stalled) {
      console.log(`[mirror-cron] backfill appears STALLED (lastChunkAt=${lastChunkAtMs ? new Date(lastChunkAtMs).toISOString() : "never"}) — re-firing chunk`);
      const fnHost = process.env.URL || process.env.DEPLOY_PRIME_URL || null;
      if (fnHost) {
        // CRITICAL: await the fetch so the chunk POST leaves this
        // container before the handler returns. Background-function
        // returns are 202 and quick (~200ms), so this is a short await
        // — but without it, Netlify can suspend the container and kill
        // the pending HTTP request before it reaches the dispatcher.
        await fetch(`${fnHost}/.netlify/functions/etsyMailSync-background`, {
          method : "POST",
          headers: { "Content-Type": "application/json" },
          body   : JSON.stringify({ mode: "backfill", action: "chunk" })
        }).then(r => {
          console.log(`[mirror-cron] watchdog chunk re-fire sent, status=${r.status}`);
        }).catch(err => {
          console.warn(`[mirror-cron] watchdog chunk re-fire failed: ${err.message}`);
        });
      }
      await writeDiagLog(invocationId, { phase: "end", outcome: "skipped", reason: "backfill_running_watchdog_refired", lastChunkAtMs });
      return { statusCode: 200, body: JSON.stringify({ skipped: true, reason: "backfill_running_watchdog_refired" }) };
    }

    console.log("[mirror-cron] backfill in progress — skipping incremental");
    await writeDiagLog(invocationId, { phase: "end", outcome: "skipped", reason: "backfill_running" });
    return { statusCode: 200, body: JSON.stringify({ skipped: true, reason: "backfill_running" }) };
  }

  // ─── Compute sync window ──────────────────────────────────────────
  // First-ever run: use 5 minutes ago as the starting point. We don't
  // want a missing watermark to trigger a fetch of all-time receipts —
  // that's what the explicit backfill is for. 5 min covers any startup
  // gap.
  const NOW_SEC = Math.floor(Date.now() / 1000);
  const lastSyncTs = (mirrorCfg && typeof mirrorCfg.lastSyncTimestamp === "number")
    ? mirrorCfg.lastSyncTimestamp
    : NOW_SEC - 5 * 60;

  // Avoid the edge case where the same second's receipts get re-fetched
  // forever — we use min_last_modified which is inclusive. By default
  // dedup happens via the Firestore set+merge, but advancing by +1 each
  // run keeps the windows clean.
  const minLastModified = lastSyncTs;

  // ─── Paginate ─────────────────────────────────────────────────────
  let accessToken;
  try {
    accessToken = await getValidEtsyAccessToken();
  } catch (e) {
    console.error("[mirror-cron] OAuth fetch failed:", e.message);
    await writeDiagLog(invocationId, { phase: "end", outcome: "error", errorMsg: e.message });
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: e.message }) };
  }

  let pagesFetched = 0;
  let receiptsProcessed = 0;
  let maxUpdatedSeen = lastSyncTs;
  let offset = 0;
  let outcome = "ok";
  let lastErrorMsg = null;
  const changedBuyerIds = new Set();
  let customerRebuild = { attempted: 0, updated: 0, skipped: 0, errors: [], missingIndexFallbackCount: 0 };

  for (let page = 0; page < MAX_PAGES; page++) {
    const params = {
      limit             : PAGE_SIZE,
      offset            : offset,
      sort_on           : "updated",
      sort_order        : "asc",
      min_last_modified : minLastModified
    };

    const result = await fetchOnePage(accessToken, params);
    pagesFetched++;

    if (!result.ok) {
      // Don't advance watermark on any failure. Next run retries.
      if (result.kind === "daily_rate_limit") {
        outcome = "rate_limited";
        lastErrorMsg = "daily_rate_limit";
      } else if (result.kind === "rate_limited") {
        outcome = "rate_limited";
        lastErrorMsg = `rate_limited retry-after=${result.retryAfter}s`;
      } else {
        outcome = "error";
        lastErrorMsg = `${result.kind}: ${result.message}`;
      }
      console.warn(`[mirror-cron] fetch failed at page ${page}: ${lastErrorMsg}`);
      break;
    }

    const receipts = (result.data && result.data.results) || [];
    if (!receipts.length) break;

    // Write this page to Firestore. If write fails, abort and DO NOT
    // advance the watermark.
    try {
      await writeReceiptBatch(receipts);
    } catch (e) {
      outcome = "error";
      lastErrorMsg = `firestore_write_failed: ${e.message}`;
      console.error(`[mirror-cron] batch write failed at page ${page}:`, e.message);
      break;
    }

    // Track changed buyers so the 3-minute receipt mirror also refreshes
    // EtsyMail_Customers/{buyerUserId}. Firestore collections are created on
    // first write, so this is the path that creates the customer folders/docs.
    for (const r of receipts) {
      if (r.buyer_user_id) changedBuyerIds.add(String(r.buyer_user_id));
    }

    // Track the max updated_timestamp we successfully wrote — that's
    // our new high-water mark.
    for (const r of receipts) {
      if (r.updated_timestamp && r.updated_timestamp > maxUpdatedSeen) {
        maxUpdatedSeen = r.updated_timestamp;
      }
    }
    receiptsProcessed += receipts.length;

    // If page wasn't full, no more to fetch.
    if (receipts.length < PAGE_SIZE) break;

    offset += PAGE_SIZE;
  }

  // ─── Persist new state ────────────────────────────────────────────
  //
  // Advance lastSyncTimestamp ONLY if at least one page succeeded. If
  // outcome=error and pagesFetched=1 and that page failed, leave the
  // watermark alone — retry next run.
  //
  // We advance to (maxUpdatedSeen + 1) so the next call's
  // min_last_modified excludes anything we already processed. Etsy's
  // min_last_modified is inclusive, so without the +1 we'd re-fetch
  // every newest receipt indefinitely.
  if (outcome === "ok" && changedBuyerIds.size) {
    customerRebuild = await rebuildCustomersForChangedBuyers(changedBuyerIds);
    if (customerRebuild.errors.length) {
      lastErrorMsg = `customer_rebuild_partial_errors=${customerRebuild.errors.length}`;
      console.warn("[mirror-cron] customer rebuild had partial errors:", customerRebuild.errors.slice(0, 5));
    }
  }

  const shouldAdvanceWatermark = receiptsProcessed > 0;
  const nextWatermark = shouldAdvanceWatermark ? (maxUpdatedSeen + 1) : lastSyncTs;

  const elapsedMs = Date.now() - invocationStartMs;

  // Compute the next-eligible-at timestamp for the randomized interval
  // gate. Uniform random between 7 and 10 minutes from NOW. We persist
  // this regardless of outcome (success, error, or "no new receipts")
  // because the goal is rate-spacing toward Etsy, not retry-on-error;
  // a transient error doesn't justify re-hitting Etsy in 3 minutes.
  // (If a critical error occurs that needs faster retry, an operator
  // can manually clear the field via the Firebase console.)
  const JITTER_MIN_MS = 7 * 60 * 1000;
  const JITTER_MAX_MS = 10 * 60 * 1000;
  const jitterMs = JITTER_MIN_MS + Math.floor(Math.random() * (JITTER_MAX_MS - JITTER_MIN_MS + 1));
  const newNextEligibleAtMs = Date.now() + jitterMs;

  const statePatch = {
    enabled               : true,
    lastSyncTimestamp     : nextWatermark,
    lastSyncCompletedAt   : FV.serverTimestamp(),
    lastSyncCallCount     : pagesFetched,
    lastSyncReceiptsCount : receiptsProcessed,
    lastSyncOutcome       : outcome,
    lastSyncErrorMsg      : lastErrorMsg,
    lastSyncElapsedMs     : elapsedMs,
    // Randomized eligibility window — next run no sooner than this.
    // See Gate 1.5 in this file for the consuming logic.
    nextEligibleAtMs      : newNextEligibleAtMs,
    lastJitterMinutes     : Math.round(jitterMs / 60000),
    lastCustomerRebuildAttempted: customerRebuild.attempted,
    lastCustomerRebuildUpdated  : customerRebuild.updated,
    lastCustomerRebuildSkipped  : customerRebuild.skipped,
    lastCustomerRebuildErrorCount: customerRebuild.errors.length,
    lastCustomerRebuildMissingIndexFallbackCount: customerRebuild.missingIndexFallbackCount
  };
  try {
    await db.doc(MIRROR_STATE_PATH).set(statePatch, { merge: true });
  } catch (e) {
    console.error("[mirror-cron] failed to persist state:", e.message);
  }

  await writeDiagLog(invocationId, {
    phase             : "end",
    outcome,
    pagesFetched,
    receiptsProcessed,
    maxUpdatedSeen,
    nextWatermark,
    elapsedMs,
    customerRebuildAttempted: customerRebuild.attempted,
    customerRebuildUpdated  : customerRebuild.updated,
    customerRebuildSkipped  : customerRebuild.skipped,
    customerRebuildErrorCount: customerRebuild.errors.length,
    customerRebuildMissingIndexFallbackCount: customerRebuild.missingIndexFallbackCount,
    customerRebuildErrors   : customerRebuild.errors.slice(0, 10),
    errorMsg          : lastErrorMsg,
    endedAt           : FV.serverTimestamp()
  });

  console.log(`[mirror-cron] outcome=${outcome} pages=${pagesFetched} receipts=${receiptsProcessed} elapsed=${elapsedMs}ms newWatermark=${nextWatermark}`);

  return {
    statusCode: 200,
    body: JSON.stringify({
      ok: outcome === "ok",
      outcome,
      pagesFetched,
      receiptsProcessed,
      customerRebuildAttempted: customerRebuild.attempted,
      customerRebuildUpdated  : customerRebuild.updated,
      customerRebuildSkipped  : customerRebuild.skipped,
      customerRebuildErrorCount: customerRebuild.errors.length,
      customerRebuildMissingIndexFallbackCount: customerRebuild.missingIndexFallbackCount,
      nextWatermark,
      elapsedMs,
      errorMsg: lastErrorMsg
    })
  };
});
