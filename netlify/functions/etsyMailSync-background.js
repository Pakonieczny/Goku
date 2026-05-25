/*  netlify/functions/etsyMailSync-background.js
 *
 *  ═══ WHAT THIS DOES ═══════════════════════════════════════════════════
 *
 *  Background function (15-min budget) supporting two modes:
 *
 *    mode = "buyer"      — On-demand aggregation of a single buyer's
 *                          customer doc. Reads receipts FROM THE
 *                          FIRESTORE MIRROR (EtsyMail_Receipts) — does
 *                          NOT call Etsy. Fast, free (1 Firestore
 *                          query + 1 write).
 *
 *    mode = "backfill"   — One-time historical pull. Paginates Etsy's
 *                          receipts endpoint over a date window
 *                          (default 24 months), writing each receipt
 *                          to EtsyMail_Receipts. Runs in CHUNKS — one
 *                          invocation does up to MAX_BACKFILL_PAGES
 *                          then returns. The UI watches Firestore
 *                          progress and re-fires the function for the
 *                          next chunk until complete.
 *
 *  ═══ ARCHITECTURAL CONTEXT ════════════════════════════════════════════
 *
 *  In May 2026 we discovered that Etsy's getShopReceipts endpoint does
 *  not honor a `buyer_user_id` filter — passing it returns the entire
 *  shop's receipts. At 3,000 orders/month this meant each buyer-sync
 *  invocation paginated up to 12,000 unrelated receipts to find a
 *  handful belonging to the requested buyer, burning daily quota in
 *  hours.
 *
 *  The architectural fix is a Firestore mirror:
 *
 *    etsyMailReceiptsMirrorCron.js   — Every 3 min, pulls receipts
 *                                      modified since last run, writes
 *                                      them to EtsyMail_Receipts.
 *
 *    THIS FILE buyer mode             — Queries the mirror by
 *                                      buyer_user_id. No Etsy calls.
 *
 *    THIS FILE backfill mode          — One-time historical population
 *                                      of the mirror.
 *
 *  After backfill, buyer-sync is essentially free regardless of how
 *  often it's invoked. Snapshot/draftReply triggers can fire on every
 *  scrape without quota concerns.
 *
 *  ═══ INVOCATION ═══════════════════════════════════════════════════════
 *
 *  POST /.netlify/functions/etsyMailSync-background
 *    { mode: "buyer", buyerUserId: "<numeric id>" }
 *
 *  POST /.netlify/functions/etsyMailSync-background
 *    { mode: "backfill", action: "start" }    — Begin a new backfill
 *    { mode: "backfill", action: "chunk"  }    — Process next chunk
 *    { mode: "backfill", action: "cancel" }    — Abort an in-progress backfill
 *
 *  ═══ CUSTOMER DOC SHAPE (unchanged from prior versions) ═══════════════
 *
 *  EtsyMail_Customers/{buyerUserId}:
 *    {
 *      buyerUserId, displayName, currency,
 *      orderCount, totalSpent,
 *      firstOrderAt, lastOrderAt,
 *      isRepeatBuyer,
 *      recentReceipts: [
 *        { receiptId, orderedAt, grandTotal, currency, status, isPaid, isShipped }
 *      ],
 *      updatedAt
 *    }
 */

"use strict";

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");
const meter = require("./_etsyApiMeter");

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

const PAGE_SIZE = 100;
const RECENT_RECEIPTS_CAP = 10;

// Backfill tuning
const BACKFILL_WINDOW_MONTHS = 24;
const MAX_BACKFILL_PAGES_PER_CHUNK = 60;   // ~60 pages × ~500ms = ~30s; well within 15-min budget
const BACKFILL_PAUSE_MS = 100;             // tiny delay between pages to be polite
const FETCH_TIMEOUT_MS = 30 * 1000;
const MAX_INVOCATION_MS = 13 * 60 * 1000;  // leave 2 min cleanup tail

// ─── OAuth — uses shared helper from _etsyMailEtsy.js ──────────────────────
// v5.1: Was previously duplicated inline here with field-name mismatch
// (expires_at_ms vs _etsyMailEtsy's expires_at) that caused the two
// implementations to invalidate each other's cached tokens. Unified
// helper adds in-memory module-level caching that survives across
// invocations within the same warm Node process.
const { getValidEtsyAccessToken } = require("./_etsyMailEtsy");

// ─── Money helper ──────────────────────────────────────────────────────────
function moneyAmt(m) {
  if (!m || typeof m.amount !== "number" || typeof m.divisor !== "number") return null;
  return m.amount / m.divisor;
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ─── Fetch one page (used by backfill) ─────────────────────────────────────
//
// Backfill-specific page fetch. On 429 daily-limit, writes the lock and
// returns failure — the caller (chunk runner) records the failure to
// backfillProgress.errorMsg and stops.
async function fetchBackfillPage(accessToken, params) {
  const qs = new URLSearchParams(Object.fromEntries(
    Object.entries(params).filter(([_, v]) => v != null && v !== "")
  )).toString();
  const url = `https://api.etsy.com/v3/application/shops/${SHOP_ID}/receipts?${qs}`;

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

  const _meterToken = meter.bump("backfill.receiptsPage");

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

  return { ok: true, data: await res.json() };
}

// ─── Receipt → mirror doc transform ────────────────────────────────────────
function receiptToMirrorDoc(r) {
  return {
    receipt_id          : String(r.receipt_id),
    buyer_user_id       : r.buyer_user_id ? String(r.buyer_user_id) : null,
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

// ─── Targeted receipt hydrate ─────────────────────────────────────────────
//
// The normal mirror cron is the cheap, predictable path. But Etsy message
// pages can expose an exact receipt/order number before the scheduled mirror
// has captured it, or after a mirror-watermark stall. In that case the UI can
// correctly show "Help request" for an order while the customer panel says
// "no purchase history" because buyer-mode only queries Firestore.
//
// When snapshot passes a receiptId, buyer-mode first ensures that specific
// receipt is present in EtsyMail_Receipts. This costs at most one Etsy call per
// previously-unmirrored order, then all future buyer aggregations remain free.
async function fetchReceiptById(accessToken, receiptId) {
  const url =
    `https://api.etsy.com/v3/application/shops/${SHOP_ID}` +
    `/receipts/${encodeURIComponent(String(receiptId))}?includes=` +
    ["Transactions", "Transactions.personalization", "Transactions.variations"].join(",");

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  const _meterToken = meter.bump("sync.targetReceiptHydrate");

  let res;
  try {
    res = await fetch(url, {
      headers: {
        Authorization : `Bearer ${accessToken}`,
        "x-api-key"   : `${CLIENT_ID}:${CLIENT_SECRET}`,
        "Content-Type": "application/json"
      },
      signal: controller.signal
    });
  } catch (err) {
    clearTimeout(timeoutId);
    _meterToken.failNet();
    if (err.name === "AbortError") {
      return { ok: false, kind: "timeout", message: "Etsy receipt fetch timeout" };
    }
    return { ok: false, kind: "network", message: err.message };
  }
  clearTimeout(timeoutId);
  _meterToken.fromHttp(res.status);

  const text = await res.text().catch(() => "");
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch {}

  if (res.status === 429) {
    const retryAfter = parseInt(res.headers.get("retry-after") || "5", 10);
    const isDaily = /daily|day/i.test(text) || retryAfter > 3600;
    if (isDaily) {
      try {
        await db.doc(SYNC_STATE_PATH).set({
          etsyDailyLimitHitAt   : FV.serverTimestamp(),
          etsyDailyLimitResetAt : admin.firestore.Timestamp.fromMillis(Date.now() + retryAfter * 1000),
          etsyDailyLimitDetail  : text.slice(0, 300)
        }, { merge: true });
      } catch {}
      return { ok: false, kind: "daily_rate_limit", retryAfter, message: text.slice(0, 200) };
    }
    return { ok: false, kind: "rate_limited", retryAfter, message: text.slice(0, 200) };
  }

  if (!res.ok) {
    const msg = (data && data.error) || text || `Etsy API ${res.status}`;
    return { ok: false, kind: "http_error", status: res.status, message: String(msg).slice(0, 300) };
  }

  return { ok: true, data };
}

async function ensureReceiptMirroredById({ receiptId, expectedBuyerUserId, threadId }) {
  if (!receiptId || !/^\d+$/.test(String(receiptId))) {
    return { attempted: false, mirrored: false, reason: "no_valid_receipt_id" };
  }

  const receiptDocRef = db.collection("EtsyMail_Receipts").doc(String(receiptId));
  const existingSnap = await receiptDocRef.get();
  if (existingSnap.exists) {
    const existing = existingSnap.data() || {};
    if (existing.buyer_user_id) {
      return {
        attempted  : false,
        mirrored   : true,
        source     : "existing_mirror",
        receiptId  : String(receiptId),
        buyerUserId: String(existing.buyer_user_id)
      };
    }
  }

  if (!SHOP_ID || !CLIENT_ID || !CLIENT_SECRET) {
    return {
      attempted: false,
      mirrored : false,
      reason   : "missing_env_vars"
    };
  }

  const accessToken = await getValidEtsyAccessToken();
  const fetched = await fetchReceiptById(accessToken, receiptId);
  if (!fetched.ok) {
    return {
      attempted: true,
      mirrored : false,
      receiptId: String(receiptId),
      errorKind: fetched.kind,
      errorMsg : fetched.message || null
    };
  }

  const receipt = fetched.data || {};
  if (!receipt.receipt_id) receipt.receipt_id = String(receiptId);

  const mirroredDoc = receiptToMirrorDoc(receipt);
  await receiptDocRef.set(mirroredDoc, { merge: true });

  const receiptBuyerId = receipt.buyer_user_id ? String(receipt.buyer_user_id) : null;

  // Patch the thread doc with the resolved buyerUserId whenever it's
  // useful: either the scrape captured nothing (expectedBuyerUserId is
  // null) and we just resolved one from the receipt, OR the scrape
  // captured a value and the receipt disagrees (correct the stale ID).
  // Both cases need the same write so the UI's customer-panel lookup
  // resolves on the next refresh.
  //
  // v4.4.1 — Original gate was `expectedBuyerUserId && receiptBuyerId !==
  // String(expectedBuyerUserId)`, which silently dropped the patch when
  // the scrape had no buyerUserId at all (the most common help-request
  // failure mode). That left the customer doc correctly written under
  // the resolved ID but the thread still pointing at nothing, so the UI
  // kept showing "No purchase history" forever.
  const expectedAsString = expectedBuyerUserId ? String(expectedBuyerUserId) : null;
  const shouldPatchThread = threadId
    && receiptBuyerId
    && (!expectedAsString || receiptBuyerId !== expectedAsString);

  if (shouldPatchThread) {
    try {
      const patch = {
        buyerUserId: receiptBuyerId,
        buyerUserIdResolvedAt: FV.serverTimestamp(),
        buyerUserIdResolvedSource: "target_receipt_hydrate"
      };
      if (expectedAsString) {
        patch.buyerUserIdCorrectedFrom = expectedAsString;
        patch.buyerUserIdCorrectedAt = FV.serverTimestamp();
        patch.buyerUserIdCorrectionSource = "target_receipt_hydrate";
      }
      await db.collection("EtsyMail_Threads").doc(String(threadId)).set(patch, { merge: true });
    } catch (e) {
      console.warn(`[buyer-sync] failed to patch thread ${threadId} buyerUserId:`, e.message);
    }
  }

  return {
    attempted  : true,
    mirrored   : true,
    source     : "target_receipt_hydrate",
    receiptId  : String(receiptId),
    buyerUserId: receiptBuyerId
  };
}

// ─── Receipt → customer summary transform (for buyer-mode aggregation) ────
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

// ─── Buyer-mode: aggregate from mirror ─────────────────────────────────────
//
// Reads ALL receipts for the buyer from the mirror, computes totals,
// writes the customer doc. If a specific receiptId was provided and that
// receipt is missing from the mirror, hydrates only that receipt first.
//
// Returns: { receiptsProcessed, customersUpdated, pagesFetched, buyerUserId }
// (pagesFetched stays at 0 — only targeted receipt hydrate may hit Etsy.)
async function runBuyerSyncFromMirror({ buyerUserId, receiptId = null, threadId = null }) {
  let effectiveBuyerUserId = buyerUserId ? String(buyerUserId) : null;

  const targetReceiptHydrate = await ensureReceiptMirroredById({
    receiptId,
    expectedBuyerUserId: effectiveBuyerUserId,
    threadId
  });

  if (targetReceiptHydrate.buyerUserId) {
    effectiveBuyerUserId = String(targetReceiptHydrate.buyerUserId);
  }

  if (!effectiveBuyerUserId) {
    throw new Error("runBuyerSyncFromMirror requires buyerUserId or a receiptId that resolves to buyer_user_id");
  }

  // Query the mirror. We pull up to 1000 most recent receipts for the
  // buyer — that's far more than RECENT_RECEIPTS_CAP, but we use the
  // full set to compute orderCount and totalSpent accurately. A buyer
  // with >1000 orders is implausible for our shop.
  const snap = await db.collection("EtsyMail_Receipts")
    .where("buyer_user_id", "==", String(effectiveBuyerUserId))
    .orderBy("created_timestamp", "desc")
    .limit(1000)
    .get();

  const summaries = [];
  let displayName = null;
  let currency = null;
  let totalSpent = 0;
  let firstMs = null;
  let lastMs = null;

  snap.forEach(doc => {
    const m = doc.data();
    const s = mirrorToSummary(m);
    summaries.push(s);
    if (s.buyerName && !displayName) displayName = s.buyerName;
    if (s.currency && !currency) currency = s.currency;
    if (typeof s.grandTotal === "number") totalSpent += s.grandTotal;
    if (s.orderedAt) {
      if (firstMs === null || s.orderedAt < firstMs) firstMs = s.orderedAt;
      if (lastMs  === null || s.orderedAt > lastMs ) lastMs  = s.orderedAt;
    }
  });

  const orderCount = summaries.length;

  // Build recentReceipts capped at RECENT_RECEIPTS_CAP, newest-first
  const recentReceipts = summaries
    .slice(0, RECENT_RECEIPTS_CAP)
    .map(s => ({
      receiptId : s.receiptId,
      orderedAt : s.orderedAt ? admin.firestore.Timestamp.fromMillis(s.orderedAt) : null,
      grandTotal: s.grandTotal,
      currency  : s.currency,
      status    : s.status,
      isPaid    : s.isPaid,
      isShipped : s.isShipped
    }));

  // Load existing customer doc to preserve fields we don't compute
  const customerRef = db.collection("EtsyMail_Customers").doc(String(effectiveBuyerUserId));
  const existingSnap = await customerRef.get();
  const existing = existingSnap.exists ? existingSnap.data() : null;

  const finalDisplayName = displayName || (existing && existing.displayName) || "Unknown";
  const finalCurrency    = currency    || (existing && existing.currency)    || "USD";

  // Compute the next-eligible-at timestamp for the per-buyer debounce.
  // Uniform random between 3 and 5 minutes from NOW. The gate at the
  // top of the handler reads this field to decide whether to short-
  // circuit incoming buyer-sync requests for this buyer.
  const BUYER_DEBOUNCE_MIN_MS = 3 * 60 * 1000;
  const BUYER_DEBOUNCE_MAX_MS = 5 * 60 * 1000;
  const buyerDebounceJitterMs = BUYER_DEBOUNCE_MIN_MS
    + Math.floor(Math.random() * (BUYER_DEBOUNCE_MAX_MS - BUYER_DEBOUNCE_MIN_MS + 1));
  const nextBuyerSyncEligibleAtMs = Date.now() + buyerDebounceJitterMs;

  const customerDoc = {
    buyerUserId  : String(effectiveBuyerUserId),
    displayName  : finalDisplayName,
    currency     : finalCurrency,
    orderCount,
    totalSpent   : Math.round(totalSpent * 100) / 100,
    firstOrderAt : firstMs ? admin.firestore.Timestamp.fromMillis(firstMs) : null,
    lastOrderAt  : lastMs  ? admin.firestore.Timestamp.fromMillis(lastMs)  : null,
    isRepeatBuyer: orderCount >= 2,
    recentReceipts,
    updatedAt    : FV.serverTimestamp(),
    // Per-buyer debounce: skip incoming buyer-sync requests for this
    // buyer until this timestamp. Window is randomized 3-5 min per
    // sync to defeat deterministic-cadence patterns.
    nextBuyerSyncEligibleAtMs,
    lastBuyerSyncJitterSeconds: Math.round(buyerDebounceJitterMs / 1000),
    // Kept for monitoring/debug visibility — answers "when did this
    // customer last get synced?" at a glance in the Firebase console.
    lastBuyerSyncAt: FV.serverTimestamp(),
    // Track the data source so any debugging can confirm which path wrote this
    syncSource   : "mirror"
  };

  await customerRef.set(customerDoc, { merge: true });

  return {
    receiptsProcessed: orderCount,
    customersUpdated : 1,
    pagesFetched     : 0,
    buyerUserId      : String(effectiveBuyerUserId),
    requestedBuyerUserId: buyerUserId ? String(buyerUserId) : null,
    receiptId        : receiptId ? String(receiptId) : null,
    targetReceiptHydrate,
    source           : targetReceiptHydrate && targetReceiptHydrate.mirrored ? "mirror_with_target_receipt" : "mirror"
  };
}

// ─── Backfill — single chunk runner ────────────────────────────────────────
//
// One invocation processes up to MAX_BACKFILL_PAGES_PER_CHUNK pages then
// returns. Caller (UI poller) detects status=running and re-fires the
// function. State lives in EtsyMail_Config/receiptsMirrorState.backfillProgress.
//
// action="start"  → Initialize progress doc. Returns immediately so the
//                   UI can show progress=0. The very next chunk picks up.
// action="chunk"  → Process the next chunk. Updates progress incrementally.
// action="cancel" → Mark progress.status=idle, leaving partial data in place.
async function runBackfill({ action, invocationStartMs }) {
  const deadlineMs = invocationStartMs + MAX_INVOCATION_MS;
  const mirrorRef = db.doc(MIRROR_STATE_PATH);

  // ─── action: cancel ───────────────────────────────────────────────
  if (action === "cancel") {
    await mirrorRef.set({
      backfillProgress: {
        status      : "idle",
        cancelledAt : FV.serverTimestamp()
      }
    }, { merge: true });
    return { ok: true, action: "cancel", status: "idle" };
  }

  // ─── action: start ────────────────────────────────────────────────
  if (action === "start") {
    const now = Date.now();
    const minCreatedSec = Math.floor((now - BACKFILL_WINDOW_MONTHS * 30 * 24 * 60 * 60 * 1000) / 1000);
    const maxCreatedSec = Math.floor(now / 1000);

    const initialProgress = {
      status            : "running",
      startedAt         : FV.serverTimestamp(),
      completedAt       : null,
      totalPagesEstimate: null,    // unknown until first page lands
      pagesProcessed    : 0,
      receiptsProcessed : 0,
      currentOffset     : 0,
      windowMinCreated  : minCreatedSec,
      windowMaxCreated  : maxCreatedSec,
      errorMsg          : null
    };

    await mirrorRef.set({
      backfillProgress: initialProgress
    }, { merge: true });

    return { ok: true, action: "start", status: "running", progress: initialProgress };
  }

  // ─── action: resume ───────────────────────────────────────────────
  // Continues a previously-paused backfill from its saved currentOffset.
  // Unlike "start", this does NOT reset offset/pagesProcessed/receipts-
  // Processed — it just flips status back to "running", clears the
  // errorMsg, and triggers the first chunk. The chain self-continues
  // from there as long as chunks succeed.
  //
  // Safe to call repeatedly: each call just resets status+errorMsg and
  // re-fires a chunk, which will pick up from currentOffset wherever
  // it last committed.
  if (action === "resume") {
    const snap = await mirrorRef.get();
    const cfg = snap.exists ? snap.data() : null;
    const progress = cfg && cfg.backfillProgress;
    if (!progress) {
      return { ok: false, reason: "no_progress_to_resume", action: "resume" };
    }
    // Don't resume if the window itself is missing — that means no
    // backfill was ever started in this Firestore environment.
    if (typeof progress.windowMinCreated !== "number" ||
        typeof progress.windowMaxCreated !== "number") {
      return { ok: false, reason: "window_missing_run_start_first", action: "resume" };
    }
    await mirrorRef.set({
      backfillProgress: {
        status     : "running",
        errorMsg   : null,
        resumedAt  : FV.serverTimestamp()
      }
    }, { merge: true });

    // Kick off the first chunk so the chain restarts.
    //
    // CRITICAL: we MUST initiate the fetch synchronously (no setTimeout)
    // and `await` the fetch promise begin — otherwise Netlify suspends
    // the container after the handler returns, killing the pending HTTP
    // request before it leaves the box. By awaiting `fetch().catch(...)`
    // (which resolves quickly because background functions return 202
    // within ~200ms), we ensure the next chunk is in Netlify's queue
    // before this handler completes.
    const fnHost = process.env.URL || process.env.DEPLOY_PRIME_URL || null;
    if (fnHost) {
      // The fetch begins immediately. We .catch() so a rejection doesn't
      // throw out of the handler — instead it logs and we move on.
      // The await suspends this handler just long enough for Netlify's
      // background-function dispatcher to ACK the chunk POST.
      await fetch(`${fnHost}/.netlify/functions/etsyMailSync-background`, {
        method : "POST",
        headers: { "Content-Type": "application/json" },
        body   : JSON.stringify({ mode: "backfill", action: "chunk" })
      }).then(r => {
        console.log(`[backfill] resume chunk trigger sent, status=${r.status}`);
      }).catch(err => {
        console.warn(`[backfill] resume chunk trigger failed: ${err.message}`);
      });
    } else {
      console.warn("[backfill] no fnHost — cannot trigger first chunk after resume");
    }
    return {
      ok                : true,
      action            : "resume",
      status            : "running",
      currentOffset     : progress.currentOffset || 0,
      pagesProcessed    : progress.pagesProcessed || 0,
      receiptsProcessed : progress.receiptsProcessed || 0
    };
  }

  // ─── action: chunk (default) ──────────────────────────────────────
  // Read existing progress; abort if not running.
  const snap = await mirrorRef.get();
  const cfg = snap.exists ? snap.data() : null;
  const progress = cfg && cfg.backfillProgress;
  if (!progress || progress.status !== "running") {
    return {
      ok    : false,
      reason: "not_running",
      progress
    };
  }

  // Daily rate-limit short-circuit
  try {
    const syncSnap = await db.doc(SYNC_STATE_PATH).get();
    if (syncSnap.exists) {
      const ss = syncSnap.data();
      const resetMs = ss.etsyDailyLimitResetAt && ss.etsyDailyLimitResetAt.toMillis
        ? ss.etsyDailyLimitResetAt.toMillis() : 0;
      if (resetMs > Date.now()) {
        await mirrorRef.set({
          backfillProgress: {
            errorMsg: `daily_rate_limit until ${new Date(resetMs).toISOString()}`
          }
        }, { merge: true });
        return { ok: false, reason: "daily_rate_limit", waitMs: resetMs - Date.now() };
      }
    }
  } catch {}

  const accessToken = await getValidEtsyAccessToken();

  let offset = progress.currentOffset || 0;
  let pagesProcessed = progress.pagesProcessed || 0;
  let receiptsProcessed = progress.receiptsProcessed || 0;
  let done = false;
  let errorMsg = null;
  let pagesThisChunk = 0;

  while (pagesThisChunk < MAX_BACKFILL_PAGES_PER_CHUNK) {
    if (Date.now() > deadlineMs) {
      console.log("[backfill] approaching invocation deadline — stopping chunk");
      break;
    }

    const params = {
      limit       : PAGE_SIZE,
      offset      : offset,
      sort_on     : "created",
      sort_order  : "desc",
      min_created : progress.windowMinCreated,
      max_created : progress.windowMaxCreated
    };

    const result = await fetchBackfillPage(accessToken, params);
    pagesThisChunk++;

    if (!result.ok) {
      errorMsg = `${result.kind}: ${result.message}`;
      console.warn(`[backfill] page fetch failed at offset ${offset}: ${errorMsg}`);
      // For rate_limited (per-second) — caller can retry next chunk
      // For daily_rate_limit — caller will skip until reset
      // For network/http_error — caller can retry
      break;
    }

    const receipts = (result.data && result.data.results) || [];
    const totalCount = result.data && result.data.count;
    if (totalCount && !progress.totalPagesEstimate) {
      progress.totalPagesEstimate = Math.ceil(totalCount / PAGE_SIZE);
    }

    if (!receipts.length) {
      done = true;
      break;
    }

    try {
      await writeReceiptBatch(receipts);
    } catch (e) {
      errorMsg = `firestore_write_failed: ${e.message}`;
      console.error(`[backfill] batch write failed at offset ${offset}: ${e.message}`);
      break;
    }

    pagesProcessed++;
    receiptsProcessed += receipts.length;
    offset += PAGE_SIZE;

    if (receipts.length < PAGE_SIZE) {
      done = true;
      break;
    }

    if (BACKFILL_PAUSE_MS > 0) await sleep(BACKFILL_PAUSE_MS);
  }

  // ─── Persist updated progress ─────────────────────────────────────
  const newStatus = done ? "complete" : (errorMsg ? "error" : "running");
  const updatedProgress = {
    status            : newStatus,
    pagesProcessed,
    receiptsProcessed,
    currentOffset     : offset,
    errorMsg,
    // Heartbeat for the mirror-cron watchdog: if backfill is "running"
    // but this timestamp is stale, the cron re-fires a chunk to restart
    // the chain (covers fire-and-forget self-trigger failures).
    lastChunkAt       : FV.serverTimestamp()
  };
  if (progress.totalPagesEstimate) {
    updatedProgress.totalPagesEstimate = progress.totalPagesEstimate;
  }
  if (done) {
    updatedProgress.completedAt = FV.serverTimestamp();
  }

  await mirrorRef.set({
    backfillProgress: updatedProgress
  }, { merge: true });

  // ─── Self-trigger next chunk ──────────────────────────────────────
  //
  // If the chunk wrapped up with status "running" (more work to do, no
  // error), fire-and-forget a POST to ourselves so the next chunk
  // starts immediately. This makes backfill progress autonomous: the
  // UI just observes, the chain continues even if the operator closes
  // the inbox or the browser. Errors stop the chain (caller must
  // re-start).
  //
  // CRITICAL: the fetch is awaited (not setTimeout'd) so the request
  // leaves this container BEFORE the handler returns. Without this,
  // Netlify suspends the container after the return and the pending
  // HTTP request gets killed before it reaches Netlify's dispatcher.
  // Background functions return 202 quickly (~200ms) so the await is
  // short — and we .catch() so a rejection doesn't bubble up.
  if (newStatus === "running") {
    const fnHost = process.env.URL || process.env.DEPLOY_PRIME_URL || null;
    if (fnHost) {
      // The Firestore write above has already completed (we `await`ed
      // it), so the next chunk will see the updated currentOffset
      // when it reads. No delay needed.
      await fetch(`${fnHost}/.netlify/functions/etsyMailSync-background`, {
        method : "POST",
        headers: { "Content-Type": "application/json" },
        body   : JSON.stringify({ mode: "backfill", action: "chunk" })
      }).then(r => {
        console.log(`[backfill] self-trigger next chunk sent, status=${r.status}`);
      }).catch(err => {
        console.warn(`[backfill] self-trigger next chunk failed: ${err.message}`);
      });
    } else {
      console.warn("[backfill] no fnHost — cannot self-trigger next chunk");
    }
  }

  return {
    ok                : !errorMsg,
    action            : "chunk",
    status            : newStatus,
    pagesThisChunk,
    pagesProcessed,
    receiptsProcessed,
    offset,
    errorMsg,
    done
  };
}

// ─── Diagnostic log helper ─────────────────────────────────────────────────
//
// Returns a promise. Caller should `await` it to ensure the write commits
// before the handler returns (otherwise Netlify may suspend the container
// before the write lands in Firestore). The internal try/catch ensures
// diagnostic failures never throw — the awaited promise always resolves.
async function writeDiagLog(invocationId, payload) {
  try {
    await db.collection("EtsyMail_DiagnosticLog").doc(invocationId).set(payload, { merge: true });
  } catch (e) {
    console.warn("[sync-bg] diagnostic write failed:", e.message);
  }
}

// ─── Handler ───────────────────────────────────────────────────────────────
exports.handler = meter.wrapHandler(async (event) => {
  const invocationStartMs = Date.now();
  const invocationId = `sync_${invocationStartMs}_${Math.random().toString(36).slice(2, 9)}`;

  // Diagnostic doc — start
  const _h = event.headers || {};
  await writeDiagLog(invocationId, {
    invocationId,
    createdAt    : FV.serverTimestamp(),
    invocationStartMs,
    function     : "etsyMailSync-background",
    phase        : "start",
    callerUA     : (_h["user-agent"]      || _h["User-Agent"]      || null),
    callerReferer: (_h["referer"]         || _h["Referer"]         || null),
    callerOrigin : (_h["origin"]          || _h["Origin"]          || null),
    callerXFF    : (_h["x-forwarded-for"] || _h["X-Forwarded-For"] || null),
    callerHost   : (_h["host"]            || _h["Host"]            || null),
    httpMethod   : event.httpMethod,
    queryString  : event.queryStringParameters || null,
    bodyRaw      : (typeof event.body === "string" ? event.body.slice(0, 500) : null)
  });
  meter.bumpSimple("sync.invocation");

  // Parse body / query
  let mode = null;
  let buyerUserId = null;
  let receiptId = null;
  let threadId = null;
  let action = null;
  try {
    if (event.body) {
      const body = JSON.parse(event.body);
      if (body.mode) mode = body.mode;
      if (body.buyerUserId) buyerUserId = String(body.buyerUserId);
      if (body.receiptId) receiptId = String(body.receiptId);
      if (body.threadId) threadId = String(body.threadId);
      if (body.action) action = body.action;
    }
    if (event.queryStringParameters) {
      if (event.queryStringParameters.mode) mode = event.queryStringParameters.mode;
      if (event.queryStringParameters.buyerUserId) buyerUserId = String(event.queryStringParameters.buyerUserId);
      if (event.queryStringParameters.receiptId) receiptId = String(event.queryStringParameters.receiptId);
      if (event.queryStringParameters.threadId) threadId = String(event.queryStringParameters.threadId);
      if (event.queryStringParameters.action) action = event.queryStringParameters.action;
    }
  } catch {}

  await writeDiagLog(invocationId, {
    parsedMode       : mode,
    parsedBuyerUserId: buyerUserId,
    parsedReceiptId  : receiptId,
    parsedThreadId   : threadId,
    parsedAction     : action
  });

  // ─── Mode dispatch ────────────────────────────────────────────────
  try {
    if (mode === "buyer") {
      if (!buyerUserId && !receiptId) {
        const out = { ok: false, error: "buyer mode requires buyerUserId or receiptId" };
        await writeDiagLog(invocationId, { phase: "end", outcome: "error", errorMsg: out.error });
        return { statusCode: 400, body: JSON.stringify(out) };
      }

      // ─── Per-buyer debounce ─────────────────────────────────────────
      // When the same buyerUserId triggers buyer-sync multiple times in
      // rapid succession (multi-message bursts, operator-opens-thread
      // races, etc.), short-circuit if we just synced this buyer.
      // Suppresses redundant Etsy calls AND helps mask deterministic
      // request patterns that can trip Etsy's anti-spam heuristics.
      //
      // The debounce window is randomized 3-5 min per successful sync.
      // Each completed sync writes `nextBuyerSyncEligibleAtMs` on the
      // customer doc; ticks inside that window skip without hitting
      // Etsy. The mirror cron is running on its own 7-10 min schedule
      // refreshing all receipts independently, so a 3-5 min per-buyer
      // gap doesn't cause stale customer context — the cron picks up
      // any genuine order changes regardless.
      //
      // Only applies when buyerUserId is known upfront. The receiptId-
      // only path (lazy recovery from a help-request thread that had
      // no buyer captured at scrape time) skips debouncing — those
      // calls are rare and important.
      if (buyerUserId) {
        try {
          const cSnap = await db.collection("EtsyMail_Customers").doc(String(buyerUserId)).get();
          if (cSnap.exists) {
            const cData = cSnap.data() || {};
            const nextEligible = cData.nextBuyerSyncEligibleAtMs;
            const nextEligibleMs = typeof nextEligible === "number" ? nextEligible : 0;
            if (nextEligibleMs && Date.now() < nextEligibleMs) {
              const waitSec = Math.ceil((nextEligibleMs - Date.now()) / 1000);
              console.log(`[buyer-sync] debounced — buyer ${buyerUserId} inside randomized window, ${waitSec}s remaining`);
              await writeDiagLog(invocationId, {
                phase: "end", outcome: "skipped", reason: "buyer_debounce",
                buyerUserId: String(buyerUserId), waitSec
              });
              return { statusCode: 200, body: JSON.stringify({ ok: true, mode: "buyer", skipped: true, reason: "buyer_debounce", waitSec }) };
            }
          }
        } catch (e) {
          console.warn(`[buyer-sync] debounce check failed (proceeding anyway): ${e.message}`);
        }
      }

      const result = await runBuyerSyncFromMirror({ buyerUserId, receiptId, threadId });
      const elapsedMs = Date.now() - invocationStartMs;
      await writeDiagLog(invocationId, {
        phase             : "end",
        outcome           : "ok",
        pagesFetched      : 0,
        receiptsProcessed : result.receiptsProcessed,
        customersUpdated  : result.customersUpdated,
        source            : result.source || "mirror",
        receiptId         : result.receiptId || null,
        targetReceiptHydrate: result.targetReceiptHydrate || null,
        elapsedMs,
        endedAt           : FV.serverTimestamp()
      });
      return { statusCode: 200, body: JSON.stringify({ ok: true, mode: "buyer", ...result }) };
    }

    if (mode === "backfill") {
      if (!SHOP_ID || !CLIENT_ID || !CLIENT_SECRET) {
        const out = { ok: false, error: "Missing env vars" };
        await writeDiagLog(invocationId, { phase: "end", outcome: "error", errorMsg: out.error });
        return { statusCode: 500, body: JSON.stringify(out) };
      }
      const result = await runBackfill({ action: action || "chunk", invocationStartMs });
      const elapsedMs = Date.now() - invocationStartMs;
      await writeDiagLog(invocationId, {
        phase   : "end",
        outcome : result.ok ? "ok" : "error",
        action  : result.action,
        status  : result.status,
        pagesThisChunk    : result.pagesThisChunk || 0,
        pagesProcessed    : result.pagesProcessed || 0,
        receiptsProcessed : result.receiptsProcessed || 0,
        errorMsg: result.errorMsg || null,
        elapsedMs,
        endedAt : FV.serverTimestamp()
      });
      return { statusCode: 200, body: JSON.stringify(result) };
    }

    // Unknown mode
    const out = {
      ok: false,
      error: `Unsupported mode "${mode}". Supported: buyer, backfill.`
    };
    await writeDiagLog(invocationId, { phase: "end", outcome: "error", errorMsg: out.error });
    return { statusCode: 400, body: JSON.stringify(out) };

  } catch (err) {
    const elapsedMs = Date.now() - invocationStartMs;
    const errorMsg = (err.message || String(err)).slice(0, 500);
    await writeDiagLog(invocationId, {
      phase   : "end",
      outcome : "error",
      errorMsg,
      elapsedMs,
      endedAt : FV.serverTimestamp()
    });
    console.error(`[sync-bg] handler failed: ${errorMsg}`);
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: errorMsg }) };
  }
});
