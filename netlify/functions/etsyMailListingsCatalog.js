/*  netlify/functions/etsyMailListingsCatalog.js
 *
 *  v2.0 Step 1 — Etsy listings catalog sync + search
 *
 *  ═══ WHAT IT DOES ════════════════════════════════════════════════════════
 *
 *  Mirrors active Etsy listings into Firestore so the AI can reference real
 *  shop products in replies. Source of truth is the Etsy API (NOT the
 *  Chrome extension scrape). The cron runs every 30 min; on-demand sync
 *  is also available from the UI.
 *
 *  ═══ THREE OPS ═══════════════════════════════════════════════════════════
 *
 *  POST { op: "sync" [, fullSync: true|false] }
 *      Cron path. Pulls active listings, paginates, batched writes to
 *      EtsyMail_Listings. With fullSync:true (the cron default), any
 *      listings NOT seen in this pass are flagged active:false. Mutex on
 *      EtsyMail_ListingsSync/global prevents concurrent syncs.
 *
 *      Gated by EtsyMail_Config/autoPipeline.listingsMirrorEnabled.
 *      When the flag is false, the cron still fires but the function
 *      early-returns with { skipped: true, reason: "disabled" }.
 *
 *  POST { op: "search", query: "...", limit?: 10 }
 *      AI tool path AND UI search path. Substring match against title,
 *      tags, and description prefix. Returns trimmed listings (the same
 *      shape the AI tool sees and the UI catalog browser displays).
 *
 *  GET  ?op=list&offset=0&limit=50
 *      UI catalog browser path. Paginated list of all listings (active +
 *      inactive), most recently synced first.
 *
 *  ═══ EXPORTED HELPER ═══════════════════════════════════════════════════
 *
 *  module.exports.searchListings(query, limit)
 *      Used by etsyMailDraftReply's `search_shop_listings` tool executor
 *      AND (in v2.0 Step 2) by etsyMailSalesAgent's `search_shop_listings`
 *      tool executor. Single source of truth for the search algorithm —
 *      no second implementation, no HTTP round-trip from sibling functions.
 *
 *  ═══ ENV VARS ══════════════════════════════════════════════════════════
 *
 *  SHOP_ID, CLIENT_ID, CLIENT_SECRET   for Etsy API (via _etsyMailEtsy)
 *  ETSYMAIL_EXTENSION_SECRET           gates manual invocations
 */

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const { isScheduledInvocation } = require("./_etsyMailScheduled");
const { etsyFetch, SHOP_ID } = require("./_etsyMailEtsy");
const meter = require("./_etsyApiMeter");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

// ─── Collections ────────────────────────────────────────────────────────
const LISTINGS_COLL = "EtsyMail_Listings";
const SYNC_COLL     = "EtsyMail_ListingsSync";
const CONFIG_COLL   = "EtsyMail_Config";
const AUDIT_COLL    = "EtsyMail_Audit";

// ─── Sync constants ────────────────────────────────────────────────────
const SYNC_PAGE_SIZE      = 100;        // Etsy v3 max
const SYNC_HARD_CAP       = 5000;       // safety cap on pagination
const SYNC_MUTEX_TTL_MS   = 10 * 60 * 1000;  // stale-lock recovery

// ─── AI-trim constants — what the model sees ───────────────────────────
// Description and tags are CAPPED before going to the model, so a
// single match doesn't blow our prompt budget. The catalog browser
// gets the full data; only AI-bound results are trimmed.
const AI_DESCRIPTION_MAX = 1500;
const AI_TAG_LIMIT       = 20;
const AI_RESULT_LIMIT    = 8;           // default # matches returned to AI

// ─── Config flag ───────────────────────────────────────────────────────
// Cached 15s like the auto-pipeline config. listingsMirrorEnabled gates
// both the cron sync AND the search op (when off, the AI gets "disabled"
// errors instead of stale data).
let _cfgCache = { value: null, fetchedAt: 0 };
const CFG_CACHE_MS = 15 * 1000;

async function getConfig() {
  if (_cfgCache.value && (Date.now() - _cfgCache.fetchedAt < CFG_CACHE_MS)) {
    return _cfgCache.value;
  }
  let value = { listingsMirrorEnabled: false };
  try {
    const doc = await db.collection(CONFIG_COLL).doc("autoPipeline").get();
    if (doc.exists) {
      const d = doc.data() || {};
      value = {
        // Default false — Step 1 ships disabled. Operator flips it on
        // explicitly in the inbox UI Settings panel after deploy verifies.
        listingsMirrorEnabled: d.listingsMirrorEnabled === true
      };
    }
    _cfgCache = { value, fetchedAt: Date.now() };
  } catch (e) {
    console.warn("listingsCatalog: config fetch failed:", e.message);
  }
  return value;
}

// ─── Helpers ───────────────────────────────────────────────────────────

function json(statusCode, body) {
  return { statusCode, headers: { ...CORS, "Content-Type": "application/json" }, body: JSON.stringify(body) };
}
function bad(msg, code = 400) { return json(code, { error: msg }); }
function ok(body)             { return json(200, { success: true, ...body }); }

async function writeAudit({ eventType, actor = "system:listingsCatalog", payload = {} }) {
  // Same canonical shape as siblings. `outcome` rides in payload — the
  // top-level schema is not extended.
  try {
    await db.collection(AUDIT_COLL).add({
      threadId : null,
      draftId  : null,
      eventType,
      actor,
      payload  : payload || {},
      createdAt: FV.serverTimestamp()
    });
  } catch (e) {
    console.warn("audit write failed (non-fatal):", e.message);
  }
}

/** Convert an Etsy v3 listing to the trimmed shape stored in Firestore.
 *  Defensive about Etsy field shapes — never trust the API blindly. */
function normalizeListing(l) {
  const listingId = String(l.listing_id || "");
  if (!listingId) return null;

  // Etsy v3 returns price as { amount, divisor, currency_code }.
  // amount/divisor → dollars (or shop currency). divisor defaults 100
  // (cents) but can be 1 for whole-unit currencies.
  let priceUsd = null;
  if (l.price && typeof l.price.amount === "number") {
    const div = l.price.divisor || 100;
    priceUsd = l.price.amount / div;
  }

  // Image URLs — prefer 570xN (medium), fall back to other sizes.
  const images = (l.images || []).map(i => ({
    url    : i.url_570xN || i.url_340x270 || i.url_fullxfull || null,
    altText: i.alt_text || ""
  })).filter(i => i.url);

  return {
    listingId,
    title       : String(l.title || "").trim(),
    description : String(l.description || "").trim(),
    tags        : Array.isArray(l.tags) ? l.tags.slice(0, 50) : [],
    priceUsd,
    priceMin    : priceUsd,                  // pending variation parsing
    priceMax    : priceUsd,
    variations  : [],                         // TODO Step 1.x — variation prices
    leadTimeDays: typeof l.processing_min === "number" ? l.processing_min : null,
    images,
    listingUrl  : l.url || `https://www.etsy.com/listing/${listingId}`,
    state       : l.state || "active",
    quantity    : typeof l.quantity === "number" ? l.quantity : 0,
    active      : l.state === "active",
    lastSyncedAt: FV.serverTimestamp()
  };
}

/** Trim a listing to the shape the AI tool/agent sees. Description and
 *  tags are capped so a single match doesn't blow the prompt budget. */
function trimForAI(listing) {
  const desc = String(listing.description || "");
  return {
    listingId   : listing.listingId,
    title       : listing.title,
    description : desc.length > AI_DESCRIPTION_MAX ? desc.slice(0, AI_DESCRIPTION_MAX) + "…" : desc,
    tags        : (listing.tags || []).slice(0, AI_TAG_LIMIT),
    priceUsd    : listing.priceUsd,
    priceMin    : listing.priceMin,
    priceMax    : listing.priceMax,
    leadTimeDays: listing.leadTimeDays,
    primaryImage: (listing.images && listing.images[0] && listing.images[0].url) || null,
    listingUrl  : listing.listingUrl,
    quantity    : listing.quantity,
    active      : listing.active
  };
}

// ─── Mutex / sync state helpers ────────────────────────────────────────

async function acquireSyncMutex() {
  const ref = db.collection(SYNC_COLL).doc("global");
  return await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const prev = snap.exists ? (snap.data() || {}) : {};
    if (prev.inFlight) {
      const startedMs = prev.inFlightStartedAt && prev.inFlightStartedAt.toMillis
        ? prev.inFlightStartedAt.toMillis() : 0;
      // Stale lock — recover. Otherwise refuse.
      if (Date.now() - startedMs < SYNC_MUTEX_TTL_MS) {
        return { acquired: false, reason: "another_sync_in_flight" };
      }
      console.warn(`listingsCatalog: clearing stale sync mutex (started ${Math.round((Date.now()-startedMs)/1000)}s ago)`);
    }
    tx.set(ref, {
      inFlight          : true,
      inFlightStartedAt : FV.serverTimestamp(),
      lastError         : null   // clear stale errors on each new attempt
    }, { merge: true });
    return { acquired: true };
  });
}

async function releaseSyncMutex({ totalListings, fullSync, lastError }) {
  await db.collection(SYNC_COLL).doc("global").set({
    inFlight          : false,
    inFlightStartedAt : null,
    lastIncrementalAt : FV.serverTimestamp(),
    ...(fullSync && !lastError ? { lastFullSyncAt: FV.serverTimestamp() } : {}),
    ...(typeof totalListings === "number" ? { totalListings } : {}),
    lastError         : lastError || null,
    updatedAt         : FV.serverTimestamp()
  }, { merge: true });
}

// ─── Sync op ───────────────────────────────────────────────────────────

async function syncCatalog({ fullSync = true, triggeredBy = "cron" } = {}) {
  const cfg = await getConfig();
  if (!cfg.listingsMirrorEnabled) {
    return { skipped: true, reason: "listingsMirrorEnabled is false" };
  }

  const mutex = await acquireSyncMutex();
  if (!mutex.acquired) {
    return { skipped: true, reason: mutex.reason };
  }

  let totalWritten = 0;
  const seenIds = new Set();
  let lastError = null;

  try {
    let offset = 0;
    let keepGoing = true;

    while (keepGoing) {
      meter.bumpSimple("catalog.activeListings");
      const data = await etsyFetch(`/shops/${SHOP_ID}/listings/active`, {
        query: { limit: SYNC_PAGE_SIZE, offset, includes: "Images" }
      });
      const results = Array.isArray(data.results) ? data.results : [];
      if (results.length === 0) break;

      const batch = db.batch();
      let batchSize = 0;
      for (const l of results) {
        const doc = normalizeListing(l);
        if (!doc) continue;
        seenIds.add(doc.listingId);
        batch.set(db.collection(LISTINGS_COLL).doc(doc.listingId), doc, { merge: true });
        batchSize++;
        totalWritten++;
      }
      if (batchSize > 0) await batch.commit();

      offset += results.length;
      // Stop when (a) page is partial (last page reached) or (b) we hit
      // the safety cap. Both are explicit exits to avoid runaway loops.
      keepGoing = results.length === SYNC_PAGE_SIZE && offset < SYNC_HARD_CAP;
    }

    // Full-sync inactivation pass: anything previously stored that's NOT
    // in this run's seenIds gets active:false. Skip on incremental syncs
    // (set fullSync:false in the request body).
    let inactivatedCount = 0;
    if (fullSync && seenIds.size > 0) {
      // We only need the active ones — flipping already-inactive listings
      // again is wasted writes. Filter by active==true to keep the read
      // set bounded.
      const allActiveSnap = await db.collection(LISTINGS_COLL)
        .where("active", "==", true)
        .get();
      const inactivateBatch = db.batch();
      let pending = 0;
      const batches = [];
      allActiveSnap.forEach(doc => {
        if (!seenIds.has(doc.id)) {
          inactivateBatch.set(doc.ref, {
            active         : false,
            deactivatedAt  : FV.serverTimestamp()
          }, { merge: true });
          inactivatedCount++;
          pending++;
        }
      });
      if (pending > 0) await inactivateBatch.commit();
    }

    await releaseSyncMutex({ totalListings: seenIds.size, fullSync, lastError: null });

    await writeAudit({
      eventType: "catalog_sync_completed",
      payload  : {
        outcome         : "success",
        triggeredBy,
        fullSync,
        totalWritten,
        totalListings   : seenIds.size,
        inactivatedCount
      }
    });

    return {
      ok: true,
      totalWritten,
      totalListings: seenIds.size,
      inactivatedCount,
      fullSync
    };

  } catch (err) {
    lastError = err.message || String(err);
    await releaseSyncMutex({ totalListings: seenIds.size, fullSync, lastError });
    await writeAudit({
      eventType: "catalog_sync_failed",
      payload  : {
        outcome     : "failure",
        error       : lastError,
        totalWritten,
        triggeredBy,
        fullSync
      }
    });
    throw err;
  }
}

// ─── Search op + EXPORTED helper ──────────────────────────────────────
//
// `searchListings` is exported AS A FUNCTION (not just an HTTP endpoint)
// so etsyMailDraftReply.js can require this module and call directly.
// This is what makes the v1.10 customer-service AI's `search_shop_listings`
// tool work without an HTTP round-trip, AND what Step 2's sales agent
// will use for the same purpose. Single source of truth.

async function searchListings(query, limit = AI_RESULT_LIMIT) {
  const q = String(query || "").trim().toLowerCase();
  if (!q) return { matches: [], count: 0 };

  const cfg = await getConfig();
  if (!cfg.listingsMirrorEnabled) {
    return { error: "Listings mirror is disabled", matches: [], count: 0 };
  }

  // Substring match strategy:
  //   1. Pull all active listings (cap at 500 — typical handmade shop is
  //      well under this). Composite indexes aren't needed because we're
  //      filtering on a single field (active==true).
  //   2. Score each listing on three fields with descending weight:
  //         - title contains query     +3
  //         - tag exact-equals query   +2
  //         - description contains q   +1
  //   3. Multi-word queries: each whitespace-separated token contributes.
  //   4. Return top `limit` by score, descending. Ties broken by recency
  //      (lastSyncedAt). Score 0 → not returned.
  //
  // If we outgrow the in-memory scan, the migration target is Algolia or
  // a Firestore full-text index. For a typical handmade shop's catalog
  // size this is plenty fast.
  const snap = await db.collection(LISTINGS_COLL)
    .where("active", "==", true)
    .limit(500)
    .get();

  const tokens = q.split(/\s+/).filter(t => t.length >= 2);
  if (tokens.length === 0) return { matches: [], count: 0 };

  const scored = [];
  snap.forEach(doc => {
    const d = doc.data() || {};
    const title = String(d.title || "").toLowerCase();
    const desc  = String(d.description || "").toLowerCase().slice(0, 500);
    const tags  = (d.tags || []).map(t => String(t).toLowerCase());

    let score = 0;
    for (const t of tokens) {
      if (title.includes(t)) score += 3;
      if (tags.includes(t))  score += 2;
      if (desc.includes(t))  score += 1;
    }
    if (score > 0) {
      scored.push({ score, doc: { listingId: doc.id, ...d } });
    }
  });

  scored.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    // Tiebreaker: more recently synced first
    const aMs = a.doc.lastSyncedAt && a.doc.lastSyncedAt.toMillis ? a.doc.lastSyncedAt.toMillis() : 0;
    const bMs = b.doc.lastSyncedAt && b.doc.lastSyncedAt.toMillis ? b.doc.lastSyncedAt.toMillis() : 0;
    return bMs - aMs;
  });

  const cap = Math.max(1, Math.min(parseInt(limit, 10) || AI_RESULT_LIMIT, 25));
  const matches = scored.slice(0, cap).map(s => trimForAI(s.doc));

  return { matches, count: matches.length, totalScored: scored.length };
}

// ─── List op (UI) ──────────────────────────────────────────────────────

async function listListings({ offset = 0, limit = 50, activeOnly = false }) {
  let q = db.collection(LISTINGS_COLL);
  if (activeOnly) q = q.where("active", "==", true);
  q = q.orderBy("lastSyncedAt", "desc")
       .limit(Math.min(parseInt(limit, 10) || 50, 200))
       .offset(Math.max(0, parseInt(offset, 10) || 0));
  const snap = await q.get();
  const docs = [];
  snap.forEach(d => docs.push({ listingId: d.id, ...d.data() }));
  return { docs, count: docs.length };
}

async function getSyncStatus() {
  const snap = await db.collection(SYNC_COLL).doc("global").get();
  if (!snap.exists) return { exists: false };
  const d = snap.data() || {};
  return {
    exists           : true,
    inFlight         : !!d.inFlight,
    totalListings    : d.totalListings || 0,
    lastIncrementalAt: d.lastIncrementalAt && d.lastIncrementalAt.toMillis ? d.lastIncrementalAt.toMillis() : null,
    lastFullSyncAt   : d.lastFullSyncAt && d.lastFullSyncAt.toMillis ? d.lastFullSyncAt.toMillis() : null,
    lastError        : d.lastError || null
  };
}

// ─── Handler ───────────────────────────────────────────────────────────

exports.handler = meter.wrapHandler(async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  // Cron invocations bypass the extension secret; manual invocations do not.
  const scheduled = isScheduledInvocation(event);

  if (!scheduled) {
    const auth = requireExtensionAuth(event);
    if (!auth.ok) return auth.response;
  }

  const qs = event.queryStringParameters || {};
  let body = {};
  if (event.body) {
    try { body = JSON.parse(event.body); }
    catch { /* scheduled invocations have non-JSON bodies; that's fine */ }
  }

  // Determine op:
  //   - Scheduled cron invocations always run "sync" (with full=true).
  //   - Manual invocations look at body.op or query.op.
  let op = body.op || qs.op;
  if (!op && scheduled) op = "sync";

  if (!op) return bad("op required (sync | search | list | status)");

  try {
    if (op === "sync") {
      // fullSync defaults true on cron, can be overridden by request body.
      // Manual incremental sync: { op: "sync", fullSync: false }.
      const fullSync = body.fullSync !== false;   // default true
      const result = await syncCatalog({
        fullSync,
        triggeredBy: scheduled ? "cron" : "manual"
      });
      return ok(result);
    }

    if (op === "search") {
      const query = body.query || qs.query;
      const limit = body.limit || qs.limit;
      if (!query) return bad("Missing query");
      const result = await searchListings(query, limit);
      return ok(result);
    }

    if (op === "list") {
      const offset     = body.offset     || qs.offset;
      const limit      = body.limit      || qs.limit;
      const activeOnly = body.activeOnly === true || qs.activeOnly === "true";
      const result = await listListings({ offset, limit, activeOnly });
      return ok(result);
    }

    if (op === "status") {
      const status = await getSyncStatus();
      return ok(status);
    }

    return bad(`Unknown op '${op}'`);

  } catch (err) {
    console.error("listingsCatalog error:", err);
    return json(500, { error: err.message || String(err), op });
  }
});

// Expose the search helper for sibling functions (etsyMailDraftReply,
// and in v2.0 Step 2: etsyMailSalesAgent). Both call this directly,
// avoiding HTTP overhead and keeping search logic single-sourced.
module.exports.searchListings = searchListings;
module.exports.trimForAI      = trimForAI;
