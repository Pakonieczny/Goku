/*  netlify/functions/etsyMailThreads.js
 *
 *  Thread CRUD for the EtsyMail automation system.
 *  Mirrors the style of firebaseOrders.js: Admin SDK, CORS preamble, single
 *  handler that dispatches on method + query params.
 *
 *  Collections (see FIRESTORE_SCHEMA.md):
 *    EtsyMail_Threads/{threadId}
 *    EtsyMail_Threads/{threadId}/messages/{messageId}
 *    EtsyMail_Audit/{eventId}
 *    EtsyMail_Jobs/{jobId}
 *
 *  Supported operations (Milestone 1):
 *    GET  ?list=1&status=...&limit=...      → list threads
 *    GET  ?threadId=...                     → fetch single thread + messages
 *    GET  ?counts=1                         → left-rail counts by status
 *    POST body:{ action:'create', ... }     → create new thread (manual or Gmail)
 *    POST body:{ action:'patch',  threadId, fields } → partial update
 *    POST body:{ action:'appendMessage', threadId, message } → append to messages subcollection
 *    POST body:{ action:'markRead', threadId } → sets lastReadAt, unread=false
 *    POST body:{ action:'markUnread', threadId } → sets unread=true (preserves lastReadAt)
 *    POST body:{ action:'setStatus', threadId, status, reason? } → state transition + audit
 *    POST body:{ action:'enqueueJob', threadId, jobType, payload } → write EtsyMail_Jobs doc
 *
 *  v3.0 additions — folder management & destructive ops:
 *    POST body:{ action:'purgeFolder', folderStatus, actor }
 *      Owner-only. Move every thread whose status matches `folderStatus`
 *      (string or array — to support multi-status folders like Auto-Reply)
 *      OUT of that folder by setting status to "pending_human_review".
 *      Threads remain in the system; only their folder membership clears.
 *      Use case: empty out a stale folder without deleting any data.
 *
 *    POST body:{ action:'masterPurgeWipe', password, actor }
 *      Owner-only AND password-gated. Permanently deletes every doc in:
 *        - EtsyMail_Threads (and their messages subcollections)
 *        - EtsyMail_Drafts
 *        - EtsyMail_Jobs
 *      Preserves: EtsyMail_Config, OAuth, listing templates, audit log,
 *      sales contexts. Writes a single audit row before deletion with
 *      actor + counts so there's a forensic record after the data is gone.
 *
 *    POST body:{ action:'masterPurgePasswordSet', password, securityQuestion, securityAnswer, actor }
 *      First-time setup OR password change. Owner-only. Stores PBKDF2
 *      hashes (never plaintext) at EtsyMail_Config/masterPurgeAuth.
 *
 *    POST body:{ action:'masterPurgePasswordReset', securityAnswer, newPassword, actor }
 *      Recover from a forgotten password by answering the security
 *      question. Owner-only. Replaces the stored password hash.
 *
 *    POST body:{ action:'masterPurgePasswordStatus', actor }
 *      Owner-only. Returns { isSet: bool, securityQuestion: string|null }
 *      so the UI can decide whether to prompt for first-time setup or
 *      enter-password.
 */

const admin = require("./firebaseAdmin");
const crypto = require("crypto");
const { requireExtensionAuth } = require("./_etsyMailAuth");
const { requireOwner, logUnauthorized } = require("./_etsyMailRoles");
const db    = admin.firestore();
const FV    = admin.firestore.FieldValue;

const THREADS_COLL  = "EtsyMail_Threads";
const AUDIT_COLL    = "EtsyMail_Audit";
const JOBS_COLL     = "EtsyMail_Jobs";
// v3.0 — collections involved in the master-purge wipe.
const DRAFTS_COLL   = "EtsyMail_Drafts";
const CONFIG_COLL   = "EtsyMail_Config";
const MASTER_PURGE_AUTH_DOC = "EtsyMail_Config/masterPurgeAuth";

// v1.2: include X-EtsyMail-Secret in allowed headers (CORS preflight)
// because the handler now enforces requireExtensionAuth. The inbox UI
// forwards the secret on every api() call; the snapshot/extension
// forwards it from env. Calls without the header now 401.
const CORS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Headers": "Content-Type,Authorization,X-EtsyMail-Secret",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
};

const VALID_STATUSES = new Set([
  "detected_from_gmail",
  "pending_etsy_scrape",
  "etsy_scraped",
  "pending_order_enrichment",
  "ready_for_ai",
  "draft_ready",
  "pending_human_review",
  "approved_for_send",
  "auto_send_eligible",
  "queued_for_auto_send",    // v1.2 — AI passed all gates, draft enqueued, awaiting Etsy send confirmation
  "auto_replied",            // v1.0 — AI auto-sent AND Etsy confirmed delivery
  "send_in_progress",
  "sent",
  "hold_uncertain",          // legacy — retained for backward compat
  "hold_missing_order",      // legacy
  "hold_login_required",     // legacy
  "failed_scrape",
  "failed_send",
  // v4.3 — sales-agent lifecycle. The agent already writes "sales_active"
  // (line 1538) and treats "sales_completed" / "sales_abandoned" as
  // terminal (TERMINAL_THREAD_STATUSES). Those statuses were missing from
  // VALID_STATUSES, which prevented the dashboard from filtering on them
  // (the ?list=1&status=... endpoint validates against this set). The
  // dashboard's "Completed Sales" menu queries status=sales_completed.
  "sales_active",
  "sales_completed",
  "sales_abandoned",
  // v3.0 — sales-funnel intermediate statuses. Without these, the
  // "Sales — Active" folder count works (etsyMailThreads ?counts=1
  // groups by raw status string) but manual setStatus into any of
  // these would 400 with "Invalid status". The sales agent already
  // writes them; the UI just couldn't move INTO them.
  "sales_discovery",
  "sales_spec",
  "sales_quote",
  "sales_revision",
  "sales_pending_close_approval",
  // v3.24 — Production Rush. Same story: the AI writes this status
  // in etsyMailDraftReply when the customer accepts a $15 rush
  // upgrade, but until v3.0 manual moves into Production Rush were
  // rejected because the status wasn't in this set.
  "production_rush",
  "archived"
]);

// ═══════════════════════════════════════════════════════════════════════
// v3.0 — Folder-purge & master-purge plumbing
// ═══════════════════════════════════════════════════════════════════════

// When `purgeFolder` un-tags threads from a folder, they need a non-empty
// status to land on (Firestore docs need SOME value, and the inbox
// front-end uses status as the primary routing field). We use the
// review queue as the safe default destination. Operators can re-route
// from there manually.
const PURGE_FOLDER_DEFAULT_DESTINATION = "pending_human_review";

// Statuses operators are NOT allowed to bulk-purge. Trying to purge
// these returns 400. They're either transient (the auto-pipeline owns
// the transition timing) or irreplaceable (terminal sales states).
const PURGE_FOLDER_BLOCKED = new Set([
  "queued_for_auto_send",   // transient; owned by etsyMailDraftSend
  "send_in_progress",       // transient; same
  "sales_completed"         // terminal; preserve for reporting
]);

// PBKDF2 parameters for the master-purge password hash.
//   Iterations: 200_000 — comfortable middle ground for Node's pbkdf2,
//                ~50ms on modern hardware. High enough to slow brute
//                force, low enough not to time out a Netlify function.
//   Key length: 64 bytes — overkill for what we need but cheap.
//   Digest    : sha512.
//   Salt      : 32 random bytes per password (regenerated on every set).
const PBKDF2_ITERATIONS = 200_000;
const PBKDF2_KEY_BYTES  = 64;
const PBKDF2_DIGEST     = "sha512";
const SALT_BYTES        = 32;

/**
 * Hash a secret with a fresh random salt. Returns
 *   { hash: <hex>, salt: <hex>, iterations, digest }
 * for storage. The salt is per-secret and stored alongside the hash;
 * this matches standard PBKDF2 practice.
 */
function hashSecret(plaintext) {
  const salt = crypto.randomBytes(SALT_BYTES);
  const hash = crypto.pbkdf2Sync(
    String(plaintext),
    salt,
    PBKDF2_ITERATIONS,
    PBKDF2_KEY_BYTES,
    PBKDF2_DIGEST
  );
  return {
    hash       : hash.toString("hex"),
    salt       : salt.toString("hex"),
    iterations : PBKDF2_ITERATIONS,
    digest     : PBKDF2_DIGEST
  };
}

/**
 * Constant-time compare a plaintext attempt against a stored
 * { hash, salt, iterations, digest } record. Returns boolean.
 *
 * Uses crypto.timingSafeEqual to prevent timing-attack discrimination
 * even though the practical attack surface here (a master-purge
 * password) is small.
 */
function verifySecret(plaintext, stored) {
  if (!stored || !stored.hash || !stored.salt) return false;
  const iterations = stored.iterations || PBKDF2_ITERATIONS;
  const digest     = stored.digest     || PBKDF2_DIGEST;
  let attempt;
  try {
    attempt = crypto.pbkdf2Sync(
      String(plaintext),
      Buffer.from(stored.salt, "hex"),
      iterations,
      PBKDF2_KEY_BYTES,
      digest
    );
  } catch {
    return false;
  }
  let stored_buf;
  try {
    stored_buf = Buffer.from(stored.hash, "hex");
  } catch {
    return false;
  }
  if (attempt.length !== stored_buf.length) return false;
  return crypto.timingSafeEqual(attempt, stored_buf);
}

/**
 * Normalize a security-question answer before hashing/comparing.
 * Lower-cased, trimmed, internal whitespace collapsed. This makes
 * "Spot" and " spot " match — operators shouldn't lock themselves out
 * over capitalization or trailing spaces.
 */
function normalizeAnswer(s) {
  return String(s || "").trim().toLowerCase().replace(/\s+/g, " ");
}

// ─── v2.4: Search support (folded in from former etsyMailSearch.js) ─────
//
// Each thread doc carries a denormalized `searchableText` field populated
// by etsyMailSnapshot.js: lowercased, normalized concatenation of customer
// name / Etsy username / subject / linked order id, plus the most recent
// ~6KB of message body. We load the most recent N threads ordered by
// updatedAt desc, run a substring match in memory, return same shape as
// firestoreProxy `op:list` so the UI can drop them into existing render
// path with no changes.
//
// At 500 threads × 6KB = ~3MB transferred per search. In-memory cache
// (15s TTL, keyed by query+limit+status) absorbs rapid keystrokes from
// the inbox UI's debounced search input. When the inbox grows beyond
// ~5K threads, swap for Algolia / Typesense / Meilisearch.

// Cap how many threads we'll lazy-backfill per request. Each backfill
// is one subcollection read + one write — too many in parallel trips
// Firestore quota / function timeout.
const MAX_BACKFILLS_PER_REQUEST = 50;

// Same normalizer the snapshot uses, kept in sync. Lowercase + collapse
// runs of whitespace + trim.
function normalizeSearchText(text = "") {
  return String(text).toLowerCase().replace(/\s+/g, " ").trim();
}

const _searchCache = new Map();
const SEARCH_CACHE_TTL_MS = 15 * 1000;
const MAX_CACHE_ENTRIES = 100;

/* Convert Firestore doc data to JSON-safe form, turning Timestamps into
 * {_ts: true, ms: <millis>} markers — same shape firestoreProxy uses, so
 * the inbox doesn't need a separate code path for search results. */
function serializeForSearch(value) {
  if (value === null || typeof value !== "object") return value;
  if (value && typeof value.toDate === "function" && typeof value.toMillis === "function") {
    return { _ts: true, ms: value.toMillis() };
  }
  if (Array.isArray(value)) return value.map(serializeForSearch);
  const out = {};
  for (const k of Object.keys(value)) out[k] = serializeForSearch(value[k]);
  return out;
}

/** Trim the heaviest internal-only field from search results. v1.6: keep
 *  `searchableText` so the UI can run further per-keystroke local
 *  filtering on the result set without an extra round trip; only drop
 *  the larger raw `searchableMessageText`. */
function trimSearchResultDoc(data) {
  const { searchableMessageText, ...rest } = data;
  return rest;
}

function gcSearchCache() {
  if (_searchCache.size <= MAX_CACHE_ENTRIES) return;
  const cutoff = Date.now() - SEARCH_CACHE_TTL_MS;
  for (const [k, v] of _searchCache.entries()) {
    if (v.at < cutoff) _searchCache.delete(k);
  }
  while (_searchCache.size > MAX_CACHE_ENTRIES) {
    const oldest = _searchCache.keys().next().value;
    _searchCache.delete(oldest);
  }
}

/** Run the full-text search. Extracted so the GET handler can dispatch
 *  to it on `?search=1`. Returns the same response shape as the former
 *  etsyMailSearch endpoint. */
async function runThreadSearch({ q, limit, statusList }) {
  const cacheKey = q + "|" + limit + "|" + statusList.sort().join(",");
  const cached = _searchCache.get(cacheKey);
  if (cached && (Date.now() - cached.at) < SEARCH_CACHE_TTL_MS) {
    return {
      docs   : cached.docs,
      q,
      count  : cached.docs.length,
      scanned: cached.scanned,
      cached : true
    };
  }

  // Query strategy mirrors fetchThreadListNow's composite-index avoidance:
  //   - status-filtered: single-field where, no orderBy (auto-index)
  //   - unfiltered:      orderBy by updatedAt (single-field auto-index)
  // Sort happens client-side anyway.
  let firestoreQuery = db.collection(THREADS_COLL);
  if (statusList.length === 1) {
    firestoreQuery = firestoreQuery.where("status", "==", statusList[0]).limit(limit);
  } else if (statusList.length > 1) {
    // Firestore `in` supports up to 10 values.
    firestoreQuery = firestoreQuery.where("status", "in", statusList).limit(limit);
  } else {
    firestoreQuery = firestoreQuery.orderBy("updatedAt", "desc").limit(limit);
  }

  const snap = await firestoreQuery.get();

  const matches = [];
  let backfilled = 0;
  const backfillPromises = [];

  snap.forEach(doc => {
    const data = doc.data() || {};
    const haystack = (data.searchableText || "").toLowerCase();

    if (haystack) {
      // Fast path: searchableText already populated.
      if (haystack.includes(q)) {
        matches.push({ id: doc.id, ...serializeForSearch(trimSearchResultDoc(data)) });
      }
      return;
    }

    // Lazy backfill for threads scraped before searchableText was
    // populated (or threads with no new messages since). Also surface
    // metadata-only matches immediately so the user sees something
    // even before the body text is read.
    const metaOnly = [
      data.customerName, data.etsyUsername, data.subject, data.linkedOrderId
    ].some(v => v && String(v).toLowerCase().includes(q));
    if (metaOnly) {
      matches.push({ id: doc.id, ...serializeForSearch(trimSearchResultDoc(data)) });
    }

    if (backfilled >= MAX_BACKFILLS_PER_REQUEST) return;
    backfilled++;

    backfillPromises.push((async () => {
      try {
        const msgsSnap = await doc.ref.collection("messages")
          .orderBy("timestamp", "desc")
          .limit(50)
          .get();
        const allText = msgsSnap.docs
          .map(d => normalizeSearchText((d.data() || {}).text || ""))
          .filter(Boolean)
          .reverse()
          .join(" ");
        const SEARCHABLE_MAX = 6000;
        const truncated = allText.length > SEARCHABLE_MAX
          ? allText.slice(allText.length - SEARCHABLE_MAX)
          : allText;
        const meta = [
          data.customerName, data.etsyUsername, data.subject, data.linkedOrderId
        ].map(s => normalizeSearchText(String(s || ""))).filter(Boolean).join(" ");
        const searchableText = (meta + " " + truncated).trim();

        await doc.ref.set({
          searchableText,
          searchableMessageText: truncated
        }, { merge: true });

        if (searchableText.includes(q) && !metaOnly) {
          const enriched = { ...data, searchableText, searchableMessageText: truncated };
          matches.push({ id: doc.id, ...serializeForSearch(trimSearchResultDoc(enriched)) });
        }
      } catch (e) {
        console.warn("etsyMailThreads search: backfill failed for", doc.id, "—", e.message);
      }
    })());
  });

  if (backfillPromises.length > 0) {
    await Promise.all(backfillPromises);
  }

  _searchCache.set(cacheKey, { docs: matches, at: Date.now(), scanned: snap.size });
  gcSearchCache();

  return {
    docs    : matches,
    q,
    count   : matches.length,
    scanned : snap.size,
    backfilled,
    cached  : false,
    maxedOut: snap.size >= limit
  };
}

function json(statusCode, body) {
  return { statusCode, headers: CORS, body: JSON.stringify(body) };
}
function bad(msg, code = 400) { return json(code, { error: msg }); }
function ok(body)             { return json(200, { success: true, ...body }); }

async function writeAudit({ threadId = null, draftId = null, eventType, actor = "system:api", payload = {} }) {
  await db.collection(AUDIT_COLL).add({
    threadId, draftId, eventType, actor, payload,
    createdAt: FV.serverTimestamp()
  });
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  // v1.2: gate every op behind the shared secret. Same approach as
  // etsyMailDraftSend. Inbox forwards the secret from localStorage on
  // every api() call. If env is unset, requireExtensionAuth allows
  // through (dev mode) and logs a loud warning.
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  try {
    const method = event.httpMethod;
    const qs     = event.queryStringParameters || {};

    /* ──────────────────────────── GET ──────────────────────────── */
    if (method === "GET") {

      /* ?search=1&q=...&limit=...&status=...
       * Full-text search across threads. v2.4: folded in from former
       * etsyMailSearch.js. The standalone /etsyMailSearch endpoint is
       * preserved as a thin shim that delegates back to this same
       * handler — see etsyMailSearch.js. */
      if (qs.search === "1") {
        const q = String(qs.q || "").trim().toLowerCase();
        const limit = Math.min(Math.max(parseInt(qs.limit || "500", 10), 1), 2000);
        const statusRaw = qs.status ? String(qs.status).trim() : "";
        const statusList = statusRaw
          ? statusRaw.split(",").map(s => s.trim()).filter(Boolean).slice(0, 10)  // Firestore `in` cap
          : [];

        // Guard: queries shorter than 2 chars would scan everything for
        // "a". The inbox UI also debounces and only fires for q.length>=2,
        // but defense in depth.
        if (q.length < 2) {
          return ok({ docs: [], q, count: 0, scanned: 0 });
        }

        try {
          const result = await runThreadSearch({ q, limit, statusList });
          return ok(result);
        } catch (err) {
          console.error("threads search error:", err);
          return json(500, { error: err.message || String(err) });
        }
      }

      /* ?counts=1 → counts by status (for left-rail badges).
       * v4.3 — also returns a synthetic _completedSales count: the
       * number of threads where salesCompletedAt is set. Used by the
       * dashboard's "Completed Sales" folder, whose membership is
       * keyed off salesCompletedAt rather than status (status gets
       * overwritten by post-sale chatter in finalizeThread).
       * v5.21 — also returns a synthetic _refundFlagged count: the
       * number of threads where refundFlaggedAt is set. Used by the
       * dashboard's "Refunds" folder, whose membership is keyed off
       * refundFlaggedAt for the same reason — refund discussions span
       * multiple turns and the thread.status changes as the AI/operator
       * replies, so an orderByField filter is the durable membership
       * criterion.
       *
       * v5.31 — Switched from a full-collection scan to Firestore
       * aggregation queries (count()). The previous implementation read
       * EVERY thread doc on every call (the dashboard polls this every
       * 15s by default), which drove a multi-million-read-per-day cost
       * on the inbox. With aggregation, each per-status count() is
       * billed as 1 read per ~1000 matched docs — so the total cost
       * for a single ?counts=1 call is now O(statuses) ≈ ~30 reads
       * regardless of how many threads exist.
       *
       * Behavior preservation:
       *   • Response shape is identical: { counts: { <status>: N, ...,
       *     _completedSales: X, _refundFlagged: Y } }.
       *   • Every key from VALID_STATUSES is present (defaults to 0
       *     when the status has no docs) — same as the old code, where
       *     missing keys also effectively meant 0.
       *   • _completedSales uses orderBy("salesCompletedAt").count(),
       *     which implicitly excludes docs where the field is unset —
       *     exactly matching the old `if (data.salesCompletedAt)` check.
       *     Same for _refundFlagged.
       *   • If aggregation fails for any reason (network, missing
       *     index, SDK incompatibility), we fall back to the legacy
       *     full-scan path so the dashboard never breaks.
       */
      if (qs.counts === "1") {
        try {
          const baseQ = db.collection(THREADS_COLL);
          const statusList = Array.from(VALID_STATUSES);

          // Fire all aggregation queries in parallel.
          //   - one count() per known status
          //   - one count() filtered to docs where salesCompletedAt is set
          //     (orderBy on a field excludes docs without that field —
          //     this is the same trick used by the orderByField folder
          //     path at ~line 510 below)
          //   - one count() filtered to docs where refundFlaggedAt is set
          //
          // v0.9.35 — Both folder-count aggregations now also exclude
          // archived threads. The orderByField folders (Completed Sales,
          // Refunds) display threads where their respective timestamp
          // field is set, MINUS threads the operator has archived. The
          // frontend rail filters archived threads out of the visible
          // list (see etsy-mail-1.html threadMatchesFilter); without
          // this matching backend exclusion the rail badge would still
          // include archived threads and disagree with the empty list.
          //
          // Firestore aggregation supports chained .where() clauses;
          // status,!= is well-supported and doesn't require a composite
          // index when combined with orderBy on the same/single field.
          const aggPromises = [
            ...statusList.map(s => baseQ.where("status", "==", s).count().get()),
            baseQ.where("status", "!=", "archived").orderBy("salesCompletedAt").count().get(),
            baseQ.where("status", "!=", "archived").orderBy("refundFlaggedAt").count().get(),
          ];

          const aggResults = await Promise.all(aggPromises);

          const counts = {};
          statusList.forEach((s, i) => {
            counts[s] = aggResults[i].data().count;
          });
          counts._completedSales = aggResults[statusList.length].data().count;
          counts._refundFlagged  = aggResults[statusList.length + 1].data().count;
          return ok({ counts });
        } catch (aggErr) {
          // Fallback to the legacy full-scan path on any aggregation
          // failure. This preserves availability if the project's
          // Firestore SDK version is older than aggregation support, or
          // if a transient index-build failure happens. Logged so the
          // operator can see why aggregation didn't take.
          //
          // v0.9.35 — Fallback path also excludes archived threads from
          // the two synthetic counts so it stays consistent with the
          // primary aggregation path above.
          console.warn("[etsyMailThreads:counts] aggregation failed, falling back to scan:", aggErr.message);
          const snap   = await db.collection(THREADS_COLL).select("status", "salesCompletedAt", "refundFlaggedAt").get();
          const counts = {};
          let completedSalesCount = 0;
          let refundFlaggedCount  = 0;
          snap.forEach(d => {
            const data = d.data() || {};
            const s = data.status || "unknown";
            counts[s] = (counts[s] || 0) + 1;
            if (s === "archived") return;  // exclude archived from orderByField folder counts
            if (data.salesCompletedAt) completedSalesCount++;
            if (data.refundFlaggedAt)  refundFlaggedCount++;
          });
          counts._completedSales = completedSalesCount;
          counts._refundFlagged  = refundFlaggedCount;
          return ok({ counts });
        }
      }

      /* ?threadId=... → single thread + messages */
      if (qs.threadId) {
        const threadId = String(qs.threadId);
        const tRef = db.collection(THREADS_COLL).doc(threadId);
        const tSnap = await tRef.get();
        if (!tSnap.exists) return json(404, { success: false, notFound: true });

        const mSnap = await tRef.collection("messages").orderBy("timestamp", "asc").limit(500).get();
        const messages = mSnap.docs.map(d => ({ id: d.id, ...d.data() }));

        return ok({ thread: { id: tSnap.id, ...tSnap.data() }, messages });
      }

      /* ?list=1 → list threads, optionally filtered by status. */
      if (qs.list === "1") {
        const statusFilter = qs.status;
        const limit        = Math.min(parseInt(qs.limit || "100", 10), 500);

        let q = db.collection(THREADS_COLL);
        if (statusFilter && VALID_STATUSES.has(statusFilter)) {
          q = q.where("status", "==", statusFilter);
        }
        // order by lastInboundAt desc, falling back to updatedAt
        q = q.orderBy("updatedAt", "desc").limit(limit);

        const snap = await q.get();
        const threads = snap.docs.map(d => ({ id: d.id, ...d.data() }));
        return ok({ threads });
      }

      return bad("GET requires ?list=1, ?threadId=..., or ?counts=1");
    }

    /* ──────────────────────────── POST ──────────────────────────── */
    if (method === "POST") {
      let body = {};
      try { body = JSON.parse(event.body || "{}"); }
      catch { return bad("Invalid JSON body"); }

      const action = body.action;
      if (!action) return bad("Missing action");

      /* ---------- create ---------- */
      if (action === "create") {
        const {
          threadId: suppliedId,
          etsyConversationId = null,
          etsyConversationUrl = null,
          gmailMessageId      = null,
          gmailThreadId       = null,
          customerName        = "Unknown",
          customerEmail       = null,
          etsyUsername        = null,
          linkedOrderId       = null,
          subject             = null,
          initialText         = null,
          source              = "manual",          // 'manual' | 'gmail' | 'extension'
          status              = "detected_from_gmail"
        } = body;

        if (!VALID_STATUSES.has(status)) return bad(`Invalid status '${status}'`);

        const threadId = suppliedId
          || (etsyConversationId ? `etsy_conv_${etsyConversationId}` : `tmp_${Date.now()}_${Math.random().toString(36).slice(2,8)}`);

        const threadRef = db.collection(THREADS_COLL).doc(threadId);
        const now = FV.serverTimestamp();

        const threadDoc = {
          threadId,
          etsyConversationId,
          etsyConversationUrl,
          gmailMessageId,
          gmailThreadId,
          gmailReceivedAt      : null,
          customerName,
          customerEmail,
          etsyUsername,
          linkedOrderId,
          linkedListingIds     : [],
          status,
          category             : null,
          confidence           : null,
          needsHumanReview     : true,
          aiDraftStatus        : "none",
          latestDraftId        : null,
          lastInboundAt        : initialText ? now : null,
          lastOutboundAt       : null,
          lastSyncedAt         : null,
          lastScrapedDomHash   : null,
          assignedTo           : null,
          tags                 : [],
          riskFlags            : [],
          messageCount         : initialText ? 1 : 0,
          unread               : !!initialText,
          lastReadAt           : null,
          subject,
          createdAt            : now,
          updatedAt            : now
        };

        const batch = db.batch();
        batch.set(threadRef, threadDoc, { merge: false });

        if (initialText) {
          const msgRef = threadRef.collection("messages").doc();
          batch.set(msgRef, {
            source          : source === "gmail" ? "gmail" : "etsy",
            direction       : "inbound",
            senderName      : customerName || "Customer",
            senderRole      : "customer",
            timestamp       : now,
            text            : String(initialText),
            normalizedText  : String(initialText).toLowerCase().replace(/\s+/g, " ").trim(),
            contentHash     : null,  // filled by first scrape with real timestamp
            imageUrls       : [],
            storageImagePaths: [],
            attachmentUrls  : [],
            createdAt       : now
          });
        }

        await batch.commit();
        await writeAudit({
          threadId,
          eventType: "thread_created",
          actor    : `system:${source}`,
          payload  : { source, hasInitialText: !!initialText }
        });

        return ok({ threadId });
      }

      /* ---------- patch ---------- */
      if (action === "patch") {
        const { threadId, fields } = body;
        if (!threadId) return bad("Missing threadId");
        if (!fields || typeof fields !== "object") return bad("Missing fields object");

        const allowed = [
          "customerName", "customerEmail", "etsyUsername", "linkedOrderId",
          "linkedListingIds", "category", "confidence", "needsHumanReview",
          "aiDraftStatus", "latestDraftId", "assignedTo", "tags", "riskFlags",
          "subject", "etsyConversationId", "etsyConversationUrl",
          "gmailMessageId", "gmailThreadId"
        ];
        const update = { updatedAt: FV.serverTimestamp() };
        for (const k of Object.keys(fields)) {
          if (allowed.includes(k)) update[k] = fields[k];
        }
        if (Object.keys(update).length === 1) return bad("No allowed fields in patch");

        await db.collection(THREADS_COLL).doc(threadId).set(update, { merge: true });
        return ok({ threadId, patched: Object.keys(update).filter(k => k !== "updatedAt") });
      }

      /* ---------- appendMessage ---------- */
      if (action === "appendMessage") {
        const { threadId, message } = body;
        if (!threadId) return bad("Missing threadId");
        if (!message || typeof message !== "object") return bad("Missing message object");

        const {
          source        = "staff",
          direction     = "outbound",
          senderName    = "Staff",
          senderRole    = "staff",
          text          = "",
          imageUrls     = [],
          attachmentUrls = []
        } = message;

        const normalizedText = String(text).toLowerCase().replace(/\s+/g, " ").trim();
        const tRef = db.collection(THREADS_COLL).doc(threadId);
        const now  = FV.serverTimestamp();

        const batch = db.batch();
        const msgRef = tRef.collection("messages").doc();
        batch.set(msgRef, {
          source, direction, senderName, senderRole,
          timestamp: now,
          text, normalizedText,
          contentHash: null,
          imageUrls, storageImagePaths: [], attachmentUrls,
          createdAt: now
        });

        const threadPatch = {
          messageCount: FV.increment(1),
          updatedAt   : now
        };
        if (direction === "inbound") {
          threadPatch.lastInboundAt = now;
          threadPatch.unread = true;
        } else {
          threadPatch.lastOutboundAt = now;
        }
        batch.set(tRef, threadPatch, { merge: true });

        await batch.commit();
        await writeAudit({
          threadId,
          eventType: "message_appended",
          actor    : `system:${source}`,
          payload  : { direction, senderName }
        });

        return ok({ threadId, messageId: msgRef.id });
      }

      /* ---------- markRead ---------- */
      if (action === "markRead") {
        const { threadId } = body;
        if (!threadId) return bad("Missing threadId");
        await db.collection(THREADS_COLL).doc(threadId).set({
          unread    : false,
          lastReadAt: FV.serverTimestamp(),
          updatedAt : FV.serverTimestamp()
        }, { merge: true });
        return ok({ threadId });
      }

      /* ---------- markUnread ----------
       * Sets unread=true so the thread regains its bold/blue-dot
       * indicator in the rail. Inverse of markRead. Used by an
       * operator-facing "Mark unread" button on the thread header.
       * Does NOT touch lastReadAt — that timestamp is a historical
       * record of when the operator last opened the thread and
       * shouldn't be erased by toggling the unread flag. */
      if (action === "markUnread") {
        const { threadId } = body;
        if (!threadId) return bad("Missing threadId");
        await db.collection(THREADS_COLL).doc(threadId).set({
          unread    : true,
          updatedAt : FV.serverTimestamp()
        }, { merge: true });
        return ok({ threadId });
      }

      /* ---------- setStatus ---------- */
      if (action === "setStatus") {
        const { threadId, status, reason = null, actor = "system:api" } = body;
        if (!threadId) return bad("Missing threadId");
        if (!VALID_STATUSES.has(status)) return bad(`Invalid status '${status}'`);

        const tRef = db.collection(THREADS_COLL).doc(threadId);
        const snap = await tRef.get();
        if (!snap.exists) return json(404, { success: false, notFound: true });

        const prev = (snap.data() || {}).status || null;

        // ── v1.4: Manual auto_replied attribution ──────────────────
        // The auto_replied status is reserved for AI-completed sends
        // (set by etsyMailDraftSend.complete after Etsy confirms
        // delivery). When an operator manually moves a thread here
        // via the move-to dropdown, we MUST distinguish it from a
        // real AI auto-reply — otherwise the "AI handled rate"
        // metric is polluted with operator decisions.
        //
        // Rule: any setStatus → auto_replied is automatically marked
        // with manuallyMovedToAutoReplied=true plus actor + timestamp.
        // The only path that creates a "real" auto_replied is
        // etsyMailDraftSend.complete, which doesn't go through this
        // endpoint.
        const patch = { status, updatedAt: FV.serverTimestamp() };
        if (status === "auto_replied") {
          patch.manuallyMovedToAutoReplied = true;
          patch.manualMoveActor            = actor;
          patch.manualMoveAt               = FV.serverTimestamp();
          patch.manualMoveReason           = reason || null;
          patch.manualMoveFromStatus       = prev;
          // Clear AI confidence/decision attribution so reporting
          // doesn't incorrectly credit the AI for this thread.
          patch.lastAutoDecision           = "manually_moved_to_auto_replied";
          patch.lastAutoDecisionAt         = FV.serverTimestamp();
        } else if (prev === "auto_replied" || prev === "queued_for_auto_send") {
          // Moving OUT of an AI-handled status — clear the manual flag
          // so a future re-entry isn't haunted by stale provenance.
          patch.manuallyMovedToAutoReplied = FV.delete();
          patch.manualMoveActor            = FV.delete();
          patch.manualMoveAt               = FV.delete();
          patch.manualMoveReason           = FV.delete();
          patch.manualMoveFromStatus       = FV.delete();
        }

        await tRef.set(patch, { merge: true });
        await writeAudit({
          threadId,
          eventType: "status_changed",
          actor,
          payload: {
            from: prev, to: status, reason,
            manualMoveFlagged: status === "auto_replied"
          }
        });
        return ok({
          threadId, from: prev, to: status,
          manuallyMovedToAutoReplied: status === "auto_replied" ? true : null
        });
      }

      /* ---------- enqueueJob ---------- */
      if (action === "enqueueJob") {
        const { threadId, jobType, payload = {} } = body;
        if (!threadId) return bad("Missing threadId");
        if (!jobType)  return bad("Missing jobType");

        const jobRef = db.collection(JOBS_COLL).doc();
        const now    = FV.serverTimestamp();
        await jobRef.set({
          jobId     : jobRef.id,
          threadId,
          jobType,
          status    : "queued",
          claimedBy : null,
          claimedAt : null,
          attempts  : 0,
          lastError : null,
          payload,
          createdAt : now,
          updatedAt : now
        });
        await writeAudit({
          threadId,
          eventType: "job_enqueued",
          actor    : "system:api",
          payload  : { jobType, jobId: jobRef.id }
        });
        return ok({ jobId: jobRef.id });
      }

      /* ---------- v3.0 — purgeFolder ---------- */
      // Owner-only. Move every thread whose status matches `folderStatus`
      // (string OR array — supports multi-status folders like Auto-Reply)
      // out of that folder by setting status to PURGE_FOLDER_DEFAULT_DESTINATION.
      // Uses paginated batched writes so the operation scales and stays
      // under the Netlify 30s budget even on large folders. Each batch
      // commits 250 threads to keep well under Firestore's 500-op limit.
      //
      // v4.1.3 — Added optional `destinationStatus` parameter so the
      // caller can override the default destination. Necessary for
      // emptying the Needs Review folder itself: the default destination
      // IS pending_human_review, so without an override the action would
      // try to move threads from pending_human_review → pending_human_review
      // and 400 with "Cannot purge the destination folder." Now the
      // frontend sends destinationStatus:"replied" when emptying Needs
      // Review, which moves the threads to the neutral "handled" status
      // — they vanish from Needs Review but stay visible in All, and a
      // new inbound message can re-classify them back into Needs Review.
      if (action === "purgeFolder") {
        const {
          folderStatus,
          destinationStatus = PURGE_FOLDER_DEFAULT_DESTINATION,
          actor = null,
          reason = null
        } = body;
        if (!actor) return bad("Missing actor (operator name) for purgeFolder");
        if (!folderStatus) return bad("Missing folderStatus");

        // Normalize to an array of statuses (mirror RAIL_FILTERS shape).
        const targetStatuses = Array.isArray(folderStatus) ? folderStatus : [folderStatus];

        // Validate every status is real AND not on the blocked list.
        for (const s of targetStatuses) {
          if (!VALID_STATUSES.has(s)) return bad(`Invalid folderStatus '${s}'`);
          if (PURGE_FOLDER_BLOCKED.has(s)) {
            return bad(`Folder '${s}' is not eligible for bulk purge (transient or terminal status)`);
          }
        }
        // Validate the destination too. It must be a real status, not
        // blocked (we don't want to dump threads into transient or
        // terminal buckets), and not equal to any of the source statuses
        // (would be a no-op that burns writes and clutters the audit log).
        if (!VALID_STATUSES.has(destinationStatus)) {
          return bad(`Invalid destinationStatus '${destinationStatus}'`);
        }
        if (PURGE_FOLDER_BLOCKED.has(destinationStatus)) {
          return bad(`Destination folder '${destinationStatus}' is not eligible as a purge target`);
        }
        if (targetStatuses.includes(destinationStatus)) {
          return bad(`Cannot purge folder '${destinationStatus}' into itself`);
        }

        const owner = await requireOwner(actor);
        if (!owner.ok) {
          await logUnauthorized({
            actor,
            eventType: "purge_folder_unauthorized",
            payload  : { folderStatus: targetStatuses, destinationStatus, reason: owner.reason }
          });
          return json(403, { error: "Owner role required", reason: owner.reason });
        }

        // Iterate per-status to keep each query under Firestore's
        // 'in' operator's 30-value cap and to make audit rows clean.
        // For each status: page through 250-doc batches until exhausted.
        let totalMoved = 0;
        const perStatusCounts = {};
        const auditSampleIds  = [];   // first 20 thread ids for the audit row
        const BATCH_SIZE = 250;

        for (const status of targetStatuses) {
          let perStatusMoved = 0;
          // Loop until the query returns fewer docs than the batch size.
          // We don't use cursoring (startAfter) because each batch
          // moves the docs OUT of the query's result set, so the next
          // page-1 query naturally returns the next set.
          for (let safetyLoop = 0; safetyLoop < 100; safetyLoop++) {
            const snap = await db.collection(THREADS_COLL)
              .where("status", "==", status)
              .limit(BATCH_SIZE)
              .get();
            if (snap.empty) break;

            const batch = db.batch();
            for (const docSnap of snap.docs) {
              batch.update(docSnap.ref, {
                status                  : destinationStatus,
                folderPurgedFromStatus  : status,
                folderPurgedAt          : FV.serverTimestamp(),
                folderPurgedBy          : actor,
                updatedAt               : FV.serverTimestamp(),
                // Clear any auto_replied provenance (mirrors the same
                // hygiene the setStatus action does on equivalent moves).
                manuallyMovedToAutoReplied: FV.delete(),
                manualMoveActor         : FV.delete(),
                manualMoveAt            : FV.delete(),
                manualMoveReason        : FV.delete(),
                manualMoveFromStatus    : FV.delete()
              });
              if (auditSampleIds.length < 20) auditSampleIds.push(docSnap.id);
            }
            await batch.commit();
            perStatusMoved += snap.size;

            // If we got fewer docs than the limit, the query is exhausted.
            if (snap.size < BATCH_SIZE) break;
          }
          perStatusCounts[status] = perStatusMoved;
          totalMoved += perStatusMoved;
        }

        await writeAudit({
          threadId : null,
          eventType: "folder_purged",
          actor,
          payload  : {
            folderStatuses    : targetStatuses,
            destinationStatus : PURGE_FOLDER_DEFAULT_DESTINATION,
            totalMoved,
            perStatusCounts,
            sampleThreadIds   : auditSampleIds,
            reason
          }
        });

        return ok({
          totalMoved,
          perStatusCounts,
          destinationStatus: PURGE_FOLDER_DEFAULT_DESTINATION
        });
      }

      /* ---------- v3.0 — masterPurgePasswordStatus ---------- */
      // Returns { isSet, securityQuestion } so the UI can decide which
      // modal to render. Owner-gated like the other master-purge actions.
      // The hash itself is NEVER returned (defense in depth — even though
      // it's a one-way hash, no reason to ship it out).
      if (action === "masterPurgePasswordStatus") {
        const { actor = null } = body;
        if (!actor) return bad("Missing actor");
        const owner = await requireOwner(actor);
        if (!owner.ok) return json(403, { error: "Owner role required", reason: owner.reason });

        const snap = await db.doc(MASTER_PURGE_AUTH_DOC).get();
        if (!snap.exists) return ok({ isSet: false, securityQuestion: null });

        const d = snap.data() || {};
        return ok({
          isSet           : !!(d.passwordHash && d.salt),
          securityQuestion: d.securityQuestion || null,
          updatedAt       : d.updatedAt && d.updatedAt.toMillis ? d.updatedAt.toMillis() : null,
          updatedBy       : d.updatedBy || null
        });
      }

      /* ---------- v3.0 — masterPurgePasswordSet ---------- */
      // First-time setup OR password change. Stores PBKDF2 hashes with
      // per-secret salts. The security question is stored as plaintext
      // (it's meant to be readable on the recovery modal); only the
      // ANSWER is hashed.
      if (action === "masterPurgePasswordSet") {
        const {
          password,
          securityQuestion,
          securityAnswer,
          actor = null
        } = body;

        if (!actor) return bad("Missing actor");
        if (!password || String(password).length < 8) {
          return bad("Password must be at least 8 characters");
        }
        if (!securityQuestion || String(securityQuestion).trim().length < 4) {
          return bad("Security question is required (min 4 chars)");
        }
        if (!securityAnswer || String(securityAnswer).trim().length < 1) {
          return bad("Security answer is required");
        }

        const owner = await requireOwner(actor);
        if (!owner.ok) {
          await logUnauthorized({
            actor,
            eventType: "master_purge_password_set_unauthorized",
            payload  : { reason: owner.reason }
          });
          return json(403, { error: "Owner role required", reason: owner.reason });
        }

        const pwd = hashSecret(password);
        const ans = hashSecret(normalizeAnswer(securityAnswer));

        await db.doc(MASTER_PURGE_AUTH_DOC).set({
          passwordHash       : pwd.hash,
          salt               : pwd.salt,
          iterations         : pwd.iterations,
          digest             : pwd.digest,
          securityQuestion   : String(securityQuestion).trim(),
          securityAnswerHash : ans.hash,
          securityAnswerSalt : ans.salt,
          securityAnswerIterations: ans.iterations,
          securityAnswerDigest    : ans.digest,
          updatedAt          : FV.serverTimestamp(),
          updatedBy          : actor
        }, { merge: false });

        await writeAudit({
          threadId : null,
          eventType: "master_purge_password_set",
          actor,
          payload  : { hadPriorPassword: false /* one-bit info; we don't tell the audit log */ }
        });

        return ok({ ok: true });
      }

      /* ---------- v3.0 — masterPurgePasswordReset ---------- */
      // Reset the password by answering the security question.
      // Re-checks owner role (defense in depth — the question is the
      // factor, the role is the gate).
      if (action === "masterPurgePasswordReset") {
        const { securityAnswer, newPassword, actor = null } = body;
        if (!actor) return bad("Missing actor");
        if (!newPassword || String(newPassword).length < 8) {
          return bad("New password must be at least 8 characters");
        }
        if (!securityAnswer) return bad("Security answer is required");

        const owner = await requireOwner(actor);
        if (!owner.ok) return json(403, { error: "Owner role required", reason: owner.reason });

        const snap = await db.doc(MASTER_PURGE_AUTH_DOC).get();
        if (!snap.exists) return bad("No master-purge password is configured yet");

        const d = snap.data() || {};
        const stored = {
          hash      : d.securityAnswerHash,
          salt      : d.securityAnswerSalt,
          iterations: d.securityAnswerIterations,
          digest    : d.securityAnswerDigest
        };
        const matches = verifySecret(normalizeAnswer(securityAnswer), stored);
        if (!matches) {
          await writeAudit({
            threadId : null,
            eventType: "master_purge_password_reset_failed",
            actor,
            payload  : { reason: "security_answer_mismatch" }
          });
          return json(403, { error: "Security answer is incorrect" });
        }

        // Re-hash the new password with a fresh salt; preserve the
        // existing security question + answer hash (resetting the
        // password doesn't reset the recovery factor).
        const pwd = hashSecret(newPassword);
        await db.doc(MASTER_PURGE_AUTH_DOC).set({
          passwordHash: pwd.hash,
          salt        : pwd.salt,
          iterations  : pwd.iterations,
          digest      : pwd.digest,
          updatedAt   : FV.serverTimestamp(),
          updatedBy   : actor,
          lastResetAt : FV.serverTimestamp()
        }, { merge: true });

        await writeAudit({
          threadId : null,
          eventType: "master_purge_password_reset",
          actor,
          payload  : {}
        });

        return ok({ ok: true });
      }

      /* ---------- v3.0 — masterPurgeWipe ---------- */
      // Owner-only AND password-gated. Deletes every doc in
      // EtsyMail_Threads (with their messages subcollections),
      // EtsyMail_Drafts, and EtsyMail_Jobs. Preserves config, OAuth,
      // listing templates, audit log, sales contexts.
      if (action === "masterPurgeWipe") {
        const { password, actor = null, confirmPhrase = "" } = body;
        if (!actor) return bad("Missing actor");
        if (!password) return bad("Password is required");
        // UI types this exact phrase as the second factor — protects
        // against autofilled passwords + accidental click-throughs.
        if (confirmPhrase !== "WIPE ALL DATA") {
          return bad("confirmPhrase must equal 'WIPE ALL DATA'");
        }

        const owner = await requireOwner(actor);
        if (!owner.ok) {
          await logUnauthorized({
            actor,
            eventType: "master_purge_wipe_unauthorized",
            payload  : { reason: owner.reason }
          });
          return json(403, { error: "Owner role required", reason: owner.reason });
        }

        const authSnap = await db.doc(MASTER_PURGE_AUTH_DOC).get();
        if (!authSnap.exists) {
          return bad("No master-purge password is configured. Set one in Settings before attempting a purge.");
        }
        const authData = authSnap.data() || {};
        const matches  = verifySecret(password, {
          hash      : authData.passwordHash,
          salt      : authData.salt,
          iterations: authData.iterations,
          digest    : authData.digest
        });
        if (!matches) {
          await writeAudit({
            threadId : null,
            eventType: "master_purge_wipe_failed",
            actor,
            payload  : { reason: "password_mismatch" }
          });
          return json(403, { error: "Incorrect password" });
        }

        // ── Pre-flight audit row ──────────────────────────────────────
        // Written BEFORE we delete anything, so even if the wipe is
        // partially executed and then the function gets killed mid-way,
        // there's a forensic record of the attempt.
        const startedAt = Date.now();
        await writeAudit({
          threadId : null,
          eventType: "master_purge_wipe_started",
          actor,
          payload  : { startedAt, confirmPhrase }
        });

        // ── Wipe pass ─────────────────────────────────────────────────
        // For each collection: page through in BATCH_SIZE chunks, batch-
        // delete. Threads also need their messages subcollections nuked,
        // which we do inline. Stops if we get within 5 seconds of the
        // 30-second function budget so we always have time to write the
        // completion audit row.
        const BATCH_SIZE = 250;
        const SOFT_BUDGET_MS = 25_000;
        const counts = { threads: 0, messages: 0, drafts: 0, jobs: 0 };
        let truncated = false;

        async function wipeCollection(collName, perDocCallback = null) {
          for (let safetyLoop = 0; safetyLoop < 200; safetyLoop++) {
            if (Date.now() - startedAt > SOFT_BUDGET_MS) {
              truncated = true;
              return;
            }
            const snap = await db.collection(collName).limit(BATCH_SIZE).get();
            if (snap.empty) return;

            // If a per-doc callback is provided (used for threads to nuke
            // their messages subcollection), run it before the parent
            // delete — Firestore doesn't cascade subcollection deletes.
            if (perDocCallback) {
              for (const docSnap of snap.docs) {
                await perDocCallback(docSnap);
                if (Date.now() - startedAt > SOFT_BUDGET_MS) {
                  truncated = true;
                  return;
                }
              }
            }

            const batch = db.batch();
            for (const docSnap of snap.docs) batch.delete(docSnap.ref);
            await batch.commit();

            if (collName === THREADS_COLL) counts.threads += snap.size;
            else if (collName === DRAFTS_COLL) counts.drafts += snap.size;
            else if (collName === JOBS_COLL)   counts.jobs   += snap.size;

            if (snap.size < BATCH_SIZE) return;
          }
        }

        // Wipe threads (with messages subcollections) first — they're
        // the most numerous and most user-facing.
        await wipeCollection(THREADS_COLL, async (threadSnap) => {
          // Wipe the messages subcollection in batches.
          for (let inner = 0; inner < 50; inner++) {
            const msgs = await threadSnap.ref.collection("messages").limit(BATCH_SIZE).get();
            if (msgs.empty) return;
            const batch = db.batch();
            for (const m of msgs.docs) batch.delete(m.ref);
            await batch.commit();
            counts.messages += msgs.size;
            if (msgs.size < BATCH_SIZE) return;
          }
        });
        if (!truncated) await wipeCollection(DRAFTS_COLL);
        if (!truncated) await wipeCollection(JOBS_COLL);

        const durationMs = Date.now() - startedAt;
        await writeAudit({
          threadId : null,
          eventType: truncated ? "master_purge_wipe_truncated" : "master_purge_wipe_completed",
          actor,
          payload  : {
            counts,
            durationMs,
            truncated,
            // If truncated, the operator should re-run to finish.
            ...(truncated ? { hint: "Hit the per-invocation budget — re-run masterPurgeWipe to delete the remainder." } : {})
          }
        });

        return ok({
          ok        : true,
          counts,
          durationMs,
          truncated,
          ...(truncated ? { hint: "Hit per-invocation budget; re-run to delete the remainder." } : {})
        });
      }

      return bad(`Unknown action '${action}'`);
    }

    return json(405, { error: "Method Not Allowed" });

  } catch (err) {
    console.error("etsyMailThreads error:", err);
    return json(500, { error: err.message || String(err) });
  }
};
