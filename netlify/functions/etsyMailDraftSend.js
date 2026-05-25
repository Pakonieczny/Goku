/*  netlify/functions/etsyMailDraftSend.js
 *
 *  M5 send-pipeline orchestrator. Because the Etsy API has no public
 *  endpoint to send a conversation message, M5 routes sends through the
 *  Chrome extension, which scripts Etsy's own compose-and-send DOM flow
 *  on the operator's browser. This function is the coordination layer.
 *
 *  Flow:
 *    [inbox UI]      POST op=enqueue  →  writes EtsyMail_Drafts/{draftId}
 *                                        with status=queued, attachments[]
 *    [inbox UI]      GET  op=status   →  polls EtsyMail_Drafts/{draftId}
 *
 *    [extension]     POST op=peek     →  "is there a queued draft for the
 *                                        Etsy thread this tab has open?"
 *    [extension]     POST op=claim    →  atomic claim via Firestore txn;
 *                                        flips status queued → sending
 *    [extension]     POST op=heartbeat → keeps the claim fresh during the
 *                                        DOM-scripting phase (≤60s stale
 *                                        before the cleanup cron reclaims)
 *    [extension]     POST op=complete → flips status sending → sent
 *    [extension]     POST op=fail     → flips status sending → failed,
 *                                        with { error, partial } detail
 *
 *  Draft document schema (EtsyMail_Drafts/{draftId}):
 *    {
 *      draftId            : "draft_etsy_conv_1651714855",
 *      threadId           : "etsy_conv_1651714855",
 *      etsyConversationUrl: "https://www.etsy.com/your/conversations/1651714855",
 *      text               : "Hi Karrie! Thanks for your order…",
 *      status             : "draft" | "queued" | "sending" | "sent" | "sent_text_only" | "failed",
 *      attachments        : [
 *        {
 *          attachmentId : "att_abc123",
 *          type         : "image" | "listing" | "tracking_image",
 *          // for image/tracking_image:
 *          storagePath  : "etsymail/drafts/.../att_abc123.png",
 *          proxyUrl     : "/.netlify/functions/etsyMailImage?path=...",
 *          contentType  : "image/png",
 *          bytes        : 12345,
 *          filename     : "screenshot.png",
 *          // for listing:
 *          listingId    : "1234567890",
 *          listingUrl   : "https://www.etsy.com/listing/1234567890",
 *          listingTitle : "Sterling Silver Cardinal Charm",
 *          thumbnail    : "...",
 *          // for tracking_image specifically:
 *          trackingCode : "9400...",
 *          carrier      : "USPS"
 *        },
 *        ...
 *      ],
 *      // Populated by the AI draft generator (M4); preserved here so the
 *      // draft doc is self-contained.
 *      generatedByAI      : true,
 *      aiModel            : "claude-sonnet-4-6",
 *      aiReasoning        : "...",
 *      aiActiveQuestion   : "...",
 *      // Lifecycle
 *      createdBy          : "Paul_K",
 *      createdAt          : Timestamp,
 *      queuedAt           : Timestamp,
 *      sentAt             : Timestamp,
 *      // Send coordination
 *      sendSessionId      : "ext_abc123",  // extension instance that claimed it
 *      sendClaimedAt      : Timestamp,
 *      sendHeartbeatAt    : Timestamp,     // refreshed every ~5s during send
 *      sendAttempts       : 2,             // 3rd try before giving up
 *      sendError          : string | null,
 *      sendPartialSuccess : true | false,  // sent text but images failed
 *      updatedAt          : Timestamp
 *    }
 *
 *  Auth:
 *    - enqueue + status ops: no secret required (same-origin inbox).
 *    - peek, claim, heartbeat, complete, fail: require X-EtsyMail-Secret
 *      (extension-invoked). Enforced per-op below.
 */

const admin = require("./firebaseAdmin");
const { requireExtensionAuth, CORS } = require("./_etsyMailAuth");
const { buildOptimisticDoc } = require("./etsyMailOptimisticMessage");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const DRAFTS_COLL = "EtsyMail_Drafts";
const AUDIT_COLL  = "EtsyMail_Audit";
const CONFIG_COLL = "EtsyMail_Config";          // v0.9.1: kill-switch lives here

const MAX_SEND_ATTEMPTS      = 3;
const STALE_HEARTBEAT_MS     = 60 * 1000;       // 60s: if no heartbeat, treat claim as abandoned
const MAX_CLAIM_LOOKBACK_MIN = 30;              // ignore draft docs older than 30 min from peek

// Kill-switch cache: avoid one Firestore read per peek/claim. The cache
// invalidates after 15 seconds, so flipping the switch in Firestore takes
// at most 15s + the next peek interval to take effect across all tabs.
let _killSwitchCache = { value: null, fetchedAt: 0 };
const KILL_SWITCH_CACHE_MS = 15 * 1000;

// v5.21 — Refund / return signal detection.
//
// Mirror of the patterns in etsyMailDraftReply.js. Duplicated here (not
// imported) to keep this function self-contained and avoid cross-module
// import overhead in the hot path. The patterns must stay in sync —
// when adding a new pattern to draftReply, mirror it here.
//
// Purpose: catch refund-relevant outbound text on the manual-send path.
// AI-drafted outbound is already detected in draftReply; this catches
// the case where an operator types a manual reply about refunds without
// using AI Draft. Setting thread.refundFlaggedAt here makes the thread
// appear in the inbox's "Refunds" folder, mirroring how draftReply
// flags the AI-handled case.
const REFUND_SIGNAL_PATTERNS = [
  // Etsy "Help with Order" structured-form fields
  /\b(?:your\s+)?ideal\s+resolution\s*:\s*(?:return|refund|replace|exchange)/i,
  /\bpreferred\s+refund\s+method\s*:/i,
  /\bI\s+want\s+to\s+message\s+the\s+seller\s+about/i,
  // Direct customer refund/return language
  /\b(?:I[\u2019']?d?|I\s+would)\s+(?:like|want)\s+to\s+(?:return|refund)\b/i,
  /\b(?:want|need|requesting?)\s+(?:to\s+)?(?:get\s+)?(?:a\s+)?refund\b/i,
  /\bcan\s+I\s+(?:return|get\s+a\s+refund|refund\s+this)\b/i,
  /\bhow\s+(?:do|can)\s+I\s+(?:return|get\s+a\s+refund|refund)\b/i,
  /\brefund\s+(?:request|please|me|for|on)\b/i,
  /\bmoney\s+back\b/i,
  /\breturning\s+(?:the|this|my|it|them|these)\b/i,
  /\bsend(?:ing)?\s+(?:it|them|these|this|the\s+\w+)\s+back\s+for\s+(?:a\s+)?(?:refund|return)/i,
  /\bI\s+(?:want|need|would\s+like)\s+to\s+send\s+(?:it|them|these|this)\s+back\b/i,
  // Staff / operator outbound return-instruction language
  /\breturn\s+address\s*:/i,
  /\bsend\s+(?:it|them|these|this|the\s+\w+)\s+back\s+(?:in\s+(?:its|their)\s+original|within\s+\d+\s+days)/i,
  /\bonce\s+(?:they|it)\s+arrive[s]?\s+we[\u2019']?ll\s+process\s+(?:your\s+)?refund/i,
  /\bhappy\s+to\s+(?:take\s+(?:these|that|it|them)\s+back|accept\s+(?:the|your)\s+return)/i,
  /\b14[\s-]?day\s+(?:return|refund)\s+window/i,
  /\bprocess\s+(?:your\s+|a\s+)?refund\s+(?:once|after|when)\b/i,
];

function _detectRefundSignals(text) {
  if (!text || typeof text !== "string") return null;
  for (const rx of REFUND_SIGNAL_PATTERNS) {
    const m = text.match(rx);
    if (m) return m[0];
  }
  return null;
}

function json(statusCode, body) {
  return { statusCode, headers: CORS, body: JSON.stringify(body) };
}
function bad(msg, code = 400) { return json(code, { error: msg }); }
function ok(payload) { return json(200, { success: true, ...payload }); }

/** Read the global send-disabled flag from EtsyMail_Config/global.
 *  Returns { disabled, reason, by }. Cached for KILL_SWITCH_CACHE_MS to
 *  avoid hot-pathing Firestore on every peek/claim. */
async function getKillSwitch() {
  if (Date.now() - _killSwitchCache.fetchedAt < KILL_SWITCH_CACHE_MS && _killSwitchCache.value) {
    return _killSwitchCache.value;
  }
  try {
    const snap = await db.collection(CONFIG_COLL).doc("global").get();
    const data = snap.exists ? snap.data() : {};
    const value = {
      disabled : !!data.sendDisabled,
      reason   : data.sendDisabledReason || null,
      by       : data.sendDisabledBy     || null,
      at       : data.sendDisabledAt     ? data.sendDisabledAt.toMillis() : null
    };
    _killSwitchCache = { value, fetchedAt: Date.now() };
    return value;
  } catch (e) {
    console.warn("killSwitch fetch failed:", e.message);
    // Fail-open: if we can't read the doc, allow sends. Better than
    // hard-failing the whole pipeline if Firestore has a transient issue.
    return { disabled: false, reason: null, by: null, at: null };
  }
}

async function audit(threadId, draftId, eventType, actor, payload) {
  try {
    await db.collection(AUDIT_COLL).add({
      threadId : threadId || null,
      draftId  : draftId  || null,
      eventType,
      actor    : actor || "system",
      payload  : payload || {},
      createdAt: FV.serverTimestamp()
    });
  } catch (e) {
    console.warn("audit write failed:", e.message);
  }
}

function serializeDoc(snap) {
  if (!snap.exists) return null;
  const data = snap.data();
  const out  = { id: snap.id };
  for (const [k, v] of Object.entries(data)) {
    if (v && typeof v.toMillis === "function") {
      out[k] = { _ts: true, ms: v.toMillis() };
    } else {
      out[k] = v;
    }
  }
  return out;
}

function isStaleHeartbeat(hbTs) {
  if (!hbTs) return true;
  const ms = hbTs.toMillis ? hbTs.toMillis() : Number(hbTs);
  if (!ms) return true;
  return (Date.now() - ms) > STALE_HEARTBEAT_MS;
}

/** True if a queued draft has been sitting too long to safely send.
 *  Operators expect that clicking Send sends NOW — if a queued draft
 *  was forgotten about for hours and an Etsy tab opens later, sending
 *  yesterday's draft today is the wrong behavior. */
function isStaleQueued(queuedAtTs) {
  if (!queuedAtTs) return false;   // no queuedAt = treat as fresh (defensive)
  const ms = queuedAtTs.toMillis ? queuedAtTs.toMillis() : Number(queuedAtTs);
  if (!ms) return false;
  return (Date.now() - ms) > (MAX_CLAIM_LOOKBACK_MIN * 60 * 1000);
}

// ───────────────────────────────────────────────────────────────────
//  v1.4: shared queued_for_auto_send demotion helpers
//
//  When a draft fails terminally (the extension reported failure, the
//  send queue reaper expired it, the peek path marked it stale, etc.),
//  the parent thread MUST be demoted out of `queued_for_auto_send` —
//  otherwise it's stuck in the Auto-Reply folder showing the animated
//  "sending…" pill forever, even though no send will ever happen.
//
//  These helpers exist so every code path that fails a draft uses the
//  same demotion logic. Two flavors:
//    - demoteThreadInTxn(tx, threadId, reason) — for callers already
//      inside a transaction (complete/fail ops); single-write, no
//      extra read since the txn already has the thread snap available.
//    - demoteThreadStandalone(threadId, reason) — for callers outside
//      a txn (peek-path stale expiration); runs its own txn for
//      consistency with concurrent pipeline runs.
//  Both are idempotent — re-running them on a thread that's already
//  past queued_for_auto_send is a no-op.
// ───────────────────────────────────────────────────────────────────

const THREADS_COLL_NAME = "EtsyMail_Threads";

/** Inside an existing transaction, read the thread doc and demote to
 *  pending_human_review if its status is queued_for_auto_send.
 *  Returns the new status (or null if no change). */
/** Inside an existing transaction, read the thread doc and demote to
 *  pending_human_review if its status is queued_for_auto_send.
 *  Returns the new status (or null if no change).
 *
 *  WARNING: This helper does a tx.get followed by a tx.set. It MUST NOT
 *  be called after any prior tx.set in the same transaction, or Firestore
 *  rejects with "all reads must be executed before all writes". For
 *  callers that already issued writes, use demoteThreadWriteOnlyInTxn
 *  with a pre-fetched snapshot instead.
 */
async function demoteThreadInTxn(tx, threadId, reason) {
  if (!threadId) return null;
  const tRef = db.collection(THREADS_COLL_NAME).doc(threadId);
  const tSnap = await tx.get(tRef);
  if (!tSnap.exists) return null;
  if (tSnap.data().status !== "queued_for_auto_send") return null;
  tx.set(tRef, {
    status            : "pending_human_review",
    lastAutoDecision  : reason,
    lastAutoDecisionAt: FV.serverTimestamp(),
    updatedAt         : FV.serverTimestamp()
  }, { merge: true });
  return "pending_human_review";
}

/** v3.15: Write-only variant of demoteThreadInTxn for use AFTER prior
 *  writes in the same transaction. Caller must pre-fetch the thread
 *  snapshot during the read phase, then pass it here. Returns the new
 *  status (or null if no change).
 *
 *  Usage:
 *    // Read phase (top of transaction):
 *    const prefetch = await prefetchThreadForDemoteInTxn(tx, threadId);
 *    // ...other reads...
 *    // Write phase:
 *    tx.set(draftRef, ...);          // some other write
 *    demoteThreadWriteOnlyInTxn(tx, prefetch, reason);
 */
async function prefetchThreadForDemoteInTxn(tx, threadId) {
  if (!threadId) return null;
  const tRef = db.collection(THREADS_COLL_NAME).doc(threadId);
  const tSnap = await tx.get(tRef);
  return {
    threadId,
    tRef,
    exists       : tSnap.exists,
    currentStatus: tSnap.exists ? (tSnap.data().status || null) : null
  };
}

function demoteThreadWriteOnlyInTxn(tx, prefetch, reason) {
  if (!prefetch || !prefetch.exists) return null;
  if (prefetch.currentStatus !== "queued_for_auto_send") return null;
  tx.set(prefetch.tRef, {
    status            : "pending_human_review",
    lastAutoDecision  : reason,
    lastAutoDecisionAt: FV.serverTimestamp(),
    updatedAt         : FV.serverTimestamp()
  }, { merge: true });
  return "pending_human_review";
}

/** Standalone (no caller txn) version. Runs its own transaction for
 *  the same atomicity guarantees. Used by peek-path stale expiration
 *  and by the send-queue reaper. */
async function demoteThreadStandalone(threadId, reason) {
  if (!threadId) return null;
  return await db.runTransaction(async (tx) => demoteThreadInTxn(tx, threadId, reason));
}

/** Normalize an attachments array for persistence. Strips sentinels,
 *  validates required fields per type, and ensures attachmentId is set. */
function normalizeAttachments(raw) {
  if (!Array.isArray(raw)) return [];
  const out = [];
  for (const a of raw) {
    if (!a || typeof a !== "object") continue;
    const type = a.type;
    if (type !== "image" && type !== "listing" && type !== "tracking_image") continue;

    const common = {
      attachmentId: a.attachmentId || ("att_" + Math.random().toString(36).slice(2, 12)),
      type
    };
    if (type === "image") {
      // Operator-uploaded images need both: storagePath identifies the
      // bucket object, proxyUrl is what the extension fetches.
      if (!a.storagePath || !a.proxyUrl) continue;
      out.push({
        ...common,
        storagePath : String(a.storagePath),
        proxyUrl    : String(a.proxyUrl),
        contentType : a.contentType || "image/png",
        bytes       : Number(a.bytes) || null,
        filename    : a.filename || null
      });
    } else if (type === "tracking_image") {
      // Tracking images only need proxyUrl — storagePath isn't always
      // populated by the M4 snapshot path, and the extension only
      // needs the proxyUrl to fetch bytes anyway. Dropping these on
      // null storagePath was silently breaking image attachment for
      // every send that included a tracking image.
      if (!a.proxyUrl) continue;
      out.push({
        ...common,
        proxyUrl      : String(a.proxyUrl),
        storagePath   : a.storagePath || null,   // optional, kept for forensics
        contentType   : a.contentType || "image/png",
        bytes         : Number(a.bytes) || null,
        filename      : a.filename || null,
        trackingCode  : a.trackingCode || null,
        carrier       : a.carrier      || null,
        trackingStatus: a.trackingStatus || null
      });
    } else if (type === "listing") {
      if (!a.listingId) continue;
      out.push({
        ...common,
        listingId   : String(a.listingId),
        listingUrl  : a.listingUrl  || `https://www.etsy.com/listing/${a.listingId}`,
        listingTitle: a.listingTitle || null,
        thumbnail   : a.thumbnail    || null,
        price       : a.price        || null
      });
    }
  }
  return out;
}

// ─── v3.30: Tracking-image attachment safety net ────────────────────────
//
// Background: the AI's generate_tracking_image tool kicks off a background
// snapshot job (etsyMailTrackingSnapshot-background) and records a
// `tracking_image` entry on draft.trackingImages + draft.attachments. On
// cache MISS the job is still pending when the AI finishes its draft.
// The inbox UI's pollTrackingJob is supposed to detect ready status and
// call syncTrackingImagesToChips, which promotes the image into the
// in-memory composer chips. If the operator clicks Send before the chip
// is promoted (or the page never had a polling session open at all), the
// send body's `attachments` array is empty, the enqueue transaction
// fully replaces draft.attachments with that empty array, and the
// customer receives text-only. The later draftWriteback can't recreate
// the attachment — it only mutates existing entries.
//
// The auto-pipeline path already solves this via its own waitForTrackingJobs
// gate (see etsyMailAutoPipeline-background.js). The manual-Send path
// did not. v3.30 adds the equivalent gate here.
//
// Policy:
//   - draft.trackingImages is the canonical record of what should be
//     attached. The send body's attachments is a HINT.
//   - Honor in-memory operator de-queue (queuedForSend === false) — those
//     stay excluded.
//   - For status === "ready" entries, synthesize a tracking_image
//     attachment if one isn't already in the body's list (dedup by
//     trackingCode).
//   - For status === "pending" or "running" entries, poll the job doc
//     for up to TRACKING_WAIT_MAX_MS, then either synthesize (if it
//     became ready) or, for manual operator sends, skip pending images
//     and continue. Auto/system sends may still opt into blocking.

const TRACKING_WAIT_MAX_MS  = 8000;   // hard cap on synchronous wait — Netlify funcs are 10s
const TRACKING_POLL_EVERY_MS = 1000;
const TRACKING_JOBS_COLL    = "EtsyMail_TrackingJobs";

/** Synthesize a tracking_image attachment from a draft.trackingImages entry.
 *  Returns null if the entry doesn't have what the extension needs to
 *  fetch bytes (proxyUrl is deterministic from trackingCode, so the gate
 *  is really just "has a trackingCode"). */
function trackingImageEntryToAttachment(img) {
  if (!img || !img.trackingCode) return null;
  const code = String(img.trackingCode);
  return {
    attachmentId  : "trk_" + code,
    type          : "tracking_image",
    trackingCode  : code,
    carrier       : img.carrierDisplay || img.carrier || "Tracking",
    trackingStatus: img.statusText || null,
    proxyUrl      : "/.netlify/functions/etsyMailTrackingImage?trackingCode=" + encodeURIComponent(code),
    storagePath   : img.imageStoragePath || null,
    contentType   : "image/png",
    bytes         : null,
    filename      : "tracking-" + code.replace(/[^a-z0-9]/gi, "_") + ".png"
  };
}

/** Read the current draft and reconcile its trackingImages with the
 *  caller-provided attachments. Waits for any pending tracking jobs up to
 *  TRACKING_WAIT_MAX_MS. Returns:
 *    { ok:true,  merged: <attachments[]> }            on success
 *    { ok:false, errorCode, error, pending: <codes> } if any are still
 *                                                    pending after the
 *                                                    wait
 *
 *  Caller-provided attachments take precedence on dedup conflicts — the
 *  inbox sometimes ships chips with extra fields (e.g. operator-toggled
 *  ones). We only synthesize an entry when there is NO existing
 *  tracking_image attachment for that trackingCode in the send body. */
async function reconcileTrackingAttachments(draftId, bodyAttachments, options = {}) {
  const bodyAtts = Array.isArray(bodyAttachments) ? [...bodyAttachments] : [];
  const blockOnPending = options.blockOnPending !== false;

  let snap;
  try {
    snap = await db.collection(DRAFTS_COLL).doc(draftId).get();
  } catch (e) {
    console.warn(`[enqueue] tracking-recon: draft read failed for ${draftId}: ${e.message}`);
    return { ok: true, merged: bodyAtts };  // fail-open — don't block enqueue on a recon-only read failure
  }
  if (!snap.exists) return { ok: true, merged: bodyAtts };

  const draft = snap.data() || {};
  const trackingImages = Array.isArray(draft.trackingImages) ? draft.trackingImages : [];
  if (!trackingImages.length) return { ok: true, merged: bodyAtts };

  // Index existing tracking_image attachments in the send body by trackingCode
  // so we don't double-attach the same image.
  const codesInBody = new Set();
  for (const a of bodyAtts) {
    if (a && a.type === "tracking_image" && a.trackingCode) {
      codesInBody.add(String(a.trackingCode));
    }
  }

  // Categorize trackingImages entries. A tracking image is sendable if
  // either its status is ready OR it already has image metadata. Older
  // draft docs sometimes carried imageUrl/imageStoragePath without updating
  // status, and treating those as pending caused avoidable text-only sends.
  const toAttachReady = [];        // ready right now → synthesize and union
  const toWaitOn      = [];        // pending/running with jobId → poll
  const noJobPending  = [];        // pending but no jobId → explicit retry, not silent skip

  for (const img of trackingImages) {
    if (!img || !img.trackingCode) continue;
    if (img.queuedForSend === false) continue;                // operator opt-out
    if (img.status === "failed") continue;                    // tracking lookup failed; skip silently
    if (codesInBody.has(String(img.trackingCode))) continue;   // already in send body

    const looksReady = img.status === "ready" || !!img.imageUrl || !!img.imageStoragePath;
    if (looksReady) {
      toAttachReady.push({ ...img, status: "ready" });
    } else if (img.jobId) {
      // status is "pending" / "running" / undefined — wait on the job doc
      toWaitOn.push(img);
    } else {
      // This should be rare, but it is the dangerous case: the draft says
      // a tracking image should be attached, but there is neither ready
      // image data nor a job to poll. Do not silently send text-only.
      noJobPending.push(img);
    }
  }

  // Fast path: nothing pending, just synthesize and return.
  if (!toWaitOn.length && !noJobPending.length) {
    for (const img of toAttachReady) {
      const a = trackingImageEntryToAttachment(img);
      if (a) bodyAtts.push(a);
    }
    return { ok: true, merged: bodyAtts };
  }

  // Slow path: poll the job docs until all are ready/failed or deadline hit.
  console.log(`[enqueue] tracking-recon: waiting on ${toWaitOn.length} pending tracking job(s) for ${draftId}`);
  const deadline = Date.now() + TRACKING_WAIT_MAX_MS;
  const stillPending = new Map(); // jobId → img
  for (const img of toWaitOn) {
    stillPending.set(img.jobId, img);
  }

  while (stillPending.size && Date.now() < deadline) {
    for (const [jobId, img] of [...stillPending.entries()]) {
      let job;
      try {
        const jSnap = await db.collection(TRACKING_JOBS_COLL).doc(jobId).get();
        if (!jSnap.exists) continue;
        job = jSnap.data();
      } catch (e) {
        console.warn(`[enqueue] tracking-recon: job poll failed for ${jobId}: ${e.message}`);
        continue;
      }
      if (job.status === "ready") {
        // Hydrate the local img with job data so the synthesized attachment
        // has the freshest fields.
        const hydrated = {
          ...img,
          status          : "ready",
          imageUrl        : job.imageUrl || img.imageUrl || null,
          imageStoragePath: job.imageStoragePath || img.imageStoragePath || null,
          imageWidth      : job.imageWidth || null,
          imageHeight     : job.imageHeight || null,
          carrier         : job.carrier || img.carrier || null,
          carrierDisplay  : job.carrierDisplay || img.carrierDisplay || null,
          statusKey       : job.statusKey || img.statusKey || null,
          statusText      : job.statusText || img.statusText || null
        };
        toAttachReady.push(hydrated);
        stillPending.delete(jobId);
      } else if (job.status === "failed") {
        console.warn(`[enqueue] tracking-recon: job ${jobId} (${img.trackingCode}) failed: ${job.error || "unknown"} — skipping attachment`);
        stillPending.delete(jobId);
      }
      // else: keep polling
    }
    if (stillPending.size) {
      await new Promise(r => setTimeout(r, TRACKING_POLL_EVERY_MS));
    }
  }

  // Anything still pending at this point can either block or be skipped.
  // Manual Send via Etsy must never be blocked by inferred/generated
  // attachment state, so the inbox passes blockOnPending:false. Auto/system
  // callers can keep the stricter 409 behavior if desired.
  const pending = [
    ...noJobPending.map(i => i.trackingCode).filter(Boolean),
    ...[...stillPending.values()].map(i => i.trackingCode).filter(Boolean)
  ];
  if (pending.length && blockOnPending) {
    return {
      ok       : false,
      errorCode: "TRACKING_STILL_PENDING",
      error    : `Tracking image${pending.length === 1 ? "" : "s"} still generating (${pending.join(", ")}). Try Send again in a few seconds.`,
      pending
    };
  }

  // Synthesize and union the ready ones. Pending entries are intentionally
  // skipped when blockOnPending:false so manual sends can continue.
  for (const img of toAttachReady) {
    const a = trackingImageEntryToAttachment(img);
    if (a) bodyAtts.push(a);
  }

  return {
    ok: true,
    merged: bodyAtts,
    skippedPendingTracking: pending
  };
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  // ── Auth: ALL ops now require X-EtsyMail-Secret ────────────────────
  // v0.9.1 (#1): the inbox subdomain is publicly reachable, so any op
  // could be hit by an outside party who finds it. Every op now requires
  // the same secret the extension sends. The inbox forwards it from
  // localStorage('etsymail_secret'). If ETSYMAIL_EXTENSION_SECRET env
  // var is unset, requireExtensionAuth falls back to passthrough (dev).
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  // ── GET ops ────────────────────────────────────────────────────────
  if (event.httpMethod === "GET") {
    const qs = event.queryStringParameters || {};
    const op = qs.op;
    if (!op) return bad("Missing op");

    /* ── status ──
     *  Inbox polls this while waiting for the extension to send. */
    if (op === "status") {
      const { draftId } = qs;
      if (!draftId) return bad("Missing draftId");
      const snap = await db.collection(DRAFTS_COLL).doc(String(draftId)).get();
      if (!snap.exists) return json(404, { error: "Draft not found", draftId });
      return ok({ draft: serializeDoc(snap) });
    }

    /* ── killswitch_status ──
     *  Inbox polls this to render the kill-switch banner. Cheap. */
    if (op === "killswitch_status") {
      const ks = await getKillSwitch();
      return ok({ killSwitch: ks });
    }

    return bad(`Unknown GET op '${op}'`);
  }

  if (event.httpMethod !== "POST") {
    return json(405, { error: "Method Not Allowed" });
  }

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return bad("Invalid JSON body"); }

  const { op } = body;
  if (!op) return bad("Missing op");

  // Note: per-op auth gating removed in v0.9.1 — ALL ops authed at top.

  try {
    /* ── enqueue (inbox → server) ─────────────────────────────────
     *  Called when the operator clicks "Send via Etsy".
     *  Input:
     *    { op:"enqueue", threadId, etsyConversationUrl, text,
     *      attachments:[...], employeeName, aiMeta:{...}?,
     *      force?:bool }     // v0.9.1 #6: needed to overwrite a draft
     *                        //          queued by a different operator
     *  Output: { draftId, status:"queued" } */
    if (op === "enqueue") {
      // v0.9.1 #8: kill-switch — global send disable
      const ks = await getKillSwitch();
      if (ks.disabled) {
        return json(503, {
          error      : "Send pipeline disabled by operator",
          errorCode  : "SEND_DISABLED",
          killSwitch : ks
        });
      }
      const {
        threadId,
        etsyConversationUrl,
        text,
        attachments  = [],
        employeeName = null,
        aiMeta       = null,
        force        = false,      // v0.9.1 #6: explicit overwrite of another operator's queued draft
        // v3.2 — How this enqueue was initiated. Two values:
        //   "manual" → operator clicked "Send via Etsy" in the inbox UI.
        //              The thread badge should read "Manually Sent" so
        //              operators can distinguish their own clicks from
        //              the AI auto-pipeline's sends, even though both
        //              paths land at thread.status === "auto_replied"
        //              and use the same draft + extension delivery
        //              machinery downstream.
        //   "auto"   → auto-pipeline / sales-agent generated this. This
        //              is the existing behavior — render the "auto-sent"
        //              badge.
        // Defaults to "auto" only when this enqueue carries an aiMeta
        // payload that flags it as AI-generated; otherwise defaults to
        // "manual" since the only non-AI caller is the inbox UI's Send
        // button. Callers can also pass the value explicitly to override.
        sendOrigin   = null,
        // v1.5: optional atomic thread finalize. When the auto-pipeline
        // calls enqueue, it passes this patch so the draft enqueue AND
        // the thread status update happen in ONE Firestore transaction.
        // Without this, the two writes were sequential — if the second
        // failed, the draft would be queued (extension picks up + sends)
        // but the thread would still be at pending_human_review, which
        // is the wrong folder. Pre-v1.5 manual enqueue path doesn't
        // pass this and behavior is unchanged.
        parentThreadFinalizePatch = null,
        // v3.32 — Explicit manual-send override. The operator clicked
        // Send via Etsy; pending generated tracking images should warn/skip,
        // not block the send.
        allowSendWithoutPendingTracking = false
      } = body;

      if (!threadId || !/^etsy_conv_\d+$/.test(String(threadId))) {
        return bad("threadId must match etsy_conv_<digits>");
      }
      // The conversation URL must match a route the extension's content
      // script recognizes. Mirrors the patterns in content-sender.js's
      // extractConversationId() so a URL the extension can read is also
      // a URL we accept on enqueue. Catches:
      //   /your/conversations/<id>
      //   /conversations/<id>
      //   /your/messages/buyer/<id>
      //   /your/messages/thread/<id>
      //   /messages/<id>
      const URL_RE = /^https:\/\/(www\.)?etsy\.com\/(?:your\/conversations|conversations|your\/messages\/(?:buyer|thread)|messages)\/\d+/;
      if (!etsyConversationUrl || !URL_RE.test(etsyConversationUrl)) {
        return bad("etsyConversationUrl must be an Etsy conversation URL");
      }
      const cleanText = String(text || "").trim();

      // v3.33 — Resolve origin BEFORE tracking reconciliation.
      // The inbox sends sendOrigin:"manual" + allowSendWithoutPendingTracking:true,
      // but the backend must also be safe if an older/alternate manual caller
      // omits those fields. The same convention used later for persisted
      // sendOrigin applies here: AI-generated aiMeta => auto, otherwise manual.
      const inferredSendOriginForRecon = sendOrigin
        || (aiMeta && aiMeta.generatedByAI ? "auto" : "manual");

      // v3.30 — Tracking-image attachment safety net. Reconciles the
      // send body's attachments with draft.trackingImages so a manual
      // Send always carries any ready (or about-to-be-ready) tracking
      // image, even when the inbox UI's chip-promotion poller hasn't
      // fired yet. See reconcileTrackingAttachments for full rationale.
      const draftIdForRecon = "draft_" + threadId;
      const recon = await reconcileTrackingAttachments(draftIdForRecon, attachments, {
        blockOnPending: !(allowSendWithoutPendingTracking === true || inferredSendOriginForRecon === "manual")
      });
      if (!recon.ok) {
        return json(409, {
          error    : recon.error,
          errorCode: recon.errorCode,
          pending  : recon.pending || []
        });
      }
      const normalized = normalizeAttachments(recon.merged);

      if (!cleanText && !normalized.length) {
        return bad("Draft must have text or at least one attachment");
      }

      // Deterministic draftId per thread: prevents stacked queued drafts
      // for the same thread, and makes peek a single doc-get not a query.
      const draftId = "draft_" + threadId;
      const ref     = db.collection(DRAFTS_COLL).doc(draftId);

      // Transaction: enqueuing overwrites any prior queued/sending state
      // with clear audit trail. If currently sending, operator must wait
      // — return 409 so the UI can show a graceful message.
      const result = await db.runTransaction(async (tx) => {
        // v1.5: if the auto-pipeline asked for an atomic thread finalize,
        // we need to read the thread doc INSIDE this transaction to
        // satisfy Firestore's read-before-write rule for new doc paths.
        // Reading a non-existent doc is fine — the set() below merges.
        if (parentThreadFinalizePatch && parentThreadFinalizePatch.threadId) {
          const tRef = db.collection(THREADS_COLL_NAME).doc(parentThreadFinalizePatch.threadId);
          await tx.get(tRef);    // ensures the txn knows about this read path
        }

        const snap = await tx.get(ref);
        const prev = snap.exists ? snap.data() : null;
        if (prev && (prev.status === "sending")) {
          return { conflict: true, prevStatus: prev.status };
        }

        // v0.9.1 #6: block second-operator queue overwrites
        // If another operator already queued this draft, refuse silently
        // unless the current operator passed force:true. The inbox catches
        // the 409 and shows a confirm dialog.
        if (prev && prev.status === "queued" && !force) {
          const prevOperator = prev.createdBy || null;
          const thisOperator = employeeName    || null;
          if (prevOperator && thisOperator && prevOperator !== thisOperator) {
            return {
              ownerConflict : true,
              prevOperator,
              thisOperator,
              prevQueuedAt  : prev.queuedAt ? prev.queuedAt.toMillis() : null
            };
          }
        }

        // v3.2 — Resolve sendOrigin if the caller didn't explicitly pass
        // one. The convention is:
        //   - Explicit value wins.
        //   - Otherwise, presence of an AI-generated aiMeta marks this
        //     as auto-pipeline; absence marks it as a manual operator
        //     send (the inbox UI is the only other caller and it doesn't
        //     send aiMeta).
        // The resolved value persists on the draft AND propagates onto
        // the thread when the send confirms — see the auto_replied
        // branch below for the thread-side write.
        const resolvedSendOrigin = inferredSendOriginForRecon;

        const payload = {
          draftId,
          threadId,
          etsyConversationUrl,
          text               : cleanText,
          attachments        : normalized,
          status             : "queued",
          createdBy          : employeeName || (prev && prev.createdBy) || null,
          // v3.2 — origin tag (manual vs auto). Stored on the draft so
          // the send-completion handler can copy it onto the thread.
          sendOrigin         : resolvedSendOrigin,
          // Preserve AI metadata if this was originally an AI draft
          generatedByAI      : (aiMeta && aiMeta.generatedByAI) != null
            ? !!aiMeta.generatedByAI
            : (prev && prev.generatedByAI) || false,
          aiModel            : (aiMeta && aiMeta.model)            || (prev && prev.aiModel) || null,
          aiReasoning        : (aiMeta && aiMeta.reasoning)        || (prev && prev.aiReasoning) || null,
          aiActiveQuestion   : (aiMeta && aiMeta.activeQuestion)   || (prev && prev.aiActiveQuestion) || null,
          // Lifecycle
          queuedAt           : FV.serverTimestamp(),
          updatedAt          : FV.serverTimestamp(),
          // Reset send-coordination state
          sendSessionId      : null,
          sendClaimedAt      : null,
          sendHeartbeatAt    : null,
          sendAttempts       : 0,
          sendError          : null,
          sendErrorCode      : null,
          sendPartialSuccess : false,
          sendStage          : "pre_click",  // v0.9.1 #2/#3: send-boundary state
          sentAt             : null
        };
        if (!snap.exists) payload.createdAt = FV.serverTimestamp();
        tx.set(ref, payload, { merge: true });

        // v1.5: atomic thread finalize. Same shape the auto-pipeline's
        // local finalizeThread used to write, but in the same txn as
        // the draft. Caller passes primitive fields (JSON-safe);
        // we reconstruct the Timestamp + serverTimestamp here.
        let threadFinalizeApplied = false;
        if (parentThreadFinalizePatch && parentThreadFinalizePatch.threadId) {
          const p = parentThreadFinalizePatch;
          const threadPatch = {
            status                       : p.newStatus,
            lastAutoDecision             : p.decision,
            lastAutoDecisionAt           : FV.serverTimestamp(),
            aiConfidence                 : p.aiConfidence != null ? p.aiConfidence : null,
            aiDifficulty                 : p.aiDifficulty != null ? p.aiDifficulty : null,
            aiDraftStatus                : "ready",
            latestDraftId                : draftId,
            updatedAt                    : FV.serverTimestamp()
          };
          if (typeof p.inboundMs === "number" && p.inboundMs > 0) {
            threadPatch.lastAutoProcessedInboundAt =
              admin.firestore.Timestamp.fromMillis(p.inboundMs);
          }
          tx.set(
            db.collection(THREADS_COLL_NAME).doc(p.threadId),
            threadPatch,
            { merge: true }
          );
          threadFinalizeApplied = true;
        }

        return { conflict: false, payload, threadFinalizeApplied };
      });

      if (result.conflict) {
        return json(409, {
          error      : `Draft is currently ${result.prevStatus}; wait for it to finish or fail`,
          errorCode  : "DRAFT_BUSY",
          draftId,
          prevStatus : result.prevStatus
        });
      }
      if (result.ownerConflict) {
        return json(409, {
          error         : `Draft is queued by ${result.prevOperator}. Send 'force:true' to overwrite.`,
          errorCode     : "QUEUE_OWNER_CONFLICT",
          draftId,
          prevOperator  : result.prevOperator,
          thisOperator  : result.thisOperator,
          prevQueuedAt  : result.prevQueuedAt
        });
      }

      await audit(threadId, draftId, "draft_enqueued", employeeName || "operator", {
        textLength   : cleanText.length,
        attachmentCount: normalized.length,
        attachmentTypes: normalized.map(a => a.type),
        skippedPendingTracking: recon.skippedPendingTracking || []
      });

      // v0.9.7: write the optimistic outbound message into the thread's
      // messages subcollection NOW — at enqueue time, before the extension
      // even claims the draft. This guarantees that the moment a send is
      // queued (whether by the operator clicking "Send via Etsy", the AI-
      // Draft confidence-based auto-send, or the sales-agent backend
      // auto-pipeline), the outbound message becomes visible in the
      // thread view for any operator viewing it — and stays visible across
      // refreshes. The renderer dedupes by text match, so when the next
      // M2 scrape pulls in the real Etsy outbound message, the optimistic
      // ghost is hidden automatically.
      //
      // Fire-and-forget: if this write fails (Firestore hiccup, etc.) the
      // draft is still enqueued correctly and the dashboard's frontend
      // backstop in showSendStatus will retry the insert when the operator
      // opens the thread. Don't let an optimistic-insert failure poison
      // the enqueue response.
      try {
        const optimDocId = "optim_" + draftId;
        const optimDoc = buildOptimisticDoc({
          draftId,
          text        : cleanText,
          employeeName: employeeName || (result.payload && result.payload.createdBy) || "AI",
          attachments : normalized
        });
        await db.collection(THREADS_COLL_NAME).doc(threadId)
          .collection("messages").doc(optimDocId)
          .set(optimDoc, { merge: false });
      } catch (e) {
        console.warn("optimistic insert at enqueue failed (non-fatal):", e.message);
      }

      return ok({
        draftId,
        status      : "queued",
        threadId,
        attachmentCount: normalized.length,
        // Return the authoritative server-side attachment list. The frontend
        // uses this for its optimistic message so the UI reflects backend
        // reconciliation, not stale composer-chip state.
        attachments : normalized,
        skippedPendingTracking: recon.skippedPendingTracking || [],
        pollUrl     : `/.netlify/functions/etsyMailDraftSend?op=status&draftId=${encodeURIComponent(draftId)}`
      });
    }

    /* ── cancel (inbox → server) ──────────────────────────────────
     *  Operator clicks "Cancel send" before the extension claims it.
     *  Only allowed while status === "queued" — if already sending, too late.
     *
     *  v1.5: also demote the parent thread if it was queued_for_auto_send.
     *  Without this, cancelling an auto-pipeline-enqueued send left the
     *  thread orphaned in Auto-Reply with the "sending…" pill while the
     *  draft itself was back at "draft". */
    if (op === "cancel") {
      const { draftId } = body;
      if (!draftId) return bad("Missing draftId");
      const ref = db.collection(DRAFTS_COLL).doc(String(draftId));
      const result = await db.runTransaction(async (tx) => {
        // v3.15: hoist all reads before any writes (Firestore rule).
        const snap = await tx.get(ref);
        if (!snap.exists) return { notFound: true };
        const prev = snap.data();
        if (prev.status !== "queued") return { badState: prev.status };

        // Pre-read the thread doc if we may need to demote it.
        let tRef = null;
        let threadCurrentStatus = null;
        if (prev.threadId) {
          tRef = db.collection(THREADS_COLL_NAME).doc(prev.threadId);
          const tSnap = await tx.get(tRef);
          if (tSnap.exists) threadCurrentStatus = tSnap.data().status || null;
        }

        // Now all writes:
        tx.set(ref, {
          status    : "draft",
          queuedAt  : null,
          updatedAt : FV.serverTimestamp()
        }, { merge: true });

        // v1.5: demote the parent thread if it was queued for auto-send.
        // No-op for manual operator sends (they're not in
        // queued_for_auto_send).
        let threadStatusUpdate = null;
        if (tRef && threadCurrentStatus === "queued_for_auto_send") {
          tx.set(tRef, {
            status            : "pending_human_review",
            lastAutoDecision  : "human_review_after_send_cancelled",
            lastAutoDecisionAt: FV.serverTimestamp(),
            updatedAt         : FV.serverTimestamp()
          }, { merge: true });
          threadStatusUpdate = "pending_human_review";
        }
        return { ok: true, threadId: prev.threadId, threadStatusUpdate };
      });
      if (result.notFound) return json(404, { error: "Draft not found" });
      if (result.badState) return json(409, { error: `Cannot cancel — draft is ${result.badState}` });
      await audit(result.threadId, draftId, "draft_cancelled", "operator", {
        threadStatusUpdate: result.threadStatusUpdate
      });
      return ok({ draftId, status: "draft", threadStatus: result.threadStatusUpdate });
    }

    /* ── peek (extension → server) ────────────────────────────────
     *  Extension content script asks: "is there a queued draft for
     *  the thread this Etsy tab is on?" Read-only, no claim.
     *  Input: { op:"peek", threadId }
     *  Output: { queued: true, draft: {...} } | { queued: false } */
    if (op === "peek") {
      const { threadId } = body;
      if (!threadId) return bad("Missing threadId");

      // v0.9.1 #8: kill-switch — peek returns no work if disabled
      const ks = await getKillSwitch();
      if (ks.disabled) {
        return ok({ queued: false, killSwitch: ks });
      }

      // Deterministic draft id keeps this a doc-get, not a query.
      const draftId = "draft_" + threadId;
      const snap = await db.collection(DRAFTS_COLL).doc(draftId).get();
      if (!snap.exists) return ok({ queued: false });
      const d = snap.data();

      if (d.status === "queued") {
        // Stale queued draft — operator clicked Send hours ago and
        // forgot. Don't surface it to the extension; instead expire
        // it so the inbox UI sees `failed` on next status poll.
        if (isStaleQueued(d.queuedAt)) {
          try {
            await db.collection(DRAFTS_COLL).doc(draftId).set({
              status        : "failed",
              sendError     : `Expired — queued more than ${MAX_CLAIM_LOOKBACK_MIN} minutes`,
              sendErrorCode : "QUEUED_EXPIRED",
              updatedAt     : FV.serverTimestamp()
            }, { merge: true });
            // v1.4: also demote the parent thread if it was queued
            // waiting for this send. Without this, the thread stays
            // in Auto-Reply showing "sending…" forever even though
            // the draft was just expired.
            const threadStatusUpdate = await demoteThreadStandalone(
              d.threadId, "human_review_after_queued_expired"
            ).catch(e => { console.warn("demote on peek-expire failed:", e.message); return null; });
            await audit(d.threadId, draftId, "draft_queue_expired", "peek", {
              ageMin: Math.round((Date.now() - d.queuedAt.toMillis()) / 60000),
              threadStatusUpdate
            });
          } catch (e) { console.warn("expire stale queued failed:", e.message); }
          return ok({ queued: false, currentStatus: "failed" });
        }
        return ok({ queued: true, draft: serializeDoc(snap) });
      }

      if (d.status !== "queued") {
        // v0.9.1 #2/#3 + v2.6: stale-sending drafts are NEVER auto-reclaimable
        // if sendStage === "post_click" — clicking Send a second time
        // would cause a duplicate message. The cleanup cron/reaper handles
        // this by marking sent_unverified (STRANDED_POST_CLICK), which
        // requires manual operator verification on Etsy.
        if (d.status === "sending" && isStaleHeartbeat(d.sendHeartbeatAt)) {
          if (d.sendStage === "post_click") {
            // Don't surface; cleanup cron/reaper will mark sent_unverified for manual review.
            return ok({ queued: false, currentStatus: "sending", stranded: true, postClick: true });
          }
          return ok({ queued: true, stale: true, draft: serializeDoc(snap) });
        }
        return ok({ queued: false, currentStatus: d.status });
      }
    }

    /* ── claim (extension → server) ───────────────────────────────
     *  Atomic. Only one extension wins a given queued draft.
     *  Input: { op:"claim", draftId, sessionId, workerId }
     *  Output: { draft, prevStatus } */
    if (op === "claim") {
      const { draftId, sessionId, workerId } = body;
      if (!draftId || !sessionId) return bad("Missing draftId or sessionId");

      // v0.9.1 #8: kill-switch — refuse to claim if disabled
      const ks = await getKillSwitch();
      if (ks.disabled) {
        return json(503, { error: "Send pipeline disabled", errorCode: "SEND_DISABLED", killSwitch: ks });
      }

      const ref = db.collection(DRAFTS_COLL).doc(String(draftId));
      const result = await db.runTransaction(async (tx) => {
        // ━━━ v3.15: Hoist all reads BEFORE any writes (Firestore rule) ━
        const snap = await tx.get(ref);
        if (!snap.exists) return { notFound: true };
        const prev = snap.data();

        // Pre-fetch thread snapshot — we may need to demote it in any
        // of the failure branches below. Doing the read up front keeps
        // every branch's writes legal.
        const threadPrefetch = await prefetchThreadForDemoteInTxn(tx, prev.threadId);

        // ─── PHASE: Decide + Write ──
        // Reject stale queued — paired with peek's expiration logic
        // so a slow extension claim can't beat the expiration sweep.
        if (prev.status === "queued" && isStaleQueued(prev.queuedAt)) {
          tx.set(ref, {
            status        : "failed",
            sendError     : `Expired — queued more than ${MAX_CLAIM_LOOKBACK_MIN} minutes`,
            sendErrorCode : "QUEUED_EXPIRED",
            updatedAt     : FV.serverTimestamp()
          }, { merge: true });
          // v1.4: also demote the thread (in the same txn for atomicity)
          const threadStatusUpdate = demoteThreadWriteOnlyInTxn(
            tx, threadPrefetch, "human_review_after_queued_expired"
          );
          return { expired: true, threadStatusUpdate };
        }

        // v0.9.1 #2/#3 + v2.6 fix: never re-claim a stranded post-click
        // draft. The Send button was clicked; the message almost certainly
        // went through (Etsy's Send is reliable). Re-clicking would risk
        // a duplicate. Mark as sent_unverified (NOT failed): operator
        // verifies on Etsy whether it went through, optimistic-insert
        // fires in the UI so the just-sent text appears in the thread.
        const staleSending = prev.status === "sending" && isStaleHeartbeat(prev.sendHeartbeatAt);
        if (staleSending && prev.sendStage === "post_click") {
          tx.set(ref, {
            status        : "sent_unverified",
            sentAt        : FV.serverTimestamp(),
            sendError     : "Tab died after clicking Etsy's Send button. The message most likely went through — verify on Etsy if you're uncertain. Do NOT re-send blindly: that would duplicate.",
            sendErrorCode : "STRANDED_POST_CLICK",
            updatedAt     : FV.serverTimestamp()
          }, { merge: true });
          // Demote so operator can verify. Same destination as
          // unverified manual sends — Needs Review.
          const threadStatusUpdate = demoteThreadWriteOnlyInTxn(
            tx, threadPrefetch, "human_review_after_stranded_post_click"
          );
          return { strandedPostClick: true, threadStatusUpdate };
        }

        // Accept queued, or sending-but-stale-pre-click (extension died before Send)
        if (prev.status !== "queued" && !staleSending) {
          return { taken: true, currentStatus: prev.status };
        }

        // Retry guard
        const nextAttempts = (prev.sendAttempts || 0) + 1;
        if (nextAttempts > MAX_SEND_ATTEMPTS) {
          tx.set(ref, {
            status       : "failed",
            sendError    : `Exceeded ${MAX_SEND_ATTEMPTS} attempts`,
            sendAttempts : nextAttempts,
            updatedAt    : FV.serverTimestamp()
          }, { merge: true });
          // v1.5: also demote the parent thread. The retry budget is
          // spent; this draft will not be sent. The thread must leave
          // queued_for_auto_send so the operator sees it in Needs
          // Review and can decide what to do.
          const threadStatusUpdate = demoteThreadWriteOnlyInTxn(
            tx, threadPrefetch, "human_review_after_retry_exhausted"
          );
          return { exhausted: true, attempts: nextAttempts, threadStatusUpdate };
        }

        tx.set(ref, {
          status         : "sending",
          sendSessionId  : String(sessionId),
          sendWorkerId   : workerId || null,
          sendClaimedAt  : FV.serverTimestamp(),
          sendHeartbeatAt: FV.serverTimestamp(),
          sendAttempts   : nextAttempts,
          sendError      : null,
          sendErrorCode  : null,
          sendStage      : "pre_click",   // v0.9.1 #2/#3: reset on every claim
          updatedAt      : FV.serverTimestamp()
        }, { merge: true });
        return { ok: true, data: prev, attempts: nextAttempts };
      });

      if (result.notFound) return json(404, { error: "Draft not found" });
      if (result.expired)  return json(410, { error: `Draft expired — was queued > ${MAX_CLAIM_LOOKBACK_MIN} min`, errorCode: "QUEUED_EXPIRED" });
      if (result.strandedPostClick) {
        return json(410, {
          error     : "Previous attempt clicked Send and went silent. Manual review required on Etsy.",
          errorCode : "STRANDED_POST_CLICK"
        });
      }
      if (result.taken)    return json(409, { error: `Draft already ${result.currentStatus}`, currentStatus: result.currentStatus });
      if (result.exhausted) return json(410, { error: "Draft exhausted retry budget", attempts: result.attempts });

      // Return the full draft payload the extension needs to execute the send.
      const freshSnap = await ref.get();
      await audit(result.data.threadId, draftId, "draft_claimed", workerId || "extension", {
        sessionId, attempt: result.attempts
      });
      return ok({ draft: serializeDoc(freshSnap), attempts: result.attempts });
    }

    /* ── heartbeat (extension → server) ───────────────────────────
     *  Refresh sendHeartbeatAt. Extension calls this every 5s while
     *  actively scripting Etsy's compose. Input must include sessionId
     *  so stolen claims are rejected. */
    if (op === "heartbeat") {
      const { draftId, sessionId, progress } = body;
      if (!draftId || !sessionId) return bad("Missing draftId or sessionId");

      const ref = db.collection(DRAFTS_COLL).doc(String(draftId));
      const result = await db.runTransaction(async (tx) => {
        const snap = await tx.get(ref);
        if (!snap.exists) return { notFound: true };
        const prev = snap.data();
        if (prev.status !== "sending") return { badState: prev.status };
        if (prev.sendSessionId !== sessionId) return { notYours: true, owner: prev.sendSessionId };

        const patch = {
          sendHeartbeatAt: FV.serverTimestamp(),
          updatedAt      : FV.serverTimestamp()
        };
        if (progress && typeof progress === "object") {
          patch.sendProgress = {
            phase       : progress.phase || null,
            stepLabel   : progress.stepLabel || null,
            attachmentsUploaded: Number(progress.attachmentsUploaded) || 0,
            attachmentsTotal   : Number(progress.attachmentsTotal)    || 0,
            ts          : Date.now()
          };
        }
        tx.set(ref, patch, { merge: true });
        return { ok: true };
      });
      if (result.notFound) return json(404, { error: "Draft not found" });
      if (result.badState) return json(409, { error: `Draft is ${result.badState}` });
      if (result.notYours) return json(403, { error: "Heartbeat from wrong session", owner: result.owner });
      return ok({ heartbeat: Date.now() });
    }

    /* ── mark_clicked (extension → server) ─────────────────────────
     *  v0.9.1 #2/#3: extension calls this immediately BEFORE clicking
     *  Etsy's Send button. The atomic flip from sendStage="pre_click"
     *  to "post_click" tells the cleanup cron and the claim path that
     *  any future stranding must NOT auto-requeue — clicking again
     *  would risk a duplicate send.
     *
     *  This must be a synchronous round-trip BEFORE the click. If the
     *  call fails, the extension MUST NOT click Send (treats as a
     *  retryable failure with errorCode=MARK_CLICKED_FAILED).
     *
     *  Input: { op:"mark_clicked", draftId, sessionId } */
    if (op === "mark_clicked") {
      const { draftId, sessionId } = body;
      if (!draftId || !sessionId) return bad("Missing draftId or sessionId");

      const ref = db.collection(DRAFTS_COLL).doc(String(draftId));
      const result = await db.runTransaction(async (tx) => {
        const snap = await tx.get(ref);
        if (!snap.exists) return { notFound: true };
        const prev = snap.data();
        if (prev.status !== "sending") return { badState: prev.status };
        if (prev.sendSessionId !== sessionId) return { notYours: true };
        tx.set(ref, {
          sendStage      : "post_click",
          sendHeartbeatAt: FV.serverTimestamp(),
          updatedAt      : FV.serverTimestamp()
        }, { merge: true });
        return { ok: true, threadId: prev.threadId };
      });
      if (result.notFound) return json(404, { error: "Draft not found" });
      if (result.badState) return json(409, { error: `Draft is ${result.badState}` });
      if (result.notYours) return json(403, { error: "mark_clicked from wrong session" });
      await audit(result.threadId, draftId, "draft_mark_clicked", sessionId, {});
      return ok({ stage: "post_click", at: Date.now() });
    }

    /* ── complete (extension → server) ────────────────────────────
     *  Extension confirms the send succeeded.
     *  Input: { draftId, sessionId, partial?, sentText?, unverified?, imagesSent? } */
    if (op === "complete") {
      const {
        draftId, sessionId,
        partial = false, sentText = true,
        unverified = false,    // v0.9.1 #4: 12s timeout with no positive signal
        imagesSent = 0, imagesTotal = 0,
        listingsSent = 0, listingsTotal = 0,
        etsyMessageId = null,
        note = null
      } = body;
      if (!draftId || !sessionId) return bad("Missing draftId or sessionId");

      const ref = db.collection(DRAFTS_COLL).doc(String(draftId));
      const result = await db.runTransaction(async (tx) => {
        // ━━━ v3.15 FIX: Firestore requires ALL reads BEFORE any writes ━━
        //
        // The previous implementation interleaved reads and writes:
        //   tx.get(draft) → tx.set(draft) → tx.get(thread) → tx.set(thread)
        // which raised:
        //   "Firestore transactions require all reads to be executed
        //    before all writes."
        //
        // Symptom: the watchdog's "complete" call returned HTTP 500 for
        // every multi-doc send. Single-doc sends (no thread promotion)
        // happened to slip through because they only touched one ref.
        //
        // Fix: read BOTH the draft doc AND the thread doc upfront, make
        // all branching decisions from those snapshots, then issue all
        // writes at the end. Functionally identical — same final state,
        // same audit semantics — just reordered to satisfy the rule.

        // ── PHASE 1: ALL READS ──────────────────────────────────────
        const snap = await tx.get(ref);
        if (!snap.exists) return { notFound: true };
        const prev = snap.data();
        if (prev.sendSessionId !== sessionId) return { notYours: true };

        // Read the thread doc up front too, IF this draft has a thread.
        // We may need its current status to decide promotion vs demotion.
        let tRef = null;
        let tSnap = null;
        let threadCurrentStatus = null;
        if (prev.threadId) {
          tRef = db.collection(THREADS_COLL_NAME).doc(prev.threadId);
          tSnap = await tx.get(tRef);
          if (tSnap.exists) {
            threadCurrentStatus = tSnap.data().status || null;
          }
        }

        // ── PHASE 2: DECIDE (no I/O, just logic) ────────────────────
        // v0.9.1 #4: terminal status reflects what we actually know.
        //   - partial=true   → sent_text_only (text confirmed, images failed)
        //   - unverified=true → sent_unverified (clicked Send, no positive
        //                       signal within 12s; needs operator verification)
        //   - else            → sent (toast or composer-cleared confirmation)
        let finalStatus = "sent";
        if (partial)         finalStatus = "sent_text_only";
        else if (unverified) finalStatus = "sent_unverified";

        // Decide thread status update based on the snapshot we already read.
        // Mirrors the original logic but uses pre-read data.
        let threadStatusUpdate = null;
        let threadDemoteReason = null;
        if (prev.threadId && tSnap && tSnap.exists) {
          if (partial || unverified) {
            // Demote — partial/unverified send needs operator review.
            // Mirror demoteThreadInTxn's behaviour but inline (no extra read).
            if (threadCurrentStatus === "queued_for_auto_send") {
              threadDemoteReason = unverified
                ? "human_review_after_unverified_send"
                : "human_review_after_partial_send";
              threadStatusUpdate = "pending_human_review";
            }
          } else {
            // Promote — clean send.
            if (threadCurrentStatus === "queued_for_auto_send") {
              threadStatusUpdate = "auto_replied";
            }
          }
        }

        // ── PHASE 3: ALL WRITES ─────────────────────────────────────
        // Write the draft completion record first.
        tx.set(ref, {
          status             : finalStatus,
          sentAt             : FV.serverTimestamp(),
          sendPartialSuccess : !!partial,
          sendUnverified     : !!unverified,
          sendImagesSent     : Number(imagesSent)     || 0,
          sendImagesTotal    : Number(imagesTotal)    || 0,
          sendListingsSent   : Number(listingsSent)   || 0,
          sendListingsTotal  : Number(listingsTotal)  || 0,
          sendTextSent       : !!sentText,
          sendNote           : note || null,
          etsyMessageId      : etsyMessageId || null,
          sendHeartbeatAt    : FV.serverTimestamp(),
          updatedAt          : FV.serverTimestamp()
        }, { merge: true });

        // Then the thread status update, if we decided one is needed.
        // v5.30 — Also writes lastOperatorReplyAt: the inbox uses
        // MAX(lastInboundAt, lastOperatorReplyAt) to sort threads, so
        // a successful operator send must bump this field for the
        // thread to rise to the top of the list. Pre-v5.30 only
        // updatedAt was written, which was masked by lastInboundAt in
        // the inbox's OR-chain — the operator's reply stayed hidden
        // below older inbound messages until the next customer reply.
        //
        // v5.21 — Scan the sent text for refund signals BEFORE composing
        // the thread patch, so refundFlaggedAt can be folded into the
        // single tx.set() write below (no extra read, no extra write).
        // Catches operator-typed manual replies about refunds that
        // bypass the AI-draft path. AI-drafted refund replies are
        // already flagged in etsyMailDraftReply.js.
        const _refundHitOutbound = _detectRefundSignals(prev.text || "");
        const _refundFields = _refundHitOutbound ? {
          refundFlaggedAt    : FV.serverTimestamp(),
          refundFlaggedReason: `outbound_send:"${_refundHitOutbound.slice(0, 60)}"`
        } : {};
        if (_refundHitOutbound) {
          console.log(
            `[draftSend ${prev.threadId || draftId}] Refund signal in outbound text ("${_refundHitOutbound}") — ` +
            `tagging thread refundFlaggedAt for Refunds folder.`
          );
        }

        if (threadStatusUpdate === "pending_human_review") {
          tx.set(tRef, {
            status              : "pending_human_review",
            lastAutoDecision    : threadDemoteReason,
            lastAutoDecisionAt  : FV.serverTimestamp(),
            lastOperatorReplyAt : FV.serverTimestamp(),
            // v3.32 — Wipe the quiet-period defer fields on every
            // successful send. lastAutoDecision is already being
            // overwritten above (so the reaper's deferred-pipeline
            // query passes over this thread), but autoPipelineDefer-
            // UntilMs would otherwise linger as orphan data. Cleared
            // here so the inbox timer-banner self-correction has a
            // consistent state to read.
            autoPipelineDeferUntilMs : FV.delete(),
            updatedAt           : FV.serverTimestamp(),
            ..._refundFields
          }, { merge: true });
        } else if (threadStatusUpdate === "auto_replied") {
          tx.set(tRef, {
            status                    : "auto_replied",
            lastAutoDecision          : "auto_send_confirmed",
            lastAutoDecisionAt        : FV.serverTimestamp(),
            lastOperatorReplyAt       : FV.serverTimestamp(),
            // v3.2 — Surface the send origin on the thread so the inbox
            // can render "Manually Sent" vs "auto-sent" without having
            // to fetch the draft. prev.sendOrigin was set at enqueue
            // time. Defaults to "auto" for old drafts that predate this
            // field — preserves prior behavior for in-flight legacy data.
            sendOrigin                : prev.sendOrigin || "auto",
            // v1.4: this is a real AI auto-reply — clear any stale
            // "manually moved" flag from a prior move.
            manuallyMovedToAutoReplied: FV.delete(),
            manualMoveActor           : FV.delete(),
            manualMoveAt              : FV.delete(),
            manualMoveReason          : FV.delete(),
            manualMoveFromStatus      : FV.delete(),
            // v3.32 — same defer-field cleanup as the demotion branch.
            autoPipelineDeferUntilMs  : FV.delete(),
            updatedAt                 : FV.serverTimestamp(),
            ..._refundFields
          }, { merge: true });
        } else if (prev.threadId && tSnap && tSnap.exists) {
          // v5.30 — No status transition needed (operator manual send
          // on a thread that stays in pending_human_review, or a
          // status-persistent flow), but the thread still has to be
          // bumped to the top of the list because the operator just
          // replied. Write JUST the bump fields — leave status,
          // sendOrigin, lastAutoDecision et al. untouched.
          //
          // v3.32 — EXCEPTION: if this thread was in deferred_quiet_-
          // period when the operator sent, the manual send is the
          // final word for this conversation turn and the defer state
          // is now obsolete. Clear lastAutoDecision (so the reaper's
          // deferred-pipeline query passes over this thread) and
          // autoPipelineDeferUntilMs (so the inbox timer hides). Other
          // lastAutoDecision values are preserved as-is — e.g. a
          // thread parked in "skipped_no_inbound" should stay that
          // way after a manual operator bump.
          const prevAutoDecision = (tSnap.data() && tSnap.data().lastAutoDecision) || null;
          const wasDeferred = prevAutoDecision === "deferred_quiet_period";
          const _deferClearFields = wasDeferred ? {
            lastAutoDecision         : "manual_send_during_defer",
            lastAutoDecisionAt       : FV.serverTimestamp(),
            autoPipelineDeferUntilMs : FV.delete()
          } : {};
          tx.set(tRef, {
            lastOperatorReplyAt: FV.serverTimestamp(),
            updatedAt          : FV.serverTimestamp(),
            ..._deferClearFields,
            ..._refundFields
          }, { merge: true });
        }

        return { ok: true, threadId: prev.threadId, status: finalStatus, threadStatusUpdate };
      });
      if (result.notFound) return json(404, { error: "Draft not found" });
      if (result.notYours) return json(403, { error: "Complete from wrong session" });
      await audit(result.threadId, draftId, "draft_sent", sessionId, {
        partial, unverified, imagesSent, imagesTotal, listingsSent, listingsTotal, note,
        threadStatusUpdate: result.threadStatusUpdate
      });
      return ok({ draftId, status: result.status, threadStatus: result.threadStatusUpdate });
    }

    /* ── fail (extension → server) ────────────────────────────────
     *  Extension reports a failure. Supports requeue-if-retryable.
     *  Input: { draftId, sessionId, error, retry?:boolean, errorCode? } */
    if (op === "fail") {
      const {
        draftId, sessionId,
        error = "unknown error",
        errorCode = null,
        retry = false
      } = body;
      if (!draftId || !sessionId) return bad("Missing draftId or sessionId");

      const ref = db.collection(DRAFTS_COLL).doc(String(draftId));
      const result = await db.runTransaction(async (tx) => {
        const snap = await tx.get(ref);
        if (!snap.exists) return { notFound: true };
        const prev = snap.data();
        if (prev.sendSessionId !== sessionId) return { notYours: true };

        // v3.15: hoist all reads BEFORE any writes (Firestore rule).
        // Pre-fetch thread so the demote write at the end is legal.
        const threadPrefetch = await prefetchThreadForDemoteInTxn(tx, prev.threadId);

        const attempts = prev.sendAttempts || 0;
        const willRetry = retry && attempts < MAX_SEND_ATTEMPTS;

        const patch = {
          sendError      : String(error).slice(0, 1000),
          sendErrorCode  : errorCode || null,
          sendHeartbeatAt: FV.serverTimestamp(),
          updatedAt      : FV.serverTimestamp()
        };
        if (willRetry) {
          patch.status        = "queued";
          patch.sendSessionId = null;
          patch.sendClaimedAt = null;
          // Keep attempts; next claim increments.
        } else {
          patch.status        = "failed";
        }
        tx.set(ref, patch, { merge: true });

        // ── v1.2 / v1.4: Auto-Reply demotion on terminal failure ──
        // If the thread was queued_for_auto_send and this is a
        // terminal failure (no retry left), demote to Needs Review so
        // the operator gets eyes on it. If we're going to retry, leave
        // the thread state alone — the next claim attempts the send
        // again.
        let threadStatusUpdate = null;
        if (!willRetry) {
          threadStatusUpdate = demoteThreadWriteOnlyInTxn(
            tx, threadPrefetch, "human_review_after_send_failure"
          );
        }
        return { ok: true, threadId: prev.threadId, requeued: willRetry, attempts, threadStatusUpdate };
      });
      if (result.notFound) return json(404, { error: "Draft not found" });
      if (result.notYours) return json(403, { error: "Fail from wrong session" });
      await audit(result.threadId, draftId, result.requeued ? "draft_send_requeued" : "draft_send_failed", sessionId, {
        error, errorCode, attempts: result.attempts,
        threadStatusUpdate: result.threadStatusUpdate
      });
      return ok({
        draftId,
        status      : result.requeued ? "queued" : "failed",
        requeued    : result.requeued,
        attempts    : result.attempts,
        threadStatus: result.threadStatusUpdate
      });
    }

    /* ── kill_switch_set (ops → server) ──────────────────────────
     *  Toggle the global send-disabled flag. Authenticated like every
     *  other op. v0.9.1 #8.
     *
     *  Input: { op:"kill_switch_set", disabled:bool, reason?, by? }
     *  Output: { killSwitch: { disabled, reason, by, at } } */
    if (op === "kill_switch_set") {
      const { disabled, reason = null, by = null } = body;
      if (typeof disabled !== "boolean") return bad("disabled must be a boolean");
      const ref = db.collection(CONFIG_COLL).doc("global");
      await ref.set({
        sendDisabled       : !!disabled,
        sendDisabledReason : disabled ? (reason || "manually disabled") : null,
        sendDisabledBy     : disabled ? (by     || "operator")          : null,
        sendDisabledAt     : disabled ? FV.serverTimestamp()            : null,
        updatedAt          : FV.serverTimestamp()
      }, { merge: true });
      // Invalidate cache immediately
      _killSwitchCache = { value: null, fetchedAt: 0 };
      await audit(null, null, disabled ? "kill_switch_enabled" : "kill_switch_disabled", by || "operator", { reason });
      return ok({ killSwitch: { disabled, reason, by, at: Date.now() } });
    }

    return bad(`Unknown op '${op}'`);

  } catch (err) {
    console.error("etsyMailDraftSend error:", err);
    return json(500, { error: err.message || String(err) });
  }
};

// ─── v1.4 helpers exported for the send-queue reaper ───────────────
// Not part of the HTTP surface — the reaper imports these directly.
module.exports.demoteThreadInTxn          = demoteThreadInTxn;
module.exports.demoteThreadStandalone     = demoteThreadStandalone;
// v3.15: write-only variant for callers that need to demote AFTER
// other writes within the same transaction (Firestore disallows reads
// after writes, so the standard variant can't be used in those cases).
module.exports.prefetchThreadForDemoteInTxn = prefetchThreadForDemoteInTxn;
module.exports.demoteThreadWriteOnlyInTxn   = demoteThreadWriteOnlyInTxn;
module.exports.isStaleQueued              = isStaleQueued;
module.exports.isStaleHeartbeat           = isStaleHeartbeat;
module.exports.MAX_CLAIM_LOOKBACK_MIN     = MAX_CLAIM_LOOKBACK_MIN;
module.exports.STALE_HEARTBEAT_MS         = STALE_HEARTBEAT_MS;
