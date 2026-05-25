/*  netlify/functions/etsyMailAutoPipeline-background.js
 *
 *  v1.0 — Auto-reply pipeline orchestrator (BACKGROUND function).
 *
 *  ═══ WHY -BACKGROUND ════════════════════════════════════════════════════
 *
 *  Netlify's `-background` suffix unlocks a 15-minute timeout (vs 10s for
 *  regular synchronous functions) and decouples invocation from response.
 *  This pipeline calls etsyMailDraftReply (Sonnet 4.6 + tool loop, 10-60s
 *  typical) and may also call etsyMailDraftSend.enqueue, so the standard
 *  10s budget is too tight. The trade-off: callers get a 202 immediately
 *  with no body, so any return data we'd want must be persisted (we
 *  write everything to Firestore and the audit trail).
 *
 *  ═══ PURPOSE ════════════════════════════════════════════════════════════
 *
 *  When a new inbound message lands in a thread, this function:
 *
 *    1. Generates an AI draft via etsyMailDraftReply (mode="initial").
 *       The compose_draft_reply tool now self-rates `confidence` and
 *       `difficulty` on every draft.
 *
 *    2. Inspects the rating against the configured confidence threshold:
 *
 *       confidence ≥ threshold  →  AUTO_SEND
 *           - Enqueue the draft via etsyMailDraftSend (op="enqueue")
 *           - Set thread status = "auto_replied"
 *           - The Chrome extension picks up the queued draft on its
 *             next peek and sends it via Etsy's compose flow (same path
 *             as a manual "Send via Etsy" click).
 *
 *       confidence < threshold  →  HUMAN_REVIEW
 *           - Leave the draft in EtsyMail_Drafts/{draftId} (status=draft)
 *           - Set thread status = "pending_human_review"
 *           - Operator finds it in the Needs Review folder with the AI's
 *             rating + reasoning visible on open.
 *
 *  Idempotency: each thread tracks lastAutoProcessedInboundAt (millis).
 *  If lastInboundAt > lastAutoProcessedInboundAt, the pipeline runs once
 *  for that inbound. We do NOT auto-reply to outbound or scraper-only
 *  updates. We do NOT auto-reply if the kill-switch is on. We do NOT
 *  auto-reply if a draft for this thread is already in-flight (queued
 *  or sending).
 *
 *  ═══ INVOCATION ═════════════════════════════════════════════════════════
 *
 *  Two invocation paths:
 *
 *  (a) Background trigger — fire-and-forget POST from etsyMailSnapshot.js
 *      right after a new inbound message lands. Netlify returns 202 to
 *      the caller immediately and runs this function asynchronously up
 *      to 15 minutes.
 *
 *  (b) Direct (operator) — POST to this endpoint with { threadId } from
 *      the inbox to manually re-run the pipeline (backfills, overrides).
 *      Same 202-and-go semantics; check the thread doc for the result.
 *
 *  ═══ REQUEST ════════════════════════════════════════════════════════════
 *
 *  POST body:
 *    {
 *      threadId         : "etsy_conv_1651714855",   // required
 *      confidenceThreshold: 0.80,                   // optional override
 *      employeeName     : "system:auto-pipeline",   // optional signature
 *      forceRerun       : false,                    // bypass idempotency
 *      dryRun           : false                     // generate but don't act
 *    }
 *
 *  ═══ RESPONSE ═══════════════════════════════════════════════════════════
 *
 *  Netlify always returns 202 (Accepted) immediately for -background
 *  functions, regardless of what we put in the response. We still write
 *  a structured response body in case Netlify changes that semantics or
 *  for local testing where this can be invoked synchronously.
 *
 *  Real "result" lives in Firestore:
 *    - thread.status: "auto_replied" or "pending_human_review"
 *    - thread.lastAutoDecision: "auto_send" | "human_review" | ...
 *    - thread.aiConfidence / thread.aiDifficulty: the rating
 *    - draft doc updated with text + rating
 *    - audit doc with eventType "auto_pipeline_*"
 *
 *  ═══ ENV VARS ═══════════════════════════════════════════════════════════
 *
 *  ETSYMAIL_EXTENSION_SECRET       gates direct-invocation auth
 *  URL / DEPLOY_URL                Netlify-provided base for inter-fn calls
 *
 *  All operational config (enabled flag, confidence threshold) lives in
 *  Firestore at EtsyMail_Config/autoPipeline so operators can tune it
 *  from the inbox UI without redeploying. See getAutoPipelineConfig().
 */

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const THREADS_COLL = "EtsyMail_Threads";
const DRAFTS_COLL  = "EtsyMail_Drafts";
const AUDIT_COLL   = "EtsyMail_Audit";
const CONFIG_COLL  = "EtsyMail_Config";

// ─── Hard-coded fallback defaults ─────────────────────────────────────
// Used only when EtsyMail_Config/autoPipeline doesn't exist yet (first
// deploy). Once the doc is written, these are ignored — the doc is the
// single source of truth and the inbox UI is the only knob that matters.
const FALLBACK_THRESHOLD = 0.80;
const FALLBACK_ENABLED   = true;

// Don't auto-reply to messages older than this when re-processing.
// Protects against accidental backfills auto-sending to old threads.
const MAX_INBOUND_AGE_MS = 7 * 24 * 60 * 60 * 1000;   // 7 days

// ─── Auto-pipeline config — Firestore-backed, cached 15s ──────────────
// Same pattern as the kill-switch in etsyMailDraftSend.js. The cache
// lives in module scope so warm-container invocations share it; cold
// starts re-fetch (acceptable, ~1 round-trip).
let _autoCfgCache = { value: null, fetchedAt: 0 };
const AUTO_CFG_CACHE_MS = 15 * 1000;

async function getAutoPipelineConfig() {
  if (_autoCfgCache.value && (Date.now() - _autoCfgCache.fetchedAt < AUTO_CFG_CACHE_MS)) {
    return _autoCfgCache.value;
  }
  let value = {
    enabled                 : FALLBACK_ENABLED,
    threshold               : FALLBACK_THRESHOLD,
    // ─── v3.30 Quiet-period debounce ────────────────────────────
    // Minutes the most recent inbound must be "settled" before the
    // pipeline acts. When a customer fires 3-4 messages in 5 minutes,
    // we defer until they stop typing rather than burn an Anthropic
    // call per draft (most of which would be obsoleted by the next
    // message anyway). 0 disables; default 5.
    quietPeriodMinutes      : 5,
    // ─── v2.0 Step 1 flags (default OFF — operator must opt in) ──
    listingsMirrorEnabled   : false,
    intentClassifierEnabled : false,
    // ─── v2.0 Step 2 flags (default OFF — operator must opt in) ──
    salesModeEnabled        : false,
    salesAutoEngage         : false,
    salesAutoSendEnabled    : false,   // v2.6: auto-send sales drafts (off by default — opt in)
    salesPilotThreadIds     : [],
    // ─── v2.0 Step 3 will add (commented for now): ───────────────
    // customOrderSendEnabled       : false,
    // customOrderHighValueThreshold: 200,
    // customOrderRequireDoubleApproval: true,
    source                  : "fallback"
  };
  try {
    const doc = await db.collection(CONFIG_COLL).doc("autoPipeline").get();
    if (doc.exists) {
      const d = doc.data() || {};
      const t = typeof d.threshold === "number" ? d.threshold : FALLBACK_THRESHOLD;
      // v3.30: clamp quiet period to a sane band [0, 60] minutes.
      // Missing field falls back to the FALLBACK default (5 min) so a
      // config doc that pre-dates this field gets debounce by default —
      // matching the rollout intent.
      const rawQuiet = typeof d.quietPeriodMinutes === "number" ? d.quietPeriodMinutes : 5;
      const quietPeriodMinutes = Math.max(0, Math.min(60, rawQuiet));
      value = {
        enabled  : d.enabled !== false,             // default true if doc exists but unset
        threshold: Math.max(0, Math.min(1, t)),
        quietPeriodMinutes,
        // v2.0 Step 1: explicit-true semantics. Missing field => false.
        // We do NOT default to true even if the surrounding doc exists,
        // because flipping these on without operator review is unsafe.
        listingsMirrorEnabled   : d.listingsMirrorEnabled === true,
        intentClassifierEnabled : d.intentClassifierEnabled === true,
        // ── v2.0 Step 2 forward-compat reads (harmless if absent) ──
        // Read but do not act on these in Step 1; Step 2 will use them.
        salesModeEnabled        : d.salesModeEnabled === true,
        salesAutoEngage         : d.salesAutoEngage === true,
        salesAutoSendEnabled    : d.salesAutoSendEnabled === true,
        salesPilotThreadIds     : Array.isArray(d.salesPilotThreadIds) ? d.salesPilotThreadIds : [],
        // ── v2.0 Step 3 forward-compat reads ──
        customOrderSendEnabled  : d.customOrderSendEnabled === true,
        updatedBy: d.updatedBy || null,
        updatedAt: d.updatedAt && d.updatedAt.toMillis ? d.updatedAt.toMillis() : null,
        source   : "firestore"
      };
    }
    _autoCfgCache = { value, fetchedAt: Date.now() };
  } catch (e) {
    console.warn("autoPipeline: config fetch failed:", e.message);
  }
  return value;
}

// ━━━ v3.18: Tracking-image-readiness gate for auto-send ━━━━━━━━━━━━━━
//
// When the AI generates a draft, generate_tracking_image returns
// IMMEDIATELY with status="pending" and a jobId — the actual carrier
// lookup + image render happens in a background function and may take
// 5-30 seconds. The draft is finalized at this point, with the
// pending-status attachment recorded in draftResp.attachments[].
//
// PROBLEM (pre-v3.18): if the auto-pipeline confidence-routes the draft
// to auto_send and immediately enqueues it, the extension picks it up
// and tries to attach an image whose proxy URL is not yet populated —
// the customer receives the text WITHOUT the tracking timeline. This
// mirrored the manual-Send race we fixed in the inbox UI (v3.16/v3.17).
//
// FIX: before the auto-pipeline enqueues, poll the tracking job docs
// in Firestore until all pending tracking images are ready (or failed),
// then proceed. Cap the wait at WAIT_TRACKING_MAX_MS — past that, fall
// through to human review so the operator can verify and re-attach.
//
// Why poll Firestore (instead of the snapshot endpoint):
//   - The background function writes status updates to
//     EtsyMail_TrackingJobs/<jobId> directly. Polling the doc is a
//     single read per poll and doesn't multiply Netlify invocations.
//   - The same source of truth the inbox UI uses, so behavior is
//     consistent across manual and auto-send paths.

const TRACKING_JOBS_COLL    = "EtsyMail_TrackingJobs";
const WAIT_TRACKING_MAX_MS  = 30 * 1000;   // 30s ceiling
const WAIT_TRACKING_INTERVAL = 1000;       // 1s between polls

/**
 * Wait until every tracking_image attachment in `attachments` is in
 * a terminal state (ready or failed). Returns:
 *   {
 *     ok       : true  if every pending one became "ready"
 *                false if any timed out or finished as "failed"
 *     attachments: updated array with current statuses + imageUrl/etc
 *     timedOut : boolean — whether the deadline was hit
 *     failed   : array of trackingCodes that ended in "failed" state
 *   }
 */
async function waitForTrackingJobs(attachments) {
  const result = {
    ok: true,
    attachments: Array.isArray(attachments) ? attachments.map(a => ({ ...a })) : [],
    timedOut: false,
    failed: []
  };

  // Find tracking_image attachments still pending (no imageUrl yet).
  const pendingIdxs = [];
  result.attachments.forEach((a, idx) => {
    if (a && a.type === "tracking_image" && a.status !== "ready" && a.status !== "failed" && a.jobId) {
      pendingIdxs.push(idx);
    }
  });

  if (pendingIdxs.length === 0) return result;

  console.log(`[autoPipeline] waiting for ${pendingIdxs.length} tracking job(s) to complete`);

  const deadline = Date.now() + WAIT_TRACKING_MAX_MS;
  const stillPending = new Set(pendingIdxs);

  while (stillPending.size > 0 && Date.now() < deadline) {
    // Poll each remaining pending job
    for (const idx of Array.from(stillPending)) {
      const att = result.attachments[idx];
      try {
        const snap = await db.collection(TRACKING_JOBS_COLL).doc(att.jobId).get();
        if (!snap.exists) continue;   // odd, but keep waiting
        const job = snap.data();
        if (job.status === "ready") {
          // Hydrate the attachment with the now-known image fields so
          // etsyMailDraftSend.normalizeAttachments doesn't reject it.
          att.status           = "ready";
          att.imageUrl         = job.imageUrl || att.imageUrl || null;
          att.imageStoragePath = job.imageStoragePath || att.imageStoragePath || null;
          att.imageWidth       = job.imageWidth   || att.imageWidth || null;
          att.imageHeight      = job.imageHeight  || att.imageHeight || null;
          att.statusKey        = job.statusKey    || att.statusKey || null;
          att.statusText       = job.statusText   || att.statusText || null;
          att.carrier          = job.carrier      || att.carrier || null;
          att.carrierDisplay   = job.carrierDisplay || att.carrierDisplay || null;
          // Ensure proxyUrl is set (extension reads this to fetch bytes)
          if (!att.proxyUrl && att.trackingCode) {
            att.proxyUrl = "/.netlify/functions/etsyMailTrackingImage?trackingCode=" +
                           encodeURIComponent(att.trackingCode);
          }
          stillPending.delete(idx);
          console.log(`[autoPipeline] tracking ready: ${att.trackingCode}`);
        } else if (job.status === "failed") {
          att.status     = "failed";
          att.errorText  = job.error || "Tracking lookup failed";
          att.errorCode  = job.errorCode || null;
          stillPending.delete(idx);
          result.ok = false;
          result.failed.push(att.trackingCode || att.jobId);
          console.warn(`[autoPipeline] tracking failed: ${att.trackingCode} — ${att.errorText}`);
        }
        // else: still pending or running — keep polling
      } catch (e) {
        console.warn(`[autoPipeline] tracking poll error for ${att.jobId}: ${e.message}`);
        // Don't bail — try again on next tick
      }
    }
    if (stillPending.size > 0) {
      await new Promise(r => setTimeout(r, WAIT_TRACKING_INTERVAL));
    }
  }

  if (stillPending.size > 0) {
    result.ok = false;
    result.timedOut = true;
    // Keep their attachments at "pending" so the operator (downstream
    // human review path) can see what was unfinished.
    for (const idx of stillPending) {
      const att = result.attachments[idx];
      console.warn(`[autoPipeline] tracking timeout after ${WAIT_TRACKING_MAX_MS}ms: ${att.trackingCode}`);
    }
  }

  return result;
}

function json(statusCode, body) {
  return { statusCode, headers: { ...CORS, "Content-Type": "application/json" }, body: JSON.stringify(body) };
}
function bad(msg, code = 400) { return json(code, { error: msg }); }
function ok(body)             { return json(200, { success: true, ...body }); }

async function writeAudit({ threadId, draftId = null, eventType, actor = "system:autoPipeline", payload = {} }) {
  await db.collection(AUDIT_COLL).add({
    threadId, draftId, eventType, actor, payload,
    createdAt: FV.serverTimestamp()
  });
}

/** Read the kill-switch from EtsyMail_Config/global.sendDisabled.
 *
 *  v1.5: this used to read EtsyMail_Config/killSwitch.disabled, which
 *  was a SEPARATE doc from the one etsyMailDraftSend reads/writes
 *  (EtsyMail_Config/global.sendDisabled). The split caused the auto-
 *  pipeline to decide "kill-switch off, proceed" while the sender
 *  refused with SEND_DISABLED — leaving threads in queued_for_auto_send
 *  with confusing audit reasons like "enqueue failed: SEND_DISABLED".
 *
 *  Now both readers share one source of truth. The kill_switch_set op
 *  in etsyMailDraftSend writes to /global, and the inbox UI's killswitch
 *  banner polls the same doc.
 *
 *  If on, we MUST skip the auto-send branch (still safe to draft + route
 *  to review). */
async function getKillSwitch() {
  try {
    const doc = await db.collection(CONFIG_COLL).doc("global").get();
    if (!doc.exists) return { disabled: false };
    const d = doc.data() || {};
    return {
      disabled: !!d.sendDisabled,
      reason  : d.sendDisabledReason || null,
      by      : d.sendDisabledBy     || null,
      at      : d.sendDisabledAt && d.sendDisabledAt.toMillis ? d.sendDisabledAt.toMillis() : null
    };
  } catch (e) {
    console.warn("autoPipeline: killSwitch fetch failed:", e.message);
    return { disabled: false };
  }
}

/** Resolve the base URL for inter-function calls. Netlify provides
 *  `URL` in the env at runtime; locally we default to localhost. */
function functionsBase() {
  return process.env.URL
      || process.env.DEPLOY_URL
      || process.env.NETLIFY_BASE_URL
      || "http://localhost:8888";
}

/** POST to a sibling Netlify function. Forwards the extension secret so
 *  endpoints that require it (etsyMailDraftReply, etsyMailDraftSend in
 *  some ops) accept the call. */
async function callFunction(name, body) {
  const url = `${functionsBase()}/.netlify/functions/${name}`;
  const headers = { "Content-Type": "application/json" };
  if (process.env.ETSYMAIL_EXTENSION_SECRET) {
    headers["X-EtsyMail-Secret"] = process.env.ETSYMAIL_EXTENSION_SECRET;
  }
  const res = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(body || {})
  });
  const text = await res.text();
  let data = {};
  try { data = JSON.parse(text); } catch { data = { raw: text }; }
  if (!res.ok) {
    const err = new Error(data.error || `${name} ${res.status}`);
    err.status = res.status;
    err.data = data;
    throw err;
  }
  return data;
}

/** Fire a background Netlify function. Background functions return 202
 *  immediately and run for up to 15 minutes. We don't await the result —
 *  the function writes its own state to Firestore which the caller polls
 *  for. Returns the 202 response object so the caller can confirm the
 *  invocation was accepted. Throws if the invoke itself failed (network
 *  error, function not found, etc.). */
async function invokeBackgroundFunction(name, body) {
  const url = `${functionsBase()}/.netlify/functions/${name}`;
  const headers = { "Content-Type": "application/json" };
  if (process.env.ETSYMAIL_EXTENSION_SECRET) {
    headers["X-EtsyMail-Secret"] = process.env.ETSYMAIL_EXTENSION_SECRET;
  }
  const res = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(body || {})
  });
  // Netlify background functions return 202 Accepted on successful
  // invoke. Any other status indicates the invoke itself failed.
  if (res.status !== 202) {
    const text = await res.text().catch(() => "");
    const err = new Error(`${name} background invoke returned ${res.status}: ${text.slice(0, 200)}`);
    err.status = res.status;
    throw err;
  }
  return { ok: true, status: res.status };
}

/** Inspect whether the most recent message in a thread is an inbound
 *  message that we haven't yet auto-processed. Returns one of:
 *    { ok: true, inboundMs, ageMs }
 *    { ok: false, reason: "..." }
 *
 *  Pure read-only — used by `claimThread` inside a transaction and by
 *  callers that want to peek without claiming.
 */
function evaluateEligibility(thread, options = {}) {
  if (!thread) return { ok: false, reason: "thread not found" };

  const status = thread.status || "";

  // ── v1.2: archived threads CAN be auto-processed ─────────────────
  // If a customer replies to an archived conversation, the new inbound
  // is the customer re-engaging — exactly when fresh AI handling helps.
  // We removed the archived-bail and instead include "archived" in
  // HIDDEN_INTAKE_STATUSES below, which causes the claim transaction to
  // surface the thread back to pending_human_review during processing.
  // The pipeline then runs normally; if AI confidence is high, the
  // thread goes to Auto-Reply. Operators can re-archive afterward.

  // The "sent" status means the operator manually sent a reply. If the
  // customer follows up, we want the AI to take a fresh look — but the
  // status will have been bumped by the next inbound scrape, so this
  // gate only fires when sent is truly the latest event.
  if (status === "sent" && !options.forceRerun) {
    return { ok: false, reason: "manual send is the latest action; not re-replying without forceRerun" };
  }

  const inboundMs = thread.lastInboundAt && thread.lastInboundAt.toMillis
    ? thread.lastInboundAt.toMillis() : null;
  if (!inboundMs) return { ok: false, reason: "no inbound message on thread" };

  // Idempotency: if we've already processed an inbound at or after this
  // timestamp, skip. forceRerun bypasses.
  const lastProcessedMs = thread.lastAutoProcessedInboundAt && thread.lastAutoProcessedInboundAt.toMillis
    ? thread.lastAutoProcessedInboundAt.toMillis() : 0;
  if (!options.forceRerun && lastProcessedMs >= inboundMs) {
    return { ok: false, reason: "already auto-processed this inbound" };
  }

  // Don't auto-reply to ancient messages (operator backfill safety)
  const ageMs = Date.now() - inboundMs;
  if (ageMs > MAX_INBOUND_AGE_MS && !options.forceRerun) {
    return { ok: false, reason: `inbound too old (${Math.round(ageMs / 86400000)}d) for auto-reply` };
  }

  // If the latest message is outbound (operator just replied manually),
  // there's nothing to respond to.
  const outboundMs = thread.lastOutboundAt && thread.lastOutboundAt.toMillis
    ? thread.lastOutboundAt.toMillis() : 0;
  if (outboundMs > inboundMs) {
    return { ok: false, reason: "latest message is outbound; nothing to reply to" };
  }

  return { ok: true, inboundMs, ageMs };
}

/** Statuses that a thread can be in when the pipeline first sees it but
 *  that aren't visible in the operator inbox folders during processing.
 *  The atomic claim upgrades them to pending_human_review so the thread
 *  is always visible during processing.
 *
 *  v1.2: `archived` is included here. A new inbound on an archived
 *  thread is the customer re-engaging — bring it back into the active
 *  queue. Operator can re-archive after the AI handles it. */
const HIDDEN_INTAKE_STATUSES = new Set([
  "detected_from_gmail",
  "pending_etsy_scrape",
  "etsy_scraped",
  "pending_order_enrichment",
  "ready_for_ai",
  "draft_ready",        // legacy folder removed in v1.1
  "sent",               // legacy folder removed in v1.1 (only re-routed on new inbound)
  "hold_uncertain",     // legacy hold
  "hold_missing_order", // legacy hold
  "hold_login_required",// legacy hold
  "failed_scrape",
  "failed_send",
  "archived"            // v1.2: re-engage on new customer inbound
]);

/** Atomically claim a thread for auto-processing. Combines the eligibility
 *  check and the "in_progress" marker in one Firestore transaction so two
 *  rapid back-to-back snapshots can't both trigger an Opus call for the
 *  same inbound.
 *
 *  Side-effects (on successful claim):
 *    - Sets `lastAutoProcessedInboundAt` to the inbound timestamp
 *      (this is the idempotency lock — subsequent calls see it and skip).
 *    - Upgrades thread status from any HIDDEN_INTAKE_STATUS to
 *      pending_human_review so the thread is immediately visible while
 *      the AI call runs. The pipeline can later upgrade to auto_replied.
 *    - Writes lastAutoDecision="in_progress" / lastAutoDecisionAt so the
 *      operator's UI can show "AI thinking…" if we add that affordance.
 *
 *  Returns the same shape as evaluateEligibility, plus { claimed: true }
 *  on success and { claimed: false } when the lock was already held.
 */
async function claimThread(threadRef, options = {}) {
  return await db.runTransaction(async (tx) => {
    const snap = await tx.get(threadRef);
    if (!snap.exists) return { ok: false, claimed: false, reason: "thread not found" };
    const data = snap.data() || {};

    const elig = evaluateEligibility(data, options);
    if (!elig.ok) return {
      ok: false, claimed: false,
      reason: elig.reason, status: data.status,
      // v3.32 — expose prior lastAutoDecision so the handler's bail
      // path can detect orphan "deferred_quiet_period" state (left
      // behind when the operator manually replied between when
      // section 1.4 set the defer and when the reaper picked it up)
      // and clean it up. Without this, the reaper re-fires the same
      // stuck thread every 5 minutes forever.
      prevLastAutoDecision: data.lastAutoDecision || null
    };

    // Decide what status to claim with. If the thread is currently in a
    // hidden intake state, surface it to Needs Review so it's visible to
    // operators during the (potentially 60-second) AI run. If it's already
    // in auto_replied or pending_human_review, leave it — those are
    // user-visible states; we'll either keep or upgrade after the AI call.
    const currentStatus = data.status || "";
    const claimStatus = HIDDEN_INTAKE_STATUSES.has(currentStatus)
      ? "pending_human_review"
      : currentStatus;

    tx.update(threadRef, {
      lastAutoProcessedInboundAt: data.lastInboundAt,        // the lock
      status                    : claimStatus,
      lastAutoDecision          : "in_progress",
      lastAutoDecisionAt        : FV.serverTimestamp(),
      updatedAt                 : FV.serverTimestamp()
    });

    return {
      ok: true, claimed: true,
      inboundMs    : elig.inboundMs,
      ageMs        : elig.ageMs,
      previousStatus: currentStatus,
      claimStatus
    };
  });
}

/** Check whether a draft for this thread is already in flight (queued,
 *  sending, or recently sent). If so, skip — don't double-send. */
async function isDraftInFlight(threadId) {
  const draftId = "draft_" + threadId;
  const snap = await db.collection(DRAFTS_COLL).doc(draftId).get();
  if (!snap.exists) return null;
  const d = snap.data() || {};
  if (d.status === "queued" || d.status === "sending") return d.status;
  return null;
}

/** Mark a thread as auto-processed at a specific inbound timestamp +
 *  set its operator-facing status. Used by all decision branches.
 *
 *  Note: `lastAutoProcessedInboundAt` was already set by `claimThread`
 *  at the start of the run; we re-set it here defensively in case the
 *  finalize timestamp differs (it shouldn't, but doc-merging is cheap). */
async function finalizeThread(threadId, { newStatus, inboundMs, decision, draftId, aiConfidence, aiDifficulty }) {
  // v4.3.2 — STICKY-COMPLETION GUARD. Once the listing-creator worker
  // writes status="sales_completed" + salesCompletedAt, this thread's
  // status MUST stay sales_completed. Without this guard, the next
  // post-purchase customer message ("thanks!" / "where's my package?")
  // routes through the auto-pipeline, lands here, and overwrites
  // status to "auto_replied" / "pending_human_review" — making the
  // dashboard's status pill flap and removing the thread from the
  // operator's mental model of "this sale is closed".
  //
  // We still update lastAutoDecision / aiDraftStatus / latestDraftId so
  // the operator can see the most recent customer-service draft
  // generated on the post-sale follow-up. We just don't touch `status`
  // or `aiConfidence` — those reflect the closed sale.
  //
  // v3.25 — STICKY-RUSH GUARD. Same pattern applied to active rush
  // production threads. When productionRush.acceptedAt is set (and
  // not removedAt), the thread should stay in the Production Rush
  // folder regardless of subsequent AI auto-processing on follow-up
  // customer messages. Without this guard, a post-acceptance "thanks!"
  // would flip status back to auto_replied and the thread would
  // silently leave the rush folder before the order is even shipped.
  let isCompletedSale = false;
  let isActiveRush = false;
  try {
    const snap = await db.collection(THREADS_COLL).doc(threadId).get();
    if (snap.exists) {
      const data = snap.data() || {};
      if (data.salesCompletedAt) isCompletedSale = true;
      if (data.productionRush
          && data.productionRush.acceptedAt
          && !data.productionRush.removedAt) {
        isActiveRush = true;
      }
    }
  } catch (e) {
    // Read failure shouldn't block finalize; fall through to normal write.
    console.warn("finalizeThread completion-check failed (proceeding):", e.message);
  }

  const patch = {
    lastAutoProcessedInboundAt   : admin.firestore.Timestamp.fromMillis(inboundMs),
    lastAutoDecision             : decision,
    lastAutoDecisionAt           : FV.serverTimestamp(),
    aiDraftStatus                : draftId ? "ready" : "none",
    updatedAt                    : FV.serverTimestamp()
  };
  // For completed sales OR active rush, keep status + aiConfidence
  // + aiDifficulty immutable. For everything else, write them as before.
  if (!isCompletedSale && !isActiveRush) {
    patch.status       = newStatus;
    patch.aiConfidence = aiConfidence;
    patch.aiDifficulty = aiDifficulty;
  } else if (isActiveRush) {
    // Still update aiConfidence/aiDifficulty so the operator sees the
    // current AI's read on this customer message — just don't overwrite
    // status. Active rush stays in the rush folder until removed/shipped.
    patch.aiConfidence = aiConfidence;
    patch.aiDifficulty = aiDifficulty;
  }
  // Only write latestDraftId when we actually have one. Avoids
  // overwriting a previous valid draftId with null when the pipeline
  // skips AI generation (e.g., when the autoPipeline is disabled).
  if (draftId) patch.latestDraftId = draftId;
  await db.collection(THREADS_COLL).doc(threadId).set(patch, { merge: true });
}

// ─── v1.2: Deterministic veto rules ─────────────────────────────────────
//
// Self-rated AI confidence is not enough for high-stakes scenarios. Even
// a model that scores its own draft at 0.95 should NOT auto-send if the
// inbound mentions a refund, a chargeback, legal escalation, etc. These
// rules are the bright-line safety net.
//
// Pattern matching is intentionally conservative — false positives push
// to human review (cheap, just wastes one auto-send opportunity); false
// negatives push to auto-send (expensive, can damage customer trust).
// When in doubt, add the pattern.
//
// Patterns are case-insensitive and word-boundary anchored. Tested
// against both the latest inbound text (highest signal) AND the AI's
// outbound draft text (catches drafts that say "I'll process your
// refund" even when the inbound was cagey).
const DETERMINISTIC_VETO_PATTERNS = [
  // ── Money-sensitive ──────────────────────────────────────────────
  { id: "refund",      pattern: /\b(refund|chargeback|dispute|money\s*back|return\s+(this|the|my)\s+(item|order|product)|process\s+(?:a|the|my)\s+refund)\b/i,
    reason: "refund/return language" },
  { id: "cancel",      pattern: /\b(cancel\s+(?:my|the|this)\s+(?:order|purchase)|cancellation\s+(?:request|policy)|cancel\s+(?:and|&)\s+refund)\b/i,
    reason: "cancellation request" },

  // ── Legal / escalation ───────────────────────────────────────────
  { id: "legal",       pattern: /\b(lawsuit|sue\s*you|small\s+claims|legal\s+action|attorney|consult\s+(?:my\s+)?lawyer|file\s+a\s+case)\b/i,
    reason: "legal escalation" },
  { id: "complaint",   pattern: /\b(BBB|Better\s+Business\s+Bureau|file\s+a\s+complaint|complaint\s+with\s+Etsy|report\s+(?:you|this\s+shop|seller))\b/i,
    reason: "formal complaint" },
  { id: "fraud",       pattern: /\b(scammer|scammed|fraudulent|fraud\s+(?:case|alert)|theft|stolen\s+(?:my|the))\b/i,
    reason: "fraud accusation" },

  // ── Order data integrity ─────────────────────────────────────────
  { id: "address",     pattern: /\b(change\s+(?:my|the)\s+(?:shipping\s+)?address|wrong\s+address|different\s+address|update\s+(?:my\s+)?address|ship\s+to\s+(?:a\s+)?different)\b/i,
    reason: "address change" },
  { id: "personalize", pattern: /\b(change\s+(?:the\s+)?(?:name|spelling|engraving|personalization|customization|wording)|wrong\s+name|misspelled|spelled\s+wrong|spell(?:ing|ed)?\s+it\s+wrong)\b/i,
    reason: "personalization correction" },

  // ── Damage / replacement ─────────────────────────────────────────
  { id: "damaged",     pattern: /\b(damaged|broken|defective|cracked|shattered|arrived\s+broken|wrong\s+item\s+received|received\s+the\s+wrong)\b/i,
    reason: "damage/wrong-item claim" },
  { id: "missing",     pattern: /\b(missing\s+(?:item|piece|part)|never\s+(?:received|arrived|came)|hasn't\s+(?:arrived|come)|never\s+got\s+(?:my|it|the))\b/i,
    reason: "non-delivery claim" },
  { id: "replace",     pattern: /\b(send\s+(?:me\s+)?(?:another|a\s+replacement|a\s+new\s+one)|replacement\s+(?:order|piece|item)|reship)\b/i,
    reason: "replacement request" },

  // ── Custom orders / deals ────────────────────────────────────────
  { id: "custom",      pattern: /\b(custom\s+order|customize\b|customise\b|special\s+request|can\s+you\s+make\s+(?:me\s+)?a|bulk\s+order|wholesale\b|discount\s+code|coupon\s+code)\b/i,
    reason: "custom-order or discount inquiry" }
];

/** Run all veto patterns against given text. Returns array of triggered
 *  veto IDs + reasons. Empty array = clean.
 *
 *  excludePatternIds: array of pattern IDs to skip. Used by the sales-
 *  agent auto-send path to skip the "custom" pattern, since sales mode
 *  exists specifically to handle custom-order inquiries — applying that
 *  veto to a sales-agent draft would block 100% of sales auto-sends. */
function runVetoPatterns(text, excludePatternIds = []) {
  if (!text || typeof text !== "string") return [];
  const skipSet = new Set(excludePatternIds);
  const hits = [];
  for (const v of DETERMINISTIC_VETO_PATTERNS) {
    if (skipSet.has(v.id)) continue;
    if (v.pattern.test(text)) hits.push({ id: v.id, reason: v.reason });
  }
  return hits;
}

/** Fetch the recent INBOUND BURST from a thread — up to the 5 most
 *  recent inbound messages, concatenated chronologically. Used by:
 *    - the intent classifier (so a final-message nudge like "please
 *      confirm?" still classifies correctly when read alongside the
 *      substantive earlier messages from the same conversation flow)
 *    - safety vetoes that scan customer text for trigger phrases
 *
 *  Why a burst instead of just the latest? Customers commonly send
 *  multiple inbounds in succession — an opening question, a follow-up
 *  detail, then a nudge — and the latest in isolation is often
 *  ambiguous. The classifier prompt is single-message-oriented but
 *  handles concatenated text fine: it picks up the strongest signals
 *  in the combined input.
 *
 *  Returns null if no inbound exists. 4000-char cap matches the
 *  classifier's input budget. */
/** v4.3.7 — Return ONLY the most recent inbound message's text (not
 *  the 5-message concatenation that loadLatestInboundText returns).
 *
 *  Used by the deterministic veto path. The veto judges whether
 *  *this turn's* customer message + draft are safe to auto-send.
 *  Multi-turn context isn't appropriate there:
 *
 *    - A "missing" pattern that matched on a message from sale #1
 *      ("I never received my tracking notification") shouldn't poison
 *      auto-sends weeks later when the customer comes back for a fresh
 *      sale (round 2+). The veto's 5-message lookback was a real bug
 *      surfaced by Joanna's banana-charm round-2: the agent's clean
 *      discovery clarifier was vetoed because of a phrase from the
 *      original baseball-charm conversation.
 *
 *    - Even within a single conversation, only the latest inbound
 *      reflects the customer's current intent. Earlier messages might
 *      have asked about refunds/cancellations and been resolved in the
 *      conversation flow; vetoing the next reply because of THAT old
 *      mention would block all subsequent auto-sends.
 *
 *  The classifier's multi-message helper (loadLatestInboundText) is
 *  preserved for its own use — it benefits from context (a "please
 *  confirm?" nudge classifies correctly when read alongside the
 *  substantive earlier messages).
 *
 *  Returns null if no inbound exists. 1500-char cap matches typical
 *  Etsy message length comfortably. */
async function loadCurrentInboundTextOnly(threadId) {
  try {
    const snap = await db.collection(THREADS_COLL).doc(threadId)
      .collection("messages")
      .orderBy("timestamp", "desc")
      .limit(50)
      .get();
    if (snap.empty) return null;
    for (const d of snap.docs) {
      const data = d.data();
      if (data.direction !== "inbound") continue;
      const t = String(data.text || "").trim();
      if (!t) continue;
      return t.slice(0, 1500);
    }
    return null;
  } catch (e) {
    console.warn("loadCurrentInboundTextOnly failed:", e.message);
    return null;
  }
}

async function loadLatestInboundText(threadId) {
  try {
    // We pull up to 50 recent messages by `timestamp desc` and filter
    // direction in JS to avoid requiring a composite index. The latest
    // 50 effectively always contain the latest 5 inbounds.
    const snap = await db.collection(THREADS_COLL).doc(threadId)
      .collection("messages")
      .orderBy("timestamp", "desc")
      .limit(50)
      .get();
    if (snap.empty) return null;
    const recentInboundsNewestFirst = [];
    for (const d of snap.docs) {
      const data = d.data();
      if (data.direction !== "inbound") continue;
      const t = String(data.text || "").trim();
      if (!t) continue;
      recentInboundsNewestFirst.push(t);
      if (recentInboundsNewestFirst.length >= 5) break;
    }
    if (recentInboundsNewestFirst.length === 0) return null;
    // Reverse to chronological so the classifier reads the customer's
    // arc oldest → newest (their opening message → their latest nudge).
    const chronological = recentInboundsNewestFirst.slice().reverse();
    return chronological.join("\n\n").slice(0, 4000);
  } catch (e) {
    console.warn("loadLatestInboundText failed:", e.message);
    return null;
  }
}

/** v2.0 Step 2: Load the latest inbound message in a single pass —
 *  returns text + a normalized attachments array suitable for handing
 *  to the sales agent's image content blocks. Snapshot already stores
 *  `imageUrls[]` per message (v1.10 schema, no change needed). We just
 *  reshape into [{url}, ...] for the agent. */
async function loadLatestInbound(threadId) {
  try {
    // Same composite-index avoidance as loadLatestInboundText above:
    // pull the latest 50 messages by timestamp (single-field index that
    // Firestore creates automatically) and filter direction in JS.
    const snap = await db.collection(THREADS_COLL).doc(threadId)
      .collection("messages")
      .orderBy("timestamp", "desc")
      .limit(50)
      .get();
    if (snap.empty) return { text: null, attachments: [], threadAttachments: [] };

    let latestInbound = null;
    const threadAttachments = [];
    const seen = new Set();

    for (const d of snap.docs) {
      const data = d.data() || {};
      if (data.direction !== "inbound") continue;
      if (!latestInbound) latestInbound = data;

      const imageUrls = Array.isArray(data.imageUrls) ? data.imageUrls : [];
      const attachmentUrls = Array.isArray(data.attachmentUrls) ? data.attachmentUrls : [];
      for (const url of [...imageUrls, ...attachmentUrls]) {
        if (typeof url !== "string" || !/^https?:\/\//.test(url) || seen.has(url)) continue;
        seen.add(url);
        threadAttachments.push({ url, source: "thread_history" });
        if (threadAttachments.length >= 12) break;
      }
      if (threadAttachments.length >= 12) break;
    }

    if (!latestInbound) return { text: null, attachments: [], threadAttachments };
    const text = String(latestInbound.text || "").slice(0, 4000);
    const latestImageUrls = Array.isArray(latestInbound.imageUrls) ? latestInbound.imageUrls : [];
    const latestAttachmentUrls = Array.isArray(latestInbound.attachmentUrls) ? latestInbound.attachmentUrls : [];
    const attachments = [...latestImageUrls, ...latestAttachmentUrls]
      .filter(u => typeof u === "string" && /^https?:\/\//.test(u))
      .map(url => ({ url, source: "latest_inbound" }));

    return { text, attachments, threadAttachments };
  } catch (e) {
    console.warn("loadLatestInbound failed:", e.message);
    return { text: null, attachments: [], threadAttachments: [] };
  }
}
/** v2.0 Step 2: True iff this thread has a SalesContext doc whose
 *  stage is one of the active (non-terminal) sales stages. Used to
 *  route stateful sales threads back to the sales agent regardless
 *  of intent classification — protects mid-funnel threads from being
 *  clobbered if the classifier hiccups on a one-word reply. */
const ACTIVE_SALES_STAGES = new Set([
  "discovery", "spec", "quote", "revision", "pending_close_approval"
]);
async function loadActiveSalesContextStage(threadId) {
  try {
    const doc = await db.collection("EtsyMail_SalesContext").doc(threadId).get();
    if (!doc.exists) return null;
    const data = doc.data() || {};
    if (ACTIVE_SALES_STAGES.has(data.stage)) return data.stage;
    return null;
  } catch (e) {
    console.warn("loadActiveSalesContextStage failed:", e.message);
    return null;
  }
}

/** Apply all deterministic safety checks. Returns { vetoed, reasons }.
 *  Combines:
 *    - inbound message regex matches
 *    - outbound draft regex matches (catches AI drafts that promise
 *      things even when the inbound was cagey)
 *    - tool-call errors in the AI's draft (lookup failed → AI is
 *      working with incomplete data → don't auto-send)
 */
function applyDeterministicVetoes({ inboundText, draftText, draftToolCalls, excludePatternIds = [] }) {
  const reasons = [];

  const inboundHits = runVetoPatterns(inboundText, excludePatternIds);
  for (const h of inboundHits) reasons.push("inbound_" + h.id + ": " + h.reason);

  const outboundHits = runVetoPatterns(draftText, excludePatternIds);
  for (const h of outboundHits) reasons.push("outbound_" + h.id + ": " + h.reason);

  // Tool-call errors: if the AI tried to look up an order or tracking
  // number and the call errored, the AI is either working with stale
  // info or flat-out hallucinating. Don't trust the draft.
  const toolErrors = (Array.isArray(draftToolCalls) ? draftToolCalls : [])
    .filter(tc => tc && tc.error && tc.name !== "compose_draft_reply");
  if (toolErrors.length) {
    reasons.push("tool_call_failed: " + toolErrors.map(tc => tc.name).join(","));
  }

  return { vetoed: reasons.length > 0, reasons };
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }
  if (event.httpMethod !== "POST") {
    return json(405, { error: "Method Not Allowed" });
  }

  // Auth: require the same shared secret as other extension endpoints.
  // The function is invoked by etsyMailSnapshot internally (which has
  // the secret in env) and can also be called by an operator from the
  // inbox (browser forwards the secret from localStorage).
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return bad("Invalid JSON body"); }

  const {
    threadId,
    confidenceThreshold,
    employeeName = "system:auto-pipeline",
    forceRerun   = false,
    forceRegenerate = false,
    bypassExistingDraft = false,
    manualRunId  = null,
    dryRun       = false
  } = body;

  if (!threadId) return bad("Missing threadId");

  // Single-source-of-truth config: Firestore at EtsyMail_Config/autoPipeline.
  const autoCfg = await getAutoPipelineConfig();

  // Per-call override via request body (lets operators dry-run a specific
  // threshold), otherwise use the Firestore config.
  const threshold = (typeof confidenceThreshold === "number" && confidenceThreshold >= 0 && confidenceThreshold <= 1)
    ? confidenceThreshold
    : autoCfg.threshold;

  const tStart = Date.now();
  const threadRef = db.collection(THREADS_COLL).doc(threadId);

  try {
    // ─── 1. Atomic claim ─────────────────────────────────────────
    // Combines eligibility check + lock acquisition into one Firestore
    // transaction. After this, the thread:
    //   - has lastAutoProcessedInboundAt set (idempotency lock)
    //   - is in pending_human_review if it was at a hidden intake status
    //     (so it's visible during the AI run, not stuck in "All" only)
    //   - has lastAutoDecision="in_progress" for observability
    //
    // If the lock is already held, the second caller bails here — no
    // wasted Opus calls in race scenarios.
    const claim = await claimThread(threadRef, { forceRerun });
    if (!claim.ok) {
      // v3.32 — Orphan defer cleanup. If this thread was sitting in
      // deferred_quiet_period when the operator manually replied,
      // nothing on the manual-send path (pre-v3.32) cleared the
      // defer fields, so the reaper kept picking the thread up
      // every 5 min and the auto-pipeline kept bailing here without
      // resolving the orphan state. Self-heal: when we bail AND
      // the thread's prior lastAutoDecision was deferred_quiet_period,
      // write a terminal value and delete autoPipelineDeferUntilMs.
      // Next reaper sweep won't see this thread (its query filters
      // on lastAutoDecision == "deferred_quiet_period") and the UI
      // timer's existing self-correction confirms hidden.
      if (claim.prevLastAutoDecision === "deferred_quiet_period") {
        await threadRef.set({
          lastAutoDecision         : "skipped_already_replied",
          lastAutoDecisionAt       : FV.serverTimestamp(),
          autoPipelineDeferUntilMs : FV.delete(),
          updatedAt                : FV.serverTimestamp()
        }, { merge: true });
        await writeAudit({
          threadId, eventType: "auto_pipeline_defer_orphan_cleared",
          payload: { reason: claim.reason, status: claim.status }
        });
      }
      await writeAudit({
        threadId, eventType: "auto_pipeline_skipped",
        payload: { reason: claim.reason, status: claim.status }
      });
      return ok({
        threadId, decision: "skipped",
        skipReason: claim.reason,
        durationMs: Date.now() - tStart
      });
    }

    // Don't double-act if a previous run is still queued/sending. We
    // check this AFTER the claim — the claim itself doesn't know about
    // draft state, only thread state.
    const inFlight = await isDraftInFlight(threadId);
    if (inFlight && !forceRerun) {
      // v1.4: explicitly unwind the claim's in_progress marker.
      // The claim transaction set lastAutoDecision="in_progress"; if
      // we just return here, the thread shows "AI thinking..." forever
      // until the stale-claim reaper kicks in 5+ minutes later. Set a
      // terminal decision so the UI reflects reality immediately.
      //
      // Thread status was already moved to a visible folder by the
      // claim if needed, so no status change is required — just clear
      // the in_progress flag.
      await db.collection(THREADS_COLL).doc(threadId).set({
        lastAutoDecision           : "skipped_draft_in_flight",
        lastAutoDecisionAt         : FV.serverTimestamp(),
        lastAutoProcessedInboundAt : null,    // allow re-trigger when in-flight clears
        updatedAt                  : FV.serverTimestamp()
      }, { merge: true });
      await writeAudit({
        threadId, eventType: "auto_pipeline_skipped",
        payload: { reason: `draft already ${inFlight}`, claimUnwound: true }
      });
      return ok({
        threadId, decision: "skipped",
        skipReason: `draft already ${inFlight}`,
        durationMs: Date.now() - tStart
      });
    }

    // ─── 1.4. Quiet-period debounce (v3.30) ─────────────────────────
    // If the customer's most recent inbound is fresher than the
    // configured quiet period, they're probably still typing
    // follow-ups. Release the claim, mark the thread
    // "deferred_quiet_period", and exit without burning the Anthropic
    // bill.
    //
    // Sliding-window semantics: each new inbound that lands during the
    // quiet period re-enters this function (snapshot fires it on every
    // ingest), the claim re-succeeds because we cleared
    // lastAutoProcessedInboundAt below, the new inboundMs is fresh
    // again, and we defer again — pushing autoPipelineDeferUntilMs
    // forward. The customer's last keystroke effectively resets the
    // timer.
    //
    // When the customer goes quiet: nothing re-triggers us, and
    // etsyMailReapers's runDeferredAutoPipelinePass picks up threads
    // whose autoPipelineDeferUntilMs has elapsed and fires the
    // pipeline once. That single fire passes through this gate
    // (sinceInboundMs >= quietMs) and proceeds to intent + drafting.
    //
    // forceRerun bypasses (operator manual trigger from the inbox UI
    // should always run, no debounce). quietPeriodMinutes:0 also
    // disables the gate entirely.
    const quietMinutes = typeof autoCfg.quietPeriodMinutes === "number"
                         ? autoCfg.quietPeriodMinutes : 5;
    const quietMs = Math.max(0, quietMinutes * 60_000);
    const inboundMs = claim.inboundMs;
    const sinceInboundMs = inboundMs ? (Date.now() - inboundMs) : Infinity;

    if (quietMs > 0 && sinceInboundMs < quietMs && !forceRerun) {
      const deferUntilMs = inboundMs + quietMs;
      await threadRef.set({
        lastAutoDecision           : "deferred_quiet_period",
        lastAutoDecisionAt         : FV.serverTimestamp(),
        lastAutoProcessedInboundAt : null,           // release the lock so new inbounds re-claim
        autoPipelineDeferUntilMs   : deferUntilMs,   // reaper pickup field
        updatedAt                  : FV.serverTimestamp()
      }, { merge: true });

      await writeAudit({
        threadId,
        eventType: "auto_pipeline_deferred",
        payload  : {
          reason          : "quiet_period",
          sinceInboundMs,
          quietPeriodMs   : quietMs,
          deferUntilMs,
          deferUntilIso   : new Date(deferUntilMs).toISOString()
        }
      });

      return ok({
        threadId,
        decision      : "deferred",
        deferUntilMs,
        sinceInboundMs,
        quietPeriodMs : quietMs,
        durationMs    : Date.now() - tStart
      });
    }

    // ─── 1.5. Intent classification (v2.0 Step 1, gated) ────────────
    // Runs before draft generation so:
    //   (a) the operator's badge is in place by the time the thread
    //       lights up in their list,
    //   (b) v2.0 Step 2's sales-lead router (stubbed below) can read
    //       the result from local scope without a second DB hit.
    //
    // Non-fatal: if classification fails (Haiku down, JSON parse error,
    // anything), we log + audit and continue with the existing
    // customer-service path. The classifier's purpose is to ENRICH the
    // pipeline, never to gate it.
    //
    // latestText is hoisted to the outer try-block scope so the Step 2
    // sales-lead router (commented block below) can reuse it without
    // a second loadLatestInboundText() round-trip.
    let latestText = null;
    let intentResp = null;
    if (autoCfg.intentClassifierEnabled) {
      try {
        // v3.0: classifier reads the thread tail itself. We still load
        // latestText separately because it's used downstream by the
        // sales-agent path (loadLatestInbound for attachments).
        latestText = await loadLatestInboundText(threadId);
        intentResp = await callFunction("etsyMailIntentClassifier", {
          threadId,
          actor: employeeName || "system:auto-pipeline"
        });
        // The classifier already wrote both:
        //   - canonical record  → EtsyMail_IntentClassifications/{threadId}
        //   - thread denormalize → EtsyMail_Threads/{threadId}.intent*
        //   - audit              → EtsyMail_Audit { eventType: "intent_classified" }
        // Nothing more to do here; the response is held only for the
        // Step 2 routing decision below.
      } catch (e) {
        console.warn("intent classify failed (non-fatal):", e.message);
        await writeAudit({
          threadId,
          eventType: "intent_classify_failed",
          payload  : {
            error    : e.message,
            errorCode: (e.data && e.data.errorCode) || null
          }
        });
      }
    }

    // ─── 1.6. v2.0 Step 2 — Sales-lead routing (LIVE) ───────────────
    //
    // Two ways a thread reaches the sales agent:
    //
    //   (a) STATEFUL: this thread already has an active SalesContext
    //       (stage is discovery/spec/quote/revision/pending_close_approval).
    //       Once a sales conversation starts, every subsequent customer
    //       reply MUST go back to the sales agent — even if the latest
    //       inbound is something the intent classifier would call
    //       "post_purchase" or "unclear". The state IS the routing
    //       authority. This protects mid-funnel deals from being
    //       clobbered by classifier hiccups on short replies.
    //
    //   (b) FRESH: classifier called this inbound a sales_lead at >= 0.7
    //       confidence AND auto-engagement is enabled AND the thread is
    //       in the pilot allow-list (or the allow-list is empty).
    //
    // Both paths require salesModeEnabled. The pilot allow-list applies
    // to BOTH paths — if a thread isn't in pilot, even an active
    // SalesContext gets ignored (matches the spec's "rollback by
    // emptying the list" semantic).

    // v4.4.0 — Help-request hard-gate.
    //
    // Etsy threads with the "Help request" heading badge are customers
    // asking for assistance with an EXISTING order. They are NEVER sales
    // conversations regardless of what the message text might suggest.
    // The Etsy scrape captures this on the thread doc as etsyHeadingBadge
    // ("Help request") together with etsyOrderId and etsyHeadingTitle.
    //
    // This deterministic gate runs BEFORE the sales-routing branch so:
    //   - Fresh help-request threads never enter the sales funnel.
    //   - The intent classifier's potential mis-classification (e.g. on
    //     a help-request that contains an engraving change) cannot
    //     override the structured Etsy signal.
    //   - An active SalesContext from a prior run on the same thread
    //     does NOT keep a help-request inbound stuck in sales mode.
    //
    // Operators can still manually move a thread into sales via the UI's
    // move-to dropdown when they truly want to override.
    const helpRequestSnap = await threadRef.get();
    const helpRequestData = helpRequestSnap.exists ? helpRequestSnap.data() : {};
    const isEtsyHelpRequest =
         !!helpRequestData.etsyHeadingBadge
      && /^\s*help\s*request\s*$/i.test(String(helpRequestData.etsyHeadingBadge));

    if (isEtsyHelpRequest) {
      await writeAudit({
        threadId,
        eventType: "sales_routing_skipped_help_request",
        payload  : {
          reason          : "Etsy 'Help request' badge — thread routed to support, not sales",
          etsyOrderId     : helpRequestData.etsyOrderId || null,
          etsyHeadingBadge: helpRequestData.etsyHeadingBadge,
          etsyHeadingTitle: helpRequestData.etsyHeadingTitle || null,
          classifierSaid  : (intentResp && intentResp.classification) || null,
          classifierConf  : (intentResp && intentResp.confidence)    || null
        }
      });
      // Fall through to the draft-reply path below. Skip sales-routing
      // branch entirely.
    } else if (autoCfg.salesModeEnabled
        && (autoCfg.salesPilotThreadIds.length === 0
            || autoCfg.salesPilotThreadIds.includes(threadId))) {

      const activeSalesStage = await loadActiveSalesContextStage(threadId);


      const freshSalesLead =
           autoCfg.salesAutoEngage
        && intentResp
        && intentResp.classification === "sales_lead"
        && typeof intentResp.confidence === "number"
        && intentResp.confidence >= 0.7;

      // v4.3.5 — Post-terminal re-engagement (returning customer for a
      // brand-new sale).
      //
      // Once a sale completes (worker writes salesCompletedAt) or is
      // abandoned (reaper writes status=sales_abandoned), the sales
      // agent's terminal-status guard refuses to re-engage even if the
      // customer comes back asking for a new product. That's correct
      // for follow-up chatter ("thanks, when will it ship?") but wrong
      // for genuine new sales leads ("I'd like to place another order").
      //
      // Detection: the thread is in a terminal sales state AND the
      // intent classifier called this inbound a fresh sales_lead at
      // high confidence. If both are true, this is round N+1 of the
      // customer's relationship and we should run the full sales-agent
      // funnel from scratch.
      //
      // What we reset:
      //   - SalesContext.stage          → "discovery" (fresh funnel)
      //   - All listing-pipeline fields → cleared (the new sale's worker
      //                                   run starts from a clean slate)
      //   - customerAccepted/At + accepted quote fields → cleared
      //   - salesCompletedAt + salesSynopsis → archived to a sub-
      //                                   collection, then cleared
      //   - thread.status               → "sales_active"
      //   - freshSalesRound             → incremented (audit trail)
      //
      // What we PRESERVE on the thread:
      //   - All scraped messages and message subcollection
      //   - SalesContext history of past quotes/specs (in subcoll if
      //     used) — only the active stage is reset
      //   - Audit log (immutable; appended-to)
      //
      // Edge cases:
      //   - In-flight worker for the prior sale (status="creating"):
      //     salesCompletedAt is NOT set yet, trigger doesn't fire,
      //     mid-flight work completes safely. Once it does set
      //     salesCompletedAt, a future inbound can trigger reset.
      //   - First-acceptance cool-down (30s after customerAcceptedAt):
      //     no longer relevant — we're clearing customerAcceptedAt as
      //     part of the reset.
      const threadDoc = await db.collection(THREADS_COLL).doc(threadId).get();
      const threadData = threadDoc.exists ? threadDoc.data() : {};
      const isPostTerminal =
           !!threadData.salesCompletedAt
        || threadData.status === "sales_abandoned";
      const shouldResetForRound2 = isPostTerminal && freshSalesLead;

      if (shouldResetForRound2) {
        try {
          const round = Number(threadData.salesRound || 1) + 1;

          // 1. Archive the prior sale's snapshot (synopsis + SalesContext)
          //    so the operator keeps visibility into past rounds. Subcollection
          //    key includes the round number for stable, predictable IDs.
          //
          // v4.3.8 — also snapshot the SalesContext (accumulatedSpec,
          //    quoteHistory, notes, family/qty/codes, etc.) so the dashboard's
          //    "Previous Sales" tabs can render the same view the operator
          //    saw at the time of completion. Without this, archived rounds
          //    would only have the listing-pipeline data; everything in the
          //    Sales conversation card (notes, spec, quote breakdown) would
          //    be missing because that data lives in EtsyMail_SalesContext
          //    and gets overwritten on round-2 reset.
          //
          // v4.3.9 — added explicit audit events for archive
          //    success/skip/failure. Without these, an archive that
          //    silently failed (e.g., Firestore quota, network blip,
          //    field-size limit) was indistinguishable from "archive
          //    not attempted" — the operator just saw an empty
          //    salesHistory subcollection with no diagnostic trail.
          let archiveOutcome = "skipped_no_data";
          let archiveError = null;
          if (threadData.salesSynopsis || threadData.customListingId) {
            try {
              const archiveDocId = `round_${threadData.salesRound || 1}_${(threadData.salesCompletedAt && threadData.salesCompletedAt.toMillis ? threadData.salesCompletedAt.toMillis() : Date.now())}`;

              // Read the current SalesContext (about to be reset to
              // discovery in step 3 below). Best-effort — if read fails
              // we still archive the listing-pipeline data so we don't
              // lose the round entirely.
              let salesContextSnapshot = null;
              try {
                const ctxSnap = await db.collection("EtsyMail_SalesContext").doc(threadId).get();
                if (ctxSnap.exists) {
                  // Strip server-side timestamp fields that don't serialize
                  // cleanly into a snapshot doc; keep everything else verbatim.
                  const ctx = ctxSnap.data() || {};
                  salesContextSnapshot = {
                    stage                : ctx.stage                 || null,
                    accumulatedSpec      : ctx.accumulatedSpec       || null,
                    quoteHistory         : ctx.quoteHistory          || null,
                    totalQuotedUsd       : ctx.totalQuotedUsd        || null,
                    missingInputs        : ctx.missingInputs         || null,
                    notes                : ctx.notes                 || null,
                    quantity             : ctx.quantity              || null,
                    wantsRush            : ctx.wantsRush             || null,
                    urgency_level        : ctx.urgency_level         || null,
                    selectedCodes        : ctx.selectedCodes         || null,
                    family               : ctx.family                || null,
                    lastTurnAt           : ctx.lastTurnAt            || null,
                    operatorOverrides    : ctx.operatorOverrides     || null
                    // Anything else on SalesContext is intentionally NOT
                    // archived — agent-internal state (lastResolverResult,
                    // _lastResolverResult, etc.) doesn't belong in a
                    // historical snapshot.
                  };
                }
              } catch (e) {
                console.warn(`[autoPipeline] SalesContext snapshot failed for ${threadId} (archiving listing data only):`, e.message);
              }

              await db.collection(THREADS_COLL).doc(threadId)
                .collection("salesHistory").doc(archiveDocId).set({
                  round                   : threadData.salesRound || 1,
                  customListingId         : threadData.customListingId    || null,
                  customListingUrl        : threadData.customListingUrl   || null,
                  acceptedQuoteUsd        : threadData.acceptedQuoteUsd   || null,
                  acceptedQuoteFamily     : threadData.acceptedQuoteFamily || null,
                  salesSynopsis           : threadData.salesSynopsis      || null,
                  salesCompletedAt        : threadData.salesCompletedAt   || null,
                  customListingSentAt     : threadData.customListingSentAt|| null,
                  // v4.3.8 — full SalesContext snapshot for dashboard tabs
                  salesContext            : salesContextSnapshot,
                  archivedAt              : FV.serverTimestamp(),
                  reason                  : "post_terminal_re_engagement"
                });
              archiveOutcome = salesContextSnapshot ? "archived_with_context" : "archived_listing_only";
              console.log(`[autoPipeline] archived round ${threadData.salesRound || 1} for ${threadId}: ${archiveOutcome} → ${archiveDocId}`);
            } catch (e) {
              // Archive failure is non-fatal — better to lose history
              // than to block the new round entirely. But surface it
              // in the audit log so it's debuggable.
              archiveOutcome = "archive_failed";
              archiveError   = e.message || String(e);
              console.warn(`[autoPipeline] salesHistory archive failed for ${threadId} (proceeding):`, e.message);
            }
          } else {
            console.log(`[autoPipeline] archive skipped for ${threadId} — no salesSynopsis or customListingId on prior round`);
          }

          // 2. Reset the thread doc fields. Use FV.delete() for fields
          //    that should not exist on a fresh thread, NOT null — the
          //    listing-creator cron's queries depend on field-presence
          //    semantics (e.g., customListingStartedAt missing = "no
          //    worker ran yet"; null would be a different state).
          await db.collection(THREADS_COLL).doc(threadId).update({
            status                       : "sales_active",
            salesRound                   : round,
            // Acceptance / quote
            customerAccepted             : FV.delete(),
            customerAcceptedAt           : FV.delete(),
            acceptedQuoteUsd             : FV.delete(),
            acceptedQuoteFamily          : FV.delete(),
            lastResolverResult           : FV.delete(),
            // Listing pipeline
            customListingStatus          : FV.delete(),
            customListingId              : FV.delete(),
            customListingUrl             : FV.delete(),
            customListingCreatedAt       : FV.delete(),
            customListingStartedAt       : FV.delete(),
            customListingAttempts        : FV.delete(),
            customListingError           : FV.delete(),
            customListingErrorAt         : FV.delete(),
            customListingErrorCount      : FV.delete(),
            customListingDraftCreatedAt  : FV.delete(),
            customListingImagesAt        : FV.delete(),
            customListingImagesCount     : FV.delete(),
            customListingUsedFallback    : FV.delete(),
            customListingSentAt          : FV.delete(),
            customListingSentListingId   : FV.delete(),
            customListingRetractedAt     : FV.delete(),
            // Completion markers (NOW archived)
            salesCompletedAt             : FV.delete(),
            salesSynopsis                : FV.delete(),
            // Operator-review flags from prior round
            needsOperatorReview          : FV.delete(),
            needsOperatorReviewReason    : FV.delete(),
            // Audit
            updatedAt                    : FV.serverTimestamp()
          });

          // 3. Reset SalesContext to a fresh discovery state.
          //
          // v4.3.10 — Critical: must explicitly wipe the funnel-state
          // fields, not just merge stage:"discovery". Earlier code did
          // a merge:true write that left accumulatedSpec, quoteHistory,
          // totalQuotedUsd, family, selectedCodes, _lastResolverResult,
          // etc. ALL intact from the prior round. The agent's new turn
          // then ran on a SalesContext that still had round-1's spec
          // data, producing notes like "New order: banana-themed
          // charm. Family TBD; prior order was a 10mm sterling silver
          // necklace" while the dashboard simultaneously showed the
          // OLD $42 / family:necklace / codes 1A-4A from round 1.
          // From the operator's perspective, round-1 data leaked into
          // the round-2 view.
          //
          // We use FieldValue.delete() (not null, not undefined) so
          // queries that check field presence — and the dashboard's
          // archive snapshot — see a clean state, identical to a fresh
          // round-1 SalesContext at thread creation.
          //
          // What we PRESERVE on the SalesContext doc (deliberately):
          //   - operatorOverrides : audit trail of manual stage flips
          //   - createdAt          : when the funnel first started for this thread
          //   - any custom telemetry fields the agent may have written
          //     (e.g. promptVersion) — those don't pollute the active
          //     funnel view.
          await db.collection("EtsyMail_SalesContext").doc(threadId).set({
            stage                 : "discovery",
            roundReset            : true,
            roundResetAt          : FV.serverTimestamp(),
            updatedAt             : FV.serverTimestamp(),
            // Wipe funnel-state fields explicitly:
            accumulatedSpec       : FV.delete(),
            quoteHistory          : FV.delete(),
            totalQuotedUsd        : FV.delete(),
            missingInputs         : FV.delete(),
            notes                 : FV.delete(),
            quantity              : FV.delete(),
            wantsRush             : FV.delete(),
            urgency_level         : FV.delete(),
            selectedCodes         : FV.delete(),
            family                : FV.delete(),
            _lastResolverResult   : FV.delete(),
            lastResolverResult    : FV.delete(),
            lastSalesAgentBlockReason: FV.delete(),
            lastDraftCustomOrderListing: FV.delete(),
            lastTurnAt            : FV.delete()
          }, { merge: true });

          // 3b. v4.3.6 — Sweep stale optimistic-ghost messages from prior
          //     rounds. Each successful send writes an `optim_<draftId>`
          //     doc into the thread's messages subcollection so the
          //     operator sees the just-sent message immediately while
          //     waiting for the next M2 scrape to pull in the real Etsy
          //     message. Once the real message arrives, the dashboard's
          //     dedup hides the ghost. But the ghost doc lives in
          //     Firestore forever.
          //
          //     On round-2 reset, the prior round's ghost doc has a
          //     timestamp from when it was written (Date.now()). The
          //     dashboard's chronological sort places it according to
          //     that timestamp — typically AFTER the customer's new
          //     inbound message, even though the corresponding real
          //     message was sent days earlier. Result: the operator
          //     sees the prior listing-URL message appearing as if it
          //     were a fresh send right after the customer's new
          //     request.
          //
          //     Fix: query the messages subcollection for docs where
          //     localOptimistic == true and delete them. Real M2-scraped
          //     outbound messages don't have this field, so they're
          //     untouched. Single-field equality query — no composite
          //     index needed.
          try {
            const msgsRef = db.collection(THREADS_COLL).doc(threadId).collection("messages");
            const ghostsSnap = await msgsRef
              .where("localOptimistic", "==", true)
              .limit(100)
              .get();
            if (!ghostsSnap.empty) {
              const batch = db.batch();
              ghostsSnap.docs.forEach(d => batch.delete(d.ref));
              await batch.commit();
              console.log(`[autoPipeline] round-${round} reset: cleaned up ${ghostsSnap.size} stale optimistic ghost(s) in ${threadId}`);
            }
          } catch (e) {
            // Non-fatal — worst case is a stale ghost stays visible
            // until next thread delete or manual cleanup. Don't block
            // the reset on this.
            console.warn(`[autoPipeline] ghost sweep failed for ${threadId} (proceeding):`, e.message);
          }

          // 4. Audit
          await writeAudit({
            threadId,
            eventType: "sales_round_reset",
            payload  : {
              round,
              priorRound       : threadData.salesRound || 1,
              priorListingId   : threadData.customListingId || null,
              intentConfidence : intentResp.confidence,
              triggeredBy      : "post_terminal_fresh_sales_lead",
              // v4.3.9 — archive diagnostics. Possible outcomes:
              //   "archived_with_context"  — full snapshot saved
              //   "archived_listing_only"  — SalesContext read failed but listing data saved
              //   "archive_failed"         — Firestore write threw (see archiveError)
              //   "skipped_no_data"        — neither salesSynopsis nor customListingId present
              archiveOutcome,
              archiveError
            }
          });

          console.log(`[autoPipeline] thread ${threadId} reset for sales round ${round}`);
        } catch (e) {
          // If any of the reset steps fail, fall through. The agent
          // would still hit its terminal guard and skip — surfacing
          // the message to the operator. Better than blocking.
          console.warn(`[autoPipeline] post-terminal reset failed for ${threadId} (falling through):`, e.message);
        }
      }

      if (activeSalesStage || freshSalesLead) {
        // Load latest inbound text + attachments in one pass. Even if
        // text was already loaded for the classifier, we still need the
        // attachment arrays so fresh sales leads with photos reach the
        // sales agent's vision path.
        const inb = await loadLatestInbound(threadId);
        let inboundText = latestText;
        if (inboundText === null) inboundText = inb.text;
        const inboundAttachments = inb.attachments;
        const threadReferenceAttachments = Array.isArray(inb.threadAttachments) && inb.threadAttachments.length
          ? inb.threadAttachments
          : inboundAttachments;

        // v2.3 — Pre-tool URL detection. Before the agent loop runs,
        // scan the inbound text for any Etsy listing URLs. If found,
        // proactively fetch the listing data from Etsy's API and inject
        // it into the agent's context summary as `referencedListings`.
        // The AI no longer has to "decide to look it up" — it sees the
        // structured data alongside the customer's message.
        //
        // Why proactive instead of letting the agent call the tool?
        // Latency. Pre-fetching here saves one round-trip in the agent
        // loop. Also makes the URL data available to the discovery
        // stage's prompt, which doesn't have the lookup tool in its
        // initial reasoning context until after the first turn.
        let referencedListings = [];
        if (typeof inboundText === "string" && inboundText.length > 12) {
          try {
            // Direct-import the parser + lookup. Falls back gracefully
            // if the new module isn't deployed yet.
            // v2.4: lookup helpers were folded into etsyMailListingsCatalog
            // (was etsyMailListingLookup). Import path updated; surface is
            // identical (findEtsyUrlsInText, lookupListingByUrl).
            const lookupMod = (() => {
              try { return require("./etsyMailListingsCatalog"); }
              catch (e) {
                console.warn("auto-pipeline: etsyMailListingsCatalog not deployed, skipping URL detection");
                return null;
              }
            })();
            if (lookupMod && typeof lookupMod.findEtsyUrlsInText === "function") {
              const urls = lookupMod.findEtsyUrlsInText(inboundText);
              if (urls.length > 0) {
                // Cap at 3 to avoid runaway API calls if the customer
                // pasted a list of 20 URLs. The AI can re-fetch others
                // on demand via the tool.
                const toFetch = urls.slice(0, 3);
                const lookups = await Promise.all(
                  toFetch.map(({ url }) =>
                    lookupMod.lookupListingByUrl({ url, threadId })
                      .catch(err => ({ found: false, reason: "LOOKUP_THREW", error: err.message }))
                  )
                );
                referencedListings = lookups.map((r, i) => ({
                  url: toFetch[i].url,
                  ...r
                }));
                console.log(`auto-pipeline: pre-fetched ${referencedListings.length} listing(s) referenced in inbound`);
              }
            }
          } catch (e) {
            // Fully-isolated try/catch so a URL-lookup failure NEVER
            // blocks the sales agent from running. Log + proceed.
            console.warn("auto-pipeline: URL pre-detection failed:", e.message);
          }
        }

        try {
          // v4.0: salesAgent runs as a BACKGROUND function. We invoke it
          // fire-and-forget (Netlify returns 202 immediately) and then
          // poll EtsyMail_Drafts/draft_<tid> for the agent's write. This
          // moves the function out from under Netlify's 26-second sync
          // invocation cap, eliminating the gateway-timeout class entirely.
          const draftId = "draft_" + threadId;
          const startedAt = Date.now();
          const PRIOR_UPDATED_MS = await (async () => {
            try {
              const dSnap = await db.collection("EtsyMail_Drafts").doc(draftId).get();
              if (!dSnap.exists) return 0;
              const u = dSnap.data().updatedAt;
              return u && u.toMillis ? u.toMillis() : 0;
            } catch { return 0; }
          })();

          // Fire-and-forget. Netlify auto-suffixes -background.js to the
          // URL when invoking. Don't await the response; the background
          // function returns 202 immediately.
          invokeBackgroundFunction("etsyMailSalesAgent-background", {
            threadId,
            latestInboundText        : inboundText,
            latestInboundAttachments : inboundAttachments,
            threadReferenceAttachments,
            referencedListings,
            customerHistory          : { isRepeat: false, orderCount: 0, lifetimeValueUsd: 0 },
            intentClassification     : intentResp ? intentResp.classification : null,
            intentConfidence         : intentResp ? intentResp.confidence : null,
            employeeName             : employeeName || "system:auto-pipeline",
            forceRegenerate          : !!forceRegenerate,
            bypassExistingDraft      : !!bypassExistingDraft,
            manualRunId              : manualRunId || null
          }).catch(e => {
            // Even the FIRE itself can fail (network, invalid endpoint).
            // Caught here so it doesn't reject the auto-pipeline turn.
            // The poll below will time out and the thread escalates.
            console.warn("salesAgent background invoke failed:", e.message);
          });

          // Poll for the draft to appear/update. Wait up to 5 minutes.
          // Most agent turns complete in 10-60s; 5min gives Opus headroom
          // even with multiple tool iterations.
          const POLL_INTERVAL_MS = 2000;
          const POLL_TIMEOUT_MS  = 5 * 60 * 1000;
          let agentCompleted = false;
          let draftDoc = null;
          while (Date.now() - startedAt < POLL_TIMEOUT_MS) {
            await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
            const dSnap = await db.collection("EtsyMail_Drafts").doc(draftId).get();
            if (dSnap.exists) {
              const d = dSnap.data();
              const u = d.updatedAt && d.updatedAt.toMillis ? d.updatedAt.toMillis() : 0;
              // The agent finished if the draft was written/updated
              // AFTER we kicked off this turn AND it was generated by
              // the sales agent (not a stale operator draft).
              const matchesManualRun = manualRunId && d.manualRunId === manualRunId;
              const freshWithoutRunId = !manualRunId && u > PRIOR_UPDATED_MS;
              if (d.generatedBySalesAgent === true && (matchesManualRun || freshWithoutRunId)) {
                agentCompleted = true;
                draftDoc = d;
                break;
              }
            }
          }

          if (!agentCompleted) {
            // Timed out waiting for the agent. Thread will sit until the
            // next inbound or operator intervention. The draft might still
            // arrive later (agent writes when it finishes), but auto-send
            // won't happen for this turn.
            await writeAudit({
              threadId,
              eventType: "sales_agent_engagement_failed",
              payload: {
                path: activeSalesStage ? "stateful" : "fresh_lead",
                error: "background agent did not write draft within poll timeout",
                timeoutMs: POLL_TIMEOUT_MS
              },
              outcome: "failure"
            });
            // Don't overwrite thread status — leave it as the claim set.
            // Operator sees no AI draft yet; can manually retry.
            return ok({
              threadId,
              decision: "sales_agent_timeout",
              path: activeSalesStage ? "stateful" : "fresh_lead",
              durationMs: Date.now() - tStart
            });
          }

          // Agent finished. Re-load the now-current thread doc since the
          // agent updated it (status: sales_active or pending_human_review).
          const tSnap = await threadRef.get();
          const threadAfter = tSnap.exists ? tSnap.data() : {};

          await writeAudit({
            threadId, draftId,
            eventType: "sales_agent_engaged",
            payload  : {
              path        : activeSalesStage ? "stateful" : "fresh_lead",
              intent      : intentResp,
              draftId,
              confidence  : draftDoc.aiConfidence || null,
              threadStatus: threadAfter.status || null,
              durationMs  : Date.now() - startedAt
            }
          });

          // ─── v4.0 — auto-send sales drafts ──────────────────────────
          // Stages no longer exist. The agent decides per-turn whether
          // its reply is auto-sendable via two flags it writes to the
          // draft and thread:
          //   - readyForHumanApproval: the agent wants a person to look
          //     before this ships (Quote-row escalation, customer
          //     accepted a quote and the operator needs to send a
          //     custom listing, anything genuinely off).
          //   - isNeedsReviewHandoff: the customer-facing reply was
          //     replaced with an operator synopsis; nothing to ship.
          //
          // If neither flag is set, the agent has produced a reply
          // safe to auto-send (subject to deterministic vetoes and
          // the kill-switch).
          // v4.3.16 — Acceptance turns are special. When the agent
          // detects customer_accepted, it marks aiDraftStatus as
          // "skipped_acceptance" because the listing-creator worker
          // will craft and send its own message ("Here's the custom
          // listing for your necklace: <url>"). If we auto-send the
          // agent's redundant "Got it, $79" text alongside, we get:
          //
          //   1. Two messages to the customer for one acceptance ("Got
          //      it $79" then "Here's the listing"). Mildly noisy.
          //
          //   2. Worse: a DRAFT_BUSY race. Agent's auto-send puts the
          //      shared draftId into status:"sending"; worker's URL
          //      enqueue then 409s because the same draftId is in
          //      flight. The worker retries with backoff but if the
          //      agent's send takes >31s (Etsy compose timeout is 60-
          //      90s), all retries fail and the worker's "send URL"
          //      step bails. The cron retries on the next minute tick
          //      and usually succeeds the second time, but the customer
          //      sees a delay and the dashboard shows FAILED briefly.
          //
          // Solution: skip auto-send for acceptance-turn drafts. The
          // worker handles outbound messaging from this point. The
          // agent's text reply remains in the dashboard as a
          // "skipped_acceptance" record but isn't shipped.
          const isAcceptanceSkip = draftDoc.aiDraftStatus === "skipped_acceptance";

          const safeToAutoSend =
            !draftDoc.readyForHumanApproval &&
            !draftDoc.isNeedsReviewHandoff &&
            !isAcceptanceSkip;

          if (isAcceptanceSkip) {
            await writeAudit({
              threadId, draftId,
              eventType: "sales_auto_send_skipped",
              payload  : { reason: "acceptance_turn_handled_by_listing_creator" }
            });
            await db.collection(THREADS_COLL).doc(threadId).set({
              lastAutoDecision  : "sales_acceptance_handed_to_worker",
              lastAutoDecisionAt: FV.serverTimestamp(),
              updatedAt         : FV.serverTimestamp()
            }, { merge: true });
          }

          if (autoCfg.salesAutoSendEnabled && safeToAutoSend) {
            try {
              const draftText = String(draftDoc.text || "").trim();
              const draftAttachments = Array.isArray(draftDoc.attachments) ? draftDoc.attachments : [];

              if (!draftText) {
                await writeAudit({
                  threadId, draftId,
                  eventType: "sales_auto_send_skipped",
                  payload  : { reason: "empty_draft_text" }
                });
              } else {
                // Same deterministic vetoes the support path uses, but
                // skip the "custom" pattern — sales mode exists to
                // handle custom-order inquiries. Other vetoes (refund,
                // cancel, legal, damaged, address, personalize,
                // missing, replace) still apply.
                //
                // v4.3.7 — Use loadCurrentInboundTextOnly (just the
                // single most recent inbound) rather than the 5-message
                // concatenation. Multi-turn context is wrong for vetoes
                // because old messages from the prior round can poison
                // auto-sends — Joanna's round-2 banana-charm draft was
                // vetoed because a phrase from the original baseball-
                // charm conversation hit the "missing" pattern.
                const inboundForVeto = await loadCurrentInboundTextOnly(threadId);
                const veto = applyDeterministicVetoes({
                  inboundText      : inboundForVeto,
                  draftText,
                  draftToolCalls   : [],   // no longer surfaced in v4
                  excludePatternIds: ["custom"]
                });
                const ks = await getKillSwitch();

                if (veto.vetoed) {
                  await writeAudit({
                    threadId, draftId,
                    eventType: "sales_auto_send_vetoed",
                    payload  : { reasons: veto.reasons }
                  });
                  // v4.3.7 — finalize the claim. Without this the
                  // thread sits in lastAutoDecision="in_progress"
                  // until the stale-claim reaper kicks in 5+ minutes
                  // later (we observed this in the audit trail). The
                  // operator's UI shows "AI thinking..." until then.
                  // Set a terminal decision so the rail reflects
                  // reality immediately.
                  await db.collection(THREADS_COLL).doc(threadId).set({
                    lastAutoDecision  : "sales_auto_send_vetoed",
                    lastAutoDecisionAt: FV.serverTimestamp(),
                    updatedAt         : FV.serverTimestamp()
                  }, { merge: true });
                } else if (ks.disabled) {
                  await writeAudit({
                    threadId, draftId,
                    eventType: "sales_auto_send_skipped",
                    payload  : { reason: "kill_switch_on" }
                  });
                  // v4.3.7 — finalize the claim (same reason as the
                  // veto branch above).
                  await db.collection(THREADS_COLL).doc(threadId).set({
                    lastAutoDecision  : "sales_auto_send_kill_switch",
                    lastAutoDecisionAt: FV.serverTimestamp(),
                    updatedAt         : FV.serverTimestamp()
                  }, { merge: true });
                } else {
                  // All clear — enqueue the send.
                  const etsyConversationUrl = threadAfter.etsyConversationUrl
                    || ("https://www.etsy.com/your/conversations/"
                       + (threadAfter.etsyConversationId || threadId.replace("etsy_conv_", "")));

                  // ━━━ v3.18: Block on pending tracking jobs (sales path) ━
                  // Same race as the main auto-send path — tracking image
                  // generation runs in a background function. Wait for any
                  // pending entries to finalize before enqueueing, otherwise
                  // the extension sends text-only with no attachment.
                  const trackingWait = await waitForTrackingJobs(draftAttachments);
                  const sendableAttachments = trackingWait.attachments;
                  if (!trackingWait.ok) {
                    await writeAudit({
                      threadId, draftId,
                      eventType: "sales_auto_send_tracking_unready",
                      payload  : {
                        timedOut: trackingWait.timedOut,
                        failed  : trackingWait.failed,
                        note    : "Sales auto-send aborted because one or " +
                                  "more tracking images did not finish " +
                                  "generating in time. Demoted to human review."
                      }
                    });
                    // Mirror the kill-switch pattern above — leave the
                    // thread for human review without enqueueing.
                    await db.collection(THREADS_COLL).doc(threadId).set({
                      lastAutoDecision  : "sales_auto_send_tracking_unready",
                      lastAutoDecisionAt: FV.serverTimestamp(),
                      updatedAt         : FV.serverTimestamp()
                    }, { merge: true });
                  } else {
                    await callFunction("etsyMailDraftSend", {
                      op                  : "enqueue",
                      threadId,
                      etsyConversationUrl,
                      text                : draftText,
                      attachments         : sendableAttachments,
                      employeeName        : employeeName || "system:auto-pipeline",
                      aiMeta              : {
                        generatedByAI         : true,
                        generatedBySalesAgent : true,
                        confidence            : draftDoc.aiConfidence || null,
                        model                 : draftDoc.aiModel || null
                      },
                      force               : true,
                      parentThreadFinalizePatch : {
                        threadId,
                        // v0.9.47 — sales-flow threads stay in
                        // sales_active throughout. The "AI auto-replied"
                        // visual signal comes from the rail pill
                        // (driven by lastAutoDecision), not from a
                        // status flip to auto_replied.
                        newStatus    : "sales_active",
                        inboundMs    : claim.inboundMs,
                        decision     : "sales_auto_send_enqueued",
                        aiConfidence : draftDoc.aiConfidence || null
                      }
                    });
                    await writeAudit({
                      threadId, draftId,
                      eventType: "sales_auto_send_enqueued",
                      payload  : {
                        confidence : draftDoc.aiConfidence || null,
                        textLen    : draftText.length,
                        attachCount: sendableAttachments.length
                      }
                    });
                  }
                  // ━━━ end v3.18 sales gate ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                }
              }
            } catch (autoSendErr) {
              console.warn("salesAgent auto-send failed (non-fatal):", autoSendErr.message);
              await writeAudit({
                threadId, draftId,
                eventType: "sales_auto_send_failed",
                payload  : { error: autoSendErr.message }
              });
            }
          } else if (autoCfg.salesAutoSendEnabled && !safeToAutoSend && !isAcceptanceSkip) {
            // Auto-send is on globally but the agent flagged this
            // turn for human review. Don't ship.
            // (acceptance-turn skip already audited above with its own
            // reason; don't double-emit here.)
            await writeAudit({
              threadId, draftId,
              eventType: "sales_auto_send_skipped",
              payload  : {
                reason: draftDoc.readyForHumanApproval
                  ? "ready_for_human_approval"
                  : "needs_review_handoff"
              }
            });
            // v4.3.7 — finalize the claim (see veto branch above).
            await db.collection(THREADS_COLL).doc(threadId).set({
              lastAutoDecision  : "sales_auto_send_human_review",
              lastAutoDecisionAt: FV.serverTimestamp(),
              updatedAt         : FV.serverTimestamp()
            }, { merge: true });
          }

          return ok({
            threadId,
            decision  : "sales_agent_handled",
            path      : activeSalesStage ? "stateful" : "fresh_lead",
            draftId,
            durationMs: Date.now() - tStart
          });

        } catch (salesErr) {
          // Wrapper failure — usually a Firestore read error during the
          // poll, since the agent itself runs in a background function
          // we can't await. The agent's own writes (draft, thread,
          // audit) happen independently of this orchestrator.
          console.warn("sales agent orchestration failed:", salesErr.message);

          await writeAudit({
            threadId,
            eventType: "sales_agent_engagement_failed",
            payload: {
              path      : activeSalesStage ? "stateful" : "fresh_lead",
              fromStage : activeSalesStage || null,
              error     : salesErr.message,
              statusCode: salesErr.status || null
            },
            outcome: "failure"
          });

          await db.collection(THREADS_COLL).doc(threadId).set({
            status                   : "pending_human_review",
            lastSalesAgentBlockReason: "AGENT_CALL_FAILED",
            updatedAt                : FV.serverTimestamp()
          }, { merge: true });

          return ok({
            threadId,
            decision  : "sales_agent_failed_escalated",
            path      : activeSalesStage ? "stateful" : "fresh_lead",
            error     : salesErr.message,
            durationMs: Date.now() - tStart
          });
        }
      }
    }

    // ─── 2. Generate AI draft ────────────────────────────────────
    // We always generate the draft, even when the auto-pipeline is
    // disabled in config. "Disabled" means "don't auto-send" — it
    // doesn't mean "don't think". An operator opening a Needs Review
    // thread should still see the AI's suggested reply alongside its
    // confidence score, ready to edit and send manually.
    //
    // The slow step — typically 10-60 seconds with Sonnet 4.6 + tool calls.
    let draftResp;
    try {
      draftResp = await callFunction("etsyMailDraftReply", {
        threadId,
        mode         : "initial",
        employeeName : employeeName || "system:auto-pipeline"
      });
    } catch (err) {
      await writeAudit({
        threadId, eventType: "auto_pipeline_failed",
        payload: { stage: "draft_generation", error: err.message }
      });
      // Thread is already in pending_human_review (from the claim) —
      // just record the failure on the thread doc so the UI shows it.
      await db.collection(THREADS_COLL).doc(threadId).set({
        aiDraftStatus     : "failed",
        lastAutoDecision  : "failed",
        lastAutoDecisionAt: FV.serverTimestamp(),
        updatedAt         : FV.serverTimestamp()
      }, { merge: true });
      return json(500, { error: "Draft generation failed: " + err.message, threadId });
    }

    const aiConfidence = (typeof draftResp.aiConfidence === "number") ? draftResp.aiConfidence
                       : (typeof draftResp.confidence   === "number") ? draftResp.confidence
                       : 0;
    const aiDifficulty = (typeof draftResp.aiDifficulty === "number") ? draftResp.aiDifficulty
                       : (typeof draftResp.difficulty   === "number") ? draftResp.difficulty
                       : null;
    const draftId      = draftResp.draftId || ("draft_" + threadId);
    const text         = draftResp.text || "";

    // ─── 3. Pipeline disabled? Route to human review with the draft ──
    // We DID generate the draft (above), we just don't auto-send it.
    if (!autoCfg.enabled) {
      await finalizeThread(threadId, {
        newStatus    : "pending_human_review",
        inboundMs    : claim.inboundMs,
        decision     : "human_review_pipeline_disabled",
        draftId,
        aiConfidence,
        aiDifficulty
      });
      await writeAudit({
        threadId, draftId,
        eventType: "auto_pipeline_routed_to_review",
        actor    : employeeName,
        payload  : {
          reason: "auto-pipeline disabled in config",
          aiConfidence, aiDifficulty,
          previousStatus: claim.previousStatus
        }
      });
      return ok({
        threadId,
        decision   : "human_review",
        reason     : "auto-pipeline disabled in config",
        aiConfidence,
        aiDifficulty,
        draftId,
        text,
        durationMs : Date.now() - tStart
      });
    }

    // ─── 4. Deterministic safety vetoes ──────────────────────────
    // Even with confidence ≥ threshold, certain customer requests must
    // never auto-send: refunds, cancellations, legal escalation,
    // damaged-item claims, address changes, custom orders, tool-call
    // errors. The veto check runs against the CURRENT INBOUND text
    // (what the customer actually said in THIS turn) and the OUTBOUND
    // draft (catches AI drafts that promise refunds even when the
    // inbound was cagey).
    //
    // v4.3.7 — Use loadCurrentInboundTextOnly (just the single most
    // recent inbound) rather than the 5-message concatenation.
    // Multi-turn lookback was a real bug source: a "missing" phrase
    // in a customer's earlier (now-resolved) message would veto every
    // subsequent auto-send for the rest of the conversation.
    const inboundText = await loadCurrentInboundTextOnly(threadId);
    const veto = applyDeterministicVetoes({
      inboundText,
      draftText      : text,
      draftToolCalls : draftResp.toolCalls
    });

    // ─── 5. Branch: auto-send vs human review ────────────────────
    const meetsThreshold = aiConfidence >= threshold;

    // Kill-switch: if global send is paused, we can still draft, but
    // can't auto-send. Force the route to human review with a clear note.
    const ks = await getKillSwitch();

    const decision = (meetsThreshold && !veto.vetoed && !ks.disabled && !dryRun)
      ? "auto_send"
      : "human_review";

    if (decision === "auto_send") {
      // Enqueue via the existing send pipeline. The Chrome extension
      // picks it up on its next peek (same path as a manual click).
      const tSnap = await threadRef.get();
      const thread = tSnap.exists ? tSnap.data() : {};
      const etsyConversationUrl = thread.etsyConversationUrl
        || ("https://www.etsy.com/your/conversations/"
           + (thread.etsyConversationId || threadId.replace("etsy_conv_", "")));

      // ━━━ v3.18: Block on pending tracking jobs ━━━━━━━━━━━━━━━━━━━━
      // If the AI included tracking images, they may still be in the
      // pending state when we arrive here (background generation
      // hasn't finished). Auto-sending now would deliver text-only.
      // Wait for them to complete; if any timeout or fail, demote to
      // human review and skip the enqueue. Mirrors the manual-Send
      // guard in etsy-mail-1.html v3.16/v3.17.
      let attachmentsForSend = Array.isArray(draftResp.attachments) ? draftResp.attachments : [];
      const trackingWait = await waitForTrackingJobs(attachmentsForSend);
      attachmentsForSend = trackingWait.attachments;
      if (!trackingWait.ok) {
        await writeAudit({
          threadId, draftId,
          eventType: "auto_pipeline_tracking_unready",
          payload: {
            timedOut: trackingWait.timedOut,
            failed  : trackingWait.failed,
            note    : "Auto-send aborted because one or more tracking " +
                      "images did not finish generating in time. " +
                      "Demoted to human review."
          }
        });
        await finalizeThread(threadId, {
          newStatus    : "pending_human_review",
          inboundMs    : claim.inboundMs,
          decision     : "human_review_after_tracking_unready",
          draftId,
          aiConfidence,
          aiDifficulty
        });
        // Skip the enqueue — operator will verify in the inbox UI.
        return;
      }
      // ━━━ end v3.18 gate ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

      try {
        // v1.5: atomic enqueue + thread finalize. Pass the finalize
        // patch as primitive fields; the enqueue op writes both the
        // draft AND the thread status in ONE Firestore transaction.
        // Pre-v1.5 this was two sequential writes — if the second
        // failed mid-flight, the draft would be queued (extension
        // sends it) but the thread would still be at pending_human_
        // review from the claim. The final folder placement was
        // wrong even though the customer got the right reply.
        await callFunction("etsyMailDraftSend", {
          op                  : "enqueue",
          threadId,
          etsyConversationUrl,
          text,
          attachments         : attachmentsForSend,
          employeeName,
          aiMeta              : {
            generatedByAI : true,
            model         : draftResp.model || null,
            reasoning     : draftResp.reasoning || null,
            activeQuestion: draftResp.activeQuestion || null,
            confidence    : aiConfidence,
            difficulty    : aiDifficulty
          },
          force               : true,
          parentThreadFinalizePatch : {
            threadId,
            newStatus    : "queued_for_auto_send",
            inboundMs    : claim.inboundMs,
            decision     : "auto_send_enqueued",
            aiConfidence,
            aiDifficulty
          }
        });
      } catch (err) {
        // If enqueue fails, fall back to human review — never silently
        // drop the AI's work. Thread is already at pending_human_review
        // from the claim; we just record the fallback.
        //
        // Note: because the enqueue txn is atomic, this catch block
        // means BOTH the draft enqueue AND the thread finalize were
        // rolled back — the thread is still at the claim's status
        // (pending_human_review). Calling finalizeThread here writes
        // the operator-facing fallback metadata (decision reason, etc.)
        // without changing the user-visible folder.
        await writeAudit({
          threadId, draftId, eventType: "auto_pipeline_enqueue_failed",
          payload: { error: err.message, errorCode: err.data && err.data.errorCode }
        });
        await finalizeThread(threadId, {
          newStatus    : "pending_human_review",
          inboundMs    : claim.inboundMs,
          decision     : "human_review_after_enqueue_failure",
          draftId,
          aiConfidence,
          aiDifficulty
        });
        return ok({
          threadId,
          decision    : "human_review",
          fallbackReason: "enqueue failed: " + err.message,
          aiConfidence,
          aiDifficulty,
          threshold,
          draftId,
          text,
          durationMs  : Date.now() - tStart
        });
      }

      // v1.5: thread was already promoted to queued_for_auto_send
      // atomically inside the enqueue transaction above. No separate
      // finalizeThread call here — pre-v1.5 there was, and a failure
      // between the two writes was the bug we just fixed. The audit
      // entry below still gets written.

      await writeAudit({
        threadId, draftId,
        eventType: "auto_pipeline_auto_sent",
        actor    : employeeName,
        payload  : {
          aiConfidence, aiDifficulty, threshold,
          model     : draftResp.model || null,
          textChars : text.length,
          attachmentCount: Array.isArray(draftResp.attachments) ? draftResp.attachments.length : 0
        }
      });

      return ok({
        threadId,
        decision  : "auto_send",
        aiConfidence,
        aiDifficulty,
        threshold,
        draftId,
        text,
        durationMs: Date.now() - tStart
      });
    }

    // ─── human_review branch ────────────────────────────────────
    // Determine the reason in priority order: vetoes first (most
    // important to surface to operators), then kill-switch, then
    // dryRun, then plain confidence-below-threshold.
    let fallbackReason;
    if (veto.vetoed) {
      fallbackReason = "deterministic veto: " + veto.reasons.join("; ");
    } else if (ks.disabled) {
      fallbackReason = "kill-switch active; not auto-sending";
    } else if (dryRun) {
      fallbackReason = "dryRun=true";
    } else {
      fallbackReason = `confidence ${aiConfidence.toFixed(2)} below threshold ${threshold.toFixed(2)}`;
    }

    await finalizeThread(threadId, {
      newStatus    : "pending_human_review",
      inboundMs    : claim.inboundMs,
      decision     : veto.vetoed ? "human_review_vetoed" : "human_review",
      draftId,
      aiConfidence,
      aiDifficulty
    });

    await writeAudit({
      threadId, draftId,
      eventType: "auto_pipeline_routed_to_review",
      actor    : employeeName,
      payload  : {
        reason     : fallbackReason,
        aiConfidence, aiDifficulty, threshold,
        vetoes     : veto.reasons,
        killSwitchDisabled: ks.disabled,
        model      : draftResp.model || null
      }
    });

    return ok({
      threadId,
      decision    : "human_review",
      reason      : fallbackReason,
      vetoes      : veto.reasons,
      aiConfidence,
      aiDifficulty,
      threshold,
      draftId,
      text,
      durationMs  : Date.now() - tStart
    });
  } catch (err) {
    console.error("etsyMailAutoPipeline error:", err);
    await writeAudit({
      threadId, eventType: "auto_pipeline_failed",
      payload: { error: err.message }
    }).catch(() => {});
    return json(500, { error: err.message || String(err) });
  }
};
