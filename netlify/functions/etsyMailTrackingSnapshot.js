/* netlify/functions/etsyMailTrackingSnapshot.js
 *
 * FAST enqueue endpoint (<2 sec).
 *
 *   POST /.netlify/functions/etsyMailTrackingSnapshot
 *   Body: { trackingCode, carrierHint?, forceRefresh? }
 *
 *   Response (200):
 *     {
 *       ok           : true,
 *       jobId        : "trk_1234567890abc",
 *       trackingCode : "...",
 *       status       : "pending",  // poll EtsyMail_TrackingJobs/{jobId}
 *       pollEndpoint : "firestoreProxy?op=get&coll=EtsyMail_TrackingJobs&id=trk_..."
 *     }
 *
 * This endpoint does three things:
 *
 *   1. Check Firestore cache (EtsyMail_TrackingCache/{code}). If a recent
 *      cached snapshot exists, return it INLINE with status: "ready" so the
 *      UI renders it instantly. No background job needed.
 *
 *   2. If cache miss: writes a job doc with status: "pending", fires the
 *      background function (POST, don't await response), returns the jobId.
 *      Client polls Firestore for status: "ready" | "failed".
 *
 *   3. The background function writes scan events + imageUrl to the same
 *      job doc when done.
 *
 * Cache hits are synchronous (fast path). Cache misses use background +
 * polling (slow path, but reliable regardless of Apify latency).
 */

const admin        = require("./firebaseAdmin");
const crypto       = require("crypto");
const fetch        = require("node-fetch");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const JOBS_COLL  = "EtsyMail_TrackingJobs";
const CACHE_COLL = "EtsyMail_TrackingCache";

const CORS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, X-EtsyMail-Secret",
  "Access-Control-Allow-Methods": "POST, GET, OPTIONS"
};

const json = (statusCode, body) => ({
  statusCode,
  headers: { ...CORS, "Content-Type": "application/json" },
  body   : JSON.stringify(body)
});

// ─── Cache TTL by status key (mirrors _etsyMailTracking.js) ──────────────
const TTL_MINUTES = {
  delivered       : Infinity,
  exception       : 60,
  out_for_delivery: 10,
  in_transit      : 15,
  pre_shipment    : 120,
  returned        : 60,
  rerouted        : 30,
  unknown         : 15
};

function isCacheFresh(cachedAt, statusKey) {
  if (!cachedAt) return false;
  const ttl = TTL_MINUTES[statusKey] ?? 15;
  if (ttl === Infinity) return true;
  const cachedMs = cachedAt.toMillis ? cachedAt.toMillis() : new Date(cachedAt).getTime();
  const ageMin   = (Date.now() - cachedMs) / 60000;
  return ageMin < ttl;
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  let trackingCode, carrierHint, forceRefresh, draftId;

  if (event.httpMethod === "GET") {
    const q = event.queryStringParameters || {};
    trackingCode = String(q.trackingCode || q.code || "").trim();
    carrierHint  = String(q.carrier || "").trim().toLowerCase();
    forceRefresh = q.refresh === "1" || q.refresh === "true";
    // v3.27 — Optional draftId: if provided, the background worker
    // will write the ready tracking state back to this draft when
    // the image finishes. See draftWriteback in the background fn.
    draftId      = String(q.draftId || "").trim();
  } else if (event.httpMethod === "POST") {
    let body;
    try { body = JSON.parse(event.body || "{}"); }
    catch { return json(400, { error: "Invalid JSON body" }); }
    trackingCode = String(body.trackingCode || body.code || "").trim();
    carrierHint  = String(body.carrierHint || body.carrier || "").trim().toLowerCase();
    forceRefresh = Boolean(body.forceRefresh);
    draftId      = String(body.draftId || "").trim();
  } else {
    return json(405, { error: "Method not allowed" });
  }

  if (!trackingCode) {
    return json(400, { error: "Missing trackingCode" });
  }

  // ─── 1. Cache check (inline fast path) ──
  if (!forceRefresh) {
    try {
      const snap = await db.collection(CACHE_COLL).doc(trackingCode).get();
      if (snap.exists) {
        const c = snap.data();
        if (c.imageUrl && isCacheFresh(c.cachedAt, c.statusKey)) {
          return json(200, {
            ok              : true,
            status          : "ready",
            trackingCode,
            carrier         : c.carrier,
            carrierDisplay  : c.carrierDisplay,
            statusText      : c.status,
            statusKey       : c.statusKey,
            estimatedDelivery: c.estimatedDelivery || null,
            destination     : c.destination || null,
            events          : c.events || [],
            imageUrl        : c.imageUrl,
            imageStoragePath: c.imageStoragePath,
            imageWidth      : c.imageWidth,
            imageHeight     : c.imageHeight,
            cached          : true,
            inline          : true   // UI flag: don't poll, we already have it
          });
        }
      }
    } catch (e) {
      console.warn(`[trackingSnapshot] cache read failed:`, e.message);
      // Continue to enqueue path
    }
  }

  // ─── 2. Enqueue: create job doc + fire background function ──
  const jobId = "trk_" + crypto.randomBytes(8).toString("hex");

  try {
    await db.collection(JOBS_COLL).doc(jobId).set({
      jobId,
      trackingCode,
      status     : "pending",
      createdAt  : FV.serverTimestamp(),
      updatedAt  : FV.serverTimestamp(),
      forceRefresh: Boolean(forceRefresh),
      carrierHint : carrierHint || null,
      // v3.27 — draftId reference (if caller supplied). Used by the
      // background worker on completion to mirror the ready state
      // back into EtsyMail_Drafts/{draftId}.trackingImages so drafts
      // become eventually consistent without requiring an inbox
      // session to be open and polling.
      draftId     : draftId || null
    });
  } catch (e) {
    return json(500, { error: `Failed to create job: ${e.message}`, trackingCode });
  }

  // Fire background worker. We CAN'T just `fetch().catch(...)` and return —
  // Netlify Functions kill the Node process the moment the handler returns,
  // and a fetch that hasn't been flushed yet gets terminated with it. The
  // TCP connection might not even open. Result: the job stays at "pending"
  // forever and the UI spinner runs until its 3-min timeout.
  //
  // Fix: AWAIT the fetch initiation just long enough for the request to be
  // sent and accepted by Netlify. Background functions return 202 quickly
  // (typically <500ms), so we cap our wait at 5s via AbortController. Past
  // 5s we abort and fall through, marking the job failed so the UI gives
  // up cleanly.
  const host = event.headers?.host || event.headers?.Host || process.env.URL;
  const scheme = (event.headers?.["x-forwarded-proto"] || "https").split(",")[0];
  const bgUrl = `${scheme}://${host}/.netlify/functions/etsyMailTrackingSnapshot-background`;

  const controller = new AbortController();
  const abortTimeout = setTimeout(() => controller.abort(), 5000);

  try {
    const bgResp = await fetch(bgUrl, {
      method : "POST",
      headers: { "Content-Type": "application/json" },
      // v3.27 — Forward draftId so the worker can write the ready
      // state back to the originating draft when generation completes.
      body   : JSON.stringify({ jobId, trackingCode, carrierHint, forceRefresh, draftId: draftId || null }),
      signal : controller.signal
    });
    clearTimeout(abortTimeout);

    // Background functions return 202 immediately — that's our success
    // signal. Anything else means Netlify rejected the invocation.
    if (bgResp.status !== 202) {
      console.error(`[trackingSnapshot] Background returned ${bgResp.status} (expected 202)`);
      const bodyText = await bgResp.text().catch(() => "");
      await db.collection(JOBS_COLL).doc(jobId).set({
        status   : "failed",
        error    : `Background invocation rejected: HTTP ${bgResp.status}${bodyText ? ` — ${bodyText.slice(0, 200)}` : ""}`,
        errorCode: "BG_INVOKE_REJECTED",
        updatedAt: FV.serverTimestamp()
      }, { merge: true }).catch(() => {});

      return json(500, {
        error: `Background trigger rejected: HTTP ${bgResp.status}`,
        jobId
      });
    }
    // 202 received — background worker has accepted the job. The actual
    // processing happens asynchronously; client polls Firestore for status.
  } catch (e) {
    clearTimeout(abortTimeout);
    const wasAborted = e.name === "AbortError";
    const reason = wasAborted
      ? "Background trigger timed out after 5s"
      : `Background trigger network error: ${e.message}`;
    console.error(`[trackingSnapshot] ${reason}`);
    await db.collection(JOBS_COLL).doc(jobId).set({
      status   : "failed",
      error    : reason,
      errorCode: wasAborted ? "BG_TRIGGER_TIMEOUT" : "BG_TRIGGER_FAILED",
      updatedAt: FV.serverTimestamp()
    }, { merge: true }).catch(() => {});

    return json(500, { error: reason, jobId });
  }

  return json(200, {
    ok          : true,
    jobId,
    trackingCode,
    status      : "pending",
    pollEndpoint: `/.netlify/functions/firestoreProxy?op=get&coll=${JOBS_COLL}&id=${jobId}`,
    inline      : false
  });
};
