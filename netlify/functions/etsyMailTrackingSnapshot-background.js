/* netlify/functions/etsyMailTrackingSnapshot-background.js
 *
 * Background worker for the tracking-image feature.
 *
 * Netlify background functions:
 *   - 15-min execution limit (vs. 10-sec sync cap)
 *   - Always return 202 immediately to the caller
 *   - The client gets no response body; communication happens via
 *     side effects (in our case, Firestore job-status doc)
 *
 * Invocation pattern:
 *   1. Caller POSTs { trackingCode, jobId, forceRefresh? } to this endpoint
 *   2. Netlify immediately responds 202, queues the function
 *   3. Caller persists the jobId and polls Firestore for status updates
 *   4. This function does: fetch carrier data → render SVG → upload PNG
 *      → updates the job doc to status: "ready" (or "failed")
 *
 * The filename MUST end in "-background" — that's how Netlify identifies
 * background functions.
 *
 * Firestore job doc shape (at EtsyMail_TrackingJobs/{jobId}):
 *   {
 *     status     : "pending" | "running" | "ready" | "failed",
 *     trackingCode,
 *     startedAt  : Timestamp,
 *     finishedAt : Timestamp | null,
 *     error      : string | null,
 *     errorCode  : string | null,
 *     carrier, carrierDisplay, statusText, statusKey,
 *     imageUrl, imageStoragePath, imageWidth, imageHeight,
 *     events     : [...] (capped to 20)
 *   }
 */

console.log("[tracking-bg] Module loading...");

let admin, snapshot, db, FV;
let moduleLoadError = null;

try {
  admin = require("./firebaseAdmin");
  console.log("[tracking-bg] firebaseAdmin loaded");
} catch (e) {
  moduleLoadError = `firebaseAdmin: ${e.message}`;
  console.error("[tracking-bg] FAILED to load firebaseAdmin:", e.message);
  console.error(e.stack);
}

try {
  ({ snapshot } = require("./_etsyMailTracking"));
  console.log("[tracking-bg] _etsyMailTracking loaded");
} catch (e) {
  moduleLoadError = moduleLoadError || `_etsyMailTracking: ${e.message}`;
  console.error("[tracking-bg] FAILED to load _etsyMailTracking:", e.message);
  console.error(e.stack);
}

if (admin) {
  try {
    db = admin.firestore();
    FV = admin.firestore.FieldValue;
    console.log("[tracking-bg] Firestore initialized");
  } catch (e) {
    moduleLoadError = moduleLoadError || `admin.firestore: ${e.message}`;
    console.error("[tracking-bg] FAILED to init Firestore:", e.message);
  }
}

const JOBS_COLL = "EtsyMail_TrackingJobs";
const DRAFTS_COLL = "EtsyMail_Drafts";

async function updateJob(jobId, patch) {
  try {
    await db.collection(JOBS_COLL).doc(jobId).set({
      ...patch,
      updatedAt: FV.serverTimestamp()
    }, { merge: true });
  } catch (e) {
    console.error(`[tracking-bg] Failed to update job ${jobId}:`, e.message);
  }
}

// v3.27 — Write the ready tracking state back into the draft document
// that originated this job. Without this, drafts stay frozen at
// status:"pending" even after the job finishes. The inbox's
// pollTrackingJob would eventually pick it up — but only if someone has
// the inbox open. For drafts headed straight to auto-send (or for the
// chip to render on the next open), the persisted draft needs to match
// reality.
//
// Operates in a transaction so we don't race the operator if they're
// also editing the draft (e.g., toggling queuedForSend, adding listing
// suggestions). Best-effort: failure logs but doesn't fail the job.
async function draftWriteback(draftId, jobId, result) {
  if (!draftId) return;  // nothing to do — caller didn't supply a target
  if (!result) return;
  const draftRef = db.collection(DRAFTS_COLL).doc(draftId);
  try {
    await db.runTransaction(async (tx) => {
      const snap = await tx.get(draftRef);
      if (!snap.exists) {
        console.warn(`[tracking-bg] draftWriteback: draft ${draftId} not found — skipping`);
        return;
      }
      const draft = snap.data() || {};
      const trackingImages = Array.isArray(draft.trackingImages) ? draft.trackingImages : [];
      const attachments    = Array.isArray(draft.attachments)    ? draft.attachments    : [];

      // Find any entry whose jobId matches — could be in either or both
      // arrays. The draft is the authoritative source for what the
      // operator sees, so we update every reference.
      let mutated = false;
      const newTrackingImages = trackingImages.map(img => {
        if (img && img.jobId === jobId) {
          mutated = true;
          return {
            ...img,
            status           : "ready",
            carrier          : result.carrier,
            carrierDisplay   : result.carrierDisplay,
            statusText       : result.status,
            statusKey        : result.statusKey,
            estimatedDelivery: result.estimatedDelivery || null,
            destination      : result.destination || null,
            imageUrl         : result.imageUrl,
            imageStoragePath : result.imageStoragePath,
            imageWidth       : result.imageWidth,
            imageHeight      : result.imageHeight,
            eventCount       : (result.events || []).length,
            latestEvent      : (result.events && result.events[0]) || null
          };
        }
        return img;
      });
      const newAttachments = attachments.map(a => {
        if (a && a.jobId === jobId && a.type === "tracking_image") {
          mutated = true;
          return {
            ...a,
            status          : "ready",
            imageUrl        : result.imageUrl,
            imageStoragePath: result.imageStoragePath,
            imageWidth      : result.imageWidth,
            imageHeight     : result.imageHeight,
            carrier         : result.carrier,
            carrierDisplay  : result.carrierDisplay,
            statusKey       : result.statusKey,
            statusText      : result.status
          };
        }
        return a;
      });

      if (!mutated) {
        console.warn(`[tracking-bg] draftWriteback: draft ${draftId} has no entries referencing job ${jobId} — skipping (may have been edited)`);
        return;
      }

      tx.set(draftRef, {
        trackingImages: newTrackingImages,
        attachments   : newAttachments,
        updatedAt     : FV.serverTimestamp()
      }, { merge: true });
    });
    console.log(`[tracking-bg] draftWriteback: ${draftId} updated from job ${jobId}`);
  } catch (e) {
    console.error(`[tracking-bg] draftWriteback failed for ${draftId}:`, e.message);
    // Don't rethrow — the job itself completed successfully; the
    // writeback is a best-effort propagation.
  }
}

exports.handler = async (event) => {
  console.log("[tracking-bg] Handler invoked");

  // If module loading failed, we need to still try to write the error
  // to Firestore so the UI shows it.
  if (moduleLoadError && !db) {
    console.error("[tracking-bg] Cannot proceed — module load failed:", moduleLoadError);
    // Without admin loaded, we can't even write to Firestore. Just log.
    return { statusCode: 202 };
  }

  // Background funcs always return 202 to the caller; we don't return meaningful
  // status codes. But we still need to parse the payload.
  let body;
  try { body = JSON.parse(event.body || "{}"); }
  catch {
    console.error("[tracking-bg] Invalid JSON body");
    return { statusCode: 202 };
  }

  const trackingCode = String(body.trackingCode || "").trim();
  const jobId        = String(body.jobId || "").trim();
  const forceRefresh = Boolean(body.forceRefresh);
  const carrierHint  = String(body.carrierHint || "").trim().toLowerCase();
  // v3.27 — Optional draftId. If present, the worker mirrors the
  // ready tracking state back into EtsyMail_Drafts/{draftId} on
  // completion so drafts become eventually consistent without
  // requiring an inbox session to poll. See draftWriteback below.
  const draftId      = String(body.draftId || "").trim();

  console.log(`[tracking-bg] jobId=${jobId} trackingCode=${trackingCode} forceRefresh=${forceRefresh} draftId=${draftId || "(none)"}`);

  if (!trackingCode || !jobId) {
    console.error(`[tracking-bg] Missing trackingCode or jobId. trackingCode=${trackingCode} jobId=${jobId}`);
    return { statusCode: 202 };
  }

  // If modules partially loaded (db available but snapshot not), surface
  // that error to the client via the job doc
  if (moduleLoadError) {
    console.error(`[tracking-bg] Module load error prevents work:`, moduleLoadError);
    await updateJob(jobId, {
      status    : "failed",
      error     : `Server module load failed: ${moduleLoadError}`,
      errorCode : "MODULE_LOAD_FAILED",
      finishedAt: FV.serverTimestamp()
    });
    return { statusCode: 202 };
  }

  console.log(`[tracking-bg] Starting work for ${jobId}`);

  await updateJob(jobId, {
    status      : "running",
    trackingCode,
    startedAt   : FV.serverTimestamp(),
    error       : null,
    errorCode   : null
  });

  try {
    const result = await snapshot(trackingCode, { forceRefresh, carrierHint });

    await updateJob(jobId, {
      status           : "ready",
      trackingCode     : result.trackingCode,
      carrier          : result.carrier,
      carrierDisplay   : result.carrierDisplay,
      statusText       : result.status,
      statusKey        : result.statusKey,
      estimatedDelivery: result.estimatedDelivery || null,
      destination      : result.destination || null,
      origin           : result.origin || null,
      shipDate         : result.shipDate || null,
      resolvedAt       : result.resolvedAt || null,
      events           : (result.events || []).slice(0, 20),
      imageUrl         : result.imageUrl,
      imageStoragePath : result.imageStoragePath,
      imageWidth       : result.imageWidth,
      imageHeight      : result.imageHeight,
      cached           : result.cached,
      durationMs       : result.durationMs,
      finishedAt       : FV.serverTimestamp()
    });

    console.log(`[tracking-bg] Job ${jobId} ready (${result.durationMs}ms, cached=${result.cached})`);

    // v3.27 — Propagate the ready state to the originating draft.
    // Best-effort: writeback failure is logged but doesn't fail the
    // job (the job is already marked ready above; the inbox's poller
    // will reconcile on next render as a fallback).
    if (draftId) {
      await draftWriteback(draftId, jobId, result);
    }

  } catch (e) {
    console.error(`[tracking-bg] Job ${jobId} failed:`, e.code, e.message);
    console.error(e.stack);
    await updateJob(jobId, {
      status    : "failed",
      error     : e.message || "Unknown error",
      errorCode : e.code || "INTERNAL",
      finishedAt: FV.serverTimestamp()
    });
  }

  return { statusCode: 202 };
};
