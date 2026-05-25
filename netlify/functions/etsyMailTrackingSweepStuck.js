/* netlify/functions/etsyMailTrackingSweepStuck.js
 *
 * Scheduled sweeper that catches tracking jobs left in `pending` or
 * `running` state for too long and marks them `failed`. This is
 * defense-in-depth against:
 *
 *   - Background fire-and-forget that never reached Netlify (job stays
 *     at `pending` with no `startedAt`)
 *   - Background function that started, hit Netlify's 15-min cap, and
 *     got hard-killed before writing its failure status (job stays at
 *     `running` with `startedAt` but no `finishedAt`)
 *
 * Without this, the inbox UI's 3-minute polling timeout DOES fire and
 * shows "Tracking lookup failed" — but the underlying Firestore doc
 * stays in a wrong state forever, polluting the cache and confusing
 * any later operator inspections.
 *
 * Schedule: every 5 minutes (declared in netlify.toml). Runs cheap —
 * one scoped query, processes results in a single batch.
 *
 * Thresholds:
 *   - `pending` for > 2 minutes → failed (BG_TRIGGER_LOST)
 *     2 min is plenty: the background function should set status="running"
 *     within ~1-2s of being invoked. Anything past 2 min means it never ran.
 *   - `running` for > 16 minutes → failed (BG_TIMEOUT)
 *     Netlify caps background functions at 15 min, so 16 min is the
 *     "definitely dead" threshold.
 *
 * Returns a summary; safe to invoke manually for testing.
 */

"use strict";

const admin = require("./firebaseAdmin");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const JOBS_COLL = "EtsyMail_TrackingJobs";

const PENDING_MAX_MS = 2  * 60 * 1000;   // 2 min
const RUNNING_MAX_MS = 16 * 60 * 1000;   // 16 min — past Netlify's 15-min cap

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

async function sweepBatch({ field, status, maxAgeMs, errorCode, errorMessage, batchLimit = 50 }) {
  const cutoff = admin.firestore.Timestamp.fromMillis(Date.now() - maxAgeMs);

  // Query: status === <status> AND <field> < cutoff
  // Note: we sort by <field> ascending to get the OLDEST stuck jobs first.
  // Using an inequality on a single field doesn't require a composite index.
  const snap = await db.collection(JOBS_COLL)
    .where("status", "==", status)
    .where(field, "<", cutoff)
    .orderBy(field, "asc")
    .limit(batchLimit)
    .get();

  if (snap.empty) return { swept: 0 };

  // Use a batch write — efficient for up to 500 docs, we cap at 50.
  const batch = db.batch();
  for (const doc of snap.docs) {
    batch.set(doc.ref, {
      status   : "failed",
      error    : errorMessage,
      errorCode,
      finishedAt: FV.serverTimestamp(),
      updatedAt : FV.serverTimestamp(),
      sweptAt   : FV.serverTimestamp()
    }, { merge: true });
  }
  await batch.commit();
  return { swept: snap.size, jobIds: snap.docs.map(d => d.id) };
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  const result = {
    pending: { swept: 0, jobIds: [] },
    running: { swept: 0, jobIds: [] },
    durationMs: 0,
    errors: []
  };
  const tStart = Date.now();

  // Sweep pending: created too long ago and never transitioned to running
  try {
    const r = await sweepBatch({
      field       : "createdAt",
      status      : "pending",
      maxAgeMs    : PENDING_MAX_MS,
      errorCode   : "BG_TRIGGER_LOST",
      errorMessage: "Background worker never started — fire-and-forget invocation lost. Likely a Netlify cold-start or fetch-flush race."
    });
    result.pending = r;
  } catch (e) {
    console.error("[trackingSweep] pending sweep failed:", e.message);
    result.errors.push(`pending: ${e.message}`);
  }

  // Sweep running: started but exceeded Netlify's 15-min background limit
  try {
    const r = await sweepBatch({
      field       : "startedAt",
      status      : "running",
      maxAgeMs    : RUNNING_MAX_MS,
      errorCode   : "BG_TIMEOUT",
      errorMessage: "Background worker exceeded Netlify's 15-min execution limit and was hard-killed."
    });
    result.running = r;
  } catch (e) {
    console.error("[trackingSweep] running sweep failed:", e.message);
    result.errors.push(`running: ${e.message}`);
  }

  result.durationMs = Date.now() - tStart;
  console.log(`[trackingSweep] complete: pending=${result.pending.swept} running=${result.running.swept} ms=${result.durationMs}`);
  return json(200, result);
};
