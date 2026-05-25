/*  netlify/functions/etsyMailSalesReaper.js
 *
 *  v2.0 Step 2 — Soft-abandonment detector for sales conversations.
 *
 *  Runs every 6 hours (cron schedule in user's netlify.toml). Detects
 *  sales threads where:
 *    1. SalesContext.stage is in [discovery, spec, quote, revision]
 *       (i.e., not yet at pending_close_approval, not already abandoned)
 *    2. SalesContext.lastTurnAt is older than ABANDON_AFTER_DAYS days ago
 *
 *  …and marks them abandoned:
 *    - SalesContext.stage  = "abandoned"
 *    - SalesContext.abandonedAt = serverTimestamp()
 *    - parent thread.status = "sales_abandoned"
 *    - audit row: { eventType: "sales_abandoned", outcome: "success" }
 *
 *  ═══ DESIGN NOTES ══════════════════════════════════════════════════════
 *
 *  1. **Race-safe transaction.** A new customer reply could land between
 *     the reaper's read and write. The transaction re-reads
 *     SalesContext.lastTurnAt INSIDE the transaction and bails if it's
 *     fresher than the threshold. No false abandonments from races.
 *
 *  2. **Stage filter is enforced server-side.** Even if a thread somehow
 *     has a stage we don't expect (e.g., already "abandoned" from prior
 *     reaper run, or "pending_close_approval" — closed deals shouldn't
 *     be reaped), the per-thread transaction re-validates and skips.
 *
 *  3. **Cron auth bypass.** Same pattern as etsyMailAutoPipelineReaper:
 *     scheduled invocations identified via x-netlify-event header or
 *     "scheduled-event" body marker. Manual invocations require
 *     X-EtsyMail-Secret. This means the reaper can be triggered by an
 *     operator from curl/Postman for debugging without enabling the
 *     cron — useful during initial pilot.
 *
 *  4. **Bounded scan.** Reaper queries up to MAX_THREADS_PER_RUN active
 *     sales threads ordered by lastTurnAt asc. If a 7-day threshold
 *     gives us 1000+ candidates we're in a degenerate state and need
 *     operator attention; the scan caps and logs.
 *
 *  ═══ ENV VARS ══════════════════════════════════════════════════════════
 *
 *    ETSYMAIL_EXTENSION_SECRET            gates manual invocations
 *    ETSYMAIL_SALES_ABANDON_AFTER_DAYS    override; default 7
 *    ETSYMAIL_SALES_REAPER_MAX_THREADS    override; default 200
 */

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const { isScheduledInvocation } = require("./_etsyMailScheduled");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const SALES_COLL    = "EtsyMail_SalesContext";
const THREADS_COLL  = "EtsyMail_Threads";
const AUDIT_COLL    = "EtsyMail_Audit";
const CONFIG_COLL   = "EtsyMail_Config";

const ABANDON_AFTER_DAYS  = parseInt(process.env.ETSYMAIL_SALES_ABANDON_AFTER_DAYS || "7", 10);
const MAX_THREADS_PER_RUN = parseInt(process.env.ETSYMAIL_SALES_REAPER_MAX_THREADS || "200", 10);

// Stages that are eligible for abandonment. pending_close_approval
// is NOT in this list — those threads are deals waiting on operator
// approval, not stalled customer conversations. Step 3's approval flow
// handles those.
const REAPABLE_STAGES = new Set(["discovery", "spec", "quote", "revision"]);

// ─── Helpers ───────────────────────────────────────────────────────────

function json(statusCode, body) {
  return { statusCode, headers: { ...CORS, "Content-Type": "application/json" }, body: JSON.stringify(body) };
}

async function writeAudit({ threadId = null, eventType, actor = "system:salesReaper",
                            payload = {}, outcome = "success", ruleViolations = [] }) {
  try {
    await db.collection(AUDIT_COLL).add({
      threadId, draftId: null, eventType, actor, payload,
      createdAt: FV.serverTimestamp(),
      outcome, ruleViolations
    });
  } catch (e) {
    console.warn("salesReaper audit write failed:", e.message);
  }
}

/** Read salesModeEnabled from config. If sales mode is OFF, the reaper
 *  is a no-op — we don't want to mark threads abandoned in a system
 *  that's been rolled back. */
async function isSalesModeEnabled() {
  try {
    const doc = await db.collection(CONFIG_COLL).doc("autoPipeline").get();
    if (!doc.exists) return false;
    return doc.data().salesModeEnabled === true;
  } catch (e) {
    console.warn("salesReaper: config read failed:", e.message);
    return false;
  }
}

// ─── Per-thread reap (race-safe transaction) ───────────────────────────

/** Returns one of:
 *    { reaped: true, fromStage, lastTurnAtMs }
 *    { reaped: false, reason: "...", details? }
 */
async function reapOneThread(threadId, thresholdMs) {
  const ctxRef    = db.collection(SALES_COLL).doc(threadId);
  const threadRef = db.collection(THREADS_COLL).doc(threadId);

  return await db.runTransaction(async (tx) => {
    const ctxSnap = await tx.get(ctxRef);
    if (!ctxSnap.exists) {
      return { reaped: false, reason: "context_missing" };
    }
    const ctx = ctxSnap.data() || {};

    // Re-validate INSIDE the transaction (race protection)
    if (!REAPABLE_STAGES.has(ctx.stage)) {
      return { reaped: false, reason: "stage_not_reapable", stage: ctx.stage };
    }

    const lastTurnMs = ctx.lastTurnAt && ctx.lastTurnAt.toMillis ? ctx.lastTurnAt.toMillis() : 0;
    if (lastTurnMs >= thresholdMs) {
      return { reaped: false, reason: "fresh", lastTurnMs, thresholdMs };
    }

    // Reap.
    tx.set(ctxRef, {
      stage      : "abandoned",
      abandonedAt: FV.serverTimestamp(),
      lastSalesAgentBlockReason: null   // clear any stale block flag
    }, { merge: true });

    tx.set(threadRef, {
      status   : "sales_abandoned",
      salesStage: "abandoned",
      updatedAt: FV.serverTimestamp()
    }, { merge: true });

    return {
      reaped: true,
      fromStage: ctx.stage,
      lastTurnAtMs: lastTurnMs
    };
  });
}

// ─── Main scan ─────────────────────────────────────────────────────────

async function runReaperScan() {
  if (!(await isSalesModeEnabled())) {
    return { ok: true, skipped: true, reason: "sales_mode_disabled" };
  }

  const tStart = Date.now();
  const thresholdMs = Date.now() - (ABANDON_AFTER_DAYS * 24 * 60 * 60 * 1000);
  const thresholdTs = admin.firestore.Timestamp.fromMillis(thresholdMs);

  // Query candidates: SalesContext where stage is in REAPABLE_STAGES
  // AND lastTurnAt is older than threshold. Firestore doesn't support
  // "stage IN [...] AND lastTurnAt < X" without a composite index for
  // each stage — instead, we use a single < filter on lastTurnAt and
  // do the stage check per-document. With MAX_THREADS_PER_RUN cap and
  // a 6-hour cron, this is small enough to scan cheaply.
  let query = db.collection(SALES_COLL)
    .where("lastTurnAt", "<", thresholdTs)
    .orderBy("lastTurnAt", "asc")
    .limit(MAX_THREADS_PER_RUN);

  let snap;
  try {
    snap = await query.get();
  } catch (e) {
    // If the index doesn't exist yet, Firestore returns an actionable
    // error message with a URL to create it. Surface that explicitly.
    if (/index/i.test(e.message)) {
      console.error("salesReaper: composite index required.", e.message);
      await writeAudit({
        eventType: "sales_reaper_index_missing",
        payload: { error: e.message }, outcome: "failure",
        ruleViolations: ["MISSING_FIRESTORE_INDEX"]
      });
      return { ok: false, error: "Missing Firestore index — see function logs", needsIndex: true };
    }
    throw e;
  }

  if (snap.empty) {
    return { ok: true, scanned: 0, reaped: 0, durationMs: Date.now() - tStart };
  }

  let reapedCount = 0;
  const reapedThreads = [];
  const skipped = [];

  for (const doc of snap.docs) {
    const threadId = doc.id;
    const ctxData = doc.data() || {};

    // Pre-filter on stage (avoid transactions on threads that aren't
    // in a reapable stage anyway — saves Firestore ops).
    if (!REAPABLE_STAGES.has(ctxData.stage)) {
      skipped.push({ threadId, reason: "stage_not_reapable", stage: ctxData.stage });
      continue;
    }

    try {
      const result = await reapOneThread(threadId, thresholdMs);
      if (result.reaped) {
        reapedCount++;
        reapedThreads.push({
          threadId,
          fromStage: result.fromStage,
          lastTurnAtMs: result.lastTurnAtMs,
          ageDays: Math.round((Date.now() - result.lastTurnAtMs) / (24 * 60 * 60 * 1000))
        });
        await writeAudit({
          threadId, eventType: "sales_abandoned",
          payload: {
            fromStage      : result.fromStage,
            lastTurnAtMs   : result.lastTurnAtMs,
            ageDays        : Math.round((Date.now() - result.lastTurnAtMs) / (24 * 60 * 60 * 1000)),
            abandonAfterDays: ABANDON_AFTER_DAYS
          }
        });
      } else {
        skipped.push({ threadId, reason: result.reason });
      }
    } catch (e) {
      console.warn(`salesReaper: thread ${threadId} reap failed:`, e.message);
      skipped.push({ threadId, reason: "transaction_error", error: e.message });
    }
  }

  // Summary audit row, only if anything happened or we hit the cap.
  if (reapedCount > 0 || snap.size >= MAX_THREADS_PER_RUN) {
    await writeAudit({
      eventType: "sales_reaper_scan_complete",
      payload: {
        scanned         : snap.size,
        reaped          : reapedCount,
        capacityHit     : snap.size >= MAX_THREADS_PER_RUN,
        abandonAfterDays: ABANDON_AFTER_DAYS,
        thresholdMs,
        reapedSample    : reapedThreads.slice(0, 10),
        durationMs      : Date.now() - tStart
      }
    });
  }

  return {
    ok        : true,
    scanned   : snap.size,
    reaped    : reapedCount,
    skipped   : skipped.length,
    capacityHit: snap.size >= MAX_THREADS_PER_RUN,
    reapedThreads,
    durationMs: Date.now() - tStart
  };
}

// ─── Handler ───────────────────────────────────────────────────────────

// v0.9.47 — Sales abandonment removed from the system per operator
// policy. The reaper is a no-op now; it returns immediately without
// touching any threads. The function is left in place so cron
// schedules and existing audit-log queries don't break, and so the
// behavior can be re-enabled by removing this short-circuit if the
// policy changes back. Operators manually archive stale sales threads
// from the Sales — Active folder.
const REAPER_DISABLED = true;

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  // Cron invocations bypass the extension secret; manual invocations do not.
  const scheduled = isScheduledInvocation(event);

  if (!scheduled) {
    const auth = requireExtensionAuth(event);
    if (!auth.ok) return auth.response;
  }

  if (REAPER_DISABLED) {
    return json(200, {
      ok: true,
      disabled: true,
      message: "salesReaper is disabled — abandonment removed from system policy."
    });
  }

  try {
    const result = await runReaperScan();
    return json(result.ok === false ? 500 : 200, result);
  } catch (err) {
    console.error("salesReaper unhandled error:", err);
    await writeAudit({
      eventType: "sales_reaper_unhandled_error",
      payload: { error: err.message, stack: err.stack ? err.stack.slice(0, 1000) : null },
      outcome: "failure"
    });
    return json(500, { error: err.message || String(err) });
  }
};

// Exposed for tests / manual debugging.
module.exports.runReaperScan  = runReaperScan;
module.exports.reapOneThread  = reapOneThread;
module.exports.REAPABLE_STAGES = Array.from(REAPABLE_STAGES);
