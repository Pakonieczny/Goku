/*  netlify/functions/etsyMailReapers.js
 *
 *  v2.5 — Consolidated reaper. Runs four reaper passes on each
 *  invocation:
 *
 *    1. AUTO_PIPELINE_CLAIMS — clear stale `in_progress` markers from
 *       crashed pipeline runs (was: etsyMailAutoPipelineReaper).
 *    2. SEND_QUEUE          — fail/abandon stranded queued + sending
 *       drafts (was: etsyMailSendQueueReaper).
 *    3. SALES_FUNNELS       — mark abandoned sales conversations
 *       (was: etsyMailSalesReaper).
 *    4. GMAIL_SCRAPE        — recover stuck Gmail-watcher → scrape jobs
 *       (v2.5; closes the loop on threads that get stranded at
 *       customerName="Unknown" because the Chrome extension wasn't
 *       running, crashed mid-claim, or scraped an Etsy page that no
 *       longer rendered the customer name).
 *
 *  ═══ WHY ONE FILE ════════════════════════════════════════════════════
 *
 *  Previously three separate scheduled functions, each on a different
 *  cron cadence (5 min / 5 min / 6 h). Consolidating reduces deploy
 *  surface and audit noise. Each pass is independently bounded
 *  (MAX_REAP_PER_RUN_*) and short-circuits when there's nothing to do,
 *  so running all passes on the most aggressive cadence (5 min) costs
 *  ~one indexed Firestore query per reaper-with-zero-work — negligible.
 *
 *  Sales-funnel scan would otherwise run 72× more often (every 5 min vs
 *  every 6 h). To keep query volume sane, the sales-funnel pass uses an
 *  internal time-gate (lastSalesScanAt in EtsyMail_Config/reaperState)
 *  so it ACTUALLY runs only once per SALES_SCAN_INTERVAL_MS. The other
 *  passes run on every invocation.
 *
 *  v3.1 — sub-sweep D: defensive Unicode unmangler. Repairs threads
 *  where the Chrome scraper stored literal `\uXXXX` escape sequences
 *  in customerName / subject (e.g. "Caitr\u00edona" instead of
 *  "Caitríona") by JSON-stringifying its inputs upstream. Snapshot
 *  ingest now decodes on the way in; this sub-sweep cleans rows that
 *  predate that fix. Idempotent — once a row is clean, it's skipped
 *  on every subsequent pass.
 *
 *  ═══ v2.5 ADDITION — gmail_scrape pass ═══════════════════════════════
 *
 *  v2.5.1 NOTE — INDEX-FREE QUERIES: All sub-sweep queries here use
 *  single-field equality only. Multi-field inequality + orderBy queries
 *  would be more efficient (Firestore could prune server-side) but they
 *  require composite indexes that have to be provisioned out-of-band in
 *  Firebase. Without those indexes the queries throw and the entire
 *  pass silently fails — exactly the symptom that surfaced in the
 *  field. We over-fetch and filter client-side instead; the volumes
 *  involved (claimed scrape jobs, detected_from_gmail threads,
 *  customerName="Unknown" threads) are all small enough that the
 *  client-side filter is fine.
 *
 *  Three sub-sweeps, run in order on every invocation:
 *
 *    Sub-sweep A — Stuck claimed scrape jobs
 *      EtsyMail_Jobs where jobType=="scrape" AND status=="claimed" AND
 *      claimedAt < now - SCRAPE_STUCK_CLAIM_MS. Revert to "queued" so the
 *      extension picks them up next poll. After MAX_SCRAPE_ATTEMPTS the
 *      job goes to "failed" instead, breaking any infinite-retry loop.
 *
 *    Sub-sweep B — detected_from_gmail threads with no live job
 *      EtsyMail_Threads where status=="detected_from_gmail" AND createdAt
 *      < now - SCRAPE_DETECTED_GRACE_MS. For each:
 *        (1) If customerName is still "Unknown", try to fill it from the
 *            email subject (Etsy notification subjects always carry the
 *            buyer's name — see extractCustomerNameFromSubject). This is
 *            independent of the job recovery; it makes the inbox useful
 *            even when the extension is permanently down.
 *        (2) Look up the deterministic gmail_<gmailMessageId> job: if
 *            missing or "failed", enqueue a fresh job; if "queued"/
 *            "claimed"/"succeeded", leave alone. After MAX_SCRAPE_ATTEMPTS
 *            the thread is tagged "scrape_exhausted" for operator follow-up.
 *
 *    Sub-sweep C — Successful scrape but customerName=="Unknown"
 *      EtsyMail_Threads where customerName=="Unknown" AND status is
 *      post-scrape (etsy_scraped, ai_drafted, etc) AND
 *      _unknownRetryAttempted!==true AND lastSyncedAt is older than
 *      SCRAPE_UNKNOWN_GRACE_MS. Two-step recovery:
 *        (1) Try the subject-fill first (cheap, no extension needed).
 *            If it succeeds, consume the one-shot guard and skip the
 *            rescrape — the thread is now labeled correctly.
 *        (2) Otherwise, set _unknownRetryAttempted=true under a
 *            transaction BEFORE enqueueing the rescrape job, so two
 *            reaper invocations racing can never produce duplicate
 *            retries. One rescrape per thread, ever.
 *
 *  The subject-fill paths in (B1) and (C1) are the high-impact recovery:
 *  Etsy notification emails always carry the customer's name in the
 *  subject ("Re: Etsy Conversation with <NAME>"), so a thread can be
 *  labeled correctly without any Etsy roundtrip — useful when the
 *  Chrome extension is offline, the operator's session has expired, or
 *  Etsy's DOM has shifted out from under the scraper.
 *
 *  Why this lives here, not in a standalone reaper file:
 *    The "consolidated reaper" pattern is the existing convention in
 *    this codebase (see "WHY ONE FILE" above). A separate
 *    etsyMailGmailScrapeReaper.js would mean another scheduled
 *    function, another netlify.toml entry, another set of audit-row
 *    actor strings to filter on. The gmail_scrape pass costs one
 *    indexed query per sub-sweep when idle — same as every other pass
 *    here — so consolidation is essentially free.
 *
 *  ═══ INVOCATION ════════════════════════════════════════════════════
 *
 *  Scheduled cron:        netlify.toml schedule (every 5 minutes)
 *  Manual full sweep:     POST /.netlify/functions/etsyMailReapers
 *  Manual single pass:    POST { op: "auto_pipeline" | "send_queue" |
 *                                    "sales_funnels" | "gmail_scrape" }
 *  Force sales pass now:  POST { op: "sales_funnels", force: true }
 *
 *  Manual invocations require X-EtsyMail-Secret. Scheduled invocations
 *  bypass auth (Netlify scheduler is the authority).
 */

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth, isScheduledInvocation } = require("./_etsyMailAuth");
// v3.30 — node-fetch required by runDeferredAutoPipelinePass to invoke
// the auto-pipeline background function for threads whose quiet period
// has elapsed. Every other pass operates on Firestore directly; this
// new pass is the only one that needs an HTTP client.
const fetch = require("node-fetch");
const {
  demoteThreadInTxn,
  isStaleQueued,
  isStaleHeartbeat,
  MAX_CLAIM_LOOKBACK_MIN
} = require("./etsyMailDraftSend");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

// ─── Collections ────────────────────────────────────────────────────────
const THREADS_COLL = "EtsyMail_Threads";
const DRAFTS_COLL  = "EtsyMail_Drafts";
const SALES_COLL   = "EtsyMail_SalesContext";
const AUDIT_COLL   = "EtsyMail_Audit";
const CONFIG_COLL  = "EtsyMail_Config";
// v2.5 — Jobs collection used by the gmail_scrape pass below.
const JOBS_COLL    = "EtsyMail_Jobs";
// v3.27 — Tracking jobs collection used by the tracking_reconcile pass.
const TRACKING_JOBS_COLL = "EtsyMail_TrackingJobs";

// ─── Auto-pipeline reaper config ───────────────────────────────────────
// A claim is "stale" once this much time has passed without a finalize.
// 5 minutes is generous — the auto-pipeline typically completes in
// 10-60 seconds; >2 minutes of in_progress almost always means death.
const STALE_CLAIM_THRESHOLD_MS  = 5 * 60 * 1000;
const MAX_REAP_PER_RUN_PIPELINE = 200;

// ─── Send-queue reaper config ──────────────────────────────────────────
const MAX_REAP_PER_RUN_SEND     = 200;

// ─── Sales-funnel reaper config ────────────────────────────────────────
const ABANDON_AFTER_DAYS  = parseInt(process.env.ETSYMAIL_SALES_ABANDON_AFTER_DAYS || "7", 10);
const MAX_THREADS_PER_RUN = parseInt(process.env.ETSYMAIL_SALES_REAPER_MAX_THREADS || "200", 10);
// Run the sales-funnel scan at most every 6 hours. Stored in
// EtsyMail_Config/reaperState.lastSalesScanAt (millis). The other two
// reapers run on every invocation; only sales is gated, because its
// query (lastTurnAt < threshold) returns the most candidates and
// running it every 5 minutes wastes Firestore reads.
const SALES_SCAN_INTERVAL_MS = 6 * 60 * 60 * 1000;

// Stages that are eligible for sales-funnel abandonment. pending_close_approval
// is NOT in this list — those threads are deals waiting on operator
// approval, not stalled customer conversations.
const REAPABLE_STAGES = new Set(["discovery", "spec", "quote", "revision"]);

// ─── Gmail-scrape reaper config (v2.5) ─────────────────────────────────
// A scrape job is "stuck claimed" if the extension grabbed it but never
// progressed. Real scrapes finish in seconds; we wait 5 min before
// declaring the worker dead. Well past the extension's 20s claim-poll
// cycle, so a healthy-but-slow worker won't be reaped.
const SCRAPE_STUCK_CLAIM_MS = 5 * 60 * 1000;

// Grace period after a thread is created at "detected_from_gmail"
// before sub-sweep B starts re-enqueueing for it. Gives the extension
// first dibs — its claim-poll runs every 20s, so 3 min is generous.
const SCRAPE_DETECTED_GRACE_MS = 3 * 60 * 1000;

// Grace period after a successful scrape that left customerName=
// "Unknown" before sub-sweep C retries. Lets any in-flight follow-up
// writes settle and avoids racing the snapshot endpoint's commit.
const SCRAPE_UNKNOWN_GRACE_MS = 2 * 60 * 1000;

// Mirrors MAX_ATTEMPTS in etsyMailJobs.js. If they ever diverge, jobs
// could get stuck in a loop where the reaper keeps requeueing past the
// extension's max-attempts threshold. Keep aligned.
//
// v0.9.17 — Raised from 3 to 10 in lockstep with MAX_ATTEMPTS in
// etsyMailJobs.js. See the comment there for the full rationale.
// Short version: 3 was hit too easily on legitimate transient
// failures (offline mid-scrape cycles, Etsy hiccups, ad-blocker
// glitches), and a "failed" job is permanently lost from the
// pipeline — no retry, no message, no record in the inbox.
const MAX_SCRAPE_ATTEMPTS = 10;

// Defense-in-depth caps per invocation. The 30s scheduled-function
// budget is plenty for these numbers; the cap is mainly to prevent a
// misconfig (e.g. an entire day of jobs all stuck) from blowing the
// budget on one tick. Anything not handled this tick gets handled the
// next.
const MAX_SCRAPE_REAP_PER_RUN = 50;

// Statuses that mean "the scrape did happen, but the result might still
// have customerName=Unknown" — sub-sweep C only retries threads that
// are already past the initial detection step.
const SCRAPE_POST_STATUSES = [
  "etsy_scraped",
  "ai_drafted",
  "needs_review",
  "auto_replied",
  "replied",
  "closed"
];

/**
 * Extract the customer name from an Etsy notification email subject.
 *
 * Source of truth: this is a copy of the same-named function in
 * etsyMailGmail-background.js. Duplicated here (rather than imported)
 * because background-function modules aren't a stable import surface
 * in Netlify, and the function is small + rarely changes. If you edit
 * one copy, edit the other — they should stay in lockstep.
 *
 * Used by sub-sweep C to skip the rescrape entirely when the email
 * subject already carries the buyer's name. Empirically that's true
 * for >99% of "Unknown" threads in this system, since Etsy's
 * notification emails always include the customer name in the subject.
 *
 * Returns the cleaned name, or null if the subject doesn't match the
 * expected "Etsy Conversation with <NAME>" pattern.
 */
function extractCustomerNameFromSubject(subject) {
  if (!subject || typeof subject !== "string") return null;
  let s = subject.replace(/^\s*(?:re|fwd|fw)\s*:\s*/gi, "").trim();
  const m = s.match(/Etsy Conversation with[\s\u00A0]+(.+)$/i);
  if (!m) return null;
  let name = m[1];
  name = name.replace(/\s+about\s+.*$/i, "");
  const comma = name.indexOf(",");
  if (comma !== -1) name = name.slice(0, comma);
  name = name.replace(/\s+/g, " ").trim();
  if (!name) return null;
  if (name.length > 200) name = name.slice(0, 200).trim();
  return name;
}

/**
 * v3.1 — Defensive decoder for JSON-stringified Unicode escape sequences.
 *
 * Source of truth: this is a copy of the same-named function in
 * etsyMailSnapshot.js. See that file's comment for the full rationale —
 * tl;dr the Chrome scraper is round-tripping non-ASCII customer names
 * and subjects through JSON.stringify, producing literal `\uXXXX`
 * escape sequences instead of the actual characters.
 *
 * Snapshot ingest now decodes on the way in. This copy is used by
 * sub-sweep D below to repair existing mangled rows that landed before
 * the snapshot fix was deployed.
 *
 * Edit both copies in lockstep if the rule changes.
 */
function unmangleEscapedUnicode(s) {
  if (typeof s !== "string" || s.length === 0) return s;
  if (s.indexOf("\\u") === -1) return s;
  // Surrogate-pair pass first (astral-plane code points like emoji).
  let out = s.replace(
    /\\u([dD][89aAbB][0-9a-fA-F]{2})\\u([dD][c-fC-F][0-9a-fA-F]{2})/g,
    (_m, hi, lo) => {
      const high = parseInt(hi, 16);
      const low  = parseInt(lo, 16);
      try {
        return String.fromCodePoint(((high - 0xD800) << 10) + (low - 0xDC00) + 0x10000);
      } catch {
        return _m;
      }
    }
  );
  // Then single-BMP escapes — but only for code points >= 0x80, so we
  // don't accidentally "decode" a literal backslash-u-ASCII pair from
  // an unrelated docstring or template field.
  out = out.replace(/\\u([0-9a-fA-F]{4})/g, (m, hex) => {
    const cp = parseInt(hex, 16);
    if (cp < 0x80) return m;
    try {
      return String.fromCharCode(cp);
    } catch {
      return m;
    }
  });
  return out;
}

/**
 * Returns true if the input is a string that contains at least one
 * `\uXXXX` escape sequence representing a non-ASCII code point — i.e.
 * a string that the unmangler would actually change.
 */
function hasMangledEscapes(s) {
  if (typeof s !== "string" || s.length === 0) return false;
  if (s.indexOf("\\u") === -1) return false;
  // Quick check: any \uHHHH where HH >= 80 (non-ASCII)?
  return /\\u(?:00[89a-fA-F]|0[1-9a-fA-F][0-9a-fA-F]|[1-9a-fA-F][0-9a-fA-F]{2})/i.test(s);
}

// ─── Helpers ───────────────────────────────────────────────────────────

function json(statusCode, body) {
  return {
    statusCode,
    headers: { ...CORS, "Content-Type": "application/json" },
    body: JSON.stringify(body)
  };
}

async function writeAudit(threadId, draftId, eventType, payload, actor = "system:reapers", outcome = "success", ruleViolations = []) {
  try {
    await db.collection(AUDIT_COLL).add({
      threadId, draftId, eventType, actor, payload,
      createdAt: FV.serverTimestamp(),
      outcome, ruleViolations
    });
  } catch (e) {
    console.warn("reapers audit write failed:", e.message);
  }
}

// ═══════════════════════════════════════════════════════════════════════
//  Pass 1 — Auto-pipeline stale-claim reaper
// ═══════════════════════════════════════════════════════════════════════
//
// The auto-pipeline atomically claims a thread by setting:
//   lastAutoDecision   = "in_progress"
//   lastAutoDecisionAt = <serverTimestamp>
// When the pipeline finishes successfully it overwrites those fields.
// When it crashes mid-run — Lambda timeout, Anthropic API hang, network
// blip, OOM — the in_progress marker is left orphaned and the thread
// shows as "AI thinking..." indefinitely in the operator UI.
//
// This pass finds threads with stale in_progress markers (older than 5
// minutes), and clears them. The thread is left at pending_human_review
// so it's visible in the operator's Needs Review folder.

async function reapStaleClaim(threadRef) {
  return await db.runTransaction(async (tx) => {
    const snap = await tx.get(threadRef);
    if (!snap.exists) return { reaped: false, reason: "thread_gone" };
    const data = snap.data() || {};

    if (data.lastAutoDecision !== "in_progress") {
      return { reaped: false, reason: "no_longer_in_progress", currentDecision: data.lastAutoDecision };
    }

    const claimedAtMs = data.lastAutoDecisionAt && data.lastAutoDecisionAt.toMillis
      ? data.lastAutoDecisionAt.toMillis() : 0;
    const ageMs = Date.now() - claimedAtMs;
    if (ageMs < STALE_CLAIM_THRESHOLD_MS) {
      return { reaped: false, reason: "not_yet_stale", ageMs };
    }

    tx.update(threadRef, {
      lastAutoDecision           : "stale_claim_recovered",
      lastAutoDecisionAt         : FV.serverTimestamp(),
      lastAutoProcessedInboundAt : null,
      aiDraftStatus              : data.aiDraftStatus === "ready" ? "ready" : "none",
      updatedAt                  : FV.serverTimestamp()
    });

    return {
      reaped: true,
      ageMs,
      previousStatus: data.status || null,
      hadDraft      : !!data.latestDraftId
    };
  });
}

async function runAutoPipelinePass() {
  const tStart = Date.now();
  const cutoffMs = Date.now() - STALE_CLAIM_THRESHOLD_MS;

  const snap = await db.collection(THREADS_COLL)
    .where("lastAutoDecision", "==", "in_progress")
    .limit(MAX_REAP_PER_RUN_PIPELINE * 2)
    .get();

  let candidates = [];
  snap.forEach(doc => {
    const data = doc.data() || {};
    const claimedAtMs = data.lastAutoDecisionAt && data.lastAutoDecisionAt.toMillis
      ? data.lastAutoDecisionAt.toMillis() : 0;
    if (claimedAtMs <= cutoffMs) {
      candidates.push({ id: doc.id, ref: doc.ref, ageMs: Date.now() - claimedAtMs });
    }
  });

  if (candidates.length > MAX_REAP_PER_RUN_PIPELINE) {
    candidates = candidates.slice(0, MAX_REAP_PER_RUN_PIPELINE);
  }

  let reapedCount = 0;
  let skippedCount = 0;
  for (const c of candidates) {
    try {
      const result = await reapStaleClaim(c.ref);
      if (result.reaped) {
        reapedCount++;
        await writeAudit(c.id, null, "auto_pipeline_stale_claim_recovered", {
          staleForMs       : c.ageMs,
          previousStatus   : result.previousStatus,
          hadDraft         : result.hadDraft,
          staleThresholdMs : STALE_CLAIM_THRESHOLD_MS
        });
      } else {
        skippedCount++;
      }
    } catch (e) {
      console.warn("reapStaleClaim failed for", c.id, e.message);
      skippedCount++;
    }
  }

  return {
    pass         : "auto_pipeline",
    scanned      : snap.size,
    candidates   : candidates.length,
    reaped       : reapedCount,
    skipped      : skippedCount,
    durationMs   : Date.now() - tStart,
    thresholdMs  : STALE_CLAIM_THRESHOLD_MS
  };
}

// ═══════════════════════════════════════════════════════════════════════
//  Pass 2 — Send-queue reaper
// ═══════════════════════════════════════════════════════════════════════
//
// Drafts that have been enqueued (status=queued) or claimed (status=
// sending) by an extension can get stranded if the operator's browser
// is closed, the tab dies pre-click, or the tab dies post-click. The
// existing peek/claim paths in etsyMailDraftSend.js handle these on
// demand — but only when the extension actually peeks. If the extension
// is offline for hours/days, the queue grows unbounded.
//
// Staleness:
//   queued + queuedAt > MAX_CLAIM_LOOKBACK_MIN (30 min)
//     → mark failed (QUEUED_EXPIRED), demote thread.
//   sending + pre_click + heartbeat > 60s old
//     → mark failed (CLAIM_ABANDONED), demote thread. Safe to re-send.
//   sending + post_click + heartbeat > 60s old
//     → mark sent_unverified (STRANDED_POST_CLICK), demote thread. Operator
//       MUST verify on Etsy before taking any further action. Never blindly re-send.

async function reapStaleDraft(draftRef, kind) {
  return await db.runTransaction(async (tx) => {
    const snap = await tx.get(draftRef);
    if (!snap.exists) return { reaped: false, reason: "draft_gone" };
    const d = snap.data();

    // Re-check staleness inside the txn
    if (d.status === "queued") {
      if (!isStaleQueued(d.queuedAt)) {
        return { reaped: false, reason: "queued_not_yet_stale" };
      }
    } else if (d.status === "sending") {
      if (!isStaleHeartbeat(d.sendHeartbeatAt)) {
        return { reaped: false, reason: "sending_heartbeat_fresh" };
      }
    } else {
      return { reaped: false, reason: "already_terminal", currentStatus: d.status };
    }

    let sendErrorCode, sendError, decisionReason;
    let terminalStatus = "failed";   // default — failed sends
    let setSentAt      = false;      // sent_unverified should also stamp sentAt
    if (d.status === "queued") {
      sendErrorCode  = "QUEUED_EXPIRED";
      sendError      = `Expired by reaper — queued more than ${MAX_CLAIM_LOOKBACK_MIN} minutes (extension may be offline)`;
      decisionReason = "human_review_after_queued_expired";
    } else if (d.sendStage === "post_click") {
      // v2.6 fix: STRANDED_POST_CLICK is NOT a failure — the extension
      // typed the message AND clicked Etsy's Send button. The "stranded"
      // part means we just don't have a confirmation toast/signal. The
      // message almost always WAS delivered (Etsy's Send is reliable),
      // so we use `sent_unverified` semantics:
      //   - status: sent_unverified  (not "failed")
      //   - sentAt: now              (so the UI's optimistic message
      //     insert fires, putting the just-sent text into the thread
      //     view immediately instead of leaving the operator wondering)
      //   - thread → human_review    (so the operator can verify)
      // Treating this as `failed` was the prior bug: the operator saw
      // a red error banner and re-sent, creating duplicate messages.
      sendErrorCode  = "STRANDED_POST_CLICK";
      sendError      = "Send was clicked. Etsy didn't return a confirmation signal within the timeout — verify on Etsy that the message went through. (Most likely it did; this status just means we couldn't auto-confirm.)";
      decisionReason = "human_review_after_stranded_post_click";
      terminalStatus = "sent_unverified";
      setSentAt      = true;
    } else {
      sendErrorCode  = "CLAIM_ABANDONED";
      sendError      = "Extension claimed the draft but never clicked Send (heartbeat stale). Safe to re-send.";
      decisionReason = "human_review_after_claim_abandoned";
    }

    const draftPatch = {
      status          : terminalStatus,
      sendError,
      sendErrorCode,
      sendHeartbeatAt : FV.serverTimestamp(),
      updatedAt       : FV.serverTimestamp()
    };
    if (setSentAt) draftPatch.sentAt = FV.serverTimestamp();
    tx.set(draftRef, draftPatch, { merge: true });

    const threadStatusUpdate = await demoteThreadInTxn(tx, d.threadId, decisionReason);

    return {
      reaped: true,
      threadId: d.threadId,
      sendErrorCode,
      threadStatusUpdate,
      sendStage: d.sendStage,
      ageMs: kind === "queued"
        ? (d.queuedAt ? Date.now() - d.queuedAt.toMillis() : null)
        : (d.sendHeartbeatAt ? Date.now() - d.sendHeartbeatAt.toMillis() : null)
    };
  });
}

async function runSendQueuePass() {
  const tStart = Date.now();
  let totalReaped = 0;
  let totalScanned = 0;
  let totalSkipped = 0;
  const failures = [];

  // ── Pass 2a: stale `queued` drafts ────────────────────────────
  const queuedSnap = await db.collection(DRAFTS_COLL)
    .where("status", "==", "queued")
    .limit(MAX_REAP_PER_RUN_SEND * 2)
    .get();
  totalScanned += queuedSnap.size;

  for (const doc of queuedSnap.docs) {
    if (totalReaped >= MAX_REAP_PER_RUN_SEND) break;
    const d = doc.data();
    if (!isStaleQueued(d.queuedAt)) { totalSkipped++; continue; }
    try {
      const r = await reapStaleDraft(doc.ref, "queued");
      if (r.reaped) {
        totalReaped++;
        await writeAudit(r.threadId, doc.id, "draft_queue_expired_by_reaper", {
          sendErrorCode: r.sendErrorCode,
          ageMs        : r.ageMs,
          threadStatusUpdate: r.threadStatusUpdate
        }, "system:sendQueueReaper");
      } else {
        totalSkipped++;
      }
    } catch (e) {
      failures.push({ draftId: doc.id, error: e.message });
    }
  }

  // ── Pass 2b: stale `sending` drafts ───────────────────────────
  if (totalReaped < MAX_REAP_PER_RUN_SEND) {
    const sendingSnap = await db.collection(DRAFTS_COLL)
      .where("status", "==", "sending")
      .limit(MAX_REAP_PER_RUN_SEND * 2)
      .get();
    totalScanned += sendingSnap.size;

    for (const doc of sendingSnap.docs) {
      if (totalReaped >= MAX_REAP_PER_RUN_SEND) break;
      const d = doc.data();
      if (!isStaleHeartbeat(d.sendHeartbeatAt)) { totalSkipped++; continue; }
      try {
        const r = await reapStaleDraft(doc.ref, "sending");
        if (r.reaped) {
          totalReaped++;
          await writeAudit(r.threadId, doc.id, "draft_send_reaped", {
            sendErrorCode: r.sendErrorCode,
            sendStage    : r.sendStage,
            ageMs        : r.ageMs,
            threadStatusUpdate: r.threadStatusUpdate
          }, "system:sendQueueReaper");
        } else {
          totalSkipped++;
        }
      } catch (e) {
        failures.push({ draftId: doc.id, error: e.message });
      }
    }
  }

  return {
    pass       : "send_queue",
    scanned    : totalScanned,
    reaped     : totalReaped,
    skipped    : totalSkipped,
    failures   : failures.length,
    failureLog : failures.slice(0, 10),
    durationMs : Date.now() - tStart
  };
}

// ═══════════════════════════════════════════════════════════════════════
//  Pass 3 — Sales-funnel abandonment reaper
// ═══════════════════════════════════════════════════════════════════════
//
// Detects sales threads where SalesContext.stage is in [discovery, spec,
// quote, revision] and lastTurnAt is older than ABANDON_AFTER_DAYS days.
// Marks them abandoned in both SalesContext and the parent thread.

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

/** Read the sales-pass time gate. Returns true iff it's been longer
 *  than SALES_SCAN_INTERVAL_MS since the last sales scan, OR the gate
 *  doc is missing (first run). */
async function shouldRunSalesPass() {
  try {
    const doc = await db.collection(CONFIG_COLL).doc("reaperState").get();
    if (!doc.exists) return true;
    const lastMs = doc.data().lastSalesScanAt && doc.data().lastSalesScanAt.toMillis
      ? doc.data().lastSalesScanAt.toMillis() : 0;
    return (Date.now() - lastMs) >= SALES_SCAN_INTERVAL_MS;
  } catch (e) {
    console.warn("salesReaper: gate read failed (proceeding):", e.message);
    return true;
  }
}

async function markSalesPassRan() {
  try {
    await db.collection(CONFIG_COLL).doc("reaperState").set({
      lastSalesScanAt: FV.serverTimestamp()
    }, { merge: true });
  } catch (e) {
    console.warn("salesReaper: gate write failed:", e.message);
  }
}

async function reapAbandonedSalesThread(threadId, thresholdMs) {
  const ctxRef    = db.collection(SALES_COLL).doc(threadId);
  const threadRef = db.collection(THREADS_COLL).doc(threadId);

  return await db.runTransaction(async (tx) => {
    const ctxSnap = await tx.get(ctxRef);
    if (!ctxSnap.exists) return { reaped: false, reason: "context_missing" };
    const ctx = ctxSnap.data() || {};

    if (!REAPABLE_STAGES.has(ctx.stage)) {
      return { reaped: false, reason: "stage_not_reapable", stage: ctx.stage };
    }

    const lastTurnMs = ctx.lastTurnAt && ctx.lastTurnAt.toMillis ? ctx.lastTurnAt.toMillis() : 0;
    if (lastTurnMs >= thresholdMs) {
      return { reaped: false, reason: "fresh", lastTurnMs, thresholdMs };
    }

    tx.set(ctxRef, {
      stage      : "abandoned",
      abandonedAt: FV.serverTimestamp(),
      lastSalesAgentBlockReason: null
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

async function runSalesFunnelPass({ force = false } = {}) {
  const tStart = Date.now();

  if (!(await isSalesModeEnabled())) {
    return { pass: "sales_funnels", skipped: true, reason: "sales_mode_disabled", durationMs: Date.now() - tStart };
  }
  if (!force && !(await shouldRunSalesPass())) {
    return { pass: "sales_funnels", skipped: true, reason: "interval_gated", intervalMs: SALES_SCAN_INTERVAL_MS, durationMs: Date.now() - tStart };
  }

  const thresholdMs = Date.now() - (ABANDON_AFTER_DAYS * 24 * 60 * 60 * 1000);
  const thresholdTs = admin.firestore.Timestamp.fromMillis(thresholdMs);

  let snap;
  try {
    snap = await db.collection(SALES_COLL)
      .where("lastTurnAt", "<", thresholdTs)
      .orderBy("lastTurnAt", "asc")
      .limit(MAX_THREADS_PER_RUN)
      .get();
  } catch (e) {
    if (/index/i.test(e.message)) {
      console.error("salesReaper: composite index required.", e.message);
      await writeAudit(null, null, "sales_reaper_index_missing", { error: e.message }, "system:salesReaper", "failure", ["MISSING_FIRESTORE_INDEX"]);
      return { pass: "sales_funnels", error: "Missing Firestore index — see function logs", needsIndex: true, durationMs: Date.now() - tStart };
    }
    throw e;
  }

  // Always mark the gate, even if scan was empty — the gate's purpose
  // is "we did the work", not "we found something". Doing it before the
  // per-thread loop means a partial-failure run still updates the gate
  // (we don't want a single bad thread re-running the entire scan in
  // 5 min).
  await markSalesPassRan();

  if (snap.empty) {
    return { pass: "sales_funnels", scanned: 0, reaped: 0, durationMs: Date.now() - tStart };
  }

  let reapedCount = 0;
  const reapedThreads = [];
  const skipped = [];

  for (const doc of snap.docs) {
    const threadId = doc.id;
    const ctxData = doc.data() || {};

    if (!REAPABLE_STAGES.has(ctxData.stage)) {
      skipped.push({ threadId, reason: "stage_not_reapable", stage: ctxData.stage });
      continue;
    }

    try {
      const result = await reapAbandonedSalesThread(threadId, thresholdMs);
      if (result.reaped) {
        reapedCount++;
        const ageDays = Math.round((Date.now() - result.lastTurnAtMs) / (24 * 60 * 60 * 1000));
        reapedThreads.push({ threadId, fromStage: result.fromStage, lastTurnAtMs: result.lastTurnAtMs, ageDays });
        await writeAudit(threadId, null, "sales_abandoned", {
          fromStage      : result.fromStage,
          lastTurnAtMs   : result.lastTurnAtMs,
          ageDays,
          abandonAfterDays: ABANDON_AFTER_DAYS
        }, "system:salesReaper");
      } else {
        skipped.push({ threadId, reason: result.reason });
      }
    } catch (e) {
      console.warn(`salesReaper: thread ${threadId} reap failed:`, e.message);
      skipped.push({ threadId, reason: "transaction_error", error: e.message });
    }
  }

  if (reapedCount > 0 || snap.size >= MAX_THREADS_PER_RUN) {
    await writeAudit(null, null, "sales_reaper_scan_complete", {
      scanned         : snap.size,
      reaped          : reapedCount,
      capacityHit     : snap.size >= MAX_THREADS_PER_RUN,
      abandonAfterDays: ABANDON_AFTER_DAYS,
      thresholdMs,
      reapedSample    : reapedThreads.slice(0, 10),
      durationMs      : Date.now() - tStart
    }, "system:salesReaper");
  }

  return {
    pass       : "sales_funnels",
    scanned    : snap.size,
    reaped     : reapedCount,
    skipped    : skipped.length,
    capacityHit: snap.size >= MAX_THREADS_PER_RUN,
    reapedThreads,
    durationMs : Date.now() - tStart
  };
}

// ═══════════════════════════════════════════════════════════════════════
//  Pass 4 — Gmail-watcher → scrape-job recovery (v2.5)
// ═══════════════════════════════════════════════════════════════════════
//
// Three sub-sweeps that together close the loop on threads stranded at
// customerName="Unknown" by the Gmail-watcher → extension-scraper
// pipeline. See file header for the full failure-mode taxonomy. Each
// sub-sweep is independently bounded and idempotent; running this pass
// twice in quick succession cannot produce duplicate work.

// Centralized "is this job alive?" check used by sub-sweep B so any
// future caller (manual-rescrape endpoint, etc.) shares the rule.
function isLiveScrapeJob(jobData) {
  if (!jobData) return false;
  return ["queued", "claimed", "succeeded"].includes(jobData.status);
}

/**
 * Try to fill customerName from the email subject already stored on
 * the thread. Used by sub-sweeps B and C as a fast-path before falling
 * back to a rescrape — Etsy notification subjects always carry the
 * buyer's name in plain text, so when the watcher has stored that
 * subject we don't need the extension to scrape anything.
 *
 * Transactional so two reaper invocations racing each other can never
 * both fill the same thread (the second one sees customerName already
 * populated and aborts).
 *
 * Returns:
 *   { filled: true,  name }  — customerName patched, audit row written
 *   { filled: false, reason: "no_subject" | "subject_unparseable"
 *                            | "already_named" | "tx_aborted" }
 */
async function tryFillCustomerNameFromSubject(threadRef, threadData) {
  const subject = threadData && threadData.subject;
  if (!subject) return { filled: false, reason: "no_subject" };

  const parsed = extractCustomerNameFromSubject(subject);
  if (!parsed) return { filled: false, reason: "subject_unparseable" };

  // Transactional check-and-fill. If another caller raced us and the
  // thread is no longer at "Unknown", we abort without overwriting.
  let outcome;
  try {
    outcome = await db.runTransaction(async (tx) => {
      const fresh = await tx.get(threadRef);
      if (!fresh.exists) return { filled: false, reason: "tx_aborted" };
      const data = fresh.data() || {};
      const isPlaceholder =
           !data.customerName
        || data.customerName === "Unknown"
        || data.customerName === "";
      if (!isPlaceholder) return { filled: false, reason: "already_named" };

      tx.update(threadRef, {
        customerName            : parsed,
        customerNameFromSubject : true,
        updatedAt               : FV.serverTimestamp()
      });
      return { filled: true, name: parsed };
    });
  } catch (err) {
    console.warn(`[gmail-scrape-reaper] fill-from-subject tx failed for ${threadRef.id}:`, err.message);
    return { filled: false, reason: "tx_aborted" };
  }

  if (outcome.filled) {
    await writeAudit(
      threadRef.id,
      null,
      "thread_customer_name_filled_from_subject",
      { customerName: parsed, subject },
      "system:reapers:gmailScrape"
    );
  }
  return outcome;
}

/**
 * Sub-sweep A — Stuck claimed scrape jobs.
 * Reverts to "queued" so the extension picks them up next poll.
 * After MAX_SCRAPE_ATTEMPTS the job goes to "failed" instead of
 * looping. Transactional check-and-flip prevents racing reapers from
 * double-requeueing.
 */
async function reapStuckScrapeClaims() {
  const stuckCutoffMs = Date.now() - SCRAPE_STUCK_CLAIM_MS;

  // Single-field equality query — no composite index required. We
  // intentionally do NOT add the claimedAt < cutoff filter here, even
  // though that would prune the candidate set, because combining
  // jobType+status+claimedAt requires a composite index in Firestore.
  // Without that index the query throws and the entire pass silently
  // fails (caught by the outer try/catch in runGmailScrapePass, but the
  // operator never sees that error). Filtering claimedAt client-side
  // costs us nothing — claimed scrape jobs are always rare relative to
  // queued/succeeded ones, so the result set is small.
  //
  // We over-fetch (5x the per-run cap) to ensure we have enough
  // candidates after client-side filtering.
  const snap = await db.collection(JOBS_COLL)
    .where("jobType", "==", "scrape")
    .where("status",  "==", "claimed")
    .limit(MAX_SCRAPE_REAP_PER_RUN * 5)
    .get();

  if (snap.empty) return { requeued: 0, exhausted: 0 };

  let requeued  = 0;
  let exhausted = 0;
  let processed = 0;

  for (const docSnap of snap.docs) {
    if (processed >= MAX_SCRAPE_REAP_PER_RUN) break;

    const data = docSnap.data() || {};
    const claimedAtMs = data.claimedAt && data.claimedAt.toMillis
      ? data.claimedAt.toMillis() : 0;
    // Client-side staleness filter — see comment above on why this isn't
    // in the query.
    if (!claimedAtMs || claimedAtMs > stuckCutoffMs) continue;

    processed++;
    const ref = docSnap.ref;
    try {
      const outcome = await db.runTransaction(async (tx) => {
        const fresh = await tx.get(ref);
        if (!fresh.exists) return { action: "gone" };
        const fdata = fresh.data() || {};
        if (fdata.status !== "claimed") return { action: "no_longer_claimed" };

        // Re-check staleness inside the tx — claimedAt might have been
        // updated between the query and now (worker submitted a heartbeat).
        const fClaimedAtMs = fdata.claimedAt && fdata.claimedAt.toMillis
          ? fdata.claimedAt.toMillis() : 0;
        if (Date.now() - fClaimedAtMs < SCRAPE_STUCK_CLAIM_MS) {
          return { action: "no_longer_stuck" };
        }

        const attempts = fdata.attempts || 0;
        if (attempts >= MAX_SCRAPE_ATTEMPTS) {
          // Don't loop — escalate to failed and let the operator look.
          tx.update(ref, {
            status   : "failed",
            lastError: `Stuck in 'claimed' past ${SCRAPE_STUCK_CLAIM_MS}ms with no heartbeat; attempts=${attempts} reached MAX_SCRAPE_ATTEMPTS`,
            updatedAt: FV.serverTimestamp()
          });
          return { action: "exhausted", attempts, threadId: fdata.threadId };
        }

        tx.update(ref, {
          status    : "queued",
          claimedBy : null,
          claimedAt : null,
          // attempts left as-is — claimNextJob's tx in etsyMailJobs.js
          // will increment it on next claim.
          lastError : `Reaped stuck claim after ${SCRAPE_STUCK_CLAIM_MS}ms (worker died?)`,
          updatedAt : FV.serverTimestamp()
        });
        return { action: "requeued", attempts, threadId: fdata.threadId };
      });

      if (outcome.action === "requeued") {
        requeued++;
        await writeAudit(outcome.threadId, null, "scrape_job_reaped_stuck_claim",
          { jobId: ref.id, attempts: outcome.attempts },
          "system:reapers:gmailScrape"
        );
      } else if (outcome.action === "exhausted") {
        exhausted++;
        await writeAudit(outcome.threadId, null, "scrape_job_exhausted",
          { jobId: ref.id, attempts: outcome.attempts, reason: "stuck_claim_max_attempts" },
          "system:reapers:gmailScrape"
        );
      }
    } catch (err) {
      console.warn(`[gmail-scrape-reaper] subsweep A tx failed for ${ref.id}:`, err.message);
    }
  }

  return { requeued, exhausted };
}

/**
 * Sub-sweep B — detected_from_gmail threads with no live job.
 *
 * Two recoveries happen here, in order:
 *
 *   1. Subject-fill (cheap, no extension needed).
 *      If the thread is still showing customerName="Unknown" but has a
 *      parseable email subject, populate the customer name from the
 *      subject. This is independent of whether the scrape ever runs —
 *      it just makes the inbox useful immediately. Threads that never
 *      get scraped (extension permanently down) still display the
 *      buyer's name instead of "Unknown".
 *
 *   2. Job re-enqueue (only if no live job is in flight).
 *      Either no job ever got created (extension wasn't running and
 *      the watcher's enqueue silently lost the doc somehow), or the
 *      prior job is in "failed" status. Either way, enqueue a fresh
 *      scrape job using the deterministic gmail_<msgId> id (matches
 *      the watcher's pattern, preserves idempotency for any concurrent
 *      watcher tick).
 *
 * The two recoveries are independent — a thread can have its name
 * filled from subject AND a fresh scrape job queued in the same tick.
 * The eventual scrape will overwrite customerName with the real Etsy
 * value, but the customerNameFromSubject flag advertises the
 * provenance so we know which value is the placeholder.
 */
async function reapDetectedThreadsWithoutLiveJobs() {
  const graceMs = SCRAPE_DETECTED_GRACE_MS;
  const graceCutoffMs = Date.now() - graceMs;

  // Single-field equality query — no composite index required. We
  // intentionally do NOT add `createdAt < cutoff` + `orderBy createdAt`
  // here because that requires a composite index in Firestore. Without
  // the index the query would throw and the entire pass would silently
  // fail. We over-fetch and apply the grace-period cutoff client-side.
  //
  // Volume-wise this is fine: detected_from_gmail is a transient
  // status (advances to etsy_scraped on first successful scrape), so
  // the result set is bounded by however many threads are mid-pipeline
  // at any given moment. The MAX_SCRAPE_REAP_PER_RUN * 5 over-fetch
  // gives us plenty of headroom.
  const snap = await db.collection(THREADS_COLL)
    .where("status", "==", "detected_from_gmail")
    .limit(MAX_SCRAPE_REAP_PER_RUN * 5)
    .get();

  if (snap.empty) return { requeued: 0, exhausted: 0, skippedAlive: 0, namesFilledFromSubject: 0 };

  let requeued     = 0;
  let exhausted    = 0;
  let skippedAlive = 0;
  let namesFilledFromSubject = 0;
  let processed    = 0;

  for (const threadSnap of snap.docs) {
    if (processed >= MAX_SCRAPE_REAP_PER_RUN) break;

    const thread          = threadSnap.data() || {};
    const threadId        = thread.threadId || threadSnap.id;
    const gmailMessageId  = thread.gmailMessageId;
    const conversationUrl = thread.etsyConversationUrl;

    // Step 1 — Fill customerName from subject if it's still "Unknown".
    // This runs BEFORE the grace-period check so newly-detected threads
    // get labeled instantly even within the grace window. Cheap (one tx,
    // no Etsy roundtrip), independent of the job-queue recovery below.
    // Even if the rest of this loop iteration bails out, the name is now
    // visible in the inbox.
    if (!thread.customerName || thread.customerName === "Unknown" || thread.customerName === "") {
      const fillResult = await tryFillCustomerNameFromSubject(threadSnap.ref, thread);
      if (fillResult.filled) {
        namesFilledFromSubject++;
        // Update the in-memory copy so the rest of this iteration sees
        // the new name (downstream branches read thread.customerName for
        // audit-payload purposes).
        thread.customerName = fillResult.name;
        thread.customerNameFromSubject = true;
      }
    }

    // Apply the grace-period cutoff client-side (see query comment).
    // We only consider threads created MORE than SCRAPE_DETECTED_GRACE_MS
    // ago — gives the extension first dibs on freshly-detected threads.
    const createdAtMs = thread.createdAt && thread.createdAt.toMillis
      ? thread.createdAt.toMillis() : 0;
    if (!createdAtMs || createdAtMs > graceCutoffMs) continue;

    processed++;

    // Without a conversation URL we have nothing to scrape. Tag and
    // skip — operator must investigate.
    if (!conversationUrl) {
      await threadSnap.ref.set({
        riskFlags: FV.arrayUnion("scrape_no_conversation_url"),
        updatedAt: FV.serverTimestamp()
      }, { merge: true });
      await writeAudit(threadId, null, "scrape_reaper_no_conversation_url", {},
        "system:reapers:gmailScrape");
      continue;
    }

    // Match the watcher's deterministic id so we don't create a parallel
    // job alongside a queued/claimed one we're not seeing yet.
    const jobId = gmailMessageId
      ? `gmail_${gmailMessageId}`
      : `rescrape_${threadId}_${Date.now()}`;
    const jobRef     = db.collection(JOBS_COLL).doc(jobId);
    const jobDocSnap = await jobRef.get();
    const jobData    = jobDocSnap.exists ? jobDocSnap.data() : null;

    if (isLiveScrapeJob(jobData)) {
      // Healthy job in flight — let it cook. Sub-sweep A handles stuck
      // claims separately.
      skippedAlive++;
      continue;
    }

    // Either no job exists or it's "failed". Decide based on attempts.
    const priorAttempts = jobData ? (jobData.attempts || 0) : 0;
    if (priorAttempts >= MAX_SCRAPE_ATTEMPTS) {
      exhausted++;
      // Tag the thread so the inbox UI can surface "scrape exhausted —
      // manual rescrape needed". Don't change status — the thread is
      // still legitimately at detected_from_gmail.
      await threadSnap.ref.set({
        riskFlags: FV.arrayUnion("scrape_exhausted"),
        updatedAt: FV.serverTimestamp()
      }, { merge: true });
      await writeAudit(threadId, null, "scrape_job_exhausted",
        { jobId, attempts: priorAttempts, reason: "detected_thread_max_attempts" },
        "system:reapers:gmailScrape"
      );
      continue;
    }

    // Re-enqueue. set with merge:false on the deterministic id resets
    // the doc cleanly — no leftover claimedBy/claimedAt from a prior
    // failed attempt.
    await jobRef.set({
      jobId,
      jobType : "scrape",
      status  : "queued",
      threadId,
      payload : {
        etsyConversationUrl: conversationUrl,
        source             : "reapers:gmailScrape",
        gmailMessageId     : gmailMessageId || null,
        gmailThreadId      : thread.gmailThreadId || null,
        rescrape           : true
      },
      attempts       : priorAttempts,   // preserve history; claim tx increments
      claimedBy      : null,
      claimedAt      : null,
      lastError      : jobData ? (jobData.lastError || null) : null,
      lastHeartbeatAt: null,
      result         : null,
      createdAt      : jobData && jobData.createdAt ? jobData.createdAt : FV.serverTimestamp(),
      updatedAt      : FV.serverTimestamp(),
      reapedAt       : FV.serverTimestamp()
    }, { merge: false });

    requeued++;
    await writeAudit(threadId, null, "scrape_job_reaped_detected_orphan",
      {
        jobId,
        priorJobStatus  : jobData ? jobData.status : "missing",
        priorAttempts,
        gmailMessageId  : gmailMessageId || null,
        conversationUrl
      },
      "system:reapers:gmailScrape"
    );
  }

  return { requeued, exhausted, skippedAlive, namesFilledFromSubject };
}

/**
 * Sub-sweep C — Successful scrape but customerName=="Unknown".
 *
 * Two paths, in order:
 *
 *   1. Subject-fill (cheap, no extension needed).
 *      Try to populate customerName from the email subject already on
 *      the thread. Etsy notification subjects always carry the buyer's
 *      name. If the fill succeeds, we're done — no rescrape is queued.
 *
 *   2. One-shot rescrape (only if subject-fill failed/skipped).
 *      The _unknownRetryAttempted flag is set transactionally BEFORE
 *      the job is enqueued, so two reaper invocations racing each
 *      other can never produce duplicate retries. After this single
 *      retry the thread either gets filled in by the rescrape, or it
 *      stays "Unknown" and the operator is on their own — we
 *      explicitly do NOT loop further automatic retries here (the
 *      user's call when wiring this up: "Yes — try once more, then
 *      leave alone").
 */
async function reapUnknownAfterScrape() {
  const cutoffMs = Date.now() - SCRAPE_UNKNOWN_GRACE_MS;

  // Firestore can't combine an `==` on customerName with a `not-in` on
  // status efficiently without a composite index. Query on the most
  // selective single field (customerName=="Unknown") and filter status
  // / grace / one-shot guard client-side. "Unknown" threads should be
  // rare in steady state, so the result set stays small even at scale.
  const snap = await db.collection(THREADS_COLL)
    .where("customerName", "==", "Unknown")
    .limit(MAX_SCRAPE_REAP_PER_RUN * 2)   // overfetch; will filter
    .get();

  if (snap.empty) return { retried: 0, skipped: 0, namesFilledFromSubject: 0 };

  let retried = 0;
  let skipped = 0;
  let namesFilledFromSubject = 0;

  for (const threadSnap of snap.docs) {
    if (retried >= MAX_SCRAPE_REAP_PER_RUN) break;

    const thread   = threadSnap.data() || {};
    const threadId = thread.threadId || threadSnap.id;

    // Subject-fill is the cheap, no-Etsy-roundtrip path and should run
    // for ANY thread still showing customerName="Unknown" — including
    // detected_from_gmail threads (sub-sweep B's primary domain). The
    // two passes are independently safe (the tryFillCustomerNameFromSubject
    // tx aborts if customerName is no longer "Unknown"), so running both
    // just gives us belt-and-suspenders coverage. The status filter
    // below only gates the rescrape branch, not the subject-fill branch.
    const fillResult = await tryFillCustomerNameFromSubject(threadSnap.ref, thread);
    if (fillResult.filled) {
      namesFilledFromSubject++;
      // Mark the one-shot guard so we don't reconsider this thread for
      // a rescrape on a future tick — the name is now correct.
      await threadSnap.ref.set({
        _unknownRetryAttempted   : true,
        _unknownRetryAttemptedAt : FV.serverTimestamp(),
        updatedAt                : FV.serverTimestamp()
      }, { merge: true });
      continue;
    }

    // Below this point we're considering the rescrape path, which is
    // the one-shot retry. Apply all the rescrape-specific guards:

    // One-shot guard. The transactional flip below is the authoritative
    // gate; this is just a fast-path skip for already-attempted threads
    // so we don't burn budget on transactions that would no-op.
    if (thread._unknownRetryAttempted === true) {
      skipped++;
      continue;
    }

    // Threads still at detected_from_gmail are sub-sweep B's job — let
    // it handle the rescrape there to keep the deterministic gmail_<id>
    // job-id semantics consistent.
    if (!SCRAPE_POST_STATUSES.includes(thread.status)) {
      skipped++;
      continue;
    }

    // Honor the post-scrape grace window so we don't race a snapshot
    // commit that's about to fill in customerName legitimately.
    const lastSyncedMs = thread.lastSyncedAt && thread.lastSyncedAt.toMillis
      ? thread.lastSyncedAt.toMillis() : 0;
    if (lastSyncedMs && lastSyncedMs > cutoffMs) {
      skipped++;
      continue;
    }

    const conversationUrl = thread.etsyConversationUrl;
    if (!conversationUrl) {
      // Can't rescrape without a URL — mark the guard so we stop
      // re-evaluating this thread on every reaper tick.
      await threadSnap.ref.set({
        _unknownRetryAttempted: true,
        riskFlags             : FV.arrayUnion("unknown_no_conversation_url"),
        updatedAt             : FV.serverTimestamp()
      }, { merge: true });
      skipped++;
      continue;
    }

    // Set the one-shot guard transactionally — if another reaper tick
    // got here first and already flipped it, abort without enqueueing.
    let didClaim = false;
    try {
      didClaim = await db.runTransaction(async (tx) => {
        const fresh = await tx.get(threadSnap.ref);
        if (!fresh.exists) return false;
        const data = fresh.data() || {};
        if (data._unknownRetryAttempted === true) return false;
        if (data.customerName !== "Unknown") return false;   // already filled in
        tx.update(threadSnap.ref, {
          _unknownRetryAttempted   : true,
          _unknownRetryAttemptedAt : FV.serverTimestamp(),
          updatedAt                : FV.serverTimestamp()
        });
        return true;
      });
    } catch (err) {
      console.warn(`[gmail-scrape-reaper] subsweep C guard tx failed for ${threadId}:`, err.message);
      continue;
    }

    if (!didClaim) {
      skipped++;
      continue;
    }

    // Fresh job id (unique per retry) so we don't collide with the
    // already-succeeded gmail_<msgId> doc — that doc's history stays
    // intact for the audit trail.
    const jobId = `rescrape_${threadId}_${Date.now()}`;
    await db.collection(JOBS_COLL).doc(jobId).set({
      jobId,
      jobType : "scrape",
      status  : "queued",
      threadId,
      payload : {
        etsyConversationUrl: conversationUrl,
        source             : "reapers:gmailScrape:unknown-retry",
        gmailMessageId     : thread.gmailMessageId || null,
        gmailThreadId      : thread.gmailThreadId  || null,
        rescrape           : true,
        reason             : "customerName=Unknown after first scrape"
      },
      attempts       : 0,
      claimedBy      : null,
      claimedAt      : null,
      lastError      : null,
      lastHeartbeatAt: null,
      result         : null,
      createdAt      : FV.serverTimestamp(),
      updatedAt      : FV.serverTimestamp(),
      reapedAt       : FV.serverTimestamp()
    }, { merge: false });

    retried++;
    await writeAudit(threadId, null, "scrape_job_reaped_unknown_retry",
      {
        jobId,
        priorStatus    : thread.status,
        gmailMessageId : thread.gmailMessageId || null,
        conversationUrl
      },
      "system:reapers:gmailScrape"
    );
  }

  return { retried, skipped, namesFilledFromSubject };
}

/**
 * Sub-sweep D — Unmangle JSON-escaped Unicode in stored thread fields.
 *
 * One-shot data-repair pass. The Chrome scraper had a bug where some
 * non-ASCII customer names + subjects arrived as literal `\uXXXX`
 * escape sequences instead of the actual character ("Caitríona" stored
 * as the 13-character string "Caitr\u00edona"). The snapshot endpoint
 * now decodes on ingest, but threads that were already created before
 * that fix landed still carry the mangled values.
 *
 * This sweep finds those threads and fixes them in place. It's
 * intentionally cheap: a single equality-free query is impossible
 * (Firestore can't filter on "string contains substring") so we walk
 * recently-active threads and only update the ones whose customerName
 * or subject contain literal `\uXXXX` escape sequences.
 *
 * Bounded by SCRAPE_UNMANGLE_BATCH so it doesn't hog the 30s function
 * budget. Repeated runs across reaper ticks gradually clean the whole
 * collection — once it's clean, every subsequent run is a no-op.
 */
async function reapMangledUnicodeFields() {
  // Walk recently-updated threads first — the operator is most likely
  // to be looking at those, so fixing them first gives the fastest
  // perceived improvement. We page through up to BATCH * 4 candidates
  // per tick and only update the ones that actually need it.
  const BATCH = MAX_SCRAPE_REAP_PER_RUN;
  const snap = await db.collection(THREADS_COLL)
    .orderBy("updatedAt", "desc")
    .limit(BATCH * 4)
    .get();

  if (snap.empty) return { fixed: 0, scanned: 0 };

  let fixed = 0;
  let scanned = 0;

  for (const threadSnap of snap.docs) {
    if (fixed >= BATCH) break;
    scanned++;
    const data = threadSnap.data() || {};

    // Check the two fields we know the scraper mangles. Skipping any
    // field that doesn't actually have a `\uXXXX` non-ASCII escape
    // means clean threads are skipped after a single property read —
    // very cheap.
    const nameMangled    = hasMangledEscapes(data.customerName);
    const subjectMangled = hasMangledEscapes(data.subject);
    const senderMangled  = hasMangledEscapes(data.lastSenderName);
    if (!nameMangled && !subjectMangled && !senderMangled) continue;

    const patch = {};
    if (nameMangled) {
      patch.customerName = unmangleEscapedUnicode(data.customerName);
    }
    if (subjectMangled) {
      patch.subject = unmangleEscapedUnicode(data.subject);
    }
    if (senderMangled) {
      patch.lastSenderName = unmangleEscapedUnicode(data.lastSenderName);
    }
    patch.updatedAt = FV.serverTimestamp();
    patch.unicodeUnmangledAt = FV.serverTimestamp();

    try {
      await threadSnap.ref.set(patch, { merge: true });
      fixed++;
      await writeAudit(
        threadSnap.id,
        null,
        "thread_unicode_unmangled",
        {
          fieldsRepaired: Object.keys(patch).filter(k => k !== "updatedAt" && k !== "unicodeUnmangledAt"),
          // Truncate originals to 80 chars so we don't bloat audit rows.
          originalCustomerName: nameMangled ? String(data.customerName).slice(0, 80) : null,
          originalSubject     : subjectMangled ? String(data.subject).slice(0, 80) : null
        },
        "system:reapers:gmailScrape"
      );
    } catch (err) {
      console.warn(`[gmail-scrape-reaper] subsweep D unmangle failed for ${threadSnap.id}:`, err.message);
    }
  }

  return { fixed, scanned };
}

// ─────────────────────────────────────────────────────────────────────
// v3.27 — Tracking-attachment reconciliation pass.
//
// Backstop for drafts whose `trackingImages` array references a tracking
// job that's already `ready`, but whose draft state is frozen at
// `pending`. Two propagation paths already exist:
//
//   1. etsyMailTrackingSnapshot-background draftWriteback (forward path)
//   2. inbox pollTrackingJob writeback                    (interactive path)
//
// Both rely on the job carrying a draftId. Pre-v3.27 jobs don't have one,
// and even on v3.27+ the worker writeback can fail (network blip,
// transaction conflict, draft created without the new pass-through). This
// pass is the third-layer safety net: it scans for stuck drafts and
// reconciles them.
//
// Idempotent and safe to run on every reaper cycle. Default limit per
// pass is 100 drafts (newest-first) — keeps Firestore reads bounded.
// Owner can invoke with a higher limit for one-shot bulk migration of
// pre-v3.27 drafts, e.g.:
//
//   POST /.netlify/functions/etsyMailReapers
//     { "op": "tracking_reconcile", "limit": 2000 }
//
// Or with dryRun:true to preview without writing.

const TRACKING_RECONCILE_MAX_PER_RUN = 100;

/** Pure helper: build merged trackingImages + attachments arrays for one
 *  draft when one of its jobs has hit "ready". Returns the new arrays and
 *  a `mutated` flag indicating whether anything actually changed.
 *  Operator edits to other fields (queuedForSend, suggestedListings,
 *  text, etc.) are preserved — we only touch the entries that match
 *  the jobId AND are not already ready.
 */
function _trackingApplyJobToArrays(trackingImages, attachments, jobId, job) {
  let mutated = false;
  const newTrackingImages = (trackingImages || []).map(img => {
    if (img && img.jobId === jobId && img.status !== "ready") {
      mutated = true;
      return {
        ...img,
        status           : "ready",
        carrier          : job.carrier          ?? img.carrier          ?? null,
        carrierDisplay   : job.carrierDisplay   ?? img.carrierDisplay   ?? null,
        statusText       : job.statusText       ?? img.statusText       ?? null,
        statusKey        : job.statusKey        ?? img.statusKey        ?? null,
        estimatedDelivery: job.estimatedDelivery ?? img.estimatedDelivery ?? null,
        destination      : job.destination      ?? img.destination      ?? null,
        imageUrl         : job.imageUrl,
        imageStoragePath : job.imageStoragePath,
        imageWidth       : job.imageWidth       ?? img.imageWidth       ?? null,
        imageHeight      : job.imageHeight      ?? img.imageHeight      ?? null,
        eventCount       : Array.isArray(job.events) ? job.events.length : (img.eventCount || 0),
        latestEvent      : Array.isArray(job.events) && job.events.length ? job.events[0] : (img.latestEvent || null)
      };
    }
    return img;
  });
  const newAttachments = (attachments || []).map(a => {
    if (a && a.jobId === jobId && a.type === "tracking_image" && a.status !== "ready") {
      mutated = true;
      return {
        ...a,
        status          : "ready",
        imageUrl        : job.imageUrl,
        imageStoragePath: job.imageStoragePath,
        imageWidth      : job.imageWidth      ?? a.imageWidth      ?? null,
        imageHeight     : job.imageHeight     ?? a.imageHeight     ?? null,
        carrier         : job.carrier         ?? a.carrier         ?? null,
        carrierDisplay  : job.carrierDisplay  ?? a.carrierDisplay  ?? null,
        statusKey       : job.statusKey       ?? a.statusKey       ?? null,
        statusText      : job.statusText      ?? a.statusText      ?? null
      };
    }
    return a;
  });
  return { newTrackingImages, newAttachments, mutated };
}

/** Reconcile a single draft: look up each referenced job, and if any are
 *  ready, transactionally write the merged state. Best-effort; per-draft
 *  errors are caught and reported in the outcome row. */
async function _reconcileOneDraft(draftRef, dryRun) {
  const outcome = {
    draftId         : draftRef.id,
    jobsExamined    : [],
    jobsReconciled  : [],
    jobsNotReady    : [],
    jobsMissing     : [],
    error           : null
  };

  let draftSnap;
  try {
    draftSnap = await draftRef.get();
  } catch (e) {
    outcome.error = `Draft read failed: ${e.message}`;
    return outcome;
  }
  if (!draftSnap.exists) {
    outcome.error = "Draft not found";
    return outcome;
  }

  const draft = draftSnap.data() || {};
  const trackingImages = Array.isArray(draft.trackingImages) ? draft.trackingImages : [];
  const pending = trackingImages.filter(i => i && i.jobId && i.status !== "ready");
  if (!pending.length) return outcome;

  // Fetch each referenced job once (outside txn — jobs are immutable
  // once ready, so no consistency issue).
  const jobsByJobId = {};
  for (const img of pending) {
    if (jobsByJobId[img.jobId] !== undefined) continue;
    outcome.jobsExamined.push(img.jobId);
    try {
      const js = await db.collection(TRACKING_JOBS_COLL).doc(img.jobId).get();
      if (!js.exists) {
        jobsByJobId[img.jobId] = null;
        outcome.jobsMissing.push(img.jobId);
        continue;
      }
      const jd = js.data() || {};
      if (jd.status !== "ready") {
        jobsByJobId[img.jobId] = null;
        outcome.jobsNotReady.push({ jobId: img.jobId, status: jd.status || "unknown" });
        continue;
      }
      jobsByJobId[img.jobId] = jd;
    } catch (e) {
      jobsByJobId[img.jobId] = null;
      outcome.error = (outcome.error ? outcome.error + " | " : "") + `Job ${img.jobId} read failed: ${e.message}`;
    }
  }

  const readyJobIds = Object.keys(jobsByJobId).filter(k => jobsByJobId[k]);
  if (!readyJobIds.length) return outcome;
  outcome.jobsReconciled = readyJobIds;
  if (dryRun) return outcome;

  try {
    await db.runTransaction(async (tx) => {
      const snap = await tx.get(draftRef);
      if (!snap.exists) throw new Error("Draft disappeared mid-transaction");
      const cur = snap.data() || {};
      let nextTracking = Array.isArray(cur.trackingImages) ? cur.trackingImages : [];
      let nextAttach   = Array.isArray(cur.attachments)    ? cur.attachments    : [];
      let anyMutation  = false;
      for (const jobId of readyJobIds) {
        const r = _trackingApplyJobToArrays(nextTracking, nextAttach, jobId, jobsByJobId[jobId]);
        if (r.mutated) {
          nextTracking = r.newTrackingImages;
          nextAttach   = r.newAttachments;
          anyMutation  = true;
        }
      }
      if (!anyMutation) return;  // operator may have already ack'd
      tx.set(draftRef, {
        trackingImages           : nextTracking,
        attachments              : nextAttach,
        updatedAt                : FV.serverTimestamp(),
        reconciledByReaperAt     : FV.serverTimestamp(),
        reconciledByReaperJobs   : FV.arrayUnion(...readyJobIds)
      }, { merge: true });
    });
    await writeAudit(draft.threadId || null, draftRef.id, "draft_tracking_reconciled_by_reaper", {
      jobIds      : readyJobIds,
      mode        : "tracking_reconcile",
      jobsExamined: outcome.jobsExamined.length,
      jobsNotReady: outcome.jobsNotReady.length,
      jobsMissing : outcome.jobsMissing.length
    });
  } catch (e) {
    outcome.error = (outcome.error ? outcome.error + " | " : "") + `Transaction failed: ${e.message}`;
    outcome.jobsReconciled = [];
  }

  return outcome;
}

/**
 * Top-level entry for the tracking_reconcile pass. Conforms to the
 * runFooPass() shape used by the other reapers — returns { pass,
 * scanned, hadPending, reaped, errors, ... }. `reaped` mirrors the
 * convention used by sibling passes (counts drafts touched, not jobs)
 * so the top-level totalReaped summation in the dispatcher remains
 * meaningful.
 *
 * Inputs (from event body, merged with defaults):
 *   limit   — max drafts to scan this pass (newest-first). default 100.
 *             Owner can bump for bulk migration: { op:"tracking_reconcile",
 *             limit:2000 }
 *   dryRun  — when true, examines but does not write. default false.
 *   verbose — when true, includes per-draft outcome rows in the result.
 */
async function runTrackingReconcilePass({ limit, dryRun = false, verbose = false } = {}) {
  const tStart = Date.now();
  const effectiveLimit = Math.min(Math.max(Number(limit) || TRACKING_RECONCILE_MAX_PER_RUN, 1), 2000);

  const summary = {
    pass            : "tracking_reconcile",
    scanned         : 0,
    hadPending      : 0,
    reaped          : 0,   // drafts actually reconciled (mirror sibling pass convention)
    errors          : 0,
    skippedNotReady : 0,
    skippedMissing  : 0,
    dryRun          : !!dryRun,
    limit           : effectiveLimit,
    drafts          : []
  };

  let draftsSnap;
  try {
    draftsSnap = await db.collection(DRAFTS_COLL)
      .orderBy("createdAt", "desc")
      .limit(effectiveLimit)
      .get();
  } catch (e) {
    summary.errors = 1;
    summary.errorMessage = `Draft scan failed: ${e.message}`;
    summary.durationMs = Date.now() - tStart;
    return summary;
  }

  for (const doc of draftsSnap.docs) {
    summary.scanned++;
    const draft = doc.data();
    const trackingImages = Array.isArray(draft.trackingImages) ? draft.trackingImages : [];
    const hasPending = trackingImages.some(i => i && i.jobId && i.status !== "ready");
    if (!hasPending) continue;
    summary.hadPending++;

    const outcome = await _reconcileOneDraft(doc.ref, dryRun);
    if (outcome.error) summary.errors++;
    if (outcome.jobsReconciled.length) summary.reaped++;
    summary.skippedNotReady += outcome.jobsNotReady.length;
    summary.skippedMissing  += outcome.jobsMissing.length;
    if (verbose) summary.drafts.push(outcome);
  }

  summary.durationMs = Date.now() - tStart;
  return summary;
}

// ═══════════════════════════════════════════════════════════════════════
//  Pass — Storage TTL reaper (v5.31)
// ═══════════════════════════════════════════════════════════════════════
//
// Firebase Storage has no automatic lifecycle policy in this project,
// so several write paths leak files forever:
//
//   1. etsymail/tracking/<code>.png — generated by the tracking-image
//      pipeline. One PNG per shipment. Once the package is delivered,
//      the cache entry usually goes stale and the PNG is never read
//      again; nothing currently deletes it.
//
//   2. etsymail/{threadId}/** — email-attachment mirrors written when
//      the snapshot worker fulfills storageMirrorState:"pending". These
//      are only deleted when the thread is manually deleted via
//      firestoreProxy deleteThread. Threads that get archived or
//      sales-closed retain their mirrored bytes indefinitely.
//
//   3. listing-generator-1/{category}/Ready_To_List/Set_N/** — slots
//      from listing-generator runs that were abandoned (operator never
//      approved or deleted them). These accumulate as the generator
//      fills new slot numbers on subsequent runs.
//
// SAFETY POSTURE
// --------------
// Deletion is destructive and silent (bucket.file().delete() leaves
// no trail). To prevent a buggy reaper from nuking active assets, this
// pass is:
//
//   • OPT-IN. Default behavior on a scheduled invocation is dry-run
//     (count what would be deleted, write nothing). To actually
//     delete, the caller must pass { apply: true } in the body OR
//     set the env var STORAGE_TTL_APPLY=1.
//
//   • EXCLUDED from the default "all" dispatch. The handler at the
//     bottom of this file only runs storage_ttl when the request body
//     specifies { op: "storage_ttl" }. Default scheduled runs (the
//     5-minute cron) will NOT touch storage.
//
//   • BOUNDED. Default cap is 500 deletions per invocation. The
//     operator can tune via { limit: N } for one-off cleanups.
//
//   • CATEGORY-SCOPED. Each prefix has its own TTL and "is this safe"
//     check (e.g., tracking PNGs reference a cache doc; we won't
//     delete a PNG whose cache doc says the package is still in
//     transit). Categories can be selectively enabled/disabled via
//     body flags so the operator can verify one at a time.
//
//   • AUDIT-LOGGED. Every actual deletion writes one EtsyMail_Audit
//     row with the full list of deleted paths and reasons.
//
// USAGE
// -----
//   Dry-run (default; see what would happen):
//     POST /etsyMailReapers { "op": "storage_ttl" }
//
//   Apply one category at a time to verify:
//     POST /etsyMailReapers { "op": "storage_ttl", "apply": true,
//                             "categories": ["tracking"] }
//
//   Apply everything (after dry-runs look reasonable):
//     POST /etsyMailReapers { "op": "storage_ttl", "apply": true }
//
//   Tune TTLs for a one-off:
//     POST /etsyMailReapers { "op": "storage_ttl", "apply": true,
//                             "trackingTtlDays": 14,
//                             "threadMirrorTtlDays": 60,
//                             "abandonedSetTtlDays": 90 }

const STORAGE_TTL_DEFAULTS = Object.freeze({
  trackingTtlDays    : 30,   // PNGs older than 30d, delivered or unreferenced
  threadMirrorTtlDays: 90,   // Mirrors for closed/archived threads >90d old
  abandonedSetTtlDays: 60,   // Ready_To_List/Set_N folders >60d old with no approved-parent
  maxDeletionsPerRun : 500,
});

const STORAGE_TTL_CATEGORIES = Object.freeze(["tracking", "thread_mirrors", "abandoned_sets"]);

// Threads whose mirrored attachments are safe to drop. These statuses
// indicate the conversation is no longer active. Anything in-flight
// (review queue, sales funnel mid-conversation, send queue) is NEVER
// touched even if old.
const MIRROR_DROPPABLE_STATUSES = new Set([
  "archived",
  "sales_completed",
  "sales_abandoned",
  "auto_replied",
  "sent",
]);

// Tracking status keys whose PNG is safe to drop once it's old enough.
// We keep PNGs whose package is still en route, even if the cache doc
// is old (carrier APIs sometimes go quiet for days).
const TRACKING_DROPPABLE_STATUS_KEYS = new Set([
  "delivered",
  "returned",
]);

/**
 * Sub-sweep: orphaned + stale tracking PNGs at etsymail/tracking/.
 *
 * For each file under the prefix:
 *   - Look up the cache doc EtsyMail_TrackingCache/<code> (where <code>
 *     is the filename minus ".png").
 *   - If the doc doesn't exist OR statusKey is in TRACKING_DROPPABLE_STATUS_KEYS
 *     AND the file is older than the TTL → safe to delete.
 *   - Otherwise: keep.
 *
 * Returns { scanned, eligible, deleted, errors, samplePaths }.
 */
async function _reapStorageTrackingTTL({ apply, ttlDays, maxDeletions, bucket }) {
  const cutoffMs = Date.now() - ttlDays * 24 * 60 * 60 * 1000;
  const prefix = "etsymail/tracking/";
  const out = { scanned: 0, eligible: 0, deleted: 0, errors: 0, samplePaths: [] };

  let files;
  try {
    [files] = await bucket.getFiles({ prefix });
  } catch (e) {
    out.errors++;
    out.errorMessage = `getFiles failed: ${e.message}`;
    return out;
  }

  for (const f of files) {
    if (out.deleted >= maxDeletions) break;
    out.scanned++;

    // Skip pseudo-folder entries (paths ending with /)
    if (!f.name || f.name.endsWith("/")) continue;

    let createdMs = 0;
    try {
      const meta = f.metadata || (await f.getMetadata())[0] || {};
      createdMs = new Date(meta.timeCreated || meta.updated || 0).getTime();
    } catch { /* fall through with 0 → won't match cutoff */ }
    if (!createdMs || createdMs > cutoffMs) continue;

    // Derive tracking code from filename.
    const tail = f.name.slice(prefix.length);
    const code = tail.replace(/\.png$/i, "");
    if (!code) continue;

    // Look up cache doc.
    let droppable = false;
    try {
      const snap = await db.collection("EtsyMail_TrackingCache").doc(code).get();
      if (!snap.exists) {
        // No cache → either the cache was wiped or this PNG is
        // orphaned. Safe to drop once past TTL.
        droppable = true;
      } else {
        const data = snap.data() || {};
        if (TRACKING_DROPPABLE_STATUS_KEYS.has(String(data.statusKey || "").toLowerCase())) {
          droppable = true;
        }
      }
    } catch (e) {
      // Treat read failures conservatively: do NOT delete on a doc-read
      // error, since we can't confirm the package is delivered.
      continue;
    }

    if (!droppable) continue;
    out.eligible++;
    if (out.samplePaths.length < 10) out.samplePaths.push(f.name);

    if (!apply) continue;

    try {
      await f.delete();
      out.deleted++;
    } catch (e) {
      out.errors++;
    }
  }

  return out;
}

/**
 * Sub-sweep: mirrored email attachments under etsymail/{threadId}/.
 *
 * Logic:
 *   - Group files by threadId (the path segment after etsymail/).
 *   - For each threadId, look up the thread doc.
 *   - If the thread doesn't exist (deleted by some other path) OR
 *     thread.status is in MIRROR_DROPPABLE_STATUSES AND thread.updatedAt
 *     is older than the TTL → all files under that prefix are eligible.
 *   - Otherwise: keep everything for that thread.
 *
 * NB: We never scan etsymail/drafts/, etsymail/tracking/, etsymail/
 * collateral-related paths — only direct etsymail/{threadId}/ trees
 * where threadId matches the etsy_conv_* pattern.
 */
async function _reapStorageThreadMirrorsTTL({ apply, ttlDays, maxDeletions, bucket }) {
  const cutoffMs = Date.now() - ttlDays * 24 * 60 * 60 * 1000;
  const out = { scanned: 0, eligibleThreads: 0, deleted: 0, errors: 0, samplePaths: [] };

  let files;
  try {
    [files] = await bucket.getFiles({ prefix: "etsymail/" });
  } catch (e) {
    out.errors++;
    out.errorMessage = `getFiles failed: ${e.message}`;
    return out;
  }

  // Group by threadId. Only consider files whose path matches
  // etsymail/etsy_conv_XXX/...; ignore the drafts/, tracking/, and
  // collateral-related subtrees (they have their own sweeps or are
  // explicitly out-of-scope).
  const byThread = new Map();
  for (const f of files) {
    out.scanned++;
    if (!f.name || f.name.endsWith("/")) continue;
    const tail = f.name.slice("etsymail/".length);
    const firstSeg = tail.split("/")[0];
    if (!/^etsy_conv_[A-Za-z0-9_-]+$/.test(firstSeg)) continue;
    if (!byThread.has(firstSeg)) byThread.set(firstSeg, []);
    byThread.get(firstSeg).push(f);
  }

  for (const [threadId, threadFiles] of byThread) {
    if (out.deleted >= maxDeletions) break;

    let droppable = false;
    try {
      const snap = await db.collection(THREADS_COLL).doc(threadId).get();
      if (!snap.exists) {
        // Thread doc gone but files remain → orphan. Drop.
        droppable = true;
      } else {
        const data = snap.data() || {};
        if (!MIRROR_DROPPABLE_STATUSES.has(String(data.status || ""))) continue;
        const updatedMs = data.updatedAt && data.updatedAt.toMillis
          ? data.updatedAt.toMillis()
          : 0;
        if (updatedMs && updatedMs < cutoffMs) droppable = true;
      }
    } catch (e) {
      // Conservative: don't delete on read failure.
      continue;
    }

    if (!droppable) continue;
    out.eligibleThreads++;

    for (const f of threadFiles) {
      if (out.deleted >= maxDeletions) break;
      if (out.samplePaths.length < 10) out.samplePaths.push(f.name);
      if (!apply) continue;
      try {
        await f.delete();
        out.deleted++;
      } catch (e) {
        out.errors++;
      }
    }
  }

  return out;
}

/**
 * Sub-sweep: abandoned listing-generator sets.
 *
 * listing-generator-1/{category}/Ready_To_List/Set_N/** that:
 *   - have a corresponding Completed_Listing_Sets/{category}_Set_N
 *     folder (meaning the move-to-completed already ran successfully
 *     but for some reason left files behind in Ready_To_List), OR
 *   - are older than the TTL AND there's no matching Completed entry
 *     AND there's no manifest indicating an in-flight batch.
 *
 * We only consider files that have a recognizable .../Set_<n>/...
 * structure; anything else is left alone.
 *
 * To keep the cost bounded, this sub-sweep limits itself to scanning
 * each generatable category root. The category list lives in the
 * geminiImageProxy GENERATABLE_CATEGORIES set; we duplicate it here
 * because that file isn't a stable import surface.
 */
const _ABANDONED_SET_CATEGORIES = [
  "Beady_Necklace",
  "Regular_Necklace",
  "Stud_Earrings",
  "Hoop_Earrings",
  "Charms",
  "Bracelets",
];

async function _reapAbandonedListingSets({ apply, ttlDays, maxDeletions, bucket }) {
  const cutoffMs = Date.now() - ttlDays * 24 * 60 * 60 * 1000;
  const out = { scanned: 0, eligibleSets: 0, deleted: 0, errors: 0, samplePaths: [] };

  for (const cat of _ABANDONED_SET_CATEGORIES) {
    if (out.deleted >= maxDeletions) break;

    const readyPrefix = `listing-generator-1/${cat}/Ready_To_List/`;
    const completedPrefix = `listing-generator-1/Generated_Listing_Sets/Completed_Listing_Sets/${cat}_Set_`;

    let readyFiles, completedFiles;
    try {
      [readyFiles] = await bucket.getFiles({ prefix: readyPrefix });
      [completedFiles] = await bucket.getFiles({ prefix: completedPrefix });
    } catch (e) {
      out.errors++;
      continue;
    }

    // Build the set of approved Ns. If Set_42 has files under
    // Completed_Listing_Sets/{cat}_Set_42/, it was approved successfully;
    // anything left in Ready_To_List/Set_42/ is orphan debris and can be
    // dropped.
    const approvedNs = new Set();
    for (const f of completedFiles) {
      const idx = f.name.indexOf(`/${cat}_Set_`);
      if (idx === -1) continue;
      const tail = f.name.slice(idx + `/${cat}_Set_`.length);
      const n = Number(tail.split("/")[0]);
      if (Number.isFinite(n) && n > 0) approvedNs.add(n);
    }

    // Group ready files by setN.
    const bySet = new Map();
    for (const f of readyFiles) {
      out.scanned++;
      if (!f.name || f.name.endsWith("/")) continue;
      const tail = f.name.slice(readyPrefix.length);
      const m = /^Set_(\d+)\//.exec(tail);
      if (!m) continue;
      const n = Number(m[1]);
      if (!bySet.has(n)) bySet.set(n, []);
      bySet.get(n).push(f);
    }

    for (const [n, setFiles] of bySet) {
      if (out.deleted >= maxDeletions) break;

      // Determine droppability:
      const isOrphanOfApproved = approvedNs.has(n);

      // Find max timeCreated across files in this set.
      let newestMs = 0;
      for (const f of setFiles) {
        try {
          const meta = f.metadata || {};
          const t = new Date(meta.timeCreated || meta.updated || 0).getTime();
          if (t > newestMs) newestMs = t;
        } catch {}
      }
      const isStale = newestMs > 0 && newestMs < cutoffMs;

      if (!isOrphanOfApproved && !isStale) continue;

      out.eligibleSets++;
      for (const f of setFiles) {
        if (out.deleted >= maxDeletions) break;
        if (out.samplePaths.length < 10) out.samplePaths.push(f.name);
        if (!apply) continue;
        try {
          await f.delete();
          out.deleted++;
        } catch (e) {
          out.errors++;
        }
      }
    }
  }

  return out;
}

/**
 * Top-level entry for the storage_ttl pass. See block comment above
 * STORAGE_TTL_DEFAULTS for behavior and safety posture.
 */
async function runStorageTtlPass(opts = {}) {
  const tStart = Date.now();
  const apply = opts.apply === true || process.env.STORAGE_TTL_APPLY === "1";
  const trackingTtlDays     = Number(opts.trackingTtlDays)     || STORAGE_TTL_DEFAULTS.trackingTtlDays;
  const threadMirrorTtlDays = Number(opts.threadMirrorTtlDays) || STORAGE_TTL_DEFAULTS.threadMirrorTtlDays;
  const abandonedSetTtlDays = Number(opts.abandonedSetTtlDays) || STORAGE_TTL_DEFAULTS.abandonedSetTtlDays;
  const maxDeletions        = Math.max(1, Number(opts.limit) || STORAGE_TTL_DEFAULTS.maxDeletionsPerRun);

  // Category selection. Default = all; operator can restrict to one or
  // more for a careful first run.
  const requested = Array.isArray(opts.categories) && opts.categories.length
    ? opts.categories.filter((c) => STORAGE_TTL_CATEGORIES.includes(c))
    : STORAGE_TTL_CATEGORIES.slice();

  const summary = {
    pass        : "storage_ttl",
    apply,
    trackingTtlDays,
    threadMirrorTtlDays,
    abandonedSetTtlDays,
    maxDeletions,
    categories  : requested,
    results     : {},
    reaped      : 0,            // for the consolidated totalReaped
    durationMs  : 0,
  };

  let bucket;
  try {
    bucket = admin.storage().bucket();
  } catch (e) {
    summary.error = `storage bucket unavailable: ${e.message}`;
    summary.durationMs = Date.now() - tStart;
    return summary;
  }

  // Run each requested sub-sweep with its own bounded share of the
  // deletion budget so one category can't starve the others.
  const perCatBudget = Math.max(1, Math.floor(maxDeletions / requested.length));

  if (requested.includes("tracking")) {
    summary.results.tracking = await _reapStorageTrackingTTL({
      apply, ttlDays: trackingTtlDays, maxDeletions: perCatBudget, bucket,
    });
    summary.reaped += summary.results.tracking.deleted || 0;
  }
  if (requested.includes("thread_mirrors")) {
    summary.results.thread_mirrors = await _reapStorageThreadMirrorsTTL({
      apply, ttlDays: threadMirrorTtlDays, maxDeletions: perCatBudget, bucket,
    });
    summary.reaped += summary.results.thread_mirrors.deleted || 0;
  }
  if (requested.includes("abandoned_sets")) {
    summary.results.abandoned_sets = await _reapAbandonedListingSets({
      apply, ttlDays: abandonedSetTtlDays, maxDeletions: perCatBudget, bucket,
    });
    summary.reaped += summary.results.abandoned_sets.deleted || 0;
  }

  // Audit row — written only on apply runs. Dry runs are visible in
  // the response/logs but don't write to the audit collection (which
  // is reserved for state-changing events).
  if (apply && summary.reaped > 0) {
    try {
      await db.collection(AUDIT_COLL).add({
        threadId  : null,
        draftId   : null,
        eventType : "storage_ttl_reaped",
        actor     : "system:reapers",
        payload   : summary,
        createdAt : FV.serverTimestamp(),
        outcome   : "success",
      });
    } catch (e) {
      summary.auditError = e.message;
    }
  }

  summary.durationMs = Date.now() - tStart;
  return summary;
}

/**
 * Top-level entry for the gmail_scrape pass. Runs four sub-sweeps in
 * order and returns a combined summary. Each sub-sweep wraps its own
 * try/catch so a partial failure on one doesn't block the others.
 */
async function runGmailScrapePass() {
  const tStart = Date.now();
  const summary = {
    pass                       : "gmail_scrape",
    stuckClaimsRequeued        : 0,
    stuckClaimsExhausted       : 0,
    detectedThreadsRequeued    : 0,
    detectedThreadsExhausted   : 0,
    detectedThreadsSkippedAlive: 0,
    detectedNamesFilledFromSubject: 0,
    unknownThreadsRetried      : 0,
    unknownThreadsSkipped      : 0,
    unknownNamesFilledFromSubject : 0,
    mangledUnicodeFixed        : 0,
    mangledUnicodeScanned      : 0,
    reaped                     : 0,    // for the consolidated totalReaped
    subErrors                  : [],
    durationMs                 : 0
  };

  try {
    const a = await reapStuckScrapeClaims();
    summary.stuckClaimsRequeued  = a.requeued;
    summary.stuckClaimsExhausted = a.exhausted;
  } catch (e) {
    summary.subErrors.push({ subsweep: "A_stuck_claims", error: e.message });
    console.error("[gmail-scrape-reaper] subsweep A failed:", e);
  }

  try {
    const b = await reapDetectedThreadsWithoutLiveJobs();
    summary.detectedThreadsRequeued        = b.requeued;
    summary.detectedThreadsExhausted       = b.exhausted;
    summary.detectedThreadsSkippedAlive    = b.skippedAlive;
    summary.detectedNamesFilledFromSubject = b.namesFilledFromSubject || 0;
  } catch (e) {
    summary.subErrors.push({ subsweep: "B_detected_orphans", error: e.message });
    console.error("[gmail-scrape-reaper] subsweep B failed:", e);
  }

  try {
    const c = await reapUnknownAfterScrape();
    summary.unknownThreadsRetried        = c.retried;
    summary.unknownThreadsSkipped        = c.skipped;
    summary.unknownNamesFilledFromSubject = c.namesFilledFromSubject || 0;
  } catch (e) {
    summary.subErrors.push({ subsweep: "C_unknown_retry", error: e.message });
    console.error("[gmail-scrape-reaper] subsweep C failed:", e);
  }

  try {
    const d = await reapMangledUnicodeFields();
    summary.mangledUnicodeFixed   = d.fixed;
    summary.mangledUnicodeScanned = d.scanned;
  } catch (e) {
    summary.subErrors.push({ subsweep: "D_unmangle", error: e.message });
    console.error("[gmail-scrape-reaper] subsweep D failed:", e);
  }

  // Roll up "reaped" for the handler's totalReaped tally. Counts every
  // action that produced a downstream effect: job requeued, job
  // exhausted-and-marked-failed, one-shot retry enqueued, customerName
  // backfilled from the email subject, OR a row repaired by the
  // unmangler. Does NOT count skipped-alive (healthy in-flight work).
  summary.reaped =
      summary.stuckClaimsRequeued
    + summary.stuckClaimsExhausted
    + summary.detectedThreadsRequeued
    + summary.detectedThreadsExhausted
    + summary.unknownThreadsRetried
    + summary.detectedNamesFilledFromSubject
    + summary.unknownNamesFilledFromSubject
    + summary.mangledUnicodeFixed;

  summary.durationMs = Date.now() - tStart;
  return summary;
}

// ═══════════════════════════════════════════════════════════════════════
//  Pass — Deferred auto-pipeline fire (v3.30)
// ═══════════════════════════════════════════════════════════════════════
//
// Companion to the quiet-period debounce added in
// etsyMailAutoPipeline-background.js. When the customer stops typing
// during a quiet-period defer, nothing re-triggers the pipeline — the
// snapshot ingest only fires on new inbound messages. This pass picks
// up threads whose `autoPipelineDeferUntilMs` has elapsed and invokes
// the auto-pipeline once so the deferred draft actually gets generated.
//
// Query: lastAutoDecision == "deferred_quiet_period"
//        AND autoPipelineDeferUntilMs <= now
//
// Requires a composite Firestore index on
// (lastAutoDecision ASC, autoPipelineDeferUntilMs ASC). Firestore will
// surface a one-click index-creation link in the console the first
// time this pass runs without it.
//
// Race notes:
//   • If a new customer message lands while the reaper is firing,
//     snapshot will independently invoke the auto-pipeline. The
//     pipeline's claim transaction is atomic, so we get one extra
//     invocation in the worst case — both runs see the SAME inboundMs
//     and the second one's claim fails on the idempotency gate.
//   • If the reaper crashes after firing N of M threads, the
//     remaining M-N stay deferred and pick up on the next tick.
//     Effectively at-least-once delivery, which the pipeline tolerates
//     via its existing idempotency lock.

const MAX_DEFERRED_FIRES_PER_RUN = 100;
const DEFERRED_FIRE_TIMEOUT_MS   = 5_000;   // background-fn returns 202 in ~1s

function _functionsBase() {
  return process.env.URL
      || process.env.DEPLOY_URL
      || process.env.NETLIFY_BASE_URL
      || "http://localhost:8888";
}

async function fireDeferredThread(threadId) {
  // Background function — Netlify returns 202 immediately and runs the
  // pipeline asynchronously up to 15 min. We just need the invoke to
  // succeed; the pipeline's own writes are the result.
  const url = `${_functionsBase()}/.netlify/functions/etsyMailAutoPipeline-background`;
  const headers = { "Content-Type": "application/json" };
  if (process.env.ETSYMAIL_EXTENSION_SECRET) {
    headers["X-EtsyMail-Secret"] = process.env.ETSYMAIL_EXTENSION_SECRET;
  }
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), DEFERRED_FIRE_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      method : "POST",
      headers,
      body   : JSON.stringify({ threadId }),
      signal : controller.signal
    });
    if (res.status !== 202 && !res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`autoPipeline returned ${res.status}: ${text.slice(0, 200)}`);
    }
    return { ok: true, status: res.status };
  } finally {
    clearTimeout(t);
  }
}

async function runDeferredAutoPipelinePass() {
  const tStart = Date.now();
  const now    = Date.now();

  // v3.32 — single-field query + in-code time filter. The prior
  // implementation used a composite (lastAutoDecision, autoPipelineDefer-
  // UntilMs) query which required a one-click Firestore index that
  // had to be created manually after first deploy. If the index was
  // never created, the query failed and the entire deferred pass
  // silently no-op'd every 5 minutes — fail-safe (no crash) but
  // fail-invisible (no AI drafts ever generated for deferred threads).
  //
  // The new path queries only on lastAutoDecision (single-field index
  // is auto-created by Firestore on first write of the field), pulls
  // up to MAX_DEFERRED_FIRES_PER_RUN candidates, then filters the
  // time check in memory. Trade-off: at most MAX candidates are read
  // even if some aren't ready yet, but in practice the deferred set
  // is small (10s, not 1000s) so this is negligible. No index
  // creation step required from the operator.
  let snap;
  try {
    snap = await db.collection(THREADS_COLL)
      .where("lastAutoDecision", "==", "deferred_quiet_period")
      .limit(MAX_DEFERRED_FIRES_PER_RUN)
      .get();
  } catch (e) {
    console.warn("[reapers] deferred pass query failed:", e.message);
    return {
      pass       : "deferred_auto_pipeline",
      scanned    : 0,
      fired      : 0,
      skipped    : 0,
      errors     : [e.message],
      durationMs : Date.now() - tStart
    };
  }

  if (snap.empty) {
    return {
      pass       : "deferred_auto_pipeline",
      scanned    : 0,
      fired      : 0,
      skipped    : 0,
      durationMs : Date.now() - tStart
    };
  }

  // In-code filter: only fire threads whose defer window has elapsed.
  // Sort ascending by defer time so the oldest-overdue thread fires
  // first (matches the prior orderBy behavior).
  const candidates = snap.docs
    .map(doc => ({
      id   : doc.id,
      data : doc.data() || {},
      deferUntilMs: (() => {
        const v = (doc.data() || {}).autoPipelineDeferUntilMs;
        return typeof v === "number" ? v : 0;
      })()
    }))
    .filter(c => c.deferUntilMs > 0 && c.deferUntilMs <= now)
    .sort((a, b) => a.deferUntilMs - b.deferUntilMs);

  if (candidates.length === 0) {
    return {
      pass       : "deferred_auto_pipeline",
      scanned    : snap.size,
      fired      : 0,
      skipped    : 0,
      notReadyYet: snap.size,   // pulled but not yet eligible
      durationMs : Date.now() - tStart
    };
  }

  let fired   = 0;
  let skipped = 0;
  const errors = [];

  for (const candidate of candidates) {
    const threadId = candidate.id;
    const deferUntilMs = candidate.deferUntilMs;

    try {
      await fireDeferredThread(threadId);
      fired++;
      writeAudit(threadId, null, "auto_pipeline_deferred_fired", {
        deferUntilMs,
        deferOverdueMs: now - deferUntilMs
      }).catch((e) => console.warn("[reapers] deferred audit failed:", e.message));
    } catch (e) {
      skipped++;
      errors.push({ threadId, error: e.message });
      console.warn(`[reapers] deferred fire failed for ${threadId}:`, e.message);
    }
  }

  return {
    pass       : "deferred_auto_pipeline",
    scanned    : snap.size,
    eligible   : candidates.length,
    fired      : fired,
    skipped    : skipped,
    errors     : errors.length ? errors : undefined,
    durationMs : Date.now() - tStart
  };
}

// ═══════════════════════════════════════════════════════════════════════
//  Handler
// ═══════════════════════════════════════════════════════════════════════

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  const scheduled = isScheduledInvocation(event);

  if (!scheduled && event.httpMethod) {
    const auth = requireExtensionAuth(event);
    if (!auth.ok) return auth.response;
  }

  // Optional body: `{ op: "auto_pipeline" | "send_queue" | "sales_funnels" |
  //                       "gmail_scrape", force?: bool }`
  // for targeted manual sweeps. Default is to run all passes.
  let body = {};
  if (event.body) {
    try { body = JSON.parse(event.body); } catch { body = {}; }
  }
  const op    = body.op || null;
  const force = body.force === true;

  const tStart = Date.now();
  const results = {};
  const errors  = [];

  try {
    if (!op || op === "auto_pipeline") {
      try { results.autoPipeline = await runAutoPipelinePass(); }
      catch (e) { errors.push({ pass: "auto_pipeline", error: e.message }); console.error("autoPipeline pass:", e); }
    }
    // v3.30 — Deferred auto-pipeline fire. Companion to the quiet-period
    // debounce in etsyMailAutoPipeline-background. Picks up threads where
    // the quiet window has elapsed with no new inbound and re-fires the
    // pipeline once so the deferred draft actually generates.
    if (!op || op === "deferred_auto_pipeline") {
      try { results.deferredAutoPipeline = await runDeferredAutoPipelinePass(); }
      catch (e) { errors.push({ pass: "deferred_auto_pipeline", error: e.message }); console.error("deferredAutoPipeline pass:", e); }
    }
    if (!op || op === "send_queue") {
      try { results.sendQueue = await runSendQueuePass(); }
      catch (e) { errors.push({ pass: "send_queue", error: e.message }); console.error("sendQueue pass:", e); }
    }
    if (!op || op === "sales_funnels") {
      try { results.salesFunnels = await runSalesFunnelPass({ force }); }
      catch (e) { errors.push({ pass: "sales_funnels", error: e.message }); console.error("salesFunnels pass:", e); }
    }
    if (!op || op === "gmail_scrape") {
      try { results.gmailScrape = await runGmailScrapePass(); }
      catch (e) { errors.push({ pass: "gmail_scrape", error: e.message }); console.error("gmailScrape pass:", e); }
    }
    // v3.27 — Tracking-attachment reconciliation. Default scan size is
    // 100 drafts (newest-first); owner can override via { limit, dryRun,
    // verbose } in the request body for bulk migration of pre-v3.27
    // stuck drafts. Runs on the same cron schedule as the other passes
    // so the system is self-healing without intervention.
    if (!op || op === "tracking_reconcile") {
      try {
        results.trackingReconcile = await runTrackingReconcilePass({
          limit  : body.limit,
          dryRun : body.dryRun === true || body.dryRun === "1" || body.dryRun === "true",
          verbose: body.verbose === true || body.verbose === "1" || body.verbose === "true"
        });
      }
      catch (e) { errors.push({ pass: "tracking_reconcile", error: e.message }); console.error("trackingReconcile pass:", e); }
    }

    // v5.31 — Storage TTL pass. EXPLICITLY OPT-IN (no `!op ||` here).
    // Default invocations of this function (the 5-minute cron and any
    // generic "run all passes" call) skip this pass entirely so an
    // unattended cron tick can never delete bytes. The pass only
    // executes when the caller explicitly sets { op: "storage_ttl" }.
    //
    // The pass itself also defaults to dry-run; actual deletion requires
    // `{ apply: true }` or env STORAGE_TTL_APPLY=1. See the block comment
    // above runStorageTtlPass for the full safety story.
    if (op === "storage_ttl") {
      try {
        results.storageTtl = await runStorageTtlPass({
          apply               : body.apply === true || body.apply === "1" || body.apply === "true",
          trackingTtlDays     : body.trackingTtlDays,
          threadMirrorTtlDays : body.threadMirrorTtlDays,
          abandonedSetTtlDays : body.abandonedSetTtlDays,
          limit               : body.limit,
          categories          : body.categories,
        });
      }
      catch (e) { errors.push({ pass: "storage_ttl", error: e.message }); console.error("storageTtl pass:", e); }
    }

    const totalReaped =
        ((results.autoPipeline      && results.autoPipeline.reaped)      || 0)
      + ((results.sendQueue         && results.sendQueue.reaped)         || 0)
      + ((results.salesFunnels      && results.salesFunnels.reaped)      || 0)
      + ((results.gmailScrape       && results.gmailScrape.reaped)       || 0)
      + ((results.trackingReconcile && results.trackingReconcile.reaped) || 0)
      + ((results.storageTtl        && results.storageTtl.reaped)        || 0);

    const summary = {
      success    : errors.length === 0,
      ranOp      : op || "all",
      totalReaped,
      results,
      errors,
      durationMs : Date.now() - tStart,
      ranAt      : new Date().toISOString()
    };

    if (totalReaped > 0 || errors.length > 0) {
      console.log("etsyMailReapers:", JSON.stringify(summary));
    }

    return json(errors.length === 0 ? 200 : 207, summary);

  } catch (err) {
    console.error("etsyMailReapers unhandled error:", err);
    return json(500, { error: err.message || String(err), durationMs: Date.now() - tStart });
  }
};

// Exports for tests / manual debugging.
module.exports.runAutoPipelinePass         = runAutoPipelinePass;
module.exports.runDeferredAutoPipelinePass = runDeferredAutoPipelinePass;
module.exports.runSendQueuePass            = runSendQueuePass;
module.exports.runSalesFunnelPass          = runSalesFunnelPass;
module.exports.runGmailScrapePass          = runGmailScrapePass;
module.exports.runStorageTtlPass           = runStorageTtlPass;
module.exports.reapStaleClaim              = reapStaleClaim;
module.exports.reapStaleDraft              = reapStaleDraft;
module.exports.reapAbandonedSalesThread    = reapAbandonedSalesThread;
module.exports.reapStuckScrapeClaims       = reapStuckScrapeClaims;
module.exports.reapDetectedThreadsWithoutLiveJobs = reapDetectedThreadsWithoutLiveJobs;
module.exports.reapUnknownAfterScrape      = reapUnknownAfterScrape;
module.exports.tryFillCustomerNameFromSubject = tryFillCustomerNameFromSubject;
module.exports.extractCustomerNameFromSubject = extractCustomerNameFromSubject;
module.exports.reapMangledUnicodeFields    = reapMangledUnicodeFields;
module.exports.unmangleEscapedUnicode      = unmangleEscapedUnicode;
module.exports.hasMangledEscapes           = hasMangledEscapes;
module.exports.REAPABLE_STAGES             = Array.from(REAPABLE_STAGES);
module.exports.STALE_CLAIM_THRESHOLD_MS    = STALE_CLAIM_THRESHOLD_MS;
module.exports.SALES_SCAN_INTERVAL_MS      = SALES_SCAN_INTERVAL_MS;
module.exports.SCRAPE_STUCK_CLAIM_MS       = SCRAPE_STUCK_CLAIM_MS;
module.exports.SCRAPE_DETECTED_GRACE_MS    = SCRAPE_DETECTED_GRACE_MS;
module.exports.SCRAPE_UNKNOWN_GRACE_MS     = SCRAPE_UNKNOWN_GRACE_MS;
