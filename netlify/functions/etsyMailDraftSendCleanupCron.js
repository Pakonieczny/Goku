/*  netlify/functions/etsyMailDraftSendCleanupCron.js
 *
 *  Scheduled rescue for M5 sends that got stranded. An extension can
 *  crash mid-send — tab closed, laptop lid shut, service worker evicted —
 *  leaving a draft stuck in status="sending" forever. Nobody else can
 *  claim it because claim requires status=queued OR stale-heartbeat.
 *
 *  This cron runs every few minutes, finds drafts where:
 *    - status == "sending"
 *    - sendHeartbeatAt is older than STALE_HEARTBEAT_MS
 *  …and resets them to "queued" (or "failed" if they've already hit the
 *  retry ceiling). Next peek from any extension picks them back up.
 *
 *  Scheduling: configured in netlify.toml:
 *      [functions."etsyMailDraftSendCleanupCron"]
 *      schedule = "every 3 minutes"
 *
 *  Runtime envelope: 30 seconds (Netlify scheduled-function cap). This
 *  query is cheap (indexed where + small limit) so 30s is plenty.
 *
 *  Telemetry: writes a summary entry to EtsyMail_Audit each run so the
 *  operator UI can surface "N stranded sends recovered today".
 */

const admin = require("./firebaseAdmin");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const DRAFTS_COLL  = "EtsyMail_Drafts";
const AUDIT_COLL   = "EtsyMail_Audit";
const THREADS_COLL = "EtsyMail_Threads";

const STALE_HEARTBEAT_MS = 60 * 1000;   // must match etsyMailDraftSend.js
const MAX_SEND_ATTEMPTS  = 3;
const SCAN_LIMIT         = 50;          // one pass; if more, next tick catches them

exports.handler = async () => {
  const started = Date.now();
  const cutoffMs = started - STALE_HEARTBEAT_MS;
  const cutoffTs = admin.firestore.Timestamp.fromMillis(cutoffMs);

  let scanned = 0, requeued = 0, failed = 0, sentUnverified = 0, skipped = 0;
  const actions = [];

  try {
    // Query: all drafts currently "sending". We then filter by heartbeat
    // client-side because Firestore won't combine inequality filters on
    // different fields without a composite index (and the cost of
    // scanning all "sending" drafts is tiny — there are rarely >50).
    const snap = await db.collection(DRAFTS_COLL)
      .where("status", "==", "sending")
      .limit(SCAN_LIMIT)
      .get();

    scanned = snap.size;
    if (!scanned) {
      return {
        statusCode: 200,
        body: JSON.stringify({ ok: true, scanned: 0, elapsedMs: Date.now() - started })
      };
    }

    for (const doc of snap.docs) {
      const data = doc.data();
      const hbTs = data.sendHeartbeatAt;
      const hbMs = hbTs && hbTs.toMillis ? hbTs.toMillis() : 0;
      if (hbMs >= cutoffMs) {
        // Still alive — extension heartbeating normally. Leave it.
        skipped++;
        continue;
      }

      // Transaction per doc: race-safe against a late heartbeat arriving
      // at the same moment (unlikely but possible).
      try {
        await db.runTransaction(async (tx) => {
          // ── ALL READS FIRST (Firestore txn rule) ──────────────────
          const fresh = await tx.get(doc.ref);
          if (!fresh.exists) return;
          const d = fresh.data();
          if (d.status !== "sending") return;
          const hb = d.sendHeartbeatAt;
          const hbms = hb && hb.toMillis ? hb.toMillis() : 0;
          if (hbms >= cutoffMs) return;  // heartbeat landed between queries

          // Pre-read the parent thread doc so we can demote it in the
          // same transaction as the draft patch. Firestore transactions
          // require every read to happen before any write, so this read
          // belongs up here next to the draft read — even though we
          // only WRITE to the thread inside the post_click and
          // exhausted-attempts branches below.
          let threadShouldDemote = false;
          let threadDemoteReason = null;
          let tRef = null;
          if (d.threadId) {
            tRef = db.collection(THREADS_COLL).doc(d.threadId);
            const tSnap = await tx.get(tRef);
            if (tSnap.exists && tSnap.data().status === "queued_for_auto_send") {
              threadShouldDemote = true;
            }
          }

          // ── NOW WRITES ─────────────────────────────────────────────

          // v2.6 fix: STRANDED_POST_CLICK is NOT a failure. The Send
          // button was clicked AND the message was typed — Etsy's Send
          // button is reliable, so the message almost certainly went
          // through. The "stranded" part is just that we lost the tab
          // before getting a confirmation signal. Treating this as
          // `failed` led operators to re-send and create duplicates.
          // Now we mark it as `sent_unverified`, the same status used
          // for manual sends that timed out without confirmation:
          //   - status: "sent_unverified"
          //   - sentAt: now (so the UI's optimistic-insert path fires
          //     and the just-sent message appears in the thread view)
          //   - operator still gets demoted-to-review for verification
          // Mirrors the matching change in etsyMailReapers.js +
          // etsyMailDraftSend.js claim path.
          if (d.sendStage === "post_click") {
            const secsAgo = Math.round((Date.now() - hbms) / 1000);
            tx.set(doc.ref, {
              status        : "sent_unverified",
              sentAt        : FV.serverTimestamp(),
              sendError     : `Send was clicked (${secsAgo}s ago). Etsy didn't return a confirmation signal in the timeout window — verify on Etsy if you're uncertain. Most likely it went through; do NOT blindly re-send (would duplicate).`,
              sendErrorCode : "STRANDED_POST_CLICK",
              updatedAt     : FV.serverTimestamp()
            }, { merge: true });
            // Demote the thread out of queued_for_auto_send so the
            // rail's "sending…" badge clears. The reaper does the
            // analogous thing in etsyMailReapers.js#reapStaleDraft;
            // we duplicate it here because the cleanup cron's 3-min
            // cadence usually beats the reaper's 5-min cadence and
            // would otherwise rescue the draft first, leaving the
            // reaper nothing to demote.
            if (threadShouldDemote) {
              threadDemoteReason = "human_review_after_stranded_post_click";
              tx.set(tRef, {
                status            : "pending_human_review",
                lastAutoDecision  : threadDemoteReason,
                lastAutoDecisionAt: FV.serverTimestamp(),
                updatedAt         : FV.serverTimestamp()
              }, { merge: true });
            }
            actions.push({ draftId: doc.id, action: "sent_unverified_post_click", attempts: d.sendAttempts || 0 });
            sentUnverified++;
            return;
          }

          const attempts = d.sendAttempts || 0;
          if (attempts >= MAX_SEND_ATTEMPTS) {
            tx.set(doc.ref, {
              status        : "failed",
              sendError     : `Stranded (no heartbeat for ${Math.round((Date.now() - hbms) / 1000)}s) — retry budget exhausted`,
              sendErrorCode : "STRANDED_EXHAUSTED",
              updatedAt     : FV.serverTimestamp()
            }, { merge: true });
            // Same demotion concern as the post_click branch.
            if (threadShouldDemote) {
              threadDemoteReason = "human_review_after_retry_exhausted";
              tx.set(tRef, {
                status            : "pending_human_review",
                lastAutoDecision  : threadDemoteReason,
                lastAutoDecisionAt: FV.serverTimestamp(),
                updatedAt         : FV.serverTimestamp()
              }, { merge: true });
            }
            actions.push({ draftId: doc.id, action: "failed", attempts });
            failed++;
          } else {
            // Requeue path — the draft goes back to "queued" and the
            // thread legitimately stays at queued_for_auto_send so the
            // next claim cycle can pick it up. No thread demotion here.
            tx.set(doc.ref, {
              status        : "queued",
              sendSessionId : null,
              sendClaimedAt : null,
              sendError     : `Previous attempt stranded (no heartbeat for ${Math.round((Date.now() - hbms) / 1000)}s) — requeued for retry`,
              sendErrorCode : "STRANDED_REQUEUED",
              updatedAt     : FV.serverTimestamp()
            }, { merge: true });
            actions.push({ draftId: doc.id, action: "requeued", attempts });
            requeued++;
          }
        });
      } catch (e) {
        console.warn(`cleanup txn error for ${doc.id}:`, e.message);
      }
    }

    // Audit summary (only if we did something)
    if (requeued || failed || sentUnverified) {
      try {
        await db.collection(AUDIT_COLL).add({
          threadId  : null,
          draftId   : null,
          eventType : "send_cleanup_cron",
          actor     : "cron",
          payload   : {
            scanned, requeued, failed, sentUnverified, skipped,
            actions, elapsedMs: Date.now() - started
          },
          createdAt : FV.serverTimestamp()
        });
      } catch (e) {
        console.warn("audit write failed:", e.message);
      }
    }

    return {
      statusCode: 200,
      body: JSON.stringify({
        ok: true, scanned, requeued, failed, sentUnverified, skipped,
        elapsedMs: Date.now() - started
      })
    };

  } catch (err) {
    console.error("etsyMailDraftSendCleanupCron error:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message, scanned, requeued, failed, sentUnverified, skipped })
    };
  }
};
