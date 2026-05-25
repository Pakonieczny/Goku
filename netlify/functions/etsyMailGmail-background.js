/*  netlify/functions/etsyMailGmail-background.js  (v1.2)
 *
 *  M6 Gmail-watcher — polls a Gmail inbox for Etsy notification emails,
 *  extracts the embedded Etsy conversation link, and enqueues a `scrape`
 *  job in EtsyMail_Jobs. The existing Chrome extension picks up that job
 *  on its next poll, opens the Etsy tab, scrapes the conversation, and
 *  POSTs the snapshot — closing the loop with ZERO changes to the
 *  scraper, the snapshot ingest, or the extension itself.
 *
 *  ═══ v1.2 CHANGE LOG ══════════════════════════════════════════════════
 *
 *  GMAIL-THREAD DEDUP: When multiple matching messages share the same
 *  Gmail threadId (e.g. customer "Vanessa" sent multiple messages over
 *  weeks; Gmail groups them all under one thread), keep only the NEWEST
 *  message per thread for processing.
 *
 *  Symptom this fixes: Vanessa sends a new message at 11:14 PM. The
 *  watcher correctly fetches it, but Gmail's API returns the message
 *  with thread context that may include stale tracker URLs from older
 *  Vanessa messages. The extractor follows one of those stale trackers,
 *  which now resolves to a different (or expired) Etsy conversation —
 *  producing an inbox thread with the wrong content (e.g. "Community
 *  Forums" landing page instead of Vanessa's actual conversation).
 *
 *  Fix: Gmail returns list results newest-first. After fetching the
 *  page of stubs, walk the list newest-first and keep only the first
 *  occurrence of each threadId. This guarantees:
 *    - Each Gmail thread is processed at most once per invocation
 *    - The version processed is always the newest-arrived message
 *      (the one with the freshest tracker URLs)
 *    - The watermark still advances normally based on the newest
 *      internalDate seen across all stubs (deduplicated or not)
 *
 *  ═══ v1.1 CHANGE LOG (retained) ═══════════════════════════════════════
 *
 *  Awaits extractEtsyConversationLink() — it became async in
 *  _etsyMailGmail v1.1 to support following SendGrid click-tracking
 *  redirects (ablink.account.etsy.com → www.etsy.com/your/conversations/<id>).
 *
 *  Etsy started wrapping their "View message" CTA in tracker URLs
 *  exclusively (observed Apr 2026, sender now no-reply@account.etsy.com),
 *  so the prior regex-only extractor returned null on every email.
 *  Symptom was "scanned N · enqueued 0" with no obvious cause; v1.1
 *  resolves it by following up to 4 tracker URLs per email until one
 *  redirects to a conversation page.
 *
 *  ═══ THE LOOP ═════════════════════════════════════════════════════════
 *
 *    Etsy → email → Gmail
 *                    │
 *                    ▼  this fn (every minute)
 *                etsyMailGmail-background
 *                    │  (1) extract conversation link from email body
 *                    │  (2) upsert EtsyMail_Threads doc with status
 *                    │      "detected_from_gmail" + gmailMessageId
 *                    │  (3) enqueue EtsyMail_Jobs doc { jobType: "scrape",
 *                    │      payload: { etsyConversationUrl } }
 *                    ▼
 *                EtsyMail_Jobs (queued)
 *                    │
 *                    ▼  Chrome extension polls every 20s
 *                background.js (extension)
 *                    │  claims job → opens tab → runs content scraper
 *                    ▼
 *                etsyMailSnapshot (existing endpoint)
 *                    │  advances detected_from_gmail → etsy_scraped
 *                    │  triggers etsyMailAutoPipeline-background
 *                    ▼
 *                EtsyMail_Threads (status: etsy_scraped, message stored)
 *                    │
 *                    ▼  auto-reply pipeline + inbox UI
 *
 *  Every step except this file already exists. Status advance, dedup
 *  (by contentHash), audit logging, image mirroring, AI draft, and
 *  send queue are all handled by the existing pipeline.
 *
 *  ═══ INVOCATION ═══════════════════════════════════════════════════════
 *
 *    Scheduled  : etsyMailGmailCron.js (every 1 minute)
 *    Manual     : etsyMailGmail.js?action=trigger
 *    Direct test: POST /.netlify/functions/etsyMailGmail-background
 *
 *  Optional body: { mode: "incremental" | "full", windowDays?: 7, query?: "..." }
 *
 *  ═══ ENV VARS ═════════════════════════════════════════════════════════
 *
 *    GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET   — required for token refresh
 *    GMAIL_QUERY (optional)                 — overrides the default Gmail
 *                                             search filter. Default:
 *                                             from:notify@etsy.com
 *    GMAIL_INITIAL_WINDOW_DAYS (optional)   — how far back to look on the
 *                                             very first run. Default 7.
 *
 *  ═══ STATE ═══════════════════════════════════════════════════════════
 *
 *  EtsyMail_Config/gmailSyncState  — single doc, fields:
 *    { lastSyncInProgress, lastSyncStartedAt, lastSyncCompletedAt,
 *      lastSyncMode, lastSyncMessagesScanned, lastSyncJobsEnqueued,
 *      lastSyncSkipped, lastSyncError, lastSyncErrorAt,
 *      lastInternalDateMs            // newest message's Gmail receive time
 *    }
 *
 *  Watermark is a Gmail-side internalDate (ms epoch). Each poll asks Gmail
 *  for `after:<seconds>` — Gmail's `after:` operator takes Unix seconds.
 *  We never look backwards through history, only forward from the
 *  watermark, so the cost-per-tick stays bounded regardless of mailbox
 *  size.
 */

"use strict";

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");
const {
  listMessages,
  getMessage,
  extractEtsyConversationLink,
  summarizeMessage
} = require("./_etsyMailGmail");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

// ─── Collections / config doc paths ──────────────────────────────────────
const THREADS_COLL = "EtsyMail_Threads";
const JOBS_COLL    = "EtsyMail_Jobs";
const AUDIT_COLL   = "EtsyMail_Audit";
// v1.7 — Index collection mapping gmailMessageId → threadId. One doc per
// linked Gmail message, ID-keyed by gmailMessageId. Lets the sweep check
// "has this gmail message EVER been linked to a thread?" cheaply — without
// it the sweep used a thread-doc field (gmailMessageId) which only stores
// the MOST-RECENT-linked id per thread, causing prior emails on the same
// Etsy conversation to look "orphaned" on every tick and re-trigger the
// scrape pipeline in a flip-flop loop (see runaway audit-log bug Apr 2026).
const GMAIL_LINKS_COLL = "EtsyMail_GmailLinks";
const SYNC_STATE_DOC = "EtsyMail_Config/gmailSyncState";   // 2-segment path
// v1.7 — Hard kill-switch consulted on every sweep run, independent of the
// gmailWatcher.enabled flag the cron checks. If a future bug recurs, an
// operator can set sweepCircuitBreaker.enabled=true in Firestore and the
// sweep bails out in the next ≤1min without touching the main flow.
const SWEEP_CIRCUIT_BREAKER_DOC = "EtsyMail_Config/gmailSweepCircuitBreaker";

// ─── Tuning constants ────────────────────────────────────────────────────

// Default Gmail search filter. Picks up notification emails from EVERY
// Etsy sender, not just the original `notify@etsy.com` notification
// address.
//
// v1.6 — Broadened from `from:notify@etsy.com` to `from:etsy.com`.
// Etsy sends conversation notifications from MULTIPLE addresses:
//   - notify@etsy.com               (original "new message" notifications)
//   - no-reply@account.etsy.com     (Apr 2026+ "View message" CTA wrapper, see v1.1 notes)
//   - conversations@etsy.com /
//     <something>@etsy.com          ("Etsy Conversations — <Customer> needs help with an order they placed")
//     ...and likely more we haven't seen.
//
// The narrow `from:notify@etsy.com` query silently dropped any email
// from a different sender. Operator-reported case (May 10 2026):
// "Samantha needs help with an order they placed" arrived from
// "Etsy Conversations" (a different sender than notify@etsy.com)
// and never appeared in the inbox. The Gmail watcher's status line
// in the topbar showed `scanned 0` — the query found zero messages —
// not because nothing was there, but because the query didn't match.
//
// `from:etsy.com` matches ANY sender at etsy.com or any subdomain
// (account.etsy.com, notifications.etsy.com, etc.). The risk of
// false positives (newsletters, transaction confirmations) is
// neutralized by extractEtsyConversationLink — emails without a
// recoverable conversation URL get audited as
// `gmail_message_skipped_no_link` and skipped. The audit volume
// itself is a useful signal: a flood of skips means Etsy started
// blasting non-conversation emails from a similar sender, and the
// operator can tighten the query via the GMAIL_QUERY env var.
//
// IMPORTANT FOR OPERATORS UPGRADING: this default ONLY applies if
// you have NOT set GMAIL_QUERY in Netlify env vars. If you have,
// update it manually to a broader pattern (or unset it to use this
// new default). Check Netlify → Environment variables → GMAIL_QUERY.
//
// NB: this is COMBINED with `after:<seconds>` from the watermark before
// being sent to Gmail. So the env var should NOT include an after: clause.
const DEFAULT_GMAIL_QUERY = "from:etsy.com";

// On the very first run (no watermark in syncState), don't pull all-time
// history — limit to the last N days. Operators can override per-request
// or via env var. 7d is generous enough to catch backlog, conservative
// enough to avoid burning the 15-min budget on years of email.
const DEFAULT_INITIAL_WINDOW_DAYS = parseInt(
  process.env.GMAIL_INITIAL_WINDOW_DAYS || "7", 10
);

// Per-page Gmail listing size. 100 is the API's max.
const PAGE_SIZE = 100;

// Cap pages per invocation as defense-in-depth — a misconfigured query
// (e.g. accidentally `from:gmail.com`) shouldn't burn the budget.
const MAX_PAGES_PER_INVOCATION = 50;

// Stop 13 min into the 15-min envelope so we have time to flush state.
const MAX_INVOCATION_MS = 13 * 60 * 1000;

// Yield between Gmail message fetches so we don't trip the per-user
// concurrency limit (250 quota units/sec, getMessage costs 5 units).
const FETCH_DELAY_MS = 50;

// ─── Helpers ─────────────────────────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function readSyncState() {
  const snap = await db.doc(SYNC_STATE_DOC).get();
  return snap.exists ? snap.data() : null;
}

async function writeSyncState(patch) {
  // serverTimestamp on updatedAt every time so observers see fresh state.
  await db.doc(SYNC_STATE_DOC).set(
    { ...patch, updatedAt: FV.serverTimestamp() },
    { merge: true }
  );
}

async function writeAudit({ threadId, eventType, actor, payload }) {
  try {
    await db.collection(AUDIT_COLL).add({
      threadId : threadId || null,
      draftId  : null,
      eventType,
      actor    : actor || "system:gmail-watcher",
      payload  : payload || {},
      createdAt: FV.serverTimestamp()
    });
  } catch (err) {
    // Audit failures are non-fatal — never let an audit write block the
    // happy path. Log and move on.
    console.warn("audit write failed:", err.message);
  }
}

/**
 * Pull the customer name out of an Etsy notification email subject.
 *
 * Etsy's notification subjects always follow the form:
 *   "Etsy Conversation with <NAME>"
 *   "Re: Etsy Conversation with <NAME>"
 *   "Re: Etsy Conversation with <NAME> about Order #..."
 *   "Re: Etsy Conversation with <NAME>, <something>"
 *
 * The name is sitting in plain text and we have it the moment Gmail
 * returns the message — there's no need to wait for the Etsy scrape to
 * fill in customerName. Pulling it here means new threads enter the
 * inbox already labeled with the buyer's name, so the operator sees
 * something useful even before (or instead of) the extension scraping
 * the conversation page.
 *
 * Returns the cleaned name string, or null if the subject doesn't match
 * the expected format. Edge cases handled:
 *   - Optional leading "Re: " / "Fwd: " (any case, repeated).
 *   - Trailing " about Order ..." / " about <anything>" trimmed.
 *   - Trailing comma + clause trimmed ("Caitríona, regarding...").
 *   - Whitespace collapsed and trimmed.
 *   - Sub-200-char clamp as defense — if a subject is malformed and the
 *     regex captures a runaway tail, we don't store kilobyte names.
 *   - Non-ASCII names (Cyrillic, accented Latin, etc.) preserved as-is.
 *   - "Sign in with Apple user" and similar Etsy placeholders are
 *     returned verbatim — they're still more useful than "Unknown" and
 *     match exactly what the operator sees in the Etsy UI.
 *
 * Examples (from real subjects):
 *   "Re: Etsy Conversation with Cristi Moore"               → "Cristi Moore"
 *   "Re: Etsy Conversation with Sarah from RaincoastGallery" → "Sarah from RaincoastGallery"
 *   "Re: Etsy Conversation with Jasmine Martinez about Order #123" → "Jasmine Martinez"
 *   "Etsy Conversation with Катерина Васильева"              → "Катерина Васильева"
 *   "Some unrelated subject"                                  → null
 */
function extractCustomerNameFromSubject(subject) {
  if (!subject || typeof subject !== "string") return null;

  // Strip any chain of leading "Re:" / "Fwd:" / "Fw:" prefixes (case
  // insensitive, with optional whitespace). Mailers love stacking these.
  let s = subject.replace(/^\s*(?:re|fwd|fw)\s*:\s*/gi, "").trim();

  // Anchor on the literal "Etsy Conversation with " phrase. \u00A0 is a
  // non-breaking space — Gmail's API has been observed to emit those in
  // some clients' subject lines; treat them like regular spaces.
  const m = s.match(/Etsy Conversation with[\s\u00A0]+(.+)$/i);
  if (!m) return null;

  let name = m[1];

  // Trim trailing " about <anything>" — Etsy appends "about Order #..."
  // when the conversation is order-linked. The name ends right before
  // the " about ".
  name = name.replace(/\s+about\s+.*$/i, "");

  // Trim trailing comma-clause ("Cristi Moore, the buyer of..."). Anything
  // after the first comma is metadata, not part of the name.
  const comma = name.indexOf(",");
  if (comma !== -1) name = name.slice(0, comma);

  // Collapse whitespace and clamp.
  name = name.replace(/\s+/g, " ").trim();
  if (!name) return null;
  if (name.length > 200) name = name.slice(0, 200).trim();

  return name;
}

/**
 * Build the Gmail search query for this invocation. Combines the static
 * filter (env var or default) with the watermark — Gmail's `after:`
 * operator wants Unix seconds.
 *
 * Returns { q, watermarkSec } so the caller can log + advance state.
 */
function buildQuery({ baseQuery, lastInternalDateMs, windowDays }) {
  let watermarkSec;
  if (lastInternalDateMs && lastInternalDateMs > 0) {
    // +1 second to skip the message at the boundary (we already processed it)
    watermarkSec = Math.floor(lastInternalDateMs / 1000) + 1;
  } else {
    // No watermark → look back windowDays (initial run / re-seed scenario)
    const days = windowDays > 0 ? windowDays : DEFAULT_INITIAL_WINDOW_DAYS;
    watermarkSec = Math.floor((Date.now() - days * 86400 * 1000) / 1000);
  }
  const q = `${baseQuery} after:${watermarkSec}`;
  return { q, watermarkSec };
}

// ─── Gmail link index ────────────────────────────────────────────────────
//
// v1.7 — Maintains a 1:1 index of gmailMessageId → threadId so the sweep
// can answer "has this Gmail message EVER been linked to a thread?" with a
// single doc lookup. The thread doc only stores the MOST RECENT
// gmailMessageId, so we can't ask it that question.
//
// v1.8 — Extended to also remember evaluated-but-no-link messages
// (transaction emails like "You made a sale", "Etsy Order Confirmation",
// etc. that match the broad from:etsy.com filter but contain no
// conversation link). Pre-v1.8 these were re-evaluated by the sweep on
// every tick, generating a continuous flood of `gmail_sweep_skipped_no_link`
// audits. v1.8 writes a `status: "no_link"` index doc the first time
// they're evaluated; subsequent sweeps see them in the index and skip
// without touching the audit log. The sweep's `linkedIds` check only
// cares whether the doc exists, so v1.7 docs (no `status` field) keep
// working — no migration needed.
//
// Doc id = gmailMessageId. Idempotent: writing the same doc twice is a
// no-op. Designed for ~free reads: the sweep's existing chunked `in`
// lookup pattern works against this collection via
// FieldPath.documentId(), and document-id lookups don't need an index.
//
// Lifetime is "as long as the Gmail message is in the user's mailbox" —
// not pruned automatically. Doc size is tiny (~100B) so even 100K linked
// emails is ~10MB. If pruning becomes necessary, the same SWEEP_WINDOW
// time window applies — anything older than 6h doesn't affect the sweep.
//
// @param status — "linked" (the normal case; threadId required) or
//                 "no_link" (sweep+main-flow saw this gmailMessageId but
//                 couldn't extract a conversation link from the email).
async function recordGmailLink(gmailMessageId, threadId, status = "linked") {
  if (!gmailMessageId) return;
  // For "linked" status we require a real threadId; "no_link" has none.
  if (status === "linked" && !threadId) return;
  try {
    const doc = {
      gmailMessageId,
      status,
      linkedAt: FV.serverTimestamp()
    };
    if (threadId) doc.threadId = threadId;
    await db.collection(GMAIL_LINKS_COLL).doc(gmailMessageId).set(doc, { merge: true });
  } catch (err) {
    // Non-fatal: a missed index write means the next sweep tick will see
    // this email as "orphaned" and try to re-link it. upsertThreadFromGmail
    // is idempotent, so the worst case is one redundant link + scrape.
    // We log but don't throw — the main flow has already done the
    // user-visible work.
    console.warn(`[gmail-link-index] recordGmailLink failed for ${gmailMessageId} (status=${status}):`, err.message);
  }
}

// ─── Thread upsert ───────────────────────────────────────────────────────
//
// Pattern matches etsyMailThreads.js action:create — same field shape, so
// the inbox UI and downstream pipeline see exactly the same thread doc
// whether it was created by Gmail detection or the snapshot endpoint.
//
// Three cases:
//   1. Thread doesn't exist        → CREATE with status "detected_from_gmail"
//   2. Thread exists, no gmailId   → PATCH to attach gmail metadata
//   3. Thread exists, has gmailId  → no-op (already linked from a prior poll)
//
// We never advance status here. The snapshot endpoint owns status
// transitions ("detected_from_gmail" → "etsy_scraped"); leaving status
// alone on the upsert means the snapshot endpoint's existing state
// machine just works.

async function upsertThreadFromGmail({
  conversationId,
  conversationUrl,
  gmailMessageId,
  gmailThreadId,
  internalDateMs,
  customerName,
  customerEmail,
  subject
}) {
  const threadId = `etsy_conv_${conversationId}`;
  const ref = db.collection(THREADS_COLL).doc(threadId);
  const now = FV.serverTimestamp();
  const gmailReceivedAt = internalDateMs
    ? admin.firestore.Timestamp.fromMillis(internalDateMs)
    : null;

  const snap = await ref.get();

  // Pull the buyer's name straight out of the email subject
  // ("Re: Etsy Conversation with <NAME>"). The watcher already has this
  // string from the Gmail headers — there's no need to wait on the
  // extension scrape to label the thread. Falls back to whatever the
  // caller passed (or "Unknown") if the subject doesn't match.
  const subjectParsedName = extractCustomerNameFromSubject(subject);
  const resolvedCustomerName = customerName || subjectParsedName || "Unknown";

  if (!snap.exists) {
    // CREATE — mirror the field shape of etsyMailThreads.js action:create
    // so the inbox UI and downstream pipeline see exactly the same thread
    // doc whether it was created by Gmail detection or the snapshot
    // endpoint. Initial status is "detected_from_gmail" — the snapshot
    // endpoint will advance to "etsy_scraped" on first successful scrape
    // (logic that already exists in etsyMailSnapshot.js line 125).
    //
    // customerName is set from (in priority order):
    //   1. The caller-provided name (currently always null on the gmail
    //      path — placeholder for a future direct-passthrough).
    //   2. The name parsed from the email subject (covers ~all real
    //      Etsy notification emails — see extractCustomerNameFromSubject).
    //   3. "Unknown" as the last-resort fallback.
    const initial = {
      threadId,
      etsyConversationId  : conversationId,
      etsyConversationUrl : conversationUrl,
      gmailMessageId,
      gmailThreadId,
      gmailReceivedAt,
      customerName        : resolvedCustomerName,
      customerEmail       : customerEmail || null,
      etsyUsername        : null,
      linkedOrderId       : null,
      linkedListingIds    : [],
      status              : "detected_from_gmail",
      category            : null,
      confidence          : null,
      needsHumanReview    : true,
      aiDraftStatus       : "none",
      latestDraftId       : null,
      lastInboundAt       : gmailReceivedAt,
      lastOutboundAt      : null,
      lastSyncedAt        : null,
      lastScrapedDomHash  : null,
      assignedTo          : null,
      tags                : [],
      riskFlags           : [],
      messageCount        : 0,
      unread              : true,
      lastReadAt          : null,
      subject             : subject || null,
      // Provenance flag: true means customerName came from the email
      // subject parse, not a real Etsy scrape. The snapshot endpoint
      // will overwrite customerName with the real scraped value when
      // it commits, so this flag self-clears.
      customerNameFromSubject: !!subjectParsedName,
      createdAt           : now,
      updatedAt           : now,
      // M3 buyer metadata fields — populated by snapshot's first scrape
      buyerUserId         : null,
      buyerPeopleUrl      : null,
      buyerAvatarUrl      : null,
      buyerIsRepeatBuyer  : false
    };
    await ref.set(initial, { merge: false });
    // v1.7 — record link index so sweep can confirm this gmailMessageId
    // has been linked even if a later email overwrites the thread doc's
    // gmailMessageId field.
    await recordGmailLink(gmailMessageId, threadId);
    await writeAudit({
      threadId,
      eventType: "thread_created",
      actor    : "system:gmail",
      payload  : {
        source                  : "gmail",
        gmailMessageId,
        gmailThreadId,
        hasInitialText          : false,
        customerNameFromSubject : !!subjectParsedName
      }
    });
    return { threadId, action: "created" };
  }

  // UPDATE path. If thread already has THIS exact gmailMessageId, skip —
  // we've already processed this email (idempotency guard for SW retries).
  const existing = snap.data() || {};
  if (existing.gmailMessageId === gmailMessageId) {
    // v1.7 — defensive: write to the index here too. The link IS already
    // established on the thread doc; if the index doc happens to be
    // missing (pre-v1.7 link, or a transient earlier write failure) this
    // backfills it so the sweep doesn't treat this id as orphaned. set+
    // merge is idempotent, so this is a no-op for the common case.
    await recordGmailLink(gmailMessageId, threadId);
    return { threadId, action: "skipped_already_linked" };
  }

  // Patch only the gmail-related fields. Don't touch status — the
  // snapshot endpoint and operator UI may have refined that. We DO
  // touch customerName here, but only in the narrow case where the
  // existing value is the placeholder "Unknown" AND we just got a
  // parseable name from the new email's subject. Real scraped names
  // (from the snapshot endpoint) and operator-edited names are
  // preserved untouched.
  const patch = {
    gmailMessageId,
    gmailThreadId,
    gmailReceivedAt,
    updatedAt: now
  };
  // Bring etsyConversationUrl up to date if missing (older threads created
  // before Gmail integration may have null URL).
  if (!existing.etsyConversationUrl) patch.etsyConversationUrl = conversationUrl;

  // customerName backfill: only when the existing slot is "Unknown" or
  // empty AND we have a parseable name from THIS message's subject.
  // The customerNameFromSubject flag advertises the provenance so the
  // snapshot endpoint can confidently overwrite later without losing
  // operator edits — operator edits never set this flag.
  const isPlaceholder =
       !existing.customerName
    || existing.customerName === "Unknown"
    || existing.customerName === "";
  if (isPlaceholder && subjectParsedName) {
    patch.customerName            = subjectParsedName;
    patch.customerNameFromSubject = true;
  }

  await ref.set(patch, { merge: true });
  // v1.7 — record link index. Note we do this on the "linked" path only;
  // the "skipped_already_linked" early-return above has already written
  // the index on the prior call that linked this same gmailMessageId.
  await recordGmailLink(gmailMessageId, threadId);
  await writeAudit({
    threadId,
    eventType: "thread_gmail_linked",
    actor    : "system:gmail",
    payload  : {
      gmailMessageId,
      gmailThreadId,
      previousGmailMessageId  : existing.gmailMessageId || null,
      customerNameBackfilled  : isPlaceholder && !!subjectParsedName
    }
  });
  return { threadId, action: "linked" };
}

// ─── Job enqueue ─────────────────────────────────────────────────────────
//
// Pattern matches the EtsyMail_Jobs schema consumed by etsyMailJobs.js
// (op:claim) — same fields, same status lifecycle, same payload shape.
// Deterministic doc id `gmail_<gmailMessageId>` makes re-enqueues from
// SW retries idempotent without needing a transaction.

async function enqueueScrapeJob({ threadId, conversationUrl, gmailMessageId, gmailThreadId }) {
  const jobId = `gmail_${gmailMessageId}`;
  const ref = db.collection(JOBS_COLL).doc(jobId);

  // Idempotency: if a job with this id already exists in any non-failed
  // state, skip. We only want to re-enqueue if the prior attempt failed
  // hard (status="failed" with no further retries).
  const existing = await ref.get();
  if (existing.exists) {
    const data = existing.data() || {};
    const skippableStatuses = ["queued", "claimed", "succeeded"];
    if (skippableStatuses.includes(data.status)) {
      return { jobId, action: "skipped_existing", existingStatus: data.status };
    }
    // status === "failed" → fall through and re-queue
  }

  await ref.set({
    jobId,
    jobType   : "scrape",
    status    : "queued",
    threadId  : threadId || null,
    payload   : {
      etsyConversationUrl: conversationUrl,
      source             : "gmail",
      gmailMessageId,
      gmailThreadId
    },
    attempts       : 0,
    claimedBy      : null,
    claimedAt      : null,
    lastError      : null,
    lastHeartbeatAt: null,
    result         : null,
    createdAt      : FV.serverTimestamp(),
    updatedAt      : FV.serverTimestamp()
  }, { merge: false });

  return { jobId, action: "enqueued" };
}

// ─── Main loop ───────────────────────────────────────────────────────────

async function runIncremental({ invocationStartMs, mode, query, windowDays }) {
  const state = await readSyncState();
  const lastInternalDateMs = state && state.lastInternalDateMs ? state.lastInternalDateMs : 0;

  // mode="full" wipes the watermark for this invocation only; the windowDays
  // cap still applies so we don't accidentally pull all-time history. Useful
  // when an operator has changed the GMAIL_QUERY filter and wants a backfill.
  const effectiveWatermark = mode === "full" ? 0 : lastInternalDateMs;

  const baseQuery = (query && query.trim()) || process.env.GMAIL_QUERY || DEFAULT_GMAIL_QUERY;
  const { q, watermarkSec } = buildQuery({
    baseQuery,
    lastInternalDateMs: effectiveWatermark,
    windowDays
  });

  console.log(`[gmail-watcher] running mode=${mode} q="${q}"`);

  let pageToken = null;
  let pagesFetched = 0;
  let messagesScanned = 0;
  let jobsEnqueued = 0;
  let threadsCreated = 0;
  let threadsLinked = 0;
  let skippedNoLink = 0;
  let skippedAlreadyProcessed = 0;
  let errors = 0;
  let newestInternalDateMs = lastInternalDateMs;

  // PAGINATE through Gmail until exhaustion or budget. We process each
  // message inside the same page loop so that if we hit the time cap the
  // watermark advances incrementally — next invocation picks up where
  // we left off without re-doing work.
  while (true) {
    if (Date.now() - invocationStartMs > MAX_INVOCATION_MS) {
      console.log("[gmail-watcher] hit invocation budget, stopping");
      break;
    }
    if (pagesFetched >= MAX_PAGES_PER_INVOCATION) {
      console.log("[gmail-watcher] hit MAX_PAGES_PER_INVOCATION, stopping");
      break;
    }

    let listResp;
    try {
      listResp = await listMessages({ q, pageToken, maxResults: PAGE_SIZE });
    } catch (err) {
      console.error("[gmail-watcher] listMessages failed:", err.message);
      errors++;
      throw err;   // fatal — let the outer handler record the error in state
    }
    pagesFetched++;

    const stubs = Array.isArray(listResp.messages) ? listResp.messages : [];
    if (stubs.length === 0 && pageToken == null) {
      // First page came back empty — nothing new since watermark
      console.log("[gmail-watcher] no new messages");
      break;
    }

    // ━━━ v1.2 GMAIL-THREAD DEDUP ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // Gmail returns list results in newest-first order. Walk the list
    // newest-first and keep only the FIRST occurrence of each threadId.
    // That's the newest message in each Gmail thread.
    //
    // Why: Gmail groups all "Vanessa sent you a message" emails (from
    // the same customer over time) into one Gmail thread. If multiple
    // matching messages are returned in a single poll (rare with the
    // watermark, but possible during catch-up after the watcher was off),
    // processing them all means following stale tracker URLs from old
    // emails — which can resolve to expired/wrong Etsy conversations.
    //
    // Only the newest tracker URL per thread is reliable, so dedup.
    // We still track every stub's internalDate for watermark advance,
    // so skipped duplicates don't cause re-processing on the next poll.
    const seenThreadIds = new Set();
    const dedupedStubs = [];
    let droppedAsThreadDupes = 0;
    for (const stub of stubs) {
      // Track every stub's internalDate via summary later; for dedup,
      // only the threadId matters here.
      if (stub.threadId && seenThreadIds.has(stub.threadId)) {
        droppedAsThreadDupes++;
        continue;
      }
      if (stub.threadId) seenThreadIds.add(stub.threadId);
      dedupedStubs.push(stub);
    }
    if (droppedAsThreadDupes > 0) {
      console.log(`[gmail-watcher] dedup: kept ${dedupedStubs.length} newest-per-thread, dropped ${droppedAsThreadDupes} older-same-thread`);
    }
    // ━━━ end v1.2 fix ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    // Process messages oldest-first WITHIN the page so the watermark
    // advances monotonically. Gmail returns newest-first, so reverse.
    const orderedStubs = dedupedStubs.slice().reverse();

    for (const stub of orderedStubs) {
      if (Date.now() - invocationStartMs > MAX_INVOCATION_MS) break;
      messagesScanned++;

      let full;
      try {
        full = await getMessage(stub.id, { format: "full" });
      } catch (err) {
        console.warn(`[gmail-watcher] getMessage(${stub.id}) failed:`, err.message);
        errors++;
        continue;
      }

      const summary = summarizeMessage(full);

      // Track newest internalDate for watermark advance — even messages
      // we skip count, otherwise we'd reprocess them on every poll.
      if (summary.internalDateMs && summary.internalDateMs > newestInternalDateMs) {
        newestInternalDateMs = summary.internalDateMs;
      }

      // EXTRACT — find the Etsy conversation link in the body.
      // v1.1: extractEtsyConversationLink is now async — it follows
      // SendGrid click-tracking redirects (ablink.account.etsy.com) up
      // to MAX_TRACKER_FOLLOWS times to find the underlying conversation
      // URL. Each tracker follow can take up to 8s; bound the total
      // per-message extraction overhead at ~32s worst case (rare).
      const link = await extractEtsyConversationLink(full);
      if (!link || !link.conversationId) {
        skippedNoLink++;
        // v1.8 — Record in the link index with status="no_link" BEFORE
        // writing the audit. This is the steady-state path for
        // non-conversation emails (transaction notifications, etc.) that
        // match from:etsy.com. Without this, every sweep tick would
        // re-evaluate and re-audit the same emails until they age past
        // SWEEP_WINDOW_HOURS — a continuous flood of skip audits.
        await recordGmailLink(summary.gmailMessageId, null, "no_link");
        // Audit each miss so operators can spot if Etsy changes their
        // email format and we're silently dropping all of them.
        await writeAudit({
          threadId : null,
          eventType: "gmail_message_skipped_no_link",
          actor    : "system:gmail",
          payload  : {
            gmailMessageId : summary.gmailMessageId,
            subject        : summary.subject,
            from           : summary.from
          }
        });
        await sleep(FETCH_DELAY_MS);
        continue;
      }

      // UPSERT thread
      let upsertResult;
      try {
        upsertResult = await upsertThreadFromGmail({
          conversationId : link.conversationId,
          conversationUrl: link.conversationUrl,
          gmailMessageId : summary.gmailMessageId,
          gmailThreadId  : summary.gmailThreadId,
          internalDateMs : summary.internalDateMs,
          customerName   : null,                     // populated by scrape
          customerEmail  : null,                     // could parse "From:" if useful
          subject        : summary.subject
        });
      } catch (err) {
        console.warn(`[gmail-watcher] upsertThread failed for ${summary.gmailMessageId}:`, err.message);
        errors++;
        continue;
      }

      if (upsertResult.action === "created") threadsCreated++;
      else if (upsertResult.action === "linked") threadsLinked++;
      else if (upsertResult.action === "skipped_already_linked") {
        skippedAlreadyProcessed++;
        // Already linked — don't enqueue another scrape job. The thread
        // is already in the pipeline. Watermark still advances so we
        // don't re-fetch this message next tick.
        await sleep(FETCH_DELAY_MS);
        continue;
      }

      // ENQUEUE scrape job → existing extension picks this up next poll
      try {
        const jobResult = await enqueueScrapeJob({
          threadId       : upsertResult.threadId,
          conversationUrl: link.conversationUrl,
          gmailMessageId : summary.gmailMessageId,
          gmailThreadId  : summary.gmailThreadId
        });
        if (jobResult.action === "enqueued") {
          jobsEnqueued++;
          await writeAudit({
            threadId : upsertResult.threadId,
            eventType: "scrape_job_enqueued",
            actor    : "system:gmail",
            payload  : {
              jobId          : jobResult.jobId,
              gmailMessageId : summary.gmailMessageId,
              conversationUrl: link.conversationUrl
            }
          });
        }
      } catch (err) {
        console.warn(`[gmail-watcher] enqueueJob failed for ${summary.gmailMessageId}:`, err.message);
        errors++;
      }

      await sleep(FETCH_DELAY_MS);
    }

    pageToken = listResp.nextPageToken || null;
    if (!pageToken) break;
  }

  // Persist the new watermark + counters. Always write — even on a no-op
  // run, the lastSyncCompletedAt advance is what the cron uses to
  // throttle subsequent triggers.
  await writeSyncState({
    lastSyncMode             : mode,
    lastSyncStartedAt        : admin.firestore.Timestamp.fromMillis(invocationStartMs),
    lastSyncCompletedAt      : FV.serverTimestamp(),
    lastSyncDurationMs       : Date.now() - invocationStartMs,
    lastSyncMessagesScanned  : messagesScanned,
    lastSyncJobsEnqueued     : jobsEnqueued,
    lastSyncThreadsCreated   : threadsCreated,
    lastSyncThreadsLinked    : threadsLinked,
    lastSyncSkippedNoLink    : skippedNoLink,
    lastSyncSkippedAlreadyProcessed: skippedAlreadyProcessed,
    lastSyncErrors           : errors,
    lastSyncPagesFetched     : pagesFetched,
    lastSyncQuery            : q,
    lastSyncWatermarkSec     : watermarkSec,
    lastInternalDateMs       : newestInternalDateMs,
    lastSyncInProgress       : false,
    // Clear the prior error if this run succeeded
    ...(errors === 0 ? { lastSyncError: null, lastSyncErrorAt: null } : {})
  });

  return {
    messagesScanned,
    jobsEnqueued,
    threadsCreated,
    threadsLinked,
    skippedNoLink,
    skippedAlreadyProcessed,
    errors,
    pagesFetched,
    newestInternalDateMs
  };
}

// ─── Safety-net sweep ──────────────────────────────────────────────────────
//
// v1.5 — The bulletproof gap-filler. Independent of the watermark.
//
// WHY THIS EXISTS
//
// The main runIncremental flow advances `newestInternalDateMs` for EVERY
// message it lists, including ones where extractEtsyConversationLink
// returned null (line ~615 in this file's main loop):
//
//     // Track newest internalDate for watermark advance — even messages
//     // we skip count, otherwise we'd reprocess them on every poll.
//     if (summary.internalDateMs && summary.internalDateMs > newestInternalDateMs) {
//       newestInternalDateMs = summary.internalDateMs;
//     }
//
// That's the right call for permanently-unparseable emails (keeps the
// queue bounded). But it's a silent data-loss bug for TRANSIENT
// extraction failures: a SendGrid 503 mid-redirect, a Branch.io
// hiccup, a momentary network blip. Those failures should self-heal
// — the message should be re-tried — but the watermark already
// advanced past them by the time the next tick runs.
//
// Real-world symptom (operator-reported, May 10 2026): customer
// "Samantha needs help with an order they placed" sat in Gmail at
// 10:05 AM but never appeared in EtsyMail. The 8:06 AM message did.
// Watermark advanced past 10:05 AM and Samantha's email was orphaned.
//
// HOW IT WORKS
//
// Every invocation, AFTER the main incremental flow:
//   1. List Gmail messages from the last SWEEP_WINDOW_HOURS (6h).
//   2. For each message id, check Firestore for an EtsyMail_Threads
//      doc with that gmailMessageId.
//   3. Any message id NOT linked to a thread doc → re-process it
//      with the same path as the main flow (extract, upsert, enqueue).
//   4. Audit each recovery so operators can see when the safety net
//      fires and which messages had to be rescued (= a signal that
//      the main flow's extraction is failing too often).
//
// The sweep does NOT touch the watermark. Both paths are write-safe
// (upsertThreadFromGmail short-circuits if the thread is already
// linked, and enqueueScrapeJob uses a deterministic doc id).
//
// COSTS
//
// Steady state: one Gmail listMessages call per tick + one chunked
// Firestore "in" query. All messages are already linked, no
// getMessage calls. Cheap (~200-500ms total).
//
// During a recovery: the actually-missed messages get full processing
// (getMessage + extractEtsyConversationLink + upsert + enqueue),
// matching the main flow's per-message cost. Bounded by the 5-page
// listing cap (max 500 messages in the window).
//
// WHAT IT DOESN'T DO
//
// It doesn't recover messages older than SWEEP_WINDOW_HOURS. A
// permanent extraction bug that lasts >6h still loses the messages
// past the window. We log every skip in the audit as
// "gmail_sweep_skipped_no_link"; operators monitoring the audit can
// catch persistent issues. For deeper recovery, a manual trigger of
// etsyMailGmail with a fresh windowDays parameter is the escape hatch.
const SWEEP_WINDOW_HOURS = 6;
const SWEEP_MAX_PAGES = 5;

async function runSafetyNetSweep({ invocationStartMs }) {
  const result = {
    listed             : 0,
    alreadyLinked      : 0,
    recoveredCreated   : 0,
    recoveredLinked    : 0,
    recoveredEnqueued  : 0,
    skippedNoLink      : 0,
    errors             : 0,
    durationMs         : 0
  };
  const t0 = Date.now();

  // v1.7 — Hard kill switch, checked FIRST. If
  // EtsyMail_Config/gmailSweepCircuitBreaker.enabled === true the sweep
  // bails immediately, even if everything else is green. This is the
  // "stop the bleeding" lever an operator can throw in Firestore if the
  // sweep ever misbehaves again — independent of the main gmailWatcher
  // toggle (which gates the entire watcher; this gates just the sweep).
  // Default-off semantics: missing doc = sweep runs normally.
  try {
    const cbSnap = await db.doc(SWEEP_CIRCUIT_BREAKER_DOC).get();
    if (cbSnap.exists && cbSnap.data() && cbSnap.data().enabled === true) {
      console.warn("[gmail-sweep] skipping (circuit breaker is engaged)");
      result.skipped = "circuit_breaker";
      result.durationMs = Date.now() - t0;
      return result;
    }
  } catch (err) {
    // If the breaker doc read fails, fall through to running the sweep —
    // failing closed on a Firestore read error would mean a flaky
    // Firestore takes the sweep offline. Log it so the failure isn't
    // silent.
    console.warn("[gmail-sweep] circuit-breaker doc read failed:", err.message);
  }

  // Budget check — if the main flow already used >12 min of the 15
  // min Netlify budget, skip the sweep this tick. The main flow's
  // watermark advance has already happened, so we'd run it next tick.
  const budgetUsedMs = Date.now() - invocationStartMs;
  if (budgetUsedMs > 12 * 60 * 1000) {
    console.warn(`[gmail-sweep] skipping (budget tight: ${budgetUsedMs}ms used)`);
    result.skipped = "budget_tight";
    return result;
  }

  // 1. List Gmail messages in the sweep window.
  const baseQuery = process.env.GMAIL_QUERY || DEFAULT_GMAIL_QUERY;
  const sinceSec = Math.floor((Date.now() - SWEEP_WINDOW_HOURS * 3600 * 1000) / 1000);
  const q = `${baseQuery} after:${sinceSec}`;
  const gmailIds = [];
  let pageToken = null;
  let pages = 0;
  try {
    while (pages < SWEEP_MAX_PAGES) {
      const resp = await listMessages({ q, pageToken, maxResults: 100 });
      pages++;
      const stubs = (resp && resp.messages) || [];
      for (const s of stubs) {
        if (s && s.id) gmailIds.push(s.id);
      }
      pageToken = resp && resp.nextPageToken;
      if (!pageToken) break;
    }
  } catch (err) {
    console.warn("[gmail-sweep] listMessages failed:", err.message);
    result.errors++;
    result.durationMs = Date.now() - t0;
    return result;
  }
  result.listed = gmailIds.length;
  if (gmailIds.length === 0) {
    result.durationMs = Date.now() - t0;
    return result;
  }

  // 2. Bulk-check Firestore: which of these gmailIds have ever been
  //    linked to a thread? Query the EtsyMail_GmailLinks index (one doc
  //    per linked gmailMessageId) rather than the thread doc's
  //    gmailMessageId field — the thread only stores the MOST RECENT
  //    linked id, so earlier emails on the same Etsy conversation would
  //    look "orphaned" forever, causing a flip-flop re-link loop. The
  //    index doc is keyed by gmailMessageId, so we can chunk-lookup via
  //    FieldPath.documentId() — no composite index needed.
  //
  // v1.7 — Firestore "in" filters take up to 30 values per query; chunk
  // accordingly. Same pattern as the prior implementation.
  const linkedIds = new Set();
  const CHUNK = 30;
  for (let i = 0; i < gmailIds.length; i += CHUNK) {
    const chunk = gmailIds.slice(i, i + CHUNK);
    try {
      const snap = await db.collection(GMAIL_LINKS_COLL)
        .where(admin.firestore.FieldPath.documentId(), "in", chunk)
        .get();
      snap.forEach(doc => { linkedIds.add(doc.id); });
    } catch (err) {
      console.warn("[gmail-sweep] gmail-link-index check chunk failed:", err.message);
      result.errors++;
      // Continue anyway — un-checked chunks just means we may re-
      // process some already-processed messages. upsertThreadFromGmail
      // is idempotent so this is safe (just slower).
    }
  }
  result.alreadyLinked = linkedIds.size;

  const orphaned = gmailIds.filter(id => !linkedIds.has(id));
  if (orphaned.length === 0) {
    // Nothing to recover; this is the steady state — fast path.
    result.durationMs = Date.now() - t0;
    return result;
  }

  console.log(`[gmail-sweep] found ${orphaned.length} orphaned message(s) in last ${SWEEP_WINDOW_HOURS}h — recovering`);

  // 3. For each orphaned message, run the same path as the main flow.
  for (const gmailId of orphaned) {
    // Per-message budget check — bail if we've exceeded Netlify's
    // overall budget envelope. Whatever's left for next tick.
    if (Date.now() - invocationStartMs > MAX_INVOCATION_MS) {
      console.warn("[gmail-sweep] hit MAX_INVOCATION_MS, deferring rest to next tick");
      break;
    }

    let full;
    try {
      full = await getMessage(gmailId);
    } catch (err) {
      console.warn(`[gmail-sweep] getMessage(${gmailId}) failed:`, err.message);
      result.errors++;
      await sleep(FETCH_DELAY_MS);
      continue;
    }
    const summary = summarizeMessage(full);

    // Same extractor as the main flow.
    let link = null;
    try {
      link = await extractEtsyConversationLink(full);
    } catch (err) {
      console.warn(`[gmail-sweep] extract failed for ${gmailId}:`, err.message);
      result.errors++;
      await sleep(FETCH_DELAY_MS);
      continue;
    }

    if (!link || !link.conversationId) {
      result.skippedNoLink++;
      // v1.8 — Record in the link index with status="no_link" so
      // subsequent sweeps see this gmailMessageId in linkedIds and skip
      // it during the orphan-detection chunk-check (line ~960), without
      // even fetching the message body. This is belt-and-suspenders
      // alongside the main-flow write: if the main flow's index write
      // ever fails for a given message, the sweep's write here catches
      // it next tick. After that tick the message is permanently in the
      // index and never re-evaluated.
      await recordGmailLink(summary.gmailMessageId, null, "no_link");
      // Audit so operators can spot persistent extraction failures
      // (same email keeps appearing in sweep audits → Etsy changed
      // their email format and the parser needs an update).
      await writeAudit({
        threadId : null,
        eventType: "gmail_sweep_skipped_no_link",
        actor    : "system:gmail-sweep",
        payload  : {
          gmailMessageId : summary.gmailMessageId,
          subject        : summary.subject,
          from           : summary.from,
          ageHours       : summary.internalDateMs
            ? ((Date.now() - summary.internalDateMs) / 3600000).toFixed(1)
            : null
        }
      });
      await sleep(FETCH_DELAY_MS);
      continue;
    }

    // UPSERT thread (idempotent; will short-circuit if already linked).
    let upsertResult;
    try {
      upsertResult = await upsertThreadFromGmail({
        conversationId : link.conversationId,
        conversationUrl: link.conversationUrl,
        gmailMessageId : summary.gmailMessageId,
        gmailThreadId  : summary.gmailThreadId,
        internalDateMs : summary.internalDateMs,
        customerName   : null,
        customerEmail  : null,
        subject        : summary.subject
      });
    } catch (err) {
      console.warn(`[gmail-sweep] upsertThread failed for ${summary.gmailMessageId}:`, err.message);
      result.errors++;
      await sleep(FETCH_DELAY_MS);
      continue;
    }
    if (upsertResult.action === "created") result.recoveredCreated++;
    else if (upsertResult.action === "linked") result.recoveredLinked++;
    else if (upsertResult.action === "skipped_already_linked") {
      // Race: the main flow ran in parallel and linked this one.
      // Don't enqueue — it's already in the pipeline.
      await sleep(FETCH_DELAY_MS);
      continue;
    }

    // ENQUEUE (idempotent — deterministic job id).
    try {
      await enqueueScrapeJob({
        threadId       : upsertResult.threadId,
        conversationUrl: link.conversationUrl,
        gmailMessageId : summary.gmailMessageId,
        gmailThreadId  : summary.gmailThreadId
      });
      result.recoveredEnqueued++;

      // Audit the recovery — this is the trail operators can use to
      // see "the safety net just saved a message that the main flow
      // missed". A burst of these audits = main flow has a bug
      // worth investigating.
      await writeAudit({
        threadId : upsertResult.threadId,
        eventType: "gmail_sweep_recovered_message",
        actor    : "system:gmail-sweep",
        payload  : {
          gmailMessageId : summary.gmailMessageId,
          conversationId : link.conversationId,
          subject        : summary.subject,
          ageHours       : summary.internalDateMs
            ? ((Date.now() - summary.internalDateMs) / 3600000).toFixed(1)
            : null,
          upsertAction   : upsertResult.action
        }
      });
    } catch (err) {
      console.warn(`[gmail-sweep] enqueue failed for ${summary.gmailMessageId}:`, err.message);
      result.errors++;
    }
    await sleep(FETCH_DELAY_MS);
  }

  result.durationMs = Date.now() - t0;
  console.log(`[gmail-sweep] done: ${JSON.stringify(result)}`);
  return result;
}

// ─── One-shot index backfill ────────────────────────────────────────────
//
// v1.7 maintenance task. Pre-v1.7, the gmailMessageId-to-threadId
// relationship existed implicitly: only in the thread doc's
// gmailMessageId field (overwritten on every new email for that thread)
// and in the EtsyMail_Audit "thread_gmail_linked" / "thread_created"
// (gmail-source) event payloads.
//
// v1.7 introduces the EtsyMail_GmailLinks index collection — but only
// new links written from this version forward populate it. Running this
// backfill once after deploy scans the audit log and writes index docs
// for every historical link, so the safety-net sweep doesn't see
// pre-deploy emails as "orphaned" and re-link/re-scrape them all on
// first run.
//
// Idempotent (set+merge on each doc), safe to re-run. Designed to be
// invoked once via:
//   POST /.netlify/functions/etsyMailGmail-background  {"mode":"backfill_index"}
// or
//   GET  /.netlify/functions/etsyMailGmail-background?mode=backfill_index
//
// Stops at MAX_INVOCATION_MS (13min) for safety; if it doesn't finish
// in one pass, the operator can re-run — already-written index docs
// will be no-ops on the second pass thanks to set+merge.
//
// v1.8 — Now also backfills no-link entries. We do two passes:
//   1. actor="system:gmail"       → link events (status: "linked")
//   2. actor="system:gmail-sweep" → skip events (status: "no_link")
// Without pass 2, disengaging the circuit breaker after v1.8 deploy
// would trigger one big flood of re-evaluation as the sweep saw every
// historical transaction-email skip as a fresh "not in index" miss.

// Classify an audit event into one of:
//   { kind: "linked",  gmailMessageId, threadId }
//   { kind: "no_link", gmailMessageId }
//   null  (skip — wrong shape or wrong type)
function classifyAuditForIndex(auditDoc) {
  const data = auditDoc.data() || {};
  const eventType = data.eventType;
  const payload = data.payload || {};
  const gmailMessageId = payload.gmailMessageId;
  if (!gmailMessageId) return null;

  if (eventType === "thread_gmail_linked" || eventType === "thread_created") {
    const threadId = data.threadId;
    if (!threadId) return null;
    return { kind: "linked", gmailMessageId, threadId };
  }
  if (eventType === "gmail_message_skipped_no_link" ||
      eventType === "gmail_sweep_skipped_no_link") {
    return { kind: "no_link", gmailMessageId };
  }
  return null;
}

// Paginate one actor's audit events and write index docs. Returns
// per-actor counts that the outer runIndexBackfill aggregates.
async function backfillIndexForActor({ actor, deadlineMs }) {
  const counts = { scanned: 0, written: 0, skipped: 0, errors: 0, hitLimit: false };
  const PAGE_SIZE = 500;
  let lastSnap = null;

  while (true) {
    if (Date.now() > deadlineMs) {
      counts.hitLimit = true;
      break;
    }

    let q = db.collection(AUDIT_COLL)
      .where("actor", "==", actor)
      .orderBy("createdAt", "asc")
      .limit(PAGE_SIZE);
    if (lastSnap) q = q.startAfter(lastSnap);

    let snap;
    try {
      snap = await q.get();
    } catch (err) {
      // The Firestore composite index (actor + createdAt asc) is shared
      // across all actor values, so once the index is provisioned for
      // system:gmail it works for system:gmail-sweep too. If it ISN'T
      // provisioned, this surfaces clearly with the URL to create it.
      console.error(`[gmail-index-backfill] audit query failed for actor=${actor}:`, err.message);
      counts.errors++;
      throw new Error(`audit query failed for actor=${actor} (may need composite index): ${err.message}`);
    }

    if (snap.empty) break;

    let batch = db.batch();
    let inBatch = 0;
    for (const doc of snap.docs) {
      const classified = classifyAuditForIndex(doc);
      if (!classified) { counts.skipped++; continue; }

      const ref = db.collection(GMAIL_LINKS_COLL).doc(classified.gmailMessageId);
      const indexDoc = {
        gmailMessageId      : classified.gmailMessageId,
        status              : classified.kind,           // "linked" | "no_link"
        linkedAt            : doc.data().createdAt || FV.serverTimestamp(),
        backfilledAt        : FV.serverTimestamp(),
        backfilledFromAudit : doc.id
      };
      if (classified.threadId) indexDoc.threadId = classified.threadId;

      batch.set(ref, indexDoc, { merge: true });
      inBatch++;
      counts.written++;

      if (inBatch >= 450) {
        try { await batch.commit(); }
        catch (err) {
          console.warn(`[gmail-index-backfill] batch commit failed for actor=${actor}:`, err.message);
          counts.errors++;
        }
        batch = db.batch();
        inBatch = 0;
      }
    }
    if (inBatch > 0) {
      try { await batch.commit(); }
      catch (err) {
        console.warn(`[gmail-index-backfill] final batch commit failed for actor=${actor}:`, err.message);
        counts.errors++;
      }
    }

    counts.scanned += snap.docs.length;
    lastSnap = snap.docs[snap.docs.length - 1];
    if (snap.docs.length < PAGE_SIZE) break;
  }

  return counts;
}

async function runIndexBackfill({ invocationStartMs }) {
  const deadlineMs = invocationStartMs + MAX_INVOCATION_MS;
  const t0 = Date.now();
  const result = {
    scanned : 0,
    written : 0,
    skipped : 0,
    errors  : 0,
    hitLimit: false,
    byActor : {},
    durationMs: 0
  };

  for (const actor of ["system:gmail", "system:gmail-sweep"]) {
    if (Date.now() > deadlineMs) {
      result.hitLimit = true;
      break;
    }
    const counts = await backfillIndexForActor({ actor, deadlineMs });
    result.byActor[actor] = counts;
    result.scanned += counts.scanned;
    result.written += counts.written;
    result.skipped += counts.skipped;
    result.errors  += counts.errors;
    if (counts.hitLimit) result.hitLimit = true;
  }

  result.durationMs = Date.now() - t0;
  return result;
}

// ─── Entry ───────────────────────────────────────────────────────────────
exports.handler = async (event) => {
  const invocationStartMs = Date.now();

  if (!process.env.GMAIL_CLIENT_ID || !process.env.GMAIL_CLIENT_SECRET) {
    console.error("[gmail-watcher] GMAIL_CLIENT_ID / GMAIL_CLIENT_SECRET not set");
    return { statusCode: 500, body: "Missing GMAIL_CLIENT_ID / GMAIL_CLIENT_SECRET env vars" };
  }

  // Parse params from body or query string. The cron passes nothing; the
  // status/trigger endpoint may pass mode/query/windowDays.
  //
  // v1.7 — also supports mode="backfill_index": a one-shot maintenance
  // run that scans the audit log for past `thread_gmail_linked` and
  // `thread_created` (gmail-source) events and writes the corresponding
  // EtsyMail_GmailLinks index docs. Run this once immediately after
  // deploying v1.7 so the safety-net sweep doesn't treat every previously
  // linked Gmail message as orphaned. Idempotent; safe to re-run.
  let mode = "incremental";
  let query = null;
  let windowDays = null;
  try {
    if (event.body) {
      const b = JSON.parse(event.body);
      if (b.mode === "full") mode = "full";
      else if (b.mode === "backfill_index") mode = "backfill_index";
      if (typeof b.query === "string") query = b.query;
      if (typeof b.windowDays === "number") windowDays = b.windowDays;
    }
    if (event.queryStringParameters) {
      const qs = event.queryStringParameters;
      if (qs.mode === "full") mode = "full";
      else if (qs.mode === "backfill_index") mode = "backfill_index";
      if (qs.query) query = qs.query;
      if (qs.windowDays) windowDays = parseInt(qs.windowDays, 10);
    }
  } catch {}

  // v1.7 — one-shot index backfill. Short-circuits the normal flow.
  if (mode === "backfill_index") {
    try {
      const summary = await runIndexBackfill({ invocationStartMs });
      console.log("[gmail-index-backfill] complete:", JSON.stringify(summary));
      return { statusCode: 200, body: JSON.stringify({ ok: true, mode: "backfill_index", ...summary }) };
    } catch (err) {
      console.error("[gmail-index-backfill] fatal:", err);
      throw err;
    }
  }

  try {
    await writeSyncState({
      lastSyncInProgress: true,
      lastSyncStartedAt : admin.firestore.Timestamp.fromMillis(invocationStartMs),
      lastSyncMode      : mode,
      lastSyncError     : null,
      lastSyncErrorAt   : null
    });

    const summary = await runIncremental({ invocationStartMs, mode, query, windowDays });

    // v1.5 — Run the safety-net sweep AFTER the main incremental flow.
    // Independent of the watermark; catches any messages the main
    // flow advanced past without processing (transient extraction
    // failures, race conditions, network blips). Errors here don't
    // fail the invocation — the main flow already succeeded.
    let sweepSummary = null;
    try {
      sweepSummary = await runSafetyNetSweep({ invocationStartMs });
    } catch (err) {
      console.warn("[gmail-watcher] safety-net sweep failed (non-fatal):", err.message);
      sweepSummary = { error: err.message || String(err) };
    }

    const combined = { ...summary, sweep: sweepSummary };
    console.log("[gmail-watcher] complete:", JSON.stringify(combined));
    return { statusCode: 200, body: JSON.stringify({ ok: true, ...combined }) };

  } catch (err) {
    console.error("[gmail-watcher] fatal:", err);
    try {
      await writeSyncState({
        lastSyncInProgress: false,
        lastSyncError     : err.message || String(err),
        lastSyncErrorAt   : FV.serverTimestamp()
      });
    } catch {}
    // Re-throw so Netlify marks the invocation as failed (visible in
    // function logs). The next cron tick will retry.
    throw err;
  }
};

// Export the subject-parse helper so other modules (e.g. the reaper in
// etsyMailReapers.js) can reuse it without re-implementing the regex.
// Adds a property to the exports object that already carries `handler`;
// Netlify ignores extra properties on background-function modules.
module.exports.extractCustomerNameFromSubject = extractCustomerNameFromSubject;
