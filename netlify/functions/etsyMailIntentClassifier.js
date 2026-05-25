/*  netlify/functions/etsyMailIntentClassifier.js
 *
 *  v2.0 Step 1 — Intent classifier
 *
 *  Classifies a single inbound customer message into one of five buckets:
 *    support | sales_lead | post_purchase | spam | unclear
 *
 *  ═══ DESIGN NOTES ════════════════════════════════════════════════════════
 *
 *  Model: claude-haiku-4-5-20251001 — cheap, fast. Classification doesn't
 *  need Opus accuracy and we want sub-second turnaround so the auto-pipeline
 *  doesn't drag. No thinking blocks (haiku family doesn't benefit and costs
 *  more tokens with them on).
 *
 *  Output shape: strict JSON, parsed defensively. Bad model output is NOT
 *  a fatal error for the caller — we return { classification: "unclear",
 *  confidence: 0, parseError: ... } so the auto-pipeline can keep going.
 *
 *  Cache: per-thread, 24h, in EtsyMail_IntentClassifications/{threadId}.
 *  The auto-pipeline calls this once per inbound; if the same thread gets
 *  another classify request within 24h we return the cached result. Force
 *  re-classification with `force: true`. The cache lifetime matters less
 *  than the fact that classification is idempotent — the SAME message
 *  yields the SAME classification (Haiku temperature is effectively 0 for
 *  short structured outputs).
 *
 *  v2.0 Step 2 forward-compat: the cache doc shape is what the sales-lead
 *  router will read in etsyMailAutoPipeline-background.js. Don't break it.
 *  Step 2 may add fields (e.g., classifierVersion, signals_v2) — additive
 *  only.
 *
 *  ═══ REQUEST ════════════════════════════════════════════════════════════
 *
 *  POST {
 *    threadId    : "etsy_conv_1651714855",     // required
 *    messageText : "do you make custom...",     // required
 *    force       : false,                        // optional; bypass cache
 *    actor       : "system:auto-pipeline"        // optional, for audit
 *  }
 *
 *  ═══ RESPONSE ═══════════════════════════════════════════════════════════
 *
 *  {
 *    success        : true,
 *    threadId       : "...",
 *    classification : "sales_lead",
 *    confidence     : 0.85,
 *    signals        : ["custom request", "asks for quote"],
 *    reasoning      : "Customer asks if you can make a custom necklace.",
 *    cached         : false,                    // true if served from cache
 *    classifiedAt   : 1714080000000,
 *    model          : "claude-haiku-4-5-20251001",
 *    parseError     : null                      // string if model output
 *                                                // didn't parse and we
 *                                                // fell back to "unclear"
 *  }
 *
 *  ═══ ENV VARS ═══════════════════════════════════════════════════════════
 *
 *  ANTHROPIC_API_KEY              required (also used by draftReply)
 *  ETSYMAIL_INTENT_MODEL          optional; default claude-haiku-4-5-20251001
 *  ETSYMAIL_EXTENSION_SECRET      gates this endpoint (same as siblings)
 */

const fs = require("fs");
const path = require("path");

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
// v5.0 — Shared utilities are now consolidated in _etsyMailAnthropic.js.
// The fetcher returns the raw documents (thread, customer, receipts,
// messages) every AI component reasons against. The investigation
// protocol is the five-step reasoning discipline every component uses.
const {
  callClaudeRaw,
  fetchClassificationContext,
  formatContextForPrompt,
  INVESTIGATION_PROTOCOL_TEXT,
  INVESTIGATION_JSON_SCHEMA,
} = require("./_etsyMailAnthropic");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

// ─── Collections ────────────────────────────────────────────────────────
const CACHE_COLL   = "EtsyMail_IntentClassifications";
const THREADS_COLL = "EtsyMail_Threads";
const AUDIT_COLL   = "EtsyMail_Audit";

// ─── Model config ───────────────────────────────────────────────────────
// v3.31: rolled back from Sonnet 4.6 → Haiku 4.5. v3.0 had upgraded to
// Sonnet 4.6 to handle multi-arc thread-aware classification (long
// threads where the customer's "current arc" sits inside months of
// older context). In practice the prompt + thread tail is well-
// structured and 5-bucket classification is a domain Haiku 4.5
// handles cleanly. Cost matters here: classification fires once per
// inbound, every inbound, before any draft is generated — so the
// per-call savings (~25x cheaper output, ~5x cheaper input) compound
// faster than for any other path in the system. If we see wrong-arc
// misroutes in the audit log on long threads, the right escalation
// is Sonnet 4.6, not back to Opus.
const INTENT_MODEL = process.env.ETSYMAIL_INTENT_MODEL || "claude-haiku-4-5-20251001";
const INTENT_MAX_TOKENS = 1500;    // v5.0: bumped from 600 to accommodate
                                   // the investigation block (5 step
                                   // fields + the classifier verdict).
                                   // 600 was tight even before v5.0.
                                    // model's "reasoning" field on
                                    // arc-boundary calls. Cap on output.
const MESSAGE_TEXT_CAP = 4000;     // legacy single-message cap; only used
                                    // when caller passes legacy messageText
                                    // for backward compat.
const TAIL_WINDOW_MS   = 30 * 24 * 60 * 60 * 1000;  // last 30 days
const TAIL_MAX_MESSAGES = 40;       // cap on messages handed to the model
const TAIL_PER_MESSAGE_CAP = 800;   // truncate individual message bodies

// Five canonical categories. The model is instructed to pick exactly one;
// anything else falls back to "unclear".
const VALID_CATEGORIES = new Set([
  "support", "sales_lead", "post_purchase", "spam", "unclear"
]);

// ─── System prompt — loaded from Firestore EtsyMail_SalesPrompts/intent_classifier ───
//
// Source-of-truth: EtsyMail_SalesPrompts/{intent_classifier} doc, field
// `systemPrompt`. Edit via the Settings → Agent Prompts panel in the
// dashboard; takes effect on the next call (no caching, no deploy).
//
// Hard fail (no file fallback) if the doc is missing or too short. The
// handler converts the failure into a 503 with errorCode "PROMPT_NOT_LOADED"
// so the operator sees the misconfiguration immediately in audit.
const PROMPTS_COLL = "EtsyMail_SalesPrompts";
const INTENT_PROMPT_DOC_ID = "intent_classifier";

async function loadSystemPrompt() {
  try {
    const doc = await db.collection(PROMPTS_COLL).doc(INTENT_PROMPT_DOC_ID).get();
    if (!doc.exists) {
      return { ok: false, error:
        `Intent classifier prompt missing. Expected ` +
        `${PROMPTS_COLL}/${INTENT_PROMPT_DOC_ID} with field "systemPrompt". ` +
        `Seed it via Settings → Agent Prompts in the dashboard.`
      };
    }
    const d = doc.data() || {};
    const sp = typeof d.systemPrompt === "string" ? d.systemPrompt : "";
    if (sp.length < 100) {
      return { ok: false, error:
        `Intent classifier prompt too short (${sp.length} chars) at ` +
        `${PROMPTS_COLL}/${INTENT_PROMPT_DOC_ID}.systemPrompt. Likely a ` +
        `placeholder. Re-upload via Settings → Agent Prompts.`
      };
    }
    return { ok: true, prompt: sp };
  } catch (e) {
    return { ok: false, error:
      `Intent classifier prompt load failed: ${e.message}`
    };
  }
}

// ─── Helpers ────────────────────────────────────────────────────────────

function json(statusCode, body) {
  return { statusCode, headers: { ...CORS, "Content-Type": "application/json" }, body: JSON.stringify(body) };
}
function bad(msg, code = 400) { return json(code, { error: msg }); }
function ok(body)             { return json(200, { success: true, ...body }); }

async function writeAudit({ threadId, eventType, actor = "system:intentClassifier", payload = {} }) {
  // Match v1.10 canonical audit shape used by every other function in this
  // codebase: { threadId, draftId, eventType, actor, payload, createdAt }.
  // Step 2's pricing/agent code will continue to use this same shape; new
  // optional fields (outcome, ruleViolations) ride inside `payload` so the
  // top-level schema never breaks.
  try {
    await db.collection(AUDIT_COLL).add({
      threadId : threadId || null,
      draftId  : null,
      eventType,
      actor,
      payload  : payload || {},
      createdAt: FV.serverTimestamp()
    });
  } catch (e) {
    console.warn("audit write failed (non-fatal):", e.message);
  }
}

/** Pull the cached classification if it's still fresh.
 *  Returns null on cache miss / stale / parse error. */
async function readCache(threadId, messageText = "") {
  try {
    const snap = await db.collection(CACHE_COLL).doc(threadId).get();
    if (!snap.exists) return null;
    const d = snap.data() || {};
    const at = d.classifiedAt && d.classifiedAt.toMillis ? d.classifiedAt.toMillis() : 0;
    if (!at || Date.now() - at > CACHE_TTL_MS) return null;
    if (!VALID_CATEGORIES.has(d.classification)) return null;

    // v2.3 hardening: cache is keyed by thread, but classification is
    // based on a single inbound message. If the customer sends a new
    // message in the same thread within 24h, the old classification must
    // not be reused or sales leads can be misrouted as stale support.
    const currentPrefix = String(messageText || "").slice(0, 200);
    if (d.inputHashPrefix && d.inputHashPrefix !== currentPrefix) return null;

    return {
      classification: d.classification,
      confidence    : typeof d.confidence === "number" ? d.confidence : 0,
      signals       : Array.isArray(d.signals) ? d.signals : [],
      reasoning     : d.reasoning || "",
      classifiedAt  : at,
      model         : d.model || null
    };
  } catch (e) {
    console.warn("intent cache read failed:", e.message);
    return null;
  }
}

/** Write the classification to BOTH:
 *    1. EtsyMail_IntentClassifications/{threadId}  (canonical, full doc)
 *    2. EtsyMail_Threads/{threadId} (denormalized fields for thread-list
 *       rendering and Step 2 routing — readers shouldn't have to do a
 *       second collection lookup just to draw a badge or decide where to
 *       route a sales-lead).
 *
 *  Both writes happen sequentially. If the canonical write succeeds and
 *  the thread-doc denormalize fails, the thread badge will be stale until
 *  the next classify run, but the canonical record is correct. Acceptable
 *  failure mode.
 */
async function writeResult(threadId, result, messageText) {
  const nowTs = FV.serverTimestamp();
  // 1. Canonical
  await db.collection(CACHE_COLL).doc(threadId).set({
    threadId,
    classification: result.classification,
    confidence    : result.confidence,
    signals       : result.signals,
    reasoning     : result.reasoning,
    // v5.0 — persist investigation findings so the audit trail captures
    // not just the verdict but the reasoning chain that produced it.
    // Lets us replay misroutes and see which investigation step went
    // sideways without re-running the classifier.
    investigation : (result.investigation && typeof result.investigation === "object")
                       ? result.investigation : null,
    model         : result.model,
    // Hash of message text (prefix 200 chars) so we can later detect when
    // the source message changed — useful for Step 2 if we want to
    // invalidate cache when the customer sends a new message.
    inputHashPrefix: String(messageText || "").slice(0, 200),
    classifiedAt  : nowTs,
    updatedAt     : nowTs
  }, { merge: true });

  // 2. Denormalize onto thread doc for fast list-row rendering and for
  // Step 2's salesAutoEngage router (which reads classification + confidence
  // off the thread doc as part of its routing decision).
  try {
    await db.collection(THREADS_COLL).doc(threadId).set({
      intentClassification: result.classification,
      intentConfidence    : result.confidence,
      intentSignals       : result.signals,
      intentClassifiedAt  : nowTs,
      updatedAt           : nowTs
    }, { merge: true });
  } catch (e) {
    console.warn("intent denormalize on thread doc failed:", e.message);
  }
}

/** Defensive JSON parse — strips markdown fences, leading/trailing text,
 *  picks out the first balanced { } block. The model SHOULDN'T emit any
 *  of that, but defensive parsing makes us robust to a 1-in-10000 stray
 *  chain-of-thought leak.
 *
 *  Returns the parsed object, or null if no parseable JSON found. */
function tryParseJson(rawText) {
  if (!rawText || typeof rawText !== "string") return null;
  let text = rawText.trim();

  // Strip ```json ... ``` fences if present
  if (text.startsWith("```")) {
    text = text.replace(/^```(?:json)?\s*/i, "").replace(/\s*```\s*$/, "");
  }

  // Try direct parse first
  try { return JSON.parse(text); } catch {}

  // Fall back: find first { ... matching } in the string
  const start = text.indexOf("{");
  if (start === -1) return null;

  let depth = 0;
  let inString = false;
  let escape = false;
  for (let i = start; i < text.length; i++) {
    const c = text[i];
    if (escape) { escape = false; continue; }
    if (c === "\\") { escape = true; continue; }
    if (c === '"') { inString = !inString; continue; }
    if (inString) continue;
    if (c === "{") depth++;
    else if (c === "}") {
      depth--;
      if (depth === 0) {
        const candidate = text.slice(start, i + 1);
        try { return JSON.parse(candidate); } catch { return null; }
      }
    }
  }
  return null;
}

/** Validate + coerce a parsed model output into the canonical shape.
 *  Anything malformed degrades gracefully to "unclear" with confidence 0
 *  and a parseError annotation. The pipeline keeps moving. */
function coerceClassification(parsed, parseError) {
  const fallback = {
    classification: "unclear",
    confidence    : 0,
    signals       : [],
    reasoning     : parseError ? `Classifier output unparseable: ${parseError}` : "Classifier returned no usable output.",
    parseError    : parseError || "no_output"
  };
  if (!parsed || typeof parsed !== "object") return fallback;

  const cls = String(parsed.classification || "").toLowerCase().trim();
  if (!VALID_CATEGORIES.has(cls)) {
    return { ...fallback, parseError: `unknown_category: ${cls || "(empty)"}`,
             reasoning: `Model returned non-canonical category '${cls}'.` };
  }

  let conf = Number(parsed.confidence);
  if (!Number.isFinite(conf)) conf = 0;
  conf = Math.max(0, Math.min(1, conf));

  let signals = parsed.signals;
  if (!Array.isArray(signals)) signals = [];
  signals = signals.slice(0, 6).map(s => String(s).slice(0, 120));

  const reasoning = String(parsed.reasoning || "").slice(0, 500);

  return {
    classification: cls,
    confidence    : conf,
    signals,
    reasoning,
    parseError    : null
  };
}

// ─── Core classify call ────────────────────────────────────────────────

/** Pull the recent thread tail for thread-aware classification.
 *  - Includes both inbound and outbound messages (so the model can see
 *    arc boundaries — replies, gaps, topic shifts).
 *  - Filters to last 30 days.
 *  - Caps at TAIL_MAX_MESSAGES.
 *  - Truncates each message body to TAIL_PER_MESSAGE_CAP chars to keep
 *    tokens bounded on outliers (long auto-replies, pasted content).
 *  - Returns chronological order (oldest first, latest last). */
async function loadThreadTail(threadId) {
  if (!threadId) return [];
  try {
    const snap = await db.collection(THREADS_COLL).doc(threadId)
      .collection("messages")
      .orderBy("timestamp", "desc")
      .limit(TAIL_MAX_MESSAGES * 2)   // overshoot since we drop blanks
      .get();
    if (snap.empty) return [];

    const cutoff = Date.now() - TAIL_WINDOW_MS;
    const newestFirst = [];
    for (const doc of snap.docs) {
      const d = doc.data() || {};
      // Normalize timestamp to milliseconds across the shapes we store
      // (Firestore Timestamp object, raw number, optimistic-marker shape).
      let ms = 0;
      const ts = d.timestamp;
      if (ts && typeof ts.toMillis === "function") ms = ts.toMillis();
      else if (typeof ts === "number") ms = ts;
      else if (ts && typeof ts.ms === "number") ms = ts.ms;
      if (ms && ms < cutoff) continue;   // older than window

      const text = String(d.text || "").trim();
      if (!text) continue;

      newestFirst.push({
        direction  : d.direction || "unknown",
        senderName : d.senderName || (d.direction === "inbound" ? "Customer" : "Shop"),
        timestampMs: ms || 0,
        text       : text.slice(0, TAIL_PER_MESSAGE_CAP)
      });
      if (newestFirst.length >= TAIL_MAX_MESSAGES) break;
    }
    return newestFirst.slice().reverse();   // chronological
  } catch (e) {
    console.warn(`loadThreadTail failed for ${threadId}: ${e.message}`);
    return [];
  }
}

function _relTime(ms) {
  if (!ms) return "unknown time";
  const diff = Date.now() - ms;
  if (diff < 60_000) return "just now";
  const m = Math.floor(diff / 60_000);
  if (m < 60) return m + " min ago";
  const h = Math.floor(m / 60);
  if (h < 24) return h + (h === 1 ? " hour ago" : " hours ago");
  const d = Math.floor(h / 24);
  return d + (d === 1 ? " day ago" : " days ago");
}

/** Render the thread tail as a single user-message string the model reads.
 *  Format intentionally simple — newest at bottom, role tags inline. */
function _formatTailForModel(tail) {
  if (!tail.length) return "(no messages in window)";
  const lines = tail.map(m => {
    const role = m.direction === "inbound" ? "CUSTOMER" : "SHOP";
    const when = _relTime(m.timestampMs);
    return `[${when} · ${role}] ${m.senderName || "(unknown)"}\n${m.text}`;
  });
  return lines.join("\n\n");
}

/** v5.0 — Investigation-protocol classification.
 *
 *  Major rewrite of the classifier internals. Old version pulled just
 *  the message tail + thin help-request metadata block and asked the
 *  model to classify from that alone. New version fetches the full raw-
 *  document context (thread, customer, recent receipts, messages) via
 *  the shared `fetchClassificationContext`, prepends the shared five-
 *  step investigation protocol to the system prompt, and forces the
 *  model to output its investigation findings BEFORE its classification
 *  verdict. The verdict is then conditioned on the findings rather
 *  than guessed from message text alone.
 *
 *  Why: the classifier was misrouting threads where the customer used
 *  language whose meaning depended on order history we never showed
 *  it. Example: "I just ordered another necklace for my Grandmom" with
 *  engraving instructions classified as `sales_lead` because the
 *  classifier couldn't see that the customer's most recent paid order
 *  was placed minutes before the conversation began. The investigation
 *  protocol forces the model to do that correlation explicitly.
 *
 *  The Firestore-stored system prompt (intent_classifier.md) keeps its
 *  category definitions and structured-override section unchanged. The
 *  investigation protocol is layered on top in code so the same protocol
 *  string is used across the classifier, sales agent, and draft-reply
 *  — one source of truth for the investigation discipline.
 */
async function classifyThread(threadId, opts = {}) {
  // ─── Fetch the full raw-document context ──────────────────────────────
  // This call is shared across every AI component in the system; the
  // model sees the same documents the sales agent and draft-reply see.
  const ctx = await fetchClassificationContext(threadId, {
    messageLimit : 40,
    perMessageCap: 800,
    receiptLimit : 10,
  });

  // Legacy single-message fallback. If the thread has no messages in
  // the window AND a legacy caller passed messageText, synthesize a
  // single-message context entry so we don't hard-fail. Otherwise
  // return the empty-thread shape.
  if ((!ctx.messages || ctx.messages.length === 0)) {
    const legacyText = String(opts.legacyMessageText || "").trim().slice(0, MESSAGE_TEXT_CAP);
    if (!legacyText) {
      return {
        classification: "unclear",
        confidence    : 0.95,
        signals       : ["no recent messages"],
        reasoning     : "Thread has no messages within the recent window and no legacy message text was provided.",
        parseError    : null,
        model         : INTENT_MODEL,
        tailSize      : 0
      };
    }
    ctx.messages = [{
      id        : "synthetic-legacy",
      direction : "inbound",
      senderName: "Customer",
      timestamp : new Date().toISOString(),
      text      : legacyText,
      imageUrls : [],
    }];
  }

  // ─── Build the system prompt ──────────────────────────────────────────
  // System prompt = (Firestore-stored classifier prompt) + (shared
  // investigation protocol) + (the JSON shape we require the model to
  // produce). The Firestore-stored prompt owns category definitions
  // and shop-specific routing rules; the investigation protocol owns
  // the reasoning discipline. Keeping these separate means the
  // investigation protocol stays in lock-step across classifier, sales
  // agent, and draft-reply without anyone having to remember to copy
  // a change across three Firestore docs.
  const promptLoad = await loadSystemPrompt();
  if (!promptLoad.ok) {
    const err = new Error(promptLoad.error);
    err.code = "PROMPT_NOT_LOADED";
    throw err;
  }

  const systemPrompt = [
    promptLoad.prompt,
    "",
    INVESTIGATION_PROTOCOL_TEXT,
    "",
    INVESTIGATION_JSON_SCHEMA,
    "",
    "═══ CLASSIFIER-SPECIFIC OUTPUT REQUIREMENTS ════════════════════════════",
    "",
    "After the `investigation` field, your output JSON MUST also include the",
    "classifier verdict, conditioned explicitly on what your investigation",
    "found. The full required output shape is:",
    "",
    "  {",
    '    "investigation": { ... as specified in the protocol above ... },',
    '    "classification": "support" | "sales_lead" | "policy_question" | "promotion" | "spam" | "unclear",',
    '    "confidence": <number between 0 and 1>,',
    '    "signals": [ "<short label>", ... ],',
    '    "reasoning": "<one paragraph tying your classification to specific findings from the investigation>"',
    "  }",
    "",
    "Your `reasoning` MUST reference at least one specific finding from your",
    "`investigation` — for example, \"per temporal_correlation, the conversation",
    "began 23 minutes after the most recent paid order, and per reference_resolution",
    "the customer's 'the charm' resolves to Order #X; this is post-purchase, so",
    "support.\" If your reasoning could be written without looking at the",
    "investigation findings, you have not done the investigation properly.",
    "",
    "If `investigation.needs_human_review` is `true`, set `classification` to",
    "`unclear` and explain in `reasoning` why a human should disambiguate.",
  ].join("\n");

  // ─── Build the user message ───────────────────────────────────────────
  // User message = raw documents + the task. The model reads the docs,
  // walks the investigation protocol, and produces its verdict.
  const userMsg = [
    formatContextForPrompt(ctx),
    "",
    "═══ YOUR TASK ═══",
    "",
    "Complete the mandatory investigation protocol against the raw documents",
    "above, then classify the CURRENT customer arc. Output ONLY the JSON",
    "object with the required shape. No preamble, no markdown fences.",
  ].join("\n");

  const resp = await callClaudeRaw({
    model      : INTENT_MODEL,
    maxTokens  : INTENT_MAX_TOKENS,
    system     : systemPrompt,
    messages   : [{ role: "user", content: userMsg }],
    useThinking: false
  });

  const textBlocks = (resp.content || []).filter(b => b && b.type === "text");
  const rawText = textBlocks.map(b => b.text || "").join("").trim();
  const parsed = tryParseJson(rawText);
  const parseError = parsed ? null : "json_parse_failed";
  const coerced = coerceClassification(parsed, parseError);

  // Surface the investigation findings in the return value so they're
  // visible to the auto-pipeline (which writes them to the audit log
  // and can include them when handing off to downstream agents).
  const investigation = (parsed && parsed.investigation && typeof parsed.investigation === "object")
    ? parsed.investigation
    : null;

  return {
    ...coerced,
    investigation,
    model    : INTENT_MODEL,
    rawText  : rawText.slice(0, 800),
    tailSize : ctx.messages.length,
    ctxBuyerUserId: (ctx.thread && (ctx.thread.buyerUserId || ctx.thread.buyer_user_id)) || null,
    ctxReceiptCount: (ctx.recentReceipts || []).length,
  };
}

// ─── Handler ───────────────────────────────────────────────────────────

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }
  if (event.httpMethod !== "POST") {
    return json(405, { error: "Method Not Allowed" });
  }

  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return bad("Invalid JSON body"); }

  const { threadId, messageText, force = false, actor = "system:intentClassifier" } = body;
  if (!threadId) return bad("Missing threadId");
  // messageText is now optional — kept for backward compatibility. v3.0
  // pulls the thread tail directly from Firestore using threadId. If the
  // thread has no messages in the last 30 days AND messageText is given,
  // we fall back to single-message classification.

  const tStart = Date.now();

  try {
    // v3.0: NO CACHE READ. Every call is a fresh classification.
    // The cache collection is still WRITTEN by writeResult() so the
    // canonical-record-per-thread pattern continues for downstream
    // consumers, but we never read from it. `force` is now a no-op.

    // ── Classify ──
    const result = await classifyThread(threadId, { legacyMessageText: messageText });

    // ── Persist (canonical record + thread doc denormalize) ──
    await writeResult(threadId, result, messageText || "");

    // ── Audit ──
    await writeAudit({
      threadId,
      eventType: "intent_classified",
      actor,
      payload  : {
        classification: result.classification,
        confidence    : result.confidence,
        signals       : result.signals,
        reasoning     : result.reasoning,
        model         : result.model,
        tailSize      : result.tailSize || 0,
        forced        : !!force,
        parseError    : result.parseError || null,
        durationMs    : Date.now() - tStart
      }
    });

    return ok({
      threadId,
      classification: result.classification,
      confidence    : result.confidence,
      signals       : result.signals,
      reasoning     : result.reasoning,
      tailSize      : result.tailSize || 0,
      cached        : false,
      classifiedAt  : Date.now(),
      model         : result.model,
      parseError    : result.parseError || null,
      durationMs    : Date.now() - tStart
    });

  } catch (err) {
    console.error("intentClassifier error:", err);
    const isPromptMissing = err && err.code === "PROMPT_NOT_LOADED";
    await writeAudit({
      threadId,
      eventType: "intent_classify_failed",
      actor,
      payload  : {
        error    : err.message || String(err),
        errorCode: err && err.code ? err.code : null
      }
    });
    return json(isPromptMissing ? 503 : 500, {
      error    : err.message || String(err),
      errorCode: err && err.code ? err.code : null,
      threadId
    });
  }
};
