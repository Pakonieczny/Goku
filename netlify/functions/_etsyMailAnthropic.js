/*  netlify/functions/_etsyMailAnthropic.js
 *
 *  Shared Anthropic API client for all EtsyMail AI features.
 *
 *  This file mirrors the exact integration pattern used by
 *  claudeCodeProxy-background.js in this same repo, for consistency:
 *    - Same env var: ANTHROPIC_API_KEY
 *    - Same endpoint + headers (incl. prompt-caching beta)
 *    - Same Opus 4.7 handling (adaptive thinking, output_config.effort,
 *      NO temperature/top_p/top_k/budget_tokens — all 400 on 4.7)
 *    - Same overload/rate-limit retry loop (5 attempts, exponential
 *      backoff with jitter, same status-code + message heuristics)
 *
 *  The one deliberate addition: a tool-use loop helper
 *  (`runToolLoop`) since EtsyMail's draft generator needs Claude to be
 *  able to call server-side tools like lookup_order_tracking and pick a
 *  terminal "compose_draft_reply" tool. claudeCodeProxy doesn't use
 *  tools so this piece is new; the HTTP client underneath is identical.
 */

const fetch = require("node-fetch");

// ─── Config ──────────────────────────────────────────────────────────────
const ANTHROPIC_URL     = "https://api.anthropic.com/v1/messages";
const ANTHROPIC_VERSION = "2023-06-01";
const ANTHROPIC_BETA    = "prompt-caching-2024-07-31";

// Retry constants — identical to claudeCodeProxy-background.js
const CLAUDE_OVERLOAD_MAX_RETRIES  = 5;
const CLAUDE_OVERLOAD_BASE_DELAY_MS = 1250;
const CLAUDE_OVERLOAD_MAX_DELAY_MS  = 12000;

// ─── Helpers ─────────────────────────────────────────────────────────────

function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

function computeClaudeRetryDelayMs(attempt) {
  const exponentialDelay = Math.min(
    CLAUDE_OVERLOAD_BASE_DELAY_MS * Math.pow(2, Math.max(0, attempt - 1)),
    CLAUDE_OVERLOAD_MAX_DELAY_MS
  );
  const jitter = Math.floor(Math.random() * 700);
  return exponentialDelay + jitter;
}

function isClaudeOverloadError(status, message = "") {
  const normalized = String(message || "").toLowerCase();
  if ([429, 500, 502, 503, 504, 529].includes(Number(status))) return true;
  if (
    normalized.includes("econnreset")     ||
    normalized.includes("econnrefused")   ||
    normalized.includes("etimedout")      ||
    normalized.includes("enotfound")      ||
    normalized.includes("socket hang up") ||
    normalized.includes("network error")  ||
    normalized.includes("fetch failed")
  ) return true;
  return (
    normalized.includes("overloaded")            ||
    normalized.includes("overload")              ||
    normalized.includes("rate limit")            ||
    normalized.includes("too many requests")     ||
    normalized.includes("capacity")              ||
    normalized.includes("temporarily unavailable")
  );
}

/** Convert a plain string system prompt into the structured cache-eligible
 *  block shape the Anthropic prompt-caching beta expects. If `system` is
 *  already an array of blocks, pass through unchanged. Matches the pattern
 *  from claudeCodeProxy-background.js::buildSystemBlocks. */
function buildSystemBlocks(system) {
  if (!system) return undefined;
  if (Array.isArray(system)) return system;
  if (typeof system === "string") {
    // Cache the entire system prompt as one ephemeral block. For EtsyMail
    // the system prompt is 2-5 KB of shop policies — big enough that cache
    // hits save meaningful input tokens on subsequent drafts in the same
    // thread. Opus 4.7 cache TTL is ~5 min by default.
    return [{ type: "text", text: system, cache_control: { type: "ephemeral" } }];
  }
  return undefined;
}

// ─── Core single-call client ─────────────────────────────────────────────

/** Make ONE request to /v1/messages with the retry loop around overload
 *  errors. Returns the raw response JSON so callers can inspect
 *  content blocks, usage, stop_reason, etc. Throws on non-retryable
 *  errors or after max retries exhausted.
 *
 *  @param {object} opts
 *  @param {string} opts.model        e.g. "claude-opus-4-7"
 *  @param {number} opts.maxTokens
 *  @param {string|object[]} opts.system
 *  @param {object[]} opts.messages   Anthropic message array
 *  @param {object[]} [opts.tools]    Tool definitions for tool-use
 *  @param {string} [opts.effort]     "low" | "medium" | "high" | "xhigh" | "max"
 *  @param {boolean} [opts.useThinking]  Opus 4.7 only; default true
 *  @param {number} [opts.budgetTokens]  Only applied to pre-4.7 models
 */
async function callClaudeRaw({
  model,
  maxTokens,
  system,
  messages,
  tools,
  effort,
  useThinking = true,
  budgetTokens
}) {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) throw new Error("Missing ANTHROPIC_API_KEY");

  const systemBlocks = buildSystemBlocks(system);

  const body = {
    model,
    max_tokens: maxTokens,
    messages
  };
  if (systemBlocks) body.system = systemBlocks;
  if (Array.isArray(tools) && tools.length) body.tools = tools;

  const isOpus47 = model && model.startsWith("claude-opus-4-7");

  if (isOpus47) {
    // Opus 4.7: adaptive thinking only; temperature/top_p/top_k/budget_tokens all 400.
    if (useThinking) body.thinking = { type: "adaptive" };
    if (effort)      body.output_config = { effort };
  } else {
    // Pre-4.7: legacy budget_tokens path
    if (budgetTokens) body.thinking = { type: "enabled", budget_tokens: budgetTokens };
    if (effort)       body.output_config = { effort };
  }

  const headers = {
    "Content-Type"     : "application/json",
    "x-api-key"        : apiKey,
    "anthropic-version": ANTHROPIC_VERSION,
    "anthropic-beta"   : ANTHROPIC_BETA
  };

  let lastError = null;

  for (let attempt = 1; attempt <= CLAUDE_OVERLOAD_MAX_RETRIES; attempt++) {
    try {
      const res = await fetch(ANTHROPIC_URL, {
        method : "POST",
        headers,
        body   : JSON.stringify(body)
      });

      const rawText = await res.text();
      let data = null;

      if (rawText) {
        try { data = JSON.parse(rawText); }
        catch (parseErr) {
          const e = new Error(`Claude returned non-JSON response: ${parseErr.message}`);
          e.status = res.status;
          e.rawText = rawText;
          e.isRetryableOverload = isClaudeOverloadError(res.status, rawText);
          throw e;
        }
      }

      if (!res.ok) {
        const errMsg = (data && data.error && data.error.message) || `Claude API error (${res.status})`;
        const e = new Error(errMsg);
        e.status = res.status;
        e.data = data;
        e.isRetryableOverload = isClaudeOverloadError(res.status, errMsg);
        throw e;
      }

      // Success — caller consumes the raw response
      console.log(
        `[callClaude] model=${model} effort=${effort || "none"} stop_reason=${data.stop_reason} ` +
        `input_tokens=${data.usage?.input_tokens || 0} output_tokens=${data.usage?.output_tokens || 0} ` +
        `cache_read=${data.usage?.cache_read_input_tokens || 0} cache_create=${data.usage?.cache_creation_input_tokens || 0} ` +
        `content_blocks=${Array.isArray(data.content) ? data.content.length : 0}`
      );
      return data;

    } catch (err) {
      const status = err && err.status ? err.status : null;
      const retryable = Boolean(err && err.isRetryableOverload) ||
                        isClaudeOverloadError(status, (err && err.message) || "");
      lastError = err;

      if (!retryable || attempt >= CLAUDE_OVERLOAD_MAX_RETRIES) throw err;

      const delayMs = computeClaudeRetryDelayMs(attempt);
      console.warn(
        `[callClaude] retrying after overload/rate-limit ` +
        `(attempt ${attempt}/${CLAUDE_OVERLOAD_MAX_RETRIES}, model=${model}, status=${status || "n/a"}, delay=${delayMs}ms): ${err.message}`
      );
      await sleep(delayMs);
    }
  }

  throw lastError || new Error("Claude request failed after retries");
}

// ─── Tool-use loop ───────────────────────────────────────────────────────

/** Run a multi-turn tool-use loop with Claude.
 *
 *  Claude returns `stop_reason: "tool_use"` when it wants to call a tool.
 *  We execute the tool, append the result as a user turn containing a
 *  tool_result block, and call Claude again. Repeat until Claude produces
 *  a natural end_turn or hits the iteration cap.
 *
 *  The caller defines tools in `toolSpecs` (Anthropic's shape) and a
 *  matching `toolExecutors` map of name → async function(input, ctx).
 *
 *  Returns { finalResponse, transcript, toolCalls, usage }.
 *    - finalResponse is the last API response (the one that ended the loop)
 *    - transcript is the full messages[] array (useful for audit/debug)
 *    - toolCalls is an array of {name, input, output, durationMs, error}
 *    - usage is aggregated across all calls (sum of input + output tokens)
 *
 *  @param {object} opts
 *  @param {string} opts.model
 *  @param {number} opts.maxTokens
 *  @param {string|object[]} opts.system
 *  @param {object[]} opts.initialMessages
 *  @param {object[]} opts.toolSpecs
 *  @param {object}   opts.toolExecutors  name → async fn(input, ctx) → result
 *  @param {object}   [opts.toolContext]  passed as 2nd arg to each executor
 *  @param {string}   [opts.effort]
 *  @param {boolean}  [opts.useThinking]
 *  @param {number}   [opts.maxIterations] default 6
 */
async function runToolLoop({
  model,
  maxTokens,
  system,
  initialMessages,
  toolSpecs,
  toolExecutors,
  toolContext = null,
  effort,
  useThinking = true,
  maxIterations = 6
}) {
  const messages = [...initialMessages];
  const toolCalls = [];
  const aggUsage = {
    input_tokens                : 0,
    output_tokens               : 0,
    cache_read_input_tokens     : 0,
    cache_creation_input_tokens : 0
  };
  let finalResponse = null;

  for (let iter = 1; iter <= maxIterations; iter++) {
    const response = await callClaudeRaw({
      model, maxTokens, system, messages,
      tools: toolSpecs, effort, useThinking
    });

    // Aggregate usage
    if (response.usage) {
      aggUsage.input_tokens                += response.usage.input_tokens                || 0;
      aggUsage.output_tokens               += response.usage.output_tokens               || 0;
      aggUsage.cache_read_input_tokens     += response.usage.cache_read_input_tokens     || 0;
      aggUsage.cache_creation_input_tokens += response.usage.cache_creation_input_tokens || 0;
    }

    finalResponse = response;

    // Append assistant turn to transcript so next iteration sees full history.
    const assistantContent = Array.isArray(response.content) ? response.content : [];
    messages.push({ role: "assistant", content: assistantContent });

    // If Claude didn't request a tool call, we're done.
    if (response.stop_reason !== "tool_use") break;

    // Gather every tool_use block and execute each one; Claude may emit
    // multiple tool calls in one turn.
    const toolUseBlocks = assistantContent.filter(b => b && b.type === "tool_use");
    if (!toolUseBlocks.length) break;  // defensive

    const toolResultBlocks = [];
    let terminalCalled = false;  // If any executor returns { __terminal: true }
                                  // we stop after processing this batch — no
                                  // further API call. This lets callers signal
                                  // a "compose final output" terminal tool
                                  // without the model burning another round-trip.

    for (const tu of toolUseBlocks) {
      const executor = toolExecutors[tu.name];
      const started  = Date.now();
      let output;
      let errMsg = null;

      if (!executor) {
        errMsg = `No executor registered for tool '${tu.name}'`;
        output = { error: errMsg };
      } else {
        try {
          output = await executor(tu.input || {}, toolContext);
        } catch (e) {
          errMsg = e.message || String(e);
          output = { error: errMsg };
        }
      }
      const durationMs = Date.now() - started;

      // Terminal-tool convention: executor returns { __terminal: true, ... }
      if (output && typeof output === "object" && output.__terminal === true) {
        terminalCalled = true;
      }

      toolCalls.push({
        name: tu.name,
        input: tu.input,
        output,
        durationMs,
        error: errMsg
      });

      // tool_result content is a string per Anthropic docs. We stringify
      // the output object so the model can read structured data.
      toolResultBlocks.push({
        type        : "tool_result",
        tool_use_id : tu.id,
        content     : typeof output === "string" ? output : JSON.stringify(output),
        is_error    : Boolean(errMsg)
      });
    }

    // Append user turn with all tool_result blocks
    messages.push({ role: "user", content: toolResultBlocks });

    // If a terminal tool fired, stop here without another API call.
    if (terminalCalled) break;
  }

  return {
    finalResponse,
    transcript: messages,
    toolCalls,
    usage: aggUsage
  };
}

module.exports = {
  callClaudeRaw,
  runToolLoop,
  isClaudeOverloadError,
  buildSystemBlocks,
  sleep,
  // v5.0 — shared context-fetcher + investigation protocol used by every
  // AI component in EtsyMail (classifier, sales agent, draft-reply,
  // design dispatch). Implementations live at the bottom of this file.
  fetchClassificationContext,
  formatContextForPrompt,
  INVESTIGATION_PROTOCOL_TEXT,
  INVESTIGATION_JSON_SCHEMA,
};

// ════════════════════════════════════════════════════════════════════════
//  v5.0 — Shared classification context + investigation protocol
// ════════════════════════════════════════════════════════════════════════
//
// Everything below this banner is the shared reasoning discipline every
// AI component in EtsyMail follows. The classifier, sales agent, draft-
// reply, and design-dispatch all import these utilities from this file
// (rather than from a separate module) to keep the Netlify-deployable
// surface small.
//
// Two pieces:
//
//   1. fetchClassificationContext(threadId) — returns the RAW Firestore
//      documents the model needs in order to reason about a thread.
//      No derived flags, no synthetic summaries, no pre-computed
//      correlations. The data is the source of truth; the model does
//      its own investigation against it. Timestamps are normalized to
//      ISO strings so the model parses them consistently (receipts
//      store created_timestamp as Unix SECONDS — without normalization
//      the model could mistake them for milliseconds).
//
//   2. INVESTIGATION_PROTOCOL_TEXT — the five-step reasoning protocol
//      every AI component prepends to its system prompt. Forces the
//      model to (1) inventory the customer's orders, (2) identify the
//      conversation arc timing, (3) compute temporal correlations,
//      (4) resolve definite references against the order history, and
//      (5) state the current ask in one paragraph — BEFORE producing
//      any component-specific output. The required JSON output shape
//      includes the investigation findings as the first field so the
//      reasoning is auditable and the downstream verdict can't quietly
//      contradict it.
//
//   3. formatContextForPrompt(ctx) — renders the raw payload as a
//      user-message-ready text block with clear delimiters.
//
// ════════════════════════════════════════════════════════════════════════

const admin = require("firebase-admin");
function _ctxDb() { return admin.firestore(); }

// ─── Collection names ────────────────────────────────────────────────────
const _CTX_THREADS_COLL   = "EtsyMail_Threads";
const _CTX_CUSTOMERS_COLL = "EtsyMail_Customers";
const _CTX_RECEIPTS_COLL  = "EtsyMail_Receipts";

// ─── Fetcher tunables ────────────────────────────────────────────────────
const _CTX_DEFAULT_MESSAGE_LIMIT  = 40;
const _CTX_DEFAULT_MESSAGE_CAP    = 1200;
const _CTX_DEFAULT_RECEIPT_LIMIT  = 10;

// ─── Timestamp normalization ─────────────────────────────────────────────
//
// Receipts store created_timestamp and updated_timestamp as Unix SECONDS
// (raw numbers, not Firestore Timestamps). Without normalization the
// model could mistake them for milliseconds (resolving to Jan 1970) and
// the investigation protocol's temporal correlation step would silently
// produce nonsense deltas. Solution: walk the payload, and whenever we
// find a numeric value at a key that looks like a timestamp, convert it
// to an ISO string. Heuristic range-checks prevent false-positive
// normalization of money values or IDs.
function _ctxLooksLikeTimestampKey(key) {
  if (typeof key !== "string") return false;
  const k = key.toLowerCase();
  return k.includes("timestamp") || k.endsWith("at") || k.endsWith("_at");
}
function _ctxMaybeNormalizeNumericTimestamp(key, value) {
  if (typeof value !== "number" || !Number.isFinite(value)) return value;
  if (!_ctxLooksLikeTimestampKey(key)) return value;
  // Unix seconds in plausible range (2001 → 2286)
  if (value >= 1e9 && value < 1e10) {
    try { return new Date(value * 1000).toISOString(); } catch { return value; }
  }
  // Unix milliseconds in plausible range
  if (value >= 1e12 && value < 1e13) {
    try { return new Date(value).toISOString(); } catch { return value; }
  }
  return value;
}
function _ctxScrubTimestamps(value, parentKey) {
  if (value === null || value === undefined) return value;
  if (typeof value === "number") {
    return _ctxMaybeNormalizeNumericTimestamp(parentKey, value);
  }
  if (typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(v => _ctxScrubTimestamps(v, parentKey));
  if (typeof value.toDate === "function") {
    try { return value.toDate().toISOString(); } catch { return null; }
  }
  if (typeof value.seconds === "number" && typeof value.nanoseconds === "number") {
    try { return new Date(value.seconds * 1000 + Math.floor(value.nanoseconds / 1e6)).toISOString(); } catch { return null; }
  }
  const out = {};
  for (const k of Object.keys(value)) out[k] = _ctxScrubTimestamps(value[k], k);
  return out;
}

// ─── Document loaders ────────────────────────────────────────────────────
async function _ctxLoadThread(threadId) {
  if (!threadId) return null;
  try {
    const snap = await _ctxDb().collection(_CTX_THREADS_COLL).doc(threadId).get();
    if (!snap.exists) return null;
    return { id: snap.id, ..._ctxScrubTimestamps(snap.data() || {}) };
  } catch (e) {
    console.warn(`[classificationContext] thread load failed for ${threadId}: ${e.message}`);
    return null;
  }
}
async function _ctxLoadCustomer(buyerUserId) {
  if (!buyerUserId) return null;
  try {
    const buyerId = String(buyerUserId);
    const snap = await _ctxDb().collection(_CTX_CUSTOMERS_COLL).doc(buyerId).get();
    if (!snap.exists) return null;
    return { id: snap.id, ..._ctxScrubTimestamps(snap.data() || {}) };
  } catch (e) {
    console.warn(`[classificationContext] customer load failed for ${buyerUserId}: ${e.message}`);
    return null;
  }
}
async function _ctxLoadReceipts(buyerUserId, limit) {
  if (!buyerUserId) return [];
  try {
    const buyerId = String(buyerUserId);
    const snap = await _ctxDb().collection(_CTX_RECEIPTS_COLL)
      .where("buyer_user_id", "==", buyerId)
      .orderBy("created_timestamp", "desc")
      .limit(limit)
      .get();
    if (snap.empty) return [];
    const out = [];
    snap.forEach(d => out.push({ id: d.id, ..._ctxScrubTimestamps(d.data() || {}) }));
    return out;
  } catch (e) {
    console.warn(`[classificationContext] receipts load failed for ${buyerUserId}: ${e.message}`);
    return [];
  }
}
async function _ctxLoadMessages(threadId, limit, perMessageCap) {
  if (!threadId) return [];
  try {
    const snap = await _ctxDb().collection(_CTX_THREADS_COLL).doc(threadId)
      .collection("messages")
      .orderBy("timestamp", "desc")
      .limit(limit * 2)
      .get();
    if (snap.empty) return [];
    const newestFirst = [];
    snap.forEach(d => {
      const m = d.data() || {};
      const text = String(m.text || "").trim();
      const imageUrls = Array.isArray(m.imageUrls)
        ? m.imageUrls.filter(u => typeof u === "string" && u)
        : [];
      if (!text && imageUrls.length === 0) return;
      let isoTime = null;
      if (m.timestamp && typeof m.timestamp.toDate === "function") {
        try { isoTime = m.timestamp.toDate().toISOString(); } catch (_) {}
      } else if (typeof m.timestamp === "number") {
        try { isoTime = new Date(m.timestamp).toISOString(); } catch (_) {}
      } else if (typeof m.timestampMs === "number") {
        try { isoTime = new Date(m.timestampMs).toISOString(); } catch (_) {}
      }
      newestFirst.push({
        id        : d.id,
        direction : m.direction || "unknown",
        senderName: m.senderName || (m.direction === "inbound" ? "Customer" : "Shop"),
        timestamp : isoTime,
        text      : text.slice(0, perMessageCap),
        imageUrls,
      });
    });
    return newestFirst.slice(0, limit).reverse(); // chronological
  } catch (e) {
    console.warn(`[classificationContext] messages load failed for ${threadId}: ${e.message}`);
    return [];
  }
}

// ─── Public: fetchClassificationContext ──────────────────────────────────
async function fetchClassificationContext(threadId, opts = {}) {
  const messageLimit  = opts.messageLimit  || _CTX_DEFAULT_MESSAGE_LIMIT;
  const perMessageCap = opts.perMessageCap || _CTX_DEFAULT_MESSAGE_CAP;
  const receiptLimit  = opts.receiptLimit  || _CTX_DEFAULT_RECEIPT_LIMIT;

  const thread = await _ctxLoadThread(threadId);
  const buyerUserId = (thread && (thread.buyerUserId || thread.buyer_user_id)) || null;

  const [customer, recentReceipts, messages] = await Promise.all([
    _ctxLoadCustomer(buyerUserId),
    _ctxLoadReceipts(buyerUserId, receiptLimit),
    _ctxLoadMessages(threadId, messageLimit, perMessageCap),
  ]);

  return {
    fetchedAt    : new Date().toISOString(),
    thread,
    customer,
    recentReceipts,
    messages,
  };
}

// ─── Investigation protocol — the shared reasoning discipline ────────────
const INVESTIGATION_PROTOCOL_TEXT = `
═══ MANDATORY INVESTIGATION PROTOCOL ═══════════════════════════════════════

Before producing any component-specific output (classification, sales
action, reply draft, design summary), you MUST complete this five-step
investigation against the raw documents provided in the user message
under "═══ THREAD CONTEXT (raw documents) ═══". No exceptions, no
shortcuts. Your downstream output will be explicitly conditioned on
your findings here.

The user message contains four raw Firestore document collections from
this conversation: \`thread\`, \`customer\`, \`recentReceipts\`, and
\`messages\`. These are real data — not pre-digested summaries. You
will read them and reason about them yourself.

─── STEP 1: ORDER HISTORY ────────────────────────────────────────────

From \`recentReceipts\` (and from \`customer\` if present), list this
customer's paid orders with their timestamps and statuses. Identify
the most recent paid order — when was it placed, what is its current
status (paid / shipped / delivered / refunded / etc.), what items did
it contain, what was the total.

If \`recentReceipts\` is empty AND \`customer\` is null, state explicitly:
"No order history on record — this customer has either never ordered,
or the receipt mirror has not yet captured their orders." This is a
valid state; many pre-purchase inquiries start this way.

─── STEP 2: CONVERSATION TIMING ──────────────────────────────────────

From \`messages\` (ordered oldest-first), identify:
  - When the CURRENT customer-led conversation arc began. A thread can
    span months with multiple arcs; the current arc starts at the
    first message in the most recent back-and-forth burst, after any
    long quiet gap.
  - When the most recent customer message was sent.
  - Whether the operator has already replied within this arc.

─── STEP 3: TEMPORAL CORRELATION ─────────────────────────────────────

Compare the timestamps from Steps 1 and 2. State explicitly:
  - How much time elapsed between the most recent paid order and the
    start of the current conversation arc. Compute the actual delta;
    do not say "recent" or "long ago" — say "23 minutes" or "4 days"
    or "3 months".
  - Did the conversation arc start BEFORE or AFTER the most recent paid
    order was placed? This matters: an arc that started after an order
    was paid is almost always about that order.
  - How recent is the most recent paid order in absolute terms — minutes,
    hours, days, weeks, months, never. State the bucket.

─── STEP 4: REFERENCE RESOLUTION (the critical step) ─────────────────

Read the customer's messages in the current arc carefully. Identify
every reference the customer makes to a product, item, or order. For
each reference, classify it:

  DEFINITE — presupposes a known antecedent. The human operator reading
    the message would automatically fill in WHICH thing. Examples:
      "the charm", "my charm", "the necklace", "that piece",
      "the one you sent", "it", "this", "my order", "for my order",
      bare nouns with "the", possessives like "my/our/his/her".

  INDEFINITE — introduces a new entity to the conversation. Examples:
      "a charm", "an engraved necklace", "one of your necklaces",
      "do you make...", "would it be possible to...".

For each DEFINITE reference, attempt resolution against
\`recentReceipts\`:
  - If exactly ONE order plausibly matches (right item type, right
    timeframe, etc.), the reference resolves to that order. State the
    order ID and your reasoning. Treat the message as post-purchase
    about that order.
  - If MULTIPLE orders could match, state which orders are candidates
    and what makes each plausible. The message is still post-purchase
    but resolution is ambiguous.
  - If NO order in \`recentReceipts\` plausibly matches, state that.
    Either (a) the customer is referring to something we don't have a
    record of yet (recent order not yet mirrored, or a different shop
    entirely), or (b) the customer is using "the" loosely to mean
    "your products in general" (in which case the framing is closer
    to an inquiry). Decide based on the rest of the arc and state
    your decision.

Indefinite references with no temporal anchor in past-tense order
language are forward-looking shopping intent.

IMPORTANT: verb tense alone is misleading. "I'd like to add engraving
to the charm" has present-tense verb morphology but a DEFINITE reference
("the charm") that resolves to the customer's existing order. The
determiner ("the" vs "a") is a stronger signal than the verb.

─── STEP 5: STATE OF THE CURRENT ASK ─────────────────────────────────

Given the findings of Steps 1-4, state in one paragraph what the
customer is actually requesting RIGHT NOW. Be specific — not "asking
about an order" but "asking to change the engraving on Order #4123 from
'My Dream' to a heart symbol, while it is still in paid-but-not-shipped
state." If reference resolution was ambiguous, state the ambiguity.

If the most plausible interpretation of the arc would meaningfully
change based on which definite reference resolves to which order, set
\`needs_human_review: true\` in your output and explain in the synopsis.
A misroute is more costly than a flagged thread for an operator to
inspect.

═══ END INVESTIGATION PROTOCOL ═════════════════════════════════════════════

After completing the investigation, produce your component-specific
output as described below. Your output JSON MUST include the
investigation findings as the first top-level field (\`investigation\`)
before any component-specific fields. The investigation is not optional
and is not "thinking" — it's the audit trail your output is grounded
in.
`.trim();

const INVESTIGATION_JSON_SCHEMA = `
The \`investigation\` field in your output JSON MUST have this shape:

  "investigation": {
    "order_history": "<step 1 finding — paragraph or bullet list>",
    "conversation_timing": "<step 2 finding — arc start, latest message, operator-replied-yet>",
    "temporal_correlation": "<step 3 finding — explicit time delta, before/after, recency bucket>",
    "reference_resolution": "<step 4 finding — each definite reference and what it resolves to>",
    "current_ask": "<step 5 finding — one-paragraph statement of what the customer needs>",
    "needs_human_review": <true if reference resolution is ambiguous in a route-changing way; otherwise false>
  }

If a step cannot be answered from the documents (e.g., no order history
at all), say so explicitly in that step's field. Do not invent data.
Do not skip steps. Empty strings are NOT acceptable for any field —
say "no data" or "none applicable" if the answer is literally nothing.
`.trim();

// ─── Context formatter — renders the raw payload for the user message ────
function formatContextForPrompt(ctx) {
  if (!ctx || typeof ctx !== "object") {
    return "═══ THREAD CONTEXT (raw documents) ═══\n(empty — no context fetched)\n═══ END THREAD CONTEXT ═══";
  }
  const lines = [];
  lines.push("═══ THREAD CONTEXT (raw documents) ═══");
  lines.push("");
  lines.push(`Context fetched at: ${ctx.fetchedAt || "(unknown)"}`);
  lines.push("");
  lines.push("─── thread doc ───");
  lines.push(JSON.stringify(ctx.thread || null, null, 2));
  lines.push("");
  lines.push("─── customer doc ───");
  lines.push(JSON.stringify(ctx.customer || null, null, 2));
  lines.push("");
  lines.push("─── recent receipts (most-recent first) ───");
  lines.push(JSON.stringify(ctx.recentReceipts || [], null, 2));
  lines.push("");
  lines.push("─── messages (oldest at top, newest at bottom) ───");
  lines.push(JSON.stringify(ctx.messages || [], null, 2));
  lines.push("");
  lines.push("═══ END THREAD CONTEXT ═══");
  return lines.join("\n");
}
