/* netlify/functions/openAiCodeProxy-background.js */
/* ═══════════════════════════════════════════════════════════════════
   TRANCHED AI PIPELINE — v3.0 (Self-Chaining)
   ─────────────────────────────────────────────────────────────────
   Each invocation handles ONE unit of work then chains to itself
   for the next, staying well under Netlify's 15-min limit.

   Invocation 0  ▸  "plan"  — OpenAI planner model creates tranches, saves state
   Invocation 1–N ▸ "tranche" — OpenAI executor model executes one tranche,
                     saves accumulated files, chains to next tranche
   Final          ▸ Writes ai_response.json so the frontend picks
                     up the completed build.

   All intermediate state lives in Firebase so each invocation is
   stateless and can reconstruct context from the pipeline file.
   ═══════════════════════════════════════════════════════════════════ */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

/* ── helper: call OpenAI Responses API ───────────────────────── */
const OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses";
// Keep Planner as Pro for deep game intelligence
const OPENAI_DEFAULT_PLANNER_MODEL = process.env.OPENAI_GAME_PLANNER_MODEL || process.env.OPENAI_MODEL || "gpt-5.4";
// Set Executor to Flash for speed and cost savings
const OPENAI_DEFAULT_EXECUTOR_MODEL = process.env.OPENAI_GAME_EXECUTOR_MODEL || "gpt-5.4";

const OPENAI_DEFAULT_REASONING_EFFORT = "medium";
const OPENAI_DEFAULT_TEXT_VERBOSITY = "low";
const OPENAI_DEFAULT_MAX_OUTPUT_TOKENS = 24000;

const OPENAI_DEFAULT_PLANNER_REASONING_EFFORT = "medium";
const OPENAI_DEFAULT_PLANNER_TEXT_VERBOSITY = "medium";
const OPENAI_DEFAULT_PLANNER_MAX_OUTPUT_TOKENS = 32000;

const OPENAI_DEFAULT_EXECUTOR_REASONING_EFFORT = "medium";
const OPENAI_DEFAULT_EXECUTOR_TEXT_VERBOSITY = "low";
const OPENAI_DEFAULT_EXECUTOR_MAX_OUTPUT_TOKENS = 24000;

const OPENAI_HTTP_TIMEOUT_MS = Number(process.env.OPENAI_HTTP_TIMEOUT_MS || 1500000);
const OPENAI_PLANNER_HTTP_TIMEOUT_MS = Number(process.env.OPENAI_PLANNER_HTTP_TIMEOUT_MS || 1500000);
const OPENAI_CHAIN_ACCEPT_TIMEOUT_MS = Number(process.env.OPENAI_CHAIN_ACCEPT_TIMEOUT_MS || 1500000);
const OPENAI_PROGRESS_STREAM_FLUSH_MS = Number(process.env.OPENAI_PROGRESS_STREAM_FLUSH_MS || 8000);
const OPENAI_PROGRESS_STREAM_MIN_CHARS = Number(process.env.OPENAI_PROGRESS_STREAM_MIN_CHARS || 1200);
const OPENAI_PROGRESS_EVENTS_LIMIT = Number(process.env.OPENAI_PROGRESS_EVENTS_LIMIT || 80);
const OPENAI_PROGRESS_STREAM_PREVIEW_LIMIT = Number(process.env.OPENAI_PROGRESS_STREAM_PREVIEW_LIMIT || 4000);

function normalizeOpenAIContentBlocks(userContent) {
  const blocks = Array.isArray(userContent) ? userContent : [{ type: "text", text: String(userContent || "") }];
  const normalized = [];

  for (const block of blocks) {
    if (!block) continue;

    if (block.type === "text") {
      normalized.push({
        type: "input_text",
        text: String(block.text || "")
      });
      continue;
    }

    if (block.type === "image") {
      const mediaType = block.source?.media_type || "image/png";
      const base64Data = block.source?.data;
      if (!base64Data) continue;
      normalized.push({
        type: "input_image",
        image_url: `data:${mediaType};base64,${base64Data}`,
        detail: "high"
      });
      continue;
    }

    if (block.type === "input_text" || block.type === "input_image") {
      normalized.push(block);
    }
  }

  return normalized;
}

function extractOpenAIText(data) {
  if (!data || typeof data !== "object") return "";
  if (typeof data.output_text === "string" && data.output_text.trim()) return data.output_text;

  const outputItems = Array.isArray(data.output) ? data.output : [];
  const textParts = [];
  for (const item of outputItems) {
    const content = Array.isArray(item?.content) ? item.content : [];
    for (const part of content) {
      if (part?.type === "output_text" && typeof part.text === "string") {
        textParts.push(part.text);
      }
    }
  }

  return textParts.join("\n").trim();
}

function buildOutputBudgetBreakdown(usage) {
  const outputTokens = Number(usage?.output_tokens || 0);
  const reasoningTokens = Number(usage?.output_tokens_details?.reasoning_tokens || 0);
  const safeReasoningTokens = Math.max(0, Math.min(outputTokens, reasoningTokens));
  const restOutputTokens = Math.max(0, outputTokens - safeReasoningTokens);

  return {
    reasoning_tokens: safeReasoningTokens,
    rest_output_tokens: restOutputTokens
  };
}

function mapOpenAIUsage(usage) {
  if (!usage || typeof usage !== "object") return null;
  const outputBudget = buildOutputBudgetBreakdown(usage);
  return {
    input_tokens: usage.input_tokens || 0,
    output_tokens: usage.output_tokens || 0,
    total_tokens: usage.total_tokens || ((usage.input_tokens || 0) + (usage.output_tokens || 0)),
    input_tokens_details: usage.input_tokens_details || null,
    output_tokens_details: usage.output_tokens_details || null,
    output_budget: outputBudget
  };
}

async function fetchJsonWithTimeout(url, options = {}, timeoutMs = OPENAI_HTTP_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.max(1000, Number(timeoutMs) || OPENAI_HTTP_TIMEOUT_MS));
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } catch (error) {
    if (error && error.name === "AbortError") {
      throw new Error(`Request timed out after ${Math.round((Math.max(1000, Number(timeoutMs) || OPENAI_HTTP_TIMEOUT_MS)) / 1000)}s`);
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

async function callOpenAI(apiKey, { model, maxTokens, system, userContent, effort, verbosity, timeoutMs, stream = true, onEvent, onTextDelta }) {
  const resolvedMaxTokens = Number(maxTokens || OPENAI_DEFAULT_MAX_OUTPUT_TOKENS);
  const resolvedVerbosity = String(verbosity || OPENAI_DEFAULT_TEXT_VERBOSITY || "medium");
  
  const body = {
    model,
    input: [
      {
        role: "developer",
        content: [{ type: "input_text", text: String(system || "") }]
      },
      {
        role: "user",
        content: normalizeOpenAIContentBlocks(userContent)
      }
    ],
    max_output_tokens: resolvedMaxTokens,
    truncation: "auto",
    stream: !!stream,
    text: {
      verbosity: resolvedVerbosity
    }
  };

  // GPT-5 family models and reasoning families support reasoning.effort.
  const supportsReasoningControls =
    typeof model === "string" &&
    (
      model.startsWith("gpt-5") ||
      model.startsWith("o1") ||
      model.startsWith("o3") ||
      model.includes("pro") ||
      model.includes("reasoning")
    );

  if (supportsReasoningControls) {
    body.reasoning = {
      effort: effort || OPENAI_DEFAULT_REASONING_EFFORT
    };
  }

  const res = await fetchJsonWithTimeout(OPENAI_RESPONSES_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${apiKey}`
    },
    body: JSON.stringify(body)
  }, timeoutMs);

  if (!res.ok) {
    const raw = await res.text();
    let data = null;
    try {
      data = raw ? JSON.parse(raw) : null;
    } catch (_) {}
    throw new Error(data?.error?.message || `OpenAI API error (${res.status})`);
  }

  if (stream) {
    const streamed = await parseOpenAIStreamResponse(res, { onEvent, onTextDelta });
    const data = streamed.data;
    const responseText = extractOpenAIText(data) || String(streamed.text || '').trim();
    if (!responseText) throw new Error("Empty response from OpenAI");
    return {
      text: responseText,
      usage: mapOpenAIUsage(data?.usage)
    };
  }

  const raw = await res.text();
  let data = null;
  try {
    data = raw ? JSON.parse(raw) : null;
  } catch (parseError) {
    throw new Error(`OpenAI API returned non-JSON response (${res.status}): ${raw.slice(0, 500)}`);
  }

  const responseText = extractOpenAIText(data);
  if (!responseText) throw new Error("Empty response from OpenAI");

  return {
    text: responseText,
    usage: mapOpenAIUsage(data.usage)
  };
}

function ensureLiveProgress(progress) {
  if (!progress || typeof progress !== "object") return null;
  if (!progress.live || typeof progress.live !== "object") {
    progress.live = {
      stage: null,
      model: null,
      label: null,
      detail: null,
      streamingText: "",
      streamBytes: 0,
      events: [],
      updatedAt: Date.now()
    };
  }
  if (!Array.isArray(progress.live.events)) progress.live.events = [];
  return progress.live;
}

function trimStreamingPreview(text, maxChars = OPENAI_PROGRESS_STREAM_PREVIEW_LIMIT) {
  const value = String(text || "");
  if (value.length <= maxChars) return value;
  return value.slice(-maxChars);
}

function appendLiveEvent(progress, event) {
  const live = ensureLiveProgress(progress);
  const item = {
    id: `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    time: Date.now(),
    type: event?.type || "info",
    label: event?.label || "Update",
    detail: event?.detail || "",
    stage: event?.stage || live.stage || null,
    model: event?.model || live.model || null
  };
  live.events.push(item);
  if (live.events.length > OPENAI_PROGRESS_EVENTS_LIMIT) {
    live.events = live.events.slice(-OPENAI_PROGRESS_EVENTS_LIMIT);
  }
  live.updatedAt = Date.now();
  return item;
}

function updateLiveState(progress, patch = {}) {
  const live = ensureLiveProgress(progress);
  Object.assign(live, patch || {});
  live.updatedAt = Date.now();
  return live;
}

function createProgressTelemetry(bucket, projectPath, progress) {
  let lastPersistAt = 0;
  let lastStreamPersistAt = 0;
  let lastStreamPersistLen = 0;
  let pendingPersistPromise = null;

  async function persist(force = false) {
    const now = Date.now();
    const minGapMs = force ? 2000 : 8000;
    if (now - lastPersistAt < minGapMs) return;
    if (pendingPersistPromise) return pendingPersistPromise;
    pendingPersistPromise = (async () => {
      await saveProgress(bucket, projectPath, progress);
      lastPersistAt = Date.now();
    })();
    try {
      await pendingPersistPromise;
    } finally {
      pendingPersistPromise = null;
    }
  }

  return {
    async event(label, detail = "", options = {}) {
      appendLiveEvent(progress, { label, detail, ...options });
      if (options?.patch) updateLiveState(progress, options.patch);
      await persist(false);
    },
    async patch(patch = {}, force = false) {
      updateLiveState(progress, patch);
      await persist(force);
    },
    async stream(delta, aggregateText, options = {}) {
      const live = updateLiveState(progress, {
        stage: options.stage || progress.live?.stage || null,
        label: options.label || progress.live?.label || null,
        detail: options.detail || progress.live?.detail || null,
        model: options.model || progress.live?.model || null,
        streamingText: trimStreamingPreview(aggregateText),
        streamBytes: Number(progress.live?.streamBytes || 0) + String(delta || "").length
      });
      const now = Date.now();
      const currentLen = String(live.streamingText || "").length;
      const shouldPersist =
        (now - lastStreamPersistAt >= OPENAI_PROGRESS_STREAM_FLUSH_MS) ||
        (currentLen - lastStreamPersistLen >= OPENAI_PROGRESS_STREAM_MIN_CHARS);
      if (shouldPersist) {
        await persist(false);
        lastStreamPersistAt = now;
        lastStreamPersistLen = currentLen;
      }
    },
    async clearStream(force = true) {
      updateLiveState(progress, { streamingText: "", streamBytes: 0 });
      await persist(false);
    }
  };
}

async function parseOpenAIStreamResponse(res, callbacks = {}) {
  const bodyStream = res.body;
  if (!bodyStream) {
    throw new Error("OpenAI API returned no response body for stream.");
  }

  const decoder = new TextDecoder("utf-8");
  let buffer = "";
  let fullText = "";
  let finalResponse = null;

  async function processEventBlock(block) {
    const lines = String(block || "").split(/\r?\n/);
    let dataLines = [];
    for (const line of lines) {
      if (line.startsWith('data:')) dataLines.push(line.slice(5).trimStart());
    }
    if (!dataLines.length) return;
    const dataStr = dataLines.join('\n').trim();
    if (!dataStr || dataStr === '[DONE]') return;
    let data;
    try {
      data = JSON.parse(dataStr);
    } catch {
      return;
    }
    if (callbacks.onEvent) await callbacks.onEvent(data);
    const eventType = data?.type || '';
    if (eventType === 'response.output_text.delta' && typeof data.delta === 'string') {
      fullText += data.delta;
      if (callbacks.onTextDelta) await callbacks.onTextDelta(data.delta, fullText, data);
    } else if (eventType === 'response.output_text.done' && typeof data.text === 'string' && !fullText) {
      fullText = data.text;
    } else if (eventType === 'response.completed' || eventType === 'response.done') {
      finalResponse = data.response || data;
    } else if (eventType === 'response.failed') {
      const err = new Error(data?.response?.error?.message || data?.error?.message || 'OpenAI streaming response failed');
      err.phase = 'openai_stream';
      err.details = JSON.stringify(data).slice(0, 4000);
      throw err;
    }
  }

  for await (const chunk of bodyStream) {
    buffer += decoder.decode(chunk, { stream: true });
    const parts = buffer.split(/\n\n/);
    buffer = parts.pop() || "";
    for (const part of parts) {
      await processEventBlock(part);
    }
  }
  if (buffer.trim()) await processEventBlock(buffer);

  return {
    data: finalResponse,
    text: fullText
  };
}

/* ── helper: strip markdown fences and prose to extract JSON ─── */
/* Used ONLY for the planning phase (Opus), which outputs pure metadata
   strings — no embedded code — so JSON is safe there.               */
function stripFences(text) {
  let cleaned = text
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/\s*```$/i, "")
    .trim();

  const firstBrace = cleaned.indexOf('{');
  const lastBrace = cleaned.lastIndexOf('}');
  if (firstBrace > 0 && lastBrace > firstBrace) {
    cleaned = cleaned.substring(firstBrace, lastBrace + 1);
  }

  return cleaned.trim();
}

const REQUIRED_TRANCHE_VALIDATION_BLOCK = `VALIDATION MANIFEST RULE (copy this block verbatim into EVERY tranche prompt you generate):
---
MANDATORY VALIDATION MANIFEST: Every file you output MUST contain a machine-readable
manifest block embedded as a comment near the top of the file, using these exact markers:

VALIDATION_MANIFEST_START
{
  "file": "<exact file path e.g. models/2>",
  "systems": [
    { "id": "<snake_case_system_id>", "keywords": ["keyword1", "keyword2"], "notes": "what this file implements for this system" }
  ]
}
VALIDATION_MANIFEST_END

Rules the validator enforces — your output will be REJECTED if you break them:
1. List ONLY systems you actually implement in that specific file with real executable code.
2. Each listed system MUST have nearby executable code evidence (function, class, event handler,
   loop, conditional, assignment) that uses at least one of the declared keywords.
3. Comments, strings, and variable names alone are NOT sufficient evidence.
4. Do NOT omit the markers — a file without VALIDATION_MANIFEST_START / VALIDATION_MANIFEST_END
   will fail validation and trigger an automatic repair pass.
5. This same marker format applies to EVERY file type, including json/assets.json.
   For json/assets.json, place the manifest inside a leading /* ... */ block comment at the very top,
   then put the valid JSON content immediately after it.
---`;

function countOccurrences(haystack, needle) {
  if (!needle) return 0;
  return String(haystack || '').split(needle).length - 1;
}

function assertTranchePromptHasRequiredManifestBlock(tranche, index) {
  const prompt = String(tranche?.prompt || '').replace(/\r\n/g, '\n').trim();
  const label = `tranche ${index + 1}${tranche?.name ? ` (${tranche.name})` : ''}`;

  if (!prompt) {
    throw new Error(`Pre-execution tranche manifest assertion failed for ${label}: prompt is empty.`);
  }

  const occurrenceCount = countOccurrences(prompt, REQUIRED_TRANCHE_VALIDATION_BLOCK);
  if (occurrenceCount !== 1) {
    throw new Error(`Pre-execution tranche manifest assertion failed for ${label}: expected exactly 1 verbatim validation block, found ${occurrenceCount}.`);
  }

  const requiredFragments = [
    'VALIDATION MANIFEST RULE (copy this block verbatim into EVERY tranche prompt you generate):',
    'MANDATORY VALIDATION MANIFEST: Every file you output MUST contain a machine-readable',
    'VALIDATION_MANIFEST_START',
    '"file": "<exact file path e.g. models/2>"',
    '"systems": [',
    '"id": "<snake_case_system_id>"',
    '"keywords": ["keyword1", "keyword2"]',
    '"notes": "what this file implements for this system"',
    'VALIDATION_MANIFEST_END',
    '4. Do NOT omit the markers',
    '5. This same marker format applies to EVERY file type, including json/assets.json.'
  ];

  const missingFragments = requiredFragments.filter(fragment => !prompt.includes(fragment));
  if (missingFragments.length) {
    throw new Error(`Pre-execution tranche manifest assertion failed for ${label}: missing required manifest block fragment(s): ${missingFragments.join(' | ')}`);
  }

  return true;
}

function enforceTrancheValidationBlock(plan) {
  if (!plan || !Array.isArray(plan.tranches)) {
    throw new Error("Planner output is missing tranches.");
  }

  const failures = [];
  plan.tranches = plan.tranches.map((tranche, index) => {
    const normalized = tranche && typeof tranche === "object" ? { ...tranche } : {};
    const prompt = String(normalized.prompt || "").trim();
    if (!prompt) {
      failures.push(`tranche ${index + 1}: empty prompt`);
      return normalized;
    }
    if (!prompt.includes(REQUIRED_TRANCHE_VALIDATION_BLOCK)) {
      normalized.prompt = `${prompt}

${REQUIRED_TRANCHE_VALIDATION_BLOCK}`;
    }
    try {
      assertTranchePromptHasRequiredManifestBlock(normalized, index);
    } catch (error) {
      failures.push(error.message);
    }
    return normalized;
  });

  if (failures.length) {
    throw new Error(`Deterministic tranche manifest assertion failed: ${failures.join('; ')}`);
  }

  return plan;
}

/* ── helper: parse tranche executor delimiter-format responses ── */
/* Tranche executors output raw file content between delimiters,
   completely bypassing JSON escaping. This eliminates the entire
   class of "Unexpected non-whitespace character after JSON" errors
   that occur when Claude embeds code inside a JSON string field.

   Expected format from the executor:
     ===FILE_START: models/2===
     ...raw file content, zero escaping needed...
     ===FILE_END: models/2===

     ===MESSAGE===
     Changelog text here
     ===END_MESSAGE===
*/
function parseDelimitedResponse(text) {
  const files = [];

  // Extract all FILE_START / FILE_END blocks
  const fileRegex = /===FILE_START:\s*([^\n]+?)\s*===\n([\s\S]*?)===FILE_END:\s*\1\s*===/g;
  let match;
  while ((match = fileRegex.exec(text)) !== null) {
    const path = match[1].trim();
    const content = match[2]; // preserve exactly — no trimming
    if (path && content !== undefined) {
      files.push({ path, content });
    }
  }

  // Extract message block
  const msgMatch = text.match(/===MESSAGE===\n([\s\S]*?)===END_MESSAGE===/);
  const message = msgMatch ? msgMatch[1].trim() : "Tranche completed.";

  // If no delimiters found at all, fall back to JSON for backwards compat
  if (files.length === 0) {
    try {
      const parsed = JSON.parse(stripFences(text));
      if (parsed && Array.isArray(parsed.updatedFiles)) {
        console.warn("Executor used JSON format instead of delimiter format — parsed as fallback.");
        return parsed;
      }
    } catch (_) { /* ignore */ }
    // Return empty-handed; caller will treat as a skippable parse error
    return null;
  }

  return { updatedFiles: files, message };
}

/* ── helper: save progress to Firebase ───────────────────────── */
async function saveProgress(bucket, projectPath, progress) {
  if (progress && typeof progress === "object") {
    progress.updatedAt = Date.now();
    ensureLiveProgress(progress);
  }
  await bucket.file(`${projectPath}/ai_progress.json`).save(
    JSON.stringify(progress),
    { contentType: "application/json", resumable: false }
  );
}

/* ── helper: save ai_response.json with freshness metadata ───── */
/* Called after every successful tranche merge (checkpoint), on
   cancellation, and at final completion so the frontend always has
   the best available snapshot and can verify payload freshness.    */
async function saveAiResponse(bucket, projectPath, allUpdatedFiles, meta = {}) {
  const payload = {
    jobId:         meta.jobId        || "unknown",
    timestamp:     Date.now(),
    trancheIndex:  meta.trancheIndex !== undefined ? meta.trancheIndex : null,
    totalTranches: meta.totalTranches || null,
    status:        meta.status       || "checkpoint", // "checkpoint" | "cancelled" | "final"
    message:       meta.message      || "",
    updatedFiles:  allUpdatedFiles   || []
  };
  await bucket.file(`${projectPath}/ai_response.json`).save(
    JSON.stringify(payload),
    { contentType: "application/json", resumable: false }
  );
}

/* ── helper: save pipeline state to Firebase ─────────────────── */
async function savePipelineState(bucket, projectPath, state) {
  await bucket.file(`${projectPath}/ai_pipeline_state.json`).save(
    JSON.stringify(state),
    { contentType: "application/json", resumable: false }
  );
}

/* ── helper: load pipeline state from Firebase ───────────────── */
async function loadPipelineState(bucket, projectPath) {
  const file = bucket.file(`${projectPath}/ai_pipeline_state.json`);
  const [exists] = await file.exists();
  if (!exists) return null;
  const [content] = await file.download();
  return JSON.parse(content.toString());
}

/* ── helper: check kill switch ───────────────────────────────── */
async function checkKillSwitch(bucket, projectPath, jobId) {
  try {
    const activeJobFile = bucket.file(`${projectPath}/ai_active_job.json`);
    const [exists] = await activeJobFile.exists();
    if (exists) {
      const [content] = await activeJobFile.download();
      const activeData = JSON.parse(content.toString());

      if (activeData.jobId && activeData.jobId !== jobId) {
        return { killed: true, reason: "superseded", newJobId: activeData.jobId };
      }
      if (activeData.cancelled) {
        return { killed: true, reason: "cancelled" };
      }
    }
  } catch (e) { /* no active job file = continue safely */ }
  return { killed: false };
}

/* ── helper: self-chain — invoke this function again ─────────── */
async function chainToSelf(payload) {
  const siteUrl = process.env.URL || process.env.DEPLOY_URL || "";
  const chainUrl = `${siteUrl}/.netlify/functions/openAiCodeProxy-background`;

  console.log(`CHAIN → next step: mode=${payload.mode}, tranche=${payload.nextTranche ?? "n/a"} → ${chainUrl}`);

  try {
    const res = await fetchJsonWithTimeout(chainUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    }, OPENAI_CHAIN_ACCEPT_TIMEOUT_MS);
    const responseText = await res.text().catch(() => "");
    console.log(`Chain response status: ${res.status}`);
    if (!res.ok) {
      const err = new Error(`Self-chain failed: HTTP ${res.status} ${res.statusText || ''}`.trim());
      err.status = res.status;
      err.phase = 'self_chain';
      err.responseBody = responseText;
      throw err;
    }
  } catch (err) {
    console.error("Chain invocation failed:", err.message);
    if (!err.phase) err.phase = 'self_chain';
    throw err;
  }
}

function buildSerializableErrorPayload(error, context = {}) {
  const payload = {
    error: error?.message || String(error || 'Unknown error'),
    name: error?.name || 'Error',
    stack: typeof error?.stack === 'string' ? error.stack : null,
    context: context || {},
    timestamp: new Date().toISOString()
  };

  if (error && typeof error === 'object') {
    if (typeof error.status === 'number') payload.status = error.status;
    if (typeof error.statusCode === 'number') payload.statusCode = error.statusCode;
    if (typeof error.phase === 'string') payload.phase = error.phase;
    if (typeof error.details === 'string') payload.details = error.details;
    if (error.responseBody != null) payload.responseBody = String(error.responseBody).slice(0, 12000);
    if (error.raw != null) payload.raw = String(error.raw).slice(0, 12000);
  }

  return payload;
}

/* ═══════════════════════════════════════════════════════════════ */
exports.handler = async (event) => {
  let projectPath = null;
  let bucket = null;
  let jobId = null;

  // ── Initialize bucket FIRST so the catch block can always write
  // ai_error.json to Firebase. If bucket init is deferred past any
  // throw, a silent failure leaves the frontend polling forever.
  try {
    bucket = admin.storage().bucket(process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app");
  } catch (bucketInitErr) {
    console.error("CRITICAL: Firebase bucket initialization failed:", bucketInitErr);
    return { statusCode: 500, body: JSON.stringify({ error: "Firebase init failed: " + bucketInitErr.message }) };
  }

  try {
    console.log("[openAiCodeProxy] Handler invoked. body length:", event.body ? event.body.length : 0);

    if (!event.body) throw new Error("Missing request body");

    const parsedBody = JSON.parse(event.body);
    projectPath = parsedBody.projectPath;
    jobId = parsedBody.jobId;

    console.log(`[openAiCodeProxy] projectPath=${projectPath} jobId=${jobId}`);

    if (!projectPath) throw new Error("Missing projectPath");
    if (!jobId) throw new Error("Missing jobId");

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error("Missing OPENAI_API_KEY — set this env var in Netlify dashboard");

    console.log(`[openAiCodeProxy] API key present (length ${apiKey.length}). Bucket ready.`);

    // ── Determine mode: "plan" (initial) or "tranche" (chained) ──
    const mode = parsedBody.mode || "plan";
    const nextTranche = parsedBody.nextTranche || 0;

    // ══════════════════════════════════════════════════════════════
    //  MODE: "plan" — First invocation, do planning then chain
    // ══════════════════════════════════════════════════════════════
    if (mode === "plan") {

      // ── 1. Download the request payload from Firebase ─────────
      const requestFile = bucket.file(`${projectPath}/ai_request.json`);
      const [content] = await requestFile.download();
      const { prompt, files, selectedAssets, inlineImages } = JSON.parse(content.toString());
      if (!prompt) throw new Error("Missing instructions inside payload");

      // ── 2. Build file context string ──────────────────────────
      let fileContext = "Here are the current project files:\n\n";
      if (files) {
        for (const [path, fileContent] of Object.entries(files)) {
          fileContext += `--- FILE: ${path} ---\n${fileContent}\n\n`;
        }
      }

      // ── 3. Build multi-modal content blocks ───────────────────
      const imageBlocks = [];

      if (selectedAssets && Array.isArray(selectedAssets) && selectedAssets.length > 0) {
        let assetContext = "\n\nThe user has designated the following files for use. Their relative paths in the project are:\n";
        for (const asset of selectedAssets) {
          assetContext += `- ${asset.path}\n`;
          const isSupportedImage =
            (asset.type && asset.type.startsWith("image/")) ||
            (asset.name && asset.name.match(/\.(png|jpe?g|webp)$/i));

          if (isSupportedImage) {
            try {
              const assetRes = await fetch(asset.url);
              if (!assetRes.ok) throw new Error(`Failed to fetch: ${assetRes.statusText}`);
              const arrayBuffer = await assetRes.arrayBuffer();
              const base64Data = Buffer.from(arrayBuffer).toString("base64");
              let mime = asset.type;
              if (!mime || !mime.startsWith("image/")) {
                if (asset.name.endsWith(".png")) mime = "image/png";
                else if (asset.name.endsWith(".jpg") || asset.name.endsWith(".jpeg")) mime = "image/jpeg";
                else if (asset.name.endsWith(".webp")) mime = "image/webp";
                else mime = "image/png";
              }
              imageBlocks.push({ type: "image", source: { type: "base64", media_type: mime, data: base64Data } });
            } catch (fetchErr) {
              console.error(`Failed to fetch visual asset ${asset.name}:`, fetchErr);
            }
          } else {
            assetContext += `  (Note: ${asset.name} is a non-image file. Reference it by path in code.)\n`;
          }
        }
        fileContext += assetContext;
      }

      if (inlineImages && Array.isArray(inlineImages) && inlineImages.length > 0) {
        for (const img of inlineImages) {
          if (img.data && img.mimeType && img.mimeType.startsWith("image/")) {
            imageBlocks.push({ type: "image", source: { type: "base64", media_type: img.mimeType, data: img.data } });
          }
        }
      }

      // ══════════════════════════════════════════════════════════
      //  STAGE 0 — PLANNING (OpenAI planner, fast bounded pass)
      // ══════════════════════════════════════════════════════════
      const progress = {
        jobId: jobId,
        status: "planning",
        planningStartTime: Date.now(),
        planningEndTime: null,
        planningAnalysis: "",
        totalTranches: 0,
        currentTranche: -1,
        tranches: [],
        tokenUsage: {
          planning: null,
          tranches: [],
          totals: {
            input_tokens: 0,
            output_tokens: 0,
            reasoning_tokens: 0,
            rest_output_tokens: 0
          }
        },
        finalMessage: null,
        error: null,
        completedTime: null,
        live: {
          stage: "planning",
          model: OPENAI_DEFAULT_PLANNER_MODEL,
          label: "Planning request queued",
          detail: "Preparing Game Intelligence + Expert Planning call...",
          streamingText: "",
          streamBytes: 0,
          events: [],
          updatedAt: Date.now()
        }
      };
      await saveProgress(bucket, projectPath, progress);
      const progressTelemetry = createProgressTelemetry(bucket, projectPath, progress);
      await progressTelemetry.event("Planning job started", "Preparing GPT planning request with full project context.", { stage: "planning", model: OPENAI_DEFAULT_PLANNER_MODEL });

      const planningSystem = `You are an expert game development architect and AI pipeline planner.

Your job: analyze the user's game build/modification request and split it into sequential, self-contained TRANCHES that can be executed one at a time by a coding AI.

RULES FOR SPLITTING:
1. Each tranche should focus on 1-2 closely related concerns (e.g., "physics + movement", "UI + scoring", "pipe spawning + scrolling").
2. Tranches MUST be ordered by dependency — later tranches build on earlier ones.
3. Each tranche prompt must be FULLY SELF-CONTAINED: include all the context, rules, and specifics the coding AI needs without referencing other tranches by name.
4. Preserve ALL technical details, variable names, slot layouts, exact code snippets, and architecture rules from the original prompt in the relevant tranche(s). Do NOT summarize or lose any detail.
5. If the prompt is simple enough (minor change, single concern), use just 1 tranche.
6. For complex game builds, use 3-7 tranches. Never exceed 8.
7. Each tranche should describe what FILES it expects to create or modify.
8. The FIRST tranche should always set up the foundational scaffold that later tranches build upon.
9. The LAST tranche should handle polish, edge cases, and integration glue.

CRITICAL FILE NAMING RULES (include in every tranche prompt):
- The main logic file is named "2" (NOT "WorldController.js"), located in "models/" folder.
- The main HTML file is named "23" (NOT "document.html"), located in "models/" folder.
- "assets.json" is in the "json/" folder.

NOTE: Do NOT include validation manifest blocks in the tranche prompts you generate. Those are injected automatically server-side.

You must respond ONLY with a valid JSON object. No markdown, no code fences, no preamble.

{
  "analysis": "Brief 1-2 sentence analysis of the overall request complexity and your splitting strategy.",
  "tranches": [
    {
      "name": "Short Name",
      "description": "2-3 sentence description of what this tranche accomplishes.",
      "expertAgents": ["agent_id_1", "agent_id_2"],
      "phase": 1,
      "dependencies": [],
      "qualityCriteria": ["Criterion 1", "Criterion 2"],
      "prompt": "THE COMPLETE, SELF-CONTAINED PROMPT for the coding AI. Include all relevant technical details.",
      "expectedFiles": ["models/2", "models/23"]
    }
  ]
}`;

      const planningUserContent = [
        { type: "text", text: `${fileContext}\n\n=== FULL USER REQUEST (analyze and split into tranches) ===\n${prompt}\n=== END USER REQUEST ===` },
        ...imageBlocks
      ];

      console.log(`[openAiCodeProxy] STAGE 0: Calling OpenAI planner — model=${OPENAI_DEFAULT_PLANNER_MODEL} maxTokens=${OPENAI_DEFAULT_PLANNER_MAX_OUTPUT_TOKENS} timeout=${Math.round(OPENAI_PLANNER_HTTP_TIMEOUT_MS / 1000)}s job=${jobId}`);
      await progressTelemetry.event("Planning request sent", `Model ${OPENAI_DEFAULT_PLANNER_MODEL} with ${OPENAI_DEFAULT_PLANNER_REASONING_EFFORT} reasoning and ${OPENAI_DEFAULT_PLANNER_TEXT_VERBOSITY} verbosity.`, {
        stage: "planning",
        model: OPENAI_DEFAULT_PLANNER_MODEL,
        patch: {
          stage: "planning",
          model: OPENAI_DEFAULT_PLANNER_MODEL,
          label: "Waiting for planner response",
          detail: `Timeout ${Math.round(OPENAI_PLANNER_HTTP_TIMEOUT_MS / 1000)}s • max output ${OPENAI_DEFAULT_PLANNER_MAX_OUTPUT_TOKENS} • verbosity ${OPENAI_DEFAULT_PLANNER_TEXT_VERBOSITY}`
        }
      });
      let plannerSawFirstToken = false;
      const planResult = await callOpenAI(apiKey, {
        model: OPENAI_DEFAULT_PLANNER_MODEL,
        maxTokens: OPENAI_DEFAULT_PLANNER_MAX_OUTPUT_TOKENS,
        effort: OPENAI_DEFAULT_PLANNER_REASONING_EFFORT,
        verbosity: OPENAI_DEFAULT_PLANNER_TEXT_VERBOSITY,
        timeoutMs: OPENAI_PLANNER_HTTP_TIMEOUT_MS,
        system: planningSystem,
        userContent: planningUserContent,
        stream: true,
        onEvent: async (evt) => {
          if (evt?.type === 'response.created') {
            await progressTelemetry.patch({ stage: 'planning', model: OPENAI_DEFAULT_PLANNER_MODEL, label: 'Planner response created', detail: 'OpenAI accepted the planning request.' }, true);
          }
        },
        onTextDelta: async (delta, aggregateText) => {
          if (!plannerSawFirstToken) {
            plannerSawFirstToken = true;
            await progressTelemetry.event('Planner streaming live output', 'Realtime planner text is now flowing into the AI Context Window.', {
              stage: 'planning',
              model: OPENAI_DEFAULT_PLANNER_MODEL,
              patch: { stage: 'planning', model: OPENAI_DEFAULT_PLANNER_MODEL, label: 'Planner is reasoning live', detail: 'Streaming planning text from OpenAI...' }
            });
          }
          await progressTelemetry.stream(delta, aggregateText, {
            stage: 'planning',
            model: OPENAI_DEFAULT_PLANNER_MODEL,
            label: 'Planner is reasoning live',
            detail: 'Streaming planning text from OpenAI...'
          });
        }
      });
      await progressTelemetry.clearStream(true);
      await progressTelemetry.event('Planning response received', 'Planner finished streaming. Validating tranche plan JSON...', {
        stage: 'planning',
        model: OPENAI_DEFAULT_PLANNER_MODEL,
        patch: { stage: 'planning', model: OPENAI_DEFAULT_PLANNER_MODEL, label: 'Validating planner output', detail: 'Parsing returned JSON plan...' }
      });

      if (planResult.usage) {
        progress.tokenUsage.planning = planResult.usage;
        progress.tokenUsage.totals.input_tokens += planResult.usage.input_tokens || 0;
        progress.tokenUsage.totals.output_tokens += planResult.usage.output_tokens || 0;
        progress.tokenUsage.totals.reasoning_tokens += planResult.usage.output_budget?.reasoning_tokens || 0;
        progress.tokenUsage.totals.rest_output_tokens += planResult.usage.output_budget?.rest_output_tokens || 0;
        await saveProgress(bucket, projectPath, progress);
      }

      let plan;
      try {
        plan = JSON.parse(stripFences(planResult.text));
      } catch (e) {
        // Attempt recovery: if JSON was truncated mid-stream, try trimming back
        // to the last fully-formed tranche and closing the array/object manually.
        let recovered = null;
        try {
          const cleaned = stripFences(planResult.text);
          const lastBrace = cleaned.lastIndexOf('}');
          if (lastBrace > 0) {
            // Walk backwards from the last '}' looking for a parseable prefix
            for (let i = lastBrace; i > 0; i--) {
              if (cleaned[i] !== '}') continue;
              try {
                const candidate = JSON.parse(cleaned.substring(0, i + 1) + ']}');
                if (candidate?.tranches?.length > 0) {
                  recovered = candidate;
                  break;
                }
              } catch (_) { /* keep walking */ }
            }
          }
        } catch (_) { /* recovery itself failed — fall through */ }

        if (!recovered || !Array.isArray(recovered.tranches) || recovered.tranches.length === 0) {
          throw new Error("Failed to parse planning output as JSON: " + e.message);
        }
        console.warn(`Planning JSON was truncated — recovered ${recovered.tranches.length} tranche(s) via repair.`);
        plan = recovered;
      }

      if (!plan.tranches || !Array.isArray(plan.tranches) || plan.tranches.length === 0) {
        throw new Error("Planner returned zero tranches.");
      }

      plan = enforceTrancheValidationBlock(plan);
      await progressTelemetry.event("Planning JSON validated", `Created ${plan.tranches.length} tranche(s).`, { stage: "planning", model: OPENAI_DEFAULT_PLANNER_MODEL });

      // Update progress with plan
      progress.status = "executing";
      progress.planningEndTime = Date.now();
      progress.planningAnalysis = plan.analysis || "";
      progress.totalTranches = plan.tranches.length;
      progress.currentTranche = 0;
      progress.tranches = plan.tranches.map((t, i) => ({
        index: i,
        name: t.name,
        description: t.description,
        expertAgents: t.expertAgents || [],
        phase: t.phase || 0,
        dependencies: t.dependencies || [],
        qualityCriteria: t.qualityCriteria || [],
        prompt: t.prompt,
        expectedFiles: t.expectedFiles || [],
        status: "pending",
        startTime: null,
        endTime: null,
        message: null,
        filesUpdated: []
      }));
      await progressTelemetry.patch({ stage: "executing", model: OPENAI_DEFAULT_EXECUTOR_MODEL, label: `Plan locked: ${plan.tranches.length} tranche(s)`, detail: "Chaining into tranche execution...", streamingText: "", streamBytes: 0 }, true);

      console.log(`Plan created: ${plan.tranches.length} tranches.`);

      // ── Save pipeline state for chained invocations ──────────
      const pipelineState = {
        jobId,
        projectPath,
        progress,
        accumulatedFiles: files ? { ...files } : {},
        allUpdatedFiles: [],
        imageBlocks,
        totalTranches: plan.tranches.length
      };
      await savePipelineState(bucket, projectPath, pipelineState);

      // ── Chain to first tranche ───────────────────────────────
      await chainToSelf({
        projectPath,
        jobId,
        mode: "tranche",
        nextTranche: 0
      });

      return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: "planning_complete" }) };
    }

    // ══════════════════════════════════════════════════════════════
    //  MODE: "tranche" — Execute one tranche, then chain to next
    // ══════════════════════════════════════════════════════════════
    if (mode === "tranche") {

      // ── Kill switch check ────────────────────────────────────
      const killCheck = await checkKillSwitch(bucket, projectPath, jobId);
      if (killCheck.killed) {
        if (killCheck.reason === "superseded") {
          console.log(`Job ${jobId} superseded by ${killCheck.newJobId}. Terminating chain.`);
          return { statusCode: 200, body: JSON.stringify({ success: true, superseded: true }) };
        }
        if (killCheck.reason === "cancelled") {
          console.log("Cancellation signal detected — aborting chain.");
          const state = await loadPipelineState(bucket, projectPath);
          if (state) {
            const activeJobFile = bucket.file(`${projectPath}/ai_active_job.json`);
            await activeJobFile.delete().catch(() => {});
            state.progress.status = "cancelled";
            state.progress.finalMessage = `Pipeline cancelled by user after ${nextTranche} tranche(s).`;
            state.progress.completedTime = Date.now();
            await saveProgress(bucket, projectPath, state.progress);

            if (state.allUpdatedFiles.length > 0) {
              await saveAiResponse(bucket, projectPath, state.allUpdatedFiles, {
                jobId:         state.jobId,
                trancheIndex:  nextTranche,
                totalTranches: state.totalTranches,
                status:        "cancelled",
                message:       `Pipeline cancelled. ${state.allUpdatedFiles.length} file(s) were updated before cancellation.`
              });
            }
          }
          return { statusCode: 200, body: JSON.stringify({ success: true, cancelled: true }) };
        }
      }

      // ── Load pipeline state ──────────────────────────────────
      const state = await loadPipelineState(bucket, projectPath);
      if (!state) throw new Error("Pipeline state not found in Firebase. Chain broken.");

      const { progress, accumulatedFiles, allUpdatedFiles, imageBlocks } = state;
      const tranche = progress.tranches[nextTranche];

      if (!tranche) throw new Error(`Tranche ${nextTranche} not found in pipeline state.`);

      // ADD THIS LINE TO FIX THE ERROR:
      const progressTelemetry = createProgressTelemetry(bucket, projectPath, progress);

      // ── Mark tranche as in-progress ──────────────────────────
      progress.currentTranche = nextTranche;
      progress.tranches[nextTranche].status = "in_progress";
      progress.tranches[nextTranche].startTime = Date.now();
      await saveProgress(bucket, projectPath, progress);

      console.log(`TRANCHE ${nextTranche + 1}/${progress.totalTranches}: ${tranche.name} (Job ${jobId})`);

      // IMPORTANT: Executors use DELIMITER FORMAT, NOT JSON.
      // Embedding raw JS/HTML code inside JSON string fields causes frequent
      // parse failures because LLMs miss-escape quotes, backslashes, and
      // newlines. Delimiters require zero escaping and are completely robust.
      const executionSystem = `You are an expert game development AI.
The user will provide project files and a focused modification request (one tranche of a larger build).

You must respond using DELIMITER FORMAT only. Do NOT use JSON. Do NOT use markdown code blocks.

For each file you update or create, output it like this:

===FILE_START: path/to/filename===
...complete raw file content here, exactly as it should be saved...
===FILE_END: path/to/filename===

After all files, add a message block:

===MESSAGE===
A detailed explanation of what you implemented in this tranche, including specific functions, variables, and logic you added or changed.
===END_MESSAGE===

EXAMPLE (two files updated):
===FILE_START: models/2===
// full JS content here
===FILE_END: models/2===

===FILE_START: models/23===
<!DOCTYPE html>...full HTML here...
===FILE_END: models/23===

===MESSAGE===
Added physics body initialization and collision handler registration.
===END_MESSAGE===

CRITICAL RULES:
- Only include files that actually need to be changed or created.
- The main logic file is named "2" in the "models" folder. Never use "WorldController.js".
- The main HTML file is named "23" in the "models" folder. Never use "document.html".
- "assets.json" is in the "json" folder.
- Always output the COMPLETE file content for each updated file — not patches or diffs.
- Build upon the existing file contents provided. Do NOT discard or overwrite work from prior tranches.
- If the file already has functions, variables, or structures from prior tranches, KEEP THEM ALL and add your new code alongside them.
- The delimiter lines (===FILE_START:=== etc.) must appear exactly as shown, on their own lines.

MANDATORY VALIDATION MANIFEST (your output will be REJECTED without this):
Every file you output MUST contain a machine-readable manifest block embedded as a comment
near the top of the file content, using these exact markers on their own lines:

VALIDATION_MANIFEST_START
{
  "file": "<exact file path matching the FILE_START delimiter, e.g. models/2>",
  "systems": [
    { "id": "<snake_case_system_id>", "keywords": ["keyword1", "keyword2"], "notes": "what this file implements for this system" }
  ]
}
VALIDATION_MANIFEST_END

Enforcement rules — the downstream validator will REJECT your file and trigger a repair pass if:
1. The VALIDATION_MANIFEST_START / VALIDATION_MANIFEST_END block is missing from any output file.
2. A declared system has no nearby executable code evidence (function body, class method,
   event handler, loop, conditional, assignment) that uses at least one of its declared keywords.
3. You declare a system that only appears in comments, strings, or variable names — not in logic.
4. The manifest JSON is malformed or unparseable.

Correct approach:
- After implementing each system in real code, add its entry to the manifest.
- Use keywords that literally appear in your function/variable/event names for that system.
- Only list systems you genuinely implement in THIS file — not aspirational or planned ones.
- For models/2 (JS): embed the manifest inside a block comment /* VALIDATION_MANIFEST_START ... VALIDATION_MANIFEST_END */
- For models/23 (HTML): embed the manifest inside an HTML comment <!-- VALIDATION_MANIFEST_START ... VALIDATION_MANIFEST_END -->
- For json/assets.json: use the exact same VALIDATION_MANIFEST_START / VALIDATION_MANIFEST_END block inside a leading /* ... */ comment at the very top, then place the valid JSON body immediately after the comment.`;



      // Build file context from accumulated state
      let trancheFileContext = "Here are the current project files (includes all output from prior tranches — you MUST preserve all existing code):\n\n";
      for (const [path, fileContent] of Object.entries(accumulatedFiles)) {
        trancheFileContext += `--- FILE: ${path} ---\n${fileContent}\n\n`;
      }

      assertTranchePromptHasRequiredManifestBlock(tranche, nextTranche);

      const trancheUserContent = [
        {
          type: "text",
          text: `${trancheFileContext}\n\n=== TRANCHE ${nextTranche + 1} of ${progress.totalTranches}: "${tranche.name}" ===\n\n${tranche.prompt}\n\n=== END TRANCHE INSTRUCTIONS ===\n\nIMPORTANT: You are working on tranche ${nextTranche + 1} of ${progress.totalTranches}. The project files above contain ALL work from prior tranches. You MUST preserve all existing code and ADD your changes on top. Output the COMPLETE updated file contents.`
        },
        ...(imageBlocks || [])
      ];

      let trancheResponseObj;
      await progressTelemetry.event(`Tranche ${nextTranche + 1} started`, tranche.name || `Executing tranche ${nextTranche + 1}.`, {
        stage: 'executing',
        model: OPENAI_DEFAULT_EXECUTOR_MODEL,
        patch: {
          stage: 'executing',
          model: OPENAI_DEFAULT_EXECUTOR_MODEL,
          label: `Executing tranche ${nextTranche + 1}/${progress.totalTranches}`,
          detail: tranche.name || tranche.description || 'Running executor...',
          currentTranche: nextTranche
        }
      });
      let executorSawFirstToken = false;
      try {
        trancheResponseObj = await callOpenAI(apiKey, {
          model: OPENAI_DEFAULT_EXECUTOR_MODEL,
          maxTokens: OPENAI_DEFAULT_EXECUTOR_MAX_OUTPUT_TOKENS,
          effort: OPENAI_DEFAULT_EXECUTOR_REASONING_EFFORT,
          verbosity: OPENAI_DEFAULT_EXECUTOR_TEXT_VERBOSITY,
          timeoutMs: OPENAI_HTTP_TIMEOUT_MS,
          system: executionSystem,
          userContent: trancheUserContent,
          stream: true,
          onEvent: async (evt) => {
            if (evt?.type === 'response.created') {
              await progressTelemetry.patch({
                stage: 'executing',
                model: OPENAI_DEFAULT_EXECUTOR_MODEL,
                label: `Tranche ${nextTranche + 1}/${progress.totalTranches} response created`,
                detail: tranche.name || tranche.description || 'Executor accepted by OpenAI.',
                currentTranche: nextTranche
              }, true);
            }
          },
          onTextDelta: async (delta, aggregateText) => {
            if (!executorSawFirstToken) {
              executorSawFirstToken = true;
              await progressTelemetry.event(`Tranche ${nextTranche + 1} streaming live output`, 'Realtime executor text is now flowing into the AI Context Window.', {
                stage: 'executing',
                model: OPENAI_DEFAULT_EXECUTOR_MODEL,
                patch: {
                  stage: 'executing',
                  model: OPENAI_DEFAULT_EXECUTOR_MODEL,
                  label: `Streaming tranche ${nextTranche + 1}/${progress.totalTranches}`,
                  detail: tranche.name || tranche.description || 'Receiving streamed executor output...',
                  currentTranche: nextTranche
                }
              });
            }
            await progressTelemetry.stream(delta, aggregateText, {
              stage: 'executing',
              model: OPENAI_DEFAULT_EXECUTOR_MODEL,
              label: `Streaming tranche ${nextTranche + 1}/${progress.totalTranches}`,
              detail: tranche.name || tranche.description || 'Receiving streamed executor output...',
              currentTranche: nextTranche
            });
          }
        });
        await progressTelemetry.clearStream(true);
        await progressTelemetry.event(`Tranche ${nextTranche + 1} response received`, 'Parsing streamed executor payload and merging files...', {
          stage: 'executing',
          model: OPENAI_DEFAULT_EXECUTOR_MODEL,
          patch: {
            stage: 'executing',
            model: OPENAI_DEFAULT_EXECUTOR_MODEL,
            label: `Parsing tranche ${nextTranche + 1}/${progress.totalTranches}`,
            detail: 'Validating delimiter blocks and file payloads...',
            currentTranche: nextTranche
          }
        });
      } catch (err) {
        progress.tranches[nextTranche].status = "error";
        progress.tranches[nextTranche].endTime = Date.now();
        progress.tranches[nextTranche].message = `Error: ${err.message}`;
        await progressTelemetry.event(`Tranche ${nextTranche + 1} failed`, err.message, { stage: "executing", model: OPENAI_DEFAULT_EXECUTOR_MODEL, type: "error", patch: { label: `Tranche ${nextTranche + 1} failed`, detail: err.message, currentTranche: nextTranche } });
        console.error(`Tranche ${nextTranche + 1} failed:`, err.message);

        // Save state and chain to next tranche (skip this one)
        state.progress = progress;
        await savePipelineState(bucket, projectPath, state);

        // Checkpoint ai_response.json with whatever was accumulated so far
        if (allUpdatedFiles.length > 0) {
          await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
            jobId:         jobId,
            trancheIndex:  nextTranche,
            totalTranches: progress.totalTranches,
            status:        "checkpoint",
            message:       `Checkpoint after tranche ${nextTranche + 1} error-skip. ${allUpdatedFiles.length} file(s) so far.`
          });
        }

        if (nextTranche + 1 < progress.totalTranches) {
          await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
          return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_error_skipped` }) };
        }
        // Fall through to finalization if last tranche
      }

      // ── Process tranche response (if we got one) ─────────────
      if (trancheResponseObj) {
        // Record token usage
        if (trancheResponseObj.usage) {
          progress.tokenUsage.tranches[nextTranche] = trancheResponseObj.usage;
          progress.tokenUsage.totals.input_tokens += trancheResponseObj.usage.input_tokens || 0;
          progress.tokenUsage.totals.output_tokens += trancheResponseObj.usage.output_tokens || 0;
          progress.tokenUsage.totals.reasoning_tokens += trancheResponseObj.usage.output_budget?.reasoning_tokens || 0;
          progress.tokenUsage.totals.rest_output_tokens += trancheResponseObj.usage.output_budget?.rest_output_tokens || 0;
          progress.tranches[nextTranche].tokenUsage = trancheResponseObj.usage;
        }

        // Parse using delimiter format — no JSON escaping issues possible
        const trancheResult = parseDelimitedResponse(trancheResponseObj.text);
        if (!trancheResult) {
          progress.tranches[nextTranche].status = "error";
          progress.tranches[nextTranche].endTime = Date.now();
          progress.tranches[nextTranche].message = "Executor returned no recognisable file delimiters or valid JSON fallback.";
          await progressTelemetry.event(`Tranche ${nextTranche + 1} parse failure`, "Executor returned no delimiter payload. Skipping to keep pipeline alive.", { stage: "executing", model: OPENAI_DEFAULT_EXECUTOR_MODEL, type: "warn", patch: { label: `Tranche ${nextTranche + 1} parse failure`, detail: "No valid delimiter output returned.", currentTranche: nextTranche } });
          console.error(`Tranche ${nextTranche + 1} produced no parseable output.`);
          console.error("Raw response (first 500 chars):", trancheResponseObj.text.slice(0, 500));

          state.progress = progress;
          await savePipelineState(bucket, projectPath, state);

          // Checkpoint ai_response.json with whatever was accumulated so far
          if (allUpdatedFiles.length > 0) {
            await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
              jobId:         jobId,
              trancheIndex:  nextTranche,
              totalTranches: progress.totalTranches,
              status:        "checkpoint",
              message:       `Checkpoint after tranche ${nextTranche + 1} parse-error skip. ${allUpdatedFiles.length} file(s) so far.`
            });
          }

          if (nextTranche + 1 < progress.totalTranches) {
            await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
            return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_parse_error` }) };
          }
          // Fall through to finalization
        }

        if (trancheResult) {
          // Merge tranche output into accumulated files
          const trancheFilesUpdated = [];
          if (trancheResult.updatedFiles && Array.isArray(trancheResult.updatedFiles)) {
            for (const file of trancheResult.updatedFiles) {
              accumulatedFiles[file.path] = file.content;
              trancheFilesUpdated.push(file.path);

              const existingIdx = allUpdatedFiles.findIndex(f => f.path === file.path);
              if (existingIdx >= 0) {
                allUpdatedFiles[existingIdx] = file;
              } else {
                allUpdatedFiles.push(file);
              }
            }
          }

          // Update progress: tranche complete
          progress.tranches[nextTranche].status = "complete";
          progress.tranches[nextTranche].endTime = Date.now();
          progress.tranches[nextTranche].message = trancheResult.message || "Tranche completed.";
          progress.tranches[nextTranche].filesUpdated = trancheFilesUpdated;
          await progressTelemetry.event(`Tranche ${nextTranche + 1} merged`, `${trancheFilesUpdated.length} file(s) updated: ${trancheFilesUpdated.join(", ") || "none"}`, { stage: "executing", model: OPENAI_DEFAULT_EXECUTOR_MODEL, type: "success", patch: { label: `Tranche ${nextTranche + 1}/${progress.totalTranches} complete`, detail: trancheResult.message || `${trancheFilesUpdated.length} file(s) updated.`, currentTranche: nextTranche } });

          console.log(`Tranche ${nextTranche + 1} complete: ${trancheFilesUpdated.length} files updated.`);

          // ── Checkpoint ai_response.json after every successful merge ──
          // This ensures the frontend always has the latest snapshot even if
          // a later tranche or finalization step fails.
          if (allUpdatedFiles.length > 0) {
            await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
              jobId:         jobId,
              trancheIndex:  nextTranche,
              totalTranches: progress.totalTranches,
              status:        "checkpoint",
              message:       `Checkpoint after tranche ${nextTranche + 1}/${progress.totalTranches}: ${trancheResult.message || "completed."}`
            });
          }
        }
      }

      // ── Save updated pipeline state ──────────────────────────
      state.progress = progress;
      state.accumulatedFiles = accumulatedFiles;
      state.allUpdatedFiles = allUpdatedFiles;
      await savePipelineState(bucket, projectPath, state);

      // ── Chain to next tranche OR finalize ─────────────────────
      if (nextTranche + 1 < progress.totalTranches) {
        await chainToSelf({
          projectPath,
          jobId,
          mode: "tranche",
          nextTranche: nextTranche + 1
        });
        return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_complete` }) };
      }

      // ══════════════════════════════════════════════════════════
      //  FINAL — All tranches done, assemble and save response
      // ══════════════════════════════════════════════════════════

      const summaryParts = progress.tranches
        .filter(t => t.status === "complete")
        .map((t) => `Tranche ${t.index + 1} — ${t.name}: ${t.message}`);

      const finalMessage = summaryParts.join("\n\n") || "Build completed.";

      await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
        jobId:         jobId,
        trancheIndex:  progress.totalTranches - 1,
        totalTranches: progress.totalTranches,
        status:        "final",
        message:       finalMessage
      });

      progress.status = "complete";
      updateLiveState(progress, { stage: "complete", model: OPENAI_DEFAULT_EXECUTOR_MODEL, label: "Pipeline complete", detail: `Updated ${allUpdatedFiles.length} file(s) across ${progress.tranches.filter(tr => tr.status === "complete").length} tranche(s).`, streamingText: "", streamBytes: 0 });
      appendLiveEvent(progress, { type: "success", label: "Pipeline complete", detail: `All tranche work finished. ${allUpdatedFiles.length} file(s) are ready in ai_response.json.`, stage: "complete", model: OPENAI_DEFAULT_EXECUTOR_MODEL });
      const t = progress.tokenUsage.totals;
      progress.finalMessage = `Build complete: ${allUpdatedFiles.length} file(s) updated across ${progress.tranches.filter(tr => tr.status === "complete").length} tranche(s). Tokens: ${t.input_tokens} in / ${t.output_tokens} out (${t.reasoning_tokens} reasoning, ${t.rest_output_tokens} rest-of-output).`;
      progress.completedTime = Date.now();
      await saveProgress(bucket, projectPath, progress);

      console.log(`Total tokens — input: ${t.input_tokens}, output: ${t.output_tokens}, reasoning: ${t.reasoning_tokens}, rest_output: ${t.rest_output_tokens}`);

      // Clean up pipeline state and request files
      try { await bucket.file(`${projectPath}/ai_pipeline_state.json`).delete(); } catch (e) {}
      try { await bucket.file(`${projectPath}/ai_request.json`).delete(); } catch (e) {}

      return { statusCode: 200, body: JSON.stringify({ success: true, phase: "complete" }) };
    }

    throw new Error(`Unknown mode: ${mode}`);

  } catch (error) {
    console.error("OpenAI Code Proxy Background Error:", error);
    const errorPayload = buildSerializableErrorPayload(error, {
      projectPath,
      jobId: jobId || 'unknown'
    });
    try {
      if (projectPath && bucket) {
        await bucket.file(`${projectPath}/ai_error.json`).save(
          JSON.stringify(errorPayload, null, 2),
          { contentType: "application/json", resumable: false }
        );
        try {
          await saveProgress(bucket, projectPath, {
            jobId: jobId || "unknown",
            status: "error",
            error: errorPayload.error,
            errorDetails: errorPayload.details || errorPayload.responseBody || null,
            errorPhase: errorPayload.phase || null,
            completedTime: Date.now()
          });
        } catch (e2) {}
      }
    } catch (e) {
      console.error("CRITICAL: Failed to write error to Firebase.", e);
    }

    return {
      statusCode: 500,
      body: JSON.stringify({
        error: errorPayload.error,
        phase: errorPayload.phase || null,
        details: errorPayload.details || errorPayload.responseBody || null
      })
    };
  }
};
