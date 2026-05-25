/* netlify/functions/claudeCodePatch.js */
/* ═══════════════════════════════════════════════════════════════════
   SYNCHRONOUS PATCH FUNCTION — v1.0
   ─────────────────────────────────────────────────────────────────
   Applies a single spec validation issue fix to the Master Prompt.
   Called by the frontend "Apply Fix" button.

   This is intentionally a REGULAR (synchronous) Netlify function —
   NOT a background function — so it can return the patched prompt
   inline in the HTTP response body.

   Flow:
     1. Read the Master Prompt from Firebase (no prompt in request body)
        Priority: ai_request.json → ai_validation_patched_prompt.txt
                  → ai_validation_original_prompt.txt
     2. Call claude-sonnet to apply the single fix
     3. Write the patched prompt back to Firebase so it stays the
        authoritative source of truth for subsequent fix calls
     4. Return { success:true, patchedPrompt } to the frontend

   Request body:  { projectPath, jobId, issue: { id, severity,
                    description, rule, recommendation } }
   Response body: { success:true, patchedPrompt, issueId }
               |  { success:false, error: "..." }
   ═══════════════════════════════════════════════════════════════════ */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

/* ─── Retry helpers (mirrors claudeCodeProxy-background.js) ────── */
const CLAUDE_OVERLOAD_MAX_RETRIES  = 5;
const CLAUDE_OVERLOAD_BASE_DELAY_MS = 1250;
const CLAUDE_OVERLOAD_MAX_DELAY_MS  = 12000;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function computeClaudeRetryDelayMs(attempt) {
  const exponentialDelay = Math.min(
    CLAUDE_OVERLOAD_BASE_DELAY_MS * Math.pow(2, Math.max(0, attempt - 1)),
    CLAUDE_OVERLOAD_MAX_DELAY_MS
  );
  return exponentialDelay + Math.floor(Math.random() * 700);
}

function isClaudeOverloadError(status, message = "") {
  const normalized = String(message || "").toLowerCase();
  if ([429, 500, 502, 503, 504, 529].includes(Number(status))) return true;
  // Network-level transient failures — no HTTP status code, matched by message
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

async function callClaude(apiKey, { model, maxTokens, system, userContent }) {
  const body = {
    model,
    max_tokens: maxTokens,
    system,
    messages: [{ role: "user", content: userContent }]
  };

  let lastError = null;

  for (let attempt = 1; attempt <= CLAUDE_OVERLOAD_MAX_RETRIES; attempt++) {
    try {
      const res = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-api-key": apiKey,
          "anthropic-version": "2023-06-01"
        },
        body: JSON.stringify(body)
      });

      const rawText = await res.text();
      let data = null;
      if (rawText) {
        try { data = JSON.parse(rawText); }
        catch (parseErr) {
          if (!res.ok) throw new Error(`Claude API error (${res.status}) with non-JSON body: ${rawText.slice(0, 500)}`);
          throw new Error(`Failed to parse Claude response JSON: ${parseErr.message}`);
        }
      }

      if (!res.ok) {
        const apiMessage = data?.error?.message || `Claude API error (${res.status})`;
        const err = new Error(apiMessage);
        err.status = res.status;
        err.isRetryableOverload = isClaudeOverloadError(res.status, apiMessage);
        throw err;
      }

      const responseText = data?.content?.find(b => b.type === "text")?.text;
      if (!responseText) throw new Error("Empty response from Claude");

      return { text: responseText, usage: data?.usage || null };

    } catch (err) {
      const status = Number(err?.status || 0);
      const retryable = Boolean(err?.isRetryableOverload) || isClaudeOverloadError(status, err?.message);
      lastError = err;
      if (!retryable || attempt >= CLAUDE_OVERLOAD_MAX_RETRIES) throw err;
      const delayMs = computeClaudeRetryDelayMs(attempt);
      console.warn(`[callClaude] retrying (attempt ${attempt}/${CLAUDE_OVERLOAD_MAX_RETRIES}, delay=${delayMs}ms): ${err.message}`);
      await sleep(delayMs);
    }
  }

  throw lastError || new Error("Claude request failed after retries");
}

/* ─── Read prompt + images from Firebase — three fallbacks in priority order */
async function readPromptFromFirebase(bucket, projectPath) {
  // 1. ai_request.json — written fresh on every pipeline submission and
  //    overwritten by the textarea debounce listener. Source of truth
  //    while the pipeline is active or has failed.
  try {
    const file = bucket.file(`${projectPath}/ai_request.json`);
    const [exists] = await file.exists();
    if (exists) {
      const [content] = await file.download();
      const parsed = JSON.parse(content.toString());
      if (parsed.prompt) {
        console.log(`[PATCH] Prompt loaded from ai_request.json (${parsed.prompt.length} chars)`);
        return { prompt: parsed.prompt, inlineImages: parsed.inlineImages || [] };
      }
    }
  } catch (e) {
    console.warn(`[PATCH] Could not read ai_request.json: ${e.message}`);
  }

  // 2. ai_validation_patched_prompt.txt — written by the internal
  //    validation auto-patch loop when it auto-patched MEDIUM issues.
  try {
    const file = bucket.file(`${projectPath}/ai_validation_patched_prompt.txt`);
    const [exists] = await file.exists();
    if (exists) {
      const [content] = await file.download();
      const text = content.toString();
      if (text.trim()) {
        console.log(`[PATCH] Prompt loaded from ai_validation_patched_prompt.txt (${text.length} chars)`);
        // No images available from this fallback path — images only live in ai_request.json
        return { prompt: text, inlineImages: [] };
      }
    }
  } catch (e) {
    console.warn(`[PATCH] Could not read ai_validation_patched_prompt.txt: ${e.message}`);
  }

  // 3. ai_validation_original_prompt.txt — preserved copy of the
  //    unmodified prompt before any auto-patching.
  try {
    const file = bucket.file(`${projectPath}/ai_validation_original_prompt.txt`);
    const [exists] = await file.exists();
    if (exists) {
      const [content] = await file.download();
      const text = content.toString();
      if (text.trim()) {
        console.log(`[PATCH] Prompt loaded from ai_validation_original_prompt.txt (${text.length} chars)`);
        return { prompt: text, inlineImages: [] };
      }
    }
  } catch (e) {
    console.warn(`[PATCH] Could not read ai_validation_original_prompt.txt: ${e.message}`);
  }

  return null;
}

/* ─── Write the patched prompt back to Firebase ─────────────────
   Updates the prompt field inside ai_request.json (preserving all
   other fields like files, selectedAssets, etc.) so the next fix
   call or pipeline resubmit picks up the latest patched version.
   Also writes ai_validation_patched_prompt.txt as a plain-text copy. */
async function writePromptToFirebase(bucket, projectPath, patchedPrompt) {
  // Update ai_request.json prompt field in-place if the file exists
  try {
    const file = bucket.file(`${projectPath}/ai_request.json`);
    const [exists] = await file.exists();
    if (exists) {
      const [content] = await file.download();
      const parsed = JSON.parse(content.toString());
      parsed.prompt = patchedPrompt;
      await file.save(JSON.stringify(parsed), { contentType: "application/json", resumable: false });
      console.log(`[PATCH] ai_request.json prompt field updated (${patchedPrompt.length} chars)`);
    }
  } catch (e) {
    console.warn(`[PATCH] Could not update ai_request.json: ${e.message}`);
  }

  // Always write the plain-text copy
  try {
    await bucket.file(`${projectPath}/ai_validation_patched_prompt.txt`)
      .save(patchedPrompt, { contentType: "text/plain", resumable: false });
  } catch (e) { /* non-fatal */ }
}

/* ═══════════════════════════════════════════════════════════════ */
exports.handler = async (event) => {
  try {
    if (!event.body) {
      return { statusCode: 400, body: JSON.stringify({ success: false, error: "Missing request body" }) };
    }

    const parsedBody = JSON.parse(event.body);
    const { projectPath, jobId, issue } = parsedBody;

    if (!projectPath) return { statusCode: 400, body: JSON.stringify({ success: false, error: "Missing projectPath" }) };
    if (!jobId)       return { statusCode: 400, body: JSON.stringify({ success: false, error: "Missing jobId" }) };
    if (!issue || !issue.recommendation) {
      return { statusCode: 400, body: JSON.stringify({ success: false, error: "Missing issue or recommendation" }) };
    }

    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) return { statusCode: 500, body: JSON.stringify({ success: false, error: "Missing ANTHROPIC_API_KEY" }) };

    const bucket = admin.storage().bucket(
      process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app"
    );

    console.log(`[PATCH] Applying fix for ${issue.id || "unknown"} (${issue.severity || "MEDIUM"}) on project ${projectPath}`);

    // ── 1. Load the Master Prompt (+ any reference images) from Firebase ─
    const firebaseData = await readPromptFromFirebase(bucket, projectPath);

    if (!firebaseData) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          success: false,
          error: "Master Prompt not found in Firebase. Please resubmit the pipeline once to re-upload the prompt, then try again."
        })
      };
    }

    const rawPrompt = firebaseData.prompt;
    const inlineImages = firebaseData.inlineImages || [];

    // Build image content blocks so the patch AI sees the same visual reference
    // that the planner and executor received during the original pipeline run.
    const imageBlocks = [];
    for (const img of inlineImages) {
      if (img.data && img.mimeType && img.mimeType.startsWith('image/')) {
        imageBlocks.push({ type: 'image', source: { type: 'base64', media_type: img.mimeType, data: img.data } });
      }
    }
    if (imageBlocks.length > 0) {
      console.log(`[PATCH] Loaded ${imageBlocks.length} reference image(s) from ai_request.json for patch context.`);
    }

    // ── 2. Build the patch prompt ─────────────────────────────────
    const imagePreamble = imageBlocks.length > 0
      ? `\nREFERENCE IMAGES: ${imageBlocks.length} game reference image(s) are attached. They carry authority equal to the Master Prompt — the same images the original planner and executor received. When applying the fix, ensure the patched spec remains consistent with the visual evidence in these images (entity complexity, layout, interaction model). Do not simplify spec language in a way that conflicts with image content.\n\n`
      : '';

    const patchPrompt = `${imagePreamble}You are a game spec editor. You have been given a Master Game Prompt \
and ONE specific issue to fix. Apply the fix described below to the prompt.

STRICT CONSTRAINTS:
- Fix ONLY the single issue listed below.
- Do NOT change any other rule, formula, condition, or threshold.
- Do NOT restructure or reformat the existing prompt.
- If the fix requires adding a new rule, append it in the same numbered-list \
style already used in the relevant section.
- If the fix requires changing a specific value or condition, change only that \
value — nothing else on the same line or in the same section.
- Output the COMPLETE updated Master Prompt — every existing line intact except \
the single change.

ISSUE TO FIX:
  ID:             ${issue.id || "n/a"}
  Severity:       ${issue.severity || "n/a"}
  Problem:        ${issue.description || "n/a"}
  Broken rule:    ${issue.rule || "n/a"}
  Required fix:   ${issue.recommendation}

MASTER GAME PROMPT:
${rawPrompt}

Output only the complete updated Master Prompt. No preamble, no explanation, \
no markdown fences — just the prompt text.`;

    // ── 3. Call Claude ────────────────────────────────────────────
    let patchedPrompt;
    try {
      const patchResult = await callClaude(apiKey, {
        model:       "claude-sonnet-4-20250514",
        maxTokens:   rawPrompt.length > 20000 ? 16000 : 8000,
        system:      "You are a game spec editor. Output only the updated Master Prompt text.",
        userContent: [
          { type: "text", text: patchPrompt },
          ...imageBlocks
        ]
      });
      patchedPrompt = patchResult.text.trim();

      if (!patchedPrompt || patchedPrompt.length < rawPrompt.length * 0.75) {
        throw new Error("Patch produced a suspiciously short result — likely truncated");
      }
    } catch (e) {
      console.error(`[PATCH] Claude call failed: ${e.message}`);
      return { statusCode: 200, body: JSON.stringify({ success: false, error: e.message }) };
    }

    // ── 4. Write patched prompt back to Firebase ──────────────────
    await writePromptToFirebase(bucket, projectPath, patchedPrompt);

    // ── 5. Audit trail ────────────────────────────────────────────
    try {
      await bucket.file(`${projectPath}/ai_validation_manual_patch_${Date.now()}.txt`)
        .save(patchedPrompt, { contentType: "text/plain", resumable: false });
    } catch (e) { /* non-fatal */ }

    console.log(`[PATCH] Complete for ${issue.id}. Original: ${rawPrompt.length} chars → Patched: ${patchedPrompt.length} chars`);

    return {
      statusCode: 200,
      body: JSON.stringify({ success: true, patchedPrompt, issueId: issue.id })
    };

  } catch (error) {
    console.error("[PATCH] Unhandled error:", error);
    return { statusCode: 500, body: JSON.stringify({ success: false, error: error.message }) };
  }
};
