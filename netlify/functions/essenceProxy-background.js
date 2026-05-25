/* netlify/functions/essenceProxy-background.js
   ═══════════════════════════════════════════════════════════════════
   GAME ESSENCE CONTRACT ENGINE — Netlify Background Function
   ─────────────────────────────────────────────────────────────────
   Executes one of two steps:

   step: "interview"
     Reads ai_essence_request.json from Firebase.
     Sends the 12-question Essence Interview to the AI.
     Saves raw answers → ai_essence_interview.json

   step: "contract"
     Reads ai_essence_request.json from Firebase (has essenceAnswers).
     Runs the Contract Compiler prompt against the answers.
     Saves the §A-§H contract → ai_essence_contract.json

   Both result files have the shape: { jobId, result: "<text>", error?: "<msg>" }
   The frontend polls these files and resolves when result is present.

   Uses Gemini (process.env.GEMINI_API_KEY) — same key as the main proxy.
   Falls back to Claude (process.env.CLAUDE_API_KEY / ANTHROPIC_API_KEY) if set.
   ═══════════════════════════════════════════════════════════════════ */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

/* ── call Gemini ─────────────────────────────────────────────── */
async function callGemini(apiKey, { model, maxTokens, systemText, userText }) {
  const body = {
    systemInstruction: { parts: [{ text: systemText }] },
    contents: [{ role: "user", parts: [{ text: userText }] }],
    generationConfig: { maxOutputTokens: maxTokens, temperature: 0.2 }
  };
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`;
  const res  = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error?.message || `Gemini API error (${res.status})`);

  const parts = data.candidates?.[0]?.content?.parts || [];
  const text  = parts.map(p => p.text || "").join("").trim();
  if (!text) throw new Error("Empty response from Gemini");
  return text;
}

/* ── call Claude (fallback) ─────────────────────────────────── */
async function callClaude(apiKey, { maxTokens, systemText, userText }) {
  const body = {
    model: "claude-sonnet-4-6",
    max_tokens: maxTokens,
    system: systemText,
    messages: [{ role: "user", content: userText }]
  };
  const res  = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-api-key": apiKey, "anthropic-version": "2023-06-01" },
    body: JSON.stringify(body)
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error?.message || `Claude API error (${res.status})`);

  const text = (data.content || []).map(b => b.text || "").join("").trim();
  if (!text) throw new Error("Empty response from Claude");
  return text;
}

/* ── AI call dispatcher ──────────────────────────────────────── */
async function callAI(opts) {
  const geminiKey = process.env.GEMINI_API_KEY;
  const claudeKey = process.env.CLAUDE_API_KEY || process.env.ANTHROPIC_API_KEY;

  if (geminiKey) {
    return callGemini(geminiKey, { model: "gemini-3.1-pro-preview", maxTokens: 24000, ...opts });
  }
  if (claudeKey) {
    return callClaude(claudeKey, { maxTokens: 24000, ...opts });
  }
  throw new Error("No AI API key found. Set GEMINI_API_KEY or CLAUDE_API_KEY in Netlify environment variables.");
}

/* ── save result to Firebase ─────────────────────────────────── */
async function saveResult(bucket, projectPath, fileName, jobId, result, error) {
  const payload = error
    ? { jobId, error }
    : { jobId, result };
  await bucket.file(`${projectPath}/${fileName}`).save(
    JSON.stringify(payload),
    { contentType: "application/json", resumable: false }
  );
}

/* ════════════════════════════════════════════════════════════════
   ESSENCE INTERVIEW PROMPT
   12 canonical questions → concrete, engine-grade answers.
   Each answer maps to a specific Cherry3D implication that the
   Contract Compiler will resolve into exact code values.
════════════════════════════════════════════════════════════════ */
function buildInterviewPrompt(gameName, gameDescription, priorGameType) {
  return {
    systemText: `You are a Cherry3D game engine architect performing a Game Essence Interview.
Your task: extract precise, engineering-grade answers from the game description.
Each answer must be specific enough that an AI could derive EXACT numbers from it.
Vague answers are not acceptable — be concrete.
Output ONLY the 12 Q&A blocks below. No preamble, no summary.`,

    userText: `Game Name: ${gameName}
Game Description: ${gameDescription}
Prior Game Type to AVOID inheriting from: ${priorGameType || "None specified"}

Answer each question in EXACTLY this format:
Q[N] · [NAME]
ANSWER: [Direct, specific answer]
ENGINE_IMPLICATION: [The exact Cherry3D consequence — property, value, or system this requires]

────────────────────────────────────────────────────────────

Q1 · MOVEMENT_PLANE
"On which geometric plane does ALL player movement occur during gameplay?"
(XZ = top-down/maze; XY = side-scroller; XYZ = full 3D)
State exact axes. State which axis (if any) is locked.

Q2 · CAMERA_RELATIONSHIP
"Describe the camera's geometric relationship to the world in one precise clause."
State: fixed overhead? follows from behind? fixed side? orbits?
Does the camera translate during normal gameplay? Yes/No.

Q3 · VERTICAL_FREEDOM
"Can the player intentionally change their height (Y world-position) during gameplay?"
Answer YES or NO first.
If NO: state the exact constant Y value (e.g. Y = 0.5 units above ground).
If YES: describe what causes height change (jump, fly, swim, fall).

Q4 · INPUT_DIRECTIONS
"List EVERY direction the player can move. Give exact count."
For each: name the key, the direction, and the world-axis sign it maps to.
(e.g. ArrowUp → forward → vz = -SPEED)

Q5 · PROJECTILE_EXISTENCE
"Does ANYTHING travel through space after being launched, thrown, or fired?"
Answer YES or NO first.
If NO: state explicitly "No projectile system exists."
If YES: name every projectile type, who fires them, and their lifetime.

Q6 · DEATH_TRIGGERS
"What causes the player to lose a life or die?"
List EVERY trigger (enemy contact, obstacle, time-out, pit fall, etc.).

Q7 · ENEMY_MOTION_TYPE
"How do adversaries/enemies move? What drives their locomotion?"
State: patrol, chase, physics, scripted, stationary?
State who drives their position: physics thread or main thread?

Q8 · WORLD_STRUCTURE
"Is the world layout determined BEFORE play begins, or does it generate DURING play?"
Fixed authored layout / procedural / wave spawning / hybrid?
If fixed: how is it defined (tilemap, hardcoded, loaded from JSON)?

Q9 · PLAYER_VISIBLE_METRICS
"List the 3-6 exact metrics always visible to the player during gameplay (HUD only)."
Do NOT include pause menus or pre-round screens.

Q10 · WIN_LOSE_CONDITIONS
"What event ENDS a round successfully? What triggers GAME OVER?"
ROUND_WIN: [exact condition]
GAME_OVER: [every condition, listed separately]

Q11 · COLLECTIBLE_DETECTION
"Does the player collect items during gameplay?"
YES or NO first.
If YES: name them, state how many exist per round, state how collection is detected
(overlap trigger, physics collision, or proximity radius).
If NO: state "No collectible system exists."

Q12 · PROHIBITED_INHERITANCE
"Name 5 specific systems from OTHER game types that must NEVER appear in ${gameName}."
Format each as:
[SYSTEM_NAME] — inherits from [GAME_TYPE] — forbidden because [REASON]`
  };
}

/* ════════════════════════════════════════════════════════════════
   CONTRACT COMPILER PROMPT
   Takes the 12 interview answers and compiles them into the
   binding §A-§H Game Contract with exact Cherry3D values.
════════════════════════════════════════════════════════════════ */
function buildContractPrompt(gameName, essenceAnswers) {
  return {
    systemText: `You are a Cherry3D Game Contract Compiler.
You translate Game Essence Interview answers into a binding, code-level §A-§H Game Contract.

This contract will be prepended to EVERY tranche execution prompt.
It must be specific enough that a coding AI cannot make wrong assumptions.
Write it as executable comments and exact values — not descriptions.

Cherry3D SDK critical rules (always apply):
- Movement: main thread writes floats[0]=vx, floats[1]=vz via rb.controls.setFloat
- Camera: Module.controls.position=[x,y,z] and .target=[x,y,z] set EVERY frame in onRender
- If position.Y === target.Y, the camera has no look direction — scene appears black
- Player Y-lock: use linearFactor=[1,0,1] in rigidbody config to prevent Y drift
- Static objects: motionType=STATIC, mass=0
- Kinematic enemies: motionType=KINEMATIC, mass=0, moved via setPosition in main thread
- Dynamic player: motionType=DYNAMIC, mass=70
- No code runs until isReady is confirmed via requestAnimationFrame polling loop

Output ONLY the §A-§H contract block. No preamble, no commentary outside the sections.`,

    userText: `Game Name: ${gameName}

Game Essence Interview Answers:
${essenceAnswers}

────────────────────────────────────────────────────────────────

Produce the §A-§H Game Contract in EXACTLY this format.
Use code-comment syntax throughout. Be precise — give exact numbers, not ranges.

## ═══════════════════════════════════════════════════════════════
## GAME CONTRACT: ${gameName}
## Auto-generated from Game Essence Interview.
## Prepend this entire block to every tranche prompt.
## ═══════════════════════════════════════════════════════════════

### §A — COORDINATE SYSTEM AXIOMS
\`\`\`
// Movement plane: [which axes move, which is locked]
// Player Y position: CONSTANT = [value] OR physics-resolved
// linearFactor for player rigidbody: [exact array, e.g. [1,0,1]]
// World origin 0,0,0: [where it is — maze center, level start, etc.]
\`\`\`

### §B — CAMERA CONTRACT
\`\`\`javascript
// Set EVERY frame in onRender() — no exceptions:
Module.controls.position = [/* exact expression */];
Module.controls.target   = [/* exact expression — Y MUST differ from position.Y */];
// Camera translates during gameplay: YES/NO
// PROHIBITED camera patterns: [list patterns from other game types]
\`\`\`

### §C — INPUT-TO-VECTOR MAP
\`\`\`
// [Key]       → [Direction]  → floats[0]=vx   floats[1]=vz   (or vy for side-scroller)
// [List every key with exact positive/negative axis values]
// Speed constant: [name] = [value]
// Note: [any axis always zero — explain why]
\`\`\`

### §D — ENEMY / ADVERSARY SPECIFICATION
\`\`\`
// Count: [number]
// rigidbody motionType: KINEMATIC / DYNAMIC
// Position driven by: main thread (setPosition) / physics thread
// Spawn positions: [world-coord description or tile reference]
// Exit/entry logic: [if spawning in confined area — exact exit path]
// On-death behaviour: [what happens when eliminated]
\`\`\`

### §E — COLLECTIBLE SPECIFICATION
\`\`\`
// Exists: YES / NO
// [If YES:]
//   Type: [name]   Count per round: [number]
//   Detection: overlap trigger / physics collision / proximity radius [value]
//   Registry: logical Map<id, position> — removed on collection, NOT physics bodies
//   Special variants: [list power-ups if any]
\`\`\`

### §F — PROHIBITED PATTERNS
\`\`\`javascript
// ⛔ The following identifiers MUST NEVER appear in ANY code file for ${gameName}.
// The moment one is seen, delete it. No exceptions, no "just this once".
//
// [List exact JS function names, variable names, class names inherited from wrong genre]
// [Each entry: identifier — inherited from [GAME_TYPE] — forbidden because [REASON]]
\`\`\`

### §G — UI FRESH-BUILD MANDATE
\`\`\`
// File "23" MUST be written entirely from scratch for ${gameName}.
// Do NOT copy, reference, or adapt any prior game's HTML overlay.
//
// REQUIRED elements (these and ONLY these exist in file "23"):
//   [#elementId] — [purpose/label shown]   (one per line)
//
// PROHIBITED elements (inherited from prior games — must not exist):
//   [list inherited modal text, button labels, or IDs to avoid]
//
// Modal title must read exactly:  "[EXACT STRING]"
// Primary action button must read: "[EXACT STRING]"
\`\`\`

### §H — PER-TRANCHE ACCEPTANCE CRITERIA
\`\`\`
// Before marking a tranche complete, ALL of the following must pass:
//
// TRANCHE — World / Foundation:
//   [ ] Camera renders scene at correct angle — no black screen
//   [ ] Player spawns at Y=[value] — not floating, not clipping into floor
//   [ ] [Game-specific world structure test — e.g. all tiles rendered, maze walkable]
//
// TRANCHE — Player Movement:
//   [ ] Each direction in §C produces correct world-space displacement
//   [ ] Player Y remains [value] after 60 frames of movement
//   [ ] No jump, fly, or Y-impulse occurs (unless §A says vertical freedom = YES)
//
// TRANCHE — Enemies:
//   [ ] All [N] enemies reach the open [arena/maze/field] within [N] seconds
//   [ ] No enemy is enclosed on all sides by impassable geometry
//   [ ] Enemy motion type matches §D specification
//
// TRANCHE — UI / HUD:
//   [ ] All §G required elements present and display correct live values
//   [ ] No §G prohibited elements present
//   [ ] Modal title reads exactly: "[EXACT STRING from §G]"
\`\`\``
  };
}

/* ════════════════════════════════════════════════════════════════
   HANDLER
════════════════════════════════════════════════════════════════ */
exports.handler = async (event) => {
  let projectPath = null;
  let bucket      = null;
  let jobId       = null;

  try {
    if (!event.body) throw new Error("Missing request body");

    const body = JSON.parse(event.body);
    projectPath  = body.projectPath;
    jobId        = body.jobId;
    const step   = body.step;   // "interview" | "contract"

    if (!projectPath) throw new Error("Missing projectPath");
    if (!jobId)       throw new Error("Missing jobId");
    if (!step)        throw new Error("Missing step");

    bucket = admin.storage().bucket(
      process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app"
    );

    // ── Read the request payload from Firebase ───────────────
    const reqFile = bucket.file(`${projectPath}/ai_essence_request.json`);
    const [reqContent] = await reqFile.download();
    const req = JSON.parse(reqContent.toString());

    // Guard against stale or mismatched requests
    if (req.jobId && req.jobId !== jobId) {
      throw new Error(`Job ID mismatch: expected ${jobId}, got ${req.jobId}`);
    }

    // ══════════════════════════════════════════════════════════
    //  STEP: interview
    // ══════════════════════════════════════════════════════════
    if (step === "interview") {
      const { gameName, gameDescription, priorGameType } = req;
      if (!gameName || !gameDescription) {
        throw new Error("Missing gameName or gameDescription in request payload");
      }

      console.log(`[essenceProxy] Interview for "${gameName}" (job ${jobId})`);

      const promptParts = buildInterviewPrompt(gameName, gameDescription, priorGameType || "");
      const result = await callAI({ ...promptParts, maxTokens: 12000 });

      await saveResult(bucket, projectPath, "ai_essence_interview.json", jobId, result);
      console.log(`[essenceProxy] Interview saved — ${result.length} chars`);

      return { statusCode: 200, body: JSON.stringify({ success: true, step: "interview" }) };
    }

    // ══════════════════════════════════════════════════════════
    //  STEP: contract
    // ══════════════════════════════════════════════════════════
    if (step === "contract") {
      const { gameName, essenceAnswers } = req;
      if (!gameName || !essenceAnswers) {
        throw new Error("Missing gameName or essenceAnswers in request payload");
      }

      console.log(`[essenceProxy] Contract derivation for "${gameName}" (job ${jobId})`);

      const promptParts = buildContractPrompt(gameName, essenceAnswers);
      const result = await callAI({ ...promptParts, maxTokens: 12000 });

      await saveResult(bucket, projectPath, "ai_essence_contract.json", jobId, result);
      console.log(`[essenceProxy] Contract saved — ${result.length} chars`);

      return { statusCode: 200, body: JSON.stringify({ success: true, step: "contract" }) };
    }

    throw new Error(`Unknown step: "${step}". Expected "interview" or "contract".`);

  } catch (error) {
    console.error("[essenceProxy] Error:", error.message);

    // Write the error to Firebase so the frontend poll can surface it
    try {
      if (projectPath && bucket && jobId) {
        const step = JSON.parse(event.body || "{}").step || "unknown";
        const resultFile = step === "interview"
          ? "ai_essence_interview.json"
          : "ai_essence_contract.json";
        await saveResult(bucket, projectPath, resultFile, jobId, null, error.message);
      }
    } catch (writeErr) {
      console.error("[essenceProxy] Failed to write error to Firebase:", writeErr.message);
    }

    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};
