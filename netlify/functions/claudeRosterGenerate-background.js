/* netlify/functions/claudeRosterGenerate-background.js */
/* ═══════════════════════════════════════════════════════════════════
   GAME-SPECIFIC ASSET ROSTER GENERATION — v7.0 (Phase 1 Only)
   ─────────────────────────────────────────────────────────────────
   Background Netlify function (suffix -background = 15-min timeout).
   Returns 202 immediately. Writes result to Firebase when done.
   Frontend polls ai_asset_roster_phase1.json to detect completion.

   Flow:
     1. Read global CSV (reorganized_assets_manifest.csv) and build a
        vocabulary index: per-subsection asset name roots + visual_tags
        words aggregated from the enriched visual_tags column.
     2. Read Master Prompt + inline images from ai_request.json.
     3. PHASE 1 — Claude analyzes the game prompt + gameplay reference
        images and produces a structured list of required particle effects
        and 3D objects. Each 3D object requirement includes searchTerms —
        terse, asset-name-style tags derived from the library vocabulary —
        instead of category guesses. No category assignment in Phase 1.
     4. Save the Phase 1 payload as ai_asset_roster_phase1.json.
     5. Frontend collects one user reference image per required 3D object.
     6. claudeRosterStageAB-background builds its own CSV search index,
        scores every asset against searchTerms (weighted: visual_tags +6,
        name tokens +4, subsection tokens +2), and passes the top matches
        directly to Stage A/B vision comparison.

   Global asset paths (shared across all projects):
     CSV:  game-generator-1/projects/BASE_Files/asset_3d_objects/reorganized_assets_manifest.csv
     Zips: game-generator-1/projects/BASE_Files/asset_3d_objects/{zipName}.zip

   Request body: { projectPath, jobId }
   Response:     202 Accepted (background function — no body)
   ═══════════════════════════════════════════════════════════════════ */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

const GLOBAL_ASSET_CSV_PATH = "game-generator-1/projects/BASE_Files/asset_3d_objects/reorganized_assets_manifest.csv";
const AVATARS_ZIP_PATH_DEFAULT = "game-generator-1/projects/BASE_Files/asset_3d_objects/Avatars.zip";

/* ─── Retry helpers ──────────────────────────────────────────────── */
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function computeRetryDelay(attempt) {
  const BASE = 1250, MAX = 12000;
  return Math.min(BASE * Math.pow(2, Math.max(0, attempt - 1)), MAX) + Math.floor(Math.random() * 700);
}

function isOverload(status, msg = "") {
  const m = String(msg).toLowerCase();
  if ([429, 500, 502, 503, 504, 529].includes(Number(status))) return true;
  if (
    m.includes("econnreset")     ||
    m.includes("econnrefused")   ||
    m.includes("etimedout")      ||
    m.includes("enotfound")      ||
    m.includes("socket hang up") ||
    m.includes("network error")  ||
    m.includes("fetch failed")
  ) return true;
  return m.includes("overloaded")        ||
         m.includes("rate limit")        ||
         m.includes("too many requests") ||
         m.includes("capacity")          ||
         m.includes("temporarily unavailable");
}

async function callClaude(apiKey, { model, maxTokens, system, userContent }) {
  const MAX_RETRIES = 5;
  const body = {
    model,
    max_tokens: maxTokens,
    system,
    messages: [{ role: "user", content: userContent }]
  };
  let last;
  for (let i = 1; i <= MAX_RETRIES; i++) {
    try {
      const res = await fetch("https://api.anthropic.com/v1/messages", {
        method:  "POST",
        headers: {
          "Content-Type":      "application/json",
          "x-api-key":         apiKey,
          "anthropic-version": "2023-06-01"
        },
        body: JSON.stringify(body)
      });
      const raw  = await res.text();
      const data = raw ? JSON.parse(raw) : null;
      if (!res.ok) {
        const msg = data?.error?.message || `Claude error (${res.status})`;
        const err = Object.assign(new Error(msg), {
          status: res.status,
          isRetryableOverload: isOverload(res.status, msg)
        });
        throw err;
      }
      const text = data?.content?.find(b => b.type === "text")?.text;
      if (!text) throw new Error("Empty response from Claude");
      return { text, usage: data?.usage || null };
    } catch (err) {
      last = err;
      if (!err.isRetryableOverload && !isOverload(err.status, err.message)) throw err;
      if (i >= MAX_RETRIES) throw err;
      await sleep(computeRetryDelay(i));
    }
  }
  throw last;
}

/* ─── CSV parsing ──────────────────────────────────────────────── */
function parseCsvRows(csvText) {
  const rows = [];
  let row = [];
  let field = '';
  let i = 0;
  let inQuotes = false;

  while (i < csvText.length) {
    const ch = csvText[i];

    if (inQuotes) {
      if (ch === '"') {
        if (csvText[i + 1] === '"') {
          field += '"';
          i += 2;
          continue;
        }
        inQuotes = false;
        i += 1;
        continue;
      }
      field += ch;
      i += 1;
      continue;
    }

    if (ch === '"') {
      inQuotes = true;
      i += 1;
      continue;
    }
    if (ch === ',') {
      row.push(field);
      field = '';
      i += 1;
      continue;
    }
    if (ch === '\n') {
      row.push(field);
      rows.push(row);
      row = [];
      field = '';
      i += 1;
      continue;
    }
    if (ch === '\r') {
      i += 1;
      continue;
    }

    field += ch;
    i += 1;
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }

  return rows.filter(r => r.some(cell => String(cell || '').trim() !== ''));
}

/* ─── Parse CSV → vocabulary index for Phase 1 searchTerm generation ──
   Returns Map<subsectionTitle, { category, roots, tagWords }>
   roots    = deduplicated asset_name root words (variant suffixes stripped)
   tagWords = deduplicated visual_tags words aggregated across the subsection
   Both are surfaced in the Phase 1 prompt so Claude generates searchTerms
   that match the actual enriched library vocabulary, not just filenames.
─────────────────────────────────────────────────────────────────── */
function assetNameRoot(name) {
  return String(name || '')
    .replace(/[_\-]\d+[_\-]?[A-Za-z]*$/, '')
    .replace(/[_\-][A-Z]{1,2}$/, '')
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
    .toLowerCase()
    .replace(/^[\s_\-]+|[\s_\-]+$/g, '');
}

function parseCsvVocabulary(csvText) {
  const rows = parseCsvRows(csvText);
  if (rows.length === 0) throw new Error('CSV is empty');

  const header  = rows[0].map(h => h.trim().toLowerCase());
  const nameIdx = header.indexOf('asset_name');
  const catIdx  = header.indexOf('new_category');
  const subIdx  = header.indexOf('subsection_title');
  const tagsIdx = header.indexOf('visual_tags');
  if (nameIdx === -1 || catIdx === -1) throw new Error("CSV missing required columns");

  const subsections = new Map();
  for (let i = 1; i < rows.length; i++) {
    const name    = (rows[i][nameIdx]  || '').trim();
    const cat     = (rows[i][catIdx]   || '').trim();
    const sub     = subIdx  !== -1 ? (rows[i][subIdx]  || '').trim() : cat.split('/').pop();
    const rawTags = tagsIdx !== -1 ? (rows[i][tagsIdx] || '').trim() : '';
    if (!name || !cat) continue;
    if (!subsections.has(sub)) subsections.set(sub, { category: cat, roots: new Set(), tagWords: new Set() });
    const entry = subsections.get(sub);

    const root = assetNameRoot(name);
    if (root.length > 1) entry.roots.add(root);

    if (rawTags) {
      for (const tag of rawTags.split('|')) {
        const t = tag.trim().toLowerCase();
        if (t.length > 1) entry.tagWords.add(t);
      }
    }
  }

  for (const [k, v] of subsections) {
    subsections.set(k, {
      category: v.category,
      roots:    [...v.roots].sort().slice(0, 8),
      tagWords: [...v.tagWords].sort().slice(0, 15)
    });
  }
  return subsections;
}

function buildVocabularyBlock(vocab) {
  const lines = [];
  for (const [sub, { category, roots, tagWords }] of [...vocab.entries()].sort()) {
    const keywords = sub.replace(/_/g, ' ').toLowerCase();
    lines.push(`  Folder: ${category}`);
    lines.push(`  Keywords: ${keywords}`);
    if (roots.length)                lines.push(`  Asset name examples: ${roots.join(', ')}`);
    if (tagWords && tagWords.length) lines.push(`  Visual tag examples: ${tagWords.join(', ')}`);
    lines.push('');
  }
  return lines.join('\n');
}

/* ─── Global vocabulary extractor ───────────────────────────────────
   Builds flat sorted arrays of every unique visual_tag word and every
   unique asset-name root token across the ENTIRE CSV (uncapped).
   Used in the Phase 1 prompt as a hard vocabulary constraint — Phase 1
   must only use words from these lists in searchTerms and variantGroup.
   Also used post-Phase-1 to filter out any hallucinated terms.
─────────────────────────────────────────────────────────────────── */
function parseCsvGlobalVocab(csvText) {
  const rows = parseCsvRows(csvText);
  if (rows.length === 0) return { visualTags: [], nameRoots: [], allTerms: new Set() };

  const header  = rows[0].map(h => h.trim().toLowerCase());
  const nameIdx = header.indexOf('asset_name');
  const tagsIdx = header.indexOf('visual_tags');
  if (nameIdx === -1) return { visualTags: [], nameRoots: [], allTerms: new Set() };

  const visualTagSet = new Set();
  const nameRootSet  = new Set();

  for (let i = 1; i < rows.length; i++) {
    const name    = (rows[i][nameIdx]  || '').trim();
    const rawTags = tagsIdx !== -1 ? (rows[i][tagsIdx] || '').trim() : '';

    // Extract individual name tokens (same logic as StageAB tokeniseAssetName)
    if (name) {
      assetNameRoot(name)
        .split(/[\s_\-]+/)
        .map(t => t.trim())
        .filter(t => t.length > 1)
        .forEach(t => nameRootSet.add(t));
    }

    // Extract all pipe-separated visual tags
    if (rawTags) {
      for (const tag of rawTags.split('|')) {
        const t = tag.trim().toLowerCase();
        if (t.length > 1) visualTagSet.add(t);
      }
    }
  }

  const visualTags = [...visualTagSet].sort();
  const nameRoots  = [...nameRootSet].sort();
  const allTerms   = new Set([...visualTags, ...nameRoots]);

  console.log(`[ROSTER-GEN] Global vocab: ${nameRoots.length} name root tokens, ${visualTags.length} visual tags`);
  return { visualTags, nameRoots, allTerms };
}

/* ─── Utilities ──────────────────────────────────────────────────── */
function stripFences(text) {
  let t = text
    .replace(/^```json\s*/i, "").replace(/^```\s*/i, "").replace(/\s*```$/i, "").trim();
  const a = t.indexOf("{"), b = t.lastIndexOf("}");
  if (a > 0 && b > a) t = t.substring(a, b + 1);
  return t.trim();
}




function buildMasterPromptLayoutGuidance(masterPrompt = "") {
  const prompt = String(masterPrompt || "");
  const hasNewStructuredLayout =
    /#\s*1\.\s*SESSION DECISIONS/i.test(prompt) &&
    /#\s*2\.\s*GAME IDENTITY/i.test(prompt) &&
    /#\s*3\.\s*IMPLEMENTATION CONTRACT/i.test(prompt);
  const hasLegacy63Layout = /\b6\.3(\.\d+)?\b/.test(prompt);

  if (hasNewStructuredLayout) {
    return `MASTER PROMPT LAYOUT DETECTED:
- Section 3.x = implementation contract for movement, camera, init, UI, lifecycle, and exact variable constraints.
- Section 4.x = mechanics/rules, object inventory, and VFX requirements.
- Section 5 = runtime registry with exact object/material/particle names and counts.
- Section 6 = author tranche plan. Useful context, but do not infer extra required assets from sequencing text alone.
- Section 7 = validation contract. Use it to confirm must-exist visible systems.
- For asset discovery, Sections 3, 4, 5, and 7 outweigh descriptive fluff.`;
  }

  if (hasLegacy63Layout) {
    return `MASTER PROMPT LAYOUT DETECTED:
- Legacy 6.3-style structure is present.
- Read the actual gameplay, object, VFX, and validation sections directly from that layout.`;
  }

  return `MASTER PROMPT LAYOUT DETECTED:
- No canonical layout markers were found.
- Infer the authoritative sections from the prompt's real headings and use them for asset discovery.`;
}


function detectRoadPipelineSettings(masterPrompt = "", existing = null) {
  const lower = String(masterPrompt || "").toLowerCase();
  const hasAny = (...patterns) => patterns.some(pattern => pattern.test(lower));

  const existingValue = (existing && typeof existing === "object") ? existing : {};

  let gameType = "other";
  if (hasAny(
    /\b(racing|race car|drift|time trial|lap timer|checkpoint racing)\b/,
    /\b(track|circuit|racetrack|raceway|road course)\b/,
    /\b(laps|finish line|starting grid|pit lane)\b/
  )) {
    gameType = "racing";
  } else if (hasAny(
    /\b(side[ -]?scroller|side[ -]?scrolling|side view|side-view|runner)\b/,
    /\b(vehicle|truck|car|bike|buggy|motorcycle|tank)\b/,
    /\b(terrain|ground traversal|hill climb|slope|road strip|terrain strip)\b/
  )) {
    gameType = "sidescroller_terrain";
  } else if (hasAny(
    /\b(platformer|platforming|run and jump|jump between platforms)\b/,
    /\b(ground traversal|terrain|ground piece|platform route|ramp)\b/
  )) {
    gameType = "platformer";
  }

  const roadExclusionFlag = gameType !== "other" || hasAny(
    /\b(road section|track segment|terrain strip|ground piece|pre-built ground|prebuilt ground)\b/,
    /\b(track layout|terrain layout|road pipeline|road\.zip)\b/
  );

  if (existingValue.roadPipelineUserOverride === true) {
    return {
      ...existingValue,
      gameType,
      source: "user_override_preserved_v1"
    };
  }

  return {
    ...existingValue,
    gameType,
    roadExclusionFlag,
    source: "prompt_heuristic_v4"
  };
}

function buildVariantGroupWarnings(objects3d = []) {
  const groups = new Map();
  for (const obj of (objects3d || [])) {
    const group = String(obj?.variantGroup || "").trim().toLowerCase();
    if (!group || group === "unique") continue;
    if (!groups.has(group)) groups.set(group, 0);
    groups.set(group, groups.get(group) + 1);
  }

  return Array.from(groups.entries())
    .filter(([, count]) => count === 1)
    .map(([group]) => group)
    .sort();
}

function buildPhase1Prompt(masterPrompt, vocab, roadPipeline = {}, globalVocab = null) {
  const vocabBlock = buildVocabularyBlock(vocab);

  // Build the canonical vocabulary constraint block from the global tag/name-root sets.
  // This ensures Phase 1 only generates searchTerms and variantGroup values that
  // actually exist in the asset library — preventing hallucinated terms that match nothing.
  const vocabConstraintBlock = globalVocab ? `
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ASSET LIBRARY VOCABULARY — variantGroup ONLY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OBJECT NAME WORDS (for variantGroup):
${globalVocab.nameRoots.join(', ')}

VISUAL / DESCRIPTOR TAGS (for variantGroup qualifier words):
${globalVocab.visualTags.join(', ')}

STEP 1 — variantGroup (MUST use vocabulary words only — no exceptions):
  Before writing anything else, identify 1–3 words from OBJECT NAME WORDS or VISUAL / DESCRIPTOR TAGS above that name what this object physically IS.
  You MUST ONLY use words from the two lists above. No other words are permitted.
  Lead with the object-type noun (e.g. "tank", "tree", "barrel"). Add 1–2 qualifier words from the same lists if needed (e.g. "military tank", "pine tree").
  Do not use gameplay-role words ("enemy", "collectible") — only physical object descriptors from the vocabulary.

STEP 2 — BUILD searchTerms (variantGroup words + free-form additional descriptors):
  searchTerms must include all variantGroup words. Then add 4–8 additional free-form descriptors — color, material, style, subtype, appearance — in natural language. These additional words are NOT restricted to the vocabulary lists. Use whatever words best describe the specific variant needed.

STEP 3 — WRITE visualDescription and gameplayRole (fully free-form):
  Rich natural-language descriptions with no constraint.
` : '';

  const roadClause = roadPipeline?.roadExclusionFlag
    ? `
ROAD-FIRST TERRAIN CLAUSE (Road.zip pipeline is active)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Road.zip is the PRIMARY source of road layout, drivable path geometry, track sections, ramps,
turns, bumps, and any authored road-piece assembly. Cherry3D primitives are ALSO allowed and
expected, but only as COMPLIMENTARY terrain fill around the assembled road layout.

DO NOT include road sections, terrain strips, ground pieces, track segments, roadside filler
planes, cliff filler blocks, embankment shells, or any equivalent structural ground asset in
your objects3d list. Those are handled by the road + primitive terrain pipeline, not by the
asset roster.

When Road.zip is active, the system will:
  - assemble the actual road using Road.zip pieces as the authoritative primary building blocks
  - add Cherry3D primitive terrain beside, under, and around those road pieces to complete the
    remaining gameplay terrain
  - use only hidden .primitives keys 4-14 for that complimentary terrain work
  - never use deprecated model primitive keys 17, 18, 21, 34, or 35
  - keep primitive terrain adjacent to and connected with the placed road pieces rather than
    floating separately or replacing the road itself

The following ARE still sourced as objects3d from the asset library (request these as normal):
  - Props placed ON TOP of the terrain: trees, bushes, rocks, boulders, grass tufts
  - Vegetation: any plant, shrub, foliage scatter prop
  - Structural props that sit in the scene but are not the ground surface itself:
    buildings, ruins, walls, fences, crates, barrels, vehicles, lamps, signs
  - Collectibles and non-character hazards
  - Any decorative or gameplay prop that has distinct shape and is not the terrain shell

In short: Road.zip builds the road. Cherry3D primitives stitch in the surrounding terrain.
The props sitting on that finished terrain shell come from the asset library as normal.
`
    : `
PRIMITIVE TERRAIN CLAUSE (Road.zip pipeline is NOT active for this game)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
All terrain structure — meaning the ground floor, terrain floor tiles, mountain body geometry,
cliff face geometry, hill shapes, sloped ground planes, raised platforms, and any other
structural ground-volume pieces — MUST be built exclusively from Cherry3D system primitives
(cube, square, plane, sphere, cylinder, capsule, cone, torus, torusknot, tetrahedron, icosahedron).
Those primitive-authored terrain pieces must resolve only through the hidden .primitives manifest
keys 4-14; deprecated model primitive keys 17, 18, 21, 34, and 35 must never be requested.
These geometry types are already available in the engine and require no external assets.

DO NOT request terrain floor pieces, ground tiles, terrain strips, mountain meshes, cliff
body OBJs, hill geometry, slope meshes, or any equivalent structural ground asset as objects3d
entries. They will not be sourced from the asset library. The terrain build tranche will
construct them procedurally using the Cherry3D primitives.

The following ARE still sourced as objects3d from the asset library (request these as normal):
  - Props placed ON TOP of the terrain: trees, bushes, rocks, boulders, grass tufts
  - Vegetation: any plant, shrub, foliage scatter prop
  - Structural props that sit in the scene but are not the ground surface itself:
    buildings, ruins, walls, fences, crates, barrels, vehicles, lamps, signs
  - Collectibles and non-character hazards
  - Any decorative or gameplay prop that has distinct shape and is not the terrain floor

In short: the terrain SHELL (floor, slopes, volumes) is primitives-only.
The props SITTING ON the terrain shell come from the asset library as normal.
`;

  return `You are a game visual requirements analyst. Your ONLY job in this phase is to study the game description and produce a structured list of every particle effect, prop/scene 3D object, and avatar/character requirement the game needs.

DO NOT reference any asset files, filenames, or packs. You have not seen them yet.
DO NOT include surface textures or road/terrain sections (these are handled by separate pipelines).
Character-role requirements MUST be emitted only in avatarRequirements, never in objects3d.
AvatarRequirements are for any visible controllable or animated character role such as player avatar, enemy, NPC, boss, companion, crowd human, creature, or animal performer.
Objects3d are only for non-character props, scenery, hazards, pickups, vehicles used as props, and environment pieces.
When the prompt contains a structured contract layout, prefer extracting requirements from the implementation contract, mechanics/object inventory, registry, and validation sections rather than from tranche sequencing text.
For visible gameplay objects, prefer authored non-primitive scene objects when the game calls for rich silhouettes or recognizable props; primitives should only be implied when the game truly wants primitive-authored visuals, particle internals, or invisible collision geometry.
Be specific and visual in your descriptions — describe what each thing looks like, its size relative to the scene, its motion characteristics, and the gameplay moment it appears in.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
UI EXCLUSION RULE — HARD PROHIBITION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
The following are NEVER 3D objects. Do not include any of them in objects3d under any circumstances, regardless of how the game prompt describes them:

BANNED — character roles in objects3d (these belong in avatarRequirements):
  - Player avatar, hero, driver, rider, pilot, gunner
  - Enemies, NPCs, bosses, companions, pets, crowd performers
  - Any visible animated character, creature, humanoid, animal, or monster that acts like an actor rather than a prop


BANNED — 2D interface elements (all handled by file 23 HTML pipeline):
  - Health bars, HP bars, life bars, stamina bars
  - Power bars, energy meters, fuel gauges, charge indicators
  - Ammo counters, bullet displays, reload indicators
  - Score displays, point counters, combo meters
  - Timer displays, countdown clocks
  - Minimap, radar, compass overlays
  - Pause menus, settings screens, main menus
  - Start screens, game over screens, victory screens
  - Button graphics, icon overlays, cursor graphics
  - Stage clear panels, reward modals, shop interfaces
  - Tutorial overlays, control hint displays
  - Any flat panel, quad, or plane used purely as a 2D display surface
  - Any object whose primary function is to show text, numbers, or 2D art to the player

If the master prompt describes any of the above as visual elements, treat them as UI pipeline items and exclude them silently. Do not mention them in your output.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ONE OBJECT PER ENTRY — HARD RULE (overrides all other rules)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Every entry in objects3d MUST represent exactly ONE individually distinct asset. This rule is absolute and overrides the Environmental Variety Mandate below.

WHEN THE GAME DEFINES A FIXED SET OF INDIVIDUALLY DISTINCT OBJECTS, emit one entry per object — no exceptions:

EXAMPLES OF CORRECT BEHAVIOUR:
  Game requires 15 billiard balls → emit 15 separate objects3d entries:
    ball_1_solid_yellow, ball_2_solid_blue, ball_3_solid_red, ... ball_9_striped_yellow, ...
  Game requires a full deck of playing cards → emit one entry per card face needed.
  Game requires chess pieces → emit one entry per distinct piece type (king, queen, rook, bishop, knight, pawn — one entry each, not "chess pieces").
  Game requires 4 coloured gems → emit 4 entries: gem_red, gem_blue, gem_green, gem_yellow.

EXAMPLES OF WRONG BEHAVIOUR (FORBIDDEN):
  ✗ Grouping "solid_balls" as one entry covering balls 1-7
  ✗ Grouping "striped_balls" as one entry covering balls 9-15
  ✗ Emitting "playing_cards" as a single entry when the game uses distinct card visuals
  ✗ Any entry whose name, visualDescription, or gameplayRole implies it covers multiple distinct objects

HOW TO DETECT WHEN THIS RULE APPLIES:
  - The game prompt explicitly names or enumerates specific individual objects (e.g. "ball 1 through 15", "4 coloured gems", "red key, blue key, green key")
  - The game requires objects that are individually tracked, scored, or interacted with as separate entities
  - The game prompt uses numbered or colour-coded variations of a core object type where each variation is a distinct gameplay entity
  - The game mechanic would break if two visually different objects were treated as the same asset (e.g. a billiards game cannot function with only "solid" and "striped" — it needs each ball individually)

When this rule applies, each object gets its own entry with a unique name (include number, colour, or other distinguishing attribute in the name field), its own visualDescription describing that specific variant, and its own searchTerms targeting that specific variant's appearance.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ENVIRONMENTAL VARIETY MANDATE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
A game environment built from one tree, one rock, and one bush is visually flat and unconvincing. You MUST request multiple variants of each prop class that the game environment requires.

Apply these minimum counts whenever the game environment calls for that prop class:

VEGETATION:
  Trees → minimum 3 variants.
  Bushes / shrubs → minimum 2 variants.
  Ground cover / grass tufts → minimum 2 variants if used as scatter props.

ROCKS / GEOLOGICAL:
  Rocks / boulders → minimum 3 variants.
  Cliff faces / rock formations → minimum 2 variants if the game has cliff scenery.

STRUCTURES / ARCHITECTURE:
  Buildings → minimum 2 variants.
  Walls / fences / barriers → minimum 2 variants.
  Ruins / debris pieces → minimum 3 variants.

VEHICLES (background / destructible):
  Any vehicle class used as scenery or obstacle → minimum 2 variants.

CONTAINERS / CRATES:
  Any crate, barrel, or container class → minimum 2 variants.

STREET / ENVIRONMENT FURNITURE:
  Lamps, signs, benches, bins, etc. → minimum 2 variants per furniture class.

ENEMIES / CHARACTERS:
  Each distinct enemy type counts as one variant — do not artificially inflate enemy counts.

EXCEPTION: Unique hero/player characters, boss objects, and named single gameplay props are exempt from minimum counts.
EXCEPTION: If the game's visual style is deliberately minimalist, reduce minimum counts to 2 per class instead of 3.

${roadClause}
${buildMasterPromptLayoutGuidance(masterPrompt)}

MASTER GAME PROMPT:
${masterPrompt}

ASSET LIBRARY VOCABULARY:
The sections below show every folder in the 3D asset library. Each entry has:
  "Keywords"            — human-readable folder name words
  "Asset name examples" — filename root words that exist in the library
  "Visual tag examples" — semantically enriched synonyms derived from thumbnail images (most valuable signal)

Use the asset library vocabulary above for variantGroup and searchTerms. Descriptions are free-form after those are established.

${vocabConstraintBlock}
${vocabBlock}

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "gameInterpretationSummary": "2-3 sentence description of the game type, environment, visual style, and key visual requirements.",
  "particleEffects": [
    {
      "name": "short_snake_case_identifier",
      "visualDescription": "What this effect looks like visually — shape, density, spread, color tone, scale",
      "behaviorDescription": "How it moves and behaves — duration, velocity, burst vs continuous",
      "triggerMoment": "When in gameplay this effect fires"
    }
  ],
  "objects3d": [
    {
      "name": "short_snake_case_identifier — MUST be unique per individually distinct asset. For numbered/coloured sets (billiard balls, gems, keys, cards) include the number or colour in the name so each entry is unambiguous (e.g. ball_1_solid_yellow, ball_9_striped_yellow). ONE entry per distinct asset — NEVER group multiple distinct objects into one entry.",
      "variantGroup": "VOCABULARY-CONSTRAINED: 1–3 words from the vocabulary lists naming what this object IS — e.g. 'tank', 'military tank', 'pine tree', 'stone arch'. For individually distinct sets use the shared type noun (e.g. 'ball', 'gem', 'key') so all members of the set share the same variantGroup.",
      "searchTerms": ["variantGroup_word_first", "then_free-form_color", "free-form_material", "free-form_style", "free-form_subtype"],
      "visualDescription": "Free-form: what THIS SPECIFIC ASSET looks like — include its unique distinguishing attributes (colour, number, pattern, stripe vs solid, etc.). Do not describe a group.",
      "gameplayRole": "Free-form: what this specific asset does in the game — obstacle, collectible, environment piece, hazard, etc."
    }
  ],
  "avatarRequirements": [
    {
      "name": "short_snake_case_identifier",
      "visualDescription": "Silhouette, species/type, outfit/armor, approximate scale",
      "gameplayRole": "player_avatar | enemy | npc | boss | companion | crowd",
      "characterType": "humanoid | creature | vehicle | robot | animal | other",
      "gameplayFunction": "What this character does mechanically — attacks, collects, guards, etc.",
      "animationNeeds": ["idle", "walk", "attack_or_action"],
      "importance": "required | optional",
      "selectionPriority": 1,
      "textureStyle": "Brief material/texture style note"
    }
  ]
}`;
}

/* ═══════════════════════════════════════════════════════════════════ */
exports.handler = async (event) => {
  let projectPath = null;
  let bucket      = null;
  let jobId       = null;

  const err400 = msg => ({ statusCode: 400, body: msg });
  const err500 = msg => ({ statusCode: 500, body: msg });

  try {
    if (!event.body) return { statusCode: 400, body: "" };

    const body = JSON.parse(event.body);
    jobId = body.jobId;
    projectPath = body.projectPath;
    if (!projectPath || !jobId) return { statusCode: 400, body: "" };

    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) throw new Error("Missing ANTHROPIC_API_KEY");

    bucket = admin.storage().bucket(
      process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app"
    );

    console.log(`[ROSTER-GEN] Starting for project ${projectPath}, job ${jobId}`);

    // ── 1. Load global CSV and extract live category list ────────────────
    console.log(`[ROSTER-GEN] Loading global asset CSV from ${GLOBAL_ASSET_CSV_PATH}`);
    const csvFile = bucket.file(GLOBAL_ASSET_CSV_PATH);
    const [csvExists] = await csvFile.exists();
    if (!csvExists) throw new Error(`Global asset CSV not found at ${GLOBAL_ASSET_CSV_PATH}`);
    const [csvBuffer] = await csvFile.download();
    const csvText = csvBuffer.toString('utf8');
    const vocab       = parseCsvVocabulary(csvText);
    const globalVocab = parseCsvGlobalVocab(csvText);
    console.log(`[ROSTER-GEN] CSV loaded: ${vocab.size} subsection folders indexed`);

    // ── 2. Load Master Prompt + inline images from ai_request.json ──────
    const requestFile = bucket.file(`${projectPath}/ai_request.json`);
    const [reqExists] = await requestFile.exists();
    if (!reqExists) return err400("ai_request.json not found. Submit prompt first.");
    const [reqContent] = await requestFile.download();
    const { prompt: masterPrompt, inlineImages = [], roadPipeline: requestRoadPipeline = null, avatarPipeline = null } = JSON.parse(reqContent.toString());
    if (!masterPrompt) return err400("No prompt found in ai_request.json");

    // ── 3. Build reference image blocks ─────────────────────────────────
    const refImageBlocks = [];
    for (const img of inlineImages) {
      if (img.data && img.mimeType && img.mimeType.startsWith("image/")) {
        refImageBlocks.push({
          type:   "image",
          source: { type: "base64", media_type: img.mimeType, data: img.data }
        });
      }
    }
    if (refImageBlocks.length > 0) {
      console.log(`[ROSTER-GEN] Loaded ${refImageBlocks.length} reference image(s)`);
    }

    // ── 4. Phase 1 — Game Visual Needs Analysis ──────────────────────────
    console.log("[ROSTER-GEN] Phase 1: analyzing game visual requirements...");

    const imagePreamble = refImageBlocks.length > 0
      ? `\nREFERENCE IMAGES: ${refImageBlocks.length} gameplay reference image(s) are attached. ` +
        `They carry authority equal to the Master Prompt. Use them to infer visual style, ` +
        `environment type, entity types, color palette, and particle FX requirements.\n\n`
      : "";

    const roadPipeline = detectRoadPipelineSettings(masterPrompt, requestRoadPipeline);
    const phase1Result = await callClaude(apiKey, {
      model:       "claude-sonnet-4-20250514",
      maxTokens:   16000,
      system:      "You are a game visual requirements analyst. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
      userContent: [
        { type: "text", text: imagePreamble + buildPhase1Prompt(masterPrompt, vocab, roadPipeline, globalVocab) },
        ...refImageBlocks
      ]
    });

    let phase1;
    try {
      phase1 = JSON.parse(stripFences(phase1Result.text));
    } catch (e) {
      const tokenInfo = phase1Result.usage
        ? `(used ${phase1Result.usage.output_tokens} output tokens — limit was 16000)`
        : "(token usage unavailable)";
      const likelyTruncated = (phase1Result.usage?.output_tokens ?? 0) >= 15900;
      console.error(
        `[ROSTER-GEN] Phase 1 JSON parse failed ${tokenInfo}` +
        (likelyTruncated ? " — response appears TRUNCATED, increase maxTokens further." : " — malformed JSON from model.")
      );
      console.error("[ROSTER-GEN] Response head:", phase1Result.text.slice(0, 300));
      console.error("[ROSTER-GEN] Response tail:", phase1Result.text.slice(-300));
      // Write error sentinel so the frontend poller surfaces the failure immediately
      // instead of spinning until its own timeout. Must happen before returning.
      if (bucket && projectPath) {
        try {
          await bucket.file(`${projectPath}/ai_asset_roster_error.json`).save(
            JSON.stringify({ error: `Phase 1 returned unparseable JSON: ${e.message}`, failedAt: Date.now(), stage: "phase1", jobId: jobId || null }),
            { contentType: "application/json", resumable: false }
          );
        } catch (writeErr) { /* non-fatal — best effort */ }
      }
      return err500(`Phase 1 returned unparseable JSON: ${e.message}`);
    }

    phase1.avatarRequirements = Array.isArray(phase1.avatarRequirements) ? phase1.avatarRequirements : [];

    // Normalise variantGroup and searchTerms. variantGroup vocabulary compliance is
    // enforced by the prompt — no post-hoc correction or validation needed here.
    for (const obj of (phase1.objects3d || [])) {

      // Normalise variantGroup to lowercase, trim whitespace/punctuation
      let vg = String(obj.variantGroup || '')
        .toLowerCase().trim().replace(/[^a-z0-9\s]/g, '').replace(/\s+/g, ' ').trim();
      if (!vg) {
        const nameParts = String(obj.name || '').toLowerCase().split(/[_\s]+/).filter(t => t.length > 1);
        vg = nameParts[0] || 'unique';
        console.warn(`[ROSTER-GEN] Object "${obj.name}" had no variantGroup — inferred "${vg}" from name`);
      }
      obj.variantGroup = vg;

      // Normalise searchTerms — deduplicate and strip non-alphanumeric characters only
      if (!Array.isArray(obj.searchTerms) || obj.searchTerms.length === 0) {
        console.warn(`[ROSTER-GEN] Object "${obj.name}" returned no searchTerms — CSV search recall will be low`);
        obj.searchTerms = [];
      } else {
        obj.searchTerms = [...new Set(
          obj.searchTerms
            .map(t => String(t || '').toLowerCase().trim().replace(/[^a-z0-9]/g, ''))
            .filter(t => t.length > 1)
        )];
      }
    }

    console.log(
      `[ROSTER-GEN] Phase 1 complete: ` +
      `${(phase1.particleEffects || []).length} particle effect(s), ` +
      `${(phase1.objects3d || []).length} 3D object(s), ${(phase1.avatarRequirements || []).length} avatar requirement(s) identified`
    );
    for (const obj of (phase1.objects3d || [])) {
      console.log(`[ROSTER-GEN]   "${obj.name}" → searchTerms: [${(obj.searchTerms || []).join(', ')}]`);
    }

    // ── 5. Write Phase 1 result + category list to Firebase ─────────────
    const phase1Payload = {
      phase1,
      roadPipeline,
      avatarPipeline: {
        zipPath: avatarPipeline?.zipPath || AVATARS_ZIP_PATH_DEFAULT
      },
      variantGroupWarnings: buildVariantGroupWarnings(phase1.objects3d || []),
      jobId,
      generatedAt:          Date.now(),
      masterPromptSnippet:  masterPrompt.slice(0, 120)
    };

    await bucket.file(`${projectPath}/ai_asset_roster_phase1.json`).save(
      JSON.stringify(phase1Payload, null, 2),
      { contentType: "application/json", resumable: false }
    );

    console.log(
      `[ROSTER-GEN] Phase 1 written to Firebase. ` +
      `Objects: ${(phase1.objects3d || []).length}, ` +
      `Avatars: ${(phase1.avatarRequirements || []).length}, ` +
      `Particles: ${(phase1.particleEffects || []).length}. ` +
      `Waiting for user reference images.`
    );

    return { statusCode: 202, body: "" };

  } catch (error) {
    console.error("[ROSTER-GEN] Unhandled error:", error);
    if (bucket && projectPath) {
      try {
        await bucket.file(`${projectPath}/ai_asset_roster_error.json`).save(
          JSON.stringify({ error: error.message, failedAt: Date.now(), stage: "phase1", jobId: jobId || null }),
          { contentType: "application/json", resumable: false }
        );
      } catch (e) { /* non-fatal */ }
    }
    return { statusCode: 202, body: "" };
  }
};
