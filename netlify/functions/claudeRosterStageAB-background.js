/* netlify/functions/claudeRosterStageAB-background.js */
/* ═══════════════════════════════════════════════════════════════════
   ASSET ROSTER — VISUAL MATCHING — v7.0
   ─────────────────────────────────────────────────────────────────
   Background Netlify function (suffix -background = 15-min timeout).
   Returns 202 immediately. Writes result to Firebase when done.
   Frontend polls ai_asset_roster_pending.json to detect completion.

   Key change from v6: collapsed two-stage (A/B) vision pass into a
   single accurate vision call per requirement. The CSV quality gate
   is now the only filter — no raw top-N fallback.
   ─────────────────────────────────────────────────────────────────
   Flow:
     1. Read Phase 1 result from ai_asset_roster_phase1.json.
     2. Read user reference images from ai_roster_ref_images.json.
     3. Read global CSV from game-generator-1/projects/BASE_Files/asset_3d_objects/
        reorganized_assets_manifest.csv → build assetName→category map.
     4. CSV text search with quality gate (rawScore ≥ MIN_QUALITY_SCORE).
        If quality gate returns 0 results the requirement is unmatched —
        no fallback to raw top-N.
     5. Single vision call per requirement: user reference image vs
        all CSV-filtered thumbnails. Model picks the single best match
        directly. No batching intermediate pass, no second selection call.
        Particles: text description vs thumbnails (unchanged, single pass).
     6. Assemble final roster, enforce limits, save pending.json.

   Global asset paths (shared across all projects):
     CSV:  game-generator-1/projects/BASE_Files/asset_3d_objects/reorganized_assets_manifest.csv
     Zips: game-generator-1/projects/BASE_Files/asset_3d_objects/{asset_name}.zip

   Request body: { projectPath, jobId }
   Response:     202 Accepted (background function — no body)
   ═══════════════════════════════════════════════════════════════════ */

const fetch  = require("node-fetch");
const admin  = require("./firebaseAdmin");
const JSZip  = require("jszip");

/* ─── Constants ──────────────────────────────────────────────────── */
const GLOBAL_ASSET_BASE    = "game-generator-1/projects/BASE_Files/asset_3d_objects";
const GLOBAL_ASSET_CSV     = `${GLOBAL_ASSET_BASE}/reorganized_assets_manifest.csv`;

const CLAUDE_MAX_RETRIES   = 5;
const CLAUDE_BASE_DELAY_MS = 1250;
const CLAUDE_MAX_DELAY_MS  = 12000;

const MAX_OBJ_ASSETS       = 50;
const MAX_PNG_ASSETS       = 50;
const IMAGES_PER_BATCH     = 20;
const BATCH_SIZE           = 17;   // candidates per Round-1 batch vision call
const MAX_FINALISTS        = 3;    // one winner per batch → at most 3 enter Round 2 (ceil(50/17))
const MAX_AVATAR_ASSETS   = 20;
const AVATARS_ZIP_PRIMARY_PATH = `${GLOBAL_ASSET_BASE}/Avatars.zip`;
const AVATARS_ZIP_LEGACY_PATH  = "game-generator-1/projects/BASE_Files/avatar_assets/Avatars.zip";

function buildAvatarZipPathCandidates(requestedPath = "") {
  return [...new Set([
    requestedPath,
    AVATARS_ZIP_PRIMARY_PATH,
    AVATARS_ZIP_LEGACY_PATH
  ].filter(Boolean))];
}

/* ─── Retry helpers ──────────────────────────────────────────────── */
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function computeRetryDelay(attempt) {
  return Math.min(
    CLAUDE_BASE_DELAY_MS * Math.pow(2, Math.max(0, attempt - 1)),
    CLAUDE_MAX_DELAY_MS
  ) + Math.floor(Math.random() * 700);
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
  const body = {
    model,
    max_tokens: maxTokens,
    system,
    messages: [{ role: "user", content: userContent }]
  };
  let last;
  for (let i = 1; i <= CLAUDE_MAX_RETRIES; i++) {
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
      if (i >= CLAUDE_MAX_RETRIES) throw err;
      await sleep(computeRetryDelay(i));
    }
  }
  throw last;
}

/* ─── CSV parsing ────────────────────────────────────────────────── */
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

/* ─── CSV search engine ─────────────────────────────────────────────
   Builds a flat array of tokenised asset records from the CSV.
   StageAB scores every record against Phase 1 searchTerms + variantGroup
   to produce a ranked candidate pool for vision matching.

   Two-stage scoring:
     1. Raw per-term max (scoreAssetVsTerms) — gates quality threshold.
        Each term contributes only its single highest match score to prevent
        double-counting across tag/name/subsection categories.
     2. IDF-weighted (scoreAssetVsTermsWeighted) — used for final ranking.
        Secondary terms are multiplied by log(N/df): rare terms like
        "helicopter" score higher than common terms like "yellow".
        Primary terms (variantGroup: "tank", "helicopter") use a fixed
        PRIMARY_BOOST multiplier instead of IDF — the object type should
        never be penalised for being well-represented in the library.

   Scoring per asset per term (raw, before multiplier):
     +6  exact match: searchTerm === visual_tag
     +4  exact match: searchTerm === asset name token
     +2  exact match: searchTerm === subsection token
     +1  substring:   searchTerm contained in (or contains) name or tag token
   Top MAX_CSV_CANDIDATES qualifying assets are passed to vision matching.
─────────────────────────────────────────────────────────────────── */

function tokeniseAssetName(name) {
  // Strip trailing variant suffixes (_1_A, _2_B, _AA) then split on _ and -
  return String(name || '')
    .replace(/[_\-]\d+[_\-]?[A-Za-z]*$/, '')
    .replace(/[_\-][A-Z]{1,2}$/, '')
    // Split camelCase before lowercasing: "FloorLight" → "Floor Light"
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
    .toLowerCase()
    .split(/[\s_\-]+/)
    .map(t => t.trim())
    .filter(t => t.length > 1);
}

function tokeniseSubsection(sub) {
  return String(sub || '').toLowerCase().split(/_+/).map(t => t.trim()).filter(t => t.length > 1);
}

function buildCsvSearchIndex(csvText) {
  const rows = parseCsvRows(csvText);
  if (rows.length === 0) throw new Error('CSV is empty');

  const header   = rows[0].map(h => h.trim().toLowerCase());
  const nameIdx  = header.indexOf('asset_name');
  const catIdx   = header.indexOf('new_category');
  const subIdx   = header.indexOf('subsection_title');
  const tagsIdx  = header.indexOf('visual_tags');  // enriched column — may not exist yet
  if (nameIdx === -1 || catIdx === -1) throw new Error("CSV missing required columns");

  const hasVisualTags = tagsIdx !== -1;
  if (!hasVisualTags) {
    console.warn('[ROSTER-AB] CSV has no "visual_tags" column — run enrichment to improve search accuracy. Scoring will use name tokens only.');
  } else {
    const enrichedCount = rows.slice(1).filter(r => (r[tagsIdx] || '').trim()).length;
    console.log(`[ROSTER-AB] visual_tags column present: ${enrichedCount}/${rows.length - 1} assets enriched`);
  }

  const records = [];
  for (let i = 1; i < rows.length; i++) {
    const assetName = (rows[i][nameIdx]  || '').trim();
    const category  = (rows[i][catIdx]   || '').trim();
    const sub       = subIdx  !== -1 ? (rows[i][subIdx]  || '').trim() : '';
    const rawTags   = tagsIdx !== -1 ? (rows[i][tagsIdx] || '').trim() : '';
    if (!assetName || !category) continue;

    // Parse pipe-separated visual tags into a token array
    const visualTags = rawTags
      ? rawTags.split('|').map(t => t.trim().toLowerCase()).filter(t => t.length > 1)
      : [];

    records.push({
      assetName,
      category,
      nameTokens: tokeniseAssetName(assetName),
      subTokens:  tokeniseSubsection(sub),
      visualTags  // empty array if not yet enriched — scoring degrades gracefully
    });
  }
  console.log(`[ROSTER-AB] CSV search index built: ${records.length} asset records`);
  return records;
}

/* ─── Scoring constants ──────────────────────────────────────────── */

// Hard cap on candidates forwarded to vision matching.
const MAX_CSV_CANDIDATES = 50;

// Raw score threshold for quality filtering (pre-IDF).
// +4 = at least one exact asset-name token match.
// Using raw score keeps the quality bar predictable regardless of IDF values.
// No fallback — if 0 assets pass this gate the requirement is unmatched.
const MIN_QUALITY_SCORE = 4;

// Fixed score multiplier applied to variantGroup (primary object-type) terms.
// These bypass IDF discounting: "tank" is common in the library precisely because
// there are many tank variants — that should not penalise the search.
const PRIMARY_BOOST = 5;

/* ─── Raw per-term scorer (used for quality threshold) ───────────── */
// Each term contributes only its SINGLE highest-scoring hit across all categories
// (per-term max, fixes Issue 5 double-counting). No IDF — raw signal strength only.
// This is the gating function: if rawScore < MIN_QUALITY_SCORE the asset is excluded
// regardless of how IDF weighting ranks it.
function scoreAssetVsTerms(record, terms) {
  let score = 0;
  for (const term of terms) {
    const t = String(term || '').toLowerCase().trim();
    if (!t) continue;
    let termBest = 0;
    for (const vt of (record.visualTags || [])) {
      if (vt === t)                         { termBest = Math.max(termBest, 6); break; }
      if (vt.includes(t) || t.includes(vt)) { termBest = Math.max(termBest, 1); break; }
    }
    for (const nt of record.nameTokens) {
      if (nt === t)                         { termBest = Math.max(termBest, 4); break; }
      if (nt.includes(t) || t.includes(nt)) { termBest = Math.max(termBest, 1); break; }
    }
    for (const st of record.subTokens) {
      if (st === t) { termBest = Math.max(termBest, 2); break; }
    }
    score += termBest;
  }
  return score;
}

/* ─── IDF computation ────────────────────────────────────────────── */
// For each search term, compute log(N / document_frequency) across the full index.
// Rare terms (e.g. "helicopter" — few assets) get high IDF; common terms
// (e.g. "yellow" — hundreds of assets) get low IDF. O(T × N) — fast at N=1532.
function computeTermIdf(terms, csvIndex) {
  const N = csvIndex.length;
  const result = {};
  for (const term of terms) {
    const t = String(term || '').toLowerCase().trim();
    if (!t) continue;
    let df = 0;
    for (const rec of csvIndex) {
      let hit = false;
      for (const vt of (rec.visualTags || [])) {
        if (vt === t || vt.includes(t) || t.includes(vt)) { hit = true; break; }
      }
      if (!hit) for (const nt of rec.nameTokens) {
        if (nt === t || nt.includes(t) || t.includes(nt)) { hit = true; break; }
      }
      if (!hit) for (const st of rec.subTokens) {
        if (st === t) { hit = true; break; }
      }
      if (hit) df++;
    }
    result[t] = df > 0 ? Math.log(N / df) : Math.log(N + 1);
  }
  return result;
}

/* ─── variantGroup tokeniser ─────────────────────────────────────── */
// Splits the Phase 1 variantGroup string ("tank", "military vehicle") into
// individual lowercase tokens used as primary boosted terms in weighted scoring.
function tokeniseVariantGroup(variantGroup = '') {
  return String(variantGroup || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, '')
    .split(/\s+/)
    .map(t => t.trim())
    .filter(t => t.length > 1);
}

/* ─── IDF-weighted scorer (used for ranking) ─────────────────────── */
// Same per-term max logic as scoreAssetVsTerms, but each term's contribution
// is multiplied by its IDF weight — EXCEPT primary terms (variantGroup), which
// use a fixed PRIMARY_BOOST multiplier instead of IDF. This prevents "tank"
// from being discounted just because the library has many tank assets.
function scoreAssetVsTermsWeighted(record, terms, termIdf, primaryTerms) {
  const primarySet = new Set(primaryTerms.map(t => String(t || '').toLowerCase().trim()));
  let score = 0;
  for (const term of terms) {
    const t = String(term || '').toLowerCase().trim();
    if (!t) continue;
    let termBest = 0;
    for (const vt of (record.visualTags || [])) {
      if (vt === t)                         { termBest = Math.max(termBest, 6); break; }
      if (vt.includes(t) || t.includes(vt)) { termBest = Math.max(termBest, 1); break; }
    }
    for (const nt of record.nameTokens) {
      if (nt === t)                         { termBest = Math.max(termBest, 4); break; }
      if (nt.includes(t) || t.includes(nt)) { termBest = Math.max(termBest, 1); break; }
    }
    for (const st of record.subTokens) {
      if (st === t) { termBest = Math.max(termBest, 2); break; }
    }
    // Primary (variantGroup) terms: fixed boost — object type must never be IDF-penalised
    // Secondary terms: IDF-weighted — common descriptors like "yellow" are discounted
    const multiplier = primarySet.has(t) ? PRIMARY_BOOST : (termIdf[t] ?? 1);
    score += termBest * multiplier;
  }
  return score;
}

/* ─── Main candidate finder ──────────────────────────────────────── */
function findCsvCandidates(req, csvIndex) {
  const secondaryTerms = Array.isArray(req.searchTerms) ? req.searchTerms.filter(Boolean) : [];
  const primaryTerms   = tokeniseVariantGroup(req.variantGroup || '');

  // Merge primary + secondary, deduplicated. Primary terms are searched too —
  // they just receive a boosted multiplier instead of IDF discounting.
  const allTerms = [...new Set([...primaryTerms, ...secondaryTerms])];

  if (allTerms.length === 0) {
    console.warn(`[ROSTER-AB] Req "${req.name}": no searchTerms or variantGroup — CSV search skipped`);
    return [];
  }

  // Compute IDF weights for all terms (primary terms will use PRIMARY_BOOST instead,
  // but we still compute IDF for logging transparency).
  const termIdf = computeTermIdf(allTerms, csvIndex);

  const scored = csvIndex.map(r => ({
    record:   r,
    // IDF-weighted + primary-boosted score — used for ranking
    score:    scoreAssetVsTermsWeighted(r, allTerms, termIdf, primaryTerms),
    // Raw per-term max score — used for quality gating only
    rawScore: scoreAssetVsTerms(r, allTerms)
  })).filter(s => s.score > 0).sort((a, b) => b.score - a.score);

  // Quality gate: raw score ≥ MIN_QUALITY_SCORE ensures at least one exact hit
  // before IDF amplification can surface a noisy match.
  // No fallback — zero qualifying results means the requirement is unmatched.
  const top = scored
    .filter(s => s.rawScore >= MIN_QUALITY_SCORE)
    .slice(0, MAX_CSV_CANDIDATES);

  console.log(
    `[ROSTER-AB] CSV search "${req.name}" ` +
    `[primary(×${PRIMARY_BOOST}): ${primaryTerms.join('+') || 'none'}] ` +
    `[secondary: ${secondaryTerms.join(', ')}]: ` +
    `${scored.length} hits → quality gate → using ${top.length}` +
    (top[0] ? ` | best: "${top[0].record.assetName}" (weighted ${top[0].score.toFixed(2)})` : ' | no qualifying assets')
  );
  return top.map(s => s.record.assetName.toLowerCase());
}



/* ─── Utilities ──────────────────────────────────────────────────── */
function stripFences(text) {
  let t = text
    .replace(/^```json\s*/i, "").replace(/^```\s*/i, "").replace(/\s*```$/i, "").trim();
  const a = t.indexOf("{"), b = t.lastIndexOf("}");
  if (a > 0 && b > a) t = t.substring(a, b + 1);
  return t.trim();
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

/* ─── Bounded concurrency pool ───────────────────────────────────────
   Runs async tasks with at most `limit` in-flight at a time.
   Preserves result order (same semantics as Promise.all).
   Prevents simultaneous Claude API calls from overwhelming rate limits
   when there are many requirements to match.
─────────────────────────────────────────────────────────────────── */
async function pooledAll(tasks, limit = 5) {
  const results = new Array(tasks.length);
  let nextIdx = 0;

  async function worker() {
    while (nextIdx < tasks.length) {
      const idx = nextIdx++;
      results[idx] = await tasks[idx]();
    }
  }

  const workers = Array.from({ length: Math.min(limit, tasks.length) }, () => worker());
  await Promise.all(workers);
  return results;
}


function normalizeAvatarRole(value = "") {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "") || "avatar";
}

function parseAnimationsTxt(raw = "") {
  return Array.from(new Set(
    String(raw || "")
      .split(/\r?\n/)
      .map(line => line.trim())
      .filter(Boolean)
      .map(line => {
        const match = line.match(/^[-*]?\s*([^:]+?)(?:\s*:\s*(.*))?$/);
        return String(match?.[1] || line).trim();
      })
      .filter(Boolean)
  ));
}

function normalizeAnimationNeedToBuckets(need = '') {
  const lower = String(need || '').toLowerCase();
  if (!lower) return [];
  if (/idle|stand|breath/.test(lower)) return ['idle'];
  if (/move|walk|step/.test(lower)) return ['walk'];
  if (/run|sprint|jog/.test(lower)) return ['run'];
  if (/jump|hop|leap/.test(lower)) return ['jump'];
  if (/attack_or_action|attack|melee|strike|slash|swing|punch|kick/.test(lower)) return ['attack_melee', 'attack_ranged'];
  if (/shoot|fire|aim|ranged/.test(lower)) return ['attack_ranged'];
  if (/hurt|hit|damage|flinch|pain/.test(lower)) return ['hurt'];
  if (/death|die|dead|dying/.test(lower)) return ['death'];
  if (/reload/.test(lower)) return ['reload'];
  if (/crouch|duck/.test(lower)) return ['crouch'];
  if (/strafe|sidestep/.test(lower)) return ['strafe'];
  if (/celebrate|victory|cheer|taunt/.test(lower)) return ['celebrate'];
  if (/fall|airborne/.test(lower)) return ['fall'];
  if (/land|touchdown/.test(lower)) return ['land'];
  return [lower.replace(/[^a-z0-9]+/g, '_')];
}

function scoreAnimationCoverage(requirement = {}, clips = []) {
  const needs = Array.isArray(requirement.animationNeeds) ? requirement.animationNeeds : [];
  const BUCKET_PATTERNS = {
    idle: /idle|stand|breathing/i,
    walk: /walk|step/i,
    run: /run|sprint|jog/i,
    jump: /jump|leap|hop/i,
    attack_melee: /attack|slash|strike|swing|melee|punch|kick/i,
    attack_ranged: /shoot|fire|aim|ranged/i,
    hurt: /hurt|hit|damage|flinch|pain/i,
    death: /death|die|dying|dead/i,
    reload: /reload/i,
    crouch: /crouch|duck/i,
    strafe: /strafe|sidestep/i,
    celebrate: /celebrate|victory|cheer|taunt/i,
    fall: /fall|falling|airborne/i,
    land: /land|touchdown/i,
  };
  const normalizedBuckets = {};
  for (const clip of clips) {
    for (const [bucket, pattern] of Object.entries(BUCKET_PATTERNS)) {
      if (pattern.test(String(clip || ''))) {
        normalizedBuckets[bucket] = normalizedBuckets[bucket] || [];
        normalizedBuckets[bucket].push(clip);
      }
    }
  }
  if (needs.length === 0) {
    return {
      required: [],
      matched: [],
      missing: [],
      score: clips.length > 0 ? 1 : 0,
      coveragePercent: clips.length > 0 ? 100 : 0,
      normalizedBuckets
    };
  }
  const matched = needs.filter(need => {
    const buckets = normalizeAnimationNeedToBuckets(need);
    return buckets.some(bucket => Array.isArray(normalizedBuckets[bucket]) && normalizedBuckets[bucket].length > 0)
      || clips.some(clip => {
        const lowerClip = String(clip || '').toLowerCase();
        const lowerNeed = String(need || '').toLowerCase();
        return lowerClip.includes(lowerNeed) || lowerNeed.includes(lowerClip);
      });
  });
  return {
    required: needs,
    matched,
    missing: needs.filter(need => !matched.includes(need)),
    score: matched.length / Math.max(1, needs.length),
    coveragePercent: Math.round((matched.length / Math.max(1, needs.length)) * 100),
    normalizedBuckets
  };
}

function splitMatchTokens(value = '') {
  return String(value || '')
    .replace(/\u0000/g, ' ')
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter(Boolean)
    .filter(token => !/^(mat|material|mesh|slot|default|obj|fbx|glb|gltf|mesh[0-9]+|[a-z]|[0-9]+)$/.test(token));
}

function scoreOrderedTokenAlignment(sharedTokens = [], leftTokens = [], rightTokens = []) {
  let score = 0;
  let rightCursor = -1;
  for (const token of sharedTokens) {
    const leftIndex = leftTokens.indexOf(token);
    const rightIndex = rightTokens.indexOf(token, rightCursor + 1);
    if (leftIndex >= 0 && rightIndex >= 0) {
      score += 5;
      rightCursor = rightIndex;
    }
  }
  return score;
}

function scoreTextureCandidates(materials = [], textureFileList = []) {
  const textureEntries = (Array.isArray(textureFileList) ? textureFileList : []).map((entryPath) => {
    const base = String(entryPath || '').split('/').pop() || String(entryPath || '');
    const lower = base.toLowerCase();
    const nameNoExt = base.replace(/\.[^.]+$/, '');
    return { entryPath, base, lower, nameNoExt, tokens: splitMatchTokens(nameNoExt) };
  });

  const contracts = (Array.isArray(materials) ? materials : []).map((material, index) => {
    const materialName = String(material?.name || `slot_${index}`);
    const materialLower = materialName.toLowerCase();
    const materialNameNoExt = materialName.replace(/\.[^.]+$/, '').toLowerCase();
    const materialTokens = splitMatchTokens(materialName);
    const ranked = textureEntries.map((texture) => {
      let score = 0;
      const reasons = [];
      const sharedTokens = materialTokens.filter(token => texture.tokens.includes(token));

      if (texture.nameNoExt.toLowerCase() === materialNameNoExt) {
        score += 100;
        reasons.push('exact full-name match');
      }
      if (sharedTokens.length > 0) {
        score += sharedTokens.length * 10;
        reasons.push(`shared tokens: ${sharedTokens.join(', ')}`);
      }
      const orderedScore = scoreOrderedTokenAlignment(sharedTokens, materialTokens, texture.tokens);
      if (orderedScore > 0) {
        score += orderedScore;
        reasons.push('ordered token alignment');
      }
      if (materialNameNoExt && (texture.nameNoExt.toLowerCase().includes(materialNameNoExt) || materialNameNoExt.includes(texture.nameNoExt.toLowerCase()))) {
        score += 8;
        reasons.push('substring containment');
      }
      if (/(diffuse|albedo|color|col|basecolor)/.test(texture.lower)) {
        score += 15;
        reasons.push('albedo/color suffix match');
      }
      if (/(thumbnail|preview|render|thumb)/.test(texture.lower)) {
        score -= 30;
        reasons.push('preview/thumbnail penalty');
      }
      if (sharedTokens.some(token => token.length <= 1 || /^[0-9]+$/.test(token))) {
        score -= 5;
        reasons.push('generic token penalty');
      }
      return {
        entryPath: texture.entryPath,
        base: texture.base,
        score,
        reason: reasons.join('; ') || 'no strong token evidence'
      };
    }).sort((a, b) => b.score - a.score || a.base.localeCompare(b.base));

    const best = ranked[0] || null;
    const second = ranked[1] || null;
    const scoreGap = best ? (best.score - (second?.score || 0)) : 0;
    let confidence = 'unresolved';
    let boundTexture = null;
    let ambiguous = false;
    if (best) {
      if (best.score >= 40) {
        confidence = 'high';
        boundTexture = best.entryPath;
      } else if (best.score >= 15 && scoreGap >= 10) {
        confidence = 'medium';
        boundTexture = best.entryPath;
      } else if (best.score < 15) {
        confidence = 'low';
      } else {
        ambiguous = true;
      }
    }
    return {
      materialName,
      slot: Number(material?.index ?? index),
      boundTexture,
      confidence,
      score: best?.score || 0,
      reason: best?.reason || 'no candidate textures available',
      secondBest: second?.entryPath || null,
      ambiguous
    };
  });

  const confidentMatches = contracts.filter(contract => contract.boundTexture && (contract.confidence === 'high' || contract.confidence === 'medium'));
  if (confidentMatches.length === 0 && contracts.length > 0) {
    const globalColormap = textureEntries.find(texture => /^colormap$/i.test(texture.nameNoExt));
    if (globalColormap) {
      return contracts.map(contract => ({
        ...contract,
        boundTexture: globalColormap.entryPath,
        confidence: 'medium',
        score: Math.max(contract.score, 15),
        reason: 'global colormap fallback because zero confident slot matches existed',
        ambiguous: false
      }));
    }
  }

  return contracts.map(contract => {
    if (contract.confidence === 'low' || contract.ambiguous) {
      return { ...contract, boundTexture: null, confidence: contract.confidence === 'low' ? 'low' : 'unresolved' };
    }
    return contract;
  });
}

let _threeFbxRuntimePromise = null;

async function getThreeFbxRuntime() {
  if (!_threeFbxRuntimePromise) {
    _threeFbxRuntimePromise = (async () => {
      const THREE = await import('three');
      const { FBXLoader } = await import('three/examples/jsm/loaders/FBXLoader.js');
      return { THREE, FBXLoader };
    })();
  }
  return _threeFbxRuntimePromise;
}

function nodeBufferToArrayBuffer(bufferLike) {
  if (bufferLike instanceof ArrayBuffer) return bufferLike;
  if (!Buffer.isBuffer(bufferLike)) {
    throw new Error('scanFbxBuffer expected a Node Buffer or ArrayBuffer');
  }
  return bufferLike.buffer.slice(bufferLike.byteOffset, bufferLike.byteOffset + bufferLike.byteLength);
}

function collectThreeMaterialNamesForAvatar(root) {
  const names = [];
  const seen = new Set();
  root?.traverse?.((node) => {
    const mats = Array.isArray(node?.material) ? node.material : (node?.material ? [node.material] : []);
    for (const mat of mats) {
      const label = String(mat?.name || mat?.type || 'UnnamedMaterial').trim() || 'UnnamedMaterial';
      if (seen.has(label)) continue;
      seen.add(label);
      names.push(label);
    }
  });
  return names;
}

function estimateMaterialSlotCountForAvatar(root) {
  let slotCount = 0;
  let sawMesh = false;
  root?.traverse?.((node) => {
    if (!node?.isMesh) return;
    sawMesh = true;
    const mats = Array.isArray(node.material) ? node.material : (node.material ? [node.material] : []);
    slotCount += Math.max(1, mats.length);
  });
  if (slotCount > 0) return slotCount;
  const uniqueMaterials = collectThreeMaterialNamesForAvatar(root).length;
  if (uniqueMaterials > 0) return uniqueMaterials;
  return sawMesh ? 1 : 0;
}

function detectDominantAxisForAvatar(size = {}) {
  const dims = [
    { axis: 'x', value: Math.abs(Number(size.x || 0)) },
    { axis: 'y', value: Math.abs(Number(size.y || 0)) },
    { axis: 'z', value: Math.abs(Number(size.z || 0)) }
  ].sort((a, b) => b.value - a.value);
  const dominantAxis = dims[0]?.axis || 'z';
  return {
    dominantAxis,
    forwardHint: dominantAxis === 'x' ? 'x' : 'z'
  };
}

function buildThreeFbxGeometryAnalysis(sceneOrRoot, sourceName = '', THREE = null) {
  if (!sceneOrRoot) return null;
  if (!THREE?.Box3) throw new Error('THREE.Box3 runtime unavailable for FBX analysis');

  sceneOrRoot.updateMatrixWorld?.(true);
  sceneOrRoot.traverse?.((node) => {
    if (node?.isMesh && typeof node.geometry?.computeBoundingBox === 'function') {
      node.geometry.computeBoundingBox();
    }
  });

  const box = new THREE.Box3().setFromObject(sceneOrRoot);
  const finite = [box.min?.x, box.min?.y, box.min?.z, box.max?.x, box.max?.y, box.max?.z].every(Number.isFinite);
  const min = finite ? {
    x: Number(box.min.x || 0),
    y: Number(box.min.y || 0),
    z: Number(box.min.z || 0)
  } : { x: 0, y: 0, z: 0 };
  const max = finite ? {
    x: Number(box.max.x || 0),
    y: Number(box.max.y || 0),
    z: Number(box.max.z || 0)
  } : { x: 0, y: 0, z: 0 };
  const size = {
    x: Number((max.x - min.x).toFixed(6)),
    y: Number((max.y - min.y).toFixed(6)),
    z: Number((max.z - min.z).toFixed(6))
  };
  const centroid = {
    x: Number((((min.x + max.x) / 2) || 0).toFixed(6)),
    y: Number((((min.y + max.y) / 2) || 0).toFixed(6)),
    z: Number((((min.z + max.z) / 2) || 0).toFixed(6))
  };

  let meshCount = 0;
  let vertexCount = 0;
  sceneOrRoot.traverse?.((node) => {
    if (!node?.isMesh) return;
    meshCount += 1;
    const posAttr = node.geometry?.attributes?.position;
    if (posAttr?.count) vertexCount += posAttr.count;
  });

  const slotCount = estimateMaterialSlotCountForAvatar(sceneOrRoot);
  const materialNames = collectThreeMaterialNamesForAvatar(sceneOrRoot);
  const maxDim = Math.max(Math.abs(size.x), Math.abs(size.y), Math.abs(size.z), 0);
  const normalizedToOneUnit = maxDim > 0 ? Number((1 / maxDim).toFixed(6)) : 1;
  const dominantAxisInfo = detectDominantAxisForAvatar(size);
  const floorY = Number((-min.y).toFixed(6));
  const ceilingY = Number((-max.y).toFixed(6));
  const centerY = Number((-centroid.y).toFixed(6));
  const centerOffsetX = Number((-centroid.x).toFixed(6));
  const centerOffsetZ = Number((-centroid.z).toFixed(6));

  return {
    sourceName,
    format: 'fbx',
    meshCount,
    slotCount,
    vertexCount,
    animationCount: Array.isArray(sceneOrRoot.animations) ? sceneOrRoot.animations.length : 0,
    materials: materialNames,
    boundingBox: {
      width: Number(size.x.toFixed(3)),
      height: Number(size.y.toFixed(3)),
      depth: Number(size.z.toFixed(3))
    },
    center: {
      x: Number(centroid.x.toFixed(3)),
      y: Number(centroid.y.toFixed(3)),
      z: Number(centroid.z.toFixed(3))
    },
    recommendedFloorYOffset: Number(floorY.toFixed(3)),
    geometry: {
      min: {
        x: Number(min.x.toFixed(6)),
        y: Number(min.y.toFixed(6)),
        z: Number(min.z.toFixed(6))
      },
      max: {
        x: Number(max.x.toFixed(6)),
        y: Number(max.y.toFixed(6)),
        z: Number(max.z.toFixed(6))
      },
      size,
      centroid
    },
    scale: {
      authoredUnit: 'unknown',
      unitToGameUnit: 1,
      normalizedToOneUnit,
      suggestedGameScale: normalizedToOneUnit,
      suggestedGameScaleVec: [normalizedToOneUnit, normalizedToOneUnit, normalizedToOneUnit],
      scaleWarning: (normalizedToOneUnit > 5 || normalizedToOneUnit < 0.1) ? 'LARGE SCALE CORRECTION NEEDED' : null
    },
    origin: {
      classification: 'unknown',
      biasY: floorY,
      biasX: centerOffsetX,
      biasZ: centerOffsetZ
    },
    placement: {
      floorY,
      ceilingY,
      centerY,
      centerOffsetX,
      centerOffsetZ,
      dominantAxis: dominantAxisInfo.dominantAxis,
      forwardHint: dominantAxisInfo.forwardHint
    },
    bounds: {
      width: Number(size.x.toFixed(3)),
      height: Number(size.y.toFixed(3)),
      depth: Number(size.z.toFixed(3))
    },
    floorOffset: Number(floorY.toFixed(3)),
    scaleHints: {
      normalizedToOneUnit,
      suggestedGameScale: normalizedToOneUnit,
      suggestedGameScaleVec: [normalizedToOneUnit, normalizedToOneUnit, normalizedToOneUnit]
    },
    multiMeshStructure: {
      isMultiMesh: meshCount > 1 || slotCount > 1,
      meshCount,
      slotCount,
      materialSlots: materialNames.map((materialName, index) => ({ slotIndex: index, materialName }))
    }
  };
}

async function scanFbxBuffer(fbxBuffer, sourceName = '') {
  const { THREE, FBXLoader } = await getThreeFbxRuntime();
  const loader = new FBXLoader();
  const arrayBuffer = nodeBufferToArrayBuffer(fbxBuffer);
  const parsed = loader.parse(arrayBuffer, '');
  if (!parsed) throw new Error(`FBXLoader.parse returned no scene for ${sourceName || 'buffer'}`);

  const geometry = buildThreeFbxGeometryAnalysis(parsed, sourceName, THREE);
  const materials = (geometry?.materials || []).map((name, index) => ({ name, index }));
  return {
    geometry,
    materials,
    meshCount: Number(geometry?.meshCount || 0),
    slotCount: Number(geometry?.slotCount || 0)
  };
}

function listAvatarTextureFiles(zip, folderPrefix) {
  const textures = [];
  for (const entryPath of Object.keys(zip.files)) {
    const entry = zip.files[entryPath];
    if (entry.dir || !entryPath.startsWith(folderPrefix)) continue;
    const base = entryPath.split('/').pop() || '';
    const lower = base.toLowerCase();
    if (base.startsWith('._') || entryPath.includes('__MACOSX')) continue;
    if ([".png",".jpg",".jpeg",".webp",".bmp",".tga"].some(ext => lower.endsWith(ext)) && !/thumbnail\./i.test(base)) {
      textures.push(entryPath);
    }
  }
  return textures.sort();
}

function selectAvatarColormapTexture(textureEntryPaths = [], fbxEntryPath = '') {
  const textures = Array.isArray(textureEntryPaths)
    ? textureEntryPaths.filter(Boolean)
    : [];
  if (textures.length === 0) {
    return {
      colormapEntryPath: null,
      colormapFile: null,
      colormapConfidence: 'NONE',
      colormapDetectionRule: 'none'
    };
  }

  const fbxBase = String(fbxEntryPath || '').split('/').pop().replace(/\.[^.]+$/, '').toLowerCase();
  const strongMatchers = [
    /(^|[_\-.\s])colou?rmap([_\-.\s]|$)/i,
    /(^|[_\-.\s])basecolor([_\-.\s]|$)/i,
    /(^|[_\-.\s])base_color([_\-.\s]|$)/i,
    /(^|[_\-.\s])albedo([_\-.\s]|$)/i,
    /(^|[_\-.\s])diffuse([_\-.\s]|$)/i,
    /(^|[_\-.\s])color([_\-.\s]|$)/i
  ];

  const scored = textures.map((entryPath, index) => {
    const fileName = String(entryPath).split('/').pop() || '';
    const lower = fileName.toLowerCase();
    let score = 0;
    let rule = 'avatar-texture';

    if (strongMatchers.some(re => re.test(lower))) {
      score += 100;
      rule = 'avatar-colormap-keyword';
    }
    if (fbxBase && lower.startsWith(`${fbxBase}.`)) {
      score += 40;
      rule = rule === 'avatar-colormap-keyword' ? 'avatar-colormap-keyword+fbx-basename' : 'avatar-fbx-basename';
    }
    if (fbxBase && lower.includes(fbxBase)) {
      score += 20;
      if (rule === 'avatar-texture') rule = 'avatar-fbx-basename-partial';
    }
    if (textures.length === 1) {
      score += 10;
      if (rule === 'avatar-texture') rule = 'single-avatar-texture';
    }

    return { entryPath, fileName, score, rule, index };
  }).sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    return a.index - b.index;
  });

  const best = scored[0] || null;
  if (!best || best.score <= 0) {
    return {
      colormapEntryPath: null,
      colormapFile: null,
      colormapConfidence: 'NONE',
      colormapDetectionRule: 'none'
    };
  }

  const confidence = best.score >= 100 ? 'HIGH'
    : best.score >= 40 ? 'MEDIUM'
    : 'LOW';

  return {
    colormapEntryPath: best.entryPath,
    colormapFile: best.fileName,
    colormapConfidence: confidence,
    colormapDetectionRule: best.rule
  };
}

/* ─── Enforce hard selection limits ─────────────────────────────── */
function enforceHardLimits(roster) {
  if (!roster) return roster;
  if (Array.isArray(roster.objects3d) && roster.objects3d.length > MAX_OBJ_ASSETS) {
    console.warn(`[ROSTER-AB] Trimming objects3d from ${roster.objects3d.length} to ${MAX_OBJ_ASSETS}`);
    roster.objects3d = roster.objects3d.slice(0, MAX_OBJ_ASSETS);
  }
  if (Array.isArray(roster.avatars) && roster.avatars.length > MAX_AVATAR_ASSETS) {
    console.warn(`[ROSTER-AB] Trimming avatars from ${roster.avatars.length} to ${MAX_AVATAR_ASSETS}`);
    roster.avatars = roster.avatars.slice(0, MAX_AVATAR_ASSETS);
  }
  if (Array.isArray(roster.textureAssets) && roster.textureAssets.length > MAX_PNG_ASSETS) {
    console.warn(`[ROSTER-AB] Trimming textureAssets from ${roster.textureAssets.length} to ${MAX_PNG_ASSETS}`);
    roster.textureAssets = roster.textureAssets.slice(0, MAX_PNG_ASSETS);
  }
  if (roster.coverageSummary) {
    roster.coverageSummary.totalObjects3d  = (roster.objects3d    || []).length;
    roster.coverageSummary.totalAvatars    = (roster.avatars      || []).length;
    roster.coverageSummary.totalTextures   = (roster.textureAssets || []).length;
    roster.coverageSummary.limitsRespected =
      roster.coverageSummary.totalObjects3d <= MAX_OBJ_ASSETS &&
      roster.coverageSummary.totalAvatars   <= MAX_AVATAR_ASSETS &&
      roster.coverageSummary.totalTextures  <= MAX_PNG_ASSETS;
  }
  return roster;
}

/* ─── Single-pass prompt: particle text-vs-image best pick ──────── */
function buildParticlePrompt(requirements) {
  const reqList = requirements.map((r, i) =>
    `  ${i + 1}. ${r.name}: ${r.visualDescription}` +
    (r.behaviorDescription ? ` — ${r.behaviorDescription}` : "")
  ).join("\n");

  return `You are a game asset visual screener. You will be shown a batch of particle effect texture thumbnail images.
Your job is to identify which images are plausible visual candidates for any of the particle effect requirements listed below.
Cast a wide net — include anything that could plausibly match, even loosely, but still respect whether the requirement reads more like a burst / impact / spark versus a trail / smoke / lingering streak.

PARTICLE EFFECT REQUIREMENTS:
${reqList}

The images in this batch are numbered sequentially starting at 1.
For each image, list which requirement numbers (1-based) it could satisfy. Use an empty array if none.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "matches": [
    { "imageIndex": 1, "matchesRequirements": [1, 3] },
    { "imageIndex": 2, "matchesRequirements": [] },
    { "imageIndex": 3, "matchesRequirements": [2] }
  ]
}`;
}

/* ─── Single-pass prompt: particle final pick ────────────────────── */
function buildParticleFinalPrompt(requirementName, requirementDesc, candidates, gameInterpretation) {
  return `GAME CONTEXT:
${gameInterpretation}

You are making the final asset selection for a game. You have been given thumbnail images of candidate particle texture assets. Pick the single best visual match for the requirement below.

REQUIREMENT:
Name: ${requirementName}
Description: ${requirementDesc}
Type: Particle Effect Texture

CANDIDATE THUMBNAILS (images attached in order):
${candidates.map((c, i) => `  Image ${i + 1}: ${c.assetFile} (${c.sourceZip})`).join("\n")}

SELECTION RULES:
- Judge purely by visual appearance vs the requirement description.
- Consider shape silhouette, density, edge softness, color tone, and whether the texture reads more like a burst / impact / spark versus a trail / smoke / lingering streak.
- Pick exactly one winner. State which image number you chose and why.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "requirementName": "${requirementName}",
  "imageNumberChosen": 1,
  "visualSelectionRationale": "What you saw in the thumbnail that matched the requirement"
}`;
}

/* ─── Round-1 batch prompt: pick the best from one batch of 10 ───── */
// Lightweight — no candidateScores needed, just the single best label.
function buildObjectBatchPrompt(requirementName, batchSize, batchNum, totalBatches) {
  return `You are an expert 3D game asset librarian performing a pure visual match.

The image labeled [REF] is the reference — the exact visual target. Do not select it as a candidate answer.
The remaining ${batchSize} images are candidate 3D object thumbnails, each preceded by its label [C1]…[C${batchSize}].
This is batch ${batchNum} of ${totalBatches} in a multi-round elimination.

Compare each candidate image directly against [REF]. Pick the single candidate whose visual appearance most closely matches [REF].
Base your decision entirely on what you see in the images — shape, silhouette, style, structure. Ignore any text.

CRITICAL: Replace "chosenLabel" with the label of your chosen candidate. Do NOT default to C1 — only choose C1 if it is genuinely the best visual match.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "requirementName": "${requirementName}",
  "chosenLabel": "C1",
  "visualSelectionRationale": "Brief note on what you saw in the images that drove your choice"
}`;
}

function buildAvatarBatchPrompt(requirementName, batchSize, batchNum, totalBatches) {
  return `You are an expert game character artist performing a pure visual match.

The image labeled [REF] is the reference — the exact visual target. Do not select it as a candidate answer.
The remaining ${batchSize} images are candidate avatar thumbnails, each preceded by its label [C1]…[C${batchSize}].
This is batch ${batchNum} of ${totalBatches} in a multi-round elimination.

Compare each candidate image directly against [REF]. Pick the single candidate whose visual appearance most closely matches [REF].
Base your decision entirely on what you see in the images — character type, silhouette, costume, art style. Ignore any text.

CRITICAL: Replace "chosenLabel" with the label of your chosen candidate. Do NOT default to C1 — only choose C1 if it is genuinely the best visual match.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "requirementName": "${requirementName}",
  "chosenLabel": "C1",
  "visualSelectionRationale": "Brief note on what you saw in the images that drove your choice"
}`;
}

/* ─── Single-pass prompt: 3D object reference-image vs candidates ── */
// The [REF] labeled image comes first, then [C1]…[CN] candidates.
// No text description is passed — the reference image is the sole ground truth.
function buildObjectSinglePassPrompt(requirementName, candidates) {
  return `You are an expert 3D game asset librarian performing a pure visual match.

The image labeled [REF] is the reference — the exact visual target. Do not select it as a candidate answer.
The remaining images are candidate 3D object thumbnails from the asset library, each preceded by its label [C1], [C2], [C3]…

Compare each candidate image directly against [REF]. Evaluate every candidate before deciding.
Base your decision entirely on what you see in the images — shape, silhouette, structure, surface style. Ignore any text.

For each candidate assess: does the object category match? How closely does the silhouette, structure, and style match [REF]?
Eliminate candidates whose object category clearly differs from [REF]. Among the rest, pick the closest visual match.

CRITICAL: You MUST replace "chosenLabel" with the label of whichever candidate YOU determined to be the best visual match after examining ALL candidates. Do NOT default to C1 — C1 is only correct if it genuinely is your best match after comparison.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "requirementName": "${requirementName}",
  "chosenLabel": "C1",
  "visualSelectionRationale": "Brief note on what you saw in the images that drove your choice",
  "candidateScores": [
    ${candidates.map((_, i) => `{ "label": "C${i + 1}", "confidence": 0 }`).join(',\n    ')}
  ]
}`;
}

/* ─── Single-pass prompt: avatar reference-image vs candidates ───── */
// The [REF] labeled image comes first, then [C1]…[CN] candidates.
// No text description is passed — the reference image is the sole ground truth.
function buildAvatarSinglePassPrompt(requirementName, candidates) {
  return `You are an expert game character artist performing a pure visual match.

The image labeled [REF] is the reference — the exact visual target. Do not select it as a candidate answer.
The remaining images are candidate avatar thumbnails from the asset library, each preceded by its label [C1], [C2], [C3]…

Compare each candidate image directly against [REF]. Evaluate every candidate before deciding.
Base your decision entirely on what you see in the images — character type, silhouette, costume, art style. Ignore any text.

For each candidate assess: does the character type match? How closely does the silhouette, costume, and style match [REF]?
Eliminate candidates whose character type clearly differs from [REF]. Among the rest, pick the closest visual match.

CRITICAL: You MUST replace "chosenLabel" with the label of whichever candidate YOU determined to be the best visual match after examining ALL candidates. Do NOT default to C1 — C1 is only correct if it genuinely is your best match after comparison.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "requirementName": "${requirementName}",
  "chosenLabel": "C1",
  "visualSelectionRationale": "Brief note on what you saw in the images that drove your choice",
  "candidateScores": [
    ${candidates.map((_, i) => `{ "label": "C${i + 1}", "confidence": 0 }`).join(',\n    ')}
  ]
}`;
}

/* ═══════════════════════════════════════════════════════════════════ */
exports.handler = async (event) => {
  let projectPath = null;
  let bucket      = null;
  let jobId       = null;

  const err400 = msg => ({ statusCode: 400, body: msg });

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

    console.log(`[ROSTER-AB] Starting visual matching for project ${projectPath}, job ${jobId}`);

    // ── 1. Load Phase 1 result ───────────────────────────────────────────
    const phase1File = bucket.file(`${projectPath}/ai_asset_roster_phase1.json`);
    const [p1Exists] = await phase1File.exists();
    if (!p1Exists) return err400("ai_asset_roster_phase1.json not found. Run Phase 1 first.");
    const [p1Content] = await phase1File.download();
    const p1Payload   = JSON.parse(p1Content.toString());
    const { phase1 }  = p1Payload;
    if (!phase1) return err400("No phase1 data in ai_asset_roster_phase1.json");

    const particleReqs       = phase1.particleEffects || [];
    const objectReqs         = phase1.objects3d || [];
    const avatarReqs         = phase1.avatarRequirements || [];
    const gameInterpretation = phase1.gameInterpretationSummary || "";
    const requestedAvatarZipPath = p1Payload.avatarPipeline?.zipPath || '';
    const avatarZipPathCandidates = buildAvatarZipPathCandidates(requestedAvatarZipPath);
    if (!avatarZipPathCandidates.length) return err400('Missing avatarPipeline.zipPath in ai_asset_roster_phase1.json');
    let resolvedAvatarZipPath = requestedAvatarZipPath || AVATARS_ZIP_PRIMARY_PATH;

    console.log(`[ROSTER-AB] Phase 1 loaded: ${particleReqs.length} particle req(s), ${objectReqs.length} object req(s), ${avatarReqs.length} avatar req(s)`);

    // ── 2. Load user reference images ───────────────────────────────────
    const refImagesFile = bucket.file(`${projectPath}/ai_roster_ref_images.json`);
    const [refExists]   = await refImagesFile.exists();
    if (!refExists) return err400("ai_roster_ref_images.json not found. Frontend must upload user reference images first.");
    const [refContent]      = await refImagesFile.download();
    const refPayload = JSON.parse(refContent.toString());
    const userRefImages = Array.isArray(refPayload.items)
      ? refPayload.items
      : (Array.isArray(refPayload.objects) ? refPayload.objects.map(item => ({ ...item, requirementType: 'object3d' })) : []);

    const refImageByName = new Map();
    for (const img of userRefImages) {
      if (img.requirementName && img.b64 && img.mimeType) {
        refImageByName.set(img.requirementName.toLowerCase(), img);
      }
    }
    console.log(`[ROSTER-AB] User reference images loaded: ${refImageByName.size} requirement(s) have reference images`);

    // ── 2b. Extract assetSource and included overrides from ref image payload ──
    // Each item in userRefImages may carry:
    //   assetSource: 'props' | 'avatars'  (which library to search)
    //   included:    true | false          (false = skip this requirement entirely)
    const assetSourceByName = new Map();  // requirementName.lower → 'props' | 'avatars'
    const includedByName    = new Map();  // requirementName.lower → boolean
    for (const img of userRefImages) {
      if (!img.requirementName) continue;
      const key = img.requirementName.toLowerCase();
      if (img.assetSource === 'props' || img.assetSource === 'avatars') {
        assetSourceByName.set(key, img.assetSource);
      }
      if (typeof img.included === 'boolean') {
        includedByName.set(key, img.included);
      }
    }

    // Filter out skipped requirements (included === false)
    const skipCount = [...includedByName.values()].filter(v => v === false).length;
    if (skipCount > 0) {
      console.log(`[ROSTER-AB] ${skipCount} requirement(s) marked as skipped by user — removing from search`);
    }
    const isIncluded = (name) => includedByName.get(name.toLowerCase()) !== false;
    // objectReqs / avatarReqs are const — filter produces new arrays used from here on
    const activeObjectReqs = objectReqs.filter(r => isIncluded(r.name));
    const activeAvatarReqs = avatarReqs.filter(r => isIncluded(r.name));
    // Cross-routed: objects the user wants from Avatars.zip, avatars from Props
    const objectsViaAvatarZip = activeObjectReqs.filter(r => assetSourceByName.get(r.name.toLowerCase()) === 'avatars');
    const avatarsViaProps      = activeAvatarReqs.filter(r => assetSourceByName.get(r.name.toLowerCase()) === 'props');
    const objectsViaProps      = activeObjectReqs.filter(r => assetSourceByName.get(r.name.toLowerCase()) !== 'avatars');
    const avatarsViaAvatarZip  = activeAvatarReqs.filter(r => assetSourceByName.get(r.name.toLowerCase()) !== 'props');

    console.log(
      `[ROSTER-AB] Routing: ` +
      `${objectsViaProps.length} object(s) via Props, ` +
      `${objectsViaAvatarZip.length} object(s) via Avatars.zip, ` +
      `${avatarsViaAvatarZip.length} avatar(s) via Avatars.zip, ` +
      `${avatarsViaProps.length} avatar(s) via Props`
    );

    // ── 3. Load global CSV → build text search index ────────────────────
    console.log(`[ROSTER-AB] Loading global asset CSV from ${GLOBAL_ASSET_CSV}`);
    const csvFile = bucket.file(GLOBAL_ASSET_CSV);
    const [csvExists] = await csvFile.exists();
    if (!csvExists) throw new Error(`Global asset CSV not found at ${GLOBAL_ASSET_CSV}`);
    const [csvBuffer] = await csvFile.download();
    const csvIndex = buildCsvSearchIndex(csvBuffer.toString("utf8"));

    // ── 4. Run CSV text search per object requirement (Props path only) ──
    // objectsViaAvatarZip are routed to the avatar library — skip CSV for them.
    const reqCsvCandidateNames = new Map();
    for (const req of objectsViaProps) {
      const names = findCsvCandidates(req, csvIndex);
      reqCsvCandidateNames.set(req.name, new Set(names));
    }
    // avatarsViaProps also need CSV text search
    const avatarViaPropsReqCsvCandidateNames = new Map();
    for (const req of avatarsViaProps) {
      const names = findCsvCandidates(req, csvIndex);
      avatarViaPropsReqCsvCandidateNames.set(req.name, new Set(names));
    }

    // ── 5. Scan particle zip files (project-local, unchanged) ───────────
    const particleAssets = []; // { assetFile, b64, mimeType, sourceZip }
    {
      const particlePrefix = `${projectPath}/asset_particle_textures/`;
      let particleFiles;
      try {
        [particleFiles] = await bucket.getFiles({ prefix: particlePrefix });
      } catch (e) {
        console.warn(`[ROSTER-AB] Could not list particle folder: ${e.message}`);
        particleFiles = [];
      }
      const particleZips = (particleFiles || []).filter(f => f.name.toLowerCase().endsWith(".zip"));
      console.log(`[ROSTER-AB] Particle zips found: ${particleZips.length}`);

      for (const zipFile of particleZips) {
        const sourceZip = zipFile.name.split("/").pop();
        try {
          const [zipBuffer] = await zipFile.download();
          const zip = await JSZip.loadAsync(zipBuffer);
          let added = 0;
          for (const entryPath of Object.keys(zip.files)) {
            if (zip.files[entryPath].dir) continue;
            const base  = entryPath.split("/").pop();
            const lower = base.toLowerCase();
            if (base.startsWith("._")) continue;
            if (![".png", ".jpg", ".jpeg", ".webp"].some(e => lower.endsWith(e))) continue;
            const blob     = await zip.files[entryPath].async("nodebuffer");
            const mimeType = lower.endsWith(".png") ? "image/png" : "image/jpeg";
            particleAssets.push({ assetFile: base, b64: blob.toString("base64"), mimeType, sourceZip });
            added++;
          }
          console.log(`[ROSTER-AB] Particle zip ${sourceZip}: ${added} asset(s) indexed`);
        } catch (e) {
          console.warn(`[ROSTER-AB] Could not process particle zip ${sourceZip}: ${e.message}`);
        }
      }
    }

    // ── 6. Scan global 3D object mega-zips, tagged with CSV category ────
    //
    // Zip structure (derived from CSV new_category column):
    //   {TopLevel}.zip / {SubCategory} / {asset_name} / {asset_name}.obj
    //                                                  / {asset_name}.jpg  ← thumbnail
    //                                                  / colormap.jpg      ← texture (locked here)
    //
    // COLORMAP LOCK: Any file whose name contains "colormap" (case-insensitive) is
    // unconditionally treated as that object's texture and locked into the roster as
    // colormapEntryPath (full zip path) at index time. This is the ONE place in the
    // entire pipeline where the obj↔texture match is established. Extract uses the
    // locked colormapEntryPath directly — no re-discovery, no classification logic.
    //
    // CSV new_category = "Architecture_Modular/Floors_Stairs_Pillars"
    //   → zip file:      Architecture_Modular.zip
    //   → internal path: Floors_Stairs_Pillars/{asset_name}/
    //
    // Top-level zip names are derived dynamically from the CSV — adding a
    // 5th zip requires no code changes, just updating the CSV and uploading.
    //
    // Strategy: load each mega-zip ONCE, index ALL assets inside it tagged
    // with their full CSV category. Vision matching filters the in-memory array
    // per-requirement — no repeat zip downloads per requirement.
    //
    // objectAssets: { objFile, objEntryPath, thumbFile, colormapFile, colormapEntryPath,
    //                 colormapConfidence, b64, mimeType, sourceZip, assetName, category }

    console.log(`[ROSTER-AB] Scanning global 3D object mega-zips from ${GLOBAL_ASSET_BASE}/`);
    const objectAssets = [];
    {
      // Derive unique top-level zip names from CSV search index
      const topLevelZipNames = new Set();
      for (const record of csvIndex) {
        const topLevel = record.category.split("/")[0];
        if (topLevel) topLevelZipNames.add(topLevel);
      }
      console.log(`[ROSTER-AB] Top-level zips derived from CSV: ${[...topLevelZipNames].join(", ")}`);

      for (const zipName of topLevelZipNames) {
        const zipPath = `${GLOBAL_ASSET_BASE}/${zipName}.zip`;
        const zipFile = bucket.file(zipPath);
        const [zipExists] = await zipFile.exists();
        if (!zipExists) {
          console.warn(`[ROSTER-AB] Mega-zip not found: ${zipPath} — skipping`);
          continue;
        }

        console.log(`[ROSTER-AB] Loading mega-zip: ${zipName}.zip`);
        let zip;
        try {
          const [zipBuffer] = await zipFile.download();
          zip = await JSZip.loadAsync(zipBuffer);
        } catch (e) {
          console.warn(`[ROSTER-AB] Could not load ${zipName}.zip: ${e.message} — skipping`);
          continue;
        }

        // Group zip entries by "SubCategory/asset_name" folder key.
        // Internal path: {SubCategory}/{asset_name}/{filename}
        // Map< "SubCategory/asset_name" → { subCategory, assetFolder, objEntry, thumbEntry, colormapEntry } >
        const assetFolderMap = new Map();

        // Detect whether the zip has a redundant root folder matching the zip name.
        // e.g. Architecture_Modular.zip may contain:
        //   Architecture_Modular/Modular_Blocks_Panels/fountain-center/file  ← extra level
        // OR the expected:
        //   Modular_Blocks_Panels/fountain-center/file                        ← direct
        // We detect this by checking if parts[0] matches zipName (case-insensitive).
        // If so, we shift the index offset by 1.
        const zipNameLower = zipName.toLowerCase();
        let depthOffset = 0;
        for (const entryPath of Object.keys(zip.files)) {
          if (zip.files[entryPath].dir) continue;
          const p = entryPath.split("/");
          if (p.length >= 1 && p[0].toLowerCase() === zipNameLower) {
            depthOffset = 1;
          }
          break; // only need to check first file
        }
        if (depthOffset > 0) {
          console.log(`[ROSTER-AB] Mega-zip ${zipName}.zip has redundant root folder — adjusting depth offset`);
        }

        for (const entryPath of Object.keys(zip.files)) {
          if (zip.files[entryPath].dir) continue;
          const parts = entryPath.split("/");
          if (parts.length < 3 + depthOffset) continue; // need SubCategory/asset_name/file
          const subCategory = parts[0 + depthOffset];
          const assetFolder = parts[1 + depthOffset];
          const fileName    = parts[parts.length - 1];
          const fileLower   = fileName.toLowerCase();
          if (fileName.startsWith("._")) continue;

          const folderKey = `${subCategory}/${assetFolder}`;
          if (!assetFolderMap.has(folderKey)) {
            assetFolderMap.set(folderKey, { subCategory, assetFolder, objEntry: null, thumbEntry: null, colormapEntry: null });
          }
          const entry = assetFolderMap.get(folderKey);

          if (fileLower.endsWith(".obj")) {
            // .obj always wins — overwrite any previously seen .fbx entry for this folder
            entry.objEntry = { entryPath, fileName };
          } else if (fileLower.endsWith(".fbx") && !entry.objEntry) {
            // .fbx accepted only when no .obj has been seen yet for this folder
            entry.objEntry = { entryPath, fileName };
          } else if ([".png", ".jpg", ".jpeg", ".webp"].some(e => fileLower.endsWith(e))) {
            if (fileLower.includes("colormap")) {
              // "colormap" anywhere in the filename = this object's texture. Locked. No other logic applies.
              if (!entry.colormapEntry) {
                entry.colormapEntry = { entryPath, fileName };
              }
            } else if (!entry.thumbEntry) {
              entry.thumbEntry = { entryPath, fileName, fileLower };
            }
          }
        }

        // Build objectAssets from folder map
        let added = 0;
        for (const [folderKey, entry] of assetFolderMap) {
          if (!entry.objEntry) continue;
          if (!entry.thumbEntry) {
            console.warn(`[ROSTER-AB] ${zipName}.zip/${folderKey}: no thumbnail — skipping`);
            continue;
          }

          // Look up category from CSV search index
          const assetNameLower = entry.assetFolder.toLowerCase();
          const csvRecord   = csvIndex.find(r => r.assetName.toLowerCase() === assetNameLower);
          const csvCategory = csvRecord?.category || null;
          if (!csvCategory) {
            console.warn(`[ROSTER-AB] ${zipName}.zip/${folderKey}: "${entry.assetFolder}" not in CSV — skipping`);
            continue;
          }

          try {
            const blob = await zip.files[entry.thumbEntry.entryPath].async("nodebuffer");
            const b64  = blob.toString("base64");
            if (!b64) continue;
            const mimeType = entry.thumbEntry.fileLower.endsWith(".png") ? "image/png" : "image/jpeg";
            if (!entry.colormapEntry) {
              console.warn(`[ROSTER-AB] ${zipName}.zip/${folderKey}: no colormap found — asset will have no texture`);
            }
            objectAssets.push({
              objFile:            entry.objEntry.fileName,
              objEntryPath:       entry.objEntry.entryPath,       // full zip path — locked at index time
              thumbFile:          entry.thumbEntry.fileName,
              colormapFile:       entry.colormapEntry?.fileName  || null,  // locked at index time
              colormapEntryPath:  entry.colormapEntry?.entryPath || null,  // full zip path — locked at index time
              colormapConfidence: entry.colormapEntry            ? "HIGH" : "NONE",
              b64,
              mimeType,
              sourceZip: `${zipName}.zip`,
              assetName: entry.assetFolder,
              category:  csvCategory          // canonical category from CSV
            });
            added++;
          } catch (e) {
            console.warn(`[ROSTER-AB] ${zipName}.zip/${folderKey}: thumbnail read failed — ${e.message}`);
          }
        }

        console.log(`[ROSTER-AB] Mega-zip ${zipName}.zip: ${added} asset(s) indexed`);
      }
    }

    const avatarAssets = [];
    {
      let avatarZip = null;
      for (const candidatePath of avatarZipPathCandidates) {
        const avatarZipFile = bucket.file(candidatePath);
        const [avatarZipExists] = await avatarZipFile.exists();
        if (!avatarZipExists) continue;
        console.log(`[ROSTER-AB] Loading avatar library from ${candidatePath}`);
        try {
          const [avatarZipBuffer] = await avatarZipFile.download();
          avatarZip = await JSZip.loadAsync(avatarZipBuffer);
          resolvedAvatarZipPath = candidatePath;
          break;
        } catch (e) {
          console.warn(`[ROSTER-AB] Could not load avatar library ${candidatePath}: ${e.message}`);
        }
      }

      if (avatarZip) {
        const folderMap = new Map();
        for (const entryPath of Object.keys(avatarZip.files)) {
          const entry = avatarZip.files[entryPath];
          if (entry.dir || entryPath.includes('__MACOSX')) continue;
          const base = entryPath.split('/').pop() || '';
          if (base.startsWith('._')) continue;
          const parts = entryPath.split('/').filter(Boolean);
          if (parts.length < 2) continue;
          const folderKey = parts.slice(0, -1).join('/');
          if (!folderMap.has(folderKey)) {
            folderMap.set(folderKey, { folderKey, folderName: parts[parts.length - 2], files: [] });
          }
          folderMap.get(folderKey).files.push(entryPath);
        }

        for (const folder of folderMap.values()) {
          const fbxEntryPath = folder.files.find(file => /\.fbx$/i.test(file));
          const thumbnailEntryPath = folder.files.find(file => /thumbnail\.(png|jpg|jpeg|webp)$/i.test(file));
          if (!fbxEntryPath || !thumbnailEntryPath) continue;
          try {
            const thumbBuffer = await avatarZip.files[thumbnailEntryPath].async('nodebuffer');
            const thumbLower = thumbnailEntryPath.toLowerCase();
            const mimeType = thumbLower.endsWith('.png') ? 'image/png' : 'image/jpeg';
            const animationManifestPath = folder.files.find(file => /animations\.txt$/i.test(file)) || null;
            const rawAnimations = animationManifestPath
              ? await avatarZip.files[animationManifestPath].async('text')
              : '';
            const animationClips = parseAnimationsTxt(rawAnimations);
            let fbxGeometry = null;
            let fbxMaterials = [];
            let fbxMeshCount = 0;
            let fbxSlotCount = 0;
            try {
              const fbxBuffer = await avatarZip.files[fbxEntryPath].async('nodebuffer');
              const scanResult = await scanFbxBuffer(fbxBuffer, fbxEntryPath);
              if (scanResult) {
                fbxGeometry = scanResult.geometry || null;
                fbxMaterials = scanResult.materials || [];
                fbxMeshCount = scanResult.meshCount || 0;
                fbxSlotCount = scanResult.slotCount || fbxMeshCount;
              }
            } catch (e) {
              console.warn(`[ROSTER-AB] Avatar FBX scan failed for ${fbxEntryPath}: ${e.message}`);
            }
            const textureFiles = listAvatarTextureFiles(avatarZip, `${folder.folderKey}/`);
            const avatarColormap = selectAvatarColormapTexture(textureFiles, fbxEntryPath);
            avatarAssets.push({
              assetName: fbxEntryPath.split('/').pop(),
              fbxEntryPath,
              thumbnailEntryPath,
              thumbnailFile: thumbnailEntryPath.split('/').pop(),
              textureFiles,
              colormapFile: avatarColormap.colormapFile,
              colormapEntryPath: avatarColormap.colormapEntryPath,
              colormapConfidence: avatarColormap.colormapConfidence,
              colormapDetectionRule: avatarColormap.colormapDetectionRule,
              animationManifestPath,
              rawAnimations,
              animationClips,
              geometryAnalysis: fbxGeometry,
              materials: fbxMaterials,
              materialAssignments: fbxMaterials.map((m, i) => ({ slot: i, materialName: m.name })),
              meshCount: fbxMeshCount,
              slotCount: fbxSlotCount,
              b64: thumbBuffer.toString('base64'),
              mimeType,
              sourceZip: resolvedAvatarZipPath.split('/').pop() || 'Avatars.zip',
              avatarFolder: folder.folderName
            });
          } catch (e) {
            console.warn(`[ROSTER-AB] Avatar folder ${folder.folderKey}: thumbnail read failed — ${e.message}`);
          }
        }
      } else {
        console.warn(`[ROSTER-AB] Avatar library not found in any expected path: ${avatarZipPathCandidates.join(', ')}`);
      }
    }

    console.log(`[ROSTER-AB] Asset library ready: ${particleAssets.length} particle textures, ${objectAssets.length} 3D objects, ${avatarAssets.length} avatars`);

    // ── 7. Single-pass visual matching ──────────────────────────────────
    // For each requirement: send reference image + all CSV-filtered candidate
    // thumbnails in ONE call. The model picks the single best match directly.
    // No intermediate batch scan, no second selection call.
    console.log("[ROSTER-AB] Starting single-pass visual matching...");

    // Particles retain a two-step approach because they have no reference image:
    //   pass 1 — batch scan to find plausible candidates (text description vs images)
    //   pass 2 — final pick among candidates (unchanged from prior design)
    const particleCandidates = new Map(particleReqs.filter(r => isIncluded(r.name)).map(r => [r.name, []]));

    async function runParticleBatchScan() {
      if (particleReqs.length === 0 || particleAssets.length === 0) return;
      const batches = chunkArray(particleAssets, IMAGES_PER_BATCH);
      console.log(`[ROSTER-AB] Particle scan: ${particleAssets.length} assets → ${batches.length} batch(es)`);

      for (let b = 0; b < batches.length; b++) {
        const batch       = batches[b];
        const imageBlocks = batch.map(asset => ({
          type:   "image",
          source: { type: "base64", media_type: asset.mimeType, data: asset.b64 }
        }));
        let batchResult;
        try {
          batchResult = await callClaude(apiKey, {
            model:       "claude-sonnet-4-20250514",
            maxTokens:   2000,
            system:      "You are a game asset visual screener. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
            userContent: [
              { type: "text", text: buildParticlePrompt(particleReqs) },
              ...imageBlocks
            ]
          });
        } catch (e) {
          console.warn(`[ROSTER-AB] Particle batch ${b + 1} failed: ${e.message} — skipping`);
          continue;
        }
        let parsed;
        try { parsed = JSON.parse(stripFences(batchResult.text)); }
        catch (e) {
          console.warn(`[ROSTER-AB] Particle batch ${b + 1} parse failed — skipping`);
          continue;
        }
        for (const match of (parsed.matches || [])) {
          const imgIdx = (match.imageIndex || 1) - 1;
          const asset  = batch[imgIdx];
          if (!asset) continue;
          for (const reqIdx of (match.matchesRequirements || [])) {
            const req = particleReqs[reqIdx - 1];
            if (!req) continue;
            const candidates = particleCandidates.get(req.name);
            if (!candidates) continue;
            if (!candidates.some(c => c.assetFile === asset.assetFile)) {
              candidates.push(asset);
            }
          }
        }
      }
    }

    async function runParticleFinalPick(req) {
      const candidates = particleCandidates.get(req.name) || [];
      if (candidates.length === 0) {
        console.warn(`[ROSTER-AB] Particle: no candidates for "${req.name}" — unmatched`);
        return null;
      }
      const imageBlocks = candidates.map(c => ({
        type:   "image",
        source: { type: "base64", media_type: c.mimeType, data: c.b64 }
      }));
      const desc = req.visualDescription + (req.behaviorDescription ? ` — ${req.behaviorDescription}` : "");
      let result;
      try {
        result = await callClaude(apiKey, {
          model:       "claude-sonnet-4-20250514",
          maxTokens:   1000,
          system:      "You are a visual asset selection specialist. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
          userContent: [
            { type: "text", text: buildParticleFinalPrompt(req.name, desc, candidates, gameInterpretation) },
            ...imageBlocks
          ]
        });
      } catch (e) {
        console.warn(`[ROSTER-AB] Particle final pick failed for "${req.name}": ${e.message} — using first candidate`);
        return { requirementName: req.name, selectedAsset: candidates[0], visualSelectionRationale: `Fallback: ${e.message}`, colormapFile: null };
      }
      let parsed;
      try { parsed = JSON.parse(stripFences(result.text)); }
      catch (e) { parsed = { imageNumberChosen: 1, visualSelectionRationale: "Fallback: parse error" }; }
      const chosenIdx = Math.min((parsed.imageNumberChosen || 1) - 1, candidates.length - 1);
      return { requirementName: req.name, selectedAsset: candidates[chosenIdx], visualSelectionRationale: parsed.visualSelectionRationale || "", colormapFile: null };
    }

    // Two-round object matcher:
    //   Round 1 — split assetPool into batches of BATCH_SIZE, pick one winner per batch
    //   Round 2 — final pick among all batch winners (≤ MAX_FINALISTS)
    async function runObjectSinglePass(req) {
      const refImg = refImageByName.get(req.name.toLowerCase());
      if (!refImg) {
        console.warn(`[ROSTER-AB] Object "${req.name}": no reference image — unmatched`);
        return null;
      }

      // Determine asset pool (Props or Avatars.zip cross-route)
      let assetPool;
      let matchKey;
      if (assetSourceByName.get(req.name.toLowerCase()) === 'avatars') {
        assetPool = avatarAssets;
        matchKey  = 'assetName';
      } else {
        const csvNames = reqCsvCandidateNames.get(req.name) || new Set();
        if (csvNames.size === 0) {
          console.warn(`[ROSTER-AB] Object "${req.name}": CSV returned 0 qualifying candidates — unmatched`);
          return null;
        }
        assetPool = objectAssets.filter(a => csvNames.has(a.assetName.toLowerCase()));
        matchKey  = 'objFile';
      }

      if (assetPool.length === 0) {
        console.warn(`[ROSTER-AB] Object "${req.name}": CSV names found but none loaded from zips — unmatched`);
        return null;
      }

      const refBlock = [
        { type: "text",  text: "[REF]" },
        { type: "image", source: { type: "base64", media_type: refImg.mimeType, data: refImg.b64 } }
      ];

      // ── Round 1: batch elimination ────────────────────────────────────
      // Each batch of BATCH_SIZE candidates gets its own focused vision call.
      // The model picks the single best from each batch → one finalist per batch.
      const batches = chunkArray(assetPool, BATCH_SIZE);
      console.log(`[ROSTER-AB] Object "${req.name}": ${assetPool.length} candidate(s) → ${batches.length} batch(es) of ≤${BATCH_SIZE} (Round 1)`);

      const finalists = [];
      for (let b = 0; b < batches.length; b++) {
        const batch = batches[b];
        const labeledBlocks = batch.flatMap((asset, i) => [
          { type: "text",  text: `[C${i + 1}]` },
          { type: "image", source: { type: "base64", media_type: asset.mimeType, data: asset.b64 } }
        ]);
        let batchResult;
        try {
          batchResult = await callClaude(apiKey, {
            model:       "claude-sonnet-4-20250514",
            maxTokens:   500,
            system:      "You are an expert 3D game asset librarian. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
            userContent: [
              { type: "text", text: buildObjectBatchPrompt(req.name, batch.length, b + 1, batches.length) },
              ...refBlock,
              ...labeledBlocks
            ]
          });
        } catch (e) {
          console.warn(`[ROSTER-AB] Object "${req.name}" batch ${b + 1} failed: ${e.message} — skipping`);
          continue;
        }
        let batchParsed;
        try { batchParsed = JSON.parse(stripFences(batchResult.text)); }
        catch (e) {
          console.warn(`[ROSTER-AB] Object "${req.name}" batch ${b + 1} parse failed — skipping`);
          continue;
        }
        const batchLabelNum = parseInt(String(batchParsed.chosenLabel || '').replace(/^C/i, ''), 10);
        if (!Number.isFinite(batchLabelNum) || batchLabelNum < 1 || batchLabelNum > batch.length) {
          console.warn(`[ROSTER-AB] Object "${req.name}" batch ${b + 1}: invalid chosenLabel "${batchParsed.chosenLabel}" — skipping`);
          continue;
        }
        const winner = batch[batchLabelNum - 1];
        finalists.push(winner);
        console.log(`[ROSTER-AB] Object "${req.name}" batch ${b + 1} winner: [C${batchLabelNum}] ${winner[matchKey]}`);
      }

      if (finalists.length === 0) {
        console.warn(`[ROSTER-AB] Object "${req.name}": no batch winners produced — unmatched`);
        return null;
      }

      // ── Round 2: final selection among finalists ──────────────────────
      // At most MAX_FINALISTS candidates (one per batch), full analysis prompt.
      console.log(`[ROSTER-AB] Object "${req.name}": ${finalists.length} finalist(s) → Round 2 final pick`);

      const finalLabeledBlocks = finalists.flatMap((asset, i) => [
        { type: "text",  text: `[C${i + 1}]` },
        { type: "image", source: { type: "base64", media_type: asset.mimeType, data: asset.b64 } }
      ]);

      let finalResult;
      try {
        finalResult = await callClaude(apiKey, {
          model:       "claude-sonnet-4-20250514",
          maxTokens:   1000,
          system:      "You are an expert 3D game asset librarian. Analyse every candidate image carefully and methodically before selecting. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
          userContent: [
            { type: "text", text: buildObjectSinglePassPrompt(req.name, finalists) },
            ...refBlock,
            ...finalLabeledBlocks
          ]
        });
      } catch (e) {
        console.warn(`[ROSTER-AB] Object "${req.name}" Round 2 failed: ${e.message} — using first finalist`);
        const fallback = finalists[0];
        return {
          requirementName:          req.name,
          selectedAsset:            fallback,
          visualSelectionRationale: `Round 2 failed — fallback to first finalist: ${e.message}`,
          debugCandidates:          finalists.map((asset, i) => ({
            label: `C${i + 1}`, assetName: asset.assetName || asset.objFile || '',
            sourceZip: asset.sourceZip || '', confidence: i === 0 ? 100 : 0,
            b64: asset.b64, mimeType: asset.mimeType
          }))
        };
      }

      let finalParsed;
      try { finalParsed = JSON.parse(stripFences(finalResult.text)); }
      catch (e) {
        console.warn(`[ROSTER-AB] Object "${req.name}" Round 2 parse failed — using first finalist`);
        finalParsed = { chosenLabel: "C1", visualSelectionRationale: "Fallback: parse error", candidateScores: [] };
      }

      const finalLabelNum = parseInt(String(finalParsed.chosenLabel || '').replace(/^C/i, ''), 10);
      const safeIdx = (Number.isFinite(finalLabelNum) && finalLabelNum >= 1 && finalLabelNum <= finalists.length)
        ? finalLabelNum - 1 : 0;
      if (safeIdx === 0 && finalLabelNum !== 1) {
        console.warn(`[ROSTER-AB] Object "${req.name}": invalid Round 2 chosenLabel "${finalParsed.chosenLabel}" — defaulting to first finalist`);
      }
      const selectedAsset = finalists[safeIdx];
      console.log(`[ROSTER-AB] Object "${req.name}": final winner [C${safeIdx + 1}] ${selectedAsset[matchKey]}`);

      // Debug candidates show the finalists that entered Round 2
      const finalCandidateScores = Array.isArray(finalParsed.candidateScores) ? finalParsed.candidateScores : [];
      const debugCandidates = finalists.map((asset, i) => {
        const label = `C${i + 1}`;
        const scoreEntry = finalCandidateScores.find(s => s.label === label);
        return {
          label,
          assetName:  asset.assetName || asset.objFile || '',
          sourceZip:  asset.sourceZip || '',
          confidence: scoreEntry ? scoreEntry.confidence : 0,
          b64:        asset.b64,
          mimeType:   asset.mimeType
        };
      });

      return {
        requirementName:          req.name,
        selectedAsset,
        visualSelectionRationale: finalParsed.visualSelectionRationale || "",
        debugCandidates
      };
    }

    // Two-round avatar matcher (same structure as runObjectSinglePass)
    async function runAvatarSinglePass(req) {
      const refImg = refImageByName.get(req.name.toLowerCase());
      if (!refImg) {
        console.warn(`[ROSTER-AB] Avatar "${req.name}": no reference image — unmatched`);
        return null;
      }

      // Determine asset pool (Avatars.zip or Props cross-route)
      let assetPool;
      if (assetSourceByName.get(req.name.toLowerCase()) === 'props') {
        const csvNames = avatarViaPropsReqCsvCandidateNames.get(req.name) || new Set();
        if (csvNames.size === 0) {
          console.warn(`[ROSTER-AB] Avatar "${req.name}" (Props cross-route): CSV returned 0 qualifying candidates — unmatched`);
          return null;
        }
        assetPool = objectAssets.filter(a => csvNames.has(a.assetName.toLowerCase()));
      } else {
        assetPool = avatarAssets;
      }

      if (assetPool.length === 0) {
        console.warn(`[ROSTER-AB] Avatar "${req.name}": no assets in pool — unmatched`);
        return null;
      }

      const refBlock = [
        { type: "text",  text: "[REF]" },
        { type: "image", source: { type: "base64", media_type: refImg.mimeType, data: refImg.b64 } }
      ];

      // ── Round 1: batch elimination ────────────────────────────────────
      const batches = chunkArray(assetPool, BATCH_SIZE);
      console.log(`[ROSTER-AB] Avatar "${req.name}": ${assetPool.length} candidate(s) → ${batches.length} batch(es) of ≤${BATCH_SIZE} (Round 1)`);

      const finalists = [];
      for (let b = 0; b < batches.length; b++) {
        const batch = batches[b];
        const labeledBlocks = batch.flatMap((asset, i) => [
          { type: "text",  text: `[C${i + 1}]` },
          { type: "image", source: { type: "base64", media_type: asset.mimeType, data: asset.b64 } }
        ]);
        let batchResult;
        try {
          batchResult = await callClaude(apiKey, {
            model:       "claude-sonnet-4-20250514",
            maxTokens:   500,
            system:      "You are an expert game character artist. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
            userContent: [
              { type: "text", text: buildAvatarBatchPrompt(req.name, batch.length, b + 1, batches.length) },
              ...refBlock,
              ...labeledBlocks
            ]
          });
        } catch (e) {
          console.warn(`[ROSTER-AB] Avatar "${req.name}" batch ${b + 1} failed: ${e.message} — skipping`);
          continue;
        }
        let batchParsed;
        try { batchParsed = JSON.parse(stripFences(batchResult.text)); }
        catch (e) {
          console.warn(`[ROSTER-AB] Avatar "${req.name}" batch ${b + 1} parse failed — skipping`);
          continue;
        }
        const batchLabelNum = parseInt(String(batchParsed.chosenLabel || '').replace(/^C/i, ''), 10);
        if (!Number.isFinite(batchLabelNum) || batchLabelNum < 1 || batchLabelNum > batch.length) {
          console.warn(`[ROSTER-AB] Avatar "${req.name}" batch ${b + 1}: invalid chosenLabel "${batchParsed.chosenLabel}" — skipping`);
          continue;
        }
        const winner = batch[batchLabelNum - 1];
        finalists.push(winner);
        console.log(`[ROSTER-AB] Avatar "${req.name}" batch ${b + 1} winner: [C${batchLabelNum}] ${winner.assetName}`);
      }

      if (finalists.length === 0) {
        console.warn(`[ROSTER-AB] Avatar "${req.name}": no batch winners produced — unmatched`);
        return null;
      }

      // ── Round 2: final selection among finalists ──────────────────────
      console.log(`[ROSTER-AB] Avatar "${req.name}": ${finalists.length} finalist(s) → Round 2 final pick`);

      const finalLabeledBlocks = finalists.flatMap((asset, i) => [
        { type: "text",  text: `[C${i + 1}]` },
        { type: "image", source: { type: "base64", media_type: asset.mimeType, data: asset.b64 } }
      ]);

      let finalResult;
      try {
        finalResult = await callClaude(apiKey, {
          model:       "claude-sonnet-4-20250514",
          maxTokens:   1000,
          system:      "You are an expert game character artist. Analyse every candidate avatar carefully and methodically before selecting. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
          userContent: [
            { type: "text", text: buildAvatarSinglePassPrompt(req.name, finalists) },
            ...refBlock,
            ...finalLabeledBlocks
          ]
        });
      } catch (e) {
        console.warn(`[ROSTER-AB] Avatar "${req.name}" Round 2 failed: ${e.message} — using first finalist`);
        const fallback = finalists[0];
        return {
          requirementName:          req.name,
          selectedAsset:            fallback,
          visualSelectionRationale: `Round 2 failed — fallback to first finalist: ${e.message}`,
          debugCandidates:          finalists.map((asset, i) => ({
            label: `C${i + 1}`, assetName: asset.assetName || '',
            sourceZip: asset.sourceZip || '', confidence: i === 0 ? 100 : 0,
            b64: asset.b64, mimeType: asset.mimeType
          }))
        };
      }

      let finalParsed;
      try { finalParsed = JSON.parse(stripFences(finalResult.text)); }
      catch (e) {
        console.warn(`[ROSTER-AB] Avatar "${req.name}" Round 2 parse failed — using first finalist`);
        finalParsed = { chosenLabel: "C1", visualSelectionRationale: "Fallback: parse error", candidateScores: [] };
      }

      const finalLabelNum = parseInt(String(finalParsed.chosenLabel || '').replace(/^C/i, ''), 10);
      const safeIdx = (Number.isFinite(finalLabelNum) && finalLabelNum >= 1 && finalLabelNum <= finalists.length)
        ? finalLabelNum - 1 : 0;
      if (safeIdx === 0 && finalLabelNum !== 1) {
        console.warn(`[ROSTER-AB] Avatar "${req.name}": invalid Round 2 chosenLabel "${finalParsed.chosenLabel}" — defaulting to first finalist`);
      }
      const selectedAsset = finalists[safeIdx];
      console.log(`[ROSTER-AB] Avatar "${req.name}": final winner [C${safeIdx + 1}] ${selectedAsset.assetName}`);

      const finalCandidateScores = Array.isArray(finalParsed.candidateScores) ? finalParsed.candidateScores : [];
      const debugCandidates = finalists.map((asset, i) => {
        const label = `C${i + 1}`;
        const scoreEntry = finalCandidateScores.find(s => s.label === label);
        return {
          label,
          assetName:  asset.assetName || '',
          sourceZip:  asset.sourceZip || '',
          confidence: scoreEntry ? scoreEntry.confidence : 0,
          b64:        asset.b64,
          mimeType:   asset.mimeType
        };
      });

      return {
        requirementName:          req.name,
        selectedAsset,
        visualSelectionRationale: finalParsed.visualSelectionRationale || "",
        debugCandidates
      };
    }

    // Run particle scan first (batch pass), then all final picks concurrently
    await runParticleBatchScan();

    // Vision calls run with a concurrency cap of 5 to avoid rate-limit bursts.
    // pooledAll preserves result order identically to Promise.all.
    const VISION_CONCURRENCY = 5;
    const [particleResults, objectResults, avatarResults] = await Promise.all([
      pooledAll(particleReqs.filter(r => isIncluded(r.name)).map(r => () => runParticleFinalPick(r)), VISION_CONCURRENCY),
      pooledAll(activeObjectReqs.map(r => () => runObjectSinglePass(r)), VISION_CONCURRENCY),
      pooledAll(activeAvatarReqs.map(r => () => runAvatarSinglePass(r)), VISION_CONCURRENCY)
    ]);

    console.log(
      `[ROSTER-AB] Visual matching complete: ${particleResults.filter(Boolean).length} particle, ` +
      `${objectResults.filter(Boolean).length} object, ` +
      `${avatarResults.filter(Boolean).length} avatar selections`
    );

    // ── 8. Assemble final roster ─────────────────────────────────────────

    // Mirrors the avatar__/prop prefix logic in claudeRosterExtract-background.js
    // buildCopiedModelFilename(), computed here so every roster entry carries
    // copiedModelFilename from Stage AB onward. This lets the frontend build
    // rosterSelected3DNames from a stable, extension-bearing filename rather than
    // falling back to assetName — which may not match what Extract stages after
    // applying the avatar__ prefix.
    function computeCopiedModelFilename(assetName, sourceZip, fbxEntryPath, animationManifestPath, avatarRole) {
      const name = String(assetName || '').trim();
      const extMatch = name.match(/(\.[^.]+)$/);
      const ext = extMatch ? extMatch[1] : '';
      const baseNoExt = name.replace(/\.[^.]+$/, '');
      const isAvatar = Boolean(
        avatarRole || animationManifestPath || fbxEntryPath ||
        /avatars?\.zip$/i.test(String(sourceZip || ''))
      );
      return isAvatar ? `avatar__${baseNoExt}${ext}` : `${baseNoExt}${ext}`;
    }

    function assembleParticleAsset(stageBResult, phase1Req) {
      if (!stageBResult) return null;
      const asset = stageBResult.selectedAsset;
      return {
        assetName:            asset.assetFile,
        sourceZip:            asset.sourceZip,
        intendedUsage:        `Particle effect: ${stageBResult.requirementName}`,
        particleEffectTarget: stageBResult.requirementName,
        matchedRequirement:   stageBResult.requirementName,
        selectionRationale:   stageBResult.visualSelectionRationale,
        thumbnailB64:         asset.b64,
        thumbnailMime:        asset.mimeType
      };
    }

    function assembleObjectAsset(stageBResult, phase1Req) {
      if (!stageBResult) return null;
      const asset = stageBResult.selectedAsset;
      const p1    = phase1Req || {};

      // Assets matched via the Avatars.zip cross-route carry avatar-shaped fields
      // (fbxEntryPath, assetName, textureFiles, animationManifestPath) instead of
      // the prop-library fields (objFile, objEntryPath, colormapFile, colormapEntryPath).
      // Detect this case and map the fields correctly so the extract script can find
      // and stage the file. Without this, assetName resolves to undefined and every
      // avatar-sourced object entry silently fails with "not_in_zip".
      const isAvatarSourced = Boolean(asset.fbxEntryPath || asset.animationManifestPath ||
        /avatars?\.zip$/i.test(String(asset.sourceZip || '')));

      if (isAvatarSourced) {
        const coverage = scoreAnimationCoverage(p1, asset.animationClips || []);
        const textureBindingContract = scoreTextureCandidates(
          asset.materials || [],
          asset.textureFiles || []
        );
        // Emit a full avatar-shaped roster entry so the extract script treats
        // this object exactly like an avatar: staging FBX + textures + manifest.
        return {
          assetName:              asset.assetName,
          copiedModelFilename:    computeCopiedModelFilename(
            asset.assetName, asset.sourceZip, asset.fbxEntryPath,
            asset.animationManifestPath, null
          ),
          fbxEntryPath:           asset.fbxEntryPath           || null,
          thumbnailEntryPath:     asset.thumbnailEntryPath      || null,
          thumbnailFile:          asset.thumbnailFile           || null,
          textureFiles:           asset.textureFiles            || [],
          colormapFile:           asset.colormapFile            || null,
          colormapEntryPath:      asset.colormapEntryPath       || null,
          colormapConfidence:     asset.colormapConfidence      || 'NONE',
          colormapDetectionRule:  asset.colormapDetectionRule   || 'none',
          textureBindingContract,
          animationManifestPath:  asset.animationManifestPath   || null,
          rawAnimations:          asset.rawAnimations            || '',
          animationClips:         asset.animationClips           || [],
          normalizedAnimations:   coverage.normalizedBuckets     || {},
          animationCoverage:      coverage,
          geometryAnalysis:       asset.geometryAnalysis         || null,
          materials:              asset.materials                || [],
          materialAssignments:    asset.materialAssignments      || [],
          meshCount:              asset.meshCount                || 0,
          slotCount:              asset.slotCount                || 0,
          // avatarRole is intentionally omitted — this is an objects3d entry, not an avatar.
          // The extract script detects avatar-path assets via fbxEntryPath / animationManifestPath
          // and stages them via the avatar code path (FBX + textures + manifest).
          intendedRole:           p1.gameplayRole || p1.visualDescription || stageBResult.requirementName || "",
          matchedRequirement:     stageBResult.requirementName,
          selectionRationale:     stageBResult.visualSelectionRationale,
          sourceZip:              asset.sourceZip,
          thumbnailB64:           asset.b64,
          thumbnailMime:          asset.mimeType
        };
      }

      return {
        assetName:           asset.objFile,
        objEntryPath:        asset.objEntryPath        || null,  // locked zip path from index time
        colormapFile:        asset.colormapFile        || null,  // locked at index time
        colormapEntryPath:   asset.colormapEntryPath   || null,  // locked zip path from index time
        colormapConfidence:  asset.colormapConfidence  || "NONE",
        thumbFile:           asset.thumbFile,
        sourceZip:           asset.sourceZip,
        category:            asset.category            || null,
        intendedRole:        p1.gameplayRole || p1.visualDescription || stageBResult.requirementName || "",
        matchedRequirement:  stageBResult.requirementName,
        selectionRationale:  stageBResult.visualSelectionRationale,
        thumbnailB64:        asset.b64,
        thumbnailMime:       asset.mimeType
      };
    }

    function assembleAvatarAsset(stageBResult, phase1Req) {
      if (!stageBResult) return null;
      const asset = stageBResult.selectedAsset;
      const p1 = phase1Req || {};
      const coverage = scoreAnimationCoverage(p1, asset.animationClips || []);
      const textureBindingContract = scoreTextureCandidates(
        asset.materials || [],
        asset.textureFiles || []
      );
      return {
        assetName: asset.assetName,
        copiedModelFilename: computeCopiedModelFilename(
          asset.assetName, asset.sourceZip, asset.fbxEntryPath,
          asset.animationManifestPath, p1.gameplayRole || stageBResult.requirementName
        ),
        fbxEntryPath: asset.fbxEntryPath || null,
        thumbnailEntryPath: asset.thumbnailEntryPath || null,
        thumbnailFile: asset.thumbnailFile || null,
        textureFiles: asset.textureFiles || [],
        colormapFile: asset.colormapFile || null,
        colormapEntryPath: asset.colormapEntryPath || null,
        colormapConfidence: asset.colormapConfidence || 'NONE',
        colormapDetectionRule: asset.colormapDetectionRule || 'none',
        textureBindingContract,
        animationManifestPath: asset.animationManifestPath || null,
        rawAnimations: asset.rawAnimations || '',
        animationClips: asset.animationClips || [],
        normalizedAnimations: coverage.normalizedBuckets || {},
        animationCoverage: coverage,
        geometryAnalysis: asset.geometryAnalysis || null,
        materials: asset.materials || [],
        materialAssignments: asset.materialAssignments || [],
        meshCount: asset.meshCount || 0,
        slotCount: asset.slotCount || 0,
        avatarRole: normalizeAvatarRole(p1.gameplayRole || stageBResult.requirementName),
        intendedRole: p1.gameplayRole || stageBResult.requirementName || "",
        matchedRequirement: stageBResult.requirementName,
        selectionRationale: stageBResult.visualSelectionRationale,
        textureStyle: p1.textureStyle || "",
        importance: p1.importance || '',
        selectionPriority: p1.selectionPriority || null,
        characterType: p1.characterType || '',
        gameplayFunction: p1.gameplayFunction || '',
        sourceZip: asset.sourceZip,
        thumbnailB64: asset.b64,
        thumbnailMime: asset.mimeType
      };
    }

    const phase1ParticleMap = new Map(particleReqs.map(r => [r.name, r]));
    const phase1ObjectMap   = new Map(objectReqs.map(r   => [r.name, r]));
    const phase1AvatarMap   = new Map(avatarReqs.map(r   => [r.name, r]));

    const textureAssets = particleResults
      .filter(Boolean)
      .map(r => assembleParticleAsset(r, phase1ParticleMap.get(r.requirementName)))
      .filter(Boolean);

    const objects3d = objectResults
      .filter(Boolean)
      .map(r => assembleObjectAsset(r, phase1ObjectMap.get(r.requirementName)))
      .filter(Boolean);

    const avatars = avatarResults
      .filter(Boolean)
      .map(r => assembleAvatarAsset(r, phase1AvatarMap.get(r.requirementName)))
      .filter(Boolean);

    const matchedParticleNames = new Set(textureAssets.map(a => a.matchedRequirement));
    const matchedObjectNames   = new Set(objects3d.map(a => a.matchedRequirement));
    const matchedAvatarNames   = new Set(avatars.map(a => a.matchedRequirement));

    // Skipped requirements are recorded separately from unmatched
    const skippedRequirements = [
      ...objectReqs.filter(r => !isIncluded(r.name)).map(r => ({
        requirementName: r.name, type: "object_3d", reason: "Skipped by user in reference image modal"
      })),
      ...avatarReqs.filter(r => !isIncluded(r.name)).map(r => ({
        requirementName: r.name, type: "avatar", reason: "Skipped by user in reference image modal"
      })),
      ...particleReqs.filter(r => !isIncluded(r.name)).map(r => ({
        requirementName: r.name, type: "particle_effect", reason: "Skipped by user in reference image modal"
      }))
    ];

    const unmatchedRequirements = [
      ...particleReqs.filter(r => isIncluded(r.name) && !matchedParticleNames.has(r.name)).map(r => ({
        requirementName: r.name, type: "particle_effect", reason: "No visual candidates found in particle scan"
      })),
      ...activeObjectReqs.filter(r => !matchedObjectNames.has(r.name)).map(r => {
        const routedToAvatars = assetSourceByName.get(r.name.toLowerCase()) === 'avatars';
        const csvNames = reqCsvCandidateNames.get(r.name) || new Set();
        return {
          requirementName: r.name,
          type: "object_3d",
          assetSource: routedToAvatars ? 'avatars' : 'props',
          reason: routedToAvatars
            ? "No visual candidates found in Avatars.zip during cross-route vision matching"
            : csvNames.size === 0
              ? "CSV text search returned 0 qualifying candidates — searchTerms may need to be broader or corrected"
              : "CSV search found candidates but none were loaded from zips",
          searchTermsUsed: r.searchTerms || [],
          csvCandidateCount: csvNames.size
        };
      }),
      ...activeAvatarReqs.filter(r => !matchedAvatarNames.has(r.name)).map(r => {
        const routedToProps = assetSourceByName.get(r.name.toLowerCase()) === 'props';
        const csvNames = avatarViaPropsReqCsvCandidateNames.get(r.name) || new Set();
        return {
          requirementName: r.name,
          type: "avatar",
          assetSource: routedToProps ? 'props' : 'avatars',
          reason: routedToProps
            ? csvNames.size === 0
              ? "Props CSV text search returned 0 qualifying candidates for avatar cross-route"
              : "Props CSV search found candidates but none were loaded from zips"
            : "No visual candidates found in Avatars.zip during vision matching"
        };
      })
    ];

    const roster = {
      documentTitle:             "Game-Specific Asset Roster",
      gameInterpretationSummary: gameInterpretation,
      objects3d,
      avatars,
      textureAssets,
      unmatchedRequirements,
      skippedRequirements,
      coverageSummary: {
        totalObjects3d:  objects3d.length,
        totalAvatars:    avatars.length,
        totalTextures:   textureAssets.length,
        totalUnmatched:  unmatchedRequirements.length,
        limitsRespected: objects3d.length <= MAX_OBJ_ASSETS && avatars.length <= MAX_AVATAR_ASSETS && textureAssets.length <= MAX_PNG_ASSETS,
        coverageNotes:   `${objects3d.length} objects, ${avatars.length} avatars, and ${textureAssets.length} particle textures selected.`
      },
      visualDirectionNotes: {}
    };

    roster._phase1Analysis = phase1;

    // ── DEBUG: attach candidate pools + confidence scores per requirement ──
    // Remove this block when debug modal is no longer needed
    const refImgByNameDebug = refImageByName;
    roster._debugCandidates = {};
    for (const r of objectResults.filter(Boolean)) {
      const p1Req = phase1ObjectMap.get(r.requirementName);
      const refImg = refImgByNameDebug.get(r.requirementName.toLowerCase());
      roster._debugCandidates[r.requirementName] = {
        type:             'object3d',
        visualDescription: p1Req ? (p1Req.visualDescription || '') : '',
        variantGroup:      p1Req ? (p1Req.variantGroup || '') : '',
        searchTerms:       p1Req ? (p1Req.searchTerms || []) : [],
        refImage:          refImg ? { b64: refImg.b64, mimeType: refImg.mimeType } : null,
        candidates:        r.debugCandidates || []
      };
    }
    for (const r of avatarResults.filter(Boolean)) {
      const p1Req = phase1AvatarMap.get(r.requirementName);
      const refImg = refImgByNameDebug.get(r.requirementName.toLowerCase());
      roster._debugCandidates[r.requirementName] = {
        type:             'avatar',
        visualDescription: p1Req ? (p1Req.visualDescription || '') : '',
        variantGroup:      p1Req ? (p1Req.variantGroup || '') : '',
        searchTerms:       p1Req ? (p1Req.searchTerms || []) : [],
        refImage:          refImg ? { b64: refImg.b64, mimeType: refImg.mimeType } : null,
        candidates:        r.debugCandidates || []
      };
    }
    // ── END DEBUG ──────────────────────────────────────────────────────────
    enforceHardLimits(roster);

    roster._meta = {
      jobId,
      generatedAt:         Date.now(),
      totalObjectAssets:   objectAssets.length,
      totalAvatarAssets:   avatarAssets.length,
      totalParticleAssets: particleAssets.length,
      refImagesUsed:       refImageByName.size,
      csvEntriesLoaded:    csvIndex.length,
      avatarZipPath:        resolvedAvatarZipPath,
      approved:            false
    };

    // ── 9. Save pending roster to Firebase ──────────────────────────────
    await bucket.file(`${projectPath}/ai_asset_roster_pending.json`).save(
      JSON.stringify(roster, null, 2),
      { contentType: "application/json", resumable: false }
    );

    console.log(
      `[ROSTER-AB] Complete. Objects: ${objects3d.length}, ` +
      `Avatars: ${avatars.length}, ` +
      `Textures: ${textureAssets.length}, ` +
      `Unmatched: ${unmatchedRequirements.length}, ` +
      `RefImages used: ${refImageByName.size}, ` +
      `CSV entries: ${csvIndex.length}`
    );

    return { statusCode: 202, body: "" };

  } catch (error) {
    console.error("[ROSTER-AB] Unhandled error:", error);
    if (bucket && projectPath) {
      try {
        await bucket.file(`${projectPath}/ai_asset_roster_stageAB_error.json`).save(
          JSON.stringify({ error: error.message, failedAt: Date.now(), stage: "visualMatching", jobId: jobId || null }),
          { contentType: "application/json", resumable: false }
        );
      } catch (e) { /* non-fatal */ }
    }
    return { statusCode: 202, body: "" };
  }
};
