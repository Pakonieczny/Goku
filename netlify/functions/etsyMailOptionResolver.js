/*  netlify/functions/etsyMailOptionResolver.js
 *
 *  v2.2 — Option Sheet Resolver with rush production + shipping summary.
 *
 *  ═══ WHAT THIS DOES ═══════════════════════════════════════════════════
 *
 *  Customer says: "I want 1F + 2B + 3A, qty 5, can you rush it?"
 *  Resolver does: validates each code is real, looks up prices, applies
 *                 stud-set math (single = 60%, mismatched = +$5), sums
 *                 per-piece subtotal, multiplies by quantity, applies the
 *                 bulk-tier discount, optionally adds rush production
 *                 fee ($15 flat per order), returns a fully itemized
 *                 result PLUS a shipping summary the AI can mention.
 *
 *  ═══ FOUR OPS ═══════════════════════════════════════════════════════════
 *
 *  POST { op: "resolveQuote", family, selectedCodes, quantity, wantsRush?,
 *         includeShippingSummary? }
 *      AI tool path. Returns:
 *        { success:true, family, lineItems, perPieceSubtotal, quantity,
 *          subtotal, bulkTier, discountAmount, rush?, total, currency,
 *          escalations, shippingSummary? }
 *
 *      `rush` populated only when wantsRush:true AND the family permits.
 *      Hard-escalates if wantsRush:true with any Quote-row code present
 *      (per Custom Brites policy — operator must confirm rush + custom
 *      pricing together).
 *
 *      `shippingSummary` populated only when includeShippingSummary:true.
 *      Read-only summary derived from EtsyMail_ShippingUpgradesCache;
 *      gives the AI a price range and fastest-days text it can mention
 *      verbatim. Never binds to a specific shipping cost — Etsy checkout
 *      shows that to the customer.
 *
 *  POST { op: "getSheet", family }
 *      Returns the full option sheet for one family. UI reads this.
 *
 *  POST { op: "listFamilies" }
 *      Returns all available families.
 *
 *  POST { op: "validateCode", family, code }
 *      Single-code lookup. Lightweight check.
 *
 *  POST { op: "putSheet", family, sheet }  — owner-only, used by import
 *
 *  ═══ EXPORTED HELPER ══════════════════════════════════════════════════
 *
 *    module.exports.resolveQuote({ family, selectedCodes, quantity,
 *                                  wantsRush, includeShippingSummary })
 *      Direct-import path for etsyMailSalesAgent. Same pattern as
 *      Step 1's searchListings.
 *
 *  ═══ FIRESTORE SHAPE ══════════════════════════════════════════════════
 *
 *    EtsyMail_OptionSheets/{family}            ← option sheets
 *    EtsyMail_ShippingUpgradesCache/current    ← shipping cache (Step 2.2)
 *
 *  ═══ ENV VARS ══════════════════════════════════════════════════════════
 *
 *    ETSYMAIL_EXTENSION_SECRET     gates this endpoint
 */

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const { requireOwner, logUnauthorized } = require("./_etsyMailRoles");

// v2.2 — direct-import shipping summary helper. Try-around so the
// resolver still works if etsyMailShippingSync hasn't been deployed yet
// (graceful degradation, same pattern as the collateral guard).
let getShippingUpgradesCache = null;
let summarizeShippingForAi   = null;
try {
  const shipMod = require("./etsyMailShippingSync");
  getShippingUpgradesCache = shipMod.getShippingUpgradesCache;
  summarizeShippingForAi   = shipMod.summarizeShippingForAi;
} catch (e) {
  console.warn("optionResolver: etsyMailShippingSync not loadable — shippingSummary will be unavailable.", e.message);
}

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const SHEETS_COLL = "EtsyMail_OptionSheets";
const AUDIT_COLL  = "EtsyMail_Audit";

// In-memory cache for option sheets. Sheets change rarely; cache invalidates
// on owner-only writes (the writes call invalidateSheetCache below).
const SHEET_CACHE_MS = 60 * 1000;
const _sheetCache = new Map();   // family → { value, fetchedAt }

// v2.5 — In-memory cache for the existing-listings catalog (separate
// Firestore doc at EtsyMail_OptionSheets/existingListings). Same TTL as
// the sheet cache so updates from the operator's Collateral Library UI
// propagate to the agent within ~1 minute. Single-doc cache keyed by a
// constant since there's only one catalog doc.
const LISTINGS_CACHE_MS = 60 * 1000;
let _listingsCacheValue = null;
let _listingsCacheAt    = 0;

// ─── Helpers ────────────────────────────────────────────────────────────

function json(statusCode, body) {
  return { statusCode, headers: { ...CORS, "Content-Type": "application/json" }, body: JSON.stringify(body) };
}
function bad(msg, code = 400) { return json(code, { error: msg }); }
function ok(body)             { return json(200, { ...body }); }

async function writeAudit({ threadId = null, draftId = null, eventType,
                            actor = "system:optionResolver", payload = {},
                            outcome = "success", ruleViolations = [] }) {
  try {
    await db.collection(AUDIT_COLL).add({
      threadId, draftId, eventType, actor, payload,
      createdAt: FV.serverTimestamp(),
      outcome, ruleViolations
    });
  } catch (e) {
    console.warn("optionResolver audit write failed:", e.message);
  }
}

function r2(n) { return Math.round(n * 100) / 100; }

function invalidateSheetCache(family) {
  if (family) _sheetCache.delete(family);
  else _sheetCache.clear();
}

// ─── Sheet load ────────────────────────────────────────────────────────

async function loadSheet(family) {
  if (!family || typeof family !== "string") return null;
  const cached = _sheetCache.get(family);
  if (cached && (Date.now() - cached.fetchedAt < SHEET_CACHE_MS)) return cached.value;
  try {
    const doc = await db.collection(SHEETS_COLL).doc(family).get();
    if (!doc.exists) {
      _sheetCache.set(family, { value: null, fetchedAt: Date.now() });
      return null;
    }
    const sheet = doc.data();
    _sheetCache.set(family, { value: sheet, fetchedAt: Date.now() });
    return sheet;
  } catch (e) {
    console.warn(`loadSheet(${family}) failed:`, e.message);
    return null;
  }
}

// ─── Existing-listings catalog (v2.5) ──────────────────────────────────
//
// Lives at EtsyMail_OptionSheets/existingListings. Keyed by Etsy listing
// ID, each entry has family + charmStyle + (optional) discDiameterMm +
// metadata. The agent's lookup_listing_specs tool calls resolveListingSpecs
// below to translate "customer pasted a URL" → "here are the dimensions".
//
// Design tenets:
//   - Silhouette dimensions are family-universal. We read them once from
//     the family sheet (loadSheet) so an operator updating the family-
//     wide silhouette spec affects every listing in that family, with no
//     per-listing edits required.
//   - Disc dimensions are per-listing. We read discDiameterMm directly
//     from the catalog entry.
//   - Placeholder values (REPLACE_WITH_*) in catalog entries flag
//     incomplete data — the agent surfaces this as 'incomplete:true' and
//     escalates, never guesses.
//   - Fuzzy match by title/slug/full-title is supported as a fallback so
//     the tool tolerates messy inputs like "the duckie charm" without an
//     ID. Direct ID lookup wins when available.

async function loadExistingListings() {
  if (_listingsCacheValue !== null && (Date.now() - _listingsCacheAt < LISTINGS_CACHE_MS)) {
    return _listingsCacheValue;
  }
  try {
    const doc = await db.collection(SHEETS_COLL).doc("existingListings").get();
    if (!doc.exists) {
      _listingsCacheValue = null;
      _listingsCacheAt    = Date.now();
      return null;
    }
    const data = doc.data() || {};
    _listingsCacheValue = data;
    _listingsCacheAt    = Date.now();
    return data;
  } catch (e) {
    console.warn("loadExistingListings failed:", e.message);
    return null;
  }
}

function invalidateExistingListingsCache() {
  _listingsCacheValue = null;
  _listingsCacheAt    = 0;
}

// Score a candidate listing against a free-text query for fuzzy fallback.
// Returns 0 if no signal; otherwise a positive score. Higher is better.
function _scoreListingMatch(entry, queryLower) {
  if (!entry || !queryLower) return 0;
  let score = 0;
  const title     = String(entry.title || "").toLowerCase();
  const fullTitle = String(entry.fullTitle || "").toLowerCase();
  const slug      = String(entry.etsyUrlFragment || "").toLowerCase();

  // Whole-substring hits on the title/slug are strong signals.
  if (title && title.includes(queryLower)) score += 100;
  if (slug && slug.includes(queryLower))   score += 90;
  if (fullTitle && fullTitle.includes(queryLower)) score += 60;
  if (title && queryLower.includes(title)) score += 40;   // query contains the title

  // Token-level overlap covers reorderings and partial matches like "duckie
  // rubber" vs "rubber duckie". Drop tokens shorter than 3 chars (stopwords).
  const qTokens = queryLower.split(/[\s/_-]+/).filter(t => t.length >= 3);
  if (qTokens.length > 0) {
    const corpus = (title + " " + fullTitle + " " + slug).toLowerCase();
    const corpusTokens = new Set(corpus.split(/[\s/_-]+/).filter(t => t.length >= 3));
    let overlap = 0;
    for (const t of qTokens) {
      if (corpusTokens.has(t)) overlap++;
    }
    score += Math.round((overlap / qTokens.length) * 50);
  }

  return score;
}

// Extract an Etsy listing ID from any of: full URL, bare ID, slugged URL,
// "/your/listings/" URL, with or without locale prefix. Returns the digit
// string or null. Etsy listing IDs are 9-10 digits in practice; we allow
// 8+ to future-proof.
function _extractListingId(query) {
  if (!query || typeof query !== "string") return null;
  // Most reliable: /listing/<digits>/
  const m = query.match(/\/listing\/(\d{8,})/i);
  if (m) return m[1];
  // Fallback: any 8-12 digit run (covers bare-ID input). Restrict to
  // 8+ to avoid matching prices or random numbers in the query.
  const m2 = query.match(/(?<!\d)(\d{8,12})(?!\d)/);
  if (m2) return m2[1];
  return null;
}

/**
 * Resolve specs (dimensions, charm style, family) for a listing.
 * Accepts flexible input — URL, bare ID, slug, or title fragment.
 *
 * Returns a flat object the AI can read directly. Possible shapes:
 *   • Success (complete):
 *     { found:true, listingId, title, family, charmStyle, dimensions,
 *       dimensionsSummary, dimensionsSource, availableMetals, basePriceUsd,
 *       regularPriceUsd?, matchKind:"id"|"fuzzy" }
 *   • Success (incomplete catalog entry):
 *     { found:true, incomplete:true, listingId, missingFields, recommendation }
 *   • No match:
 *     { found:false, reason:"LISTING_NOT_IN_CATALOG", query, recommendation }
 *   • Catalog not deployed:
 *     { found:false, reason:"CATALOG_NOT_DEPLOYED" }
 *
 * In every case where found is false OR incomplete is true, the
 * recommendation is to ESCALATE — never estimate.
 */
async function resolveListingSpecs({ query }) {
  const catalog = await loadExistingListings();
  if (!catalog) {
    return { found: false, reason: "CATALOG_NOT_DEPLOYED" };
  }
  const listings = (catalog && catalog.listings) || {};

  // Pass 1: direct lookup by extracted listing ID.
  let entry = null;
  let listingId = _extractListingId(query);
  let matchKind = null;
  if (listingId && listings[listingId]) {
    entry = listings[listingId];
    matchKind = "id";
  }

  // Pass 2: fuzzy match by title/slug/fullTitle if no ID hit.
  if (!entry) {
    const q = String(query || "").toLowerCase().trim();
    if (q) {
      const ranked = Object.entries(listings)
        .filter(([_, v]) => v && v.active !== false)
        .map(([k, v]) => ({ id: k, entry: v, score: _scoreListingMatch(v, q) }))
        .filter(x => x.score >= 60)   // threshold: avoid weak matches
        .sort((a, b) => b.score - a.score);
      if (ranked.length > 0) {
        entry = ranked[0].entry;
        listingId = ranked[0].id;
        matchKind = "fuzzy";
      }
    }
  }

  if (!entry) {
    return {
      found: false,
      reason: "LISTING_NOT_IN_CATALOG",
      query,
      recommendation: "Listing isn't in the internal catalog. Escalate to human review — do NOT estimate dimensions from similar items."
    };
  }

  // Detect placeholder/incomplete data (REPLACE_WITH_* tokens or missing fields).
  const incompleteFields = [];
  const family     = entry.family || null;
  const charmStyle = entry.charmStyle || null;
  if (!family || /REPLACE_WITH/i.test(family))        incompleteFields.push("family");
  if (!charmStyle || /REPLACE_WITH/i.test(charmStyle)) incompleteFields.push("charmStyle");
  if (charmStyle === "disc" && (entry.discDiameterMm == null || isNaN(Number(entry.discDiameterMm)))) {
    incompleteFields.push("discDiameterMm");
  }
  if (incompleteFields.length > 0) {
    return {
      found: true,
      incomplete: true,
      listingId,
      title: entry.title || null,
      missingFields: incompleteFields,
      recommendation: "Listing is in catalog but missing required fields. Escalate to human review — do NOT estimate."
    };
  }

  // Resolve dimensions based on style.
  // SILHOUETTE: family-universal — read from the family sheet's charmStyles.silhouette.universalDimensions.
  // DISC: per-listing — read discDiameterMm from the catalog entry, jump-ring from the family sheet.
  let dimensions = null;
  let dimensionsSummary = null;
  let dimensionsSource  = null;

  const familySheet = await loadSheet(family);
  const charmStyles = (familySheet && familySheet.charmStyles) || {};

  if (charmStyle === "silhouette") {
    const sil = charmStyles.silhouette || {};
    const u = sil.universalDimensions || {};
    dimensionsSource = "family.charmStyles.silhouette.universalDimensions";
    if (family === "huggie") {
      // huggie universal: charmWidthMm × charmHeightMm + ringInnerDiameterMm
      dimensions = {
        charmWidthMm: u.charmWidthMm ?? null,
        charmHeightMm: u.charmHeightMm ?? null,
        ringInnerDiameterMm: u.ringInnerDiameterMm ?? null
      };
      const w = u.charmWidthMm, h = u.charmHeightMm, r = u.ringInnerDiameterMm;
      if (w != null && h != null) {
        dimensionsSummary = `${w}×${h}mm silhouette charm` + (r != null ? ` with ${r}mm I.D. mounting ring` : "");
      }
    } else if (family === "necklace") {
      // necklace universal: charmSizeMm string range + family jumpRingDiameter
      const jr = sil.jumpRingDiameter || null;
      dimensions = {
        charmSizeMm: u.charmSizeMm ?? null,
        jumpRingDiameter: jr
      };
      if (u.charmSizeMm) {
        dimensionsSummary = `${u.charmSizeMm}mm silhouette charm` + (jr ? ` with ${jr} jump ring` : "");
      }
    } else {
      // stud or other — fall through to whatever universalDimensions has
      dimensions = { ...u };
      dimensionsSummary = `silhouette ${family} charm`;
    }
  } else if (charmStyle === "disc") {
    const disc = charmStyles.disc || {};
    const jr = disc.jumpRingDiameter || null;
    dimensions = {
      discDiameterMm: Number(entry.discDiameterMm),
      jumpRingDiameter: jr
    };
    dimensionsSource = `existingListings.listings[${listingId}].discDiameterMm`;
    dimensionsSummary = `${entry.discDiameterMm}mm disc with engraved design` + (jr ? `, ${jr} jump ring` : "");
  } else {
    // Style is neither silhouette nor disc — unsupported; flag and escalate.
    return {
      found: true,
      incomplete: true,
      listingId,
      title: entry.title || null,
      missingFields: ["charmStyle (unsupported value: " + charmStyle + ")"],
      recommendation: "Listing's charmStyle is not 'silhouette' or 'disc'. Escalate."
    };
  }

  return {
    found: true,
    listingId,
    title: entry.title || null,
    fullTitle: entry.fullTitle || null,
    etsyUrl: entry.etsyUrl || null,
    family,
    charmStyle,
    dimensions,
    dimensionsSummary,
    dimensionsSource,
    // v2.6 — Surface the family's metalSpecs alongside dimensions so the
    // agent has thickness/gauge info available for any follow-up
    // question without a second tool call. metalSpecs is universal
    // across the product line (same value on every family doc).
    metalSpecs: (familySheet && familySheet.metalSpecs) || null,
    availableMetals: Array.isArray(entry.availableMetals) ? entry.availableMetals : null,
    basePriceUsd: typeof entry.basePriceUsd === "number" ? entry.basePriceUsd : null,
    regularPriceUsd: typeof entry.regularPriceUsd === "number" ? entry.regularPriceUsd : null,
    matchKind   // "id" or "fuzzy" — lets the prompt know how confident the match is
  };
}

// Index a sheet's options by code for O(1) lookup. Stud section 2 (Choose
// Your Set) has codes that are MODIFIERS, not standalone options — they're
// indexed too but flagged.
function indexSheetCodes(sheet) {
  const idx = new Map();
  if (!sheet || !Array.isArray(sheet.sections)) return idx;
  for (const section of sheet.sections) {
    if (!Array.isArray(section.options)) continue;
    for (const option of section.options) {
      if (!option.code) continue;
      idx.set(String(option.code).toUpperCase(), {
        sectionId  : section.sectionId,
        sectionName: section.name,
        required   : section.required === true,
        isAutomatic: section.isAutomatic === true,
        option
      });
    }
  }
  return idx;
}

// Pick the bulk tier matching `quantity`. Each sheet has exactly one
// bulkSavings section (auto-applied).
function pickBulkTier(sheet, quantity) {
  for (const section of (sheet.sections || [])) {
    if (!Array.isArray(section.bulkSavings)) continue;
    for (const tier of section.bulkSavings) {
      const minOk = quantity >= tier.minQty;
      const maxOk = tier.maxQty === null || tier.maxQty === undefined || quantity <= tier.maxQty;
      if (minOk && maxOk) return tier;
    }
  }
  return null;
}

// ─── The resolver ──────────────────────────────────────────────────────

/** Resolve a list of selected codes into a fully itemized quote.
 *
 *  Inputs:
 *    family                  — "huggie" | "necklace" | "stud"
 *    selectedCodes           — array of code strings, e.g. ["1F", "2B", "3A"]
 *    quantity                — integer ≥ 1
 *    wantsRush               — boolean (optional, default false). If true,
 *                              add the family's rush production fee to the
 *                              total. Hard-fails if the rush policy
 *                              forbids it (qty over cap, or any Quote-row
 *                              code present per hardEscalateWithQuoteRow).
 *    includeShippingSummary  — boolean (optional, default false). If true,
 *                              attach a shippingSummary object derived
 *                              from EtsyMail_ShippingUpgradesCache. Does
 *                              not affect the math; it's a read-only
 *                              summary the AI can mention to the customer.
 *
 *  Returns:
 *    { success: true,  ... }           on full success
 *    { success: false, reason, ... }   on hard failure
 *
 *  Soft-escalation: if any code is a priceQuote row, the resolver
 *  STILL completes the partial quote (sums what it can) and returns
 *  with `escalations[]` populated. The agent then composes a Needs
 *  Review handoff per the synopsis spec.
 *
 *  Hard failure cases:
 *    UNKNOWN_FAMILY, UNKNOWN_CODE, INVALID_QUANTITY, NOT_AVAILABLE,
 *    REQUIRED_SECTION_MISSING, DEPENDENT_SECTION_MISSING_PARENT,
 *    NO_BULK_TIER_FOR_QUANTITY, RUSH_NOT_AVAILABLE,
 *    RUSH_QTY_OVER_CAP, RUSH_BLOCKED_BY_QUOTE_ROW
 */
async function resolveQuote({ family, selectedCodes, quantity,
                              wantsRush = false,
                              includeShippingSummary = false }) {
  if (!family || typeof family !== "string") {
    return { success: false, reason: "UNKNOWN_FAMILY" };
  }
  if (!Array.isArray(selectedCodes) || selectedCodes.length === 0) {
    return { success: false, reason: "NO_CODES_SELECTED" };
  }
  const qty = parseInt(quantity, 10);
  if (!Number.isFinite(qty) || qty < 1) {
    return { success: false, reason: "INVALID_QUANTITY", quantity };
  }

  const sheet = await loadSheet(family);
  if (!sheet || sheet.active === false) {
    return { success: false, reason: "UNKNOWN_FAMILY", family };
  }

  const codeIndex = indexSheetCodes(sheet);
  const lineItems = [];
  const escalations = [];
  const notAvailable = [];
  const unknownCodes = [];

  // Track which sections have been "covered" so we can validate required
  // sections + dependencies AFTER the loop.
  const coveredSections = new Set();
  // Stud section 2 modifiers are tracked separately (they don't add a
  // line item; they transform section 1's price).
  const studSetModifier = { type: null, pct: null, amountUsd: 0 };

  for (const rawCode of selectedCodes) {
    const code = String(rawCode || "").toUpperCase().trim();
    if (!code) continue;
    const entry = codeIndex.get(code);
    if (!entry) {
      unknownCodes.push(code);
      continue;
    }
    coveredSections.add(entry.sectionId);
    const opt = entry.option;

    // ─── Not-available code: hard fail. Caller must re-prompt. ───────
    if (opt.priceNotAvailable === true) {
      notAvailable.push({
        code,
        section: entry.sectionName,
        message: opt.notAvailableMessage ||
                 `The selection ${code} is not available. Please choose an alternative.`
      });
      continue;
    }

    // ─── Stud section 2 (Choose Your Set) — modifier, not a line item ─
    // Family-specific path: only stud has this.
    if (family === "stud" && entry.sectionId === 2 && opt.modifier) {
      const m = opt.modifier;
      if (m.type === "asIs") {
        studSetModifier.type = "asIs";
      } else if (m.type === "percentOfPair") {
        studSetModifier.type = "percentOfPair";
        studSetModifier.pct = m.pct;
      } else if (m.type === "addToTotal") {
        studSetModifier.type = "addToTotal";
        studSetModifier.amountUsd = m.amountUsd;
      }
      lineItems.push({
        code,
        sectionId: entry.sectionId,
        sectionName: entry.sectionName,
        label: opt.label,
        priceUsd: 0,                            // modifier, not standalone
        modifierApplied: m,
        explainer: opt.explainer || null,
        isModifier: true
      });
      continue;
    }

    // ─── Quote row: soft-escalation. Record it but don't hard-fail. ──
    if (opt.priceQuote === true) {
      escalations.push({
        code,
        section: entry.sectionName,
        sectionId: entry.sectionId,
        reason: "PRICE_QUOTE_REQUIRED",
        details: opt
      });
      lineItems.push({
        code,
        sectionId: entry.sectionId,
        sectionName: entry.sectionName,
        label: optLabelFor(opt),
        priceUsd: null,                         // unknown — quote required
        priceQuote: true,
        priceRange: opt.priceRange || null,
        explainer: opt.explainer || null
      });
      continue;
    }

    // ─── Normal priced option ────────────────────────────────────────
    lineItems.push({
      code,
      sectionId: entry.sectionId,
      sectionName: entry.sectionName,
      label: optLabelFor(opt),
      priceUsd: opt.priceUsd,
      explainer: opt.explainer || null
    });
  }

  // Hard fails BEFORE summing
  if (unknownCodes.length) {
    return {
      success: false,
      reason: "UNKNOWN_CODE",
      unknownCodes,
      hint: `Selected code(s) ${unknownCodes.join(", ")} are not in the ${family} option sheet. Re-prompt the customer with the valid codes for that section.`
    };
  }
  if (notAvailable.length) {
    return {
      success: false,
      reason: "NOT_AVAILABLE",
      notAvailable,
      hint: "Re-prompt the customer with the suggested alternatives in notAvailableMessage."
    };
  }

  // Required-section check
  const missingRequired = [];
  for (const section of (sheet.sections || [])) {
    if (section.required !== true) continue;
    if (section.isAutomatic === true) continue;   // bulkSavings auto-picks
    if (!coveredSections.has(section.sectionId)) {
      missingRequired.push({ sectionId: section.sectionId, name: section.name });
    }
  }
  if (missingRequired.length) {
    return {
      success: false,
      reason: "REQUIRED_SECTION_MISSING",
      missingRequired,
      hint: "Ask the customer to choose an option from each required section."
    };
  }

  // Dependent-section check
  for (const section of (sheet.sections || [])) {
    if (!section.dependencies || !section.dependencies.requires) continue;
    const requires = String(section.dependencies.requires);
    // Format: "section3:any" → requires section 3 to be covered
    const m = /^section(\d+):any$/.exec(requires);
    if (m) {
      const parentId = parseInt(m[1], 10);
      const parentCovered = coveredSections.has(parentId);
      const childCovered  = coveredSections.has(section.sectionId);
      if (childCovered && !parentCovered) {
        return {
          success: false,
          reason: "DEPENDENT_SECTION_MISSING_PARENT",
          dependentSection: { sectionId: section.sectionId, name: section.name },
          parentSection: { sectionId: parentId },
          hint: `Section ${section.sectionId} (${section.name}) requires a selection in section ${parentId} first.`
        };
      }
      // If parent isn't covered AND child isn't either, that's fine —
      // optional dependent skipped naturally.
    }
  }

  // ─── Per-piece subtotal calc ─────────────────────────────────────────
  // Sum priced (non-modifier, non-quote) line items.
  let perPiecePriced = 0;
  for (const li of lineItems) {
    if (li.priceQuote) continue;       // unknown — not summed
    if (li.isModifier) continue;       // stud-set modifier, applied next
    perPiecePriced += li.priceUsd || 0;
  }
  perPiecePriced = r2(perPiecePriced);

  // Apply stud-set modifier (only for stud family).
  let perPieceAfterModifier = perPiecePriced;
  let modifierExplainer = null;
  if (family === "stud") {
    if (studSetModifier.type === "percentOfPair") {
      // 60% of pair price applies to the WHOLE per-piece total
      // (charm price + any other priced items).
      perPieceAfterModifier = r2(perPiecePriced * (studSetModifier.pct / 100));
      modifierExplainer = `Single Stud: ${studSetModifier.pct}% of pair price`;
    } else if (studSetModifier.type === "addToTotal") {
      perPieceAfterModifier = r2(perPiecePriced + studSetModifier.amountUsd);
      modifierExplainer = `Mismatched Pair: pair price + $${studSetModifier.amountUsd}`;
    } else {
      modifierExplainer = "Pair (default)";
    }
  }

  // ─── Quantity and bulk discount ──────────────────────────────────────
  const subtotal = r2(perPieceAfterModifier * qty);
  const tier = pickBulkTier(sheet, qty);
  if (!tier) {
    return {
      success: false,
      reason: "NO_BULK_TIER_FOR_QUANTITY",
      quantity: qty,
      hint: "Sheet's bulk-tier ranges don't cover this quantity. Verify bulkSavings ranges in the option sheet."
    };
  }
  const discountAmount = r2(subtotal * (tier.discountPct / 100));
  const subtotalAfterDiscount = r2(subtotal - discountAmount);

  // ─── Rush production (v2.2) ──────────────────────────────────────────
  // Per-order flat fee, regardless of quantity. Capped by qty per family
  // policy. Hard-escalates with Quote-row codes per Custom Brites rule:
  // an operator must confirm rush + custom pricing together — the AI
  // can't promise both autonomously.
  let rush = null;
  if (wantsRush === true) {
    const policy = sheet.rushProduction || null;
    if (!policy || policy.available !== true) {
      return {
        success: false,
        reason : "RUSH_NOT_AVAILABLE",
        family,
        hint   : "This product family does not currently offer rush production."
      };
    }
    if (typeof policy.qtyMaxForRush === "number" && qty > policy.qtyMaxForRush) {
      return {
        success: false,
        reason : "RUSH_QTY_OVER_CAP",
        family,
        quantity: qty,
        rushQtyMax: policy.qtyMaxForRush,
        hint: `Rush production is only available for orders of ${policy.qtyMaxForRush} pieces or fewer. This order is ${qty}; either reduce the quantity or accept standard production.`
      };
    }
    if (policy.hardEscalateWithQuoteRow === true && escalations.length > 0) {
      return {
        success: false,
        reason : "RUSH_BLOCKED_BY_QUOTE_ROW",
        family,
        escalations,
        hint   : "Rush production combined with a custom-quoted item requires operator approval. Hard-escalate to Needs Review with a synopsis."
      };
    }
    rush = {
      requested: true,
      feeUsd                    : policy.feeUsd,
      feeStructure              : policy.feeStructure || "per_order",
      productionDaysStandardMin : policy.productionDaysStandardMin || null,
      productionDaysStandardMax : policy.productionDaysStandardMax || null,
      productionDaysRushMin     : policy.productionDaysRushMin     || null,
      productionDaysRushMax     : policy.productionDaysRushMax     || null,
      customerFacingDescription : policy.customerFacingDescription || null
    };
  }

  // Final total: subtotal after bulk discount + rush fee (if any)
  const total = r2(subtotalAfterDiscount + (rush ? rush.feeUsd : 0));

  // ─── Shipping summary (v2.2) ─────────────────────────────────────────
  // Read-only summary pulled from EtsyMail_ShippingUpgradesCache. Does
  // not affect the math; the AI uses it to mention shipping options
  // verbatim ("expedited shipping is available at checkout, typically
  // $X.XX-$Y.YY"). Customer picks the actual upgrade at Etsy checkout.
  let shippingSummary = null;
  if (includeShippingSummary === true && getShippingUpgradesCache && summarizeShippingForAi) {
    try {
      const cache = await getShippingUpgradesCache();
      shippingSummary = summarizeShippingForAi(cache);
    } catch (e) {
      // Don't fail the whole quote because shipping cache had a hiccup.
      console.warn("resolveQuote: shippingSummary unavailable:", e.message);
      shippingSummary = { available: false, reason: "CACHE_READ_FAILED" };
    }
  }

  return {
    success: true,
    family,
    familyDisplayName: sheet.displayName || family,
    unitOfMeasure: sheet.unitOfMeasure || null,
    lineItems,
    perPiecePriced,
    perPieceAfterModifier,
    modifierExplainer,
    quantity: qty,
    subtotal,
    bulkTier: { code: tier.code, label: tier.label, discountPct: tier.discountPct },
    discountAmount,
    subtotalAfterDiscount,
    rush,                                          // null or { feeUsd, feeStructure, ... }
    total,                                         // = subtotalAfterDiscount + (rush ? rush.feeUsd : 0)
    currency: "USD",
    escalations,                                   // non-empty = soft-escalation
    requiresNeedsReviewHandoff: escalations.length > 0,
    shippingSummary                                // null or { rangeText, anyUpgrades, fastestDaysText, available }
  };
}

// Best-effort label for a line item — different sections have different
// shape (charm size+metal, hoop+metal, chain+metal, simple label).
function optLabelFor(opt) {
  if (opt.label) return opt.label;
  if (opt.size && opt.metal)        return `${opt.size} ${opt.metal}`;
  if (opt.hoopSize && opt.metal)    return `${opt.hoopSize} hoop ${opt.metal}`;
  if (opt.chainStyle && opt.metal)  return `${opt.chainStyle} ${opt.metal}`;
  if (opt.length)                   return `${opt.length}`;
  if (opt.chainStyle)               return opt.chainStyle;
  return opt.code || "(unlabeled)";
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

  const op = body.op;
  if (!op) return bad("op required");

  try {
    if (op === "resolveQuote") {
      const {
        family, selectedCodes, quantity, threadId,
        wantsRush, includeShippingSummary
      } = body;
      const result = await resolveQuote({
        family, selectedCodes, quantity,
        wantsRush: wantsRush === true,
        includeShippingSummary: includeShippingSummary === true
      });

      // Audit: every quote computation, success or failure. Used for
      // pricing forensics if a customer disputes a number.
      await writeAudit({
        threadId: threadId || null,
        eventType: result.success ? "option_quote_resolved" : "option_quote_failed",
        actor: "system:optionResolver",
        payload: { family, selectedCodes, quantity, wantsRush: wantsRush === true,
                   includeShippingSummary: includeShippingSummary === true, result },
        outcome: result.success ? "success" : "blocked",
        ruleViolations: result.success ? [] : [result.reason || "UNKNOWN"]
      });

      return ok(result);
    }

    if (op === "getSheet") {
      const sheet = await loadSheet(body.family);
      if (!sheet) return json(404, { success: false, reason: "UNKNOWN_FAMILY", family: body.family });
      return ok({ success: true, sheet });
    }

    // v2.5 — Expose the existing-listings catalog to the inbox UI viewer
    // so the operator can see which listings are catalogued (and their
    // resolved dimensions) per family. The agent uses the same catalog
    // via resolveListingSpecs at runtime.
    if (op === "getExistingListings") {
      const catalog = await loadExistingListings();
      if (!catalog) return ok({ success: true, catalog: { listings: {} } });
      return ok({ success: true, catalog });
    }

    if (op === "listFamilies") {
      const snap = await db.collection(SHEETS_COLL).limit(50).get();
      const families = [];
      snap.forEach(d => {
        const data = d.data() || {};
        families.push({
          id: d.id,
          family: data.family || d.id,
          displayName: data.displayName || d.id,
          active: data.active !== false,
          sectionCount: Array.isArray(data.sections) ? data.sections.length : 0
        });
      });
      return ok({ success: true, families });
    }

    if (op === "validateCode") {
      const { family, code } = body;
      if (!family || !code) return bad("family and code required");
      const sheet = await loadSheet(family);
      if (!sheet) return ok({ found: false, reason: "UNKNOWN_FAMILY" });
      const idx = indexSheetCodes(sheet);
      const entry = idx.get(String(code).toUpperCase());
      if (!entry) return ok({ found: false, reason: "UNKNOWN_CODE" });
      return ok({
        found: true,
        code: String(code).toUpperCase(),
        sectionId: entry.sectionId,
        sectionName: entry.sectionName,
        option: entry.option
      });
    }

    if (op === "putSheet") {
      // Owner-only: write a complete option sheet doc. Used for bulk
      // import from the seed JSON, or future "edit sheet" UI.
      const ownerCheck = await requireOwner(body.actor);
      if (!ownerCheck.ok) {
        await logUnauthorized({
          actor: body.actor,
          eventType: "option_sheet_put_unauthorized",
          payload: { family: body.family, reason: ownerCheck.reason }
        });
        return json(403, { error: "Owner role required", reason: ownerCheck.reason });
      }
      const { family, sheet } = body;
      if (!family || !sheet) return bad("family and sheet required");
      if (sheet.family && sheet.family !== family) {
        return bad("sheet.family must match top-level family field");
      }
      // Stamp metadata
      const toWrite = {
        ...sheet,
        family,
        lastUpdatedBy: body.actor,
        updatedAt: FV.serverTimestamp()
      };
      await db.collection(SHEETS_COLL).doc(family).set(toWrite, { merge: false });
      invalidateSheetCache(family);
      await writeAudit({
        eventType: "option_sheet_put",
        actor: body.actor,
        payload: { family }
      });
      return ok({ success: true, family });
    }

    /* ─── v2.5: Multi-family upload from seed-file shape ──────────
     * Owner-only. Accepts the same JSON the operator would have
     * edited locally and run through `seeds/import_seeds.js` —
     * the multi-family wrapper:
     *   { _meta?, huggie: {...}, necklace: {...}, stud: {...} }
     *
     * Top-level `_meta` (and any other key whose value is not an
     * object) is ignored. Every other top-level key is treated as a
     * family name and its value as a sheet. Each family is validated
     * BEFORE any write happens — partial imports would leave Firestore
     * in a half-updated state with no way to roll back.
     *
     * Reply shape:
     *   { success, written: ["huggie","necklace","stud"], skipped: ["_meta"] }
     *
     * If any family fails validation, the response is 422 with
     * `{ error, family, reason }` and NOTHING is written. The seed
     * script's import is one-shot; this UI path mirrors that
     * atomicity so an operator never ends up with two families
     * matching the new file and one matching the old. */
    if (op === "putSheets") {
      const ownerCheck = await requireOwner(body.actor);
      if (!ownerCheck.ok) {
        await logUnauthorized({
          actor: body.actor,
          eventType: "option_sheets_put_unauthorized",
          payload: { reason: ownerCheck.reason }
        });
        return json(403, { error: "Owner role required", reason: ownerCheck.reason });
      }
      const { sheets } = body;
      if (!sheets || typeof sheets !== "object" || Array.isArray(sheets)) {
        return bad("sheets must be an object keyed by family name");
      }

      // Phase 1 — partition + validate every family before any write.
      // v2.5: also recognize an allowlist of "config doc" keys that
      // don't follow the family-sheet shape (no sections[] array) but
      // ARE meant to be written as docs in EtsyMail_OptionSheets. Each
      // gets a light schema check against its expected field.
      const KNOWN_CONFIG_DOCS = {
        customerInquiryGuidance: {
          requiredField: "rules",
          requiredType : "array"
        },
        existingListings: {
          requiredField: "listings",
          requiredType : "object"
        }
      };
      const candidates  = []; // [{ family, sheet }]
      const configDocs  = []; // [{ key, doc }]
      const skipped     = []; // top-level keys we deliberately ignore
      for (const [key, val] of Object.entries(sheets)) {
        // Ignore _meta and any non-object top-level entry. The seed
        // file's _meta block carries human-readable notes about the
        // bulk-tier matrix etc.; persisting it as a sheet would create
        // a phantom family the resolver would index against.
        if (key.startsWith("_") || !val || typeof val !== "object" || Array.isArray(val)) {
          skipped.push(key);
          continue;
        }
        // v2.5 — config-doc path. Light schema check; no family/sections
        // validation since these aren't family sheets.
        if (KNOWN_CONFIG_DOCS[key]) {
          const cfg = KNOWN_CONFIG_DOCS[key];
          const fieldVal = val[cfg.requiredField];
          const fieldOk = cfg.requiredType === "array"
            ? Array.isArray(fieldVal)
            : (fieldVal && typeof fieldVal === "object" && !Array.isArray(fieldVal));
          if (!fieldOk) {
            return json(422, {
              error: "Config doc schema invalid",
              key,
              reason: `${key} must have a '${cfg.requiredField}' ${cfg.requiredType}`
            });
          }
          configDocs.push({ key, doc: val });
          continue;
        }
        // Family-sheet path — existing strict validation.
        if (val.family && val.family !== key) {
          return json(422, {
            error: "Family mismatch",
            family: key,
            reason: `top-level key '${key}' but sheet.family is '${val.family}'`
          });
        }
        if (!Array.isArray(val.sections) || val.sections.length === 0) {
          return json(422, {
            error: "Sheet missing sections",
            family: key,
            reason: "sheet.sections must be a non-empty array"
          });
        }
        candidates.push({ family: key, sheet: val });
      }
      if (candidates.length === 0 && configDocs.length === 0) {
        return bad("No valid sheets or config docs found in payload (every top-level key was skipped)");
      }

      // Phase 2 — write each family + config doc, stamp metadata,
      // invalidate caches. Single batch so the multi-family import is
      // still atomic; config docs join the same batch.
      const batch = db.batch();
      for (const { family, sheet } of candidates) {
        const toWrite = {
          ...sheet,
          family,
          lastUpdatedBy: body.actor,
          updatedAt: FV.serverTimestamp()
        };
        batch.set(db.collection(SHEETS_COLL).doc(family), toWrite, { merge: false });
      }
      // v2.5 — config doc writes. Same collection, but no `family` stamp
      // (these aren't families) and we don't run the sheet validators
      // against them.
      for (const { key, doc } of configDocs) {
        const toWrite = {
          ...doc,
          lastUpdatedBy: body.actor,
          updatedAt: FV.serverTimestamp()
        };
        batch.set(db.collection(SHEETS_COLL).doc(key), toWrite, { merge: false });
      }
      await batch.commit();

      // Cache invalidation must run AFTER the commit so a concurrent
      // resolveQuote during the write can't repopulate the cache from
      // the old doc.
      for (const { family } of candidates) invalidateSheetCache(family);
      // v2.5 — the existing-listings catalog has its own cache.
      if (configDocs.some(c => c.key === "existingListings")) {
        invalidateExistingListingsCache();
      }

      await writeAudit({
        eventType: "option_sheets_put_bulk",
        actor: body.actor,
        payload: {
          written: candidates.map(c => c.family),
          writtenConfigDocs: configDocs.map(c => c.key),
          skipped
        }
      });

      return ok({
        success: true,
        written: candidates.map(c => c.family),
        writtenConfigDocs: configDocs.map(c => c.key),
        skipped
      });
    }

    return bad(`Unknown op '${op}'`);

  } catch (err) {
    console.error("optionResolver error:", err);
    return json(500, { error: err.message || String(err), op });
  }
};

// Direct-import path for sibling functions (etsyMailSalesAgent and
// future Step 3's etsyMailCustomOrderDraft). Same pattern as Step 1's
// searchListings + Step 2's computeQuoteBand.
module.exports.resolveQuote        = resolveQuote;
module.exports.loadSheet           = loadSheet;
module.exports.indexSheetCodes     = indexSheetCodes;
module.exports.invalidateSheetCache = invalidateSheetCache;
// v2.5 — Existing-listings catalog reader + spec resolver. Consumed by
// the salesAgent's lookup_listing_specs tool.
module.exports.loadExistingListings          = loadExistingListings;
module.exports.resolveListingSpecs           = resolveListingSpecs;
module.exports.invalidateExistingListingsCache = invalidateExistingListingsCache;
