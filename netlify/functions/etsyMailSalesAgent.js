/*  netlify/functions/etsyMailSalesAgent.js
 *
 *  v2.1 — Sales-mode state machine orchestrator (option-sheet edition).
 *  v2.8.3 — Care/sizing collateral auto-attachment (PORTED from -background)
 */

const fs   = require("fs");
const path = require("path");

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const { runToolLoop } = require("./_etsyMailAnthropic");

let searchListings = null;
try {
  ({ searchListings } = require("./etsyMailListingsCatalog"));
} catch (e) {
  console.warn("salesAgent: etsyMailListingsCatalog not loadable.", e.message);
}

let resolveQuote = null;
let loadOptionSheet = null;
try {
  ({ resolveQuote, loadSheet: loadOptionSheet } = require("./etsyMailOptionResolver"));
} catch (e) {
  console.error("salesAgent: etsyMailOptionResolver not loadable.", e.message);
}

let lookupListingByUrl = null;
let lookupListingById  = null;
try {
  ({ lookupListingByUrl, lookupListingById } = require("./etsyMailListingsCatalog"));
} catch (e) {
  console.warn("salesAgent: etsyMailListingsCatalog (lookup) not loadable.", e.message);
}

let searchCollateral = null;
try {
  ({ searchCollateral } = require("./etsyMailCollateral"));
} catch (e) {
  console.warn("salesAgent: etsyMailCollateral not loadable.", e.message);
}

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const SALES_COLL    = "EtsyMail_SalesContext";
const PROMPTS_COLL  = "EtsyMail_SalesPrompts";
const THREADS_COLL  = "EtsyMail_Threads";
const DRAFTS_COLL   = "EtsyMail_Drafts";
const AUDIT_COLL    = "EtsyMail_Audit";
const CONFIG_COLL   = "EtsyMail_Config";

const AI_MODEL          = process.env.ETSYMAIL_SALES_MODEL || "claude-sonnet-4-6";
const _ALLOWED_EFFORTS = new Set(["low", "medium", "high", "xhigh", "max"]);
const _RAW_EFFORT = process.env.ETSYMAIL_SALES_EFFORT || "high";
const AI_EFFORT = _ALLOWED_EFFORTS.has(_RAW_EFFORT) ? _RAW_EFFORT : "high";
if (_RAW_EFFORT !== AI_EFFORT) {
  console.warn(`salesAgent: ETSYMAIL_SALES_EFFORT='${_RAW_EFFORT}' invalid — using '${AI_EFFORT}'.`);
}
const AI_MAX_TOKENS     = parseInt(process.env.ETSYMAIL_SALES_MAX_TOKENS || "6000", 10);
const MAX_TOOL_ITERATIONS = 8;

let _cfgCache = { value: null, fetchedAt: 0 };
const CFG_CACHE_MS = 15 * 1000;

async function getConfig() {
  if (_cfgCache.value && (Date.now() - _cfgCache.fetchedAt < CFG_CACHE_MS)) {
    return _cfgCache.value;
  }
  let value = {
    salesModeEnabled       : false,
    salesAutoEngage        : false,
    salesPilotThreadIds    : [],
    listingsMirrorEnabled  : false,
    intentClassifierEnabled: false
  };
  try {
    const doc = await db.collection(CONFIG_COLL).doc("autoPipeline").get();
    if (doc.exists) {
      const d = doc.data() || {};
      value = {
        salesModeEnabled       : d.salesModeEnabled === true,
        salesAutoEngage        : d.salesAutoEngage === true,
        salesPilotThreadIds    : Array.isArray(d.salesPilotThreadIds) ? d.salesPilotThreadIds : [],
        listingsMirrorEnabled  : d.listingsMirrorEnabled === true,
        intentClassifierEnabled: d.intentClassifierEnabled === true
      };
    }
    _cfgCache = { value, fetchedAt: Date.now() };
  } catch (e) {
    console.warn("salesAgent: config fetch failed:", e.message);
  }
  return value;
}

const TERMINAL_THREAD_STATUSES = new Set([
  "sales_completed",
  "sales_abandoned",
  "pending_human_review"
]);

const SALES_PROMPT_DOC_ID = "sales";

async function loadSalesPrompt() {
  try {
    const doc = await db.collection(PROMPTS_COLL).doc(SALES_PROMPT_DOC_ID).get();
    if (!doc.exists) {
      return {
        ok: false,
        error: `Sales prompt missing. Expected ${PROMPTS_COLL}/${SALES_PROMPT_DOC_ID} with field "systemPrompt".`
      };
    }
    const d = doc.data() || {};
    const sp = typeof d.systemPrompt === "string" ? d.systemPrompt : "";
    if (sp.length < 100) {
      return { ok: false, error: `Sales prompt is too short (${sp.length} chars).` };
    }
    return { ok: true, prompt: sp };
  } catch (e) {
    return { ok: false, error: `Sales prompt load failed: ${e.message}` };
  }
}

async function loadOrInitSalesContext(threadId) {
  const ref = db.collection(SALES_COLL).doc(threadId);
  const doc = await ref.get();
  if (doc.exists) {
    return { ...doc.data(), _ref: ref, _isNew: false };
  }
  const init = {
    threadId,
    stage             : "discovery",
    accumulatedSpec   : {},
    missingInputs     : [],
    quoteHistory      : [],
    itemsProposed     : [],
    itemsAccepted     : [],
    totalQuotedUsd    : null,
    discountAppliedPct: 0,
    operatorOverrides : [],
    createdAt         : FV.serverTimestamp(),
    lastTurnAt        : FV.serverTimestamp(),
    lastAdvancedAt    : FV.serverTimestamp(),
    abandonedAt       : null,
    lastSalesAgentBlockReason: null
  };
  await ref.set(init);
  return { ...init, _ref: ref, _isNew: true };
}

async function writeAudit({ threadId = null, draftId = null, eventType,
                            actor = "sales-agent", payload = {},
                            outcome = "success", ruleViolations = [] }) {
  try {
    await db.collection(AUDIT_COLL).add({
      threadId, draftId, eventType, actor, payload,
      createdAt: FV.serverTimestamp(),
      outcome, ruleViolations
    });
  } catch (e) {
    console.warn("salesAgent audit write failed:", e.message);
  }
}

function isCustomerVisibleUrl(url) {
  return typeof url === "string"
    && /^https?:\/\//i.test(url)
    && !/REPLACE_WITH_PUBLIC_URL/i.test(url)
    && !/example\.com/i.test(url);
}

function normalizeAttachment(raw, source = "thread") {
  if (!raw) return null;
  const url = typeof raw === "string"
    ? raw
    : (raw.url || raw.proxyUrl || raw.imageUrl || raw.attachmentUrl || raw.href || "");
  if (!isCustomerVisibleUrl(url)) return null;
  return {
    url,
    source,
    type: raw.type || (raw.contentType && /^image\//i.test(raw.contentType) ? "image" : "file"),
    contentType: raw.contentType || null,
    filename: raw.filename || raw.name || null
  };
}

function mergeAttachments(...sets) {
  const out = [];
  const seen = new Set();
  for (const set of sets) {
    if (!Array.isArray(set)) continue;
    for (const raw of set) {
      const att = normalizeAttachment(raw, raw && raw.source ? raw.source : "thread");
      if (!att || seen.has(att.url)) continue;
      seen.add(att.url);
      out.push(att);
      if (out.length >= 12) return out;
    }
  }
  return out;
}

function compactAttachmentList(atts) {
  return (Array.isArray(atts) ? atts : []).slice(0, 12).map(a => ({
    url: a.url,
    type: a.type || "file",
    source: a.source || "thread",
    filename: a.filename || null
  }));
}

async function loadRecentThreadMessages(threadId, limit = 12) {
  try {
    const snap = await db.collection(THREADS_COLL).doc(threadId)
      .collection("messages")
      .orderBy("timestamp", "desc")
      .limit(Math.max(1, Math.min(limit, 30)))
      .get();
    const rows = [];
    for (const d of snap.docs) {
      const m = d.data() || {};
      const text = String(m.text || "").trim();
      const imageUrls = Array.isArray(m.imageUrls) ? m.imageUrls : [];
      const attachmentUrls = Array.isArray(m.attachmentUrls) ? m.attachmentUrls : [];
      if (!text && imageUrls.length === 0 && attachmentUrls.length === 0) continue;
      rows.push({
        id: d.id,
        direction: m.direction || null,
        text: text.slice(0, 900),
        hasAttachments: imageUrls.length + attachmentUrls.length > 0,
        attachmentCount: imageUrls.length + attachmentUrls.length,
        timestamp: m.timestamp && typeof m.timestamp.toMillis === "function" ? m.timestamp.toMillis() : null
      });
    }
    return rows.reverse();
  } catch (e) {
    console.warn("salesAgent: recent thread message load failed:", e.message);
    return [];
  }
}

function recentOutboundTextsFromMessages(messages) {
  return (Array.isArray(messages) ? messages : [])
    .filter(m => m && m.direction === "outbound" && m.text)
    .slice(-4)
    .map(m => String(m.text).trim())
    .filter(Boolean);
}

function normalizeForRepeat(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/https?:\/\/\S+/g, "")
    .replace(/[^a-z0-9$]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function splitSentencesLight(s) {
  return String(s || "")
    .split(/(?<=[.!?])\s+/)
    .map(x => x.trim())
    .filter(Boolean);
}

function applySalesReplyGuard(reply, priorOutboundTexts = []) {
  const raw = String(reply || "").trim();
  if (!raw) return raw;
  const priorSentenceSet = new Set();
  for (const prior of priorOutboundTexts) {
    for (const sent of splitSentencesLight(prior)) {
      const n = normalizeForRepeat(sent);
      if (n.length >= 32) priorSentenceSet.add(n);
    }
  }
  if (!priorSentenceSet.size) return raw;

  const kept = [];
  const priorJoined = Array.from(priorSentenceSet).join(" | ");
  for (const sent of splitSentencesLight(raw)) {
    const n = normalizeForRepeat(sent);
    const capabilityReset = /custom .*charm.*photo.*absolutely.*(do|possible|make)/i.test(sent)
      && /custom .*charm.*photo.*absolutely.*(do|possible|make)/i.test(priorJoined);
    if (n.length >= 32 && (priorSentenceSet.has(n) || capabilityReset)) continue;
    kept.push(sent);
  }
  return kept.length ? kept.join(" ") : raw;
}

function inferFamilyFromTextAndContext(text, salesCtx = {}) {
  const spec = salesCtx.accumulatedSpec || {};
  if (spec.family === "huggie" || spec.family === "necklace" || spec.family === "stud") return spec.family;
  const t = String(text || "").toLowerCase();
  if (/huggie|huggy|hoop/.test(t)) return "huggie";
  if (/necklace|chain|pendant/.test(t)) return "necklace";
  if (/stud|studs|earring/.test(t)) return "stud";
  if (/charm/.test(t) && /chain|necklace|pendant/.test(t)) return "necklace";
  return null;
}

function compactOption(option) {
  if (!option || typeof option !== "object") return null;
  return {
    code: option.code || null,
    label: option.label || null,
    size: option.size || null,
    metal: option.metal || null,
    chainStyle: option.chainStyle || null,
    length: option.length || null,
    hoopSize: option.hoopSize || null,
    priceUsd: typeof option.priceUsd === "number" ? option.priceUsd : null,
    priceQuote: option.priceQuote === true,
    priceNotAvailable: option.priceNotAvailable === true,
    explainer: option.explainer || null
  };
}

function compactOptionSheetForAi(sheet) {
  if (!sheet || !Array.isArray(sheet.sections)) return null;
  return {
    family: sheet.family || null,
    displayName: sheet.displayName || sheet.family || null,
    unitOfMeasure: sheet.unitOfMeasure || null,
    sections: sheet.sections.map(sec => ({
      sectionId: sec.sectionId,
      name: sec.name,
      instruction: sec.instruction || null,
      required: sec.required === true,
      dependencies: sec.dependencies || null,
      options: Array.isArray(sec.options) ? sec.options.map(compactOption).filter(Boolean) : [],
      bulkSavings: Array.isArray(sec.bulkSavings) ? sec.bulkSavings : []
    }))
  };
}

async function prefetchLineSheetCollateral({ latestInboundText, salesCtx }) {
  const family = inferFamilyFromTextAndContext(latestInboundText, salesCtx);
  if (!family || !searchCollateral) return [];
  try {
    const result = await searchCollateral({ category: family, kind: "line_sheet", limit: 3 });
    const matches = Array.isArray(result && result.matches) ? result.matches : [];
    return matches.filter(m => m && isCustomerVisibleUrl(m.url)).slice(0, 3);
  } catch (e) {
    console.warn("salesAgent: line-sheet collateral prefetch failed:", e.message);
    return [];
  }
}

// ─── v2.8.x — Care / sizing collateral topic detection + prefetch ─────
//
// Care/durability/tarnish topic detector. Returns boolean indicating
// whether the customer's question is about caring for or comparing the
// metals. Matches across recent thread context, not just latest inbound,
// so a multi-turn care discussion stays detected.
const CARE_TOPIC_KEYWORDS = [
  // direct
  "tarnish", "tarnishing", "tarnishes", "rust", "rusting", "corrode", "corrosion",
  "shower", "showering", "swim", "swimming", "water", "wet", "soap", "shampoo",
  "chlorine", "pool", "hot tub", "sauna", "ocean", "sweat", "workout",
  "care", "caring", "clean", "cleaning", "polish", "polishing", "store", "storage",
  "wear it everyday", "daily wear", "every day", "long-lasting", "longevity",
  "hypoallergenic", "allergic", "allergy", "sensitive skin",
  "durable", "durability", "fade", "fading",
  "lotion", "perfume", "cologne", "deodorant", "skincare",
  // metal comparison
  "gold filled", "gold-filled", "gold plated", "gold-plated", "filled vs plated",
  "filled vs solid", "plated vs solid", "vermeil", "metal comparison",
  "what's the difference", "difference between", "which is better",
  "solid gold", "real gold", "14k gold", "14k solid",
  "sterling vs", "silver vs", "gold vs",
  // questions framed about long-term wear
  "hold up", "holds up", "last forever", "lifetime", "for years"
];

function _careTopicDetected(haystackLower) {
  return CARE_TOPIC_KEYWORDS.some(kw => haystackLower.includes(kw));
}

async function prefetchCareCollateral({ latestInboundText, recentThreadMessages }) {
  // Detect topic from inbound + last few thread messages
  const haystack = (
    (latestInboundText || "") + " " +
    (Array.isArray(recentThreadMessages)
      ? recentThreadMessages.slice(-5).map(m => (m && m.text) || "").join(" ")
      : "")
  ).toLowerCase();
  const topicDetected = _careTopicDetected(haystack);
  if (!topicDetected) {
    return { matches: [], topicDetected: false, rawCount: 0, filteredOutPlaceholder: 0, reason: "topic_not_detected" };
  }
  if (!searchCollateral) {
    return { matches: [], topicDetected: true, rawCount: 0, filteredOutPlaceholder: 0, reason: "search_unavailable" };
  }
  try {
    // Two searches; deduplicate by id. First tries the care/comparison
    // categories, second falls back to keyword search.
    const out = [];
    const seen = new Set();
    let rawCount = 0;
    let filteredOutPlaceholder = 0;
    for (const query of [
      { category: "aftercare", limit: 5 },
      { category: "metals_education", limit: 5 },
      { keywords: ["tarnish", "care", "shower", "metal", "gold filled", "comparison", "aftercare"], limit: 5 }
    ]) {
      let result;
      try { result = await searchCollateral(query); } catch { continue; }
      const matches = Array.isArray(result && result.matches) ? result.matches : [];
      rawCount += matches.length;
      for (const m of matches) {
        if (!m || !m.id) continue;
        if (seen.has(m.id)) continue;
        if (!isCustomerVisibleUrl(m.url)) { filteredOutPlaceholder++; continue; }
        seen.add(m.id);
        out.push(m);
      }
    }
    return {
      matches: out.slice(0, 4),
      topicDetected: true,
      rawCount, filteredOutPlaceholder,
      reason: out.length ? "matches_found" : "no_matches_after_filter"
    };
  } catch (e) {
    console.warn("salesAgent: care collateral prefetch failed:", e.message);
    return { matches: [], topicDetected: true, rawCount: 0, filteredOutPlaceholder: 0, reason: "search_error" };
  }
}

const SIZING_TOPIC_KEYWORDS = [
  // direct
  "size", "sizing", "sized", "fit", "fits", "fitting", "measurement", "measure",
  "how long", "how short", "how big", "how small",
  "chain length", "necklace length", "what length",
  "wrist", "wrist size", "wrist circumference",
  "neck size", "collarbone", "drop", "pendant drop", "on body",
  "inches", "inch", "cm", "centimeter", "mm",
  // common framings
  "what size should i get", "what size do i need", "will it fit",
  "too long", "too short", "too big", "too small", "right size",
  // specific length references
  "16 inch", "16-inch", "16in", "16\"",
  "18 inch", "18-inch", "18in", "18\"",
  "20 inch", "20-inch", "20in", "20\"",
  "22 inch", "22-inch", "22in", "22\"",
  "24 inch", "24-inch", "24in", "24\"",
  // body reference
  "lies on", "sits at", "falls at", "bustline", "collarbone", "choker"
];

function _sizingTopicDetected(haystackLower) {
  return SIZING_TOPIC_KEYWORDS.some(kw => haystackLower.includes(kw));
}

async function prefetchSizingCollateral({ latestInboundText, recentThreadMessages }) {
  const haystack = (
    (latestInboundText || "") + " " +
    (Array.isArray(recentThreadMessages)
      ? recentThreadMessages.slice(-5).map(m => (m && m.text) || "").join(" ")
      : "")
  ).toLowerCase();
  const topicDetected = _sizingTopicDetected(haystack);
  if (!topicDetected) {
    return { matches: [], topicDetected: false, rawCount: 0, filteredOutPlaceholder: 0, reason: "topic_not_detected" };
  }
  if (!searchCollateral) {
    return { matches: [], topicDetected: true, rawCount: 0, filteredOutPlaceholder: 0, reason: "search_unavailable" };
  }
  try {
    const out = [];
    const seen = new Set();
    let rawCount = 0;
    let filteredOutPlaceholder = 0;
    for (const query of [
      { category: "sizing", limit: 5 },
      { keywords: ["sizing", "fit", "chain length", "wrist", "necklace fit", "bracelet sizing"], limit: 5 }
    ]) {
      let result;
      try { result = await searchCollateral(query); } catch { continue; }
      const matches = Array.isArray(result && result.matches) ? result.matches : [];
      rawCount += matches.length;
      for (const m of matches) {
        if (!m || !m.id) continue;
        if (seen.has(m.id)) continue;
        if (!isCustomerVisibleUrl(m.url)) { filteredOutPlaceholder++; continue; }
        seen.add(m.id);
        out.push(m);
      }
    }
    return {
      matches: out.slice(0, 4),
      topicDetected: true,
      rawCount, filteredOutPlaceholder,
      reason: out.length ? "matches_found" : "no_matches_after_filter"
    };
  } catch (e) {
    console.warn("salesAgent: sizing collateral prefetch failed:", e.message);
    return { matches: [], topicDetected: true, rawCount: 0, filteredOutPlaceholder: 0, reason: "search_error" };
  }
}

// ─── Tool executors ────────────────────────────────────────────────────

function buildToolExecutors({ threadId, salesCtx, customerHistory, cfg }) {
  return {
    search_shop_listings: async ({ query, limit = 8 }) => {
      if (!cfg.listingsMirrorEnabled) {
        return { error: "Listings mirror is disabled.", note: "Reply without referencing specific listings." };
      }
      if (!searchListings) {
        return { error: "Listings catalog module is not available.", note: "Reply without referencing specific listings." };
      }
      try {
        const result = await searchListings(String(query || ""), limit);
        if (result && result.error) return result;
        return { query, matches: result.matches || [], count: result.count || 0, totalScored: result.totalScored || 0 };
      } catch (e) {
        return { error: `search_shop_listings failed: ${e.message}`, query };
      }
    },

    resolveQuote: async ({ family, selectedCodes, quantity, wantsRush = false, includeShippingSummary = false }) => {
      if (!resolveQuote) {
        const unavailResult = {
          success: false, reason: "RESOLVER_UNAVAILABLE",
          customerMessage: "Our pricing system is temporarily unavailable. A team member will follow up with your quote shortly."
        };
        await writeAudit({
          threadId, eventType: "option_quote_failed", actor: "system:salesAgent",
          payload: { family, selectedCodes, quantity, wantsRush, reason: "RESOLVER_UNAVAILABLE" },
          outcome: "failure", ruleViolations: ["RESOLVER_UNAVAILABLE"]
        });
        return unavailResult;
      }
      try {
        const result = await resolveQuote({
          family, selectedCodes, quantity,
          wantsRush: wantsRush === true,
          includeShippingSummary: includeShippingSummary === true
        });
        salesCtx._lastResolverResult = result;
        await writeAudit({
          threadId,
          eventType: result && result.success ? "option_quote_resolved" : "option_quote_failed",
          actor: "system:salesAgent",
          payload: {
            family, selectedCodes: Array.isArray(selectedCodes) ? selectedCodes : [],
            quantity, wantsRush: wantsRush === true,
            includeShippingSummary: includeShippingSummary === true,
            total: result && typeof result.total === "number" ? result.total : null,
            reason: result && result.reason ? result.reason : null,
            escalations: result && Array.isArray(result.escalations) ? result.escalations : []
          },
          outcome: result && result.success ? "success" : "blocked",
          ruleViolations: result && result.success ? [] : [result && result.reason ? result.reason : "QUOTE_FAILED"]
        });
        return result;
      } catch (e) {
        await writeAudit({
          threadId, eventType: "option_quote_failed", actor: "system:salesAgent",
          payload: { family, selectedCodes: Array.isArray(selectedCodes) ? selectedCodes : [], quantity,
                     wantsRush: wantsRush === true, includeShippingSummary: includeShippingSummary === true,
                     reason: "RESOLVER_ERROR", error: e.message },
          outcome: "failure", ruleViolations: ["RESOLVER_ERROR"]
        });
        return { success: false, reason: "RESOLVER_ERROR", error: e.message };
      }
    },

    request_photo: async ({ reason }) => ({ ok: true, reason: String(reason || "") }),
    request_dimensions: async ({ what }) => ({ ok: true, what: String(what || "") }),

    lookup_listing_by_url: async ({ url }) => {
      if (!lookupListingByUrl) {
        return { found: false, reason: "LOOKUP_UNAVAILABLE", error: "Listing lookup module is not available." };
      }
      try { return await lookupListingByUrl({ url, threadId }); }
      catch (e) { return { found: false, reason: "LOOKUP_ERROR", error: e.message }; }
    },

    get_option_sheet: async ({ family }) => {
      const fam = String(family || "").toLowerCase().trim();
      if (!["huggie", "necklace", "stud"].includes(fam)) {
        return { success: false, reason: "UNKNOWN_FAMILY", family };
      }
      if (!loadOptionSheet) return { success: false, reason: "OPTION_SHEET_UNAVAILABLE" };
      try {
        const sheet = await loadOptionSheet(fam);
        if (!sheet || sheet.active === false) return { success: false, reason: "UNKNOWN_FAMILY", family: fam };
        return { success: true, sheet: compactOptionSheetForAi(sheet) };
      } catch (e) { return { success: false, reason: "OPTION_SHEET_ERROR", error: e.message }; }
    },

    get_collateral: async ({ category, kind, keywords }) => {
      if (!searchCollateral) {
        return { matches: [], note: "Collateral retrieval is not yet deployed." };
      }
      try {
        const result = await searchCollateral({
          category: category ? String(category) : undefined,
          kind    : kind     ? String(kind)     : undefined,
          keywords: Array.isArray(keywords) ? keywords : undefined,
          limit   : 5
        });
        if (result && Array.isArray(result.matches)) {
          result.matches = result.matches.filter(m => m && isCustomerVisibleUrl(m.url));
          result.count = result.matches.length;
        }
        return result;
      } catch (e) { return { matches: [], error: e.message }; }
    }
  };
}

const TOOL_SPEC_SEARCH_LISTINGS = {
  name: "search_shop_listings",
  description: "Search the shop's active Etsy catalog.",
  input_schema: { type: "object", properties: { query: { type: "string" }, limit: { type: "integer", minimum: 1, maximum: 25 } }, required: ["query"] }
};

const TOOL_SPEC_REQUEST_PHOTO = {
  name: "request_photo",
  description: "Signal that you need a photo from the customer. Your reply text should ask naturally.",
  input_schema: { type: "object", properties: { reason: { type: "string" } }, required: ["reason"] }
};

const TOOL_SPEC_REQUEST_DIMENSIONS = {
  name: "request_dimensions",
  description: "Signal that you need specific measurements. Your reply text should ask naturally.",
  input_schema: { type: "object", properties: { what: { type: "string" } }, required: ["what"] }
};

const TOOL_SPEC_RESOLVE_QUOTE = {
  name: "resolveQuote",
  description: "Compute the EXACT price for a Custom Brites custom order using the line-sheet option resolver. This is the ONLY way to quote a price.",
  input_schema: {
    type: "object",
    properties: {
      family: { type: "string", enum: ["huggie", "necklace", "stud"] },
      selectedCodes: { type: "array", items: { type: "string" } },
      quantity: { type: "integer", minimum: 1 },
      wantsRush: { type: "boolean" },
      includeShippingSummary: { type: "boolean" }
    },
    required: ["family", "selectedCodes", "quantity"]
  }
};

const TOOL_SPEC_GET_OPTION_SHEET = {
  name: "get_option_sheet",
  description: "Fetch the current option sheet for a product family.",
  input_schema: { type: "object", properties: { family: { type: "string", enum: ["huggie", "necklace", "stud"] } }, required: ["family"] }
};

const TOOL_SPEC_GET_COLLATERAL = {
  name: "get_collateral",
  description: "Retrieve operator-curated collateral by category. NOTE: care/sizing collateral are AUTO-ATTACHED as image files when those topics come up — you do not call this tool for them.",
  input_schema: {
    type: "object",
    properties: {
      category: { type: "string" },
      kind: { type: "string", enum: ["line_sheet", "product_card", "lookbook", "image_set", "terms"] },
      keywords: { type: "array", items: { type: "string" } }
    },
    required: ["category"]
  }
};

const TOOL_SPEC_LOOKUP_LISTING_BY_URL = {
  name: "lookup_listing_by_url",
  description: "Fetch full data for a specific Etsy listing when the customer pasted a URL.",
  input_schema: { type: "object", properties: { url: { type: "string" } }, required: ["url"] }
};

function buildToolSpecs() {
  return [
    TOOL_SPEC_SEARCH_LISTINGS,
    TOOL_SPEC_GET_OPTION_SHEET,
    TOOL_SPEC_LOOKUP_LISTING_BY_URL,
    TOOL_SPEC_REQUEST_PHOTO,
    TOOL_SPEC_REQUEST_DIMENSIONS,
    TOOL_SPEC_RESOLVE_QUOTE,
    TOOL_SPEC_GET_COLLATERAL
  ];
}

function tryParseJson(rawText) {
  if (!rawText || typeof rawText !== "string") return null;
  let text = rawText.trim();
  if (text.startsWith("```")) {
    text = text.replace(/^```(?:json|JSON)?\s*\n?/i, "").replace(/\s*```\s*$/, "");
  }
  try { return JSON.parse(text); } catch {}
  const start = text.indexOf("{");
  if (start === -1) return null;
  let depth = 0, arrDepth = 0, inStr = false, esc = false, balancedEnd = -1;
  for (let i = start; i < text.length; i++) {
    const c = text[i];
    if (esc) { esc = false; continue; }
    if (c === "\\") { esc = true; continue; }
    if (c === '"') { inStr = !inStr; continue; }
    if (inStr) continue;
    if      (c === "{") depth++;
    else if (c === "}") { depth--; if (depth === 0 && arrDepth === 0) { balancedEnd = i; break; } }
    else if (c === "[") arrDepth++;
    else if (c === "]") arrDepth--;
  }
  if (balancedEnd !== -1) {
    const slice = text.slice(start, balancedEnd + 1);
    try { return JSON.parse(slice); } catch {}
    try { return JSON.parse(repairTrailingCommas(slice)); } catch {}
  }
  let repair = text.slice(start);
  if (inStr) repair += '"';
  while (arrDepth-- > 0) repair += "]";
  while (depth--    > 0) repair += "}";
  try { return JSON.parse(repair); } catch {}
  try { return JSON.parse(repairTrailingCommas(repair)); } catch {}
  return null;
}

function repairTrailingCommas(s) {
  let out = "";
  let inStr = false, esc = false;
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (esc) { out += c; esc = false; continue; }
    if (c === "\\") { out += c; esc = true; continue; }
    if (c === '"') { inStr = !inStr; out += c; continue; }
    if (inStr) { out += c; continue; }
    if (c === ",") {
      let j = i + 1;
      while (j < s.length && /\s/.test(s[j])) j++;
      if (j < s.length && (s[j] === "}" || s[j] === "]")) continue;
    }
    out += c;
  }
  return out;
}

async function validateQuotedPriceIfPresent({ threadId, parsed, salesCtx }) {
  const quotedTotalUsd = (typeof parsed.quoted_total_usd === "number")
    ? parsed.quoted_total_usd
    : (parsed.draft_custom_order_listing && typeof parsed.draft_custom_order_listing.totalUsd === "number")
      ? parsed.draft_custom_order_listing.totalUsd
      : null;
  if (typeof quotedTotalUsd !== "number" || quotedTotalUsd <= 0) return { skip: true };

  let family, selectedCodes, quantity, wantsRush = false;
  const lrr = salesCtx._lastResolverResult;
  if (lrr && lrr.success) {
    family        = lrr.family;
    quantity      = lrr.quantity;
    selectedCodes = (lrr.lineItems || []).map(li => li.code).filter(Boolean);
    wantsRush     = lrr.rush !== null && lrr.rush !== undefined;
  }
  if ((!family || !Array.isArray(selectedCodes) || selectedCodes.length === 0) && parsed.items_quoted) {
    family        = parsed.items_quoted.family || family;
    selectedCodes = parsed.items_quoted.selectedCodes || selectedCodes;
    quantity      = parsed.items_quoted.quantity      || quantity;
    if (typeof parsed.items_quoted.wantsRush === "boolean") wantsRush = parsed.items_quoted.wantsRush;
  }
  if (!family || !Array.isArray(selectedCodes) || selectedCodes.length === 0 || !quantity) {
    return { valid: false, reason: "VALIDATE_ITEMS_UNKNOWN", detail: "Could not determine family/selectedCodes/quantity." };
  }
  if (!resolveQuote) {
    return { valid: false, reason: "RESOLVER_UNAVAILABLE", detail: "Option resolver not loaded." };
  }
  const fresh = await resolveQuote({ family, selectedCodes, quantity, wantsRush });
  if (!fresh.success) {
    return { valid: false, reason: "RESOLVER_REJECTED_AT_VALIDATION", resolverReason: fresh.reason, resolverDetail: fresh };
  }
  if (Array.isArray(fresh.escalations) && fresh.escalations.length > 0) {
    return { valid: false, reason: "QUOTE_ROW_NOT_RESOLVED", escalations: fresh.escalations, resolverTotal: fresh.total };
  }
  const drift = Math.abs(quotedTotalUsd - fresh.total);
  if (drift > 0.01) {
    return { valid: false, reason: "QUOTED_PRICE_MISMATCH", quotedPrice: quotedTotalUsd, resolverTotal: fresh.total, drift };
  }
  return { valid: true, family, selectedCodes, quantity, resolverTotal: fresh.total };
}

function buildInitialMessages({ contextSummary, latestInboundText, referenceAttachments }) {
  const safeRefAttachments = compactAttachmentList(referenceAttachments);
  const userContent = [
    {
      type: "text",
      text: [
        "═══ Sales context ═══",
        JSON.stringify(contextSummary, null, 2),
        "",
        "═══ Latest customer message ═══",
        String(latestInboundText || "(no text)").slice(0, 6000)
      ].join("\n")
    }
  ];
  if (safeRefAttachments.length) {
    userContent.push({
      type: "text",
      text: "Customer-provided reference attachments retained from this thread:\n" +
            safeRefAttachments.map((a, i) => String(i + 1) + ". " + (a.filename || a.type || "attachment") + ": " + a.url).join("\n")
    });
  }
  let imgCount = 0;
  for (const att of safeRefAttachments) {
    if (imgCount >= 4) break;
    if (att && isCustomerVisibleUrl(att.url)) {
      userContent.push({ type: "image", source: { type: "url", url: att.url } });
      imgCount++;
    }
  }
  return [{ role: "user", content: userContent }];
}

// ─── Main handler ──────────────────────────────────────────────────────

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }
  if (event.httpMethod !== "POST") {
    return { statusCode: 405, headers: CORS, body: JSON.stringify({ error: "Method Not Allowed" }) };
  }

  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: "Invalid JSON body" }) }; }

  const {
    threadId,
    latestInboundText,
    latestInboundAttachments = [],
    threadReferenceAttachments = [],
    referencedListings       = [],
    customerHistory          = {},
    intentClassification     = null,
    intentConfidence         = null,
    employeeName             = "system:auto-pipeline"
  } = body;

  if (!threadId) {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: "threadId required" }) };
  }

  const tStart = Date.now();
  const cfg = await getConfig();

  if (!cfg.salesModeEnabled) {
    return { statusCode: 403, headers: CORS, body: JSON.stringify({ error: "Sales mode is disabled in config" }) };
  }
  if (cfg.salesPilotThreadIds.length > 0 && !cfg.salesPilotThreadIds.includes(threadId)) {
    return { statusCode: 403, headers: CORS,
             body: JSON.stringify({ error: "Thread not in pilot allow-list", pilotListLength: cfg.salesPilotThreadIds.length }) };
  }

  try {
    const salesCtx = await loadOrInitSalesContext(threadId);

    const referenceAttachments = mergeAttachments(
      Array.isArray(latestInboundAttachments)
        ? latestInboundAttachments.map(a => ({ ...a, source: "latest_inbound" })) : [],
      Array.isArray(threadReferenceAttachments)
        ? threadReferenceAttachments.map(a => ({ ...a, source: "thread_history" })) : [],
      Array.isArray(salesCtx.referenceAttachments)
        ? salesCtx.referenceAttachments.map(a => ({ ...a, source: a.source || "sales_context" })) : []
    );

    const recentThreadMessages = await loadRecentThreadMessages(threadId, 12);
    const priorOutboundTexts = recentOutboundTextsFromMessages(recentThreadMessages);

    const recommendedCollateral = await prefetchLineSheetCollateral({ latestInboundText, salesCtx });

    // v2.8.3 — Deterministic care/sizing prefetch. Result drives the
    // auto-set of attach_* flags after the AI replies (see below).
    // URLs are NOT surfaced to the AI; the attachment system writes the
    // actual image files to the draft, same as line sheets.
    const prefetchedCareCollateralResult   = await prefetchCareCollateral({ latestInboundText, recentThreadMessages });
    const prefetchedSizingCollateralResult = await prefetchSizingCollateral({ latestInboundText, recentThreadMessages });
    const prefetchedCareCollateral   = prefetchedCareCollateralResult.matches || [];
    const prefetchedSizingCollateral = prefetchedSizingCollateralResult.matches || [];

    const promptLoad = await loadSalesPrompt();
    if (!promptLoad.ok) {
      await writeAudit({
        threadId, eventType: "sales_agent_prompt_unavailable",
        payload: { error: promptLoad.error }, outcome: "failure"
      });
      return { statusCode: 503, headers: CORS,
               body: JSON.stringify({ error: promptLoad.error, errorCode: "SALES_PROMPT_NOT_AVAILABLE" }) };
    }

    const toolSpecs     = buildToolSpecs();
    const toolExecutors = buildToolExecutors({ threadId, salesCtx, customerHistory, cfg });

    const compactReferencedListings = (Array.isArray(referencedListings) ? referencedListings : [])
      .map(r => {
        if (!r || !r.found) {
          return { url: r && r.url ? r.url : null, found: false, reason: (r && r.reason) || "UNKNOWN" };
        }
        const li = r.listing || {};
        return {
          url           : r.url,
          found         : true,
          listingId     : r.listingId,
          source        : r.source,
          notOurShop    : !!r.notOurShop,
          isActive      : !!r.isActive,
          title         : li.title || null,
          priceUsd      : li.priceUsd ?? null,
          state         : li.state || null,
          quantity      : li.quantity ?? null,
          listingUrl    : li.listingUrl || null,
          primaryImageUrl : li.primaryImageUrl || null,
          descriptionShort: li.descriptionShort ? li.descriptionShort.slice(0, 400) : null,
          tags          : Array.isArray(li.tags) ? li.tags.slice(0, 8) : [],
          materials     : Array.isArray(li.materials) ? li.materials.slice(0, 6) : [],
          isCustomizable: li.isCustomizable
        };
      });

    const contextSummary = {
      accumulatedSpec    : salesCtx.accumulatedSpec || {},
      quoteHistory       : (salesCtx.quoteHistory || []).slice(-3),
      lastResolverResult : (salesCtx._lastResolverResult && salesCtx._lastResolverResult.success)
                            ? {
                                family   : salesCtx._lastResolverResult.family,
                                total    : salesCtx._lastResolverResult.total,
                                quantity : salesCtx._lastResolverResult.quantity,
                                bulkTier : salesCtx._lastResolverResult.bulkTier,
                                escalations: salesCtx._lastResolverResult.escalations || []
                              }
                            : null,
      referencedListings : compactReferencedListings,
      recentThreadMessages,
      previousOutboundReplies: priorOutboundTexts.slice(-3),
      referenceAttachments: compactAttachmentList(referenceAttachments),
      hasReferenceImage: referenceAttachments.length > 0,
      recommendedCollateral,
      // v2.8.3 — Care/sizing prefetched URLs are intentionally NOT in
      // the AI context. They're auto-attached as image files via the
      // attach_* flag mechanism. The AI references the attachment
      // naturally without URLs (same UX as line sheets).
      customerHistory    : {
        isRepeat          : !!(customerHistory && customerHistory.isRepeat),
        orderCount        : (customerHistory && customerHistory.orderCount) || 0,
        lifetimeValueUsd  : (customerHistory && customerHistory.lifetimeValueUsd) || 0
      },
      intentClassification, intentConfidence
    };
    const initialMessages = buildInitialMessages({ contextSummary, latestInboundText, referenceAttachments });

    let loopResult;
    try {
      loopResult = await runToolLoop({
        model         : AI_MODEL,
        maxTokens     : AI_MAX_TOKENS,
        system        : promptLoad.prompt,
        initialMessages,
        toolSpecs,
        toolExecutors,
        toolContext   : { threadId, salesCtx },
        effort        : AI_EFFORT,
        useThinking   : true,
        maxIterations : MAX_TOOL_ITERATIONS
      });
    } catch (e) {
      await writeAudit({ threadId, eventType: "sales_agent_call_failed",
                         payload: { error: e.message }, outcome: "failure" });
      return { statusCode: 502, headers: CORS, body: JSON.stringify({ error: `AI call failed: ${e.message}` }) };
    }

    const finalText = (loopResult.finalResponse && Array.isArray(loopResult.finalResponse.content)
      ? loopResult.finalResponse.content : []
    ).filter(b => b && b.type === "text").map(b => b.text || "").join("\n").trim();

    const parsed = tryParseJson(finalText);
    if (!parsed) {
      await writeAudit({
        threadId, eventType: "sales_agent_unparseable_output",
        payload: { rawPreview: finalText.slice(0, 500),
                   toolCalls: (loopResult.toolCalls || []).map(t => t.name) },
        outcome: "failure"
      });
      await db.collection(THREADS_COLL).doc(threadId).set({
        status: "pending_human_review",
        lastSalesAgentBlockReason: "unparseable_output",
        updatedAt: FV.serverTimestamp()
      }, { merge: true });
      return { statusCode: 500, headers: CORS,
               body: JSON.stringify({ error: "AI output not parseable JSON",
                                       rawPreview: finalText.slice(0, 500), escalated: true }) };
    }

    if (typeof parsed.reply === "string") {
      parsed.reply = applySalesReplyGuard(parsed.reply, priorOutboundTexts);
    }

    // ─── v2.8.3 — Deterministic care/sizing collateral attachment ──
    // When prefetch detected the topic, auto-set the matching attach_*
    // flag. The attachment construction code below resolves each flag
    // to a collateral entry via findAttachableForKind and writes the
    // image to draft.attachments — same path as line sheets.
    if (prefetchedCareCollateralResult && prefetchedCareCollateralResult.topicDetected) {
      if (parsed.attach_care_instructions !== true) {
        parsed.attach_care_instructions = true;
        parsed._attachCareInstructionsAutoSet = true;
      }
      if (parsed.attach_metal_comparison !== true) {
        parsed.attach_metal_comparison = true;
        parsed._attachMetalComparisonAutoSet = true;
      }
    }
    if (prefetchedSizingCollateralResult && prefetchedSizingCollateralResult.topicDetected) {
      const sizingHaystack = (
        (latestInboundText || "") + " " +
        (Array.isArray(recentThreadMessages)
          ? recentThreadMessages.slice(-3).map(m => (m && m.text) || "").join(" ")
          : "")
      ).toLowerCase();
      const isNecklaceSizing =
        /\b(necklace|chain length|chain size|chain inches|pendant|collarbone|drop length|bustline|on body|lies on|sits at|falls at)\b/.test(sizingHaystack) ||
        /\b(16|18|20|22|24)[\s-]?(?:inch|in|")/.test(sizingHaystack);
      const isBraceletSizing = /\b(wrist|bracelet)\b/.test(sizingHaystack);
      if (isNecklaceSizing && parsed.attach_fit_reference !== true) {
        parsed.attach_fit_reference = true;
        parsed._attachFitReferenceAutoSet = true;
      }
      if (isBraceletSizing && parsed.attach_bracelet_sizing !== true) {
        parsed.attach_bracelet_sizing = true;
        parsed._attachBraceletSizingAutoSet = true;
      }
      if (!isNecklaceSizing && !isBraceletSizing) {
        if (parsed.attach_fit_reference !== true) {
          parsed.attach_fit_reference = true;
          parsed._attachFitReferenceAutoSet = true;
        }
        if (parsed.attach_bracelet_sizing !== true) {
          parsed.attach_bracelet_sizing = true;
          parsed._attachBraceletSizingAutoSet = true;
        }
      }
    }

    // ── Text-pattern fallback for line-sheet attachment (parity with -background) ──
    if (parsed.attach_line_sheet !== true && typeof parsed.reply === "string") {
      const r = parsed.reply.toLowerCase();
      const lineSheetPromisePatterns = [
        /\bhere'?s\s+(?:our\s+)?(?:\w+\s+)?(?:charm\s+)?line\s+sheet\b/i,
        /\battach(?:ed|ing)?\s+(?:is\s+)?(?:our\s+)?(?:\w+\s+)?(?:charm\s+)?line\s+sheet\b/i,
        /\bsend(?:ing)?\s+(?:over\s+)?(?:our\s+)?(?:\w+\s+)?(?:charm\s+)?line\s+sheet\b/i,
        /\bsee\s+the\s+(?:attached\s+)?line\s+sheet\b/i,
        /\btake\s+a\s+look\s+at\s+(?:the\s+)?(?:attached\s+)?(?:\w+\s+)?line\s+sheet\b/i,
        /\bcheck\s+out\s+(?:the\s+)?(?:attached\s+)?line\s+sheet\b/i
      ];
      if (lineSheetPromisePatterns.some(rx => rx.test(r))) {
        parsed.attach_line_sheet = true;
        parsed._attachLineSheetInferredFromText = true;
      }
    }

    const quoteValidation = await validateQuotedPriceIfPresent({ threadId, parsed, salesCtx });
    if (quoteValidation && quoteValidation.valid === false && !quoteValidation.skip) {
      await writeAudit({
        threadId, eventType: "sales_agent_quote_invalid",
        payload: {
          reason         : quoteValidation.reason,
          rejectedQuote  : parsed.quoted_total_usd
                         || (parsed.draft_custom_order_listing && parsed.draft_custom_order_listing.totalUsd),
          violations     : quoteValidation.violations || null
        },
        outcome: "blocked", ruleViolations: [quoteValidation.reason]
      });
      await db.collection(THREADS_COLL).doc(threadId).set({
        status: "pending_human_review",
        lastSalesAgentBlockReason: quoteValidation.reason,
        updatedAt: FV.serverTimestamp()
      }, { merge: true });
      await salesCtx._ref.set({
        lastSalesAgentBlockReason: quoteValidation.reason,
        lastTurnAt: FV.serverTimestamp()
      }, { merge: true });
      return { statusCode: 422, headers: CORS,
               body: JSON.stringify({
                 error: "Quote validation failed — escalated to human review",
                 reason: quoteValidation.reason, escalated: true
               }) };
    }

    const rawAdvance = parsed.advance_stage || null;
    const wantsHumanReview = rawAdvance === "human_review" || !!parsed.ready_for_human_approval;
    const isAbandoned = rawAdvance === "abandoned";

    const draftId = "draft_" + threadId;

    const isNeedsReviewHandoff =
      (typeof parsed.needs_review_synopsis === "string" && parsed.needs_review_synopsis.trim().length > 50)
      || parsed.advance_stage === "human_review";

    const customerFacingReply = (typeof parsed.reply === "string" && parsed.reply.trim())
      ? parsed.reply.trim()
      : "(The AI did not produce a reply for this turn — operator review needed.)";

    const replyText = isNeedsReviewHandoff && typeof parsed.needs_review_synopsis === "string" && parsed.needs_review_synopsis.trim().length > 50
      ? parsed.needs_review_synopsis.trim()
      : customerFacingReply;

    const aiConfidence = (typeof parsed.confidence === "number" && parsed.confidence >= 0 && parsed.confidence <= 1)
      ? parsed.confidence : 0.5;

    // ─── v2.8.3 — Multi-kind collateral attachment construction ───
    //
    // For each kind whose attach_* flag was set (by AI or by auto-set
    // above), look up the matching collateral and build an image
    // attachment record. Operator UI renders these as chips above the
    // staff reply textarea, and the customer sees the images inline in
    // their Etsy conversation, same as line sheets.
    const COLLATERAL_KINDS_REQUESTED = [
      { flag: "attach_line_sheet",       kind: "line_sheet",       label: "line sheet" },
      { flag: "attach_fit_reference",    kind: "fit_reference",    label: "fit reference"  },
      { flag: "attach_metal_comparison", kind: "metal_comparison", label: "metal comparison" },
      { flag: "attach_care_instructions",kind: "care_instructions",label: "care instructions" },
      { flag: "attach_bracelet_sizing",  kind: "bracelet_sizing",  label: "bracelet sizing" }
    ];

    let attachmentsToWrite = [];
    let collateralAttachInfo = [];
    let lineSheetAttachInfo = null;

    async function findAttachableForKind(kind) {
      // 1. Strict: check recommendedCollateral first (family-relevant subset)
      const recList = Array.isArray(recommendedCollateral) ? recommendedCollateral : [];
      const recHit = recList.find(c => c && c.kind === kind && c.storagePath && c.uploadedContentType);
      if (recHit) return recHit;

      // 2. Strict: search all active collateral with exact kind match
      try {
        const result = await searchCollateral ? searchCollateral({ kind, limit: 5 }) : null;
        const matches = (result && Array.isArray(result.matches)) ? result.matches : [];
        const exactKindHit = matches.find(c => c && c.kind === kind && c.storagePath && c.uploadedContentType);
        if (exactKindHit) return exactKindHit;
      } catch {}

      // 3. Name-based fallback: operator entries with kind="line_sheet"
      // (the default) but recognizable by name. Lets the system work
      // with existing data without requiring kind metadata cleanup.
      const KIND_NAME_KEYWORDS = {
        care_instructions: ["care", "aftercare", "cleaning", "polish"],
        metal_comparison : ["metal comparison", "gold filled", "gold plated", "filled vs", "plated vs", "metals comparison"],
        fit_reference    : ["fit reference", "chain length on body", "necklace fit", "on body"],
        bracelet_sizing  : ["bracelet sizing", "wrist sizing", "wrist chart", "bracelet size"],
        line_sheet       : []
      };
      const nameKeywords = KIND_NAME_KEYWORDS[kind] || [];
      if (nameKeywords.length > 0 && searchCollateral) {
        try {
          const result = await searchCollateral({ limit: 50 });
          const matches = (result && Array.isArray(result.matches)) ? result.matches : [];
          const nameHit = matches.find(c => {
            if (!c || !c.storagePath || !c.uploadedContentType) return false;
            const name = String(c.name || "").toLowerCase();
            return nameKeywords.some(kw => name.includes(kw));
          });
          if (nameHit) return nameHit;
        } catch {}
      }
      return null;
    }

    for (const { flag, kind, label } of COLLATERAL_KINDS_REQUESTED) {
      if (parsed[flag] !== true) continue;
      const hit = await findAttachableForKind(kind);
      if (hit) {
        const synthId = "att_collateral_" + (hit.id || Math.random().toString(36).slice(2, 10));
        const ct = hit.uploadedContentType;
        attachmentsToWrite.push({
          attachmentId : synthId,
          type         : "image",
          storagePath  : hit.storagePath,
          proxyUrl     : "/.netlify/functions/etsyMailImage?path=" + encodeURIComponent(hit.storagePath),
          contentType  : ct,
          bytes        : typeof hit.uploadedSizeBytes === "number" ? hit.uploadedSizeBytes : null,
          filename     : hit.uploadedFilename || ((hit.name || kind) + "." + ((ct.split("/")[1] || "png"))),
          source       : "collateral",
          collateralId : hit.id || null,
          collateralName : hit.name || null,
          collateralKind : hit.kind || kind
        });
        const info = { kind, label, decided: true, attached: true,
                       collateralId: hit.id || null, collateralName: hit.name || null };
        collateralAttachInfo.push(info);
        if (kind === "line_sheet") lineSheetAttachInfo = info;
      } else {
        const info = { kind, label, decided: true, attached: false, reason: "no_active_collateral_for_kind" };
        collateralAttachInfo.push(info);
        if (kind === "line_sheet") lineSheetAttachInfo = info;
        console.warn(`salesAgent: ${flag}=true but no attachable ${label} collateral for thread ${threadId}`);
      }
    }
    // ─────────────────────────────────────────────────────────────

    await db.collection(DRAFTS_COLL).doc(draftId).set({
      draftId,
      threadId,
      text                  : replyText,
      // v2.8.3 — was hardcoded []; now real collateral attachments.
      attachments           : attachmentsToWrite,
      // v0.9.18 parity — mirror attachments into draftAttachments so
      // the operator UI's hydrateComposerFromDraft sees the chip above
      // the staff reply textarea, same as the line-sheet path.
      draftAttachments      : attachmentsToWrite,
      referenceAttachments  : compactAttachmentList(referenceAttachments),
      status                : "draft",
      generatedByAI         : true,
      generatedBySalesAgent : true,
      aiConfidence,
      aiReasoning           : String(parsed.reasoning || "").slice(0, 1000),
      aiNeedsPhoto          : !!parsed.needs_photo,
      aiMissingInputs       : Array.isArray(parsed.missing_inputs) ? parsed.missing_inputs.slice(0, 12) : [],
      aiCollateralReferenced: Array.isArray(parsed.collateral_referenced) ? parsed.collateral_referenced : [],
      aiRecommendedCollateral: recommendedCollateral,
      // v2.8.3 — Diagnostics that make silent failures impossible.
      aiPrefetchedCareCollateral: prefetchedCareCollateral,
      aiCareCollateralDiagnostic: {
        topicDetected         : prefetchedCareCollateralResult.topicDetected,
        rawCount              : prefetchedCareCollateralResult.rawCount,
        filteredOutPlaceholder: prefetchedCareCollateralResult.filteredOutPlaceholder,
        reason                : prefetchedCareCollateralResult.reason
      },
      aiPrefetchedSizingCollateral: prefetchedSizingCollateral,
      aiSizingCollateralDiagnostic: {
        topicDetected         : prefetchedSizingCollateralResult.topicDetected,
        rawCount              : prefetchedSizingCollateralResult.rawCount,
        filteredOutPlaceholder: prefetchedSizingCollateralResult.filteredOutPlaceholder,
        reason                : prefetchedSizingCollateralResult.reason
      },
      aiCollateralAttachInfo: collateralAttachInfo,
      aiAutoSetFlags: {
        attach_care_instructions: parsed._attachCareInstructionsAutoSet === true,
        attach_metal_comparison : parsed._attachMetalComparisonAutoSet === true,
        attach_fit_reference    : parsed._attachFitReferenceAutoSet === true,
        attach_bracelet_sizing  : parsed._attachBraceletSizingAutoSet === true
      },
      readyForHumanApproval : !!parsed.ready_for_human_approval,
      draftCustomOrderListing: parsed.draft_custom_order_listing || null,
      isNeedsReviewHandoff       : isNeedsReviewHandoff,
      needsReviewSynopsis        : isNeedsReviewHandoff ? (parsed.needs_review_synopsis || null) : null,
      customerFacingReplyDraft   : isNeedsReviewHandoff ? customerFacingReply : null,
      resolverResult        : (salesCtx._lastResolverResult && salesCtx._lastResolverResult.success)
                              ? salesCtx._lastResolverResult : null,
      createdBy             : "sales-agent",
      createdAt             : FV.serverTimestamp(),
      updatedAt             : FV.serverTimestamp(),
      sendSessionId         : null,
      sendClaimedAt         : null,
      sendHeartbeatAt       : null,
      sendAttempts          : 0,
      sendError             : null,
      sentAt                : null
    }, { merge: true });

    const ctxUpdates = {
      accumulatedSpec : { ...(salesCtx.accumulatedSpec || {}), ...(parsed.extracted_spec || {}) },
      referenceAttachments: compactAttachmentList(referenceAttachments),
      lastCustomerFacingReply: customerFacingReply,
      lastTurnAt      : FV.serverTimestamp(),
      lastSalesAgentBlockReason: null
    };
    if (salesCtx._lastResolverResult) ctxUpdates._lastResolverResult = salesCtx._lastResolverResult;
    const quotedTotal = (typeof parsed.quoted_total_usd === "number") ? parsed.quoted_total_usd
                     : (parsed.draft_custom_order_listing && typeof parsed.draft_custom_order_listing.totalUsd === "number")
                       ? parsed.draft_custom_order_listing.totalUsd : null;
    if (typeof quotedTotal === "number") {
      const lrr = salesCtx._lastResolverResult;
      ctxUpdates.quoteHistory = FV.arrayUnion({
        at: Date.now(), total: quotedTotal, validated: true,
        resolverResult: (lrr && lrr.success) ? {
          family: lrr.family, quantity: lrr.quantity, total: lrr.total,
          perPieceAfterModifier: lrr.perPieceAfterModifier,
          subtotal: lrr.subtotal, discountAmount: lrr.discountAmount,
          bulkTier: lrr.bulkTier,
          escalations: lrr.escalations || [], rush: lrr.rush || null,
          shippingSummary: lrr.shippingSummary || null,
          lineItems: (lrr.lineItems || []).map(li => ({
            code: li.code, label: li.label,
            priceUsd: li.priceUsd ?? null,
            priceQuote: !!li.priceQuote, isModifier: !!li.isModifier
          }))
        } : null,
        hadEscalations: !!(lrr && Array.isArray(lrr.escalations) && lrr.escalations.length)
      });
      ctxUpdates.totalQuotedUsd = quotedTotal;
    }
    await salesCtx._ref.set(ctxUpdates, { merge: true });

    let threadStatus;
    if (wantsHumanReview)      threadStatus = "pending_human_review";
    else if (isAbandoned)      threadStatus = "sales_abandoned";
    else                       threadStatus = "sales_active";

    await db.collection(THREADS_COLL).doc(threadId).set({
      status        : threadStatus,
      aiDraftStatus : "ready",
      latestDraftId : draftId,
      aiConfidence,
      readyForHumanApproval: !!parsed.ready_for_human_approval,
      updatedAt     : FV.serverTimestamp()
    }, { merge: true });

    await writeAudit({
      threadId, draftId, eventType: "sales_agent_turn",
      payload: {
        threadStatus, confidence: aiConfidence,
        toolCalls: (loopResult.toolCalls || []).map(t => ({
          name: t.name, durationMs: t.durationMs, error: t.error || null
        })),
        quotedTotal: quotedTotal || null,
        readyForHumanApproval: !!parsed.ready_for_human_approval,
        rawAdvance,
        referenceAttachmentCount: referenceAttachments.length,
        collateralAttachInfo,
        usage: loopResult.usage || null
      }
    });

    return {
      statusCode: 200,
      headers: { ...CORS, "Content-Type": "application/json" },
      body: JSON.stringify({
        success                    : true,
        threadStatus,
        draftId,
        confidence                 : aiConfidence,
        ready_for_human_approval   : !!parsed.ready_for_human_approval,
        draft_custom_order_listing : parsed.draft_custom_order_listing || null,
        quoteValidation            : quoteValidation || null,
        toolCalls                  : (loopResult.toolCalls || []).map(t => ({ name: t.name, error: t.error || null })),
        durationMs                 : Date.now() - tStart
      })
    };

  } catch (err) {
    console.error("salesAgent unhandled error:", err);
    await writeAudit({
      threadId, eventType: "sales_agent_unhandled_error",
      payload: { error: err.message, stack: err.stack ? err.stack.slice(0, 1000) : null },
      outcome: "failure"
    });
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: err.message || String(err) }) };
  }
};
