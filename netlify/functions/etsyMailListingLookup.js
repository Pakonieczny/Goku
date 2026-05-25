/*  netlify/functions/etsyMailListingLookup.js
 *
 *  v2.3 — Etsy Listing Lookup. Recognizes pasted Etsy URLs in customer
 *  messages, extracts the listing ID, and fetches authoritative listing
 *  data from the Etsy API. Used by the sales agent to handle
 *  conversations like "I want something similar to your X listing".
 *
 *  ═══ WHY THIS EXISTS ═══════════════════════════════════════════════════
 *
 *  Before v2.3, the sales agent could only do keyword searches against
 *  the Step 1 catalog mirror. If a customer pasted a specific listing
 *  URL, the agent saw plain text — no extraction, no lookup. This left
 *  a real conversation gap when a customer says "I want to order this
 *  one I'm linking" with the URL right there.
 *
 *  ═══ ETSY-API-FIRST POLICY ═════════════════════════════════════════════
 *
 *  Per design decision: when a customer references a specific listing,
 *  we fetch FRESH from Etsy's API rather than reading the cache. The
 *  cache is on a 30-min sync; an actively-discussed listing whose price
 *  just changed deserves fresh data. The Step 1 catalog mirror is used
 *  as a FALLBACK only — when the API call fails (rate limit, network
 *  hiccup, transient error). Fail-soft, never fail-blank.
 *
 *  Hierarchy (in order):
 *    1. Etsy API: getListing(listingId) → fresh listing detail
 *    2. Etsy API: getListingImages(listingId) → primary image URL
 *    3. Step 1 cache: catalog mirror at EtsyMail_Listings/{listingId}
 *       (only if API path failed)
 *    4. Hard fail: { found:false, reason:"LISTING_NOT_FOUND_API_AND_CACHE" }
 *
 *  ═══ URL FORMATS RECOGNIZED ════════════════════════════════════════════
 *
 *    https://www.etsy.com/listing/1234567890                         (canonical)
 *    https://www.etsy.com/listing/1234567890/optional-slug-text      (with slug)
 *    https://www.etsy.com/listing/1234567890?ref=...                 (with query)
 *    https://etsy.com/listing/1234567890                             (no www)
 *    https://www.etsy.com/uk/listing/1234567890                      (locale prefix)
 *    https://www.etsy.com/de-en/listing/1234567890                   (locale prefix)
 *    https://www.etsy.com/your/listings/edit/1234567890              (seller's own edit URL)
 *    https://www.etsy.com/your/listings/1234567890                   (seller's own listings)
 *    https://etsy.me/aBc1Xy                                          (short link — see notes)
 *
 *  Short links (etsy.me/...) are recognized but NOT auto-resolved server-
 *  side. They need a 302-follow which adds latency + a privacy concern
 *  (Etsy's redirect endpoint logs every hit). When a short link is
 *  detected, we return { found:false, reason:"SHORT_LINK_UNRESOLVED" }
 *  with a hint for the agent to ask the customer for the full URL.
 *
 *  ═══ TWO OPS ═══════════════════════════════════════════════════════════
 *
 *  POST { op: "lookupByUrl", url, threadId? }
 *      AI tool path. Pass the URL the customer pasted (or any URL the
 *      AI suspects might be a listing). Returns the listing record on
 *      success or a structured failure reason.
 *
 *  POST { op: "lookupById", listingId, threadId? }
 *      Direct ID lookup. Used when the agent already has an ID (e.g.,
 *      the URL parser returned one and the agent wants to follow up
 *      on the same listing in a later turn).
 *
 *  ═══ EXPORTED HELPERS ══════════════════════════════════════════════════
 *
 *    extractListingIdFromUrl(text) → listingId | null | "SHORT_LINK"
 *      Pure synchronous parser, no I/O. Useful in pre-tool message
 *      preprocessing (Feature #2 in this build).
 *
 *    findEtsyUrlsInText(text) → [{ url, listingId | null | "SHORT_LINK" }]
 *      Scans free text for any Etsy URLs. Used by the auto-pipeline to
 *      proactively look up references before the agent loop runs.
 *
 *    lookupListingByUrl({ url, threadId? }) → { found, listing?, reason? }
 *      Direct-import path for sibling functions. Same semantics as the
 *      HTTP op:lookupByUrl handler.
 *
 *    lookupListingById({ listingId, threadId? }) → same shape
 *
 *  ═══ RETURN SHAPE ══════════════════════════════════════════════════════
 *
 *  Success:
 *    {
 *      found: true,
 *      listingId: "1234567890",
 *      source: "etsy_api" | "catalog_cache_fallback",
 *      listing: {
 *        listingId, title, descriptionShort, priceUsd, currencyCode,
 *        state, quantity, listingUrl, primaryImageUrl, imageUrls: [...],
 *        tags: [...], materials: [...], whoMade, whenMade, isCustomizable,
 *        shopId, processingMin, processingMax,
 *        fetchedAt: <ms epoch>
 *      },
 *      etsyApiCallSuccessful: <bool>,
 *      cacheFallbackUsed:    <bool>
 *    }
 *
 *  Failure (structured reasons in `reason` field, only when `found:false`):
 *    NOT_AN_ETSY_URL           — text didn't contain a recognizable URL
 *    SHORT_LINK_UNRESOLVED     — etsy.me/... short link, not auto-followed
 *    LISTING_ID_PARSE_FAILED   — URL matched the prefix but no numeric ID
 *    LISTING_NOT_FOUND_API_AND_CACHE — Etsy API returned 404 AND cache miss
 *    ETSY_API_ERROR            — non-404 error, cache also missed
 *    INVALID_INPUT             — caller passed empty/garbage
 *
 *  Soft signals on `found:true` results (NOT failure reasons —
 *  the listing data IS returned, the AI must just handle these states):
 *    notOurShop:true           — listing belongs to a different shop_id
 *    isActive:false            — listing state isn't "active"
 *                                (sold out, expired, draft, inactive)
 *
 *  ═══ ENV VARS ══════════════════════════════════════════════════════════
 *
 *    ETSYMAIL_EXTENSION_SECRET   gates this endpoint
 *    SHOP_ID                     used for shop ownership check
 *    (CLIENT_ID, CLIENT_SECRET   used by etsyFetch via existing helper)
 */

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const { getListing, getListingImages, getListingInventory, SHOP_ID } = require("./_etsyMailEtsy");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const LISTINGS_COLL = "EtsyMail_Listings";    // Step 1 catalog mirror
const AUDIT_COLL    = "EtsyMail_Audit";

// In-memory short-cache (60s) so back-to-back agent turns referencing
// the same listing don't hit Etsy's API twice. Keyed by listingId.
// Hard cap on size to prevent unbounded growth on warm instances —
// when the cap is hit, oldest entry is evicted.
const LOOKUP_CACHE_MS         = 60 * 1000;
const LOOKUP_CACHE_MAX_ENTRIES = 200;
const _lookupCache = new Map();   // listingId → { value, fetchedAt }
                                  //   Map preserves insertion order, so
                                  //   the first key is the oldest — used
                                  //   for FIFO eviction at cap.

/** Sweep stale entries from the in-process cache. Cheap O(N) walk over
 *  the Map. Called from lookupListingById's read path; piggybacks on
 *  every lookup, no separate timer needed. */
function _sweepStaleCacheEntries() {
  const now = Date.now();
  for (const [key, entry] of _lookupCache.entries()) {
    if (now - entry.fetchedAt >= LOOKUP_CACHE_MS) {
      _lookupCache.delete(key);
    }
  }
}

// ─── URL parsing (pure synchronous) ────────────────────────────────────

/** Match canonical listing URLs (with optional locale prefix, slug, query
 *  params, fragment). Captures the listing_id in group 1.
 *
 *  Pattern walkthrough:
 *    ^https?://             scheme
 *    (?:www\.)?             optional www
 *    etsy\.com/             host
 *    (?:[a-z]{2}(?:-[a-z]{2})?/)?  optional locale prefix like "uk/" or "de-en/"
 *    listing/               literal path
 *    (\d+)                  the listing ID we want
 *
 *  We don't anchor on the END of the URL — slugs/queries/fragments are
 *  all fine, we just want the ID. */
const RE_CANONICAL_LISTING = /^https?:\/\/(?:www\.)?etsy\.com\/(?:[a-z]{2}(?:-[a-z]{2})?\/)?listing\/(\d+)/i;

/** Seller's own internal URLs from Shop Manager.
 *    /your/listings/12345
 *    /your/listings/edit/12345
 *  Customers won't typically paste these, but operators sometimes do. */
const RE_SELLER_INTERNAL = /^https?:\/\/(?:www\.)?etsy\.com\/your\/listings(?:\/edit)?\/(\d+)/i;

/** Etsy short links (etsy.me/...). We can recognize but not resolve
 *  without a 302 follow, which we deliberately skip (latency + privacy).
 *  Returns the literal token "SHORT_LINK". */
const RE_SHORT_LINK = /^https?:\/\/etsy\.me\/[A-Za-z0-9]+/i;

/** Bare ID detector — for cases where the customer typed just the
 *  number ("listing 1234567890"). Conservative: 8-12 digits standalone,
 *  must be word-bounded to avoid matching phone numbers, dates, etc.
 *
 *  We do NOT use this in the URL extractor — it's too prone to false
 *  positives. Available as a separate exported helper if a future
 *  feature wants opt-in numeric matching. */

/** Given a URL string, return the listing ID, or null if not a
 *  listing URL, or the literal "SHORT_LINK" if it's an etsy.me link.
 *
 *  Pure function, no I/O. */
function extractListingIdFromUrl(urlOrText) {
  if (typeof urlOrText !== "string" || urlOrText.length < 12) return null;
  // v2.6 — accept bare-scheme URLs. The scanner regex now matches three
  // forms: https?://..., www.etsy.com/..., and etsy.com/.... Internally
  // we still need https?:// for RE_CANONICAL_LISTING etc, so normalize
  // before matching.
  const urlMatches = urlOrText.match(
    /(?:https?:\/\/[^\s<>"']+|www\.(?:etsy\.com|etsy\.me)\/[^\s<>"']+|(?:^|\s|[(\[])(?:etsy\.com|etsy\.me)\/[^\s<>"']+)/gi
  ) || [];
  // Allow callers to pass the bare URL too
  const rawCandidates = urlMatches.length > 0 ? urlMatches : [urlOrText.trim()];

  for (const raw of rawCandidates) {
    let candidate = raw.replace(/^[\s(\[]+/, "");
    if (!/^https?:\/\//i.test(candidate)) {
      candidate = "https://" + candidate.replace(/^\/\//, "");
    }
    let m = RE_CANONICAL_LISTING.exec(candidate);
    if (m && m[1]) return m[1];
    m = RE_SELLER_INTERNAL.exec(candidate);
    if (m && m[1]) return m[1];
    if (RE_SHORT_LINK.test(candidate)) return "SHORT_LINK";
  }
  return null;
}

/** Scan free text for ALL Etsy URLs. Returns an array of
 *    { url, listingId: "<id>" | "SHORT_LINK" | null }
 *  one entry per URL found (de-duplicated by listingId).
 *
 *  Used by the auto-pipeline preprocessor (Feature #2) to proactively
 *  look up listings before the agent loop runs. */
function findEtsyUrlsInText(text) {
  if (typeof text !== "string" || text.length < 12) return [];
  // v2.6 — Accept bare URLs without a scheme. Customers regularly paste
  // "www.etsy.com/listing/..." or "etsy.com/pl/listing/..." with no
  // https:// prefix. Previous regex required https?:// and missed them
  // entirely, causing the prefetch in draftReply v3.29 to detect zero
  // URLs and skip the listing fetch. Three alternates now:
  //   - Full scheme: https?://...
  //   - Bare www.:   www.etsy.com/... or www.etsy.me/...
  //   - Bare host:   etsy.com/... or etsy.me/... (word-boundary anchored)
  const urlMatches = text.match(
    /(?:https?:\/\/[^\s<>"']+|www\.(?:etsy\.com|etsy\.me)\/[^\s<>"']+|(?:^|\s|[(\[])(etsy\.com|etsy\.me)\/[^\s<>"']+)/gi
  ) || [];
  const seen = new Set();
  const results = [];
  for (const rawMatch of urlMatches) {
    // Strip any leading whitespace/bracket the bare-host alt picked up
    let candidate = rawMatch.replace(/^[\s(\[]+/, "");
    // Strip trailing punctuation
    candidate = candidate.replace(/[.,;:!?)\]]+$/, "");
    // Normalize scheme so extractListingIdFromUrl (which still requires
    // https?://) matches. Bare URLs become https://.
    let normalized = candidate;
    if (!/^https?:\/\//i.test(normalized)) {
      normalized = "https://" + normalized.replace(/^\/\//, "");
    }
    const id = extractListingIdFromUrl(normalized);
    if (id === null) continue;
    const dedupKey = id === "SHORT_LINK" ? `short:${normalized}` : id;
    if (seen.has(dedupKey)) continue;
    seen.add(dedupKey);
    results.push({ url: normalized, listingId: id });
  }
  return results;
}

// ─── Etsy price normalization ──────────────────────────────────────────

/** Etsy returns prices as { amount, divisor, currency_code }. Convert
 *  to a plain USD number. Returns null on unsupported currency or
 *  malformed shape. */
function priceToUsd(p) {
  if (!p || typeof p !== "object") return null;
  const amount = Number(p.amount), divisor = Number(p.divisor);
  if (!Number.isFinite(amount) || !Number.isFinite(divisor) || divisor === 0) return null;
  if (p.currency_code && p.currency_code !== "USD") return null;
  return Math.round((amount / divisor) * 100) / 100;
}

// ─── Audit ─────────────────────────────────────────────────────────────

async function writeAudit({ threadId = null, eventType, actor = "system:listingLookup",
                            payload = {}, outcome = "success", ruleViolations = [] }) {
  try {
    await db.collection(AUDIT_COLL).add({
      threadId, draftId: null, eventType, actor, payload,
      createdAt: FV.serverTimestamp(),
      outcome, ruleViolations
    });
  } catch (e) {
    console.warn("listingLookup audit write failed:", e.message);
  }
}

// ─── Cache fallback ────────────────────────────────────────────────────

/** Step 1 catalog mirror lookup. Returns the cached doc or null.
 *  Used when the live API call fails. */
async function getListingFromCacheById(listingId) {
  if (!listingId) return null;
  try {
    const snap = await db.collection(LISTINGS_COLL).doc(String(listingId)).get();
    if (!snap.exists) return null;
    return snap.data();
  } catch (e) {
    console.warn(`getListingFromCacheById(${listingId}) failed:`, e.message);
    return null;
  }
}

// ─── Normalization ─────────────────────────────────────────────────────

/** Normalize raw Etsy API response (or cache doc) into a single shape
 *  the AI consumes. `apiData` is the result of getListing(); `imageInfo`
 *  is the result of getListingImages(). Either may be null when this is
 *  called against the cache-fallback path. */
function normalizeListing(apiData, imageInfo, fallbackCacheDoc, inventoryInfo) {
  // Prefer fresh API data when present, fall back to cache for missing
  // fields (cache has older but still-valid title/description/etc.)
  const pickStr = (apiKey, cacheKey) => {
    if (apiData && apiData[apiKey] != null) return String(apiData[apiKey]);
    if (fallbackCacheDoc && fallbackCacheDoc[cacheKey] != null) return String(fallbackCacheDoc[cacheKey]);
    return null;
  };
  const pickNum = (apiKey, cacheKey) => {
    if (apiData && Number.isFinite(Number(apiData[apiKey]))) return Number(apiData[apiKey]);
    if (fallbackCacheDoc && Number.isFinite(Number(fallbackCacheDoc[cacheKey]))) return Number(fallbackCacheDoc[cacheKey]);
    return null;
  };

  // Price: API returns { price: { amount, divisor, currency_code } }.
  // Cache stores priceUsd directly.
  let priceUsd = null;
  let currencyCode = null;
  if (apiData && apiData.price) {
    priceUsd = priceToUsd(apiData.price);
    currencyCode = apiData.price.currency_code || null;
  }
  if (priceUsd === null && fallbackCacheDoc && Number.isFinite(Number(fallbackCacheDoc.priceUsd))) {
    priceUsd = Number(fallbackCacheDoc.priceUsd);
    currencyCode = "USD";
  }

  // Image URLs: prefer freshly-fetched imageInfo (4 size variants),
  // fall back to cache's images[] (Step 1 catalog stores an array of
  // {url, alt_text} pairs).
  let imageUrls = [];
  let primaryImageUrl = null;
  if (imageInfo) {
    primaryImageUrl = imageInfo.url_570xN || imageInfo.url_340x270 || imageInfo.url_fullxfull || imageInfo.url_170x135 || null;
    imageUrls = [
      imageInfo.url_570xN, imageInfo.url_fullxfull, imageInfo.url_340x270, imageInfo.url_170x135
    ].filter(Boolean);
  }
  if (!primaryImageUrl && fallbackCacheDoc && Array.isArray(fallbackCacheDoc.images) && fallbackCacheDoc.images.length > 0) {
    // Step 1 catalog stores `images: [{url, altText}, ...]`. Take the
    // first one as the primary; populate imageUrls with all of them.
    const cachedImageUrls = fallbackCacheDoc.images
      .map(i => (i && typeof i.url === "string") ? i.url : null)
      .filter(Boolean);
    if (cachedImageUrls.length > 0) {
      primaryImageUrl = cachedImageUrls[0];
      imageUrls = cachedImageUrls;
    }
  }

  // Description: cap at 800 chars for the AI's context window.
  // Catalog stores it as `description` (full text, already trimmed by
  // catalog's intake). API returns it as `description` too.
  let descriptionShort = null;
  if (apiData && typeof apiData.description === "string") {
    descriptionShort = apiData.description.slice(0, 800);
  } else if (fallbackCacheDoc && typeof fallbackCacheDoc.description === "string") {
    descriptionShort = fallbackCacheDoc.description.slice(0, 800);
  }

  return {
    listingId        : pickStr("listing_id", "listingId"),
    title            : pickStr("title", "title"),
    descriptionShort,
    priceUsd,
    currencyCode     : currencyCode || (priceUsd !== null ? "USD" : null),
    state            : pickStr("state", "state") || null,
    quantity         : pickNum("quantity", "quantity"),
    listingUrl       : (apiData && apiData.url) || (fallbackCacheDoc && fallbackCacheDoc.listingUrl) ||
                       (apiData && apiData.listing_id ? `https://www.etsy.com/listing/${apiData.listing_id}` : null),
    primaryImageUrl,
    imageUrls,
    tags             : Array.isArray(apiData && apiData.tags) ? apiData.tags
                       : Array.isArray(fallbackCacheDoc && fallbackCacheDoc.tags) ? fallbackCacheDoc.tags
                       : [],
    materials        : Array.isArray(apiData && apiData.materials) ? apiData.materials
                       : Array.isArray(fallbackCacheDoc && fallbackCacheDoc.materials) ? fallbackCacheDoc.materials
                       : [],
    whoMade          : pickStr("who_made", "whoMade"),
    whenMade         : pickStr("when_made", "whenMade"),
    isCustomizable   : (apiData && typeof apiData.is_customizable === "boolean")
                        ? apiData.is_customizable
                        : (fallbackCacheDoc && typeof fallbackCacheDoc.isCustomizable === "boolean"
                            ? fallbackCacheDoc.isCustomizable : null),
    shopId           : pickNum("shop_id", "shopId"),
    // Catalog stores processing as `leadTimeDays` (single number, derived
    // from API's processing_min). Cache fallback uses that for both min
    // and max — better than null, the AI sees an approximate.
    processingMin    : (apiData && Number.isFinite(Number(apiData.processing_min)))
                        ? Number(apiData.processing_min)
                        : (fallbackCacheDoc && Number.isFinite(Number(fallbackCacheDoc.leadTimeDays))
                            ? Number(fallbackCacheDoc.leadTimeDays) : null),
    processingMax    : (apiData && Number.isFinite(Number(apiData.processing_max)))
                        ? Number(apiData.processing_max)
                        : (fallbackCacheDoc && Number.isFinite(Number(fallbackCacheDoc.leadTimeDays))
                            ? Number(fallbackCacheDoc.leadTimeDays) : null),
    // v2.5 — Live variants from Etsy's /listings/{id}/inventory endpoint.
    // The exact list the buyer sees in the listing's option dropdown,
    // with current prices and stock status. Source of truth for "what's
    // actually orderable on this listing" — replaces guesswork from
    // title/description.
    variants         : normalizeInventoryVariants(inventoryInfo),
    fetchedAt        : Date.now()
  };
}

/** Convert getListingInventory() raw response into a slim variant array.
 *  Each Etsy "product" is one variant combination (e.g. "14K Solid Gold"
 *  alone, or "Gold + Engrave" for a multi-property listing).
 *  Returns: [{ name, propertyName, priceUsd, enabled, quantity }, ...]
 *  Returns [] if input is missing/malformed. */
function normalizeInventoryVariants(inventoryInfo) {
  const products = (inventoryInfo && Array.isArray(inventoryInfo.products))
    ? inventoryInfo.products : [];
  return products
    .filter(p => p && p.is_deleted !== true)
    .map(p => {
      const propValues = Array.isArray(p.property_values) ? p.property_values : [];
      const nameParts  = [];
      let propertyName = null;
      for (const pv of propValues) {
        if (!pv) continue;
        if (!propertyName) propertyName = pv.property_name || null;
        const vals = Array.isArray(pv.values) ? pv.values : [];
        if (vals.length) nameParts.push(vals.join("/"));
      }
      const name = nameParts.join(" + ").trim() || null;

      const firstOffering = Array.isArray(p.offerings) ? p.offerings[0] : null;
      const priceObj      = firstOffering && firstOffering.price;
      const priceUsd      = priceObj && typeof priceObj.amount === "number" && typeof priceObj.divisor === "number"
        ? Math.round((priceObj.amount / priceObj.divisor) * 100) / 100
        : null;
      const enabled       = firstOffering ? firstOffering.is_enabled !== false : true;
      const quantity      = firstOffering && typeof firstOffering.quantity === "number"
        ? firstOffering.quantity : null;

      return { name, propertyName, priceUsd, enabled, quantity };
    })
    .filter(v => v.name);
}

// ─── Core lookup ───────────────────────────────────────────────────────

/** Look up a listing by its numeric ID. Tries Etsy API first, falls
 *  back to the catalog cache. Returns the structured response shape
 *  documented at the top of the file. */
async function lookupListingById({ listingId, threadId = null }) {
  if (!listingId || !/^\d+$/.test(String(listingId))) {
    return { found: false, reason: "INVALID_INPUT" };
  }

  // 60s in-process cache. Sweep stale entries on read (cheap walk,
  // gives memory back without a separate timer).
  _sweepStaleCacheEntries();
  const cached = _lookupCache.get(String(listingId));
  if (cached && (Date.now() - cached.fetchedAt < LOOKUP_CACHE_MS)) {
    return cached.value;
  }

  let apiData = null, imageInfo = null, inventoryInfo = null;
  let etsyApiCallSuccessful = false;
  let etsyApiError = null;
  let cacheDoc = null;
  let cacheFallbackUsed = false;

  // ── Primary path: Etsy API ──
  // v2.3 audit fix — parallelize getListing + getListingImages. They
  // both take `listingId` directly, no dependency between them. Cuts
  // typical lookup latency from ~700ms to ~400ms (one round-trip
  // instead of two). Image fetch failure is non-fatal.
  // v2.5 — also fetch inventory in parallel so the AI sees the actual
  // variants the buyer sees in the listing's option dropdown (metals,
  // engraving options, prices per variant, etc.). The Firestore mirror's
  // catalog doesn't store variant data; the live API is the only
  // source of truth. Failure is non-fatal — listing data still returns.
  let apiCallErr = null, imageErr = null, inventoryErr = null;
  const [apiRes, imgRes, invRes] = await Promise.allSettled([
    getListing(listingId),
    getListingImages(listingId),
    getListingInventory(listingId)
  ]);
  if (apiRes.status === "fulfilled") {
    apiData = apiRes.value;
    etsyApiCallSuccessful = !!(apiData && apiData.listing_id);
  } else {
    apiCallErr = apiRes.reason && (apiRes.reason.message || String(apiRes.reason));
    etsyApiError = apiCallErr;
  }
  if (imgRes.status === "fulfilled") {
    imageInfo = imgRes.value;
  } else {
    imageErr = imgRes.reason && (imgRes.reason.message || String(imgRes.reason));
    if (etsyApiCallSuccessful) {
      console.warn(`getListingImages(${listingId}) failed (non-fatal):`, imageErr);
    }
  }
  if (invRes.status === "fulfilled") {
    inventoryInfo = invRes.value;
  } else {
    inventoryErr = invRes.reason && (invRes.reason.message || String(invRes.reason));
    if (etsyApiCallSuccessful) {
      console.warn(`getListingInventory(${listingId}) failed (non-fatal):`, inventoryErr);
    }
  }

  // ── Fallback: Step 1 catalog mirror ──
  if (!etsyApiCallSuccessful) {
    cacheDoc = await getListingFromCacheById(listingId);
    if (cacheDoc) cacheFallbackUsed = true;
  }

  // ── Both paths failed ──
  if (!etsyApiCallSuccessful && !cacheDoc) {
    const result = {
      found: false,
      reason: etsyApiError ? "ETSY_API_ERROR" : "LISTING_NOT_FOUND_API_AND_CACHE",
      listingId: String(listingId),
      etsyApiError
    };
    await writeAudit({
      threadId, eventType: "listing_lookup_failed",
      payload: { listingId, etsyApiError, cacheFallbackUsed: false },
      outcome: "failure",
      ruleViolations: [result.reason]
    });
    return result;
  }

  // ── Normalize ──
  const normalized = normalizeListing(apiData, imageInfo, cacheDoc, inventoryInfo);

  // ── Shop ownership check ──
  // If the API returned a shop_id and it doesn't match ours, the customer
  // pasted a competitor's listing. Politely flag — this is NOT a hard
  // failure, the agent might still want to acknowledge "that's not mine
  // but here are some similar items I do offer".
  let notOurShop = false;
  if (SHOP_ID && normalized.shopId && String(normalized.shopId) !== String(SHOP_ID)) {
    notOurShop = true;
  }

  // ── Active state check ──
  // A listing in state "draft", "expired", "sold_out", "inactive" still
  // returns from the API — but we shouldn't tell the customer they can
  // buy it. Surface the state on the result; the AI prompt explains
  // what to do with each state.
  const isActive = (normalized.state === "active");

  const success = {
    found: true,
    listingId: String(listingId),
    source: cacheFallbackUsed ? "catalog_cache_fallback" : "etsy_api",
    etsyApiCallSuccessful,
    cacheFallbackUsed,
    notOurShop,
    isActive,
    listing: normalized
  };

  // Cache the result
  // FIFO evict if we're at the cap. Map preserves insertion order, so
  // the first key in the iterator is the oldest entry.
  if (_lookupCache.size >= LOOKUP_CACHE_MAX_ENTRIES) {
    const oldestKey = _lookupCache.keys().next().value;
    if (oldestKey !== undefined) _lookupCache.delete(oldestKey);
  }
  _lookupCache.set(String(listingId), { value: success, fetchedAt: Date.now() });

  await writeAudit({
    threadId, eventType: "listing_lookup_success",
    payload: {
      listingId: String(listingId),
      source: success.source,
      cacheFallbackUsed,
      notOurShop,
      isActive,
      title: normalized.title,
      priceUsd: normalized.priceUsd
    }
  });

  return success;
}

/** Look up a listing by its URL. Wraps extractListingIdFromUrl. */
async function lookupListingByUrl({ url, threadId = null }) {
  if (typeof url !== "string" || url.length < 12) {
    return { found: false, reason: "INVALID_INPUT" };
  }
  const id = extractListingIdFromUrl(url);
  if (id === null) {
    return { found: false, reason: "NOT_AN_ETSY_URL" };
  }
  if (id === "SHORT_LINK") {
    return {
      found: false,
      reason: "SHORT_LINK_UNRESOLVED",
      url,
      hint: "Etsy short links (etsy.me/...) aren't auto-resolved. Ask the customer for the full URL or the listing's title."
    };
  }
  return await lookupListingById({ listingId: id, threadId });
}

// ─── Handler ───────────────────────────────────────────────────────────

function json(statusCode, body) {
  return { statusCode, headers: { ...CORS, "Content-Type": "application/json" }, body: JSON.stringify(body) };
}

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
  catch { return json(400, { error: "Invalid JSON body" }); }

  const op = body.op;

  if (op === "lookupByUrl") {
    const result = await lookupListingByUrl({ url: body.url, threadId: body.threadId });
    return json(200, result);
  }

  if (op === "lookupById") {
    const result = await lookupListingById({ listingId: body.listingId, threadId: body.threadId });
    return json(200, result);
  }

  return json(400, { error: `Unknown op '${op}'` });
};

// ─── Direct-import surface ─────────────────────────────────────────────

module.exports.extractListingIdFromUrl = extractListingIdFromUrl;
module.exports.findEtsyUrlsInText      = findEtsyUrlsInText;
module.exports.lookupListingByUrl      = lookupListingByUrl;
module.exports.lookupListingById       = lookupListingById;
