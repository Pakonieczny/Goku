/*  netlify/functions/etsyMailShippingSync.js
 *
 *  v2.2 — Etsy shipping sync. Periodically pulls the shop's shipping
 *  profiles + upgrades + destinations from Etsy's Open API v3 and caches
 *  them in EtsyMail_ShippingUpgradesCache for the sales agent to read
 *  on demand.
 *
 *  ═══ WHY ═══════════════════════════════════════════════════════════════
 *
 *  When a customer expresses urgency in a sales conversation, the AI needs
 *  to mention available shipping options as part of the speed-up package.
 *  Calling Etsy's API on every customer message is wasteful (rate limits,
 *  added latency, network errors during sales calls). Instead we sync on
 *  a 6-hour cron and serve the cache.
 *
 *  ═══ WHAT THIS DOES ════════════════════════════════════════════════════
 *
 *  1. Authenticated fetch: GET /shops/{shop_id}/shipping-profiles
 *  2. For each profile: GET .../upgrades (the expedited options) and
 *     GET .../destinations (per-region base costs)
 *  3. Normalize the price structure (Etsy returns {amount, divisor,
 *     currency_code}; we flatten to USD).
 *  4. Compute summary stats: cheapestUpgradeUsd, fastestUpgradeDays, range.
 *  5. Write to EtsyMail_ShippingUpgradesCache/current as a single doc.
 *  6. Audit the sync result.
 *
 *  ═══ ENDPOINTS USED ════════════════════════════════════════════════════
 *
 *  All three endpoints require shops_r OAuth scope and use existing
 *  etsyFetch helper (OAuth token refresh handled there):
 *
 *    GET /shops/{shop_id}/shipping-profiles
 *    GET /shops/{shop_id}/shipping-profiles/{id}/upgrades
 *    GET /shops/{shop_id}/shipping-profiles/{id}/destinations
 *
 *  Etsy is currently mid-migration from "shipping profiles" to "processing
 *  profiles" but explicitly says third-party developers should NOT expose
 *  processing-profile features to live customers yet. So we stick with
 *  shipping profiles (the production path).
 *
 *  ═══ TWO INVOCATION PATHS ══════════════════════════════════════════════
 *
 *  1. Scheduled (Netlify cron, every 6h) — auto-refresh, bypasses
 *     extension-secret check.
 *  2. POST { op: "syncNow" } — manual trigger for operators (requires
 *     extension secret + owner role).
 *  3. POST { op: "getCache" } — read-only, returns the cached data
 *     without re-syncing. Used by the option resolver and UI.
 *
 *  ═══ CACHE SHAPE ═══════════════════════════════════════════════════════
 *
 *    EtsyMail_ShippingUpgradesCache/current = {
 *      lastSyncedAt: serverTimestamp(),
 *      lastSyncOutcome: "success" | "partial" | "failure",
 *      lastSyncError: <string or null>,
 *      profileCount: <int>,
 *      profiles: [
 *        {
 *          shippingProfileId: 12345678,
 *          title: "US Standard",
 *          originCountryIso: "US",
 *          minProcessingDays: 4,
 *          maxProcessingDays: 5,
 *          processingDaysDisplayLabel: "4-5 business days",
 *          minDeliveryDays: 3,
 *          maxDeliveryDays: 7,
 *          destinations: [
 *            { destinationCountryIso: "US", primaryCostUsd: 4.5, secondaryCostUsd: 1.0 },
 *            ...
 *          ],
 *          upgrades: [
 *            { upgradeId, upgradeName, type: "domestic"|"international",
 *              priceUsd, secondaryPriceUsd, mailClass, minDeliveryDays, maxDeliveryDays }
 *          ]
 *        },
 *        ...
 *      ],
 *      summary: {
 *        anyUpgradesAvailable: <bool>,
 *        cheapestUpgradeUsd: <number or null>,
 *        priciestUpgradeUsd: <number or null>,
 *        fastestUpgradeMinDays: <number or null>,
 *        domesticUpgradeRangeText: "$X.XX-$Y.YY"
 *      }
 *    }
 *
 *  ═══ EXPORTED HELPER ═══════════════════════════════════════════════════
 *
 *    module.exports.getShippingUpgradesCache()
 *      Direct-import path for sibling functions. Returns the cache doc
 *      or null if it hasn't been synced yet. Caches the result in-memory
 *      for 60 seconds to avoid hammering Firestore.
 *
 *    module.exports.summarizeShippingForAi(cache)
 *      Returns a compact { rangeText, anyUpgrades, fastestDaysText }
 *      summary the sales agent can reference in customer replies.
 *
 *  ═══ ENV VARS ══════════════════════════════════════════════════════════
 *
 *    SHOP_ID, CLIENT_ID, CLIENT_SECRET   — used by _etsyMailEtsy.etsyFetch
 *    ETSYMAIL_EXTENSION_SECRET           — gates non-cron POST entry
 *    NETLIFY_SCHEDULED_FUNCTION_HEADER   — implicit, set by Netlify cron
 */

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const { isScheduledInvocation } = require("./_etsyMailScheduled");
const { requireOwner, logUnauthorized } = require("./_etsyMailRoles");
const { etsyFetch, SHOP_ID } = require("./_etsyMailEtsy");
const meter = require("./_etsyApiMeter");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const CACHE_COLL = "EtsyMail_ShippingUpgradesCache";
const CACHE_DOC  = "current";
const AUDIT_COLL = "EtsyMail_Audit";

// In-memory cache for direct-import callers. Refreshed on read after
// 60s to keep Firestore reads cheap during a busy sales conversation.
const READ_CACHE_MS = 60 * 1000;
let _readCache = { value: null, fetchedAt: 0 };

// ─── Helpers ────────────────────────────────────────────────────────────

function json(statusCode, body) {
  return { statusCode, headers: { ...CORS, "Content-Type": "application/json" }, body: JSON.stringify(body) };
}


async function writeAudit({ eventType, actor = "system:shippingSync", payload = {},
                            outcome = "success", ruleViolations = [] }) {
  try {
    await db.collection(AUDIT_COLL).add({
      threadId: null, draftId: null, eventType, actor, payload,
      createdAt: FV.serverTimestamp(),
      outcome, ruleViolations
    });
  } catch (e) {
    console.warn("shippingSync audit write failed:", e.message);
  }
}

/** Etsy returns prices as {amount, divisor, currency_code}. Convert to a
 *  plain USD number. Returns null if the shape is unexpected. */
function priceToUsd(p) {
  if (!p || typeof p !== "object") return null;
  const amount  = Number(p.amount);
  const divisor = Number(p.divisor);
  if (!Number.isFinite(amount) || !Number.isFinite(divisor) || divisor === 0) return null;
  // We only support USD right now. If the shop currency ever changes,
  // this is the spot to plug in a conversion table.
  if (p.currency_code && p.currency_code !== "USD") {
    // Don't pretend to know an FX rate — return null and let the caller
    // skip this upgrade rather than quoting a wrong number.
    return null;
  }
  return Math.round((amount / divisor) * 100) / 100;
}

/** The Etsy upgrade type field is an integer enum (0 = domestic,
 *  1 = international). Map to a string for the cache. */
function upgradeTypeToString(t) {
  if (t === 0 || t === "0") return "domestic";
  if (t === 1 || t === "1") return "international";
  return "unknown";
}

// ─── The sync ──────────────────────────────────────────────────────────

async function syncShippingFromEtsy() {
  const tStart = Date.now();
  if (!SHOP_ID) {
    return { success: false, error: "SHOP_ID env var missing", profiles: [] };
  }

  let profilesRaw;
  try {
    meter.bumpSimple("shipping.profiles");
    const resp = await etsyFetch(`/shops/${SHOP_ID}/shipping-profiles`);
    profilesRaw = (resp && Array.isArray(resp.results)) ? resp.results : [];
  } catch (e) {
    return { success: false, error: `getShopShippingProfiles failed: ${e.message}`, profiles: [] };
  }

  const profiles = [];
  let anyUpgradesAvailable = false;
  let cheapestUpgradeUsd = null;
  let priciestUpgradeUsd = null;
  let fastestUpgradeMinDays = null;
  let partialFailure = false;

  for (const pr of profilesRaw) {
    const profileId = pr.shipping_profile_id;
    if (!profileId) continue;

    // Fetch upgrades + destinations for this profile in parallel
    meter.bumpSimple("shipping.upgrades");
    meter.bumpSimple("shipping.destinations");
    const [upgradesRes, destinationsRes] = await Promise.allSettled([
      etsyFetch(`/shops/${SHOP_ID}/shipping-profiles/${profileId}/upgrades`),
      etsyFetch(`/shops/${SHOP_ID}/shipping-profiles/${profileId}/destinations`)
    ]);

    let upgradesNorm = [];
    if (upgradesRes.status === "fulfilled" && upgradesRes.value && Array.isArray(upgradesRes.value.results)) {
      upgradesNorm = upgradesRes.value.results.map(u => {
        const priceUsd          = priceToUsd(u.price);
        const secondaryPriceUsd = priceToUsd(u.secondary_price);
        if (priceUsd !== null) {
          if (cheapestUpgradeUsd === null || priceUsd < cheapestUpgradeUsd) cheapestUpgradeUsd = priceUsd;
          if (priciestUpgradeUsd === null || priceUsd > priciestUpgradeUsd) priciestUpgradeUsd = priceUsd;
        }
        if (typeof u.min_delivery_days === "number") {
          if (fastestUpgradeMinDays === null || u.min_delivery_days < fastestUpgradeMinDays) {
            fastestUpgradeMinDays = u.min_delivery_days;
          }
        }
        return {
          upgradeId         : u.upgrade_id || null,
          upgradeName       : u.upgrade_name || "(unnamed)",
          type              : upgradeTypeToString(u.type),
          priceUsd,
          secondaryPriceUsd,
          shippingCarrierId : u.shipping_carrier_id || null,
          mailClass         : u.mail_class || null,
          minDeliveryDays   : (typeof u.min_delivery_days === "number") ? u.min_delivery_days : null,
          maxDeliveryDays   : (typeof u.max_delivery_days === "number") ? u.max_delivery_days : null
        };
      });
      if (upgradesNorm.length > 0) anyUpgradesAvailable = true;
    } else if (upgradesRes.status === "rejected") {
      partialFailure = true;
      console.warn(`shippingSync: upgrades fetch failed for profile ${profileId}:`, upgradesRes.reason && upgradesRes.reason.message);
    }

    let destinationsNorm = [];
    if (destinationsRes.status === "fulfilled" && destinationsRes.value && Array.isArray(destinationsRes.value.results)) {
      destinationsNorm = destinationsRes.value.results.map(d => ({
        shippingProfileDestinationId: d.shipping_profile_destination_id || null,
        destinationCountryIso       : d.destination_country_iso || null,
        destinationRegion           : d.destination_region || null,
        primaryCostUsd              : priceToUsd(d.primary_cost),
        secondaryCostUsd            : priceToUsd(d.secondary_cost),
        mailClass                   : d.mail_class || null,
        minDeliveryDays             : (typeof d.min_delivery_days === "number") ? d.min_delivery_days : null,
        maxDeliveryDays             : (typeof d.max_delivery_days === "number") ? d.max_delivery_days : null
      }));
    } else if (destinationsRes.status === "rejected") {
      partialFailure = true;
      console.warn(`shippingSync: destinations fetch failed for profile ${profileId}:`, destinationsRes.reason && destinationsRes.reason.message);
    }

    profiles.push({
      shippingProfileId            : profileId,
      title                        : pr.title || "(untitled)",
      userId                       : pr.user_id || null,
      originCountryIso             : pr.origin_country_iso || null,
      // These fields are deprecated at the shipping-profile level (Etsy is
      // moving them to processing profiles), but still populated for now.
      // We capture both names and the display label.
      minProcessingDays            : (typeof pr.min_processing_days === "number") ? pr.min_processing_days : null,
      maxProcessingDays            : (typeof pr.max_processing_days === "number") ? pr.max_processing_days : null,
      processingDaysDisplayLabel   : pr.processing_days_display_label || null,
      minDeliveryDays              : (typeof pr.min_delivery_days === "number") ? pr.min_delivery_days : null,
      maxDeliveryDays              : (typeof pr.max_delivery_days === "number") ? pr.max_delivery_days : null,
      destinations                 : destinationsNorm,
      upgrades                     : upgradesNorm
    });
  }

  // Build customer-facing range text. The AI uses this verbatim so we
  // don't ask it to format USD.
  let domesticUpgradeRangeText = null;
  if (cheapestUpgradeUsd !== null && priciestUpgradeUsd !== null) {
    if (Math.abs(cheapestUpgradeUsd - priciestUpgradeUsd) < 0.01) {
      domesticUpgradeRangeText = `$${cheapestUpgradeUsd.toFixed(2)}`;
    } else {
      domesticUpgradeRangeText = `$${cheapestUpgradeUsd.toFixed(2)}-$${priciestUpgradeUsd.toFixed(2)}`;
    }
  }

  const cacheDoc = {
    lastSyncedAt   : FV.serverTimestamp(),
    lastSyncOutcome: partialFailure ? "partial" : "success",
    lastSyncError  : null,
    profileCount   : profiles.length,
    profiles,
    summary: {
      anyUpgradesAvailable,
      cheapestUpgradeUsd,
      priciestUpgradeUsd,
      fastestUpgradeMinDays,
      domesticUpgradeRangeText
    }
  };

  await db.collection(CACHE_COLL).doc(CACHE_DOC).set(cacheDoc, { merge: false });
  // Bust the in-memory read cache too
  _readCache = { value: null, fetchedAt: 0 };

  await writeAudit({
    eventType: "shipping_sync_complete",
    payload  : {
      profileCount: profiles.length,
      summary: cacheDoc.summary,
      partialFailure,
      durationMs: Date.now() - tStart
    },
    outcome: partialFailure ? "failure" : "success"
  });

  return {
    success: !partialFailure,
    profileCount: profiles.length,
    summary: cacheDoc.summary,
    partialFailure
  };
}

// ─── Read helper (direct-import path) ──────────────────────────────────

/** Read the cached shipping data. 60s in-memory cache to avoid hammering
 *  Firestore during back-to-back agent turns. */
async function getShippingUpgradesCache() {
  if (_readCache.value && (Date.now() - _readCache.fetchedAt < READ_CACHE_MS)) {
    return _readCache.value;
  }
  try {
    const snap = await db.collection(CACHE_COLL).doc(CACHE_DOC).get();
    if (!snap.exists) {
      _readCache = { value: null, fetchedAt: Date.now() };
      return null;
    }
    const data = snap.data();
    _readCache = { value: data, fetchedAt: Date.now() };
    return data;
  } catch (e) {
    console.warn("getShippingUpgradesCache failed:", e.message);
    return null;
  }
}

/** Compact summary for the AI to drop into a customer reply.
 *  Returns { rangeText, anyUpgrades, fastestDaysText } or a graceful
 *  null shape if no cache or no upgrades available. */
function summarizeShippingForAi(cache) {
  if (!cache || !cache.summary) {
    return { rangeText: null, anyUpgrades: false, fastestDaysText: null, available: false };
  }
  const s = cache.summary;
  if (!s.anyUpgradesAvailable) {
    return { rangeText: null, anyUpgrades: false, fastestDaysText: null, available: false };
  }
  const fastestDaysText = (typeof s.fastestUpgradeMinDays === "number")
    ? `as fast as ${s.fastestUpgradeMinDays} days`
    : null;
  return {
    rangeText      : s.domesticUpgradeRangeText || null,
    anyUpgrades    : true,
    fastestDaysText,
    available      : true
  };
}

// ─── Handler ───────────────────────────────────────────────────────────

exports.handler = meter.wrapHandler(async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  // Cron path: scheduled invocations bypass extension-secret check.
  // Same pattern as v1.10's etsyMailAutoPipelineReaper.
  if (isScheduledInvocation(event)) {
    try {
      const result = await syncShippingFromEtsy();
      return json(200, { ok: true, scheduled: true, ...result });
    } catch (err) {
      console.error("shippingSync (scheduled) error:", err);
      await writeAudit({
        eventType: "shipping_sync_failed",
        payload  : { error: err.message, scheduled: true },
        outcome  : "failure"
      });
      return json(500, { ok: false, error: err.message });
    }
  }

  // Manual paths require auth
  if (event.httpMethod !== "POST") return json(405, { error: "Method Not Allowed" });
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return json(400, { error: "Invalid JSON body" }); }

  const op = body.op;

  if (op === "getCache") {
    // Read-only — anyone with the secret can read
    const cache = await getShippingUpgradesCache();
    return json(200, { success: true, cache });
  }

  if (op === "syncNow") {
    // Owner-only
    const ownerCheck = await requireOwner(body.actor);
    if (!ownerCheck.ok) {
      await logUnauthorized({
        actor: body.actor,
        eventType: "shipping_sync_unauthorized",
        payload: { reason: ownerCheck.reason }
      });
      return json(403, { error: "Owner role required", reason: ownerCheck.reason });
    }
    try {
      const result = await syncShippingFromEtsy();
      return json(200, { ok: true, manual: true, ...result });
    } catch (err) {
      console.error("shippingSync (manual) error:", err);
      await writeAudit({
        eventType: "shipping_sync_failed",
        actor    : body.actor,
        payload  : { error: err.message, manual: true },
        outcome  : "failure"
      });
      return json(500, { ok: false, error: err.message });
    }
  }

  return json(400, { error: `Unknown op '${op}'` });
});

module.exports.getShippingUpgradesCache = getShippingUpgradesCache;
module.exports.summarizeShippingForAi   = summarizeShippingForAi;
module.exports.syncShippingFromEtsy     = syncShippingFromEtsy;
