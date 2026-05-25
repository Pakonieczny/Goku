/*  netlify/functions/_etsyMailEtsy.js
 *
 *  Shared Etsy API helpers for the EtsyMail system.
 *
 *  Centralizes three things that were copy-pasted across
 *  etsyMailOrder.js and etsyMailSync-background.js:
 *    1. OAuth token read + auto-refresh from config/etsyOauth
 *    2. Standard fetch wrapper with the right auth headers
 *    3. Domain helpers used by the AI draft function:
 *         - getShop()                   → shop metadata for prompt enrichment
 *         - getShopSections()           → shop category structure
 *         - getShopReceiptFull(id)      → full receipt incl. transactions,
 *                                         variations, personalization
 *         - getShopReceiptShipments(id) → tracking codes + carriers
 *
 *  Env vars required (same as other functions):
 *    SHOP_ID
 *    CLIENT_ID
 *    CLIENT_SECRET (or ETSY_SHARED_SECRET)
 *
 *  OAuth tokens are NOT env vars — they live in Firestore at
 *  config/etsyOauth (2-segment path) and rotate on every refresh.
 *
 *  Note: etsyMailOrder.js and etsyMailSync-background.js are NOT being
 *  migrated to this module in this PR — they continue to use their own
 *  inline copies. This module is a going-forward consolidation.
 *
 *  ═══ METERING ═══════════════════════════════════════════════════════════
 *  Every Etsy API call made through this module is logged to the live
 *  counter doc at EtsyMail_Config/etsyApiCounters via _etsyApiMeter.js.
 *  Two layers of instrumentation:
 *    1. The low-level etsyFetch() bumps "helper.etsyFetch" (parent counter)
 *    2. Each domain helper (getShop/getListing/etc.) bumps its OWN siteId
 *       BEFORE calling etsyFetch — so the UI can see per-endpoint counts.
 *    3. refreshEtsyToken() bumps "helper.oauthRefresh".
 *  See _etsyApiMeter.js for the full site-ID list.
 */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");
const meter = require("./_etsyApiMeter");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const SHOP_ID       = process.env.SHOP_ID;
const CLIENT_ID     = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET || process.env.ETSY_SHARED_SECRET;

const OAUTH_DOC_PATH = "config/etsyOauth";
const TOKEN_REFRESH_BUFFER_MS = 2 * 60 * 1000;   // refresh if <2min to expiry

// ─── OAuth token management ──────────────────────────────────────────────
// Mirrors etsyMailOrder.js — read current token, refresh if stale.
// ═══ OAuth token caching ══════════════════════════════════════════════════
//
// Token state is cached in TWO layers:
//
//   1. Firestore (`config/etsyOauth`) — durable, shared across function
//      invocations. Every Netlify function reads from here on cold start.
//   2. Module-level in-memory cache — survives across multiple invocations
//      within the same warm Node.js process. Saves a Firestore round-trip
//      per Etsy call when the same function instance is reused.
//
// On a cold function start, the first Etsy call reads from Firestore and
// populates the in-memory cache. Subsequent calls in that same process
// use the in-memory copy until it's stale (TOKEN_REFRESH_BUFFER_MS before
// expiry), at which point we refresh and re-cache both layers.
//
// When refreshing, we write BOTH `expires_at` and `expires_at_ms` to the
// Firestore doc — historical compat with three other files in the
// codebase that read the long-form field name. Once all callers have been
// migrated to this helper, the compat field can be retired.
//
// Etsy rotates refresh_token on each refresh, so the new one is persisted.
let _inMemoryAccessToken = null;
let _inMemoryExpiresAtMs = 0;

function _cacheTokenInMemory(accessToken, expiresAtMs) {
  _inMemoryAccessToken = accessToken;
  _inMemoryExpiresAtMs = expiresAtMs;
}

async function refreshEtsyToken(oldRefreshToken) {
  const t = meter.bump("helper.oauthRefresh");
  let res;
  try {
    res = await fetch("https://api.etsy.com/v3/public/oauth/token", {
      method : "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body   : new URLSearchParams({
        grant_type   : "refresh_token",
        client_id    : CLIENT_ID,
        refresh_token: oldRefreshToken
      })
    });
  } catch (err) {
    t.failNet();
    throw err;
  }
  t.fromHttp(res.status);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Etsy token refresh failed: ${res.status} ${body}`);
  }
  const data = await res.json();
  const expiresAtMs = Date.now() + Math.max(0, (data.expires_in - 120)) * 1000;
  await db.doc(OAUTH_DOC_PATH).set({
    access_token : data.access_token,
    refresh_token: data.refresh_token || oldRefreshToken,
    // Dual-field write for compat with files that haven't been migrated
    // to use this helper yet. Both fields carry the same Unix-ms value;
    // once all readers have migrated, drop `expires_at` and keep
    // `expires_at_ms` as the canonical name.
    expires_at   : expiresAtMs,
    expires_at_ms: expiresAtMs,
    expires_in   : data.expires_in,
    refreshed_at : FV.serverTimestamp(),
    updatedAt    : FV.serverTimestamp()
  }, { merge: true });
  _cacheTokenInMemory(data.access_token, expiresAtMs);
  return data.access_token;
}

async function getValidEtsyAccessToken() {
  // Layer 1 — in-memory cache. Reused across invocations within the same
  // warm Node process. Cuts the Firestore round-trip out entirely when
  // the cached token is still well within its lifetime.
  if (_inMemoryAccessToken && _inMemoryExpiresAtMs - Date.now() > TOKEN_REFRESH_BUFFER_MS) {
    return _inMemoryAccessToken;
  }

  // Layer 2 — Firestore cache. Shared across all function invocations
  // and across cold starts.
  const snap = await db.doc(OAUTH_DOC_PATH).get();
  if (!snap.exists) throw new Error(`Etsy OAuth not seeded at ${OAUTH_DOC_PATH}. Run etsyMailSeedTokens first.`);
  const tok = snap.data();
  if (!tok.refresh_token) throw new Error("No refresh_token in OAuth doc");

  // Read either field name — handles tokens that were last written by the
  // legacy paths that used `expires_at_ms`. Once all callers migrate to
  // this helper, this fallback is no-op.
  const expiresAtMs = (typeof tok.expires_at_ms === "number" && tok.expires_at_ms)
    || (typeof tok.expires_at === "number" && tok.expires_at)
    || 0;

  if (tok.access_token && expiresAtMs - Date.now() > TOKEN_REFRESH_BUFFER_MS) {
    // Firestore token is still valid — warm the in-memory cache and
    // return it without a refresh.
    _cacheTokenInMemory(tok.access_token, expiresAtMs);
    return tok.access_token;
  }

  // Both caches stale — refresh from Etsy.
  return await refreshEtsyToken(tok.refresh_token);
}

// ─── Low-level fetch wrapper ────────────────────────────────────────────
//
// Bumps the parent "helper.etsyFetch" counter on every call. Domain
// helpers below bump their OWN site IDs before calling here, so a single
// /listings/{id} call shows up under BOTH "helper.etsyFetch" and
// "helper.getListing" in the UI — making it easy to spot which helper
// is dominating the day's quota.
async function etsyFetch(path, { method = "GET", query = null, body = null } = {}) {
  if (!SHOP_ID || !CLIENT_ID || !CLIENT_SECRET) {
    throw new Error("Missing SHOP_ID / CLIENT_ID / CLIENT_SECRET env vars");
  }
  const accessToken = await getValidEtsyAccessToken();

  let url = `https://api.etsy.com/v3/application${path}`;
  if (query) {
    const sp = new URLSearchParams();
    for (const [k, v] of Object.entries(query)) {
      if (v != null) sp.append(k, String(v));
    }
    const qs = sp.toString();
    if (qs) url += "?" + qs;
  }

  const t = meter.bump("helper.etsyFetch");
  let res;
  try {
    res = await fetch(url, {
      method,
      headers: {
        Authorization : `Bearer ${accessToken}`,
        "x-api-key"   : `${CLIENT_ID}:${CLIENT_SECRET}`,
        "Content-Type": "application/json"
      },
      body: body ? JSON.stringify(body) : undefined
    });
  } catch (err) {
    t.failNet();
    throw err;
  }
  t.fromHttp(res.status);

  const text = await res.text();
  let data;
  try { data = text ? JSON.parse(text) : {}; }
  catch { throw new Error(`Etsy API returned non-JSON for ${path}: ${text.slice(0, 200)}`); }

  if (!res.ok) {
    const errMsg = (data && data.error) || `Etsy API ${res.status}`;
    const e = new Error(`Etsy ${path}: ${errMsg}`);
    e.status = res.status;
    e.data = data;
    throw e;
  }
  return data;
}

// ─── Domain helpers ──────────────────────────────────────────────────────
//
// Each helper bumps its own per-endpoint counter BEFORE delegating to
// etsyFetch(). The per-endpoint counter is purely a "which helper drove
// this call" tag — the underlying HTTP outcome is recorded against
// "helper.etsyFetch". For the per-endpoint counter we record `attempt`
// only (no outcome) because the outcome is identical to etsyFetch's.
// The UI displays both rows independently — daily total per row, plus
// the parent etsyFetch total at the top for the aggregated view.
//
// Note: we use bumpSimple(siteId, "attempt") here — there's no need to
// record success/failure separately for the per-helper counter, since
// the underlying etsyFetch call already records outcome under its own
// site ID. Counting attempt only avoids double-booking failure rates.

/** Shop-level metadata: title, announcement, policies, shipping defaults.
 *  Used by the AI draft prompt enricher so Claude can reference real
 *  shop policies instead of only the baked-in defaults.
 *  GET /shops/{shop_id} */
async function getShop() {
  meter.bumpSimple("helper.getShop");
  return etsyFetch(`/shops/${SHOP_ID}`);
}

/** Top-level category sections for the shop. Useful for AI to reference
 *  categories when suggesting listings.
 *  GET /shops/{shop_id}/sections */
async function getShopSections() {
  meter.bumpSimple("helper.getShopSections");
  const data = await etsyFetch(`/shops/${SHOP_ID}/sections`);
  return Array.isArray(data.results) ? data.results : [];
}

/** Full receipt with transactions, variations, and personalization.
 *  Used by the AI's lookup_order_details tool for custom-order discussions.
 *  GET /shops/{shop_id}/receipts/{receipt_id} */
async function getShopReceiptFull(receiptId) {
  meter.bumpSimple("helper.getShopReceiptFull");
  const includes = ["Transactions", "Transactions.personalization", "Transactions.variations"];
  return etsyFetch(`/shops/${SHOP_ID}/receipts/${receiptId}`, {
    query: { includes: includes.join(",") }
  });
}

/** Shipments for a receipt — this is where tracking codes + carriers live.
 *  Etsy attaches shipments to receipts once the seller marks items shipped.
 *  GET /shops/{shop_id}/receipts/{receipt_id}  (shipments embedded in payload)
 *
 *  Returns a slim object with just the fields the AI needs:
 *    {
 *      receiptId, isShipped, shipments: [
 *        { trackingCode, carrier, trackingUrl, shipDate, note }
 *      ],
 *      estimatedDelivery: {min, max},
 *      status: 'paid'|'shipped'|'delivered'|'unpaid'
 *    }
 */
async function getShopReceiptShipments(receiptId) {
  meter.bumpSimple("helper.getShopReceiptShip");
  // The receipt endpoint returns shipments in the `shipments` array by default
  // — no extra include needed. We fetch without includes to keep the payload small.
  const receipt = await etsyFetch(`/shops/${SHOP_ID}/receipts/${receiptId}`);

  const shipments = Array.isArray(receipt.shipments) ? receipt.shipments : [];
  const slimShipments = shipments.map(s => ({
    trackingCode: s.tracking_code  || null,
    carrier     : s.carrier_name   || null,
    trackingUrl : s.tracking_url   || null,
    shipDate    : s.shipment_notification_timestamp
                    ? new Date(s.shipment_notification_timestamp * 1000).toISOString()
                    : null,
    note        : s.buyer_note || null
  }));

  let status = "unpaid";
  if (receipt.is_paid)    status = "paid";
  if (receipt.is_shipped) status = "shipped";

  return {
    receiptId : String(receiptId),
    isPaid    : !!receipt.is_paid,
    isShipped : !!receipt.is_shipped,
    status,
    shipments : slimShipments,
    shippedAt : receipt.is_shipped && shipments.length
                  ? slimShipments[0].shipDate
                  : null,
    estimatedDelivery: {
      min: receipt.min_expected_shipping_date || null,
      max: receipt.max_expected_shipping_date || null
    },
    currency  : receipt.total_price && receipt.total_price.currency_code || null,
    grandTotal: receipt.grandtotal && Number(receipt.grandtotal.amount) / Math.pow(10, receipt.grandtotal.divisor || 2) || null,
    buyerName : receipt.name        || null
  };
}

/** Fetches listing images for a single listing_id and returns the URL
 *  list (preferred sizes first). Used for suggestedListings preview
 *  thumbnails in the inbox UI.
 *  GET /listings/{listing_id}/images */
async function getListingImages(listingId) {
  meter.bumpSimple("helper.getListingImages");
  const data = await etsyFetch(`/listings/${listingId}/images`);
  const results = Array.isArray(data.results) ? data.results : [];
  if (!results.length) return null;
  const img = results[0];
  return {
    url_170x135 : img.url_170x135  || null,
    url_340x270 : img.url_340x270  || null,
    url_570xN   : img.url_570xN    || null,
    url_fullxfull: img.url_fullxfull || null
  };
}

/** Minimal listing metadata — title, price, URL. Used by AI to verify
 *  a listing actually exists before suggesting it.
 *  GET /listings/{listing_id} */
async function getListing(listingId) {
  meter.bumpSimple("helper.getListing");
  return etsyFetch(`/listings/${listingId}`);
}

/** Full inventory for a listing: every variant the buyer sees in the
 *  option dropdown, with price, SKU, quantity, and enabled state.
 *  GET /listings/{listing_id}/inventory */
async function getListingInventory(listingId) {
  meter.bumpSimple("helper.getListingInventory");
  return etsyFetch(`/listings/${listingId}/inventory`);
}

module.exports = {
  getValidEtsyAccessToken,
  etsyFetch,
  getShop,
  getShopSections,
  getShopReceiptFull,
  getShopReceiptShipments,
  getListing,
  getListingImages,
  getListingInventory,
  SHOP_ID
};
