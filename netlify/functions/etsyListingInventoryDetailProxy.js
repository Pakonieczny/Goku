// netlify/functions/etsyListingInventoryDetailProxy.js
// Listing inventory reader.
//
// Two response shapes, selected by query param:
//
//   ?listingId=123                → LEGACY shape (unchanged): { listing_id,
//     products:[{product_id, sku}] }. Preserved for the SKU console at
//     sku.goldenspike.app. Do not remove.
//
//   ?listingId=123&inventory_only=1 → PRICING CONSOLE shape: { listing_id,
//     inventory, snapshot_hash, pricing_health, fetched_at }. inventory is the
//     full products array plus *_on_property maps. snapshot_hash is the
//     canonical hash the write proxy uses for optimistic-concurrency (409
//     STALE_INVENTORY) checks — both sides require _etsyInventoryCanonical.js.

const { etsyFetch } = require("./etsyRateLimiter");
const { snapshotHash, pricingHealth, toDecimalPrice } = require("./_etsyInventoryCanonical");

// The editor renders and edits plain decimal prices; Etsy's GET returns
// Money objects ({amount, divisor}). Normalize before responding so the
// client never sees NaN. Hashing is unaffected — the canonical module
// normalizes both forms identically.
function decimalizeProducts(products) {
  return (products || []).map(p => ({
    ...p,
    offerings: (p.offerings || []).map(o => ({ ...o, price: toDecimalPrice(o.price) }))
  }));
}

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type,Authorization,Access-Token,access-token",
  "Access-Control-Allow-Methods": "GET,OPTIONS"
};


// Etsy requires "keystring:shared_secret" in x-api-key for these endpoints
// when a shared secret exists (same pattern as etsyShopListingsProxy).
function apiKey() {
  const clientId = process.env.CLIENT_ID;
  const secret = process.env.CLIENT_SECRET;
  if (!clientId) return null;
  return secret ? `${clientId}:${secret}` : clientId;
}

function json(statusCode, body) {
  return { statusCode, headers: CORS, body: JSON.stringify(body) };
}

async function parseJson(resp) {
  const text = await resp.text();
  if (!text) return {};
  try { return JSON.parse(text); }
  catch { return { error: text.slice(0, 1000) }; }
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };
  if (event.httpMethod !== "GET") return json(405, { error: "Method not allowed", etsy_call_count: 0 });

  let etsyCalls = 0;
  try {
    const q = event.queryStringParameters || {};
    const listingId = String(q.listingId || "").trim();
    const inventoryOnly = q.inventory_only === "1" || String(q.inventory_only).toLowerCase() === "true";
    const accessToken = event.headers["access-token"] || event.headers["Access-Token"];
    const clientId = apiKey();

    if (!/^\d+$/.test(listingId)) return json(400, { error: "Missing or invalid listingId", etsy_call_count: 0 });
    if (!accessToken) return json(400, { error: "Missing access token", etsy_call_count: 0 });
    if (!clientId) return json(500, { error: "Missing CLIENT_ID", etsy_call_count: 0 });

    const url = `https://openapi.etsy.com/v3/application/listings/${listingId}/inventory`;
    etsyCalls++;
    const resp = await etsyFetch(url, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "x-api-key": clientId,
        "Content-Type": "application/json"
      }
    }, { bucket: "etsy-listing-console" });

    const payload = await parseJson(resp);
    if (!resp.ok) {
      // Pass Etsy's exact status and error through untouched.
      return json(resp.status, { ...payload, etsy_call_count: etsyCalls });
    }

    if (!inventoryOnly) {
      // Legacy consumers (SKU console).
      const products = Array.isArray(payload.products) ? payload.products.map(p => ({
        product_id: p.product_id,
        sku: (p.sku || "").trim()
      })) : [];
      return json(200, { listing_id: Number(listingId), products, etsy_call_count: etsyCalls });
    }

    const inventory = {
      products: decimalizeProducts(Array.isArray(payload.products) ? payload.products : []),
      price_on_property: payload.price_on_property || [],
      quantity_on_property: payload.quantity_on_property || [],
      sku_on_property: payload.sku_on_property || []
    };

    return json(200, {
      listing_id: Number(listingId),
      inventory,
      snapshot_hash: snapshotHash(inventory),
      pricing_health: pricingHealth(inventory),
      fetched_at: new Date().toISOString(),
      etsy_call_count: etsyCalls
    });
  } catch (err) {
    return json(500, { error: err.message, etsy_call_count: etsyCalls });
  }
};
