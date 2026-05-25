/*  netlify/functions/etsyMailOrder.js
 *
 *  Live Etsy receipt fetcher for the inbox customer panel.
 *
 *  When an operator clicks an order in the customer panel, the inbox calls:
 *    GET /.netlify/functions/etsyMailOrder?receiptId=4040875933
 *
 *  This endpoint:
 *    1. Reads the seeded OAuth token from Firestore (config/etsyOauth)
 *    2. Auto-refreshes if within 2 min of expiry
 *    3. Calls Etsy's getShopReceipt endpoint with transactions + variations
 *    4. Returns the raw receipt JSON to the client
 *
 *  Unlike your existing etsyOrderProxy.js, this does NOT require the client
 *  to pass an access-token header — the inbox (different subdomain) doesn't
 *  have those tokens in its localStorage. Server-side token management only.
 *
 *  No auth on this endpoint because:
 *    - It's read-only
 *    - The receiptId must match a receipt from OUR shop (Etsy enforces this
 *      server-side via the shops/{shop_id}/receipts/{receipt_id} path)
 *    - Callers already need to know a specific receipt ID
 *
 *  If you want to tighten this later, add requireExtensionAuth or a CORS
 *  origin allowlist check.
 *
 *  ═══ METERING ═══════════════════════════════════════════════════════════
 *  This file does NOT use _etsyMailEtsy.js (it has its own inline OAuth +
 *  fetch code, kept for compatibility). To make sure every API call is
 *  still tracked, we explicitly bump the meter at each of its three call
 *  sites:
 *    - order.oauthRefresh  → refreshEtsyToken()
 *    - order.receiptFetch  → main /shops/{shop}/receipts/{rid} fetch
 *    - order.imageFetch    → per-listing /listings/{id}/images fetch
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
const TOKEN_REFRESH_BUFFER_MS = 2 * 60 * 1000;

const CORS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "GET,OPTIONS"
};

function json(statusCode, body) {
  return {
    statusCode,
    headers: { ...CORS, "Content-Type": "application/json" },
    body: JSON.stringify(body)
  };
}

// OAuth token helpers — v5.1, replaced inline duplicate with shared
// helper from _etsyMailEtsy.js. The shared helper has Firestore-backed
// caching plus a module-level in-memory cache that survives across
// invocations within the same warm Node process.
const { getValidEtsyAccessToken } = require("./_etsyMailEtsy");

exports.handler = meter.wrapHandler(async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: CORS, body: "" };
  }
  if (event.httpMethod !== "GET") {
    return json(405, { error: "Method Not Allowed" });
  }

  const qs = event.queryStringParameters || {};
  const receiptId = qs.receiptId;

  if (!receiptId) return json(400, { error: "Missing receiptId query parameter" });
  if (!/^\d+$/.test(String(receiptId))) {
    return json(400, { error: "receiptId must be numeric" });
  }

  if (!SHOP_ID || !CLIENT_ID || !CLIENT_SECRET) {
    return json(500, { error: "Missing SHOP_ID / CLIENT_ID / CLIENT_SECRET env vars" });
  }

  try {
    const accessToken = await getValidEtsyAccessToken();

    // Step 1 — fetch the receipt with its transactions.
    const url =
      `https://api.etsy.com/v3/application/shops/${SHOP_ID}` +
      `/receipts/${receiptId}?includes=` +
      ["Transactions", "Transactions.personalization", "Transactions.variations"].join(",");

    const t1 = meter.bump("order.receiptFetch");
    let res;
    try {
      res = await fetch(url, {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          "x-api-key": `${CLIENT_ID}:${CLIENT_SECRET}`,
          "Content-Type": "application/json"
        }
      });
    } catch (err) {
      t1.failNet();
      throw err;
    }
    t1.fromHttp(res.status);

    const payload = await res.json();
    if (!res.ok) {
      return json(res.status, { error: (payload && payload.error) || `Etsy API ${res.status}`, details: payload });
    }

    // Step 2 — enrich each transaction with a listing thumbnail + listing URL.
    //
    // Etsy's getShopReceipt doesn't include listing images in the transaction
    // payload (despite supporting various `includes`, images aren't one of the
    // options for transactions). We fetch them in parallel from
    // getListingImages for each unique listing_id in the receipt.
    //
    // Typical receipt has 1-5 line items → 1-5 parallel image fetches. At ~200ms
    // each with parallelism, this adds ~200-400ms to the modal open.
    //
    // We pick the FIRST image (index 0) for each listing — the primary product
    // photo. If getListingImages fails for a listing (e.g. listing was deleted
    // since the purchase), we fall back to null and the UI shows a placeholder.
    const transactions = Array.isArray(payload.transactions) ? payload.transactions : [];
    const uniqueListingIds = Array.from(new Set(
      transactions.map(t => t.listing_id).filter(id => id != null)
    ));

    const imageUrlByListingId = {};
    if (uniqueListingIds.length) {
      await Promise.all(uniqueListingIds.map(async (listingId) => {
        const t2 = meter.bump("order.imageFetch");
        let imgRes;
        try {
          imgRes = await fetch(
            `https://api.etsy.com/v3/application/listings/${listingId}/images`,
            {
              headers: {
                Authorization: `Bearer ${accessToken}`,
                "x-api-key": `${CLIENT_ID}:${CLIENT_SECRET}`,
                "Content-Type": "application/json"
              }
            }
          );
        } catch (err) {
          t2.failNet();
          // Don't let one failed image break the whole modal
          console.warn(`Image fetch failed for listing ${listingId}:`, err.message);
          return;
        }
        t2.fromHttp(imgRes.status);
        try {
          if (!imgRes.ok) return;  // skip on failure; UI handles gracefully
          const imgData = await imgRes.json();
          const results = Array.isArray(imgData.results) ? imgData.results : [];
          if (!results.length) return;
          // Prefer a reasonably-sized thumbnail. Etsy's listing-image object has
          // url_75x75, url_170x135, url_224xN, url_340x270, url_570xN, url_fullxfull.
          // 170x135 is ideal for a modal row thumbnail.
          const img = results[0];
          imageUrlByListingId[listingId] =
            img.url_170x135 ||
            img.url_224xN ||
            img.url_75x75 ||
            img.url_340x270 ||
            img.url_570xN ||
            img.url_fullxfull ||
            null;
        } catch (err) {
          // Don't let one failed image break the whole modal
          console.warn(`Image fetch parse failed for listing ${listingId}:`, err.message);
        }
      }));
    }

    // Step 3 — attach thumbnail + listingUrl to each transaction.
    for (const t of transactions) {
      if (t.listing_id) {
        t.thumbnail_url = imageUrlByListingId[t.listing_id] || null;
        t.listing_url = `https://www.etsy.com/listing/${t.listing_id}`;
      } else {
        t.thumbnail_url = null;
        t.listing_url = null;
      }
    }

    return json(200, payload);

  } catch (err) {
    console.error("etsyMailOrder error:", err);
    return json(500, { error: err.message || "Unknown error" });
  }
});
