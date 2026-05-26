// /netlify/functions/listingImagesPurge.js
//
// Deletes ALL existing images from a draft Etsy listing.
//
// Called by runEtsyJob() in Index.html BEFORE uploading the new slot
// images, so each listing starts from a clean slate. Without this step
// Etsy's uploadListingImage endpoint APPENDS — so any pre-existing
// images on the draft (Etsy auto-placeholders, leftovers from a prior
// partial run, or anything the seller manually attached) would stack
// on top of the new uploads, producing duplicates and cross-pollination
// between listings.
//
// Etsy v3 endpoints used:
//   GET    https://api.etsy.com/v3/application/shops/{shop_id}/listings/{listing_id}/images
//   DELETE https://api.etsy.com/v3/application/shops/{shop_id}/listings/{listing_id}/images/{listing_image_id}
//
// Required headers on both:
//   Authorization: Bearer <user OAuth token>   (must have listings_d scope)
//   x-api-key:    <CLIENT_ID:CLIENT_SECRET>    (same format used by
//                                               imageUpload.js / etsyProxy.js)
//
// Required env vars (same set already used by imageUpload.js):
//   CLIENT_ID                — Etsy App API keystring
//   CLIENT_SECRET            — Etsy App shared secret
//   SHOP_ID                  — Etsy shop ID
// Aliases also accepted for parity with etsyProxy.js:
//   ETSY_CLIENT_ID / ETSY_API_KEY / API_KEY
//   ETSY_CLIENT_SECRET / ETSY_SHARED_SECRET
//
// Inputs (query string OR JSON body):
//   listingId — the Etsy draft listing ID
//   token     — the user's OAuth access token
//
// Response (200):
//   {
//     ok: true,
//     removed: <int>,        // how many DELETE calls succeeded
//     totalSeen: <int>,      // how many images the listing had before purge
//     failures: [{ imgId, status, detail }, ...]
//   }

const fetch = require("node-fetch");

const API_BASE = "https://api.etsy.com/v3/application";

exports.handler = async function (event) {
  try {
    // ---- Inputs -----------------------------------------------------
    const qs = event.queryStringParameters || {};
    let body = {};
    if (event.body) {
      try { body = JSON.parse(event.body); } catch (_) { /* allow other body shapes */ }
    }
    const listingId = String(qs.listingId || body.listingId || "").trim();
    const token =
      String(qs.token || body.token || "").trim() ||
      // Also accept the Authorization header for symmetry with other functions
      ((event.headers && (event.headers.authorization || event.headers.Authorization) || "")
        .replace(/^Bearer\s+/i, "").trim());

    if (!listingId) {
      return resp(400, { ok: false, error: "Missing listingId" });
    }
    if (!token) {
      return resp(401, { ok: false, error: "Missing access token" });
    }

    // ---- Env (mirrors imageUpload.js / etsyProxy.js) ----------------
    const clientId =
      process.env.CLIENT_ID ||
      process.env.ETSY_CLIENT_ID ||
      process.env.ETSY_API_KEY ||
      process.env.API_KEY;

    const clientSecret =
      process.env.CLIENT_SECRET ||
      process.env.ETSY_CLIENT_SECRET ||
      process.env.ETSY_SHARED_SECRET;

    const shopId = process.env.SHOP_ID;

    if (!clientId) {
      console.error("listingImagesPurge: missing Etsy app key env var.");
      return resp(500, {
        ok: false,
        error: "Missing Etsy app key env var for x-api-key header.",
        checked: ["CLIENT_ID", "ETSY_CLIENT_ID", "ETSY_API_KEY", "API_KEY"],
      });
    }
    if (!clientSecret) {
      console.error("listingImagesPurge: missing Etsy shared secret env var.");
      return resp(500, {
        ok: false,
        error: "Missing Etsy shared secret env var for x-api-key header.",
        checked: ["CLIENT_SECRET", "ETSY_CLIENT_SECRET", "ETSY_SHARED_SECRET"],
      });
    }
    if (!shopId) {
      console.error("listingImagesPurge: missing SHOP_ID env var.");
      return resp(500, { ok: false, error: "SHOP_ID environment variable is not set." });
    }

    const xApiKey = `${String(clientId).trim()}:${String(clientSecret).trim()}`;
    const baseHeaders = {
      Authorization: `Bearer ${token}`,
      "x-api-key": xApiKey,
      Accept: "application/json",
    };

    // ---- 1) List current images on the listing ----------------------
    let imageIds = [];
    {
      // The shop-scoped route is the newer convention and matches
      // imageUpload.js exactly.
      const listUrl = `${API_BASE}/shops/${encodeURIComponent(shopId)}/listings/${encodeURIComponent(listingId)}/images`;
      const listResp = await fetch(listUrl, { method: "GET", headers: baseHeaders });

      if (listResp.status === 401) {
        const t = await safeText(listResp);
        return resp(401, { ok: false, error: "unauthorized", detail: t });
      }
      if (!listResp.ok) {
        const t = await safeText(listResp);
        return resp(listResp.status, { ok: false, error: "list_failed", detail: t });
      }
      let json;
      try { json = await listResp.json(); } catch { json = {}; }
      const results = Array.isArray(json && json.results) ? json.results : [];
      imageIds = results.map(r => r && r.listing_image_id).filter(Boolean);
    }

    if (imageIds.length === 0) {
      return resp(200, { ok: true, removed: 0, totalSeen: 0, failures: [] });
    }

    // ---- 2) Delete each image (sequential, well below rate limit) ---
    // A draft listing carries at most ~10 images, so sequential DELETEs
    // are fine and stay well under Etsy's default 10 req/s ceiling.
    let removed = 0;
    const failures = [];
    for (const imgId of imageIds) {
      const delUrl =
        `${API_BASE}/shops/${encodeURIComponent(shopId)}` +
        `/listings/${encodeURIComponent(listingId)}` +
        `/images/${encodeURIComponent(imgId)}`;
      try {
        const delResp = await fetch(delUrl, { method: "DELETE", headers: baseHeaders });
        // Etsy returns 204 No Content on success.
        if (delResp.ok || delResp.status === 204) {
          removed++;
        } else {
          const t = await safeText(delResp);
          failures.push({ imgId, status: delResp.status, detail: t });
        }
      } catch (e) {
        failures.push({ imgId, error: String((e && e.message) || e) });
      }
    }

    return resp(200, {
      ok: true,
      removed,
      totalSeen: imageIds.length,
      failures,
    });
  } catch (err) {
    console.error("listingImagesPurge: fatal error:", err);
    return resp(500, { ok: false, error: String((err && err.message) || err) });
  }
};

function resp(statusCode, payload) {
  return {
    statusCode,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  };
}

async function safeText(r) {
  try { return await r.text(); } catch { return ""; }
}
