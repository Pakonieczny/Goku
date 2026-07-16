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

    // ---- Selective mode: ?imageIds=1,2,3 deletes ONLY those images ----
    // Used by the post-upload reconciliation pass to remove duplicate
    // retry-twins without touching the listing's legitimate images.
    const onlyRaw =
      (event.queryStringParameters && event.queryStringParameters.imageIds) ||
      body.imageIds ||
      "";
    const selective = (Array.isArray(onlyRaw) ? onlyRaw : String(onlyRaw).split(","))
      .map(s => String(s || "").trim())
      .filter(Boolean);

    // ---- 1) List current images on the listing ----------------------
    let imageIds = [];
    if (selective.length) {
      imageIds = selective;
    } else {
      // ENDPOINT FIX: the shop-scoped images route
      // (/shops/{shop}/listings/{id}/images) does NOT return dormant image
      // associations on DRAFT listings — drafts that inherited images
      // (e.g. via Etsy's Copy) list as EMPTY here, so list-then-delete
      // silently removed nothing while getListing?includes=Images (and the
      // published storefront) showed the very same records. Diagnostics
      // proved the divergence live: purge saw 0, includes=Images saw 4.
      // List through the endpoint that actually sees them.
      const listUrl = `${API_BASE}/listings/${encodeURIComponent(listingId)}?includes=Images`;
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
      const results = Array.isArray(json && json.images) ? json.images
                    : Array.isArray(json && json.Images) ? json.Images
                    : Array.isArray(json && json.results) ? json.results : [];
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
      // DELETE is idempotent: a 404 means the record is already gone, which
      // IS the goal — count it as removed. Transient Etsy errors (5xx/409)
      // get exactly one retry after a short pause; a repeat 404 on retry is
      // likewise success.
      let done = false, lastStatus = 0, lastDetail = "";
      for (let attempt = 0; attempt < 2 && !done; attempt++) {
        if (attempt > 0) await new Promise(r => setTimeout(r, 1200));
        try {
          const delResp = await fetch(delUrl, { method: "DELETE", headers: baseHeaders });
          if (delResp.ok || delResp.status === 204 || delResp.status === 404) {
            removed++; done = true;
          } else {
            lastStatus = delResp.status;
            lastDetail = await safeText(delResp);
            if (delResp.status === 401 || delResp.status === 403) break; // auth/scope: retry won't help
          }
        } catch (e) {
          lastStatus = 0; lastDetail = String((e && e.message) || e);
        }
      }
      if (!done) failures.push({ imgId, status: lastStatus, detail: String(lastDetail).slice(0, 200) });
    }

    return resp(200, {
      ok: true,
      removed,
      totalSeen: imageIds.length,
      selective: selective.length > 0,
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
