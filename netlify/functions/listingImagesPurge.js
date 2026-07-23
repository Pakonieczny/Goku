// /netlify/functions/listingImagesPurge.js
//
// Reconciles existing images on a draft Etsy listing.
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
// preserveAltTexts may contain the stable alt text of generic brand images.
// One matching image per value is retained instead of being deleted and then
// uploaded again. The next main-image uploads push those retained records to
// the reserved ranks, saving two DELETEs plus two POSTs on prepared drafts.
//
// Response (200):
//   {
//     ok: true,
//     removed: <int>,        // how many DELETE calls succeeded
//     totalSeen: <int>,      // how many images the listing had before purge
//     deleteRequested: <int>,
//     keptImages: [...],
//     failures: [{ imgId, status, detail }, ...]
//   }

const { etsyFetch } = require("./etsyRateLimiter");

const API_BASE = "https://api.etsy.com/v3/application";
const SNAPSHOT_COLLECTION = "EtsyListingSnapshots";
const SNAPSHOT_TTL_MS = 30 * 60 * 1000;

let db = null;
try {
  db = require("./firebaseAdmin").firestore();
} catch (error) {
  console.warn("listingImagesPurge: snapshot cache unavailable.", error.message);
}

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
    const preserveRaw = body.preserveAltTexts || qs.preserveAltTexts || [];
    const preserveAltTexts = (Array.isArray(preserveRaw) ? preserveRaw : String(preserveRaw).split("|"))
      .map(normalizeAlt)
      .filter(Boolean);

    // ---- 1) List current images on the listing ----------------------
    let imageIds = [];
    let imageRecords = [];
    let source = "selective";
    if (selective.length) {
      imageIds = selective;
    } else {
      // The batch-prime function normally populated this seconds ago. Reading
      // the Firestore snapshot costs no Etsy quota. If it is missing/stale,
      // fall back to Etsy so correctness never depends on the optimization.
      if (db) {
        try {
          const snap = await db.collection(SNAPSHOT_COLLECTION).doc(listingId).get();
          const data = snap.exists ? (snap.data() || {}) : {};
          if (Array.isArray(data.images) &&
              Date.now() - Number(data.imagesCapturedAt || 0) <= SNAPSHOT_TTL_MS) {
            imageRecords = data.images;
            source = "batch-cache";
          }
        } catch (error) {
          console.warn("listingImagesPurge: cache read failed; using Etsy.", error.message);
        }
      }

      // ENDPOINT FIX: the shop-scoped images route
      // (/shops/{shop}/listings/{id}/images) does NOT return dormant image
      // associations on DRAFT listings — drafts that inherited images
      // (e.g. via Etsy's Copy) list as EMPTY here, so list-then-delete
      // silently removed nothing while getListing?includes=Images (and the
      // published storefront) showed the very same records. Diagnostics
      // proved the divergence live: purge saw 0, includes=Images saw 4.
      // List through the endpoint that actually sees them.
      if (!imageRecords.length && source !== "batch-cache") {
        const listUrl = `${API_BASE}/listings/${encodeURIComponent(listingId)}?includes=Images`;
        const listResp = await etsyFetch(listUrl, { method: "GET", headers: baseHeaders });

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
        imageRecords = Array.isArray(json && json.images) ? json.images
                     : Array.isArray(json && json.Images) ? json.Images
                     : Array.isArray(json && json.results) ? json.results : [];
        source = "etsy";
      }

      // Retain at most one exact match for each requested brand-image alt.
      // Everything else remains subject to the clean-gallery purge.
      const unmatched = new Set(preserveAltTexts);
      const keptImages = [];
      const deleteRecords = [];
      for (const image of imageRecords) {
        const alt = normalizeAlt(image?.alt_text);
        if (alt && unmatched.has(alt)) {
          unmatched.delete(alt);
          keptImages.push(compactImage(image));
        } else {
          deleteRecords.push(image);
        }
      }
      imageIds = deleteRecords.map(r => r && r.listing_image_id).filter(Boolean);
      body.__keptImages = keptImages.filter(Boolean);
      body.__totalSeen = imageRecords.length;
    }

    if (imageIds.length === 0) {
      if (db && !selective.length) await invalidateImageSnapshot(listingId);
      return resp(200, {
        ok: true,
        removed: 0,
        totalSeen: selective.length ? 0 : Number(body.__totalSeen || 0),
        deleteRequested: 0,
        keptImages: body.__keptImages || [],
        source,
        failures: []
      });
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
      // IS the goal. Retry only statuses that can genuinely recover; retrying
      // deterministic 4xx responses wastes quota.
      let done = false, lastStatus = 0, lastDetail = "";
      for (let attempt = 0; attempt < 2 && !done; attempt++) {
        if (attempt > 0) await new Promise(r => setTimeout(r, 1200));
        try {
          const delResp = await etsyFetch(delUrl, { method: "DELETE", headers: baseHeaders });
          if (delResp.ok || delResp.status === 204 || delResp.status === 404) {
            removed++; done = true;
          } else {
            lastStatus = delResp.status;
            lastDetail = await safeText(delResp);
            const retryable =
              [408, 409, 425, 500, 502, 503, 504].includes(delResp.status);
            if (!retryable) break;
          }
        } catch (e) {
          lastStatus = 0; lastDetail = String((e && e.message) || e);
        }
      }
      if (!done) failures.push({ imgId, status: lastStatus, detail: String(lastDetail).slice(0, 200) });
    }

    if (db && !selective.length) await invalidateImageSnapshot(listingId);
    return resp(200, {
      ok: true,
      removed,
      totalSeen: selective.length ? imageIds.length : Number(body.__totalSeen || imageIds.length),
      deleteRequested: imageIds.length,
      keptImages: body.__keptImages || [],
      source,
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

function normalizeAlt(value) {
  return String(value || "").trim().replace(/\s+/g, " ").toLowerCase();
}

function compactImage(image) {
  if (!image?.listing_image_id) return null;
  return {
    listing_image_id: image.listing_image_id,
    rank: Number(image.rank) || 0,
    alt: String(image.alt_text || "").slice(0, 250),
    src: image.url_fullxfull || image.url_570xN || image.url || null,
  };
}

async function invalidateImageSnapshot(listingId) {
  try {
    await db.collection(SNAPSHOT_COLLECTION).doc(String(listingId)).set(
      { imagesCapturedAt: 0, updatedAt: Date.now() },
      { merge: true }
    );
  } catch (error) {
    console.warn("listingImagesPurge: cache invalidation failed.", error.message);
  }
}
