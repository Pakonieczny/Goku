// netlify/functions/etsyListingBatchSnapshot.js
//
// Amortizes the two read calls every Etsy listing used to pay individually:
// one listing/images read and one inventory read. Etsy's official batch
// endpoints accept up to 100 IDs; the browser primes the next 25 queued
// drafts, and this function stores each result in Firestore for the purge and
// inventory-update functions to consume.

const { etsyFetch } = require("./etsyRateLimiter");

const API_BASE = "https://api.etsy.com/v3/application";
const COLLECTION = "EtsyListingSnapshots";
const SNAPSHOT_TTL_MS = 30 * 60 * 1000;
const MAX_IDS = 100;

let db = null;
try {
  db = require("./firebaseAdmin").firestore();
} catch (error) {
  console.warn("etsyListingBatchSnapshot: Firestore unavailable.", error.message);
}

exports.handler = async function (event) {
  const headers = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Access-Control-Allow-Methods": "POST,OPTIONS",
    "Content-Type": "application/json",
  };
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers, body: "" };
  }
  if (event.httpMethod !== "POST") {
    return reply(405, { ok: false, error: "Method not allowed" }, headers);
  }
  if (!db) {
    // Soft failure: callers retain their existing per-listing read fallback.
    return reply(503, { ok: false, error: "Snapshot cache unavailable" }, headers);
  }

  try {
    let body = {};
    try { body = event.body ? JSON.parse(event.body) : {}; } catch (_) {}

    const ids = Array.from(new Set(
      (Array.isArray(body.listingIds) ? body.listingIds : [])
        .map((value) => String(value || "").trim())
        .filter((value) => /^\d{1,20}$/.test(value))
    )).slice(0, MAX_IDS);
    const token =
      String(body.token || "").trim() ||
      String(event.headers?.authorization || event.headers?.Authorization || "")
        .replace(/^Bearer\s+/i, "").trim();

    if (!ids.length) return reply(400, { ok: false, error: "No valid listing IDs" }, headers);
    if (!token) return reply(401, { ok: false, error: "Missing access token" }, headers);

    const clientId =
      process.env.CLIENT_ID ||
      process.env.ETSY_CLIENT_ID ||
      process.env.ETSY_API_KEY ||
      process.env.API_KEY;
    const clientSecret =
      process.env.CLIENT_SECRET ||
      process.env.ETSY_CLIENT_SECRET ||
      process.env.ETSY_SHARED_SECRET;
    if (!clientId || !clientSecret) {
      return reply(500, { ok: false, error: "Missing Etsy app credentials" }, headers);
    }

    const now = Date.now();
    const refs = ids.map((id) => db.collection(COLLECTION).doc(id));
    const cached = await db.getAll(...refs);
    const missingImages = [];
    const missingInventory = [];

    cached.forEach((snap, index) => {
      const id = ids[index];
      const data = snap.exists ? (snap.data() || {}) : {};
      if (!Array.isArray(data.images) || now - Number(data.imagesCapturedAt || 0) > SNAPSHOT_TTL_MS) {
        missingImages.push(id);
      }
      if (!Object.prototype.hasOwnProperty.call(data, "inventory") ||
          now - Number(data.inventoryCapturedAt || 0) > SNAPSHOT_TTL_MS) {
        missingInventory.push(id);
      }
    });

    const commonHeaders = {
      Accept: "application/json",
      Authorization: `Bearer ${token}`,
      "x-api-key": `${String(clientId).trim()}:${String(clientSecret).trim()}`,
    };
    const errors = [];
    let etsyCalls = 0;
    const patches = new Map();
    const patchFor = (id) => {
      if (!patches.has(id)) patches.set(id, {});
      return patches.get(id);
    };

    // These are independent reads. Run them concurrently; the distributed
    // limiter still controls Etsy QPS, while the Netlify invocation avoids
    // paying two full network latencies sequentially.
    const reads = [];
    if (missingImages.length) {
      etsyCalls++;
      reads.push((async () => {
        const url =
          `${API_BASE}/listings/batch?listing_ids=${encodeURIComponent(missingImages.join(","))}` +
          "&includes=Images";
        const response = await etsyFetch(url, { method: "GET", headers: commonHeaders });
        const text = await response.text();
        let json; try { json = JSON.parse(text); } catch { json = {}; }
        if (!response.ok) {
          errors.push({ kind: "images", status: response.status, detail: text.slice(0, 300) });
          return;
        }
        for (const listing of extractResults(json)) {
          const id = String(listing?.listing_id || listing?.listingId || "").trim();
          if (!missingImages.includes(id)) continue;
          const images =
            Array.isArray(listing?.images) ? listing.images :
            Array.isArray(listing?.Images) ? listing.Images : [];
          Object.assign(patchFor(id), {
            images: images.map(compactImage).filter(Boolean),
            imagesCapturedAt: now,
          });
        }
      })());
    }

    if (missingInventory.length) {
      etsyCalls++;
      reads.push((async () => {
        const url =
          `${API_BASE}/listings/batch/inventory?listing_ids=` +
          encodeURIComponent(missingInventory.join(","));
        const response = await etsyFetch(url, { method: "GET", headers: commonHeaders });
        const text = await response.text();
        let json; try { json = JSON.parse(text); } catch { json = {}; }
        if (!response.ok) {
          errors.push({ kind: "inventory", status: response.status, detail: text.slice(0, 300) });
          return;
        }
        for (const listing of extractResults(json)) {
          const id = String(listing?.listing_id || listing?.listingId || "").trim();
          if (!missingInventory.includes(id)) continue;
          const inventory =
            Object.prototype.hasOwnProperty.call(listing || {}, "inventory")
              ? listing.inventory
              : (listing?.products ? listing : null);
          Object.assign(patchFor(id), { inventory, inventoryCapturedAt: now });
        }
      })());
    }
    await Promise.all(reads);

    if (patches.size) {
      const batch = db.batch();
      for (const [id, patch] of patches) {
        batch.set(
          db.collection(COLLECTION).doc(id),
          { ...patch, updatedAt: now },
          { merge: true }
        );
      }
      await batch.commit();
    }

    return reply(200, {
      ok: errors.length === 0,
      requested: ids.length,
      etsyCalls,
      cachedReads: (missingImages.length ? 0 : 1) + (missingInventory.length ? 0 : 1),
      refreshedImages: missingImages.length,
      refreshedInventory: missingInventory.length,
      errors,
    }, headers);
  } catch (error) {
    console.error("etsyListingBatchSnapshot:", error);
    return reply(500, { ok: false, error: error.message }, headers);
  }
};

function extractResults(json) {
  if (Array.isArray(json)) return json;
  if (Array.isArray(json?.results)) return json.results;
  if (Array.isArray(json?.listings)) return json.listings;
  return [];
}

function compactImage(image) {
  const id = image?.listing_image_id;
  if (!id) return null;
  return {
    listing_image_id: id,
    rank: Number(image.rank) || 0,
    alt_text: String(image.alt_text || "").slice(0, 250),
    url_fullxfull: image.url_fullxfull || null,
    url_570xN: image.url_570xN || null,
    url: image.url || null,
  };
}

function reply(statusCode, payload, headers) {
  return { statusCode, headers, body: JSON.stringify(payload) };
}
