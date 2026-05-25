/* netlify/functions/_etsyMailTracking.js
 *
 * Shared tracking-snapshot core logic.
 *
 * This module does the full pipeline:
 *   1. Check Firestore cache (skip if forceRefresh)
 *   2. Call the carrier driver to fetch scan events
 *   3. Render the SVG/PNG timeline
 *   4. Upload PNG to Firebase Storage
 *   5. Write the cache doc
 *   6. Return a normalized result
 *
 * Called by:
 *   - etsyMailTrackingSnapshot.js  (public HTTP endpoint)
 *   - etsyMailDraftReply.js        (generate_tracking_image tool executor)
 *
 * Calling it directly (rather than via HTTP self-call) avoids:
 *   - Stacked Netlify function invocations (each has its own cold-start)
 *   - Timeout compounding (tool has ~9s, snapshot endpoint has ~10s, so
 *     a self-call leaves no margin for actual work)
 *   - Error-surfacing indirection (a 502 in the snapshot endpoint becomes
 *     an opaque "502" to the tool executor; direct calls preserve the
 *     original error object)
 *
 * Uses the shared ./firebaseAdmin module for admin + bucket initialization.
 */

const admin     = require("./firebaseAdmin");
const crypto    = require("crypto");
const carriers  = require("./_etsyMailCarriersRouter");
const renderer  = require("./_etsyMailTrackingRender");

const db     = admin.firestore();
const bucket = admin.storage().bucket();
const FV     = admin.firestore.FieldValue;

const CACHE_COLL     = "EtsyMail_TrackingCache";
const STORAGE_PREFIX = "etsymail/tracking/";

// ─── Cache TTL by status key ────────────────────────────────────────────
const TTL_MINUTES = {
  delivered       : Infinity,
  exception       : 60,
  out_for_delivery: 10,
  in_transit      : 15,
  pre_shipment    : 120,
  returned        : 60,
  rerouted        : 30,
  unknown         : 15
};

function isCacheFresh(cachedAt, statusKey) {
  if (!cachedAt) return false;
  const ttl = TTL_MINUTES[statusKey] ?? 15;
  if (ttl === Infinity) return true;
  const cachedMs = cachedAt.toMillis ? cachedAt.toMillis() : new Date(cachedAt).getTime();
  const ageMin   = (Date.now() - cachedMs) / 60000;
  return ageMin < ttl;
}

async function readCache(trackingCode) {
  const ref = db.collection(CACHE_COLL).doc(trackingCode);
  const snap = await ref.get();
  if (!snap.exists) return null;
  return { id: snap.id, ...snap.data() };
}

async function writeCache(trackingCode, data) {
  const ref = db.collection(CACHE_COLL).doc(trackingCode);
  await ref.set({
    ...data,
    cachedAt : FV.serverTimestamp(),
    updatedAt: FV.serverTimestamp()
  }, { merge: true });
}

async function uploadPng(trackingCode, pngBuffer) {
  const safeCode = String(trackingCode).replace(/[^a-zA-Z0-9]/g, "_");
  const path = `${STORAGE_PREFIX}${safeCode}.png`;

  const file = bucket.file(path);
  const downloadToken = crypto.randomUUID();

  await file.save(pngBuffer, {
    metadata: {
      contentType : "image/png",
      cacheControl: "public, max-age=900",
      metadata: {
        firebaseStorageDownloadTokens: downloadToken
      }
    },
    resumable: false
  });

  const encodedPath = encodeURIComponent(path);
  const url = `https://firebasestorage.googleapis.com/v0/b/${bucket.name}/o/${encodedPath}?alt=media&token=${downloadToken}`;

  return { path, url };
}

/**
 * Run the full tracking snapshot pipeline.
 *
 * @param {string} trackingCode
 * @param {object} [options]
 * @param {boolean} [options.forceRefresh=false]  Bypass cache
 * @param {string}  [options.carrierHint]          "usps" | "chitchats"
 *
 * @returns {Promise<{
 *   ok: boolean,
 *   trackingCode: string,
 *   carrier: string,
 *   carrierDisplay: string,
 *   status: string,
 *   statusKey: string,
 *   estimatedDelivery: string | null,
 *   destination: string | null,
 *   origin: string | null,
 *   shipDate: string | null,
 *   resolvedAt: string | null,
 *   events: Array,
 *   imageUrl: string,
 *   imageStoragePath: string,
 *   imageWidth: number,
 *   imageHeight: number,
 *   cached: boolean,
 *   generatedAt: string,
 *   durationMs: number
 * }>}
 *
 * @throws Error with .code set to one of:
 *   UNKNOWN_CARRIER | NOT_FOUND | APIFY_NO_RESULTS | APIFY_ERROR |
 *   APIFY_NETWORK | RENDER_FAILED | UPLOAD_FAILED
 */
async function snapshot(trackingCode, options = {}) {
  const { forceRefresh = false, carrierHint = "" } = options;
  const tStart = Date.now();

  const code = String(trackingCode || "").trim();
  if (!code) {
    const err = new Error("Missing trackingCode");
    err.code = "INVALID_INPUT";
    throw err;
  }

  // ─── 1. Cache lookup ──
  if (!forceRefresh) {
    try {
      const cached = await readCache(code);
      if (cached && cached.imageUrl && isCacheFresh(cached.cachedAt, cached.statusKey)) {
        return {
          ok               : true,
          trackingCode     : code,
          carrier          : cached.carrier,
          carrierDisplay   : cached.carrierDisplay,
          status           : cached.status,
          statusKey        : cached.statusKey,
          estimatedDelivery: cached.estimatedDelivery || null,
          destination      : cached.destination || null,
          origin           : cached.origin || null,
          shipDate         : cached.shipDate || null,
          resolvedAt       : cached.resolvedAt || null,
          events           : cached.events || [],
          imageUrl         : cached.imageUrl,
          imageStoragePath : cached.imageStoragePath,
          imageWidth       : cached.imageWidth,
          imageHeight      : cached.imageHeight,
          cached           : true,
          cachedAt         : cached.cachedAt?.toDate?.()?.toISOString() || null,
          generatedAt      : new Date().toISOString(),
          durationMs       : Date.now() - tStart
        };
      }
    } catch (e) {
      // Cache read failure shouldn't fail the whole request. Log + continue.
      console.warn(`[tracking] cache read failed for ${code}:`, e.message);
    }
  }

  // ─── 2. Fetch fresh from carrier ──
  const tracking = await carriers.lookupTracking(code, { carrierHint });

  // ─── 3. Render image (or use pre-rendered screenshot from driver) ──
  let rendered;
  if (tracking.imageBuffer && Buffer.isBuffer(tracking.imageBuffer)) {
    // Driver already produced a PNG (e.g. USPS Puppeteer screenshot).
    // Use it directly instead of running the SVG renderer.
    console.log(`[tracking] using pre-rendered screenshot from driver (${tracking.imageBuffer.length} bytes, ${tracking.imageSource || "unknown source"})`);
    rendered = {
      png   : tracking.imageBuffer,
      width : tracking.imageWidth || 0,
      height: tracking.imageHeight || 0
    };
  } else {
    try {
      rendered = await renderer.render(tracking);
    } catch (e) {
      const err = new Error(`Image render failed: ${e.message}`);
      err.code = "RENDER_FAILED";
      throw err;
    }
  }

  // ─── 4. Upload to Storage ──
  let upload;
  try {
    upload = await uploadPng(code, rendered.png);
  } catch (e) {
    const err = new Error(`Storage upload failed: ${e.message}`);
    err.code = "UPLOAD_FAILED";
    throw err;
  }

  // ─── 5. Build response + write cache ──
  const result = {
    ok               : true,
    trackingCode     : code,
    carrier          : tracking.carrier,
    carrierDisplay   : tracking.carrierDisplay,
    status           : tracking.status,
    statusKey        : tracking.statusKey,
    estimatedDelivery: tracking.estimatedDelivery || null,
    destination      : tracking.destination || null,
    origin           : tracking.origin || null,
    shipDate         : tracking.shipDate || null,
    resolvedAt       : tracking.resolvedAt || null,
    events           : tracking.events || [],
    imageUrl         : upload.url,
    imageStoragePath : upload.path,
    imageWidth       : rendered.width,
    imageHeight      : rendered.height,
    cached           : false,
    generatedAt      : new Date().toISOString(),
    durationMs       : Date.now() - tStart
  };

  // Cache write is fire-and-forget; don't block on it
  writeCache(code, {
    carrier          : result.carrier,
    carrierDisplay   : result.carrierDisplay,
    status           : result.status,
    statusKey        : result.statusKey,
    estimatedDelivery: result.estimatedDelivery,
    destination      : result.destination,
    origin           : result.origin,
    shipDate         : result.shipDate,
    resolvedAt       : result.resolvedAt,
    events           : result.events,
    imageUrl         : result.imageUrl,
    imageStoragePath : result.imageStoragePath,
    imageWidth       : result.imageWidth,
    imageHeight      : result.imageHeight
  }).catch((e) => {
    console.warn(`[tracking] cache write failed for ${code}:`, e.message);
  });

  return result;
}

module.exports = { snapshot };
