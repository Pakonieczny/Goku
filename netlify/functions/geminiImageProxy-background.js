/* netlify/functions/geminiImageProxy-background.js
   Background Function: runs long Gemini image generation/edits without browser/edge inactivity 504s.
   Writes realtime status to Firestore + uploads final PNG to Firebase Storage.
*/

const admin = require("./firebaseAdmin");
// const sharp = require("sharp"); // ensure sharp is installed in package.json
const { initializeFirestore, getFirestore } = require("firebase-admin/firestore");

// Node 18 on Netlify provides fetch/FormData/Blob globally.
// If your build ever lacks fetch, uncomment:
// const fetch = require("node-fetch");

const JOBS_COLL = "ListingGenerator1Jobs";
const IMAGES_COLL = "ListingGenerator1Images";

// -------------------------
// Storage bucket selection
// -------------------------
function getBucket() {
  const name =
    process.env.FIREBASE_STORAGE_BUCKET ||
    process.env.GCLOUD_STORAGE_BUCKET ||
    admin.app()?.options?.storageBucket ||
    "gokudatabase.firebasestorage.app";
  return admin.storage().bucket(name);
}

// Hard-lock the Gemini image model (ignore any client-provided model)
const GEMINI_IMAGE_MODEL = "gemini-3.1-flash-image-preview";

const GENERATABLE_CATEGORIES = new Set([
  "Beady_Necklace",
  "Regular_Necklace",
  "Stud_Earrings",
  "Hoop_Earrings",
  "Charms",
  "Bracelets",
]);

// ---- helpers ----

function normalizeCategory(s) {
  return String(s || "").trim();
}

function json(statusCode, obj) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
      "Access-Control-Allow-Methods": "POST, OPTIONS",
    },
    body: JSON.stringify(obj),
  };
}

function parseJsonBody(event) {
  try {
    return event?.body ? JSON.parse(event.body) : null;
  } catch {
    return null;
  }
}

function dataUrlToBuffer(dataUrl) {
  // data:image/png;base64,xxxx
  const m = /^data:([^;]+);base64,(.+)$/.exec(dataUrl || "");
  if (!m) throw new Error("input_image must be a data URL: data:<mime>;base64,<...>");
  const mime = m[1];
  const b64 = m[2];
  return { mime, buffer: Buffer.from(b64, "base64") };
}

/**
 * Read an already-uploaded image from Firebase Storage.
 * Avoids sending large base64 payloads from the browser.
 */
async function storagePathToBuffer(storagePath) {
  const p = String(storagePath || "").trim();
  if (!p) throw new Error("input_storage_path must be a non-empty string");

  // Allowlist to prevent arbitrary bucket reads.
  const ALLOWED_INPUT_PREFIXES = [
    "listing-generator-1/Beady_Necklace/",
    "listing-generator-1/Regular_Necklace/",
    "listing-generator-1/Stud_Earrings/",
    "listing-generator-1/Hoop_Earrings/",
    "listing-generator-1/Charms/",
    "listing-generator-1/Bracelets/",
    "listing-generator-1/Charm_Maker/", // ✅ Updated to cover the new structure broadly
    "listing-generator-1/generated/",
  ];

  if (!ALLOWED_INPUT_PREFIXES.some((prefix) => p.startsWith(prefix))) {
    throw new Error("input_storage_path not allowed: " + p);
  }

  const bucket = getBucket();
  const file = bucket.file(p);

  const [exists] = await file.exists();
  if (!exists) throw new Error(`input_storage_path not found: ${p}`);

  let mime = "application/octet-stream";
  try {
    const [meta] = await file.getMetadata();
    if (meta?.contentType) mime = meta.contentType;
  } catch {
    // ignore metadata failure; still download bytes
  }

  const [buffer] = await file.download();
  return { mime, buffer };
}

function safeErr(err) {
  return {
    message: err?.message || String(err),
    name: err?.name,
    stack: err?.stack,
  };
}

function clampNumber(n, min, max, fallback) {
  const x = Number(n);
  if (!Number.isFinite(x)) return fallback;
  return Math.max(min, Math.min(max, x));
}

// ============================================================
// Set-number allocation
// ------------------------------------------------------------
// Atomic Set_N allocator backed by Firestore counter docs.
//
// PROBLEM IT SOLVES: The previous implementation scanned Firebase
// Storage for the highest Set_N folder and returned maxN+1. That
// works only when set folders are physically created before the next
// alloc runs. In batch mode, a submission can reserve 4 set numbers
// minutes before any files are written (Gemini takes hours), and
// Storage doesn't track empty folders. So a second submission run
// before the first collected would see no Set folders, hand out the
// same numbers, and overwrite the first batch on collection.
//
// FIX: Each category has a Firestore document at
//   listingGenerator1/setCounters/{category}
// with field `nextSetN`. allocNextSet runs a transaction that reads
// the current value, increments by 1, and writes back. Strictly
// monotonic across any number of concurrent callers.
//
// BOOTSTRAP: On first ever call for a category, the counter doc
// doesn't exist. We seed it from Storage by scanning for the highest
// existing Set_N and starting from there + 1. This way, deployments
// that already have Set_42 in Storage start the counter at 43.
// ============================================================
const SET_COUNTERS_COLL = "listingGenerator1_setCounters";

async function _seedSetCounter(cat) {
  const bucket = admin.storage().bucket();
  const prefix = `listing-generator-1/${cat}/Ready_To_List/`;
  const [_files, _next, apiResponse] = await bucket.getFiles({
    prefix,
    delimiter: "/",
    autoPaginate: false,
  });
  const prefixes = apiResponse?.prefixes || [];
  let maxN = 0;
  for (const p of prefixes) {
    const m = p.match(/\/Set_(\d+)\/$/);
    if (m) maxN = Math.max(maxN, Number(m[1]) || 0);
  }
  return maxN;
}

async function allocNextSet(activeCategory) {
  const cat = normalizeCategory(activeCategory);
  if (!GENERATABLE_CATEGORIES.has(cat)) throw new Error("activeCategory not generatable");

  const db = getDb();
  const ref = db.collection(SET_COUNTERS_COLL).doc(cat);

  const setN = await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    let current;
    if (snap.exists) {
      current = Number(snap.data()?.nextSetN || 0);
    } else {
      // First call ever for this category. Seed from Storage to avoid
      // colliding with any existing Set folders. We do this OUTSIDE
      // the transaction conceptually, but reading Storage from inside
      // a Firestore transaction is allowed (it's not a Firestore op,
      // just a network call) — the transaction will retry if the
      // create races another caller.
      const seed = await _seedSetCounter(cat);
      current = seed; // counter holds "highest used"; next is +1
    }
    const next = current + 1;
    tx.set(ref, {
      nextSetN: next,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    return next;
  });

  const outputBasePath = `listing-generator-1/${cat}/Ready_To_List/Set_${setN}`;
  return { setN, outputBasePath };
}

function assertAllowedOutputBase(base) {
  const b = String(base || "").trim();
  // Must be: listing-generator-1/{Category}/Ready_To_List/Set_N
  // OR: listing-generator-1/Charm_Maker/Generated_Charm_Sets/Deriv_N
  const isSet = /^listing-generator-1\/[^/]+\/Ready_To_List\/Set_\d+$/i.test(b);
  const isDeriv = /^listing-generator-1\/Charm_Maker\/Generated_Charm_Sets\/Deriv_\d+$/i.test(b); // ✅ Updated Regex

  if (!isSet && !isDeriv) {
    throw new Error("output_base_path not allowed: " + b);
  }
  return b;
}

async function signedUrlFor(bucketFile) {
  const [url] = await bucketFile.getSignedUrl({
    action: "read",
    expires: Date.now() + 1000 * 60 * 60 * 24 * 7, // 7 days
  });
  return url;
}

// -------------------------
// Firestore (Admin) hardening for serverless
let _db;
function getDb() {
  if (_db) return _db;
  try {
    _db = initializeFirestore(admin.app(), { preferRest: true });
  } catch (e) {
    _db = getFirestore(admin.app());
  }
  return _db;
}
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
function isRetryableFirestoreError(err) {
  const code = err?.code || err?.details;
  const msg = String(err?.message || "").toLowerCase();
  return (
    code === "deadline-exceeded" ||
    code === "resource-exhausted" ||
    code === "unavailable" ||
    code === "aborted" ||
    code === "internal" ||
    msg.includes("deadline") ||
    msg.includes("resource") ||
    msg.includes("unavailable")
  );
}
async function firestoreRetry(fn, label = "firestore") {
  let lastErr;
  for (let attempt = 1; attempt <= 8; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (attempt === 8 || !isRetryableFirestoreError(err)) throw err;
      const backoff = Math.min(6000, 250 * (2 ** (attempt - 1))) + Math.floor(Math.random() * 250);
      console.log(`[${label}] retry ${attempt} after ${backoff}ms`, safeErr(err));
      await sleep(backoff);
    }
  }
  throw lastErr;
}

// -------------------------
// GEMINI RETRY HELPER (UPDATED: Aggressive 6s+ Backoff & 10 Retries)
// -------------------------
async function callGeminiWithRetry(fn, label = "gemini", maxRetries = 10) {
  let lastErr;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      const msg = String(err?.message || "").toLowerCase();
      
      // Check specifically for overload/rate-limit signals
      const isOverloaded = 
        msg.includes("overloaded") || 
        msg.includes("429") || 
        msg.includes("503") ||
        msg.includes("502") || 
        msg.includes("500") || 
        msg.includes("internal error") || 
        msg.includes("resource exhausted");

      if (!isOverloaded || attempt === maxRetries) {
        throw err; // Fatal error or out of retries
      }

      // Aggressive Backoff: 6s, 12s, 24s, 48s... + random jitter
      const backoff = (6000 * Math.pow(2, attempt)) + Math.floor(Math.random() * 2000);
      console.log(`[${label}] Model overloaded/failed (attempt ${attempt + 1}/${maxRetries}), retrying in ${backoff}ms...`, safeErr(err));
      await sleep(backoff);
    }
  }
  throw lastErr;
}

// Deterministic final framing: crop -> resize back to original size.
async function applyFinalFrameZoomIfNeeded(buf, postprocess = {}) {
  const z = Number(postprocess?.finalFrameZoom);
  if (!Number.isFinite(z) || z <= 1.0001) return buf;

  let sharp;
  try { sharp = require("sharp"); }
  catch (_) {
    throw new Error("finalFrameZoom requires the 'sharp' dependency in your top-level package.json.");
  }

  const ax = clampNumber(postprocess?.anchorX, 0, 1, 0.5);
  const ay = clampNumber(postprocess?.anchorY, 0, 1, 0.45);

  const meta = await sharp(buf).metadata();
  const w = meta?.width || 0;
  const h = meta?.height || 0;
  if (!w || !h) return buf;

  const cropW = Math.max(1, Math.round(w / z));
  const cropH = Math.max(1, Math.round(h / z));
  let left = Math.round(w * ax - cropW / 2);
  let top  = Math.round(h * ay - cropH / 2);
  left = Math.max(0, Math.min(w - cropW, left));
  top  = Math.max(0, Math.min(h - cropH, top));

  return await sharp(buf)
    .extract({ left, top, width: cropW, height: cropH })
    .resize(w, h, { kernel: "lanczos3" })
    .png()
    .toBuffer();
}

function filenameForMime(base, mime) {
  const m = String(mime || "").toLowerCase();
  const ext =
    m.includes("jpeg") || m.includes("jpg") ? "jpg" :
    m.includes("png") ? "png" :
    (m.split("/")[1] || "bin");
  return `${base}.${ext}`;
}

async function callGeminiImagesEdits({
  apiKey,
  model,
  prompt,
  size,
  quality,
  output_format,
  images, 
}) {
  return callGeminiGenerateContentImage({
    apiKey,
    model,
    prompt,
    size,
    images,
  });
}

async function callGeminiImagesGenerations({
  apiKey,
  model,
  prompt,
  size,
  quality,
  output_format,
}) {
  return callGeminiGenerateContentImage({
    apiKey,
    model,
    prompt,
    size,
    images: [],
  });
}

function sizeToAspectRatio(size = "2048x2048") {
  const m = /^(\d+)\s*x\s*(\d+)$/.exec(String(size || "").trim());
  if (!m) return "1:1";
  const w = Number(m[1]), h = Number(m[2]);
  if (!Number.isFinite(w) || !Number.isFinite(h) || w <= 0 || h <= 0) return "1:1";
  if (Math.abs(w - h) < 2) return "1:1";
  const gcd = (a,b)=> b ? gcd(b, a%b) : a;
  const g = gcd(w, h);
  return `${Math.round(w/g)}:${Math.round(h/g)}`;
}

function stripUndefined(obj) {
  if (!obj || typeof obj !== "object") return obj;
  const out = Array.isArray(obj) ? [] : {};
  for (const [k, v] of Object.entries(obj)) {
    if (v === undefined) continue;
    out[k] = stripUndefined(v);
  }
  return out;
}

async function callGeminiGenerateContentImage({
  apiKey,
  model,
  prompt,
  size,
  images,
}) {
  const geminiModel =
    String(model || "gemini-3.1-flash-image-preview").trim() ||
    "gemini-3.1-flash-image-preview";
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${geminiModel}:generateContent`;

  // ============================================================
  // 2K HARD-LOCK — every synchronous image generated by this
  // function (Redo button, Standard mode generate, Charm Maker,
  // run_set_async edits tasks, etc.) outputs at 2K. The batch path
  // has its own JSONL builder that's also locked to 2K, so every
  // image-producing surface in the app is consistent. Any `size`
  // string passed by callers (e.g., "2048x2048") is honored only
  // for the post-process resize and the prompt's size hint; the
  // tier sent to Gemini via imageConfig is always 2K.
  // ============================================================
  const sizeKey = "2K";

  // Existing WxH derivation, used for the post-process resize and
  // the in-prompt size hint. Default 2048×2048 when no size string
  // was passed, so the hint matches the locked tier.
  const m = /^(\d+)\s*x\s*(\d+)$/.exec(String(size || "").trim());
  const wantW = m ? Number(m[1]) : 2048;
  const wantH = m ? Number(m[2]) : 2048;
  const wantAR = sizeToAspectRatio(size) || "1:1";

  const promptText =
    `${String(prompt || "").trim()}\\n\\n` +
    `OUTPUT (NON-NEGOTIABLE): Return a photorealistic ${wantAR} image. ` +
    `Exact size ${wantW}x${wantH}. ` +
    `Return an image suitable for a product photo.`;

 const parts = [{ text: promptText }];
  for (const img of images || []) {
    // Gemini will hard-crash if passed application/octet-stream.
    // Force a valid image MIME type so the API attempts to process the buffer.
    let safeMime = img?.mime || "image/png";
    if (safeMime.includes("octet-stream") || !safeMime.startsWith("image/")) {
      safeMime = "image/png"; 
    }

    parts.push({
      inline_data: {
        mime_type: safeMime,
        data: Buffer.from(img?.buffer || Buffer.alloc(0)).toString("base64"),
      },
    });
  }

  const body = stripUndefined({
    contents: [{ role: "user", parts }],
    generationConfig: {
      responseModalities: ["TEXT", "IMAGE"],
      // imageConfig.imageSize: "2K" tells Gemini to natively produce
      // a 2048×2048 image. Without this, the model uses its default
      // (~1024 area) and post-process upscaling has to compensate,
      // which loses fidelity. Same field/structure the batch JSONL
      // builder uses (see buildBatchJsonlLine).
      imageConfig: { imageSize: sizeKey },
    },
  });

  const resp = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-goog-api-key": apiKey,
    },
    body: JSON.stringify(body),
  });

  const raw = await resp.text().catch(() => "");
  let data = null;
  try { data = raw ? JSON.parse(raw) : null; } catch { data = null; }

  if (!resp.ok) {
    const msg =
      data?.error?.message ||
      raw ||
      `Gemini generateContent failed with HTTP ${resp.status} (empty body)`;
    throw new Error(msg);
  }
  const partsOut = data?.candidates?.[0]?.content?.parts || [];

  const imgPart =
    partsOut.find((p) => p?.inline_data?.data) ||
    partsOut.find((p) => p?.inlineData?.data) ||
    null;

  const b64 = imgPart?.inline_data?.data || imgPart?.inlineData?.data;
  if (!b64) {
    const textOnly = partsOut
      .map((p) => p?.text)
      .filter(Boolean)
      .join("\\n")
      .slice(0, 600);
    throw new Error(
      `Gemini response missing inline_data image payload. Text: ${
        textOnly || "(none)"
      }`
    );
  }

  let outBuf = Buffer.from(b64, "base64");

  // Normalize to PNG + requested size
  try {
    let sharp;
    try { sharp = require("sharp"); } catch(_) {}
    if (sharp) {
      const img = sharp(outBuf);
      const meta = await img.metadata();
      const needResize =
        wantW && wantH && (meta?.width !== wantW || meta?.height !== wantH);
      if (needResize) {
        outBuf = await img.resize(wantW, wantH, { fit: "cover" }).png().toBuffer();
      } else {
        outBuf = await img.png().toBuffer();
      }
    }
  } catch (_) {
    // If sharp fails or not present, still return raw bytes.
  }

  return outBuf;
}

 function newDownloadToken() {
   return globalThis.crypto?.randomUUID
     ? globalThis.crypto.randomUUID()
     : require("crypto").randomUUID();
 }

 function tokenDownloadURLFor(bucketName, storagePath, token) {
   const encoded = encodeURIComponent(storagePath).replace(/%2F/g, "%2F");
   return `https://firebasestorage.googleapis.com/v0/b/${bucketName}/o/${encoded}?alt=media&token=${token}`;
 }

async function uploadPngBufferToStorage({ outBuf, jobId, runId, slotIndex, outputBasePath }) {
  const bucket = admin.storage().bucket();
  const token = newDownloadToken();

  const effectiveRunId = runId || `lg1_${Date.now()}`;
  const effectiveSlot = typeof slotIndex === "number" ? slotIndex : null;

  let storagePath;
  if (outputBasePath) {
    const base = String(outputBasePath).trim();
    const isSet = /^listing-generator-1\/[^/]+\/Ready_To_List\/Set_\d+$/i.test(base);
    const isDeriv = /^listing-generator-1\/Charm_Maker\/Generated_Charm_Sets\/Deriv_\d+$/i.test(base); // ✅ Updated Regex
    if (!isSet && !isDeriv) {
      throw new Error("output_base_path not allowed");
    }
    storagePath = `${base}/Slot_${effectiveSlot + 1}.png`;
  } else {
    // fallback legacy
    storagePath = `listing-generator-1/generated/${effectiveRunId}/slot_${effectiveSlot + 1}.png`;
  }
  const file = bucket.file(storagePath);

  await file.save(outBuf, {
    resumable: false,
    contentType: "image/png",
    metadata: {
      metadata: {
        firebaseStorageDownloadTokens: token,
      },
    },
  });

  const bucketName = bucket.name;
  const downloadURL = tokenDownloadURLFor(bucketName, storagePath, token);

  return { storagePath, downloadURL, effectiveRunId, effectiveSlot };
}

async function uploadPngBufferToSetPath(outBuf, basePath, sIndex, fallbackJobId, fallbackRunId) {
  const bucket = admin.storage().bucket();
  const effectiveSlot = Number.isFinite(Number(sIndex)) && Number(sIndex) >= 0 ? Number(sIndex) : 0;

  if (basePath) {
    const base = assertAllowedOutputBase(basePath);
    const storagePath = `${base}/Slot_${effectiveSlot + 1}.png`;
    const file = bucket.file(storagePath);
     // IMPORTANT: Always attach firebaseStorageDownloadTokens so browser previews can load reliably.
     const token = newDownloadToken();

     await file.save(outBuf, {
       resumable: false,
       contentType: "image/png",
       metadata: {
         metadata: {
           firebaseStorageDownloadTokens: token,
         },
       },
     });

     const downloadURL = tokenDownloadURLFor(bucket.name, storagePath, token);
     return { storagePath, downloadURL, effectiveRunId: fallbackRunId || fallbackJobId || null, effectiveSlot };
  }

  return await uploadPngBufferToStorage({ outBuf, jobId: fallbackJobId, runId: fallbackRunId, slotIndex: effectiveSlot });
}

/**
 * Postprocess pipeline:
 * charm_postscale logic
 */
async function postScaleCharmComposite({
  passABuf,
  baseNoCharmBuf,
  scale,
  targetPx,
  shadowOpacity,
  shadowBlur,
  diffThreshold,
}) {
  let sharp;
  try {
    sharp = require("sharp");
  } catch (e) {
    throw new Error(
      "Missing dependency: sharp. Add it to your Netlify functions bundle (npm i sharp) to use kind=charm_postscale."
    );
  }

  const aMeta = await sharp(passABuf).metadata();
  const bMeta = await sharp(baseNoCharmBuf).metadata();
  if (!aMeta?.width || !aMeta?.height || !bMeta?.width || !bMeta?.height) {
    throw new Error("Could not read image metadata for postprocess.");
  }
  if (aMeta.width !== bMeta.width || aMeta.height !== bMeta.height) {
    throw new Error(
      `postprocess requires same dimensions. passA=${aMeta.width}x${aMeta.height}, base=${bMeta.width}x${bMeta.height}`
    );
  }

  const width = aMeta.width;
  const height = aMeta.height;

  // Decode raw RGBA
  const aRaw = await sharp(passABuf).ensureAlpha().raw().toBuffer();
  const bRaw = await sharp(baseNoCharmBuf).ensureAlpha().raw().toBuffer();

  const diffVals = new Uint8Array(width * height);
  for (let i = 0, p = 0; i < aRaw.length; i += 4, p++) {
    const dr = Math.abs(aRaw[i] - bRaw[i]);
    const dg = Math.abs(aRaw[i + 1] - bRaw[i + 1]);
    const db = Math.abs(aRaw[i + 2] - bRaw[i + 2]);
    diffVals[p] = Math.max(dr, dg, db);
  }

  async function buildMaskAndBBox(thr, feather) {
    const mask = Buffer.alloc(width * height);
    for (let p = 0; p < diffVals.length; p++) mask[p] = diffVals[p] > thr ? 255 : 0;

    const maskRaw = await sharp(mask, { raw: { width, height, channels: 1 } })
      .blur(feather)
      .threshold(18)
      .raw()
      .toBuffer();

    let minX = width, minY = height, maxX = -1, maxY = -1;
    let count = 0;

    for (let y = 0; y < height; y++) {
      const row = y * width;
      for (let x = 0; x < width; x++) {
        const v = maskRaw[row + x];
        if (v > 0) {
          count++;
          if (x < minX) minX = x;
          if (y < minY) minY = y;
          if (x > maxX) maxX = x;
          if (y > maxY) maxY = y;
        }
      }
    }

    const found = maxX >= 0;
    const bboxArea = found ? (maxX - minX + 1) * (maxY - minY + 1) : 0;
    const density = found && bboxArea > 0 ? count / bboxArea : 0; 
    return { found, maskRaw, minX, minY, maxX, maxY, count, bboxArea, density };
  }

  const baseThr = clampNumber(diffThreshold, 8, 120, 40);
  const feather = 1;

  const totalPx = width * height;
  const MAX_MASK_PX_RATIO = 0.035; 
  const MAX_BBOX_AREA_RATIO = 0.075; 
  const MAX_BBOX_W_RATIO = 0.35; 
  const MAX_BBOX_H_RATIO = 0.35; 
  const MIN_DENSITY = 0.035; 
  const CENTER_X_MIN = 0.12, CENTER_X_MAX = 0.88; 
  const CENTER_Y_MIN = 0.12, CENTER_Y_MAX = 0.88;

  let chosen = null;
  let best = null; 

  for (let thr = baseThr; thr <= 90; thr += 8) {
    const m = await buildMaskAndBBox(thr, feather);
    if (!m.found) continue;

    if (!best || m.bboxArea < best.bboxArea) best = { ...m, thr };

    const bboxW = (m.maxX - m.minX + 1);
    const bboxH = (m.maxY - m.minY + 1);
    const bboxWR = bboxW / width;
    const bboxHR = bboxH / height;
    const cx = (m.minX + m.maxX) / 2;
    const cy = (m.minY + m.maxY) / 2;
    const okCenter =
      (cx >= width * CENTER_X_MIN && cx <= width * CENTER_X_MAX) &&
      (cy >= height * CENTER_Y_MIN && cy <= height * CENTER_Y_MAX);

    const okMask = m.count <= totalPx * MAX_MASK_PX_RATIO;
    const okBox = m.bboxArea <= totalPx * MAX_BBOX_AREA_RATIO;
    const okW = bboxWR <= MAX_BBOX_W_RATIO;
    const okH = bboxHR <= MAX_BBOX_H_RATIO;
    const okDense = (m.density || 0) >= MIN_DENSITY;

    if (okMask && okBox && okW && okH && okDense && okCenter) {
      chosen = { ...m, thr };
      break;
    }
  }

  if (!chosen) chosen = best;
  if (!chosen || !chosen.found) return passABuf;

  if (chosen.bboxArea > totalPx * 0.25) {
    console.log("[postscale] bbox too large; skipping charm_postscale", {
      bboxArea: chosen.bboxArea,
      totalPx,
      thr: chosen.thr,
    });
    return passABuf;
  }

  let { maskRaw, minX, minY, maxX, maxY } = chosen;

  try {
    const DS = 4;
    const smallW = Math.max(1, Math.round(width / DS));
    const smallH = Math.max(1, Math.round(height / DS));

    const small = await sharp(maskRaw, { raw: { width, height, channels: 1 } })
      .resize(smallW, smallH, { kernel: "nearest" })
      .threshold(1)
      .raw()
      .toBuffer();

    const visited = new Uint8Array(smallW * smallH);
    let bestArea = 0;
    let best = null;

    const qx = new Int32Array(smallW * smallH);
    const qy = new Int32Array(smallW * smallH);

    for (let y = 0; y < smallH; y++) {
      for (let x = 0; x < smallW; x++) {
        const idx = y * smallW + x;
        if (visited[idx]) continue;
        if (small[idx] === 0) { visited[idx] = 1; continue; }

        visited[idx] = 1;
        let head = 0, tail = 0;
        qx[tail] = x; qy[tail] = y; tail++;

        let area = 0;
        let mnx = x, mny = y, mxx = x, mxy = y;

        while (head < tail) {
          const cx = qx[head];
          const cy = qy[head];
          head++;
          area++;
          if (cx < mnx) mnx = cx;
          if (cy < mny) mny = cy;
          if (cx > mxx) mxx = cx;
          if (cy > mxy) mxy = cy;

          const n1 = cx > 0 ? (cy * smallW + (cx - 1)) : -1;
          const n2 = cx + 1 < smallW ? (cy * smallW + (cx + 1)) : -1;
          const n3 = cy > 0 ? ((cy - 1) * smallW + cx) : -1;
          const n4 = cy + 1 < smallH ? ((cy + 1) * smallW + cx) : -1;

          if (n1 >= 0 && !visited[n1] && small[n1]) { visited[n1] = 1; qx[tail] = cx - 1; qy[tail] = cy; tail++; }
          if (n2 >= 0 && !visited[n2] && small[n2]) { visited[n2] = 1; qx[tail] = cx + 1; qy[tail] = cy; tail++; }
          if (n3 >= 0 && !visited[n3] && small[n3]) { visited[n3] = 1; qx[tail] = cx; qy[tail] = cy - 1; tail++; }
          if (n4 >= 0 && !visited[n4] && small[n4]) { visited[n4] = 1; qx[tail] = cx; qy[tail] = cy + 1; tail++; }
        }

        if (area < 12) continue;

        if (area > bestArea) {
          bestArea = area;
          best = { mnx, mny, mxx, mxy };
        }
      }
    }

    if (best) {
      const padSmall = 2;
      const sx1 = Math.max(0, best.mnx - padSmall);
      const sy1 = Math.max(0, best.mny - padSmall);
      const sx2 = Math.min(smallW - 1, best.mxx + padSmall);
      const sy2 = Math.min(smallH - 1, best.mxy + padSmall);

      minX = Math.max(0, Math.floor(sx1 * DS));
      minY = Math.max(0, Math.floor(sy1 * DS));
      maxX = Math.min(width - 1, Math.ceil((sx2 + 1) * DS) - 1);
      maxY = Math.min(height - 1, Math.ceil((sy2 + 1) * DS) - 1);
    }
  } catch (_) {
    // If refinement fails for any reason, keep the original bbox.
  }

  const pad = 6;
  const left = Math.max(0, minX - pad);
  const top = Math.max(0, minY - pad);
  const bboxW = Math.min(width - left, maxX - minX + 1 + pad * 2);
  const bboxH = Math.min(height - top, maxY - minY + 1 + pad * 2);

  const maskPng = await sharp(maskRaw, { raw: { width, height, channels: 1 } })
    .extract({ left, top, width: bboxW, height: bboxH })
    .blur(0.8)
    .png()
    .toBuffer();

  const charmCrop = await sharp(passABuf)
    .extract({ left, top, width: bboxW, height: bboxH })
    .removeAlpha()
    .joinChannel(maskPng)
    .png()
    .toBuffer();

  const tp = Number(targetPx);
  let outW, outH;
  if (Number.isFinite(tp)) {
    const targetH = Math.round(clampNumber(tp, 4, 96, 14));
    const aspect = bboxH > 0 ? (bboxW / bboxH) : 1;
    outH = Math.max(1, targetH);
    outW = Math.max(1, Math.round(outH * aspect));

    if (outW > width) {
      const k = width / outW;
      outW = Math.max(1, Math.floor(outW * k));
      outH = Math.max(1, Math.floor(outH * k));
    }
    if (outH > height) {
      const k = height / outH;
      outW = Math.max(1, Math.floor(outW * k));
      outH = Math.max(1, Math.floor(outH * k));
    }
  } else {
    const s = clampNumber(scale, 0.50, 0.70, 0.65);
    outW = Math.max(1, Math.round(bboxW * s));
    outH = Math.max(1, Math.round(bboxH * s));
  }

  const scaledCharm = await sharp(charmCrop)
    .resize(outW, outH, { kernel: "lanczos3" })
    .sharpen(0.6)
    .png()
    .toBuffer();

  try {
    const aStats = await sharp(scaledCharm).extractChannel(3).stats();
    if (!aStats?.channels?.[0] || aStats.channels[0].max === 0) return passABuf;
  } catch (_) {
    return passABuf;
  }

  const shBlur = clampNumber(shadowBlur, 0, 12, 2);
  const shOp = clampNumber(shadowOpacity, 0, 0.6, 0.28);

  const shadowAlphaRaw = await sharp(scaledCharm)
    .extractChannel(3)
    .blur(shBlur)
    .linear(shOp, 0)
    .raw()
    .toBuffer();

  const shadowLayer = await sharp({
    create: {
      width: outW,
      height: outH,
      channels: 3,
      background: { r: 0, g: 0, b: 0 },
    },
  })
    .joinChannel(shadowAlphaRaw, { raw: { width: outW, height: outH, channels: 1 } })
    .png()
    .toBuffer();

  const anchorX = left + Math.round(bboxW / 2);
  const newLeft = Math.max(0, Math.min(width - outW, Math.round(anchorX - outW / 2)));
  const newTop = Math.max(0, Math.min(height - outH, top));

  const finalBuf = await sharp(baseNoCharmBuf)
    .composite([
      { input: shadowLayer, left: newLeft, top: Math.min(height - outH, newTop + 1), blend: "multiply" },
      { input: scaledCharm, left: newLeft, top: newTop, blend: "over" },
    ])
    .png()
    .toBuffer();

  return finalBuf;
}

// ============================================================
// Gemini Batch API helpers (Listing Generator)
// Reference: https://ai.google.dev/gemini-api/docs/batch-api
//
// These helpers are strictly additive. The synchronous (non-batch)
// pipeline elsewhere in this file is untouched. Batch-related kinds
// (batch_submit, batch_status, batch_collect, batch_list, batch_cancel)
// live in their own block in exports.handler below.
// ============================================================

const BATCHES_COLL = "ListingGenerator1Batches";
const GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta";
const GEMINI_UPLOAD_BASE = "https://generativelanguage.googleapis.com/upload/v1beta";

// Build one JSONL request line for the Batch API. Encodes both reference
// images as inline_data (base64). responseModalities: ["IMAGE"] saves a
// few output tokens by skipping the model's default text preamble.
// imageConfig.imageSize: "2K" is the supported way to lock 2048x2048
// output per Google's image-generation docs.
function buildBatchJsonlLine(key, prompt, refMime, refBase64, charmMime, charmBase64, imageSize) {
  const sizeKey = String(imageSize || "2K").toUpperCase();
  const safeRefMime = (refMime && refMime.startsWith("image/")) ? refMime : "image/png";
  const safeCharmMime = (charmMime && charmMime.startsWith("image/")) ? charmMime : "image/png";

  // Mirror the prompt augmentation Standard mode uses in
  // callGeminiGenerateContentImage(). Without this suffix, Gemini
  // may reframe or change the aspect ratio of the output, which in
  // turn causes the charm to appear at the wrong scale relative to
  // the reference image. Standard and Batch modes must produce the
  // same output for the same inputs.
  // Pixel-size hint: at 2K we tell the model 2048x2048; at 1K, 1024x1024.
  const sizeHint = sizeKey === "1K" ? "1024x1024"
                 : sizeKey === "2K" ? "2048x2048"
                 : sizeKey === "4K" ? "4096x4096"
                 : "2048x2048";
  const augmentedPrompt =
    `${String(prompt || "").trim()}\n\n` +
    `OUTPUT (NON-NEGOTIABLE): Return a photorealistic 1:1 image. ` +
    `Exact size ${sizeHint}. ` +
    `Return an image suitable for a product photo.`;

  return {
    key,
    request: {
      contents: [{
        role: "user",
        parts: [
          { text: augmentedPrompt },
          { inline_data: { mime_type: safeRefMime, data: refBase64 } },
          { inline_data: { mime_type: safeCharmMime, data: charmBase64 } },
        ],
      }],
      generation_config: {
        // Match Standard mode's modalities exactly. The model may
        // adjust framing/scaling decisions when text output is
        // disallowed, which contributed to charm-size mismatches.
        responseModalities: ["TEXT", "IMAGE"],
        imageConfig: { imageSize: sizeKey },
      },
    },
  };
}

// Upload a JSONL file via the Files API using the resumable protocol
// (the only protocol Google documents for the Batch flow). Returns the
// `files/abc123` resource name.
async function uploadJsonlToGeminiFiles(apiKey, jsonlData, displayName) {
  // jsonlData may be a Buffer (preferred — avoids string→bytes
  // re-encoding inside fetch) or a string. We compute byte length
  // accordingly so the resumable headers are accurate either way.
  const isBuffer = Buffer.isBuffer(jsonlData);
  const bytes = isBuffer ? jsonlData.length : Buffer.byteLength(jsonlData, "utf8");

  const startResp = await fetch(`${GEMINI_UPLOAD_BASE}/files`, {
    method: "POST",
    headers: {
      "x-goog-api-key": apiKey,
      "X-Goog-Upload-Protocol": "resumable",
      "X-Goog-Upload-Command": "start",
      "X-Goog-Upload-Header-Content-Length": String(bytes),
      "X-Goog-Upload-Header-Content-Type": "application/jsonl",
      "Content-Type": "application/jsonl",
    },
    body: JSON.stringify({ file: { display_name: displayName || "lg1-batch" } }),
  });
  if (!startResp.ok) {
    const t = await startResp.text().catch(() => "");
    throw new Error(`Files API resumable-start failed: HTTP ${startResp.status} ${t.slice(0, 400)}`);
  }
  const uploadUrl = startResp.headers.get("x-goog-upload-url");
  if (!uploadUrl) throw new Error("Files API did not return x-goog-upload-url header");

  const uploadResp = await fetch(uploadUrl, {
    method: "POST",
    headers: {
      "Content-Length": String(bytes),
      "X-Goog-Upload-Offset": "0",
      "X-Goog-Upload-Command": "upload, finalize",
    },
    body: jsonlData,
  });
  if (!uploadResp.ok) {
    const t = await uploadResp.text().catch(() => "");
    throw new Error(`Files API upload-finalize failed: HTTP ${uploadResp.status} ${t.slice(0, 400)}`);
  }
  const data = await uploadResp.json().catch(() => ({}));
  const name = data?.file?.name;
  if (!name) throw new Error("Files API upload returned no file.name");
  return name;
}

async function createGeminiBatchJob(apiKey, model, fileName, displayName) {
  const url = `${GEMINI_BASE}/models/${model}:batchGenerateContent`;
  const body = {
    batch: {
      display_name: displayName || "lg1-batch",
      input_config: { file_name: fileName },
    },
  };
  const resp = await fetch(url, {
    method: "POST",
    headers: { "x-goog-api-key": apiKey, "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`Batch create failed: HTTP ${resp.status} ${text.slice(0, 600)}`);
  }
  let parsed;
  try { parsed = JSON.parse(text); }
  catch { throw new Error(`Batch create returned non-JSON: ${text.slice(0, 200)}`); }
  // Returns a long-running operation; the actual batch name is at
  // .name (e.g., "batches/abc123") OR within .metadata. Per the docs,
  // the top-level .name is the operation/batch name we use for polling.
  const batchName = parsed?.name;
  if (!batchName) throw new Error(`Batch create returned no name: ${text.slice(0, 400)}`);
  return { batchName, raw: parsed };
}

async function getGeminiBatchJob(apiKey, batchName) {
  // batchName is "batches/abc123" — keep it as-is in the URL.
  const resp = await fetch(`${GEMINI_BASE}/${batchName}`, {
    method: "GET",
    headers: { "x-goog-api-key": apiKey, "Content-Type": "application/json" },
  });
  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`Batch get failed: HTTP ${resp.status} ${text.slice(0, 400)}`);
  }
  const parsed = JSON.parse(text);

  // Normalize state names. Google's batch API returns BATCH_STATE_*
  // strings; our code and Firestore documents use JOB_STATE_*. Map at
  // the source so all downstream logic compares against JOB_STATE_*.
  // Done in-place so state appears in both .metadata.state and .state.
  const norm = (s) => {
    if (!s || typeof s !== "string") return s;
    if (s.startsWith("BATCH_STATE_")) return "JOB_STATE_" + s.slice("BATCH_STATE_".length);
    return s;
  };
  if (parsed?.metadata?.state) parsed.metadata.state = norm(parsed.metadata.state);
  if (parsed?.state) parsed.state = norm(parsed.state);
  return parsed;
}

async function cancelGeminiBatchJob(apiKey, batchName) {
  const resp = await fetch(`${GEMINI_BASE}/${batchName}:cancel`, {
    method: "POST",
    headers: { "x-goog-api-key": apiKey, "Content-Type": "application/json" },
    body: "{}",
  });
  if (!resp.ok) {
    const t = await resp.text().catch(() => "");
    throw new Error(`Batch cancel failed: HTTP ${resp.status} ${t.slice(0, 200)}`);
  }
  return true;
}

// Stream the result JSONL line-by-line instead of loading the entire
// (potentially 2GB+) file into memory. Returns an async generator that
// yields each JSON-parsed line. Throws on non-OK HTTP. Empty/blank
// lines and parse errors are silently skipped (mirrors the older code).
async function* streamGeminiResultLines(apiKey, fileName) {
  const url = `https://generativelanguage.googleapis.com/download/v1beta/${fileName}:download?alt=media`;
  const resp = await fetch(url, {
    method: "GET",
    headers: { "x-goog-api-key": apiKey },
  });
  if (!resp.ok) {
    const t = await resp.text().catch(() => "");
    throw new Error(`Result file download failed: HTTP ${resp.status} ${t.slice(0, 400)}`);
  }
  if (!resp.body) {
    // Fallback for environments without ReadableStream support.
    const text = await resp.text();
    for (const raw of text.split("\n")) {
      const line = raw.trim();
      if (!line) continue;
      try { yield JSON.parse(line); } catch { /* skip */ }
    }
    return;
  }

  // The stream gives us Uint8Array chunks. We accumulate a small text
  // buffer and emit one parsed JSON object per newline. Crucially we
  // never hold more than the in-flight chunk + the trailing partial
  // line, so memory stays bounded regardless of file size.
  const decoder = new TextDecoder("utf-8");
  const reader = resp.body.getReader();
  let buf = "";
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });
    let nl;
    while ((nl = buf.indexOf("\n")) >= 0) {
      const line = buf.slice(0, nl).trim();
      buf = buf.slice(nl + 1);
      if (!line) continue;
      try { yield JSON.parse(line); } catch { /* skip malformed */ }
    }
  }
  buf += decoder.decode(); // flush any trailing bytes
  const tail = buf.trim();
  if (tail) {
    try { yield JSON.parse(tail); } catch { /* skip */ }
  }
}

// Run async tasks with bounded concurrency. Each task receives no
// arguments and returns a promise. Max in-flight = `limit`. Returns
// the array of results in input order. Used by batch_collect to
// upload result PNGs without holding all 600 image buffers in RAM
// at once.
async function runBoundedConcurrent(items, limit, worker) {
  const results = new Array(items.length);
  let nextIdx = 0;
  async function loop() {
    while (true) {
      const i = nextIdx++;
      if (i >= items.length) return;
      try {
        results[i] = await worker(items[i], i);
      } catch (e) {
        results[i] = { __error: e };
      }
    }
  }
  const runners = Array.from({ length: Math.min(limit, items.length) }, loop);
  await Promise.all(runners);
  return results;
}

// Sanitize a Gemini batch name "batches/abc123" → "abc123" for use as
// a Firestore doc id. Firestore disallows "/" in doc ids.
function batchDocIdFromName(batchName) {
  return String(batchName || "").replace(/^batches\//, "").replace(/[^\w-]/g, "_");
}

exports.handler = async (event) => {
  // Top-level safety net. Any error inside the handler that isn't
  // caught by the inner try/catch blocks would otherwise bubble up to
  // Netlify and surface as an opaque "Internal Error. ID: xxx" 500
  // with no useful message. This wrapper guarantees we always return
  // a JSON response with the actual error text so the browser can
  // display it. Crucial for debugging large batch submissions where
  // Netlify's function logs aren't always visible.
  try {
    return await _handlerImpl(event);
  } catch (err) {
    console.error("[handler] unhandled error:", err);
    return {
      statusCode: 500,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
      body: JSON.stringify({
        ok: false,
        error: {
          message: String(err?.message || err),
          stack: String(err?.stack || "").split("\n").slice(0, 5).join(" | "),
          kind: (() => { try { return JSON.parse(event?.body || "{}")?.kind; } catch { return null; } })(),
        },
      }),
    };
  }
};

async function _handlerImpl(event) {
  if (event.httpMethod === "OPTIONS") return json(200, { ok: true });
  if (event.httpMethod !== "POST") return json(405, { error: { message: "Method not allowed" } });

  const body = parseJsonBody(event);
  if (!body) return json(400, { error: { message: "Invalid JSON body" } });

  const {
    jobId,
    runId,
    slotIndex,
    kind = "edits",
    model: _clientModel, // ignored
    prompt,
    size = "2048x2048",
    quality = "high",
    output_format = "png",
    input_storage_path,
    input_image,
    input_charm_storage_path,
    input_charm_image,
    remove_prompt,
    postprocess,
    base_storage_path,
    base_image,
    activeCategory,
    output_base_path,
    source_storage_path,
    manifest,
  } = body || {};

  const model = GEMINI_IMAGE_MODEL;

  // ---------- NEW: non-job operations (no jobId required) ----------
  try {
    if (kind === "alloc_set") {
      const { setN, outputBasePath } = await allocNextSet(activeCategory);
      return json(200, { ok: true, setN, outputBasePath });
    }

    // ============================================================
    // SCAN EMPTY SETS — for partial-failure recovery
    // ------------------------------------------------------------
    // After a batch submission, some Set_N folders may end up empty
    // (allocated by alloc_set but never populated with slot images
    // because the underlying batch failed or never collected). This
    // kind enumerates them so the frontend can offer a "Resume" flow
    // that re-submits batches for just the empty ones, leaving the
    // populated ones untouched.
    //
    // Algorithm:
    //   1. Read the Firestore set counter for the category — gives us
    //      the highest setN that's ever been issued.
    //   2. List every file under Ready_To_List/ in one bucket call.
    //   3. Bucket the files by Set_N. A set with at least one
    //      Slot_*.png file counts as "has content"; everything else
    //      (no files at all, or only a stale manifest) counts as
    //      "empty".
    //   4. Return a sorted list of {setN, outputBasePath} for empty
    //      slots in [1, highestN]. Populated slots are NOT returned.
    // ============================================================
    if (kind === "scan_empty_sets") {
      const cat = normalizeCategory(activeCategory);
      if (!GENERATABLE_CATEGORIES.has(cat)) {
        return json(400, { error: { message: "activeCategory not generatable" } });
      }

      const db = getDb();
      const counterSnap = await db.collection(SET_COUNTERS_COLL).doc(cat).get();
      const highestN = counterSnap.exists ? Number(counterSnap.data()?.nextSetN || 0) : 0;
      if (highestN <= 0) {
        return json(200, { ok: true, highestN, empty: [], populated: [], message: "No sets allocated yet for this category." });
      }

      const bucket = admin.storage().bucket();
      const prefix = `listing-generator-1/${cat}/Ready_To_List/`;
      const [files] = await bucket.getFiles({ prefix });

      // Bucket file names by setN. Track whether each setN has at least
      // one Slot_*.png file (the gating signal for "has content"). A
      // manifest.json alone does not count — alloc_set never writes
      // files, so a folder with only a stale manifest is still "empty"
      // by our definition (the manifest typically arrives only after
      // batch_collect, which also writes slot images).
      const hasSlots = new Set();
      const re = /\/Set_(\d+)\/(Slot_\d+\.png|.+)$/;
      for (const f of files) {
        const m = f.name.match(re);
        if (!m) continue;
        const setN = Number(m[1]);
        const filename = m[2];
        if (/^Slot_\d+\.png$/i.test(filename)) {
          hasSlots.add(setN);
        }
      }

      // Identify already-approved sets so we don't classify them as
      // "empty" and accidentally regenerate them when the user clicks
      // Resume Empty Sets. The approval flow moves files from
      // Ready_To_List/Set_N/ to Completed_Listing_Sets/{cat}_Set_N/,
      // leaving Ready_To_List/Set_N/ empty (no slot images). Without
      // this filter, every approved set would look like a gap and get
      // re-filled with brand-new charms — destroying the user's
      // approved work.
      const approvedSetNs = new Set();
      const completedPrefix = `listing-generator-1/Generated_Listing_Sets/Completed_Listing_Sets/${cat}_Set_`;
      try {
        const [completedFiles] = await bucket.getFiles({ prefix: completedPrefix });
        for (const f of completedFiles) {
          // Path looks like ".../Completed_Listing_Sets/{cat}_Set_42/..."
          const idx = f.name.indexOf(`/${cat}_Set_`);
          if (idx === -1) continue;
          const tail = f.name.slice(idx + `/${cat}_Set_`.length);
          const numStr = tail.split("/")[0];
          const num = Number(numStr);
          if (Number.isFinite(num) && num > 0) approvedSetNs.add(num);
        }
      } catch (e) {
        console.warn("[scan_empty_sets] could not enumerate Completed_Listing_Sets:", e?.message || e);
      }

      const empty = [];
      const populated = [];
      const skippedApproved = [];
      for (let n = 1; n <= highestN; n++) {
        if (approvedSetNs.has(n)) {
          skippedApproved.push(n);
          continue;
        }
        const outputBasePath = `listing-generator-1/${cat}/Ready_To_List/Set_${n}`;
        if (hasSlots.has(n)) {
          populated.push({ setN: n, outputBasePath });
        } else {
          empty.push({ setN: n, outputBasePath });
        }
      }

      return json(200, {
        ok: true,
        highestN,
        emptyCount: empty.length,
        populatedCount: populated.length,
        approvedCount: skippedApproved.length,
        empty,
        // populated and skippedApproved returned only as counts to keep
        // the response payload small for large counters.
      });
    }

    // ============================================================
    // CHARM POOL — pick & release for batch mode
    // ------------------------------------------------------------
    // Each generated set must use a UNIQUE charm from the shared pool,
    // and we must avoid duplicate picks across concurrent submissions.
    //
    // Pick algorithm:
    //   1. List all charms in the active pool (necklace or earring).
    //   2. Enumerate every uncollected batch's `sets[].tasks[].input_charm_storage_path`
    //      to find currently-reserved charms.
    //   3. Subtract: available = pool - reserved.
    //   4. Return the first `count` available paths.
    //
    // Atomicity: the pick step doesn't write anything — the
    // reservation happens implicitly when the caller subsequently
    // submits a batch with these paths in its tasks. There's a small
    // race window (two clients pick same charms simultaneously then
    // both submit), which we accept because:
    //   - The window is sub-second in practice
    //   - The user-visible result is just one charm getting reused
    //     once across two concurrent submissions
    //   - The full atomicity solution (Firestore-tracked reservations
    //     with ttl) adds significant complexity for a problem that
    //     basically only manifests if you submit two batches within
    //     the same second
    // ============================================================
    if (kind === "charm_pool_pick") {
      const isEarring = !!body?.isEarring;
      const want = Math.max(1, Number(body?.count || 1));

      const bucket = admin.storage().bucket();
      const poolPrefix = isEarring
        ? "listing-generator-1/Charm_Maker/New_Charms_Earrings/"
        : "listing-generator-1/Charm_Maker/New_Charms/";

      // Step 1: list pool. Skip pseudo-folders (paths ending with "/").
      const [poolFiles] = await bucket.getFiles({ prefix: poolPrefix });
      const poolPaths = poolFiles
        .map((f) => f.name)
        .filter((n) => !n.endsWith("/") && /\.(png|jpg|jpeg|webp)$/i.test(n));

      if (poolPaths.length === 0) {
        return json(200, { ok: true, charms: [], poolSize: 0, reservedCount: 0 });
      }

      // Step 2: enumerate reservations from uncollected batches.
      const db = getDb();
      const reservedSet = new Set();
      try {
        const snap = await db.collection(BATCHES_COLL)
          .where("collected", "==", false).get();
        snap.forEach((doc) => {
          const sets = Array.isArray(doc.data()?.sets) ? doc.data().sets : [];
          for (const s of sets) {
            for (const t of (s.tasks || [])) {
              const p = t?.input_charm_storage_path;
              if (p) reservedSet.add(p);
            }
          }
        });
      } catch (e) {
        console.warn("charm_pool_pick: failed to enumerate reservations", e?.message || e);
      }

      // Step 3: filter and return.
      const available = poolPaths.filter((p) => !reservedSet.has(p));

      // Shuffle so concurrent submissions don't all start from the
      // same position. Fisher-Yates.
      for (let i = available.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [available[i], available[j]] = [available[j], available[i]];
      }

      const picked = available.slice(0, want);
      return json(200, {
        ok: true,
        charms: picked,
        poolSize: poolPaths.length,
        reservedCount: reservedSet.size,
        availableCount: available.length,
      });
    }

    // ============================================================
    // BATCH MODE — Listing Generator only (Charm Maker out of scope)
    //
    // Flow:
    //   1. Client calls batch_submit with an array of `sets`. Each
    //      `set` is { category, outputBasePath, setN, tasks: [...] }
    //      where tasks come from SLOT_MAP. Tasks of type "copy" run
    //      synchronously here (no Gemini call). Tasks of type "edits"
    //      become JSONL lines uploaded to Gemini Files API.
    //   2. Server builds the JSONL with explicit imageConfig.imageSize="2K"
    //      and submits to Gemini batchGenerateContent. The batch name
    //      ("batches/abc123") is persisted in Firestore alongside per-
    //      task routing metadata so we can later land each output in
    //      the correct Slot_N.png path.
    //   3. Client polls batch_status. When state=JOB_STATE_SUCCEEDED,
    //      client calls batch_collect. Server downloads result JSONL,
    //      decodes each base64 image, uploads to its destination, and
    //      writes per-set manifests.
    //
    // Constraint discovery: see `keyForTask()` for how we route results.
    // The Gemini Batch API echoes our per-line `key` in each response,
    // so we encode the routing tuple (setSeq, slotIndex) in the key.
    // ============================================================
    if (kind === "batch_submit") {
      // Memory profiling — log at every stage so we can see where the
      // OOM happens. process.memoryUsage() reports:
      //   rss        = total resident set (everything Node uses)
      //   heapUsed   = JS heap actively used
      //   heapTotal  = JS heap allocated
      //   external   = C++ buffers (file I/O, fetch bodies, etc.)
      //   arrayBuffers = TypedArray-backed buffers (subset of external)
      // The OOM in Netlify is "rss exceeds container limit". Watch rss.
      const memLog = (stage) => {
        try {
          const m = process.memoryUsage();
          const fmt = (n) => (n / 1024 / 1024).toFixed(1) + "MB";
          console.log(`[batch_submit:mem] ${stage} rss=${fmt(m.rss)} heap=${fmt(m.heapUsed)}/${fmt(m.heapTotal)} ext=${fmt(m.external)}`);
        } catch (_) {}
      };
      memLog("entry");

      const apiKey = process.env.GEMINI_API_KEY;
      if (!apiKey) return json(400, { error: { message: "Missing GEMINI_API_KEY env var" } });

      const sets = Array.isArray(body?.sets) ? body.sets : null;
      if (!sets || sets.length === 0) {
        return json(400, { error: { message: "sets must be a non-empty array" } });
      }
      // Sanity cap. Even at 200 sets * 8 tasks * ~1.2MB inline data ≈ 1.9GB,
      // we approach the Files API 2GB limit. 100 keeps headroom.
      if (sets.length > 100) {
        return json(400, { error: { message: "Per-batch limit is 100 sets. Split into multiple batches." } });
      }

      const displayName = String(body?.displayName || `lg1-batch-${Date.now()}`).slice(0, 100);
      const imageSize = String(body?.imageSize || "2K").toUpperCase();
      const bucket = admin.storage().bucket();

      // Validate every output_base_path up front so we fail fast.
      for (const s of sets) {
        const cat = normalizeCategory(s?.category);
        if (!GENERATABLE_CATEGORIES.has(cat)) {
          return json(400, { error: { message: `category not generatable: ${cat}` } });
        }
        try { assertAllowedOutputBase(s?.outputBasePath); }
        catch (e) {
          return json(400, { error: { message: `Invalid outputBasePath: ${e.message}` } });
        }
        if (!Array.isArray(s?.tasks) || s.tasks.length === 0) {
          return json(400, { error: { message: `set ${s?.outputBasePath} has no tasks` } });
        }
      }

      // Step A: Run all "copy" tasks synchronously. They don't go through
      // Gemini and shouldn't wait for batch turnaround. This mirrors what
      // copy_to_slot does, but inline so we don't double-network-trip.
      // We also do per-task storagePath validation here.
      let copiedCount = 0;
      let copyErrors = [];
      const copyPromises = [];
      for (const s of sets) {
        const base = s.outputBasePath;
        for (const t of s.tasks) {
          if (String(t?.type) !== "copy") continue;
          const slot = Number(t?.slotIndex);
          if (!Number.isFinite(slot) || slot < 0) continue;
          const src = String(t?.source_storage_path || "").trim();
          if (!src) {
            copyErrors.push({ outputBasePath: base, slotIndex: slot, error: "missing source_storage_path" });
            continue;
          }
          copyPromises.push((async () => {
            try {
              const dst = `${base}/Slot_${slot + 1}.png`;
              const dstFile = bucket.file(dst);
              await bucket.file(src).copy(dstFile);
              const token = newDownloadToken();
              await dstFile.setMetadata({ metadata: { firebaseStorageDownloadTokens: token } });
              copiedCount++;
            } catch (e) {
              copyErrors.push({ outputBasePath: base, slotIndex: slot, error: String(e?.message || e) });
            }
          })());
        }
      }
      await Promise.all(copyPromises);
      memLog("after copy step");

      // Step B: Build the JSONL for "edits" tasks. Each line carries a
      // routing key encoding setIndex and slotIndex so we can place
      // the result back in the right folder.
      //
      // MEMORY-CRITICAL DESIGN NOTE
      // ----------------------------
      // We never accumulate full line OBJECTS in memory. Instead:
      //   1. Worker fetches both reference images.
      //   2. Worker base64-encodes them into a single line object.
      //   3. Worker JSON.stringify's the line, encodes UTF-8 → Buffer.
      //   4. Worker pushes that Buffer into jsonlChunks[] and drops
      //      every other reference (line object, base64 strings, raw
      //      buffers) so GC can reclaim them immediately.
      //
      // This prevents the OOM that happened previously when we held
      // all 96 line objects (each ~1.6MB) alive simultaneously, then
      // doubled memory by serializing them into one giant string.
      // Per worker peak ≈ 5 MB; total peak ≈ jsonlChunks size + (10
      // workers × 5 MB).
      const jsonlChunks = []; // Buffer[] — each chunk is one JSONL line + "\n"
      const routes = []; // parallel array: routes[i] = { setIndex, slotIndex, outputBasePath }
      const fetchJobs = []; // { setIdx, slot, promptT, refPath, charmPath, outputBasePath }
      for (let setIdx = 0; setIdx < sets.length; setIdx++) {
        const s = sets[setIdx];
        for (const t of s.tasks) {
          if (String(t?.type) !== "edits") continue;
          const slot = Number(t?.slotIndex);
          if (!Number.isFinite(slot) || slot < 0) continue;
          const refPath = String(t?.input_storage_path || "").trim();
          const charmPath = String(t?.input_charm_storage_path || "").trim();
          const promptT = String(t?.prompt || "").trim();
          if (!refPath || !charmPath || !promptT) continue;
          fetchJobs.push({ setIdx, slot, promptT, refPath, charmPath, outputBasePath: s.outputBasePath });
        }
      }

      // Routes need a fixed slot per fetchJob. Pre-populate so workers
      // can write into routes[index] without needing a mutex.
      for (let i = 0; i < fetchJobs.length; i++) {
        const j = fetchJobs[i];
        routes.push({ setIndex: j.setIdx, slotIndex: j.slot, outputBasePath: j.outputBasePath });
      }

      // Fetch + build with low concurrency. See memory note above.
      // Concurrency 10 means at most 10 base64 working sets exist at
      // once. Each working set ≈ 5 MB peak (raw + base64 + stringified
      // line + Buffer chunk). After the worker returns, only the
      // pushed Buffer chunk in jsonlChunks survives.
      const FETCH_CONCURRENCY = 10;
      memLog(`before fetch loop (${fetchJobs.length} jobs)`);
      let fetchCounter = 0;
      await runBoundedConcurrent(fetchJobs, FETCH_CONCURRENCY, async (j, idx) => {
        const [ref, charm] = await Promise.all([
          storagePathToBuffer(j.refPath),
          storagePathToBuffer(j.charmPath),
        ]);
        const key = `s${j.setIdx}_slot${j.slot}`;

        // Build the line, stringify, encode to Buffer, then explicitly
        // null out big locals so V8 has a strong hint to free them
        // before the next worker iteration starts.
        let line = buildBatchJsonlLine(
          key, j.promptT,
          ref.mime, ref.buffer.toString("base64"),
          charm.mime, charm.buffer.toString("base64"),
          imageSize
        );
        let jsonStr = JSON.stringify(line) + "\n";
        line = null;
        const chunkBuf = Buffer.from(jsonStr, "utf8");
        jsonStr = null;

        // jsonlChunks order doesn't matter for correctness — the JSONL
        // routing key on each line tells Gemini which response is which.
        jsonlChunks.push(chunkBuf);
        // Periodic memory log so we see in-loop growth.
        const c = ++fetchCounter;
        if (c === 1 || c % 5 === 0 || c === fetchJobs.length) {
          memLog(`fetch ${c}/${fetchJobs.length}`);
        }
      });
      memLog(`after fetch loop`);

      if (jsonlChunks.length === 0) {
        // Edge case: all-copy batch (no Gemini calls needed). Write
        // manifests immediately and short-circuit. We emit slots in the
        // same shape as the gen-collect path so the Review tab renders
        // consistently regardless of which path produced the set.
        for (const s of sets) {
          const manifestPath = `${s.outputBasePath}/manifest.json`;
          const slotsOut = (s.tasks || [])
            .filter((t) => Number.isFinite(Number(t?.slotIndex)))
            .sort((a, b) => Number(a.slotIndex) - Number(b.slotIndex))
            .map((t) => {
              const slotIdx = Number(t.slotIndex);
              const slot = slotIdx + 1;
              const isCopy = String(t?.type) === "copy";
              return isCopy
                ? {
                    slot,
                    type: "copy",
                    source: t?.source_storage_path || null,
                    newCharm: null,
                    output: `${s.outputBasePath}/Slot_${slot}.png`,
                  }
                : {
                    slot,
                    type: "gen",
                    source: t?.input_storage_path || null,
                    newCharm: t?.input_charm_storage_path || null,
                    output: null, // not generated (no edits tasks)
                  };
            });
          const m = {
            category: s.category,
            setN: s.setN,
            outputBasePath: s.outputBasePath,
            // For consistency with the gen-collect path. Copy-only sets
            // usually have no gen tasks and therefore no charm, but if
            // there happens to be one task of type !=="copy" with a charm
            // path, we record it.
            sourceCharm: (() => {
              for (const t of (s.tasks || [])) {
                if (String(t?.type) !== "copy" && t?.input_charm_storage_path) {
                  return t.input_charm_storage_path;
                }
              }
              return null;
            })(),
            sourceCharmName: (() => {
              for (const t of (s.tasks || [])) {
                if (String(t?.type) !== "copy" && t?.input_charm_storage_path) {
                  return String(t.input_charm_storage_path).split("/").pop() || null;
                }
              }
              return null;
            })(),
            timestamp: new Date().toISOString(),
            model: GEMINI_IMAGE_MODEL,
            batchMode: false,
            copyOnly: true,
            slots: slotsOut,
          };
          await bucket.file(manifestPath).save(
            Buffer.from(JSON.stringify(m, null, 2), "utf8"),
            { contentType: "application/json", resumable: false }
          );
        }
        return json(200, {
          ok: true, batchName: null, copyOnly: true,
          copied: copiedCount, copyErrors, message: "All tasks were copy-only; no batch job needed."
        });
      }

      // Concat once into a single Buffer for upload. This is the only
      // moment when we hold the full JSONL in memory — and it's a
      // single contiguous Buffer, not a doubled UTF-16 string.
      memLog(`before Buffer.concat (${jsonlChunks.length} chunks)`);
      const jsonlBuffer = Buffer.concat(jsonlChunks);
      const jsonlBytes = jsonlBuffer.length;
      // Free chunk array — Buffer.concat copied them, originals can go.
      jsonlChunks.length = 0;
      memLog(`after Buffer.concat (${(jsonlBytes/1e6).toFixed(1)}MB JSONL)`);

      // Hard cap: Files API limit is 2GB. We reject early to avoid uploading.
      if (jsonlBytes > 1.9 * 1024 * 1024 * 1024) {
        return json(400, {
          error: { message: `JSONL too large (${(jsonlBytes / 1e9).toFixed(2)} GB). Reduce sets per batch.` }
        });
      }

      // Step C: Upload JSONL to Gemini Files API, then create the batch.
      const fileName = await uploadJsonlToGeminiFiles(apiKey, jsonlBuffer, displayName);
      memLog(`after Files API upload`);
      const { batchName, raw } = await createGeminiBatchJob(apiKey, GEMINI_IMAGE_MODEL, fileName, displayName);
      memLog(`after batch create`);

      // Step D: Persist routing data in Firestore. The doc id is a
      // sanitized batch name so the client can fetch it directly.
      const db = getDb();
      const docId = batchDocIdFromName(batchName);
      const persistDoc = {
        batchName,
        docId,
        displayName,
        sessionId: String(body?.sessionId || ""),
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        state: "JOB_STATE_PENDING",
        collected: false,
        model: GEMINI_IMAGE_MODEL,
        imageSize,
        inputFileName: fileName,
        inputJsonlBytes: jsonlBytes,
        // sets metadata (so we can route + write manifests later)
        sets: sets.map((s) => ({
          category: s.category,
          outputBasePath: s.outputBasePath,
          setN: s.setN,
          tasks: s.tasks, // store the raw task list so we can write a faithful manifest later
        })),
        routes,
        copyStats: { copied: copiedCount, errors: copyErrors },
        rawCreate: raw || null,
      };
      await firestoreRetry(
        () => db.collection(BATCHES_COLL).doc(docId).set(persistDoc, { merge: true }),
        "batch.create"
      );

      return json(200, {
        ok: true,
        batchName,
        docId,
        requestCount: routes.length,
        setsCount: sets.length,
        copyStats: { copied: copiedCount, errors: copyErrors },
        inputJsonlBytes: jsonlBytes,
      });
    }

    if (kind === "batch_status") {
      const apiKey = process.env.GEMINI_API_KEY;
      if (!apiKey) return json(400, { error: { message: "Missing GEMINI_API_KEY env var" } });

      const batchName = String(body?.batchName || "").trim();
      if (!batchName.startsWith("batches/")) {
        return json(400, { error: { message: "batchName must start with batches/" } });
      }

      const data = await getGeminiBatchJob(apiKey, batchName);
      // Gemini wraps the batch info under .metadata for long-running ops.
      // Per the docs, .metadata.state and .response.responsesFile are
      // where progress and the result file land.
      const state = data?.metadata?.state || data?.state || "UNKNOWN";
      const stats = data?.metadata?.batchStats || data?.batchStats || null;
      const respFile = data?.response?.responsesFile || data?.dest?.fileName || null;

      // Mirror state into Firestore so the dashboard can show it without
      // the user having a tab open during the polling phase.
      try {
        const db = getDb();
        const docId = batchDocIdFromName(batchName);
        await firestoreRetry(
          () => db.collection(BATCHES_COLL).doc(docId).set({
            state, batchStats: stats || null, responsesFile: respFile || null,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          }, { merge: true }),
          "batch.statusMirror"
        );
      } catch (_) { /* non-fatal */ }

      return json(200, {
        ok: true, batchName, state,
        batchStats: stats, responsesFile: respFile,
        done: state === "JOB_STATE_SUCCEEDED" || state === "JOB_STATE_FAILED" ||
              state === "JOB_STATE_CANCELLED" || state === "JOB_STATE_EXPIRED",
        succeeded: state === "JOB_STATE_SUCCEEDED",
      });
    }

    if (kind === "batch_collect") {
      const apiKey = process.env.GEMINI_API_KEY;
      if (!apiKey) return json(400, { error: { message: "Missing GEMINI_API_KEY env var" } });

      const batchName = String(body?.batchName || "").trim();
      if (!batchName.startsWith("batches/")) {
        return json(400, { error: { message: "batchName must start with batches/" } });
      }

      const db = getDb();
      const docId = batchDocIdFromName(batchName);
      const docSnap = await db.collection(BATCHES_COLL).doc(docId).get();
      if (!docSnap.exists) {
        return json(400, { error: { message: `No Firestore record for ${batchName}. Submit went through here?` } });
      }
      const docData = docSnap.data();

      if (docData.collected) {
        return json(200, { ok: true, alreadyCollected: true, batchName, results: docData.results || null });
      }

      const live = await getGeminiBatchJob(apiKey, batchName);
      const state = live?.metadata?.state || live?.state || "UNKNOWN";
      const forceMode = !!body?.force;

      // Standard download requires SUCCEEDED. Force download accepts any
      // state and pulls whatever responsesFile Google has produced —
      // useful when a job hangs, fails partway, or expires. Only failed
      // lines stay empty; successful ones still upload.
      if (!forceMode && state !== "JOB_STATE_SUCCEEDED") {
        return json(400, { error: { message: `Batch not succeeded; state=${state}. Use Force download to recover partial results.` } });
      }

      const respFileName = live?.response?.responsesFile || live?.dest?.fileName;
      if (!respFileName) {
        if (forceMode) {
          return json(400, { error: { message: `Force download: no result file available yet for state=${state}. Try again later or cancel.` } });
        }
        return json(500, { error: { message: "Succeeded batch has no responsesFile/fileName" } });
      }

      // STREAMING COLLECT
      // ------------------
      // We can't load the result JSONL into memory (potentially 2GB+) and
      // we can't queue every decoded image buffer either. Instead we
      // stream the JSONL line-by-line, decode each image inline, and
      // dispatch uploads through a bounded-concurrency worker pool.
      //
      // Concurrency limit (UPLOAD_CONCURRENCY) caps in-flight buffers,
      // keeping peak RAM at roughly UPLOAD_CONCURRENCY × ~3MB ≈ 60MB
      // even when collecting 100-set batches. Well under Netlify's
      // 1.5GB free-tier / 3GB Pro-tier function memory cap.
      const UPLOAD_CONCURRENCY = 20;

      const bucket = admin.storage().bucket();
      const routes = Array.isArray(docData.routes) ? docData.routes : [];
      const setsMeta = Array.isArray(docData.sets) ? docData.sets : [];

      const routeByKey = new Map();
      for (let i = 0; i < routes.length; i++) {
        const r = routes[i];
        routeByKey.set(`s${r.setIndex}_slot${r.slotIndex}`, r);
      }

      let succeededCount = 0;
      let failedCount = 0;
      const perSetSlotResults = new Map();
      const failures = [];

      // Producer: streams parsed lines from Google's result JSONL and
      // pushes upload tasks into a bounded queue. Consumers drain the
      // queue concurrently. We use a simple semaphore (slots[]) to
      // throttle in-flight uploads without buffering the whole list.
      const inFlight = new Set();
      const recordResult = (route, ok, storagePath, error) => {
        const arr = perSetSlotResults.get(route.setIndex) || [];
        if (ok) {
          succeededCount++;
          arr.push({ slotIndex: route.slotIndex, ok: true, storagePath });
        } else {
          failedCount++;
          failures.push({ key: `s${route.setIndex}_slot${route.slotIndex}`, error });
          arr.push({ slotIndex: route.slotIndex, ok: false, error });
        }
        perSetSlotResults.set(route.setIndex, arr);
      };

      const uploadOne = async (route, buffer, key) => {
        try {
          const storagePath = `${route.outputBasePath}/Slot_${route.slotIndex + 1}.png`;
          const file = bucket.file(storagePath);
          const token = newDownloadToken();
          await file.save(buffer, {
            resumable: false,
            contentType: "image/png",
            metadata: { metadata: { firebaseStorageDownloadTokens: token } },
          });
          recordResult(route, true, storagePath);
        } catch (e) {
          recordResult(route, false, null, String(e?.message || e));
        }
        // Buffer drops out of scope once this fn returns; GC reclaims it.
      };

      // Drive the stream. For each line, parse → find route → decode b64
      // → spawn upload promise. If we hit the concurrency cap, wait for
      // the fastest one to finish before spawning the next. This keeps
      // memory bounded by UPLOAD_CONCURRENCY × per-image size.
      try {
        for await (const parsed of streamGeminiResultLines(apiKey, respFileName)) {
          const key = parsed?.key;
          const route = routeByKey.get(key);
          if (!route) {
            failures.push({ key: key || "(none)", error: "no route for key" });
            continue;
          }
          if (parsed?.error) {
            recordResult(route, false, null, parsed.error?.message || "batch error");
            continue;
          }
          const partsOut = parsed?.response?.candidates?.[0]?.content?.parts || [];
          const imgPart = partsOut.find((p) => p?.inline_data?.data) ||
                          partsOut.find((p) => p?.inlineData?.data) || null;
          const b64 = imgPart?.inline_data?.data || imgPart?.inlineData?.data;
          if (!b64) {
            recordResult(route, false, null, "no inline_data in response");
            continue;
          }

          // Decode + queue. Throttle once we hit the concurrency cap.
          const buffer = Buffer.from(b64, "base64");
          const p = uploadOne(route, buffer, key).finally(() => inFlight.delete(p));
          inFlight.add(p);
          if (inFlight.size >= UPLOAD_CONCURRENCY) {
            await Promise.race(inFlight);
          }
        }
        // Drain any remaining in-flight uploads.
        await Promise.all(inFlight);
      } catch (streamErr) {
        // If the stream itself fails, surface the partial state so the
        // user can see what was already uploaded before the break.
        await Promise.all(inFlight);
        failures.push({ key: "(stream)", error: String(streamErr?.message || streamErr) });
      }

      // Step: write a manifest per set, mirroring what Standard mode writes.
      // Manifest format intentionally matches the existing structure so the
      // Approved/Review tabs render batch results identically.
      //
      // The Review tab (loadSetImages → manifest.slots[].source) needs each
      // slot entry to carry its original `source` storage path so the UI
      // can display "Source: …". Standard mode also writes `newCharm` for
      // gen slots; we preserve that field here for cross-mode parity.
      for (let setIdx = 0; setIdx < setsMeta.length; setIdx++) {
        const s = setsMeta[setIdx];
        const slotResults = (perSetSlotResults.get(setIdx) || []).sort((a, b) => a.slotIndex - b.slotIndex);

        // Build a slotIndex → original-task lookup so we can pull source/newCharm.
        const taskBySlot = new Map();
        for (const t of (s.tasks || [])) {
          if (Number.isFinite(Number(t?.slotIndex))) taskBySlot.set(Number(t.slotIndex), t);
        }

        // Compose the slots array. We emit one entry per planned slot
        // (not just the ones that came back from Gemini), so the Review
        // tab can render every slot with its source label even if a
        // particular gen failed.
        const planSlotsByIndex = new Map();
        for (const t of (s.tasks || [])) {
          if (Number.isFinite(Number(t?.slotIndex))) planSlotsByIndex.set(Number(t.slotIndex), t);
        }
        const allSlotIndices = Array.from(planSlotsByIndex.keys()).sort((a, b) => a - b);

        const slotsOut = allSlotIndices.map((slotIdx) => {
          const t = planSlotsByIndex.get(slotIdx);
          // Look up generation result, if any (copies have no result entry — they
          // succeeded synchronously at submit time).
          const r = slotResults.find((x) => x.slotIndex === slotIdx);
          const isCopy = String(t?.type) === "copy";
          // For copy tasks, source == the synced source; output path is canonical.
          const slot = slotIdx + 1;
          if (isCopy) {
            return {
              slot,
              type: "copy",
              source: t?.source_storage_path || null,
              newCharm: null,
              output: `${s.outputBasePath}/Slot_${slot}.png`,
            };
          }
          // Gen task. r may be undefined if Gemini didn't return this key
          // (e.g., upstream filtering); we still emit the slot for UI consistency.
          return {
            slot,
            type: r?.ok === false ? "error" : "gen",
            source: t?.input_storage_path || null,
            newCharm: t?.input_charm_storage_path || null,
            output: (r?.ok && r?.storagePath) ? r.storagePath : null,
            error: r?.error || null,
          };
        });

        const m = {
          category: s.category,
          setN: s.setN,
          outputBasePath: s.outputBasePath,
          // Lock the listing's charm into the manifest so the Review tab's
          // Redo flow can recover it even after batch_collect moves the
          // charm out of New_Charms/ into Used_*_Charm_Pool/. We pull
          // from the first gen task with an input_charm_storage_path; in
          // batch mode every gen task in a set shares the same charm
          // (one charm per listing).
          sourceCharm: (() => {
            for (const t of (s.tasks || [])) {
              if (String(t?.type) !== "copy" && t?.input_charm_storage_path) {
                return t.input_charm_storage_path;
              }
            }
            return null;
          })(),
          sourceCharmName: (() => {
            for (const t of (s.tasks || [])) {
              if (String(t?.type) !== "copy" && t?.input_charm_storage_path) {
                return String(t.input_charm_storage_path).split("/").pop() || null;
              }
            }
            return null;
          })(),
          timestamp: new Date().toISOString(),
          model: GEMINI_IMAGE_MODEL,
          batchMode: true,
          batchName,
          batchState: state,
          partial: forceMode && state !== "JOB_STATE_SUCCEEDED",
          imageSize: docData.imageSize || "2K",
          slots: slotsOut,
        };
        const manifestPath = `${s.outputBasePath}/manifest.json`;
        try {
          await bucket.file(manifestPath).save(
            Buffer.from(JSON.stringify(m, null, 2), "utf8"),
            { contentType: "application/json", resumable: false }
          );
        } catch (e) {
          failures.push({ key: `manifest:${s.outputBasePath}`, error: String(e?.message || e) });
        }
      }

      // Move used charms out of the active pool.
      // ---------------------------------------
      // After successful collection, every unique charm path referenced
      // in this batch's tasks is moved to the matching Used pool:
      //   New_Charms          → Used_Necklace_Charm_Pool
      //   New_Charms_Earrings → Used_Earring_Charm_Pool
      //
      // This ensures the same charm never gets picked again by a
      // future submission. Move happens AFTER images write so a
      // mid-collection failure doesn't strand the charm in the wrong
      // place. We use copy+delete (not rename, which Firebase Storage
      // doesn't support natively) and ignore per-charm errors so a
      // single flaky charm doesn't block the batch from being marked
      // collected.
      const charmsUsed = new Set();
      for (const s of setsMeta) {
        for (const t of (s.tasks || [])) {
          const p = t?.input_charm_storage_path;
          if (p) charmsUsed.add(p);
        }
      }
      const charmMoveErrors = [];
      let charmsMoved = 0;
      for (const srcPath of charmsUsed) {
        try {
          let destPrefix = null;
          if (srcPath.includes("/Charm_Maker/New_Charms_Earrings/")) {
            destPrefix = "listing-generator-1/Charm_Maker/Used_Earring_Charm_Pool/";
          } else if (srcPath.includes("/Charm_Maker/New_Charms/")) {
            destPrefix = "listing-generator-1/Charm_Maker/Used_Necklace_Charm_Pool/";
          } else {
            // Charm came from somewhere else — don't move it.
            continue;
          }
          const filename = srcPath.split("/").pop();
          if (!filename) continue;
          const destPath = destPrefix + filename;

          const srcFile = bucket.file(srcPath);
          const [exists] = await srcFile.exists();
          if (!exists) {
            // Already moved by an earlier collection of an overlapping
            // batch — not an error, just skip.
            continue;
          }
          await srcFile.copy(bucket.file(destPath));
          // Preserve a download token on the destination so it renders
          // in any UI that expects one.
          try {
            await bucket.file(destPath).setMetadata({
              metadata: { firebaseStorageDownloadTokens: newDownloadToken() },
            });
          } catch (_) {}
          await srcFile.delete();
          charmsMoved++;
        } catch (e) {
          charmMoveErrors.push({ charm: srcPath, error: String(e?.message || e) });
        }
      }

      // Mark Firestore record as collected.
      await firestoreRetry(
        () => db.collection(BATCHES_COLL).doc(docId).set({
          collected: true,
          collectedAt: admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          results: {
            succeededCount, failedCount, failures: failures.slice(0, 200),
            charmsMoved, charmMoveErrors: charmMoveErrors.slice(0, 50),
          },
        }, { merge: true }),
        "batch.markCollected"
      );

      return json(200, {
        ok: true, batchName, collected: true,
        succeededCount, failedCount,
        charmsMoved, charmMoveErrors: charmMoveErrors.slice(0, 20),
        failures: failures.slice(0, 50),
      });
    }

    if (kind === "batch_list") {
      const includeCollected = !!body?.includeCollected;
      // Ceiling raised from 200 → 1000 so the panel and the per-batch
      // collect-poll loop can find batches in submissions of up to 1000
      // sets. Default stays at 50 (cheap query for the common small case).
      const limit = clampNumber(body?.limit, 1, 1000, 50);
      const db = getDb();
      let q = db.collection(BATCHES_COLL).orderBy("createdAt", "desc").limit(limit);
      const snap = await q.get();
      const out = [];
      snap.forEach((doc) => {
        const d = doc.data();
        if (!includeCollected && d.collected) return;
        out.push({
          docId: doc.id,
          batchName: d.batchName,
          displayName: d.displayName,
          sessionId: d.sessionId || null,
          state: d.state,
          collected: !!d.collected,
          batchStats: d.batchStats || null,
          createdAt: d.createdAt?.toMillis ? d.createdAt.toMillis() : null,
          updatedAt: d.updatedAt?.toMillis ? d.updatedAt.toMillis() : null,
          collectedAt: d.collectedAt?.toMillis ? d.collectedAt.toMillis() : null,
          setsCount: Array.isArray(d.sets) ? d.sets.length : 0,
          requestCount: Array.isArray(d.routes) ? d.routes.length : 0,
          results: d.results || null,
        });
      });
      return json(200, { ok: true, batches: out });
    }

    if (kind === "batch_cancel") {
      const apiKey = process.env.GEMINI_API_KEY;
      if (!apiKey) return json(400, { error: { message: "Missing GEMINI_API_KEY env var" } });
      const batchName = String(body?.batchName || "").trim();
      if (!batchName.startsWith("batches/")) {
        return json(400, { error: { message: "batchName must start with batches/" } });
      }
      await cancelGeminiBatchJob(apiKey, batchName);
      try {
        const db = getDb();
        await firestoreRetry(
          () => db.collection(BATCHES_COLL).doc(batchDocIdFromName(batchName)).set({
            state: "JOB_STATE_CANCELLED",
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          }, { merge: true }),
          "batch.cancelMirror"
        );
      } catch (_) { /* non-fatal */ }
      return json(200, { ok: true, cancelled: true, batchName });
    }

    // ------------------------------------------------------------
    // files_cleanup
    //   Lists ALL files uploaded to the Gemini Files API for this API key
    //   and deletes them. Used to free the 20 GB cumulative
    //   file_storage_bytes quota that accumulates across batch_submit
    //   JSONL/reference uploads.
    //
    //   Optional body params:
    //     maxDelete  number  — cap deletion at N files this call (default:
    //                          no cap; use for chunked cleanup if you
    //                          have thousands of files and the function
    //                          might time out)
    //     prefix     string  — only delete files whose displayName starts
    //                          with this prefix (e.g., "lg1-Beady_Necklace-").
    //                          Useful if the same API key is shared with
    //                          other projects.
    //
    //   Returns:
    //     { ok, deleted, totalListed, candidateCount, bytesFreed,
    //       truncated, errors }
    //
    //   CAUTION: deleting Files API uploads breaks any in-flight batch
    //   jobs that still reference those input files. Caller is responsible
    //   for confirmation before invoking.
    // ------------------------------------------------------------
    if (kind === "files_cleanup") {
      const apiKey = process.env.GEMINI_API_KEY;
      if (!apiKey) return json(400, { error: { message: "Missing GEMINI_API_KEY env var" } });

      const maxDeleteRaw = Number(body?.maxDelete);
      const maxDelete = Number.isFinite(maxDeleteRaw) && maxDeleteRaw > 0 ? maxDeleteRaw : Infinity;
      const prefix = typeof body?.prefix === "string" && body.prefix.length > 0 ? body.prefix : null;

      const baseUrl = "https://generativelanguage.googleapis.com/v1beta";

      // 1) Paginate through all files. Hard guard against runaway
      //    pagination at 200 pages × 100/page = 20,000 files.
      const listed = [];
      let pageToken = null;
      let pages = 0;
      const MAX_PAGES = 200;
      do {
        const url = `${baseUrl}/files?key=${encodeURIComponent(apiKey)}&pageSize=100` +
                    (pageToken ? `&pageToken=${encodeURIComponent(pageToken)}` : "");
        const r = await fetch(url);
        if (!r.ok) {
          const errBody = await r.text().catch(() => "");
          return json(r.status, {
            error: { message: `Files API list failed: HTTP ${r.status}: ${errBody.slice(0, 300)}` },
          });
        }
        const data = await r.json();
        for (const f of (data.files || [])) listed.push(f);
        pageToken = data.nextPageToken || null;
        pages++;
      } while (pageToken && pages < MAX_PAGES);

      // 2) Optional displayName prefix filter
      const candidates = prefix
        ? listed.filter((f) => typeof f.displayName === "string" && f.displayName.startsWith(prefix))
        : listed.slice();

      // 3) Cap deletion count (for chunked cleanup if maxDelete supplied)
      const toDelete = maxDelete === Infinity
        ? candidates
        : candidates.slice(0, maxDelete);

      // 4) Delete with concurrency 10 — fast enough that even 1000s of
      //    files complete inside the 15-minute background-function budget,
      //    while not so parallel that we trigger our own rate-limit on
      //    the Files API delete endpoint.
      let deleted = 0;
      let bytesFreed = 0;
      const errors = [];
      let idx = 0;
      const CONC = 10;

      async function deleteWorker() {
        while (idx < toDelete.length) {
          const myIdx = idx++;
          const f = toDelete[myIdx];
          try {
            const delUrl = `${baseUrl}/${f.name}?key=${encodeURIComponent(apiKey)}`;
            const r = await fetch(delUrl, { method: "DELETE" });
            if (!r.ok) {
              const errBody = await r.text().catch(() => "");
              errors.push(`${f.name}: HTTP ${r.status} ${errBody.slice(0, 80)}`);
            } else {
              deleted++;
              bytesFreed += Number(f.sizeBytes || 0);
            }
          } catch (e) {
            errors.push(`${f.name}: ${e?.message || e}`);
          }
        }
      }

      const workers = [];
      for (let i = 0; i < CONC; i++) workers.push(deleteWorker());
      await Promise.all(workers);

      return json(200, {
        ok: true,
        deleted,
        totalListed: listed.length,
        candidateCount: candidates.length,
        bytesFreed,
        truncated: candidates.length > toDelete.length,
        errors: errors.slice(0, 20),
      });
    }

// ------------------------------------------------------------
    // NEW: run_set_async
    // - One request kicks off the whole set
    // - Server processes tasks ASYNCHRONOUSLY (Parallel)
    // - Enforces delayMs only on START times (staggered launch)
    // ------------------------------------------------------------
    if (kind === "run_set_async") {
      const cat = normalizeCategory(activeCategory);
      if (!GENERATABLE_CATEGORIES.has(cat)) {
        return json(400, { error: { message: "activeCategory not generatable" } });
      }

      const base = assertAllowedOutputBase(output_base_path);
      const delayMs = clampNumber(body?.delayMs ?? body?.delay_ms ?? 1000, 0, 10000, 1000);
      const tasks = Array.isArray(body?.tasks) ? body.tasks : null;
      if (!tasks || !tasks.length) {
        return json(400, { error: { message: "tasks must be a non-empty array" } });
      }
      if (tasks.length > 8) {
        return json(400, { error: { message: "tasks max length is 8" } });
      }

      const apiKey = process.env.GEMINI_API_KEY;
      if (!apiKey) return json(400, { error: { message: "Missing GEMINI_API_KEY env var" } });

      const runToken =
        globalThis.crypto?.randomUUID ? globalThis.crypto.randomUUID() : require("crypto").randomUUID();

      // ✅ FIX: Use Promise.all() to run tasks in parallel
      // We map the tasks to an array of promises, allowing them to execute concurrently.
      await Promise.all(tasks.map(async (t, i) => {
        const slot = Number(t?.slotIndex);
        if (!Number.isFinite(slot) || slot < 0) return;

        try {
          // ✅ STAGGERED START:
          // Instead of waiting for the previous task to *finish*, we only delay the *start*.
          // Task 0 starts at 0ms. Task 1 starts at 1500ms (if delayMs=1500).
          // They will all be processing simultaneously after the initial delay.
          const startDelay = i * delayMs;
          if (startDelay > 0) await sleep(startDelay);

          const bucket = admin.storage().bucket();

          // 1. Handle "Copy" tasks (instant)
          if (String(t?.type) === "copy") {
            const src = String(t?.source_storage_path || "").trim();
            if (!src) throw new Error("copy task missing source_storage_path");
            const dst = `${base}/Slot_${slot + 1}.png`;
            const dstFile = bucket.file(dst);
            
            await bucket.file(src).copy(dstFile);

            // Ensure browser previews can load: getDownloadURL() relies on firebaseStorageDownloadTokens.
            const token = newDownloadToken();
            await dstFile.setMetadata({ metadata: { firebaseStorageDownloadTokens: token } });
            return; // Done with this task
          }

          // 2. Handle "Edits" tasks (Gemini)
          const basePath0 = String(t?.input_storage_path || "").trim();
          const basePath1 = String(t?.input_charm_storage_path || "").trim();
          const promptT = String(t?.prompt || "").trim();
          
          if (!basePath0 || !basePath1 || !promptT) {
               console.warn(`Skipping invalid task slot ${slot}`);
               return;
          }

          const img0 = await storagePathToBuffer(basePath0);
          const img1 = await storagePathToBuffer(basePath1);

          // ✅ RETRY WRAPPER: Handles "Model overloaded" (503/429) automatically
          let outBuf = await callGeminiWithRetry(async () => {
             return await callGeminiImagesEdits({
              apiKey,
              model: GEMINI_IMAGE_MODEL,
              prompt: promptT,
              size: String(t?.size || body?.size || "2048x2048"),
              quality: "high",
              output_format: "png",
              images: [
                { buffer: img0.buffer, mime: img0.mime, filename: filenameForMime("image0", img0.mime) },
                { buffer: img1.buffer, mime: img1.mime, filename: filenameForMime("image1", img1.mime) },
              ],
            });
          }, `slot_${slot}`);

          outBuf = await applyFinalFrameZoomIfNeeded(outBuf, t?.postprocess || body?.postprocess);
          
          // Upload result (Client polling will detect this file appearing)
          await uploadPngBufferToSetPath(outBuf, base, slot, null, runToken);

        } catch (err) {
          console.error(`[run_set_async] task failed at slot ${slot}`, safeErr(err));
          // We catch errors here so Promise.all() doesn't fail the entire set if one slot fails.
        }
      }));

      // Return successful completion (client received 202 long ago)
      return json(200, { ok: true, finished: true, runId: runToken });
    }

    if (kind === "copy_to_slot") {
      const cat = normalizeCategory(activeCategory);
      if (!GENERATABLE_CATEGORIES.has(cat)) return json(400, { error: { message: "activeCategory not generatable" } });

      const src = String(source_storage_path || "").trim();
      if (!src) return json(400, { error: { message: "source_storage_path is required" } });

      const base = assertAllowedOutputBase(output_base_path);
      const effectiveSlot = Number.isFinite(Number(slotIndex)) && Number(slotIndex) >= 0 ? Number(slotIndex) : 0;
      const dst = `${base}/Slot_${effectiveSlot + 1}.png`;

      const bucket = admin.storage().bucket();
       const dstFile = bucket.file(dst);
       await bucket.file(src).copy(dstFile);

       // Ensure a Firebase download token exists on the copied object.
       const token = newDownloadToken();
       await dstFile.setMetadata({ metadata: { firebaseStorageDownloadTokens: token } });
       const downloadURL = tokenDownloadURLFor(bucket.name, dst, token);
      return json(200, { ok: true, storagePath: dst, downloadURL });
    }

   if (kind === "edits") {
      const cat = normalizeCategory(activeCategory);
      if (!GENERATABLE_CATEGORIES.has(cat)) return json(400, { error: { message: "activeCategory not generatable" } });

      const basePath0 = String(input_storage_path || "").trim();
      const basePath1 = String(input_charm_storage_path || "").trim();
      if (!basePath0) return json(400, { error: { message: "input_storage_path is required" } });
      // input_charm_storage_path is OPTIONAL.
      //   • Two-image flow (regeneration / fresh composite): caller supplies
      //     both — image0 is the model/reference, image1 is the charm — and
      //     Gemini composites them per the prompt.
      //   • One-image flow (in-place edit / Listing-Generator adjustment-mode
      //     redo): caller supplies only basePath0 and a prompt that describes
      //     the requested edit. We send a single image to Gemini so it does
      //     not search for differences between two identical inputs or try
      //     to composite them.

      const apiKey = process.env.GEMINI_API_KEY;
      if (!apiKey) return json(400, { error: { message: "Missing GEMINI_API_KEY env var" } });

      const outputBasePath = assertAllowedOutputBase(output_base_path);
      const effectiveSlot = Number.isFinite(Number(slotIndex)) && Number(slotIndex) >= 0 ? Number(slotIndex) : 0;

      const img0 = await storagePathToBuffer(basePath0);
      const img1 = basePath1 ? await storagePathToBuffer(basePath1) : null;

      const images = [
        { buffer: img0.buffer, mime: img0.mime, filename: filenameForMime("image0", img0.mime) },
      ];
      if (img1) {
        images.push({ buffer: img1.buffer, mime: img1.mime, filename: filenameForMime("image1", img1.mime) });
      }

      let outBuf = await callGeminiImagesEdits({
        apiKey,
        model,
        prompt,
        size,
        quality,
        output_format,
        images,
      });

      outBuf = await applyFinalFrameZoomIfNeeded(outBuf, postprocess);

      const saved = await uploadPngBufferToSetPath(outBuf, outputBasePath, effectiveSlot, null, null);
      return json(200, { ok: true, storagePath: saved.storagePath, downloadURL: saved.downloadURL });
    }

    if (kind === "write_manifest") {
      const cat = normalizeCategory(activeCategory);
      if (!GENERATABLE_CATEGORIES.has(cat)) return json(400, { error: { message: "activeCategory not generatable" } });

      const base = assertAllowedOutputBase(output_base_path);
      const bucket = admin.storage().bucket();
      const p = `${base}/manifest.json`;
      const buf = Buffer.from(JSON.stringify(manifest || {}, null, 2), "utf8");
      await bucket.file(p).save(buf, { contentType: "application/json", resumable: false });
      return json(200, { ok: true, storagePath: p });
    }

    // ============================================================
    // move_set_to_completed (v5.31)
    // ------------------------------------------------------------
    // Server-side replacement for the browser-driven
    // runBackgroundApproval flow in Listing_Generator_1.html.
    //
    // The old flow downloaded every PNG from Ready_To_List/Set_N
    // into the browser, re-uploaded it to Completed_Listing_Sets/
    // {cat}_Set_N, then deleted the source. That round-tripped 100%
    // of the bytes through the operator's bandwidth and Firebase
    // egress on every approval. For our typical 4–8 MB per slot ×
    // 4–6 slots per set × dozens of approvals/day, this was a
    // significant chunk of the monthly bandwidth bill — and worse,
    // it could leave files orphaned in Ready_To_List if any step
    // failed mid-loop because the UI hides the set immediately
    // (addHiddenSets) without retrying.
    //
    // This handler does the same work entirely server-side:
    //   • bucket.file(src).copy(dst) is a Google-internal byte
    //     transfer with no egress charge.
    //   • Manifest tidying for skipped slots happens here too, so
    //     the client doesn't need to know about manifest structure.
    //   • Per-file errors are collected and returned so the client
    //     can decide whether to surface them; the loop continues
    //     past failures (best-effort) so a single flaky file
    //     doesn't strand a 95%-complete approval.
    //
    // Body:
    //   {
    //     kind        : "move_set_to_completed",
    //     activeCategory: "Beady_Necklace" | ... (one of GENERATABLE_CATEGORIES),
    //     setName     : "Set_42",           // matches /^Set_\d+$/
    //     skippedSlots: ["Slot_3.png", ...] // optional; these get
    //                                       // deleted from src instead of moved
    //   }
    //
    // Response:
    //   { ok: true, moved: N, skippedDeleted: M, errors: [...] }
    //
    // The client should treat ANY non-2xx response or `ok:false` as
    // a signal to fall back to the legacy browser-mediated flow so
    // approvals never get stuck on a deploy lag.
    if (kind === "move_set_to_completed") {
      const cat = normalizeCategory(activeCategory);
      if (!GENERATABLE_CATEGORIES.has(cat)) {
        return json(400, { error: { message: "activeCategory not generatable" } });
      }
      const setName = String(body?.setName || "").trim();
      if (!/^Set_\d+$/.test(setName)) {
        return json(400, { error: { message: "setName must look like 'Set_<n>'" } });
      }
      const skippedSlots = Array.isArray(body?.skippedSlots)
        ? body.skippedSlots.filter((s) => typeof s === "string" && /^Slot_\d+\.png$/.test(s))
        : [];
      const skipSet = new Set(skippedSlots);

      const bucket = admin.storage().bucket();
      const srcPrefix = `listing-generator-1/${cat}/Ready_To_List/${setName}/`;
      const dstPrefix = `listing-generator-1/Generated_Listing_Sets/Completed_Listing_Sets/${cat}_${setName}/`;

      // Step 1: Enumerate source files. getFiles with a prefix is a
      // single-shot list — no pagination needed at this scale (a set
      // is at most ~10 files).
      let files;
      try {
        const [f] = await bucket.getFiles({ prefix: srcPrefix });
        files = f;
      } catch (e) {
        return json(500, { ok: false, error: { message: `getFiles failed: ${e?.message || e}` } });
      }
      if (!files.length) {
        // Nothing to do — set folder is already empty. Treat as success
        // so callers (and the optional fallback path on the client) can
        // proceed without surfacing a confusing "no files" error.
        return json(200, { ok: true, moved: 0, skippedDeleted: 0, errors: [], note: "source folder empty" });
      }

      // Step 2: If there are skipped slots and a manifest is present,
      // tidy the manifest before moving. We mirror the exact transform
      // the old browser code performed:
      //   slots[i] = { ...slots[i], skipped: true, output: null }
      //     for any slot whose Slot_<n>.png is in skipSet.
      // This keeps the manifest semantically truthful in its new home.
      const manifestFile = files.find((f) => f.name === srcPrefix + "manifest.json");
      let tidiedManifestBuf = null;
      if (manifestFile && skipSet.size > 0) {
        try {
          const [raw] = await manifestFile.download();
          const data = JSON.parse(raw.toString("utf8"));
          const tidied = {
            ...data,
            slots: (data.slots || []).map((s) =>
              s && typeof s.slot === "number" && skipSet.has(`Slot_${s.slot}.png`)
                ? { ...s, skipped: true, output: null }
                : s
            ),
          };
          tidiedManifestBuf = Buffer.from(JSON.stringify(tidied, null, 2), "utf8");
        } catch (e) {
          // Manifest unreadable — proceed without tidying. The original
          // manifest will be copied verbatim below, which mirrors the
          // browser code's fallback when its own manifest read fails.
          console.warn(`[move_set_to_completed] manifest read failed for ${srcPrefix}: ${e?.message || e}`);
        }
      }

      // Step 3: Walk files, mirroring the browser logic.
      let moved = 0;
      let skippedDeleted = 0;
      const errors = [];
      for (const f of files) {
        const filename = f.name.slice(srcPrefix.length);
        if (!filename) continue; // pseudo-folder entry

        try {
          if (skipSet.has(filename)) {
            // Skipped slot → delete from src, do not copy.
            await f.delete();
            skippedDeleted++;
          } else if (filename === "manifest.json" && tidiedManifestBuf) {
            // Manifest with tidying applied → write tidied version to
            // dst, then delete src.
            const dstFile = bucket.file(dstPrefix + filename);
            await dstFile.save(tidiedManifestBuf, {
              contentType: "application/json",
              resumable: false,
            });
            await f.delete();
            moved++;
          } else {
            // Default: server-side copy + delete.
            const dstFile = bucket.file(dstPrefix + filename);
            await f.copy(dstFile);
            await f.delete();
            moved++;
          }
        } catch (e) {
          // Per-file failure: log + continue. Returning the error list
          // lets the client decide whether to retry or fall back.
          errors.push({ file: filename, error: String(e?.message || e) });
        }
      }

      return json(200, {
        ok: errors.length === 0,
        moved,
        skippedDeleted,
        errors: errors.slice(0, 20),
        srcPrefix,
        dstPrefix,
      });
    }
  } catch (e) {
    return json(400, { ok: false, error: safeErr(e) });
  }

  // ---------- existing job-based operations (jobId required) ----------
  if (!jobId) return json(400, { error: { message: "jobId is required" } });

  const db = getDb();
  const jobRef = db.collection(JOBS_COLL).doc(jobId);

  try {
    await firestoreRetry(
      () =>
        jobRef.set(
          {
            status: "running",
            stage: "starting",
            runId: runId || null,
            slotIndex: typeof slotIndex === "number" ? slotIndex : null,
            kind,
            model,
            clientModel: _clientModel || null,
            activeCategory: activeCategory || null,
            outputBasePath: output_base_path || null,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
            createdAt: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        ),
      "jobRef.set"
    );

    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) throw new Error("Missing GEMINI_API_KEY env var");

    // -------------------------
    // SPECIAL: charm_postscale
    // -------------------------
    if (kind === "charm_postscale") {
      if (!input_storage_path && !input_image) {
        throw new Error("charm_postscale requires input_storage_path or input_image (Pass A output)");
      }

      await firestoreRetry(
        () =>
          jobRef.set(
            {
              stage: "downloading_inputs",
              updatedAt: admin.firestore.FieldValue.serverTimestamp(),
            },
            { merge: true }
          ),
        "jobRef.set"
      );

      const passA = input_storage_path
        ? await storagePathToBuffer(input_storage_path)
        : dataUrlToBuffer(input_image);

      await firestoreRetry(() => jobRef.set(
        { stage: "removing_charm", updatedAt: admin.firestore.FieldValue.serverTimestamp() },
        { merge: true }
      ), "jobRef.set");

      let rp = String(remove_prompt || "").trim();
      let baseNoCharmBuf;

      if (base_storage_path || base_image) {
        const base = base_storage_path
          ? await storagePathToBuffer(base_storage_path)
          : dataUrlToBuffer(base_image);

        const [mA, mB] = await Promise.all([
          sharp(passA.buffer).metadata(),
          sharp(base.buffer).metadata(),
        ]);

        if (mA?.width && mA?.height && (mA.width !== mB.width || mA.height !== mB.height)) {
          baseNoCharmBuf = await sharp(base.buffer)
            .resize(mA.width, mA.height, { kernel: "lanczos3" })
            .png()
            .toBuffer();
        } else {
          baseNoCharmBuf = base.buffer;
        }
      } else {
        rp = rp || "Remove the pendant charm + jump ring completely...";
        baseNoCharmBuf = await callGeminiImagesEdits({
          apiKey,
          model,
          prompt: rp,
          size,
          quality,
          output_format,
          images: [{ buffer: passA.buffer, mime: passA.mime, filename: "passA.png" }],
        });
      }

      await firestoreRetry(() => jobRef.set(
        { stage: "postprocessing", updatedAt: admin.firestore.FieldValue.serverTimestamp() },
        { merge: true }
      ), "jobRef.set");

      let finalBuf = await postScaleCharmComposite({
        passABuf: passA.buffer,
        baseNoCharmBuf,
        targetPx: postprocess?.targetPx,
        scale: postprocess?.scale,
        shadowOpacity: postprocess?.shadowOpacity,
        shadowBlur: postprocess?.shadowBlur,
        diffThreshold: postprocess?.diffThreshold,
      });

      finalBuf = await applyFinalFrameZoomIfNeeded(finalBuf, postprocess);

      await firestoreRetry(() => jobRef.set(
        { stage: "uploading", updatedAt: admin.firestore.FieldValue.serverTimestamp() },
        { merge: true }
      ), "jobRef.set");

      const { storagePath, downloadURL, effectiveRunId, effectiveSlot } =
      await uploadPngBufferToSetPath(finalBuf, output_base_path, slotIndex, jobId, runId);

      await firestoreRetry(() => db.collection(IMAGES_COLL).add({
        runId: effectiveRunId,
        slotIndex: effectiveSlot,
        createdAt: new Date(),
        storagePath,
        downloadURL,
        model,
        prompt: rp,
        traits: body.traits || null,
        jobId,
        kind,
        postprocess: postprocess || null,
      }), "images.add");

      await firestoreRetry(
        () =>
          jobRef.set(
            {
              status: "done",
              stage: "done",
              storagePath,
              downloadURL,
              finishedAt: admin.firestore.FieldValue.serverTimestamp(),
              updatedAt: admin.firestore.FieldValue.serverTimestamp(),
            },
            { merge: true }
          ),
        "jobRef.set"
      );

      return json(202, { ok: true, jobId });
    }

    // -------------------------
    // DEFAULT: edits / generations behavior
    // -------------------------
    if (!prompt) return json(400, { error: { message: "prompt is required" } });

    if (kind !== "edits" && kind !== "generations") {
      return json(400, { error: { message: "kind must be 'edits', 'generations', or 'charm_postscale' (or use alloc_set/copy_to_slot/write_manifest)" } });
    }

    await firestoreRetry(
      () =>
        jobRef.set(
          {
            stage: "uploading",
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        ),
      "jobRef.set"
    );

    let outBuf;

    if (kind === "generations") {
      outBuf = await callGeminiImagesGenerations({
        apiKey,
        model,
        prompt,
        size,
        quality,
        output_format,
      });
    } else {
      // kind === "edits"
      if (!input_image && !input_storage_path) {
        return json(400, { error: { message: "Missing input_image or input_storage_path" } });
      }

      const ref = input_storage_path
        ? await storagePathToBuffer(input_storage_path)
        : dataUrlToBuffer(input_image);

      let charm = null;
      if (input_charm_storage_path || input_charm_image) {
        charm = input_charm_storage_path
          ? await storagePathToBuffer(input_charm_storage_path)
          : dataUrlToBuffer(input_charm_image);
      }

      const images = [{ buffer: ref.buffer, mime: ref.mime, filename: filenameForMime("reference", ref.mime) }];

      if (charm) {
        images.push({ buffer: charm.buffer, mime: charm.mime, filename: filenameForMime("charm_macro", charm.mime) });
      }

      outBuf = await callGeminiImagesEdits({
        apiKey,
        model,
        prompt,
        size,
        quality,
        output_format,
        images,
      });
    }

    outBuf = await applyFinalFrameZoomIfNeeded(outBuf, postprocess);

    await firestoreRetry(
      () =>
        jobRef.set(
          {
            stage: "uploading",
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        ),
      "jobRef.set"
    );

    const { storagePath, downloadURL, effectiveRunId, effectiveSlot } =
      await uploadPngBufferToSetPath(outBuf, output_base_path, slotIndex, jobId, runId);

    await firestoreRetry(() => db.collection(IMAGES_COLL).add({
      runId: effectiveRunId,
      slotIndex: effectiveSlot,
      createdAt: new Date(),
      storagePath,
      downloadURL,
      model,
      prompt,
      traits: body.traits || null,
      jobId,
      kind,
      activeCategory: activeCategory || null,
      outputBasePath: output_base_path || null,
    }), "images.add");

    await firestoreRetry(() => jobRef.set(
      {
        status: "done",
        stage: "done",
        storagePath,
        downloadURL,
        finishedAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    ), "jobRef.set");

    return json(202, { ok: true, jobId });
  } catch (err) {
    await firestoreRetry(
      () =>
        jobRef.set(
          {
            status: "error",
            stage: "error",
            error: safeErr(err),
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        ),
      "jobRef.set"
    );

    return json(202, { ok: false, jobId, error: safeErr(err) });
  }
};