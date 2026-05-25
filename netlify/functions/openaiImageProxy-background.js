/* netlify/functions/openaiImageProxy-background.js
   Background Function: runs long Gemini image generation/edits without browser/edge inactivity 504s.
   Writes realtime status to Firestore + uploads final PNG to Firebase Storage.
*/

const admin = require("./firebaseAdmin");
const sharp = require("sharp");

// Node 18 on Netlify provides fetch/FormData/Blob globally.
// If your build ever lacks fetch, uncomment:
// const fetch = require("node-fetch");

const JOBS_COLL = "ListingGenerator1Jobs";
const IMAGES_COLL = "ListingGenerator1Images";

// Hard-lock the Gemini image model (ignore any client-provided model)
const GEMINI_IMAGE_MODEL = "gemini-3-pro-image-preview";

// ---- helpers ----
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
  // Must match the prefixes your pipeline uses (reference, charm macro, pass-A outputs).
  const ALLOWED_INPUT_PREFIXES = [
    "listing-generator-1/reference/",
    "listing-generator-1/charm-macro/",
    "listing-generator-1/Beady Necklace/Slot_",
    "listing-generator-1/generated/",
  ];

  if (!ALLOWED_INPUT_PREFIXES.some((prefix) => p.startsWith(prefix))) {
    throw new Error("input_storage_path not allowed");
  }

  const bucket = admin.storage().bucket();
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

// Deterministic final framing: crop -> resize back to original size.
// This is applied ONLY to the OUTPUT buffer, never to the reference input.
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
  images, // [{ buffer, mime, filename }]
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
  // Common cases you use
  if (w === 2048 && h === 2048) return "1:1";
  if (w === 2048 && h === 2048) return "1:1";
  // Fallback: reduce ratio
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
  images, // [{ buffer, mime, filename }]
}) {
  const geminiModel =
    String(model || "gemini-3-pro-image-preview").trim() ||
    "gemini-3-pro-image-preview";
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${geminiModel}:generateContent`;

  // Strong output spec: Gemini image models can still vary; this nudges consistent dimensions/format.
  const m = /^(\\d+)\\s*x\\s*(\\d+)$/.exec(String(size || "").trim());
  const wantW = m ? Number(m[1]) : null;
  const wantH = m ? Number(m[2]) : null;
  const wantAR = sizeToAspectRatio(size);

  const promptText =
    `${String(prompt || "").trim()}\\n\\n` +
    `OUTPUT (NON-NEGOTIABLE): Return a photorealistic ${wantAR} image. ` +
    (wantW && wantH ? `Exact size ${wantW}x${wantH}. ` : "") +
    `Return an image suitable for a product photo.`;

  // NOTE: For multimodal editing, parts can include multiple inline_data images.
  // We keep text first (instruction) then images (conditioning inputs).
  const parts = [{ text: promptText }];
  for (const img of images || []) {
    parts.push({
      inline_data: {
        mime_type: img?.mime || "image/png",
        data: Buffer.from(img?.buffer || Buffer.alloc(0)).toString("base64"),
      },
    });
  }

  const body = stripUndefined({
    contents: [{ role: "user", parts }],
    generationConfig: {
      // Gemini image models typically return both; request both to avoid empty responses.
      responseModalities: ["TEXT", "IMAGE"],
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

  // Normalize to PNG + requested size (defensive for downstream pipeline)
  try {
    const img = sharp(outBuf);
    const meta = await img.metadata();
    const needResize =
      wantW && wantH && (meta?.width !== wantW || meta?.height !== wantH);
    if (needResize) {
      outBuf = await img.resize(wantW, wantH, { fit: "cover" }).png().toBuffer();
    } else {
      outBuf = await img.png().toBuffer();
    }
  } catch (_) {
    // If sharp fails, still return raw bytes.
  }

  return outBuf;}

async function uploadPngBufferToStorage({ outBuf, jobId, runId, slotIndex }) {
  const bucket = admin.storage().bucket(); // uses storageBucket from firebaseAdmin.js init
  const token = globalThis.crypto?.randomUUID
    ? globalThis.crypto.randomUUID()
    : require("crypto").randomUUID();

  const effectiveRunId = runId || `lg1_${Date.now()}`;
  const effectiveSlot = typeof slotIndex === "number" ? slotIndex : null;

  const storagePath = `listing-generator-1/generated/${effectiveRunId}/${jobId}.png`;
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
  const encoded = encodeURIComponent(storagePath).replace(/%2F/g, "%2F");
  const downloadURL = `https://firebasestorage.googleapis.com/v0/b/${bucketName}/o/${encoded}?alt=media&token=${token}`;

  return { storagePath, downloadURL, effectiveRunId, effectiveSlot };
}

/**
 * Postprocess pipeline:
 * - Given passA (with oversized charm),
 * - inpaint-remove charm to recover base pixels,
 * - compute diff mask to isolate charm pixels from passA,
 * - scale charm crop down with Lanczos,
 * - add subtle contact shadow,
 * - composite onto recovered base.
 */
async function postScaleCharmComposite({
  passABuf,
  baseNoCharmBuf,
  scale,
  targetPx,          // NEW: preferred final charm height in pixels (stable slider behavior)
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

  // Decode raw RGBA for both images
  const aRaw = await sharp(passABuf).ensureAlpha().raw().toBuffer();
  const bRaw = await sharp(baseNoCharmBuf).ensureAlpha().raw().toBuffer();

  // Build per-pixel diff map (max RGB channel delta)
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

    // Feather + tighten after blur to reduce speckle
    const maskRaw = await sharp(mask, { raw: { width, height, channels: 1 } })
      .blur(feather)
      .threshold(18)
      .raw()
      .toBuffer();

    let minX = width,
      minY = height,
      maxX = -1,
      maxY = -1;
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
    const density = found && bboxArea > 0 ? count / bboxArea : 0; // sparse speckle guard
    return { found, maskRaw, minX, minY, maxX, maxY, count, bboxArea };
  }

  // Adaptive thresholding: raise threshold until bbox is plausibly “just the charm”
  const baseThr = clampNumber(diffThreshold, 8, 120, 40);
  const feather = 1; // keep tight; we're only trying to reduce speckle, not grow the region

  const totalPx = width * height;
  // Tighten these so we never "accidentally select half the shirt/skin"
  const MAX_MASK_PX_RATIO = 0.035;     // 3.5%
  const MAX_BBOX_AREA_RATIO = 0.075;   // 7.5%
  const MAX_BBOX_W_RATIO = 0.35;       // 35% of width
  const MAX_BBOX_H_RATIO = 0.35;       // 35% of height
  const MIN_DENSITY = 0.035;           // reject sparse speckle bboxes
  const CENTER_X_MIN = 0.12, CENTER_X_MAX = 0.88; // charm should be roughly central
  const CENTER_Y_MIN = 0.12, CENTER_Y_MAX = 0.88;

  let chosen = null;
  let best = null; // smallest bbox fallback

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

  // Hard safety: if bbox is still huge, skip postscale to avoid picture-in-picture.
  if (chosen.bboxArea > totalPx * 0.25) {
    console.log("[postscale] bbox too large; skipping charm_postscale", {
      bboxArea: chosen.bboxArea,
      totalPx,
      thr: chosen.thr,
    });
    return passABuf;
  }

  let { maskRaw, minX, minY, maxX, maxY } = chosen;

  // Refine bbox to the dominant connected component (stabilizes size when the diff-mask catches
  // tiny speckles on skin/necklace that would otherwise inflate the bbox).
  // Runs on a 4× downsampled binary mask to stay fast.
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

        // BFS
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

          // 4-neighborhood
          const n1 = cx > 0 ? (cy * smallW + (cx - 1)) : -1;
          const n2 = cx + 1 < smallW ? (cy * smallW + (cx + 1)) : -1;
          const n3 = cy > 0 ? ((cy - 1) * smallW + cx) : -1;
          const n4 = cy + 1 < smallH ? ((cy + 1) * smallW + cx) : -1;

          if (n1 >= 0 && !visited[n1] && small[n1]) { visited[n1] = 1; qx[tail] = cx - 1; qy[tail] = cy; tail++; }
          if (n2 >= 0 && !visited[n2] && small[n2]) { visited[n2] = 1; qx[tail] = cx + 1; qy[tail] = cy; tail++; }
          if (n3 >= 0 && !visited[n3] && small[n3]) { visited[n3] = 1; qx[tail] = cx; qy[tail] = cy - 1; tail++; }
          if (n4 >= 0 && !visited[n4] && small[n4]) { visited[n4] = 1; qx[tail] = cx; qy[tail] = cy + 1; tail++; }
        }

        // Ignore tiny speckle components (noise)
        if (area < 12) continue;

        if (area > bestArea) {
          bestArea = area;
          best = { mnx, mny, mxx, mxy };
        }
      }
    }

    if (best) {
      // Scale bbox back up to full-res coordinates
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

  // Expand bbox slightly (jump ring + edge pixels)
  const pad = 6;
  const left = Math.max(0, minX - pad);
  const top = Math.max(0, minY - pad);
  const bboxW = Math.min(width - left, maxX - minX + 1 + pad * 2);
  const bboxH = Math.min(height - top, maxY - minY + 1 + pad * 2);

  // Make a soft alpha crop for cleaner edges
  const maskPng = await sharp(maskRaw, { raw: { width, height, channels: 1 } })
    .extract({ left, top, width: bboxW, height: bboxH })
    .blur(0.8)
    .png()
    .toBuffer();

  // Extract charm crop from passA; set alpha from diff-mask crop
  const charmCrop = await sharp(passABuf)
    .extract({ left, top, width: bboxW, height: bboxH })
    .removeAlpha()
    .joinChannel(maskPng)
    .png()
    .toBuffer();

  // -------------------------
  // OUTPUT SIZE (NEW)
  // Prefer an absolute pixel target for height (stable + proportional slider behavior).
  // Falls back to old scale-multiplier behavior if targetPx isn't provided.
  // -------------------------
  const tp = Number(targetPx);
  let outW, outH;
  if (Number.isFinite(tp)) {
    // Clamp to a sane range. (UI may go very small; server allows it, but extremely small
    // values will naturally lose engraving readability.)
    const targetH = Math.round(clampNumber(tp, 4, 96, 14));
    const aspect = bboxH > 0 ? (bboxW / bboxH) : 1;
    outH = Math.max(1, targetH);
    outW = Math.max(1, Math.round(outH * aspect));

    // Safety: never exceed canvas
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
    // Old behavior (kept for compatibility)
    const s = clampNumber(scale, 0.50, 0.70, 0.65);
    outW = Math.max(1, Math.round(bboxW * s));
    outH = Math.max(1, Math.round(bboxH * s));
  }

  // Downscale charm crop (high-quality resampling)
  const scaledCharm = await sharp(charmCrop)
    .resize(outW, outH, { kernel: "lanczos3" })
    // Mild sharpen helps micro-engraving survive aggressive downscales.
    .sharpen(0.6)
    .png()
    .toBuffer();

  // If alpha somehow collapses, never try to composite (prevents black blocks)
  try {
    const aStats = await sharp(scaledCharm).extractChannel(3).stats();
    if (!aStats?.channels?.[0] || aStats.channels[0].max === 0) return passABuf;
  } catch (_) {
    // if stats fails, fall back safely
    return passABuf;
  }

  // Contact shadow from the scaled alpha channel
  const shBlur = clampNumber(shadowBlur, 0, 12, 2);
  const shOp = clampNumber(shadowOpacity, 0, 0.6, 0.28);

  // Build shadow as RGBA with a real alpha channel (prevents occasional "solid black rectangle" artifacts)
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

  // Anchor: keep top-center-ish of original bbox fixed so jump ring stays on chain.
  // (Center anchor is robust; if you want tighter ring-lock later, we can move anchor up.)
  const anchorX = left + Math.round(bboxW / 2);
  const newLeft = Math.max(0, Math.min(width - outW, Math.round(anchorX - outW / 2)));
  const newTop = Math.max(0, Math.min(height - outH, top));

  // Composite: recovered base -> shadow -> charm
  const finalBuf = await sharp(baseNoCharmBuf)
    .composite([
      { input: shadowLayer, left: newLeft, top: Math.min(height - outH, newTop + 1), blend: "multiply" },
      { input: scaledCharm, left: newLeft, top: newTop, blend: "over" },
    ])
    .png()
    .toBuffer();

  return finalBuf;
}

// ---- handler ----
exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return json(200, { ok: true });
  if (event.httpMethod !== "POST") return json(405, { error: { message: "Method not allowed" } });

  const body = parseJsonBody(event);
  if (!body) return json(400, { error: { message: "Invalid JSON body" } });

  const {
    jobId,
    runId,
    slotIndex,
    kind = "edits", // "edits" | "generations" | "charm_postscale"
    model: _clientModel,
    prompt,
    size = "2048x2048",
    quality = "high",
    output_format = "png",

    // Reference image input (either already in storage or inline base64)
    input_storage_path,
    input_image,

    // Charm macro optional second image (for normal edits flow)
    input_charm_storage_path,
    input_charm_image,

    // For charm_postscale:
    remove_prompt,
    postprocess,

   // Optional: explicit "no-charm base" input for charm_postscale
   // (preferred over inpainting removal because it's deterministic)
   base_storage_path,
   base_image,

  } = body || {};

  if (!jobId) return json(400, { error: { message: "jobId is required" } });

  const db = admin.firestore();
  const jobRef = db.collection(JOBS_COLL).doc(jobId);

  try {
    await jobRef.set(
      {
        status: "running",
        stage: "starting",
        runId: runId || null,
        slotIndex: typeof slotIndex === "number" ? slotIndex : null,
        kind,
        model,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) throw new Error("Missing GEMINI_API_KEY env var");

    // -------------------------
    // SPECIAL: charm_postscale
    // -------------------------
    if (kind === "charm_postscale") {
      // input_storage_path must point to Pass A output (oversized charm)
      if (!input_storage_path && !input_image) {
        throw new Error("charm_postscale requires input_storage_path or input_image (Pass A output)");
      }

      await jobRef.set(
        { stage: "downloading_inputs", updatedAt: admin.firestore.FieldValue.serverTimestamp() },
        { merge: true }
      );

      const passA = input_storage_path
        ? await storagePathToBuffer(input_storage_path)
        : dataUrlToBuffer(input_image);

      // Step 1: obtain a "no-charm base" with identical framing.
      // Prefer an explicit base (original Image[0]) to avoid inpaint drift that breaks diff-masking.
      await jobRef.set(
        { stage: "removing_charm", updatedAt: admin.firestore.FieldValue.serverTimestamp() },
        { merge: true }
      );

      let rp = String(remove_prompt || "").trim();
      let baseNoCharmBuf;

      if (base_storage_path || base_image) {
        const base = base_storage_path
          ? await storagePathToBuffer(base_storage_path)
          : dataUrlToBuffer(base_image);

        // Ensure dimensions match Pass A (defensive, but should already match in your pipeline)
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
        // Fallback (older behavior): inpaint-remove charm
        rp =
          rp ||
          "Remove the pendant charm + jump ring completely and reconstruct the satellite chain and skin behind it. Keep EVERYTHING else identical. Do not change framing, color grade, wardrobe, lighting, face, pose. Only remove the pendant and restore the pixels behind it.";

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

      // Step 2: postprocess scale (no re-generation of engraving)
      await jobRef.set(
        { stage: "postprocessing", updatedAt: admin.firestore.FieldValue.serverTimestamp() },
        { merge: true }
      );

      let finalBuf = await postScaleCharmComposite({
        passABuf: passA.buffer,
        baseNoCharmBuf,
        // NEW: stable sizing control (preferred)
        targetPx: postprocess?.targetPx,
        // OLD: multiplier control (fallback)
        scale: postprocess?.scale,
        shadowOpacity: postprocess?.shadowOpacity,
        shadowBlur: postprocess?.shadowBlur,
        diffThreshold: postprocess?.diffThreshold,
      });

      // Final deterministic framing (OUTPUT only)
      finalBuf = await applyFinalFrameZoomIfNeeded(finalBuf, postprocess);

      await jobRef.set(
        { stage: "uploading", updatedAt: admin.firestore.FieldValue.serverTimestamp() },
        { merge: true }
      );

      const { storagePath, downloadURL, effectiveRunId, effectiveSlot } =
        await uploadPngBufferToStorage({ outBuf: finalBuf, jobId, runId, slotIndex });

      await db.collection(IMAGES_COLL).add({
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
      });

      await jobRef.set(
        {
          status: "done",
          stage: "done",
          storagePath,
          downloadURL,
          finishedAt: admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

      return json(202, { ok: true, jobId });
    }

    // -------------------------
    // DEFAULT: edits / generations behavior (preserved)
    // -------------------------
    if (!prompt) return json(400, { error: { message: "prompt is required" } });

    if (kind !== "edits" && kind !== "generations") {
      return json(400, { error: { message: "kind must be 'edits', 'generations', or 'charm_postscale'" } });
    }

    await jobRef.set(
      {
        stage: "calling_gemini"
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    let outBuf;

    if (kind === "generations") {
      // Correct JSON path for generations
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

      // Reference image (Image[0])
      const ref = input_storage_path
        ? await storagePathToBuffer(input_storage_path)
        : dataUrlToBuffer(input_image);

      // Charm macro (Image[1]) — optional second image
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

    await jobRef.set(
      {
        stage: "uploading",
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

       // Final deterministic framing (OUTPUT only)
      outBuf = await applyFinalFrameZoomIfNeeded(outBuf, postprocess);

      const { storagePath, downloadURL, effectiveRunId, effectiveSlot } =
      await uploadPngBufferToStorage({ outBuf, jobId, runId, slotIndex });

    await db.collection(IMAGES_COLL).add({
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
    });

    await jobRef.set(
      {
        status: "done",
        stage: "done",
        storagePath,
        downloadURL,
        finishedAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    return json(202, { ok: true, jobId });
  } catch (err) {
    await jobRef.set(
      {
        status: "error",
        stage: "error",
        error: safeErr(err),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    // Still 202 so the browser doesn’t treat the request as “failed to enqueue”
    return json(202, { ok: false, jobId, error: safeErr(err) });
  }
};