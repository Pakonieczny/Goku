/**
 * corsProxy
 * - Server-side proxy for Firebase Storage download URLs so the browser can use them in <canvas>.
 * - Prevents CORS failures like:
 *   "No 'Access-Control-Allow-Origin' header is present..."
 *
 * Update:
 * - Adds optional image conversion to JPEG (NO RESIZE) to reduce payload size and avoid 413.
 * - Uses sharp if available; otherwise falls back to passthrough.
 *
 * Query params:
 *   ?url=<encoded>
 *   &format=jpeg|png|webp   (default: jpeg)
 *   &quality=1..100         (default: 85)
 */

// Netlify Functions runtime can vary; ensure fetch exists.
async function getFetch() {
  if (typeof fetch === "function") return fetch;
  const mod = await import("node-fetch");
  return mod.default;
}

// Best-effort: use sharp if present. If not installed, we fall back to passthrough.
async function getSharp() {
  try {
    const mod = await import("sharp");
    return mod.default || mod;
  } catch {
    return null;
  }
}

exports.handler = async (event) => {
  // Preflight
  if (event.httpMethod === "OPTIONS") {
    return {
      statusCode: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,OPTIONS",
        "Access-Control-Allow-Headers": "*",
      },
      body: "",
    };
  }

  try {
    const url = event.queryStringParameters?.url;
    if (!url) {
      return {
        statusCode: 400,
        headers: { "Access-Control-Allow-Origin": "*" },
        body: "Missing required query param: url",
      };
    }

    // Transform knobs (NO RESIZE)
    const format = String(event.queryStringParameters?.format || "jpeg").toLowerCase();
    const quality = Math.min(100, Math.max(1, Number(event.queryStringParameters?.quality ?? 85)));

    // Basic SSRF guard: only allow known Firebase/Google Storage hosts.
    const u = new URL(url);
    const allowedHosts = new Set([
      "firebasestorage.googleapis.com",
      "storage.googleapis.com",
    ]);
    if (!allowedHosts.has(u.hostname)) {
      return {
        statusCode: 400,
        headers: { "Access-Control-Allow-Origin": "*" },
        body: "Disallowed host",
      };
    }

    const _fetch = await getFetch();

    // Timeout guard so we don't hang and surface as 502
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), 20000); // 20s

    const resp = await _fetch(url, { redirect: "follow", signal: controller.signal });
    clearTimeout(t);

    let contentType = resp.headers.get("content-type") || "application/octet-stream";
    const cacheControl = resp.headers.get("cache-control") || "public, max-age=86400";

    let buf = Buffer.from(await resp.arrayBuffer());

    // If it's an image, try to convert format (NO RESIZE).
    const looksLikeImage = /^image\//i.test(contentType);
    if (looksLikeImage) {
      const sharp = await getSharp();
      if (sharp) {
        try {
          let pipeline = sharp(buf, { failOnError: false }).rotate();

          if (format === "webp") {
            pipeline = pipeline.webp({ quality });
            contentType = "image/webp";
          } else if (format === "png") {
            pipeline = pipeline.png({ compressionLevel: 9 });
            contentType = "image/png";
          } else {
            // Default: JPEG (most effective size drop vs PNG)
            pipeline = pipeline.jpeg({ quality, mozjpeg: true });
            contentType = "image/jpeg";
          }

          buf = await pipeline.toBuffer();
        } catch {
          // Transform failed — fall back to original bytes.
        }
      }
    }

    // Safety: avoid returning extremely large base64 payloads that can trip platform limits.
    // If this triggers, the proper fix is Firebase bucket CORS (so you don't need proxy for canvas).
    const MAX_BYTES = 8_000_000; // raised since JPEG conversion usually shrinks PNG a lot
    if (buf.length > MAX_BYTES) {
      return {
        statusCode: 413,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET,OPTIONS",
          "Access-Control-Allow-Headers": "*",
          "Content-Type": "text/plain",
        },
        body:
          "Image too large to proxy via Netlify Function. Try proxy params (format=jpeg&quality=80) or configure Firebase Storage CORS for your domain.",
      };
    }

    return {
      statusCode: resp.status,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,OPTIONS",
        "Access-Control-Allow-Headers": "*",
        "Content-Type": contentType,
        // Keep caching reasonable; these Firebase token URLs are already effectively versioned.
        "Cache-Control": cacheControl,
      },
      body: buf.toString("base64"),
      isBase64Encoded: true,
    };
  } catch (err) {
    return {
      statusCode: 500,
      headers: { "Access-Control-Allow-Origin": "*" },
      body: "Proxy error: " + (err?.message || String(err)),
    };
  }
};