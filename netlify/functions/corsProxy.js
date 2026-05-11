/**
 * corsProxy — chunked, lossless passthrough
 *
 * Server-side proxy for Firebase Storage download URLs so the browser can use
 * them in <img>, <canvas>, and fetch() despite the bucket not sending CORS
 * headers for this origin.
 *
 * Design:
 *   Netlify Functions cap each response body at ~6 MB (base64-encoded, which
 *   inflates by ~33%, so the practical binary ceiling is ~4.5 MB). Earlier
 *   versions of this function compressed/resized images to fit, which
 *   degraded photo quality. We now transfer the ORIGINAL bytes in chunks
 *   using HTTP Range requests against Firebase upstream (Firebase Storage
 *   honors Range natively). The client assembles the chunks into a Blob and
 *   uses URL.createObjectURL() to get a same-origin URL with the original
 *   bytes intact — bit-for-bit identical to the source. Zero quality loss.
 *
 * Query params:
 *   ?url=<encoded>          (required)  — full Firebase download URL
 *   &chunk=<index>          (optional)  — 0-indexed chunk number, default 0
 *
 * Response (200):
 *   Body: raw bytes (base64-encoded by Netlify)
 *   Headers:
 *     Content-Type: forwarded from upstream
 *     X-Total-Size: total bytes for the whole resource (decimal)
 *     X-Chunk-Index: echo of requested chunk index
 *     X-Chunk-Bytes-Start / X-Chunk-Bytes-End: inclusive byte range returned
 *     X-Is-Last-Chunk: "true" when this chunk completes the resource
 *     Access-Control-Expose-Headers: lists the X-* headers so the browser
 *                                    can read them from a CORS response
 */

// 4 MB binary → ~5.3 MB base64. Comfortable margin under Netlify's ~6 MB cap.
const CHUNK_SIZE = 4_000_000;

async function getFetch() {
  if (typeof fetch === "function") return fetch;
  const mod = await import("node-fetch");
  return mod.default;
}

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,OPTIONS",
  "Access-Control-Allow-Headers": "*",
};

function errorResponse(statusCode, message) {
  return {
    statusCode,
    headers: { ...CORS_HEADERS, "Content-Type": "text/plain" },
    body: String(message),
  };
}

exports.handler = async (event) => {
  // CORS preflight
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: CORS_HEADERS, body: "" };
  }

  const url = event.queryStringParameters?.url;
  if (!url) return errorResponse(400, "Missing required query param: url");

  // SSRF guard — only allow Firebase/Google Storage hosts.
  let u;
  try {
    u = new URL(url);
  } catch {
    return errorResponse(400, "Invalid url");
  }
  const allowedHosts = new Set([
    "firebasestorage.googleapis.com",
    "storage.googleapis.com",
  ]);
  if (!allowedHosts.has(u.hostname)) {
    return errorResponse(400, "Disallowed host");
  }

  // Parse chunk index. Default 0.
  const chunkRaw = event.queryStringParameters?.chunk;
  const chunkIndex = Math.max(0, parseInt(chunkRaw ?? "0", 10) || 0);
  const start = chunkIndex * CHUNK_SIZE;
  const end = start + CHUNK_SIZE - 1; // inclusive

  const _fetch = await getFetch();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 20000); // 20s upstream timeout

  try {
    const resp = await _fetch(url, {
      headers: { Range: `bytes=${start}-${end}` },
      redirect: "follow",
      signal: controller.signal,
    });
    clearTimeout(timer);

    // Firebase returns 206 for honored Range requests, 200 for whole-file
    // responses (e.g. when the requested range covers the entire file).
    if (resp.status !== 200 && resp.status !== 206) {
      const upstreamBody = await resp.text();
      return errorResponse(
        resp.status,
        `Upstream returned ${resp.status}: ${upstreamBody.slice(0, 500)}`
      );
    }

    const buf = Buffer.from(await resp.arrayBuffer());

    // Determine total resource size. With 206, Content-Range looks like:
    //   "bytes 0-3999999/12345678"
    // With 200, Content-Length is the whole file size.
    let totalSize = buf.length;
    const contentRange = resp.headers.get("content-range");
    if (contentRange) {
      const m = /\/(\d+)\s*$/.exec(contentRange);
      if (m) totalSize = parseInt(m[1], 10) || totalSize;
    } else {
      const cl = resp.headers.get("content-length");
      if (cl) totalSize = parseInt(cl, 10) || totalSize;
    }

    const chunkEnd = start + buf.length - 1;
    const isLastChunk = chunkEnd + 1 >= totalSize;

    return {
      statusCode: 200,
      headers: {
        ...CORS_HEADERS,
        "Content-Type": resp.headers.get("content-type") || "application/octet-stream",
        "Cache-Control": resp.headers.get("cache-control") || "public, max-age=86400",
        "X-Total-Size": String(totalSize),
        "X-Chunk-Index": String(chunkIndex),
        "X-Chunk-Bytes-Start": String(start),
        "X-Chunk-Bytes-End": String(chunkEnd),
        "X-Is-Last-Chunk": isLastChunk ? "true" : "false",
        // Without this, browsers can't read the X-* headers from a CORS response.
        "Access-Control-Expose-Headers":
          "X-Total-Size, X-Chunk-Index, X-Chunk-Bytes-Start, X-Chunk-Bytes-End, X-Is-Last-Chunk",
      },
      body: buf.toString("base64"),
      isBase64Encoded: true,
    };
  } catch (err) {
    clearTimeout(timer);
    return errorResponse(500, "Proxy error: " + (err?.message || String(err)));
  }
};
