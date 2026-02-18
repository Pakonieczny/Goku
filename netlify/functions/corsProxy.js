/**
 * corsProxy
 * - Server-side proxy for Firebase Storage download URLs so the browser can use them in <canvas>.
 * - Prevents CORS failures like:
 *   "No 'Access-Control-Allow-Origin' header is present..."
 */

// Netlify Functions runtime can vary; ensure fetch exists.
async function getFetch() {
  if (typeof fetch === "function") return fetch;
  const mod = await import("node-fetch");
  return mod.default;
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
    const contentType = resp.headers.get("content-type") || "application/octet-stream";
    const cacheControl =
      resp.headers.get("cache-control") || "public, max-age=86400";

    const buf = Buffer.from(await resp.arrayBuffer());

    // Safety: avoid returning extremely large base64 payloads that can trip platform limits.
    // If this triggers, the *proper* fix is Firebase bucket CORS (see note below).
    const MAX_BYTES = 4_500_000; // ~4.5MB raw -> ~6MB base64-ish
    if (buf.length > MAX_BYTES) {
      return {
        statusCode: 413,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET,OPTIONS",
          "Access-Control-Allow-Headers": "*",
          "Content-Type": "text/plain",
        },
        body: "Image too large to proxy via Netlify Function. Configure Firebase Storage CORS for your domain instead.",
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
}