/**
 * corsProxy
 * - Server-side proxy for Firebase Storage download URLs so the browser can use them in <canvas>.
 * - Prevents CORS failures like:
 *   "No 'Access-Control-Allow-Origin' header is present..."
 */

export async function handler(event) {
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

    const resp = await fetch(url, { redirect: "follow" });
    const contentType = resp.headers.get("content-type") || "application/octet-stream";
    const cacheControl =
      resp.headers.get("cache-control") || "public, max-age=86400";

    const buf = Buffer.from(await resp.arrayBuffer());

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