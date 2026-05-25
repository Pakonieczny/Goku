/*  netlify/functions/etsyMailImage.js
 *
 *  Image proxy for mirrored Etsy message images.
 *
 *  The mirror function (etsyMailMirrorImage) uploads Etsy CDN images to
 *  Firebase Storage at paths like:
 *     etsymail/etsy_conv_12345/etsy_<contentHash>/<imgHash>.jpg
 *
 *  This endpoint fetches those bytes server-side and streams them back to
 *  the browser with Cross-Origin-Resource-Policy: cross-origin so the
 *  response is embeddable from any page (including inbox pages that have
 *  Cross-Origin-Embedder-Policy enabled).
 *
 *  Usage (from the inbox UI):
 *     GET /.netlify/functions/etsyMailImage?path=etsymail/.../hash.jpg
 *
 *  Response: 200 OK with image/* Content-Type and the raw bytes. Browser
 *  renders it directly into an <img> or <a> target.
 *
 *  Previous versions used 302 redirect to a signed GCS URL. That broke on
 *  inbox pages with COEP set because GCS doesn't send
 *  Cross-Origin-Resource-Policy. Proxy-streaming avoids the cross-origin
 *  fetch entirely — browser only sees our Netlify-hosted response.
 *
 *  Security:
 *    - Path must begin with "etsymail/" — prevents arbitrary bucket access
 *    - No auth required; image paths are content-hashed and non-guessable
 *    - Path traversal (../, backslash) rejected
 */

const admin = require("./firebaseAdmin");

const bucket = admin.storage().bucket();

// Browser-level CORS + CORP; images must be embeddable cross-origin,
// including from pages that have COEP: require-corp enabled.
const BASE_HEADERS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Max-Age"      : "86400",
  "Cross-Origin-Resource-Policy": "cross-origin"
};

// v4.3.12 — Allow both the original drafts/messages prefix AND the
// collateral upload prefix. Collateral images live at
// 'etsymail-collateral/...' (note the hyphen), drafts/snapshots at
// 'etsymail/...'. Both are first-party and need to be servable
// through the proxy so the Chrome extension's image-injection step
// can fetch line-sheet attachments the sales agent constructs.
const ALLOWED_PATH_PREFIXES = ["etsymail/", "etsymail-collateral/"];
const PATH_PREFIX = ALLOWED_PATH_PREFIXES[0];   // legacy alias for any below code that still references it

// Infer a Content-Type from a storage path's extension, used as fallback
// if the stored GCS metadata doesn't have a contentType.
function contentTypeFromPath(path) {
  const ext = (path.split(".").pop() || "").toLowerCase();
  switch (ext) {
    case "jpg":
    case "jpeg": return "image/jpeg";
    case "png":  return "image/png";
    case "gif":  return "image/gif";
    case "webp": return "image/webp";
    case "svg":  return "image/svg+xml";
    default:     return "application/octet-stream";
  }
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: BASE_HEADERS, body: "" };
  }
  if (event.httpMethod !== "GET") {
    return {
      statusCode: 405,
      headers: { ...BASE_HEADERS, "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Method Not Allowed" })
    };
  }

  try {
    const qs = event.queryStringParameters || {};
    const rawPath = qs.path || "";

    if (!rawPath) {
      return {
        statusCode: 400,
        headers: { ...BASE_HEADERS, "Content-Type": "application/json" },
        body: JSON.stringify({ error: "Missing 'path' query parameter" })
      };
    }
    if (!ALLOWED_PATH_PREFIXES.some(p => rawPath.startsWith(p))) {
      return {
        statusCode: 403,
        headers: { ...BASE_HEADERS, "Content-Type": "application/json" },
        body: JSON.stringify({ error: "Path must start with 'etsymail/' or 'etsymail-collateral/'" })
      };
    }
    if (rawPath.includes("..") || rawPath.includes("\\")) {
      return {
        statusCode: 400,
        headers: { ...BASE_HEADERS, "Content-Type": "application/json" },
        body: JSON.stringify({ error: "Invalid path" })
      };
    }

    const file = bucket.file(rawPath);

    const [exists] = await file.exists();
    if (!exists) {
      return {
        statusCode: 404,
        headers: { ...BASE_HEADERS, "Content-Type": "application/json" },
        body: JSON.stringify({ error: "Image not found", path: rawPath })
      };
    }

    // Fetch the file contents AND its stored metadata. The stored content-type
    // (set by the mirror function) is more reliable than extension inference.
    const [buf] = await file.download();
    let contentType = null;
    try {
      const [meta] = await file.getMetadata();
      if (meta && meta.contentType) contentType = meta.contentType;
    } catch { /* fall through to extension inference */ }
    if (!contentType) contentType = contentTypeFromPath(rawPath);

    // Return the bytes. Netlify functions expect base64 for binary responses.
    return {
      statusCode: 200,
      headers: {
        ...BASE_HEADERS,
        "Content-Type" : contentType,
        "Content-Length": String(buf.length),
        // Cache aggressively at the browser — image bytes are immutable
        // (content-hashed filenames); if the content changed the URL would
        // change too. One day of browser cache is reasonable.
        "Cache-Control": "public, max-age=86400, immutable"
      },
      body: buf.toString("base64"),
      isBase64Encoded: true
    };

  } catch (err) {
    console.error("etsyMailImage error:", err);
    return {
      statusCode: 500,
      headers: { ...BASE_HEADERS, "Content-Type": "application/json" },
      body: JSON.stringify({ error: err.message || "Unknown error" })
    };
  }
};
