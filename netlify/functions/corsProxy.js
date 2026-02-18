/* netlify/functions/corsProxy.js */
const http = require("http");
const https = require("https");
const { URL } = require("url");

// Best-effort: run bucket CORS setup (no-op if env vars/permissions aren’t present).
try { require("./firebaseAdmin"); } catch (e) { /* ignore */ }

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,HEAD,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Access-Control-Max-Age": "86400"
};

function isAllowedHost(hostname) {
  const h = String(hostname || "").toLowerCase();
  return (
    h === "firebasestorage.googleapis.com" ||
    h.endsWith(".googleapis.com") ||
    h.endsWith(".googleusercontent.com") ||
    h.endsWith(".gstatic.com") ||
    h.endsWith(".appspot.com")
  );
}

exports.handler = async (event, context) => {
 if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: CORS_HEADERS, body: "" };
  }

 const targetUrl = event.queryStringParameters?.url;

  if (!targetUrl) {
    return { statusCode: 400, headers: CORS_HEADERS, body: "Missing 'url' parameter" };
  }

  // Helper to handle redirects recursively
  const fetchUrl = (currentUrl, attempts = 0) => {
    return new Promise((resolve) => {
      if (attempts > 5) {
        return resolve({ statusCode: 502, headers: CORS_HEADERS, body: "Too many redirects" });
      }

         let parsed;
      try { parsed = new URL(currentUrl); }
      catch { return resolve({ statusCode: 400, headers: CORS_HEADERS, body: "Invalid URL" }); }

      if (!isAllowedHost(parsed.hostname)) {
        return resolve({ statusCode: 400, headers: CORS_HEADERS, body: "Host not allowed" });
      }

      const lib = parsed.protocol === "http:" ? http : https;

      const req = lib.get(parsed.toString(), (res) => {
        // Handle Redirects
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          const nextUrl = new URL(res.headers.location, parsed).toString();
          return resolve(fetchUrl(nextUrl, attempts + 1));
        }

        // Handle Errors from Upstream
        if (res.statusCode >= 400) {
          return resolve({ statusCode: res.statusCode, headers: CORS_HEADERS, body: `Upstream failed: ${res.statusCode}` });
        }

        const chunks = [];
        res.on('data', (chunk) => chunks.push(chunk));
        res.on('end', () => {
          const buffer = Buffer.concat(chunks);
          resolve({
            statusCode: 200,
            headers: {
              ...CORS_HEADERS,
              "Content-Type": res.headers['content-type'] || 'application/octet-stream',
              "Cache-Control": "public, max-age=86400",
              "Vary": "Origin"
            },
            body: buffer.toString('base64'),
            isBase64Encoded: true
          });
        });
      });

      req.on('error', (e) => {
        console.error("Proxy Error:", e);
        resolve({ statusCode: 502, headers: CORS_HEADERS, body: `Proxy error: ${e.message}` });
      });
      
      // Set a strict timeout to prevent Netlify hanging
      req.setTimeout(8000, () => {
        req.destroy();
        resolve({ statusCode: 504, headers: CORS_HEADERS, body: "Upstream timeout" });
      });
    });
  };

  return fetchUrl(targetUrl);
};