/* netlify/functions/corsProxy.js */
const https = require('https');
const url = require('url');

exports.handler = async (event, context) => {
  const targetUrl = event.queryStringParameters.url;

  if (!targetUrl) {
    return { statusCode: 400, body: "Missing 'url' parameter" };
  }

  // Helper to handle redirects recursively
  const fetchUrl = (currentUrl, attempts = 0) => {
    return new Promise((resolve) => {
      if (attempts > 5) {
        return resolve({ statusCode: 502, body: "Too many redirects" });
      }

      const req = https.get(currentUrl, (res) => {
        // Handle Redirects
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          return resolve(fetchUrl(res.headers.location, attempts + 1));
        }

        // Handle Errors from Upstream
        if (res.statusCode >= 400) {
          return resolve({ statusCode: res.statusCode, body: `Upstream failed: ${res.statusCode}` });
        }

        const chunks = [];
        res.on('data', (chunk) => chunks.push(chunk));
        res.on('end', () => {
          const buffer = Buffer.concat(chunks);
          resolve({
            statusCode: 200,
            headers: {
              "Access-Control-Allow-Origin": "*",
              "Content-Type": res.headers['content-type'] || 'application/octet-stream',
              "Cache-Control": "public, max-age=86400"
            },
            body: buffer.toString('base64'),
            isBase64Encoded: true
          });
        });
      });

      req.on('error', (e) => {
        console.error("Proxy Error:", e);
        resolve({ statusCode: 502, body: `Proxy error: ${e.message}` });
      });
      
      // Set a strict timeout to prevent Netlify hanging
      req.setTimeout(8000, () => {
        req.destroy();
        resolve({ statusCode: 504, body: "Upstream timeout" });
      });
    });
  };

  return fetchUrl(targetUrl);
};