/* netlify/functions/etsyMailTrackingDebugSvg.js
 *
 * Diagnostic endpoint — returns the raw SVG (as text/xml) that the renderer
 * would normally rasterize to PNG. Useful for debugging font/rendering
 * issues: if the SVG renders correctly in a browser but resvg produces
 * empty output, the issue is font matching. If the SVG itself is wrong,
 * the issue is in our build logic.
 *
 * Usage:
 *   GET /.netlify/functions/etsyMailTrackingDebugSvg?trackingCode=<code>
 *     → Returns the SVG as text/xml, same-origin so COEP is fine.
 *     → Returns the SVG that would be rendered for this code, with text
 *       intact. Open directly in browser to see what resvg is given.
 */

const admin     = require("./firebaseAdmin");
const carriers  = require("./_etsyMailCarriersRouter");
const renderer  = require("./_etsyMailTrackingRender");

exports.handler = async (event) => {
  if (event.httpMethod !== "GET") {
    return { statusCode: 405, body: "Method not allowed" };
  }

  const q = event.queryStringParameters || {};
  const trackingCode = String(q.trackingCode || q.code || "").trim();
  if (!trackingCode) {
    return { statusCode: 400, body: "Missing trackingCode" };
  }

  try {
    // Look up the tracking via the same path as the real renderer
    const tracking = await carriers.lookup(trackingCode);

    // buildSvg returns { svg, width, height } — we just want the SVG string
    const { svg } = renderer.buildSvg(tracking);

    return {
      statusCode: 200,
      headers: {
        "Content-Type": "image/svg+xml",
        "Cross-Origin-Resource-Policy": "cross-origin",
        "Cache-Control": "no-store"
      },
      body: svg
    };
  } catch (e) {
    return {
      statusCode: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: e.message, code: e.code, stack: e.stack })
    };
  }
};
