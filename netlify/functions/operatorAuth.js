/* =============================================================================
 *  operatorAuth.js  —  passcode verification for the Listing Generator console
 * =============================================================================
 *  The only job of this function is to answer one question: is this passcode
 *  the operator passcode for this site? It holds no other logic, requires no
 *  other module, and touches no third-party API — so it cannot be broken by a
 *  dependency that lives on another Netlify project. That is the whole point:
 *  the previous gate leaned on googleAdsAutopilotKick.js, which requires the
 *  googleAdsAutopilot engine, and that engine only exists on goldenspike.
 *
 *  Contract (identical to the ads console, so one shared passcode still works):
 *      POST { passcode }  or  header  X-Edit-Passcode
 *        200 → { ok: true }        accepted
 *        401 → { error }           wrong passcode
 *
 *  Set EDIT_PASSCODE in this site's Netlify environment variables.
 *  Site settings → Environment variables → EDIT_PASSCODE.
 * ========================================================================== */

const HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, X-Edit-Passcode",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Content-Type": "application/json"
};

/*  Constant-time compare. A plain === leaks the length of the matching prefix
 *  through response timing. The window is tiny over HTTP and this is an
 *  internal tool, but a passcode check is exactly the wrong place to be
 *  casual, and the fix costs four lines. */
function safeEqual(a, b) {
  a = String(a || "");
  b = String(b || "");
  let diff = a.length ^ b.length;
  const n = Math.max(a.length, b.length);
  for (let i = 0; i < n; i++) diff |= a.charCodeAt(i % (a.length || 1)) ^ b.charCodeAt(i % (b.length || 1));
  return diff === 0;
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: HEADERS, body: "" };
  if (event.httpMethod !== "POST") {
    return { statusCode: 405, headers: HEADERS, body: JSON.stringify({ error: "POST only" }) };
  }

  const expected = process.env.EDIT_PASSCODE || "";

  /*  Unset passcode = open console, matching googleAdsAutopilotKick's
   *  behaviour so the two consoles never disagree about what "unset" means.
   *  `open:true` is echoed back so the sign-in screen can say so out loud
   *  rather than pretending a real check happened. */
  if (!expected) return { statusCode: 200, headers: HEADERS, body: JSON.stringify({ ok: true, open: true }) };

  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch (e) {}

  const h = (event.headers && (event.headers["x-edit-passcode"] || event.headers["X-Edit-Passcode"])) || "";
  const supplied = h || body.passcode || "";

  if (!safeEqual(supplied, expected)) {
    return { statusCode: 401, headers: HEADERS, body: JSON.stringify({ error: "unauthorized" }) };
  }
  return { statusCode: 200, headers: HEADERS, body: JSON.stringify({ ok: true }) };
};
