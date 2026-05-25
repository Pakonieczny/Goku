/*  netlify/functions/etsyMailSeedTokens.js
 *
 *  ONE-TIME SETUP endpoint. Seeds Etsy OAuth tokens into Firestore at
 *  config/etsyOauth so the server-side sync function can use them.
 *
 *  Why this exists:
 *    Your existing OAuth flow (exchangeToken.js + refreshEtsyToken.js)
 *    stores tokens CLIENT-SIDE. My server-side sync function has no way
 *    to get them. This endpoint accepts tokens once, stores them in
 *    Firestore, and from then on the sync function auto-refreshes them.
 *
 *  Usage (once, from inbox dev console):
 *    fetch('/.netlify/functions/etsyMailSeedTokens', {
 *      method: 'POST',
 *      headers: {
 *        'Content-Type': 'application/json',
 *        'X-EtsyMail-Secret': '<YOUR_SECRET_OR_OMIT_IF_NOT_SET>'
 *      },
 *      body: JSON.stringify({
 *        access_token: 'ABC123...',
 *        refresh_token: 'DEF456...',
 *        expires_in: 3600         // seconds, optional but recommended
 *      })
 *    }).then(r => r.json()).then(console.log);
 *
 *  Response: { ok: true, expires_at: <ms>, path: 'config/etsyOauth' }
 *
 *  After seeding, the sync function will auto-refresh tokens when they're
 *  within 2 min of expiring. As long as refresh succeeds (Etsy rotates the
 *  refresh token on each use), tokens stay fresh forever.
 *
 *  If refresh ever fails (e.g., user revoked app access), re-seed using
 *  this endpoint.
 */

const admin = require("./firebaseAdmin");
const { requireExtensionAuth, CORS } = require("./_etsyMailAuth");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const DOC_PATH = "config/etsyOauth";  // 2 segments: collection "config", doc "etsyOauth"

function json(statusCode, body) {
  return {
    statusCode,
    headers: { ...CORS, "Content-Type": "application/json" },
    body: JSON.stringify(body)
  };
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };
  if (event.httpMethod !== "POST")     return json(405, { error: "Method Not Allowed" });

  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return json(400, { error: "Invalid JSON body" }); }

  const { access_token, refresh_token, expires_in } = body;

  if (!access_token || typeof access_token !== "string") {
    return json(400, { error: "Missing or invalid access_token (string required)" });
  }
  if (!refresh_token || typeof refresh_token !== "string") {
    return json(400, { error: "Missing or invalid refresh_token (string required)" });
  }

  // Compute expiry. If not provided, default to "already-expired" so the
  // sync function force-refreshes on first call (safer than assuming a
  // long expiry).
  const expiresInSec = (typeof expires_in === "number" && expires_in > 0) ? expires_in : 0;
  const expires_at = expiresInSec > 0
    ? Date.now() + Math.max(0, (expiresInSec - 120)) * 1000
    : 0;

  try {
    await db.doc(DOC_PATH).set({
      access_token,
      refresh_token,
      expires_at,
      seededAt : FV.serverTimestamp(),
      seededBy : "etsyMailSeedTokens"
    }, { merge: true });

    return json(200, {
      ok: true,
      path: DOC_PATH,
      expires_at,
      expires_at_iso: expires_at ? new Date(expires_at).toISOString() : null,
      willRefreshOnFirstUse: expires_at === 0,
      nextStep: "OAuth seeded. The inbox will populate customer order data automatically as customers message in (snapshot pipeline triggers per-buyer sync). No manual backfill required — the cron-driven full/incremental sync paths were removed 2026-05-21."
    });

  } catch (err) {
    console.error("etsyMailSeedTokens error:", err);
    return json(500, { error: err.message || "Unknown error" });
  }
};
