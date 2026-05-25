/*  netlify/functions/etsyMailGmailSeedTokens.js
 *
 *  ONE-TIME SETUP endpoint. Seeds Gmail OAuth tokens into Firestore at
 *  config/gmailOauth so the server-side Gmail polling function can use them.
 *
 *  Mirrors etsyMailSeedTokens.js exactly — same shape, same pattern, just
 *  pointed at the Gmail OAuth doc and Google's token endpoint.
 *
 *  ═══ HOW TO USE ════════════════════════════════════════════════════════
 *
 *  1. In Google Cloud Console, create an OAuth 2.0 Client ID (Web app or
 *     Desktop app — both work). Note the client_id and client_secret.
 *  2. Set Netlify env vars:
 *       GMAIL_CLIENT_ID     = <your client id>
 *       GMAIL_CLIENT_SECRET = <your client secret>
 *  3. Run the OAuth dance once to get a refresh_token. Easiest method:
 *     Google's OAuth Playground (https://developers.google.com/oauthplayground)
 *
 *     a. Click the gear icon → "Use your own OAuth credentials" → paste
 *        your client id and secret.
 *     b. In Step 1, paste this scope: https://www.googleapis.com/auth/gmail.readonly
 *     c. Click "Authorize APIs" → sign in as the Gmail account whose
 *        inbox you want to poll → grant consent.
 *     d. In Step 2, click "Exchange authorization code for tokens".
 *     e. Copy the access_token, refresh_token, and expires_in.
 *
 *  4. POST those values to this endpoint:
 *
 *       fetch('/.netlify/functions/etsyMailGmailSeedTokens', {
 *         method: 'POST',
 *         headers: {
 *           'Content-Type': 'application/json',
 *           'X-EtsyMail-Secret': '<your secret>'
 *         },
 *         body: JSON.stringify({
 *           access_token : 'ya29.a0...',
 *           refresh_token: '1//0g...',
 *           expires_in   : 3600,
 *           scope        : 'https://www.googleapis.com/auth/gmail.readonly',
 *           emailAddress : 'you@gmail.com'   // optional, for display
 *         })
 *       }).then(r => r.json()).then(console.log);
 *
 *  Response: { ok: true, expires_at: <ms>, path: 'config/gmailOauth' }
 *
 *  After seeding, the polling function will auto-refresh tokens. Google
 *  refresh_tokens generally don't rotate on use (unlike Etsy's), so the
 *  same refresh_token persists for 6+ months unless the user revokes
 *  app access in their Google account settings. If the worker ever logs
 *  "Gmail token refresh failed: 400 invalid_grant", re-seed.
 *
 *  ═══ RELATED ═══════════════════════════════════════════════════════════
 *
 *    _etsyMailGmail.js              — reads tokens from this Firestore doc
 *    etsyMailGmail-background.js    — does the actual polling work
 *    etsyMailGmail.js               — status / manual-trigger endpoint
 *    etsyMailGmailCron.js           — scheduled trigger
 */

const admin = require("./firebaseAdmin");
const { requireExtensionAuth, CORS } = require("./_etsyMailAuth");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const DOC_PATH = "config/gmailOauth";   // 2 segments: collection "config", doc "gmailOauth"

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

  const {
    access_token,
    refresh_token,
    expires_in,
    scope        = null,
    token_type   = "Bearer",
    emailAddress = null
  } = body;

  if (!access_token || typeof access_token !== "string") {
    return json(400, { error: "Missing or invalid access_token (string required)" });
  }
  if (!refresh_token || typeof refresh_token !== "string") {
    return json(400, { error: "Missing or invalid refresh_token (string required)" });
  }

  // Compute expiry. If not provided, default to "already-expired" so the
  // worker force-refreshes on first call (safer than assuming a long expiry).
  // Same pattern as etsyMailSeedTokens.js.
  const expiresInSec = (typeof expires_in === "number" && expires_in > 0) ? expires_in : 0;
  const expires_at = expiresInSec > 0
    ? Date.now() + Math.max(0, (expiresInSec - 120)) * 1000
    : 0;

  try {
    await db.doc(DOC_PATH).set({
      access_token,
      refresh_token,
      expires_at,
      scope,
      token_type,
      emailAddress,                              // for display in status endpoint only
      seededAt : FV.serverTimestamp(),
      seededBy : "etsyMailGmailSeedTokens"
    }, { merge: true });

    return json(200, {
      ok: true,
      path: DOC_PATH,
      expires_at,
      expires_at_iso       : expires_at ? new Date(expires_at).toISOString() : null,
      willRefreshOnFirstUse: expires_at === 0,
      emailAddress,
      nextStep: "Trigger a manual sync via /.netlify/functions/etsyMailGmail?action=trigger (POST with X-EtsyMail-Secret), or wait for the next scheduled cron tick."
    });

  } catch (err) {
    console.error("etsyMailGmailSeedTokens error:", err);
    return json(500, { error: err.message || "Unknown error" });
  }
};
