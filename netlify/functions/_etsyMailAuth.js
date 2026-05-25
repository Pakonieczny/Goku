/*  netlify/functions/_etsyMailAuth.js
 *
 *  Shared request helper for every EtsyMail function. Two responsibilities:
 *
 *  1. requireExtensionAuth(event) — validates the X-EtsyMail-Secret header
 *     used by the Chrome extension, the inbox UI, and inter-function calls.
 *  2. isScheduledInvocation(event) — detects whether the current request
 *     is a Netlify cron invocation (so cron functions can bypass the
 *     extension-secret check; the scheduler is the authority). Folded in
 *     from the former _etsyMailScheduled.js (v2.4 consolidation).
 *
 *  Netlify env var to set:
 *    ETSYMAIL_EXTENSION_SECRET = <a long random string you generate>
 *
 *  If the env var is unset, requireExtensionAuth returns { ok: true } so
 *  local dev doesn't block — except in CONTEXT=production, where it
 *  fails closed.
 */

const CORS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Headers": "Content-Type,Authorization,X-EtsyMail-Secret",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
};

function requireExtensionAuth(event) {
  const expected = process.env.ETSYMAIL_EXTENSION_SECRET;

  // v1.5: fail-closed in production. Pre-v1.5, if the env var was unset
  // we'd allow the request through (dev-friendly, production-dangerous).
  // Production deploys must have the secret configured; the helper
  // refuses if it's missing instead of silently passing through.
  //
  // The "production" detection uses Netlify's CONTEXT env var:
  //   - "production"      → main branch, customer-facing
  //   - "deploy-preview"  → PR previews
  //   - "branch-deploy"   → other branches
  //   - "dev" (or unset)  → local netlify dev / netlify functions:serve
  // We only fail-closed for "production". Deploy previews and dev still
  // pass through with a warning — useful for testing and demos.
  if (!expected) {
    if (process.env.CONTEXT === "production") {
      console.error("✗ ETSYMAIL_EXTENSION_SECRET not set in production — refusing request.");
      return {
        ok: false,
        response: {
          statusCode: 500,
          headers: CORS,
          body: JSON.stringify({
            error: "Server misconfigured: ETSYMAIL_EXTENSION_SECRET is required in production",
            errorCode: "AUTH_NOT_CONFIGURED"
          })
        }
      };
    }
    console.warn("⚠ ETSYMAIL_EXTENSION_SECRET not set — allowing request without auth (CONTEXT=" +
      (process.env.CONTEXT || "unknown") + "). MUST set this before promoting to production.");
    return { ok: true };
  }

  // Headers are lowercased by Netlify — check both cases to be safe.
  const got =
    event.headers["x-etsymail-secret"] ||
    event.headers["X-EtsyMail-Secret"] ||
    (event.multiValueHeaders && event.multiValueHeaders["x-etsymail-secret"] && event.multiValueHeaders["x-etsymail-secret"][0]);

  if (!got) {
    return { ok: false, response: { statusCode: 401, headers: CORS, body: JSON.stringify({ error: "Missing X-EtsyMail-Secret header" }) } };
  }
  if (got !== expected) {
    return { ok: false, response: { statusCode: 403, headers: CORS, body: JSON.stringify({ error: "Invalid X-EtsyMail-Secret" }) } };
  }
  return { ok: true };
}

// ─── Scheduled-invocation detection ────────────────────────────────────
// Folded in from the former _etsyMailScheduled.js helper. Used by every
// cron function (etsyMailReapers, etsyMailListingsCatalog, etsyMail-
// ShippingSync) to skip the extension-secret check when Netlify's
// scheduler is the caller. Supports the modern `x-nf-event-source`
// header, the older `x-netlify-event` header, and a local/manual
// `{ _scheduled: true }` JSON body marker for testing.
function isScheduledInvocation(event = {}) {
  const rawHeaders = event.headers || {};
  const headers = {};
  for (const [k, v] of Object.entries(rawHeaders)) headers[String(k).toLowerCase()] = v;

  if (headers["x-nf-event-source"] === "scheduled") return true;
  if (headers["x-netlify-event"] === "schedule") return true;

  const body = typeof event.body === "string" ? event.body : "";
  if (body.includes("scheduled-event")) return true;
  if (body) {
    try {
      const parsed = JSON.parse(body);
      if (parsed && parsed._scheduled === true) return true;
    } catch {}
  }
  return false;
}

module.exports = { requireExtensionAuth, CORS, isScheduledInvocation };
