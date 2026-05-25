/* netlify/functions/_etsyMailScheduled.js
 * Shared scheduled-function detector. Supports Netlify's modern
 * x-nf-event-source header, older x-netlify-event header, and local/manual
 * scheduled body markers used by prior EtsyMail cron functions.
 */
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

module.exports = { isScheduledInvocation };
