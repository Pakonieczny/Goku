/*  netlify/functions/etsyPricingApplyResult.js
 *
 *  GET ?request_id=...&listing_id=...
 *
 *  The read side of the background pricing channel. Netlify answers a
 *  background function with 202 and NO body, so the browser cannot learn the
 *  pricing outcome from the invocation itself. etsyPricingApplyOne-background
 *  writes its outcome to one Firestore doc keyed by the browser-generated
 *  request_id; this function serves that doc. Index.html polls it every few
 *  seconds after firing the background call.
 *
 *  This is a plain synchronous function ON PURPOSE: it makes exactly one
 *  Firestore read and touches Etsy not at all, so it sits comfortably inside
 *  the 10-second budget that the pricing work itself could not.
 *
 *  Responses:
 *    { pending: true }                      — no result yet, keep polling
 *    { done: true, result: {...} }          — the outcome, in exactly the
 *                                             shape the old synchronous
 *                                             endpoint returned
 *    { pending: true, unavailable: true }   — Firestore is not configured on
 *                                             this site, so results can never
 *                                             arrive; the browser should stop
 *                                             polling and fall back to "check
 *                                             the Pricing Console"
 */

"use strict";

const { readResult } = require("./_pricingResultStore");

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, Accept, access-token, Authorization",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
};
const json = (statusCode, body) => ({
  statusCode,
  headers: { "Content-Type": "application/json", "Cache-Control": "no-store", ...CORS },
  body: JSON.stringify(body),
});

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };
  if (event.httpMethod !== "GET") return json(405, { error: "Method not allowed" });

  const q = event.queryStringParameters || {};
  const requestId = String(q.request_id || q.requestId || "").trim();
  const listingId = String(q.listing_id || q.listingId || "").trim();
  if (!requestId && !listingId) return json(400, { error: "request_id or listing_id is required" });

  try {
    const r = await readResult(requestId, listingId);
    if (r.unavailable) {
      // Tell the browser plainly that polling can never succeed here, instead
      // of letting it burn its full poll window against a site with no
      // Firestore credentials.
      return json(200, { pending: true, unavailable: true,
        error: "Pricing results are not reportable on this site (no Firebase credentials in env). The pricing itself still ran — check the listing in the Pricing Console." });
    }
    return json(200, r);
  } catch (e) {
    return json(200, { pending: true, error: (e && e.message) || String(e) });
  }
};
