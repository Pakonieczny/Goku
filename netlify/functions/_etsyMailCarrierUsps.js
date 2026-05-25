/* netlify/functions/_etsyMailCarrierUsps.js
 *
 * USPS tracking driver — routes through Chit Chats' API to get scan data.
 *
 * WHY THIS APPROACH:
 *   CustomBrites prints shipping labels through Chit Chats. For US-bound
 *   orders, Chit Chats hands off to USPS in Niagara Falls, but the full
 *   scan history (including USPS events like "Accepted at USPS Origin
 *   Facility" and "Arrived at Post Office") is synced back to Chit Chats
 *   and exposed via their API.
 *
 *   Chit Chats' /shipments endpoint supports `?q=<code>` search, which
 *   matches against the carrier_tracking_code field — so given a raw USPS
 *   number like "4206524889869212490362894333301444" we can find the
 *   Chit Chats shipment that owns it and retrieve the full event list.
 *
 *   This approach:
 *     - No third-party APIs, no per-lookup cost
 *     - No USPS anti-bot problems (we never touch USPS directly)
 *     - Fast — ~1-2 second lookups vs ~25 sec for Puppeteer
 *     - Works identically for USPS labels AND international shipments
 *     - Returns structured events so the AI can reference specific scans
 *     - Renders using our existing SVG timeline (clean, branded image)
 *
 *   Trade-off:
 *     - Only works for labels printed through Chit Chats (fine for
 *       CustomBrites' actual business, since all shipments go through
 *       their account)
 *     - If a customer asks about a tracking number that ISN'T in the
 *       Chit Chats account, returns NOT_FOUND (which is honest — it's
 *       not our shipment to track)
 *
 * Architecture:
 *   We simply delegate to the existing Chit Chats driver, which already
 *   implements `resolveByCarrierCode()` to do exactly this lookup.
 *
 * Env vars (inherited from Chit Chats driver):
 *   CHIT_CHATS_CLIENT_ID       required
 *   CHIT_CHATS_ACCESS_TOKEN    required
 *   CHIT_CHATS_BASE_URL        optional, defaults to https://chitchats.com/api/v1
 */

const chitchats = require("./_etsyMailCarrierChitchats");

/**
 * Look up a USPS tracking number via Chit Chats' API.
 *
 * @param {string} trackingCode  The USPS label (any format, 12-34 digits)
 * @returns {Promise<object>}    Normalized tracking result
 */
async function lookup(trackingCode) {
  const code = String(trackingCode || "").trim();
  if (!code) {
    throw Object.assign(new Error("Missing tracking code"), { code: "INVALID_INPUT" });
  }

  console.log(`[usps→chitchats] looking up ${code} via Chit Chats carrier-code search`);

  let result;
  try {
    result = await chitchats.lookup(code);
  } catch (e) {
    // If Chit Chats can't find it, surface a clearer error that explains
    // WHY — the operator sees "not in our Chit Chats account" instead of
    // a generic not-found, which is more actionable.
    if (e.code === "NOT_FOUND" || /not found/i.test(e.message)) {
      throw Object.assign(
        new Error(`No Chit Chats shipment found for tracking code ${code}. ` +
                  `This usually means the label wasn't printed through Chit Chats, ` +
                  `the shipment was archived, or the tracking number has a typo.`),
        { code: "NOT_FOUND" }
      );
    }
    throw e;
  }

  // Chit Chats returns a result with carrier="chitchats". For USPS labels,
  // re-label it so the UI shows "USPS" rather than "Chit Chats (USPS)".
  // The scan events came from USPS anyway — we're just using Chit Chats as
  // the data aggregator.
  const carrierDisplay = result.carrierDisplay || "";
  const isUspsHandoff = /usps/i.test(carrierDisplay) ||
                        /usps/i.test(result.raw?.postage_type || "") ||
                        /^\d{12,34}$/.test(code);

  if (isUspsHandoff) {
    result.carrier        = "usps";
    result.carrierDisplay = "USPS";
  }

  console.log(`[usps→chitchats] ${code} → ${result.carrierDisplay} ${result.status} (${result.events.length} events)`);
  return result;
}

module.exports = { lookup };
