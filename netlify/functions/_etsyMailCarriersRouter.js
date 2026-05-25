/* netlify/functions/_etsyMailCarriers/index.js
 *
 * Carrier router for the tracking-image feature.
 *
 * Detects the carrier from the tracking number format, then dispatches
 * to the appropriate driver. Each driver normalizes its return shape
 * so the upstream renderer doesn't need to care which carrier was used.
 *
 * Common shape returned by all drivers:
 *   {
 *     carrier            : "usps" | "chitchats" | "unknown",
 *     carrierDisplay     : "USPS" | "Chit Chats",
 *     trackingCode       : string,             // echoed back
 *     status             : string,             // human-readable, e.g. "In Transit"
 *     statusKey          : string,             // machine-readable, e.g. "in_transit"
 *     estimatedDelivery  : string | null,      // ISO date if known
 *     destination        : string | null,      // "DEL RIO, TX 78840" etc.
 *     origin             : string | null,
 *     shipDate           : string | null,      // ISO date
 *     resolvedAt         : string | null,      // ISO ts when delivered/resolved, null if in transit
 *     events             : [
 *       {
 *         at        : ISO string,              // required
 *         title     : string,                  // required (e.g. "Arrived at USPS Regional Origin Facility")
 *         subtitle  : string | null,           // optional secondary line
 *         location  : string | null,           // "NIAGARA FALLS, NY 14304"
 *         status    : string | null            // carrier-specific status code if present
 *       },
 *       ...
 *     ],
 *     raw                : object              // the driver-specific raw payload for debugging
 *   }
 *
 * Detection rules:
 *   Chit Chats shipment_id    →  /^[A-Za-z0-9]{10}$/  (10 char alphanumeric, e.g. "B5T85U7E1C")
 *   UPU S10 (international)   →  /^[A-Z]{2}\d{9}[A-Z]{2}$/  (e.g. "LX057703046NL")
 *   USPS label                →  /^\d{12}$|^\d{15}$|^\d{20}$|^\d{22}$|^\d{26}$/
 *
 * Because LX057703046NL (UPU S10) could be delivered by multiple carriers but
 * is very commonly a Chit Chats label when it came from your shop, we dispatch
 * it to Chit Chats first. Chit Chats' API supports looking up by either the
 * shipment_id or the carrier_tracking_code (see chitchats.js for details).
 */

const usps      = require("./_etsyMailCarrierUsps");
const chitchats = require("./_etsyMailCarrierChitchats");

const UPU_S10 = /^[A-Z]{2}\d{9}[A-Z]{2}$/;
const CC_ID   = /^[A-Za-z0-9]{10}$/;
// USPS labels come in multiple lengths depending on service class:
//   12 digits — legacy Delivery Confirmation
//   15 digits — older Priority / Express
//   20, 22 digits — Signature Confirmation / older IMpb
//   26 digits — standard IMpb (most common today)
//   30, 34 digits — longer IMpb variants (Parcel Select, ePacket, some
//                   Chit Chats-generated labels include extra app/routing ID)
// Accepting any purely-numeric string of 12-34 digits covers all of these
// without false-matching UPU S10 (which has letters) or CC IDs (10 chars +
// has letters).
const USPS_LABEL = /^\d{12,34}$/;

function detectCarrier(trackingCode) {
  const code = String(trackingCode || "").trim();
  if (!code) return { carrier: "unknown", reason: "empty tracking code" };

  // USPS numeric labels (most common for US delivery)
  if (USPS_LABEL.test(code)) return { carrier: "usps" };

  // UPU S10 international code — almost always a Chit Chats shipment when coming
  // from the CustomBrites shop, since Chit Chats handles CA→US and international.
  if (UPU_S10.test(code)) return { carrier: "chitchats" };

  // Chit Chats internal shipment ID
  if (CC_ID.test(code) && /[A-Za-z]/.test(code) && /\d/.test(code)) {
    // Note: USPS 12-digit labels are pure numeric and won't match because we
    // require at least one letter AND one digit.
    return { carrier: "chitchats" };
  }

  return {
    carrier: "unknown",
    reason: `tracking code '${code}' does not match any known carrier format`
  };
}

/**
 * Look up tracking details for a tracking code.
 *
 * @param {string} trackingCode  The tracking code to look up
 * @param {object} [options]
 * @param {string} [options.carrierHint]  Override auto-detection with an explicit carrier
 *                                         ("usps" or "chitchats")
 * @returns {Promise<object>}    Normalized tracking result (see shape above)
 * @throws                       If carrier unknown or driver lookup fails
 */
async function lookupTracking(trackingCode, options = {}) {
  const hint = (options.carrierHint || "").toLowerCase();
  let detected;

  if (hint === "usps" || hint === "chitchats") {
    detected = { carrier: hint };
  } else {
    detected = detectCarrier(trackingCode);
  }

  if (detected.carrier === "unknown") {
    const err = new Error(`Could not detect carrier: ${detected.reason}`);
    err.code = "UNKNOWN_CARRIER";
    throw err;
  }

  if (detected.carrier === "usps") {
    return await usps.lookup(trackingCode);
  }
  if (detected.carrier === "chitchats") {
    return await chitchats.lookup(trackingCode);
  }

  // Should never reach here given the switch above, but defensive
  const err = new Error(`Unsupported carrier: ${detected.carrier}`);
  err.code = "UNSUPPORTED_CARRIER";
  throw err;
}

module.exports = {
  detectCarrier,
  lookupTracking,
  // Export drivers for direct access in tests / future tools
  drivers: { usps, chitchats }
};
