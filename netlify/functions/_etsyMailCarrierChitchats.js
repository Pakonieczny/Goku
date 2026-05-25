/* netlify/functions/_etsyMailCarriers/chitchats.js
 *
 * Chit Chats tracking driver — uses the NATIVE Chit Chats API.
 * No scraping needed: their API returns tracking_events directly.
 *
 * Two lookup paths (tried in order):
 *
 *   1. Authenticated API  (GET /clients/{client_id}/shipments/{shipment_id})
 *      Returns full shipment object with tracking_details array.
 *      Requires: CHIT_CHATS_CLIENT_ID + CHIT_CHATS_ACCESS_TOKEN env vars.
 *      Works only for shipments owned by YOUR Chit Chats account.
 *
 *   2. Public JSON tracking  (GET chitchats.com/tracking/{shipment_id}.json)
 *      Returns { shipment: { tracking_events: [...] } }.
 *      No authentication needed. Works for any recent Chit Chats shipment.
 *      Fallback when the authenticated path fails (wrong account, token expired).
 *
 * The input to lookup() can be EITHER:
 *   - A 10-char Chit Chats shipment_id  (e.g. "B5T85U7E1C")
 *   - A UPU S10 carrier_tracking_code   (e.g. "LX057703046NL")
 *
 * For (a) we can hit /shipments/{id} directly.
 * For (b) we need to first resolve the carrier code to a shipment_id by
 * searching /shipments?q=<code> and matching on carrier_tracking_code.
 *
 * Env vars:
 *   CHIT_CHATS_CLIENT_ID      required for authenticated path
 *   CHIT_CHATS_ACCESS_TOKEN   required for authenticated path
 *   CHIT_CHATS_BASE_URL       optional, defaults to https://chitchats.com/api/v1
 *
 * If the authenticated path is unavailable (env vars missing or shipment not
 * in account), we fall back to the public JSON tracking URL. This requires
 * having the shipment_id though — we can't resolve a carrier_tracking_code
 * to a shipment_id without the authenticated API, so if both fail we return
 * a helpful error.
 */

const fetch = require("node-fetch");

const DEFAULT_BASE = "https://chitchats.com/api/v1";
const PUBLIC_BASE  = "https://chitchats.com/tracking";

const CLIENT_ID    = process.env.CHIT_CHATS_CLIENT_ID || "";
const ACCESS_TOKEN = process.env.CHIT_CHATS_ACCESS_TOKEN || "";
const BASE         = process.env.CHIT_CHATS_BASE_URL || DEFAULT_BASE;

const CC_ID     = /^[A-Za-z0-9]{10}$/;
const UPU_S10   = /^[A-Z]{2}\d{9}[A-Z]{2}$/;
const USPS_NUM  = /^\d{12,34}$/;  // USPS numeric tracking (12-34 digit IMpb)

const authHeaders = () => ({
  "Authorization": ACCESS_TOKEN,   // raw token per Chit Chats docs (NOT "Bearer ...")
  "Content-Type" : "application/json; charset=utf-8"
});

/** Fetch JSON from a URL with good error surfacing. */
async function getJSON(url, headers = {}, timeout = 30000) {
  let res;
  try {
    res = await fetch(url, { headers, timeout });
  } catch (e) {
    throw new Error(`Network error calling ${url}: ${e.message}`);
  }
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = text; }
  if (!res.ok) {
    const msg = typeof data === "string" ? data : (data.error || JSON.stringify(data));
    const err = new Error(`Chit Chats ${res.status}: ${msg}`);
    err.status = res.status;
    throw err;
  }
  return data;
}

/**
 * Fetch a shipment by its Chit Chats shipment_id using the authenticated API.
 * Returns null if the shipment isn't in this account (404) or env missing.
 */
async function fetchShipmentById(shipmentId) {
  if (!CLIENT_ID || !ACCESS_TOKEN) {
    console.log(`[chitchats] fetchShipmentById skipped - no CLIENT_ID/ACCESS_TOKEN`);
    return null;
  }

  try {
    const url = `${BASE}/clients/${encodeURIComponent(CLIENT_ID)}/shipments/${encodeURIComponent(shipmentId)}`;
    console.log(`[chitchats] fetchShipmentById ${url}`);
    const result = await getJSON(url, authHeaders());
    const eventCount = (result?.shipment?.tracking_events || result?.tracking_events || []).length;
    console.log(`[chitchats] fetchShipmentById ${shipmentId} → events=${eventCount} status=${result?.status || result?.shipment?.status}`);
    // Chit Chats v1 wraps single-shipment responses in { shipment: {...} }
    return result?.shipment || result;
  } catch (e) {
    console.log(`[chitchats] fetchShipmentById ${shipmentId} failed: ${e.message}`);
    if (e.status === 404) return null;  // not in this account
    throw e;
  }
}

/**
 * Resolve a carrier_tracking_code (e.g. "LX057703046NL") to a shipment_id by
 * querying the /shipments endpoint with q=<code>.
 * Returns the shipment object if found, or null.
 */
async function resolveByCarrierCode(carrierCode) {
  if (!CLIENT_ID || !ACCESS_TOKEN) {
    console.log(`[chitchats] resolveByCarrierCode skipped - no CLIENT_ID/ACCESS_TOKEN`);
    return null;
  }

  const url = `${BASE}/clients/${encodeURIComponent(CLIENT_ID)}/shipments?limit=20&q=${encodeURIComponent(carrierCode)}`;
  console.log(`[chitchats] resolveByCarrierCode ${url}`);
  try {
    // Use 30s timeout — Chit Chats q= searches can be slow on large accounts.
    // Background function has 15 min budget so we can afford to wait.
    const result = await getJSON(url, authHeaders(), 30000);
    // v1 wraps in { shipments: [...] } on newer versions, raw array on older
    const list = Array.isArray(result) ? result : (result?.shipments || []);
    console.log(`[chitchats] resolveByCarrierCode ${carrierCode} → ${list.length} candidate(s)`);

    if (list.length === 0) return null;

    const normalized = String(carrierCode).toUpperCase();
    const match = list.find((s) =>
      String(s.carrier_tracking_code || "").toUpperCase() === normalized
    );

    if (match) {
      console.log(`[chitchats] resolveByCarrierCode match → shipment_id=${match.shipment_id || match.id}`);
    } else {
      console.log(`[chitchats] resolveByCarrierCode NO exact match in ${list.length} candidates`);
      // Log the first candidate's carrier_tracking_code for debugging
      if (list[0]) {
        console.log(`[chitchats]   first candidate carrier_tracking_code=${list[0].carrier_tracking_code}`);
      }
    }
    return match || list[0] || null;   // fall back to first candidate if no exact match
  } catch (e) {
    console.log(`[chitchats] resolveByCarrierCode ${carrierCode} failed: ${e.message}`);
    return null;
  }
}

/**
 * Fetch the public tracking JSON for a given Chit Chats shipment ID.
 * Works without auth. Returns { shipment: {...} } or null.
 */
async function fetchPublicTrackingJSON(shipmentId) {
  // Chit Chats public tracking URL is case-insensitive but typically lowercased
  const url = `${PUBLIC_BASE}/${encodeURIComponent(shipmentId.toLowerCase())}.json`;
  try {
    const data = await getJSON(url, {});
    return data && data.shipment ? data.shipment : null;
  } catch {
    return null;
  }
}

/** Normalize Chit Chats resolution/status into a machine-friendly key. */
function normalizeStatusKey(shipment) {
  const resolution = String(shipment.resolution || "").toLowerCase();
  const status     = String(shipment.status || "").toLowerCase();

  if (resolution === "delivered")   return "delivered";
  if (resolution === "exception")   return "exception";
  if (resolution === "returned")    return "returned";
  if (resolution === "rerouted")    return "rerouted";

  if (status === "in_transit")      return "in_transit";
  if (status === "ready")           return "pre_shipment";
  if (status === "received")        return "in_transit";
  if (status === "released")        return "in_transit";
  if (status === "inducted")        return "in_transit";
  if (status === "pending")         return "pre_shipment";
  if (status === "resolved")        return "delivered";

  return "in_transit";
}

/** Turn Chit Chats resolution_description or status into display string. */
function statusDisplay(shipment) {
  if (shipment.resolution_description) return shipment.resolution_description;

  const sk = normalizeStatusKey(shipment);
  return ({
    delivered       : "Delivered",
    out_for_delivery: "Out for Delivery",
    in_transit      : "In Transit",
    pre_shipment    : "Pre-Shipment",
    returned        : "Returned to Sender",
    exception       : "Delivery Exception",
    rerouted        : "Rerouted"
  })[sk] || "In Transit";
}

/** Normalize a Chit Chats tracking event into our shared shape. */
function normalizeEvent(e) {
  return {
    at       : e.created_at || e.timestamp || e.date || null,
    title    : e.title || e.event || e.description || "Scan",
    subtitle : e.subtitle || null,
    location : e.location_description || e.location || null,
    status   : e.status || null
  };
}

/**
 * Sanitize an event for customer-facing display.
 *
 * Customers should never see references to Chit Chats' internal operations,
 * sorting facilities, or border crossings — they're irrelevant to the
 * customer and create confusion. We present a clean USPS-flavored narrative:
 * the label was created, postage was purchased, and then USPS scans begin.
 *
 * Returns:
 *   - null        → drop this event entirely (Chit Chats/border activity)
 *   - { ...event} → show with optionally rewritten title/location
 */
function sanitizeEvent(e) {
  const t = String(e.title || "").toLowerCase();
  const loc = String(e.location || "").toLowerCase();
  const combined = (t + " " + loc).toLowerCase();

  // DROP: events mentioning border activity. Customers don't need to see
  // the Canada→US handoff — from their perspective it's just "in transit."
  if (/border/i.test(t) ||
      /crossed.*border|border.*crossing|pending.*induction/i.test(t)) {
    return null;
  }

  // DROP: internal Chit Chats operations (arrived at / departed from / received
  // by Chit Chats, shipment created for Chit Chats). These are all irrelevant
  // pre-USPS logistics.
  if (/chit\s*chats/i.test(combined) &&
      /(arrived|departed|received|shipping partner|awaiting|shipment created)/i.test(t)) {
    return null;
  }

  // Clone the event so we don't mutate the original
  const sanitized = {
    at       : e.at,
    title    : e.title,
    subtitle : e.subtitle,
    location : e.location,
    status   : e.status
  };

  // REWRITE: keep Label Created / Postage Purchased but strip any location
  // that names Chit Chats or reveals origin facility details. The customer
  // doesn't need to know the label was generated in Niagara Falls.
  if (/shipping label created|label created/i.test(t)) {
    sanitized.title = "Label Created";
    sanitized.location = null;
  } else if (/postage purchased/i.test(t)) {
    sanitized.title = "Postage Purchased";
    sanitized.location = null;
  } else if (/arrived shipping partner/i.test(t)) {
    // This is the handoff to Chit Chats' partner — drop it too
    return null;
  }

  // Final safety net: scrub any lingering forbidden strings anywhere.
  // We never want the customer to see:
  //   - "Chit Chats" (our shipping partner, customers don't need to know)
  //   - Any reference to Canada, Canadian cities, or Canadian provinces
  //     (hides international origin of the package)
  const forbidden = [
    /\bChit\s*Chats?\b/gi,
    /\bCanada\b/gi,
    /\bCanadian\b/gi,
    // Canadian provinces (commonly in location strings as "CITY, ON" etc.)
    /,\s*(ON|QC|BC|AB|MB|SK|NS|NB|NL|PE|YT|NT|NU)\b/gi,
    // Mississauga is Chit Chats' main facility city — scrub by name too
    /\bMISSISSAUGA\b/gi,
    /\bNiagara\s*Falls,?\s*ON\b/gi
  ];
  const scrub = (s) => {
    if (!s) return s;
    let out = String(s);
    for (const re of forbidden) out = out.replace(re, "");
    return out.replace(/\s+,/g, ",").replace(/\s+/g, " ").replace(/[,·\s]+$/, "").trim();
  };
  sanitized.title    = scrub(sanitized.title);
  sanitized.location = scrub(sanitized.location);
  sanitized.subtitle = scrub(sanitized.subtitle);

  // If scrubbing emptied the title, drop the event
  if (!sanitized.title) return null;
  // If scrubbing emptied the location to just whitespace/commas, null it
  if (sanitized.location && !/[A-Za-z0-9]/.test(sanitized.location)) {
    sanitized.location = null;
  }

  return sanitized;
}

/** Turn a Chit Chats shipment object into our normalized tracking result. */
function shapeShipment(shipment, trackingCodeRequested) {
  const rawEvents = shipment.tracking_events || shipment.tracking_details || [];
  console.log(`[chitchats] shapeShipment: keys=${Object.keys(shipment).join(",")}`);
  console.log(`[chitchats] shapeShipment: rawEvents.length=${Array.isArray(rawEvents) ? rawEvents.length : "not array"}, status=${shipment.status}, resolution=${shipment.resolution}`);
  if (Array.isArray(rawEvents) && rawEvents.length > 0) {
    console.log(`[chitchats] shapeShipment: first event keys=${Object.keys(rawEvents[0]).join(",")}`);
  }

  const events = (Array.isArray(rawEvents) ? rawEvents : [])
    .map(normalizeEvent)
    .filter((e) => e.title)
    .map(sanitizeEvent)          // Hide Chit Chats / border events
    .filter(Boolean);             // Drop nulls from sanitizer

  console.log(`[chitchats] shapeShipment: after sanitization events.length=${events.length}`);

  // Sort newest first for display
  events.sort((a, b) => {
    const ta = a.at ? new Date(a.at).getTime() : 0;
    const tb = b.at ? new Date(b.at).getTime() : 0;
    return tb - ta;
  });

  const statusKey = normalizeStatusKey(shipment);

  // Build a destination string
  const dest = [
    shipment.to_city,
    shipment.to_province_code,
    shipment.to_postal_code
  ].filter(Boolean).join(", ") || null;

  const origin = [
    shipment.from_city,
    shipment.from_province_code,
    shipment.from_postal_code
  ].filter(Boolean).join(", ") || null;

  return {
    carrier          : "chitchats",
    carrierDisplay   : shipment.carrier_description
                         ? `Chit Chats (${shipment.carrier_description})`
                         : "Chit Chats",
    trackingCode     : trackingCodeRequested,
    shipmentId       : shipment.shipment_id || shipment.id || null,
    carrierTracking  : shipment.carrier_tracking_code || null,
    trackingUrl      : shipment.tracking_url || null,
    status           : statusDisplay(shipment),
    statusKey,
    estimatedDelivery: shipment.estimated_delivery_at || null,
    destination      : dest,
    origin,
    shipDate         : shipment.ship_date || null,
    resolvedAt       : shipment.resolved_at || null,
    events,
    raw              : shipment
  };
}

/**
 * Look up Chit Chats tracking.
 *
 * @param {string} trackingCode  Either a shipment_id or a carrier_tracking_code
 * @returns {Promise<object>}    Normalized tracking result
 */
async function lookup(trackingCode) {
  const code = String(trackingCode || "").trim();
  if (!code) throw new Error("Missing tracking code");

  const isShipmentId  = CC_ID.test(code);
  // A "carrier code" in Chit Chats terms is anything searchable by /shipments?q=
  // which includes both UPU S10 codes (international) and USPS numeric codes.
  const isCarrierCode = UPU_S10.test(code) || USPS_NUM.test(code);

  console.log(`[chitchats] lookup("${code}") isShipmentId=${isShipmentId} isCarrierCode=${isCarrierCode}`);

  // Path A: input looks like a Chit Chats shipment_id — try direct fetch first
  if (isShipmentId) {
    // Try authenticated API
    const ship = await fetchShipmentById(code);
    if (ship) return shapeShipment(ship, code);

    // Fallback to public JSON tracking URL (no auth needed)
    const pub = await fetchPublicTrackingJSON(code);
    if (pub) return shapeShipment(pub, code);
  }

  // Path B: input looks like a carrier_tracking_code — resolve to shipment_id
  if (isCarrierCode) {
    const ship = await resolveByCarrierCode(code);
    if (ship) {
      // The /shipments list endpoint may return abbreviated data.
      // Re-fetch the full shipment (including tracking_events) by ID.
      if (ship.id || ship.shipment_id) {
        const shipmentId = ship.shipment_id || ship.id;
        const full = await fetchShipmentById(shipmentId);
        if (full) return shapeShipment(full, code);

        // If full fetch fails, try public JSON tracking for that shipment_id
        const pub = await fetchPublicTrackingJSON(shipmentId);
        if (pub) return shapeShipment(pub, code);

        // Final fallback: shape whatever the search returned
        return shapeShipment(ship, code);
      }
      return shapeShipment(ship, code);
    }
  }

  // Path C: input shape is ambiguous — try both paths opportunistically
  // (e.g., a 10-char code that could be either format)
  if (!isShipmentId && !isCarrierCode) {
    // Try treating it as a shipment_id
    const ship = await fetchShipmentById(code);
    if (ship) return shapeShipment(ship, code);

    // Try resolving as carrier code
    const byCarrier = await resolveByCarrierCode(code);
    if (byCarrier) {
      const sid = byCarrier.shipment_id || byCarrier.id;
      if (sid) {
        const full = await fetchShipmentById(sid);
        if (full) return shapeShipment(full, code);
      }
      return shapeShipment(byCarrier, code);
    }

    // Try public JSON tracking (uses the input as-is)
    const pub = await fetchPublicTrackingJSON(code);
    if (pub) return shapeShipment(pub, code);
  }

  const err = new Error(`Chit Chats shipment not found for tracking code: ${code}`);
  err.code = "NOT_FOUND";
  throw err;
}

module.exports = { lookup };
