/*  netlify/functions/etsyMailListing.js
 *
 *  Fetches slim metadata for a single Etsy listing — used by the M5
 *  composer when an operator pastes or drops an Etsy listing URL into
 *  the reply box. The composer renders the returned metadata as a
 *  "chip" (thumbnail + title + price) above the textarea.
 *
 *  Endpoint:
 *    GET /.netlify/functions/etsyMailListing?listingId=1234567890
 *
 *  Response (200):
 *    {
 *      success     : true,
 *      listingId   : "1234567890",
 *      title       : "Sterling Silver Cardinal Charm",
 *      url         : "https://www.etsy.com/listing/1234567890",
 *      price       : "$24.00",
 *      priceAmount : 24.00,
 *      currency    : "USD",
 *      state       : "active" | "inactive" | "sold_out" | ...,
 *      thumbnail   : {
 *        url_170x135 : "...",
 *        url_340x270 : "...",
 *        url_570xN   : "...",
 *        url_fullxfull: "..."
 *      }
 *    }
 *
 *  Response (404):
 *    { error: "Listing not found", listingId }
 *
 *  This uses the same OAuth token as etsyMailOrder.js — shared helpers
 *  from _etsyMailEtsy.js handle the refresh dance.
 *
 *  No extension auth required. This endpoint is called from the inbox
 *  UI (same-origin), and all reads are against public Etsy listing data.
 *  If you want to tighten access, drop in requireExtensionAuth() from
 *  _etsyMailAuth.js — but be aware the inbox doesn't currently send the
 *  secret header.
 */

const {
  getListing,
  getListingImages
} = require("./_etsyMailEtsy");
const { requireExtensionAuth } = require("./_etsyMailAuth");
const meter = require("./_etsyApiMeter");

const CORS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, X-EtsyMail-Secret",
  "Access-Control-Allow-Methods": "GET, OPTIONS"
};

function json(statusCode, body) {
  return { statusCode, headers: { ...CORS, "Content-Type": "application/json" }, body: JSON.stringify(body) };
}

/** Etsy prices come back as { amount: 2400, divisor: 100, currency_code: "USD" }.
 *  Turn that into a numeric price + a formatted string. */
function normalizePrice(priceObj) {
  if (!priceObj || typeof priceObj !== "object") return { amount: null, formatted: null, currency: null };
  const divisor = priceObj.divisor || 100;
  const amount  = priceObj.amount != null ? priceObj.amount / divisor : null;
  const currency = priceObj.currency_code || null;
  let formatted = null;
  if (amount != null) {
    try {
      formatted = new Intl.NumberFormat("en-US", {
        style: "currency",
        currency: currency || "USD"
      }).format(amount);
    } catch {
      formatted = `$${amount.toFixed(2)}`;
    }
  }
  return { amount, formatted, currency };
}

/** Extract a numeric listing_id from either a bare id string or a full URL. */
function parseListingId(raw) {
  if (!raw) return null;
  const s = String(raw).trim();
  if (/^\d+$/.test(s)) return s;
  const m = s.match(/etsy\.com\/listing\/(\d+)/i);
  return m ? m[1] : null;
}

exports.handler = meter.wrapHandler(async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }
  if (event.httpMethod !== "GET") {
    return json(405, { error: "Method Not Allowed" });
  }

  // v0.9.1 #1: auth required (inbox forwards X-EtsyMail-Secret).
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  const qs = event.queryStringParameters || {};
  const listingId = parseListingId(qs.listingId || qs.id || qs.url);

  if (!listingId) {
    return json(400, { error: "Missing listingId (number or Etsy listing URL)" });
  }

  try {
    // Fetch listing + first image in parallel. Both can 404 independently.
    const [listingResult, imagesResult] = await Promise.allSettled([
      getListing(listingId),
      getListingImages(listingId)
    ]);

    if (listingResult.status === "rejected") {
      const err = listingResult.reason;
      const status = err && err.status ? err.status : 500;
      if (status === 404) {
        return json(404, { error: "Listing not found", listingId });
      }
      console.error(`etsyMailListing(${listingId}) error:`, err && err.message);
      return json(status, {
        error    : "Etsy API error",
        detail   : err && err.message,
        listingId
      });
    }

    const listing = listingResult.value;
    const thumbnail = imagesResult.status === "fulfilled" ? imagesResult.value : null;

    const { amount, formatted, currency } = normalizePrice(listing.price);

    return json(200, {
      success    : true,
      listingId  : String(listing.listing_id),
      title      : listing.title || "Untitled listing",
      description: (listing.description || "").slice(0, 400),   // first ~400 chars, enough for chip preview
      url        : listing.url || `https://www.etsy.com/listing/${listing.listing_id}`,
      price      : formatted,
      priceAmount: amount,
      currency,
      state      : listing.state || null,
      quantity   : typeof listing.quantity === "number" ? listing.quantity : null,
      sku        : Array.isArray(listing.skus) && listing.skus.length ? listing.skus[0] : null,
      shopSectionId: listing.shop_section_id || null,
      hasVariations: Boolean(listing.has_variations),
      isCustomizable: Boolean(listing.is_customizable),
      thumbnail
    });

  } catch (err) {
    console.error(`etsyMailListing(${listingId}) unhandled:`, err);
    const status = err && err.status ? err.status : 500;
    return json(status, {
      error : err.message || "Unknown error",
      listingId
    });
  }
});
