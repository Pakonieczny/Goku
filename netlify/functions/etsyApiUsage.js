// netlify/functions/etsyApiUsage.js
// Read-only, no-cache live API usage snapshot for Index.html.

const admin = require("./firebaseAdmin");
const {
  torontoDayKey,
  USAGE_COLLECTION,
  DAILY_BUDGET,
  RATE_PER_SEC,
} = require("./etsyRateLimiter");

const HEADERS = {
  "Content-Type": "application/json",
  "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
  Pragma: "no-cache",
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "GET,OPTIONS",
};

/*  Both counter docs change only when an Etsy call is made, but every open Index.html tab asks every 15 s (2 reads each
 *  time). Keep the pair on the warm instance for CACHE_MS and let concurrent polls share one read: N tabs now cost one
 *  read pair per CACHE_MS instead of N per 15 s. A failed read is never cached. */
const CACHE_MS = 10 * 1000;
let _cache = null; // { date, at, p }
function readCounters(db, date) {
  if (_cache && _cache.date === date && Date.now() - _cache.at < CACHE_MS) return _cache.p;
  const ent = {
    date, at: Date.now(),
    p: Promise.all([
      db.collection(USAGE_COLLECTION).doc(date).get(),
      db.collection("EtsyPricing_ApiUsage").doc(date).get(),
    ]).then(([listingGeneratorSnap, pricingConsoleSnap]) => ({
      data: listingGeneratorSnap.exists ? listingGeneratorSnap.data() : {},
      pricingData: pricingConsoleSnap.exists ? pricingConsoleSnap.data() : {},
    })),
  };
  _cache = ent;
  ent.p.catch(() => { if (_cache === ent) _cache = null; });
  return ent.p;
}

exports.handler = async function (event) {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: HEADERS, body: "" };
  }
  if (event.httpMethod !== "GET") {
    return json(405, { error: "Method not allowed" });
  }

  try {
    const date = torontoDayKey();
    const db = admin.firestore();
    const { data, pricingData } = await readCounters(db, date);

    // APP statistics always come from this Listing Generator's own exact
    // counter. KEY statistics represent the shared Etsy API key, so use the
    // freshest authoritative Etsy response header available from either this
    // app or the Etsy Pricing console.
    const keyCandidates = [
      { source: "listing-generator", data },
      { source: "etsy-pricing", data: pricingData },
    ].filter(({ data: candidate }) =>
      candidate &&
      candidate.etsy_limit_per_day != null &&
      candidate.etsy_remaining_today != null
    ).sort((a, b) =>
      Number(b.data.etsy_reported_at || 0) - Number(a.data.etsy_reported_at || 0)
    );
    const freshestKey = keyCandidates[0] || null;
    const keyData = freshestKey ? freshestKey.data : {};

    return json(200, {
      ok: true,
      verified: true,
      source: "server-firestore-and-etsy-headers",
      date,
      count: Number(data.count || 0),
      count_since: data.count_since || null,
      updated_at: data.updated_at || null,
      max_qps: Number(data.max_qps || 0),
      etsy_limit_per_day:
        keyData.etsy_limit_per_day != null ? Number(keyData.etsy_limit_per_day) : null,
      etsy_remaining_today:
        keyData.etsy_remaining_today != null ? Number(keyData.etsy_remaining_today) : null,
      etsy_reported_at: keyData.etsy_reported_at || null,
      etsy_key_source: freshestKey ? freshestKey.source : null,
      budget: DAILY_BUDGET,
      qps_cap: RATE_PER_SEC,
      server_time: Date.now(),
    });
  } catch (error) {
    console.error("etsyApiUsage:", error);
    return json(500, { ok: false, error: error.message });
  }
};

function json(statusCode, payload) {
  return { statusCode, headers: HEADERS, body: JSON.stringify(payload) };
}
