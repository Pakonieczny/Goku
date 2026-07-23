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

exports.handler = async function (event) {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: HEADERS, body: "" };
  }
  if (event.httpMethod !== "GET") {
    return json(405, { error: "Method not allowed" });
  }

  try {
    const date = torontoDayKey();
    const snap = await admin.firestore().collection(USAGE_COLLECTION).doc(date).get();
    const data = snap.exists ? snap.data() : {};

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
        data.etsy_limit_per_day != null ? Number(data.etsy_limit_per_day) : null,
      etsy_remaining_today:
        data.etsy_remaining_today != null ? Number(data.etsy_remaining_today) : null,
      etsy_reported_at: data.etsy_reported_at || null,
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
