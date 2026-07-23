// netlify/functions/etsyApiProbe.js
//
// Makes one lightweight authenticated Etsy application request so the shared
// limiter can initialize today's app count and capture Etsy's authoritative
// whole-key quota headers. Called once after connection / connected page load;
// the 15-second UI refresh reads Firestore and does NOT repeat this Etsy call.

const { etsyFetch } = require("./etsyRateLimiter");

const HEADERS = {
  "Content-Type": "application/json",
  "Cache-Control": "no-store",
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, Access-Token, Authorization",
  "Access-Control-Allow-Methods": "GET,OPTIONS",
};

exports.handler = async function (event) {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: HEADERS, body: "" };
  }
  if (event.httpMethod !== "GET") {
    return json(405, { ok: false, error: "Method not allowed" });
  }

  try {
    const requestHeaders = event.headers || {};
    const token = String(
      requestHeaders["access-token"] ||
      requestHeaders["Access-Token"] ||
      requestHeaders.authorization ||
      requestHeaders.Authorization ||
      ""
    ).replace(/^Bearer\s+/i, "").trim();

    if (!token) {
      return json(401, { ok: false, error: "Missing Etsy access token" });
    }

    const clientId =
      process.env.CLIENT_ID ||
      process.env.ETSY_CLIENT_ID ||
      process.env.ETSY_API_KEY ||
      process.env.API_KEY;
    const clientSecret =
      process.env.CLIENT_SECRET ||
      process.env.ETSY_CLIENT_SECRET ||
      process.env.ETSY_SHARED_SECRET;

    if (!clientId || !clientSecret) {
      return json(500, {
        ok: false,
        error: "Missing Etsy CLIENT_ID or CLIENT_SECRET environment variable",
      });
    }

    const xApiKey = `${String(clientId).trim()}:${String(clientSecret).trim()}`;
    const response = await etsyFetch(
      "https://api.etsy.com/v3/application/users/me",
      {
        method: "GET",
        headers: {
          Accept: "application/json",
          Authorization: `Bearer ${token}`,
          "x-api-key": xApiKey,
        },
      },
      { retries: 2 }
    );

    // Do not expose Etsy user data; this endpoint exists solely to verify the
    // connection and capture the response's rate-limit headers.
    if (!response.ok) {
      const detail = await response.text().catch(() => "");
      return json(response.status, {
        ok: false,
        error: "Etsy verification request failed",
        detail: String(detail).slice(0, 500),
      });
    }

    await response.text().catch(() => "");
    return json(200, { ok: true, verified: true });
  } catch (error) {
    console.error("etsyApiProbe:", error);
    return json(500, { ok: false, error: error.message });
  }
};

function json(statusCode, payload) {
  return { statusCode, headers: HEADERS, body: JSON.stringify(payload) };
}
