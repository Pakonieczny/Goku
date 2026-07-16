const fetch = require("node-fetch");

const JSON_HEADERS = {
  "Content-Type": "application/json",
  "Cache-Control": "no-store"
};

exports.handler = async function (event) {
  try {
    if (event.httpMethod === "OPTIONS") {
      return {
        statusCode: 204,
        headers: {
          ...JSON_HEADERS,
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Headers": "Content-Type",
          "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
        },
        body: ""
      };
    }

    const query = event.queryStringParameters || {};
    let body = {};
    if (event.body) {
      try { body = JSON.parse(event.body); } catch (_) {}
    }

    const grantType = String(body.grant_type || query.grant_type || "authorization_code").trim();
    const code = String(body.code || query.code || "").trim();
    const codeVerifier = String(body.code_verifier || query.code_verifier || "").trim();
    const refreshToken = String(body.refresh_token || query.refresh_token || "").trim();

    const CLIENT_ID =
      process.env.CLIENT_ID ||
      process.env.ETSY_CLIENT_ID ||
      process.env.ETSY_API_KEY ||
      process.env.API_KEY;
    const CLIENT_SECRET = process.env.CLIENT_SECRET || process.env.ETSY_CLIENT_SECRET;
    const REDIRECT_URI = process.env.REDIRECT_URI || process.env.ETSY_REDIRECT_URI;

    if (!CLIENT_ID) {
      return response(500, { error: "Missing Etsy CLIENT_ID environment variable." });
    }

    const params = new URLSearchParams();
    params.set("grant_type", grantType);
    params.set("client_id", CLIENT_ID);

    if (grantType === "refresh_token") {
      if (!refreshToken) {
        return response(400, { error: "Missing refresh_token." });
      }
      params.set("refresh_token", refreshToken);
    } else if (grantType === "authorization_code") {
      if (!CLIENT_SECRET || !REDIRECT_URI) {
        return response(500, {
          error: "Missing Etsy OAuth env vars for authorization-code exchange.",
          missing: [
            !CLIENT_SECRET && "CLIENT_SECRET",
            !REDIRECT_URI && "REDIRECT_URI"
          ].filter(Boolean)
        });
      }
      if (!code || !codeVerifier) {
        return response(400, { error: "Missing code or code_verifier." });
      }
      params.set("client_secret", CLIENT_SECRET);
      params.set("code", code);
      params.set("redirect_uri", REDIRECT_URI);
      params.set("code_verifier", codeVerifier);
    } else {
      return response(400, { error: `Unsupported grant_type: ${grantType}` });
    }

    const upstream = await fetch("https://api.etsy.com/v3/public/oauth/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: params
    });

    const text = await upstream.text();
    let data;
    try { data = JSON.parse(text); } catch (_) { data = { error: text || "Invalid Etsy token response" }; }

    return response(upstream.status, data);
  } catch (error) {
    console.error("Error in exchangeToken:", error);
    return response(500, { error: String(error && error.message || error) });
  }
};

function response(statusCode, payload) {
  return {
    statusCode,
    headers: JSON_HEADERS,
    body: JSON.stringify(payload)
  };
}
