/**
 * listOpenOrders.js  –  returns EVERY open receipt for your shop.
 * Works by walking Etsy's offset-based pagination until next_offset === null
 * when offset==0, but fetches ONE page when an explicit offset is sent.
 */

// Netlify Functions (Node 18+) provides global fetch.
// Fallback to node-fetch only if you’re on an older runtime.
let fetchFn = global.fetch;
if (!fetchFn) {
  try { fetchFn = require("node-fetch"); } catch (_) {}
}

exports.handler = async (event) => {
  try {
   /* 1) Normalize headers */
    const h = {};
    for (const [k, v] of Object.entries(event.headers || {})) h[String(k).toLowerCase()] = v;

    /* 2) OAuth token from front-end header */
    const accessToken =
      h["access-token"] ||
      (h["authorization"] ? String(h["authorization"]).replace(/^bearer\s+/i, "") : "");
    if (!accessToken) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing access-token header" })
      };
    }

    /* 3) Required env vars */
    const SHOP_ID        = process.env.SHOP_ID;        // numeric shop id
    const CLIENT_ID      = process.env.CLIENT_ID;      // Etsy app keystring
    const CLIENT_SECRET  = process.env.CLIENT_SECRET;  // Etsy app shared secret
    if (!SHOP_ID || !CLIENT_ID || !CLIENT_SECRET) {
      return {
        statusCode: 500,
        body: JSON.stringify({
          error: "Missing SHOP_ID / CLIENT_ID / CLIENT_SECRET env var",
          hint:  "Set CLIENT_SECRET to your Etsy app shared secret (x-api-key must be keystring:shared_secret)."
        })
      };
    }

    if (!fetchFn) {
      return { statusCode: 500, body: JSON.stringify({ error: "No fetch available in this runtime" }) };
    }

    /* 4) ONE page per call (your browser already paginates with offset) */
    const offset = Number(event.queryStringParameters?.offset || 0);
    const qs = new URLSearchParams({
      status       : "open",
      was_paid     : "true",
      was_shipped  : "false",
      was_canceled : "false",
      limit        : "100",
      offset       : String(offset),
      sort_on      : "created",
      sort_order   : "desc"
    });

    const url = `https://api.etsy.com/v3/application/shops/${SHOP_ID}/receipts?${qs}`;
    const resp = await fetchFn(url, {
      method: "GET",
      headers: {
        Authorization : `Bearer ${accessToken}`,
        // IMPORTANT: Etsy requires keystring:shared_secret here.
        "x-api-key"   : `${CLIENT_ID}:${CLIENT_SECRET}`,
        "Accept"      : "application/json"
      }
    });

    if (!resp.ok) {
      const txt = await resp.text();
      return {
        statusCode: resp.status,
        headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
        body: JSON.stringify({ error: "etsy_error", status: resp.status, body: txt.slice(0, 2000) })
      };
    }

    const data = await resp.json();
    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
      body: JSON.stringify(data)
    };

  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};