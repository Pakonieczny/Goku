// netlify/functions/etsyShopListingsProxy.js
const fetch = require("node-fetch");

exports.handler = async function(event) {
  try {
    const q = event.queryStringParameters || {};
    const limit  = Math.min(100, Math.max(1, parseInt(q.limit || "50", 10)));
    const offset = Math.max(0, parseInt(q.offset || "0", 10));
    const state  = String(q.state || "active"); // active|inactive|draft (as needed)

    const accessToken = event.headers["access-token"] || event.headers["Access-Token"];
    const shopId   = process.env.SHOP_ID;
    const clientId = process.env.CLIENT_ID;

    if (!accessToken) return { statusCode: 400, body: JSON.stringify({ error: "Missing access token" }) };
    if (!shopId)      return { statusCode: 500, body: JSON.stringify({ error: "Missing SHOP_ID" }) };
    if (!clientId)    return { statusCode: 500, body: JSON.stringify({ error: "Missing CLIENT_ID" }) };

    const url = `https://openapi.etsy.com/v3/application/shops/${shopId}/listings?limit=${limit}&offset=${offset}&state=${encodeURIComponent(state)}`;

    const resp = await fetch(url, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "x-api-key": clientId,
        "Content-Type": "application/json"
      }
    });

    const payload = await resp.json();
    return { statusCode: resp.status, body: JSON.stringify(payload) };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};