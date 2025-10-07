// netlify/functions/etsyListingInventoryDetailProxy.js
const fetch = require("node-fetch");

exports.handler = async (event) => {
  try {
    const listingId   = event.queryStringParameters && event.queryStringParameters.listingId;
    const accessToken = event.headers["access-token"] || event.headers["Access-Token"];
    const clientId    = process.env.CLIENT_ID;

    if (!listingId)   return { statusCode: 400, body: JSON.stringify({ error: "Missing listingId" }) };
    if (!accessToken) return { statusCode: 400, body: JSON.stringify({ error: "Missing access token" }) };
    if (!clientId)    return { statusCode: 500, body: JSON.stringify({ error: "Missing CLIENT_ID" }) };

    const url = `https://openapi.etsy.com/v3/application/listings/${listingId}/inventory`;
    const resp = await fetch(url, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "x-api-key": clientId,
        "Content-Type": "application/json"
      }
    });

    const payload = await resp.json();
    if (!resp.ok) return { statusCode: resp.status, body: JSON.stringify(payload) };

    // Return minimal shape used by editor
    const products = Array.isArray(payload.products) ? payload.products.map(p => ({
      product_id: p.product_id,
      sku: (p.sku || "").trim()
    })) : [];

    return { statusCode: 200, body: JSON.stringify({ listing_id: Number(listingId), products }) };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};