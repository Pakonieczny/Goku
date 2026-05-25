// netlify/functions/etsyListingInventoryProxy.js
const fetch = require("node-fetch");

exports.handler = async function(event){
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

    // Extract SKUs from products array (if present)
    const skus = [];
    if (payload && payload.products && Array.isArray(payload.products)) {
      for (const p of payload.products) {
        const sku = (p.sku || "").trim();
        if (sku) skus.push(sku);
      }
    }
    return { statusCode: 200, body: JSON.stringify({ listing_id: listingId, skus }) };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};