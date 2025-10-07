// netlify/functions/etsyListingImagesProxy.js
const fetch = require("node-fetch");

exports.handler = async function(event){
  try {
    const listingId   = event.queryStringParameters && event.queryStringParameters.listingId;
    const accessToken = event.headers["access-token"] || event.headers["Access-Token"];
    const clientId    = process.env.CLIENT_ID;
    if (!listingId)   return { statusCode: 400, body: JSON.stringify({ error: "Missing listingId" }) };
    if (!accessToken) return { statusCode: 400, body: JSON.stringify({ error: "Missing access token" }) };
    if (!clientId)    return { statusCode: 500, body: JSON.stringify({ error: "Missing CLIENT_ID" }) };

    const url = `https://openapi.etsy.com/v3/application/listings/${listingId}/images`;
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