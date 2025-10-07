// etsyOrderProxy.js  – DROP-IN REPLACEMENT
const fetch = require("node-fetch");

exports.handler = async function (event) {
  try {
    /* ------------------------------------------------------------------
     * 1.  INPUTS & ENV
     * ------------------------------------------------------------------ */
    const orderId     = event.queryStringParameters.orderId;          // Etsy “receipt_id”
    const accessToken = event.headers["access-token"] || event.headers["Access-Token"];
    const shopId      = process.env.SHOP_ID;
    const clientId    = process.env.CLIENT_ID;

    if (!orderId)      return { statusCode: 400, body: JSON.stringify({ error: "Missing orderId parameter" }) };
    if (!accessToken)  return { statusCode: 400, body: JSON.stringify({ error: "Missing access token" }) };
    if (!shopId)       return { statusCode: 500, body: JSON.stringify({ error: "Missing SHOP_ID environment variable" }) };
    if (!clientId)     return { statusCode: 500, body: JSON.stringify({ error: "Missing CLIENT_ID environment variable" }) };

    /* ------------------------------------------------------------------
     * 2.  BUILD URL  — one call returns receipt + transactions
     * ------------------------------------------------------------------ */
    const etsyUrl =
      `https://openapi.etsy.com/v3/application/shops/${shopId}` +
      `/receipts/${orderId}?includes=` +
      [
        "Transactions",
        "Transactions.personalization",
        "Transactions.variations"       // ← ensure buyer selections are returned
      ].join(",");

    /* ------------------------------------------------------------------
     * 3.  MAKE REQUEST
     * ------------------------------------------------------------------ */
    const response = await fetch(etsyUrl, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "x-api-key": clientId,
        "Content-Type": "application/json"
      }
    });

    const payload = await response.json();
    return {
      statusCode: response.status,
      body: JSON.stringify(payload)
    };

  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};