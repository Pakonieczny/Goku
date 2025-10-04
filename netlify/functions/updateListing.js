const fetch = require('node-fetch');

exports.handler = async function(event) {
  try {
    const listingId = event.queryStringParameters && event.queryStringParameters.listingId;
    const token =
      (event.queryStringParameters && event.queryStringParameters.token) ||
      event.headers['access-token'] ||
      event.headers['Access-Token'];
    const clientId = process.env.CLIENT_ID;
    const shopId = process.env.SHOP_ID;

    if (!listingId) {
      return { statusCode: 400, body: JSON.stringify({ error: "Missing listingId parameter" }) };
    }
    if (!token) {
      return { statusCode: 400, body: JSON.stringify({ error: "Missing access token" }) };
    }
    if (!shopId) {
      return { statusCode: 500, body: JSON.stringify({ error: "SHOP_ID environment variable is not set." }) };
    }
    if (!clientId) {
      console.error("CLIENT_ID environment variable is not set.");
    }

    let payload = {};
    try {
      payload = event.body ? JSON.parse(event.body) : {};
    } catch (e) {
      console.warn("Invalid JSON body; defaulting to empty object. Error:", e.message);
      payload = {};
    }

    const form = new URLSearchParams();
    for (const [key, value] of Object.entries(payload)) {
      if (value == null) continue;
      if (Array.isArray(value)) {
        for (const v of value) {
          if (v != null) form.append(key, String(v));
        }
      } else {
        form.append(key, String(value));
      }
    }

    const updateUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${encodeURIComponent(listingId)}`;

    const response = await fetch(updateUrl, {
      method: "PATCH",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId
      },
      body: form.toString()
    });

    const text = await response.text();
    let data;
    try {
      data = JSON.parse(text);
    } catch {
      data = { raw: text };
    }

    if (!response.ok) {
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: "Error updating listing", details: data })
      };
    }

    return {
      statusCode: 200,
      body: JSON.stringify(data)
    };
  } catch (error) {
    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};