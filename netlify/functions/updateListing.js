// netlify/functions/updateListing.js
const fetch = require('node-fetch');

exports.handler = async function (event) {
  try {
    const listingId =
      (event.queryStringParameters && event.queryStringParameters.listingId) || null;

    // Token can come via query (?token=...), or headers
    const token =
      (event.queryStringParameters && event.queryStringParameters.token) ||
      event.headers['access-token'] ||
      event.headers['Access-Token'] ||
      event.headers['authorization']?.replace(/^Bearer\s+/i, '');

    const clientId =
      process.env.CLIENT_ID ||
      process.env.ETSY_CLIENT_ID ||
      process.env.ETSY_API_KEY ||
      process.env.API_KEY;
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
      console.error("Missing Etsy app key env var for x-api-key header.");
      console.log("Env presence:", {
        CLIENT_ID: !!process.env.CLIENT_ID,
        ETSY_CLIENT_ID: !!process.env.ETSY_CLIENT_ID,
        ETSY_API_KEY: !!process.env.ETSY_API_KEY,
        API_KEY: !!process.env.API_KEY,
      });
      return {
        statusCode: 500,
        body: JSON.stringify({
          error: "Missing Etsy app key env var for x-api-key header.",
          checked: ["CLIENT_ID", "ETSY_CLIENT_ID", "ETSY_API_KEY", "API_KEY"],
        }),
      };
    }

    // Parse JSON payload (title/description/tags/etc.)
    let payload = {};
    try {
      payload = event.body ? JSON.parse(event.body) : {};
    } catch (e) {
      console.warn("Invalid JSON body; defaulting to empty object. Error:", e.message);
      payload = {};
    }

    // Build x-www-form-urlencoded body.
    // IMPORTANT: Etsy v3 expects arrays (e.g., tags) as a SINGLE comma-separated string.
    const form = new URLSearchParams();
    for (const [key, value] of Object.entries(payload)) {
      if (value == null) continue;

      if (Array.isArray(value)) {
        // Filter null/undefined, stringify, join with commas
        const csv = value
          .filter(v => v != null && String(v).trim() !== "")
          .map(v => String(v))
          .join(',');
        form.append(key, csv);
      } else if (typeof value === 'object') {
        // If you ever send structured fields, serialize safely
        form.append(key, JSON.stringify(value));
      } else {
        form.append(key, String(value));
      }
    }

    const updateUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${encodeURIComponent(listingId)}`;

    const response = await fetch(updateUrl, {
      method: "PATCH",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
        "Accept": "application/json",
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