const fetch = require("node-fetch");

exports.handler = async function (event, context) {
  try {
    // Extract listingId, token, and userId (required for Etsy v3 Bearer format) from query parameters.
    const { listingId, token, userId } = event.queryStringParameters || {};
    if (!listingId || !token || !userId) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing listingId, token, or userId" }),
      };
    }
    console.log("Received listingId:", listingId);
    console.log("Received userId:", userId);
    console.log("Received token (first 6):", (token || "").slice(0, 6) + "*****");

    // Retrieve CLIENT_ID and SHOP_ID from environment variables.
    const clientId = process.env.CLIENT_ID;
    const shopId = process.env.SHOP_ID;
    if (!clientId) {
      console.error("CLIENT_ID environment variable is not set.");
    } else {
      console.log("Using CLIENT_ID (first 6):", clientId.slice(0, 6) + "*****");
    }
    if (!shopId) {
      console.error("SHOP_ID environment variable is not set.");
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "SHOP_ID environment variable is not set." }),
      };
    }

    // Read the update payload from the request body (assumes JSON payload)
    let payload = {};
    try {
      payload = event.body ? JSON.parse(event.body) : {};
    } catch (e) {
      console.warn("Invalid JSON body; defaulting to empty object. Error:", e.message);
      payload = {};
    }

    // Build x-www-form-urlencoded body from payload (arrays become repeated keys)
    const form = new URLSearchParams();
    for (const [key, value] of Object.entries(payload)) {
      if (value === undefined || value === null) continue;
      if (Array.isArray(value)) {
        for (const v of value) {
          if (v !== undefined && v !== null) form.append(key, String(v));
        }
      } else {
        form.append(key, String(value));
      }
    }

    // Log URL and keys for troubleshooting (do not log full sensitive data)
    const updateUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${listingId}`;
    console.log("PATCH Update URL:", updateUrl);
    console.log("Headers to send:", {
      "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
      "Authorization": `Bearer ${userId}.<redacted>`,
      "x-api-key": clientId ? clientId.slice(0, 6) + "*****" : "(unset)",
    });
    console.log("Form keys being sent:", Array.from(form.keys()));

    // Make the PATCH request to update the listing (Etsy v3 requires PATCH + x-www-form-urlencoded)
    const response = await fetch(updateUrl, {
      method: "PATCH",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
        "Authorization": `Bearer ${userId}.${token}`,
        "x-api-key": clientId,
      },
      body: form.toString(),
    });

    console.log("PATCH update response status:", response.status);

    const text = await response.text();
    // Try to parse JSON if possible; otherwise return the raw text.
    let data;
    try {
      data = JSON.parse(text);
    } catch {
      data = { raw: text };
    }

    if (!response.ok) {
      console.error("Error updating listing. PATCH failed:", text);
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: "Error updating listing", details: data }),
      };
    }

    console.log("Listing updated successfully.");
    return {
      statusCode: 200,
      body: JSON.stringify(data),
    };
  } catch (error) {
    console.error("Exception in updateListing handler:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
};