const fetch = require('node-fetch');

exports.handler = async function(event) {
  try {
    const listingId =
      event.queryStringParameters && event.queryStringParameters.listingId;
    const accessToken =
      event.headers['access-token'] || event.headers['Access-Token'];
    const clientId =
      process.env.CLIENT_ID ||
      process.env.ETSY_CLIENT_ID ||
      process.env.ETSY_API_KEY ||
      process.env.API_KEY;

    const clientSecret =
      process.env.CLIENT_SECRET ||
      process.env.ETSY_CLIENT_SECRET ||
      process.env.ETSY_SHARED_SECRET;

    if (!listingId) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing listingId parameter" })
      };
    }
    if (!accessToken) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing access token" })
      };
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

  if (!clientSecret) {
      console.error("Missing Etsy shared secret env var for x-api-key header.");
      console.log("Env presence:", {
        CLIENT_SECRET: !!process.env.CLIENT_SECRET,
        ETSY_CLIENT_SECRET: !!process.env.ETSY_CLIENT_SECRET,
        ETSY_SHARED_SECRET: !!process.env.ETSY_SHARED_SECRET,
      });
      return {
        statusCode: 500,
        body: JSON.stringify({
          error: "Missing Etsy shared secret env var for x-api-key header.",
          checked: ["CLIENT_SECRET", "ETSY_CLIENT_SECRET", "ETSY_SHARED_SECRET"],
        }),
      };
    }

    const xApiKey = `${String(clientId).trim()}:${String(clientSecret).trim()}`;

    const etsyUrl = `https://api.etsy.com/v3/application/listings/${encodeURIComponent(
      listingId
    )}`;

    const response = await fetch(etsyUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${accessToken}`, // Etsy expects the raw access_token here
        "x-api-key": xApiKey,
        "Content-Type": "application/json"
      }
    });

    const text = await response.text();
    let data;
    try {
      data = JSON.parse(text);
    } catch {
      data = { raw: text };
    }

    return {
      statusCode: response.status,
      body: JSON.stringify(data)
    };
  } catch (error) {
    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};