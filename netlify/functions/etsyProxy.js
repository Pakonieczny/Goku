const fetch = require('node-fetch');

exports.handler = async function(event) {
  try {
    const listingId =
      event.queryStringParameters && event.queryStringParameters.listingId;
    const accessToken =
      event.headers['access-token'] || event.headers['Access-Token'];
    const clientId = process.env.CLIENT_ID;

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
      console.error("CLIENT_ID environment variable is not set.");
    }

    const etsyUrl = `https://api.etsy.com/v3/application/listings/${encodeURIComponent(
      listingId
    )}`;

    const response = await fetch(etsyUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${accessToken}`, // Etsy expects the raw access_token here
        "x-api-key": clientId,
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