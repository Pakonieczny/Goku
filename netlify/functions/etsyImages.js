// etsyImages.js
const fetch = require("node-fetch");

exports.handler = async function (event, context) {
  try {
    // Get the listingId from the query parameters.
    const listingId = (event.queryStringParameters || {}).listingId;

    // Get the access token from request headers.
    const accessToken =
      event.headers["access-token"] ||
      event.headers["Access-Token"] ||
      event.headers["authorization"] ||
      event.headers["Authorization"];

    if (!listingId) {
      return { statusCode: 400, body: JSON.stringify({ error: "Missing listingId parameter" }) };
    }
    if (!accessToken) {
      return { statusCode: 401, body: JSON.stringify({ error: "Missing access token" }) };
    }

    // Retrieve your CLIENT_ID + CLIENT_SECRET (Etsy shared secret) from environment variables.
    const CLIENT_ID = process.env.CLIENT_ID;
    const CLIENT_SECRET = process.env.CLIENT_SECRET || process.env.ETSY_SHARED_SECRET;

    if (!CLIENT_ID) {
      return { statusCode: 500, body: JSON.stringify({ error: "Missing CLIENT_ID environment variable" }) };
    }
    if (!CLIENT_SECRET) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing CLIENT_SECRET (Etsy shared secret) environment variable" }),
      };
    }

    // Construct the Etsy API URL.
    const etsyUrl = `https://api.etsy.com/v3/application/listings/${encodeURIComponent(listingId)}/images`;

    // Normalize bearer token in case Authorization: Bearer ... is ever passed through.
    const bearer = String(accessToken).toLowerCase().startsWith("bearer ")
      ? String(accessToken)
      : `Bearer ${accessToken}`;

    // Make the API call.
    const response = await fetch(etsyUrl, {
      method: "GET",
      headers: {
        Authorization: bearer,
        "Content-Type": "application/json",
        // Etsy v3 requires keystring:shared_secret in x-api-key
        "x-api-key": `${CLIENT_ID}:${CLIENT_SECRET}`,
      },
    });

    const data = await response.json();
    return {
      statusCode: response.status,
      body: JSON.stringify(data),
    };
  } catch (error) {
    console.error("Error in etsyImages:", error);
    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};