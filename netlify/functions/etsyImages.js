// etsyImages.js
const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  try {
    // Get the listingId from the query parameters.
    const listingId = event.queryStringParameters.listingId;
    // Get the access token from request headers.
    const accessToken = event.headers['access-token'] || event.headers['Access-Token'];
    if (!listingId) {
      return { statusCode: 400, body: JSON.stringify({ error: "Missing listingId parameter" }) };
    }
    if (!accessToken) {
      return { statusCode: 400, body: JSON.stringify({ error: "Missing access token" }) };
    }
    // Retrieve your CLIENT_ID (which also serves as your API key) from environment variables.
    const CLIENT_ID = process.env.CLIENT_ID;
    // Construct the Etsy API URL.
    const etsyUrl = `https://api.etsy.com/v3/application/listings/${listingId}/images`;
    
    // Make the API call.
    const response = await fetch(etsyUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json",
        "x-api-key": CLIENT_ID
      }
    });
    const data = await response.json();
    return {
      statusCode: response.status,
      body: JSON.stringify(data)
    };
  } catch (error) {
    console.error("Error in etsyImages:", error);
    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};