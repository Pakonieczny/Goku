const fetch = require("node-fetch");

exports.handler = async function (event, context) {
  try {
    // Extract listingId and token from query parameters.
    const { listingId, token } = event.queryStringParameters;
    if (!listingId || !token) {
      console.error("Missing listingId or token.");
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing listingId or token" }),
      };
    }
    console.log("Received listingId:", listingId);
    console.log("Received token:", token);

    // Parse the request body for the update payload.
    let payload;
    try {
      payload = JSON.parse(event.body);
    } catch (parseError) {
      console.error("Invalid JSON in request body:", parseError);
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Invalid JSON payload" }),
      };
    }
    console.log("Update payload:", payload);

    // Retrieve CLIENT_ID from environment variables.
    const clientId = process.env.CLIENT_ID;
    if (!clientId) {
      console.error("CLIENT_ID environment variable is not set.");
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "CLIENT_ID environment variable is not set." }),
      };
    }
    console.log("Using CLIENT_ID:", clientId.slice(0, 5) + "*****");

    // Build the Etsy API endpoint URL for updating the listing.
    const updateUrl = `https://api.etsy.com/v3/application/listings/${listingId}`;
    console.log("Sending PUT request to:", updateUrl);

    // Send the PUT request to update the listing.
    const response = await fetch(updateUrl, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId,
      },
      body: JSON.stringify(payload),
    });

    console.log("Response status:", response.status);
    if (!response.ok) {
      const errorText = await response.text();
      console.error("PUT request failed:", errorText);
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: "Error updating listing", details: errorText }),
      };
    }

    const data = await response.json();
    console.log("Listing updated successfully:", data);

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