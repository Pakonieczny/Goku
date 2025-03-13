const fetch = require("node-fetch");

exports.handler = async function (event, context) {
  try {
    // Log the entire query string parameters for debugging
    console.log("Received query string parameters:", event.queryStringParameters);

    // Extract required parameters from the query string
    const { listingId, token, title, description, tags } = event.queryStringParameters || {};
    if (!listingId || !token || !title || !description || !tags) {
      console.error("Missing one or more required parameters.");
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing listingId, token, title, description, or tags" }),
      };
    }

    // Retrieve CLIENT_ID and SHOP_ID from environment variables
    const clientId = process.env.CLIENT_ID;
    const shopId = process.env.SHOP_ID;
    if (!clientId || !shopId) {
      console.error("CLIENT_ID or SHOP_ID environment variable is not set.");
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "CLIENT_ID or SHOP_ID environment variable is not set." }),
      };
    }
    console.log("Using CLIENT_ID:", clientId.slice(0, 5) + "*****");
    console.log("Using SHOP_ID:", shopId);

    // Build the update payload using the generated data
    // Note: We're updating only title, description, and tags. All other fields remain unchanged.
    const updatePayload = {
      title: title,
      description: description,
      tags: tags.split(",").map(tag => tag.trim())
    };

    // Construct the Etsy API endpoint URL for updating the listing.
    // Etsy expects a PUT request to: /v3/application/shops/{shopId}/listings/{listingId}
    const updateUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${listingId}`;
    console.log("Update URL:", updateUrl);
    console.log("Update Payload:", JSON.stringify(updatePayload, null, 2));

    // Make the PUT request to update the listing.
    const response = await fetch(updateUrl, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId,
      },
      body: JSON.stringify(updatePayload),
    });
    console.log("PUT update response status:", response.status);
    if (!response.ok) {
      const errorText = await response.text();
      console.error("Error updating listing. PUT failed:", errorText);
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: "Error updating listing", details: errorText }),
      };
    }
    const updatedListingData = await response.json();
    console.log("Listing updated successfully:", updatedListingData);

    return {
      statusCode: 200,
      body: JSON.stringify(updatedListingData),
    };
  } catch (error) {
    console.error("Exception in updateListing handler:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
};