const fetch = require("node-fetch");

exports.handler = async function (event, context) {
  try {
    // Extract required parameters from query string
    const { listingId, token, title, description, tags } = event.queryStringParameters;
    if (!listingId || !token || !title || !description || !tags) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing listingId, token, title, description, or tags" }),
      };
    }
    console.log("Received listingId:", listingId);
    console.log("Received token:", token);

    // Retrieve CLIENT_ID and SHOP_ID from environment variables.
    const clientId = process.env.CLIENT_ID;
    const shopId = process.env.SHOP_ID;
    if (!clientId) {
      console.error("CLIENT_ID environment variable is not set.");
    } else {
      console.log("Using CLIENT_ID:", clientId.slice(0, 5) + "*****");
    }
    if (!shopId) {
      console.error("SHOP_ID environment variable is not set.");
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "SHOP_ID environment variable is not set." }),
      };
    }

    // Build the update payload using the actual generated data.
    const updatePayload = {
      title: title,
      description: description,
      tags: tags.split(",").map(tag => tag.trim())
    };

    // Construct the Etsy API endpoint URL for updating the listing.
    // Etsy expects a PUT request to the endpoint with shop ID included:
    // PUT https://api.etsy.com/v3/application/shops/{shopId}/listings/{listingId}
    const updateUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${listingId}`;
    console.log("Update URL:", updateUrl);
    console.log("Request Headers:", {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${token}`,
      "x-api-key": clientId,
    });
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