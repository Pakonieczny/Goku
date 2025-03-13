const fetch = require("node-fetch");

exports.handler = async function (event, context) {
  try {
    // Expect a JSON payload with listingId, title, description, tags, token.
    const body = JSON.parse(event.body);
    const { listingId, title, description, tags, token } = body;
    if (!listingId || !token || !title || !description || !tags) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing required fields: listingId, title, description, tags, or token" }),
      };
    }
    
    const clientId = process.env.CLIENT_ID;
    if (!clientId) {
      console.error("CLIENT_ID environment variable is not set.");
    } else {
      console.log("Using CLIENT_ID:", clientId.slice(0, 5) + "*****");
    }
    
    // Build the payload for updating the listing.
    const payload = {
      title,
      description,
      tags
    };
    
    const updateUrl = `https://api.etsy.com/v3/application/listings/${listingId}`;
    console.log("Updating listing at URL:", updateUrl);
    console.log("Update payload:", payload);
    
    const response = await fetch(updateUrl, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId,
      },
      body: JSON.stringify(payload)
    });
    
    console.log("Update listing response status:", response.status);
    if (!response.ok) {
      const errorText = await response.text();
      console.error("Error updating listing. PUT failed:", errorText);
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: "Error updating listing", details: errorText }),
      };
    }
    
    const updatedListing = await response.json();
    console.log("Listing updated successfully:", updatedListing);
    return {
      statusCode: 200,
      body: JSON.stringify(updatedListing),
    };
  } catch (error) {
    console.error("Exception in updateListing function:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
};