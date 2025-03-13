const fetch = require("node-fetch");

exports.handler = async function (event, context) {
  try {
    // Extract listingId and token from query parameters.
    const { listingId, token } = event.queryStringParameters;
    if (!listingId || !token) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing listingId or token" }),
      };
    }
    console.log("Received listingId:", listingId);
    console.log("Received token:", token);

    // Retrieve CLIENT_ID from environment variables.
    const clientId = process.env.CLIENT_ID;
    if (!clientId) {
      console.error("CLIENT_ID environment variable is not set.");
    } else {
      console.log("Using CLIENT_ID:", clientId.slice(0, 5) + "*****");
    }

    // Define the updated fields: title, description, and tags.
    // (Replace these placeholder values with your generated data as needed.)
    const updatedTitle = "Updated Title Placeholder";
    const updatedDescription = "Updated description placeholder.";
    const updatedTags = ["updated", "tag1", "tag2"];

    // Build the update payload. Only these fields will be updated.
    const payload = {
      title: updatedTitle,
      description: updatedDescription,
      tags: updatedTags
    };

    // Construct the Etsy API endpoint URL for updating the listing.
    const updateUrl = `https://api.etsy.com/v3/application/listings/${listingId}`;

    // Log the full endpoint URL, headers, and payload for troubleshooting.
    console.log("Update URL:", updateUrl);
    console.log("Request Headers:", {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${token}`,
      "x-api-key": clientId,
    });
    console.log("Update Payload:", JSON.stringify(payload, null, 2));

    // Make the PUT request to update the listing.
    const response = await fetch(updateUrl, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId,
      },
      body: JSON.stringify(payload),
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