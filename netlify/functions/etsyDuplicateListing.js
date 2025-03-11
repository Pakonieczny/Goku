const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  try {
    // Extract query parameters from the incoming request
    const { listingId, token } = event.queryStringParameters;
    if (!listingId || !token) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing listingId or token" })
      };
    }

    console.log("Received listingId:", listingId);
    console.log("Received token:", token);

    const apiKey = process.env.CLIENT_ID;
    if (!apiKey) {
      console.error("CLIENT_ID environment variable is not set.");
    } else {
      console.log("Using CLIENT_ID:", apiKey.slice(0, 5) + "*****");
    }

    const etsyApiUrl = `https://api.etsy.com/v3/application/listings/${listingId}`;
    const getResponse = await fetch(etsyApiUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${token}`,
        "x-api-key": apiKey
      }
    });

    console.log("GET response status:", getResponse.status);
    if (!getResponse.ok) {
      const errorText = await getResponse.text();
      console.error("GET request failed:", errorText);
      return {
        statusCode: getResponse.status,
        body: JSON.stringify({ error: "GET request failed", details: errorText })
      };
    }

    const listingData = await getResponse.json();
    console.log("Listing data fetched:", listingData);

    // Ensure that the price is formatted as a number with at least one decimal.
    let priceNumber = listingData.price ? parseFloat(listingData.price) : 0;
    let formattedPrice = priceNumber.toFixed(2); // e.g., "12.00"
    
    const payload = {
      quantity: listingData.quantity || 1,
      title: listingData.title || "Duplicated Listing",
      description: listingData.description || "",
      price: formattedPrice, // This will now be a string like "12.00"
      who_made: listingData.who_made || "i_did",
      when_made: listingData.when_made || "made_to_order",
      taxonomy_id: listingData.taxonomy_id || 0
    };

    console.log("Payload for new listing:", payload);

    const postUrl = `https://api.etsy.com/v3/application/shops/${process.env.SHOP_ID}/listings`;

    // Option 1: Send as application/x-www-form-urlencoded
    // const urlEncodedBody = new URLSearchParams(payload);

    // Option 2 (alternative): Send as JSON
    const jsonBody = JSON.stringify(payload);

    // Choose one approach. Here we try the URL-encoded approach:
    const postResponse = await fetch(postUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded", // Change to "application/json" if needed
        "Authorization": `Bearer ${token}`,
        "x-api-key": apiKey
      },
      body: urlEncodedBody // Or jsonBody if using JSON
    });

    console.log("POST response status:", postResponse.status);
    if (!postResponse.ok) {
      const errorText = await postResponse.text();
      console.error("Error duplicating listing. POST failed:", errorText);
      return {
        statusCode: postResponse.status,
        body: JSON.stringify({ error: "Error duplicating listing", details: errorText })
      };
    }

    const newListingData = await postResponse.json();
    console.log("New listing created:", newListingData);
    return {
      statusCode: 200,
      body: JSON.stringify(newListingData)
    };
  } catch (error) {
    console.error("Exception in etsyDuplicateListing:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};
