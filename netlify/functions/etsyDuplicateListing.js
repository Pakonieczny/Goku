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

    // Retrieve the API key from environment variables
    const apiKey = process.env.CLIENT_ID;
    if (!apiKey) {
      console.error("CLIENT_ID environment variable is not set.");
    } else {
      console.log("Using CLIENT_ID:", apiKey.slice(0, 5) + "*****");
    }

    // Build the Etsy API URL for fetching listing details
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

    // Handle the price: if null, undefined, or empty string, set to 0.00
    let priceValue = listingData.price;
    if (priceValue === null || priceValue === undefined || priceValue === "") {
      priceValue = 0.00;
    } else {
      priceValue = parseFloat(priceValue);
    }
    let formattedPrice = parseFloat(priceValue.toFixed(2));
    
    // Build the payload for the new listing
    const payload = {
      quantity: listingData.quantity || 1,
      title: listingData.title || "Duplicated Listing",
      description: listingData.description || "",
      price: formattedPrice, // Always a float value
      who_made: listingData.who_made || "i_did",
      when_made: listingData.when_made || "made_to_order",
      taxonomy_id: listingData.taxonomy_id || 0
    };

    console.log("Payload for new listing:", payload);

    // Build the Etsy API URL for creating a new listing using SHOP_ID from environment
    const postUrl = `https://api.etsy.com/v3/application/shops/${process.env.SHOP_ID}/listings`;

    // Send the payload as a JSON body
    const jsonBody = JSON.stringify(payload);

    const postResponse = await fetch(postUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
        "x-api-key": apiKey
      },
      body: jsonBody
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