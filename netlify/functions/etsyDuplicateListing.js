const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  try {
    // Extract listingId and token from query parameters.
    const { listingId, token } = event.queryStringParameters;
    if (!listingId || !token) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing listingId or token parameter." })
      };
    }
    
    // Get the SHOP_ID from environment variables.
    const SHOP_ID = process.env.SHOP_ID;
    if (!SHOP_ID) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "SHOP_ID is not configured in environment variables." })
      };
    }
    
    // Step 1: Fetch the original listing details from Etsy.
    const listingUrl = `https://api.etsy.com/v3/application/listings/${listingId}`;
    const getResponse = await fetch(listingUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${token}`
      }
    });
    
    if (!getResponse.ok) {
      const errorText = await getResponse.text();
      return {
        statusCode: getResponse.status,
        body: JSON.stringify({ error: `Error fetching listing details: ${errorText}` })
      };
    }
    
    const listingData = await getResponse.json();
    
    // Step 2: Build the payload for the new listing.
    const payload = {
      quantity: listingData.quantity || 1,
      title: listingData.title || "Duplicated Listing",
      description: listingData.description || "",
      price: listingData.price || 0,
      who_made: listingData.who_made || "i_did",
      when_made: listingData.when_made || "made_to_order",
      taxonomy_id: listingData.taxonomy_id || 0
    };
    
    // Prepare URL-encoded body.
    const urlParams = new URLSearchParams();
    for (const key in payload) {
      urlParams.append(key, payload[key]);
    }
    
    // Step 3: Create the new (duplicated) listing via POST.
    const createUrl = `https://api.etsy.com/v3/application/shops/${SHOP_ID}/listings`;
    const postResponse = await fetch(createUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": `Bearer ${token}`
      },
      body: urlParams.toString()
    });
    
    if (!postResponse.ok) {
      const errorText = await postResponse.text();
      return {
        statusCode: postResponse.status,
        body: JSON.stringify({ error: `Error duplicating listing: ${errorText}` })
      };
    }
    
    const newListingData = await postResponse.json();
    
    return {
      statusCode: 200,
      body: JSON.stringify(newListingData)
    };
    
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};