const fetch = require('node-fetch');

exports.handler = async function(event, context) {
  try {
    const listingId = event.queryStringParameters.listingId;
    const token = event.queryStringParameters.token;
    if (!listingId || !token) {
      console.error("Missing listingId or token", { listingId, token });
      return { statusCode: 400, body: JSON.stringify({ error: "Missing listingId or token" }) };
    }
    // Fetch listing details from Etsy
    const url = `https://api.etsy.com/v3/application/listings/${listingId}`;
    const getResponse = await fetch(url, {
      method: 'GET',
      headers: { "Authorization": `Bearer ${token}` }
    });
    if (!getResponse.ok) {
      const errorText = await getResponse.text();
      console.error("GET request failed", { status: getResponse.status, errorText });
      return { statusCode: getResponse.status, body: JSON.stringify({ error: "GET request failed", details: errorText }) };
    }
    const listingData = await getResponse.json();
    console.log("Fetched listing data:", listingData);
    
    // Build payload for duplicating the listing
    const payload = {
      quantity: listingData.quantity || 1,
      title: listingData.title || "Duplicated Listing",
      description: listingData.description || "",
      price: listingData.price || 0,
      who_made: listingData.who_made || "i_did",
      when_made: listingData.when_made || "made_to_order",
      taxonomy_id: listingData.taxonomy_id || 0
    };
    const shopId = process.env.SHOP_ID;
    const postUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings`;
    const postResponse = await fetch(postUrl, {
      method: 'POST',
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": `Bearer ${token}`
      },
      body: new URLSearchParams(payload)
    });
    if (!postResponse.ok) {
      const errorText = await postResponse.text();
      console.error("POST request failed", { status: postResponse.status, errorText });
      return { statusCode: postResponse.status, body: JSON.stringify({ error: "POST request failed", details: errorText }) };
    }
    const newListing = await postResponse.json();
    console.log("New listing created successfully:", newListing);
    return {
      statusCode: 200,
      body: JSON.stringify(newListing)
    };
  } catch (error) {
    console.error("Unexpected error in etsyDuplicateListing:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};