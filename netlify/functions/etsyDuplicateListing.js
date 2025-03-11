const fetch = require('node-fetch');

exports.handler = async function(event, context) {
  try {
    const listingId = event.queryStringParameters.listingId;
    const token = event.queryStringParameters.token;
    if (!listingId || !token) {
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
      return { statusCode: getResponse.status, body: errorText };
    }
    const listingData = await getResponse.json();
    // Build payload for duplicating listing
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
      return { statusCode: postResponse.status, body: errorText };
    }
    const newListing = await postResponse.json();
    return {
      statusCode: 200,
      body: JSON.stringify(newListing)
    };
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};