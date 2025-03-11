// etsyDuplicateListing.js
const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  try {
    // Expect listingId to be passed as a query parameter and accessToken as well.
    const { listingId, accessToken } = event.queryStringParameters;
    if (!listingId || !accessToken) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing listingId or accessToken" })
      };
    }
    console.log("Received listingId:", listingId);
    console.log("Received token:", accessToken);

    // Fetch the original listing details from Etsy
    const getUrl = `https://api.etsy.com/v3/application/listings/${listingId}`;
    const getResponse = await fetch(getUrl, {
      method: "GET",
      headers: { "Authorization": `Bearer ${accessToken}` }
    });
    if (!getResponse.ok) {
      const errorText = await getResponse.text();
      console.error("Error fetching listing details:", errorText);
      return { statusCode: getResponse.status, body: errorText };
    }
    const listingData = await getResponse.json();
    console.log("Fetched listing data:", listingData);

    // Convert price from object to float (if present)
    let priceValue = null;
    if (listingData.price && listingData.price.amount && listingData.price.divisor) {
      priceValue = listingData.price.amount / listingData.price.divisor;
    } else {
      console.warn("Price object not found or incomplete in listing data.");
    }

    // Build payload with as many fields as allowed
    // (Fields not accepted by the API when creating a listing will be ignored.)
    const payload = {
      quantity: listingData.quantity || 1,
      title: listingData.title || "Duplicated Listing",
      description: listingData.description || "",
      price: priceValue, // must be a float
      who_made: listingData.who_made || "i_did",
      when_made: listingData.when_made || "made_to_order",
      taxonomy_id: listingData.taxonomy_id || 0,
      shipping_profile_id: listingData.shipping_profile_id, // required for physical listings
      return_policy_id: listingData.return_policy_id,
      // For arrays, if not present, default to empty array
      tags: listingData.tags ? listingData.tags : [],
      materials: listingData.materials ? listingData.materials : [],
      skus: listingData.skus ? listingData.skus : [],
      style: listingData.style ? listingData.style : [],
      // Boolean flags – default to false if not present
      has_variations: listingData.has_variations || false,
      is_customizable: listingData.is_customizable || false,
      is_personalizable: listingData.is_personalizable || false
    };

    // Convert the payload to URL-encoded form for POST
    // For array values, join them with commas.
    const urlEncodedBody = new URLSearchParams();
    for (const key in payload) {
      let value = payload[key];
      if (Array.isArray(value)) {
        value = value.join(",");
      } else if (value === null || value === undefined) {
        value = "";
      }
      urlEncodedBody.append(key, value);
    }
    console.log("Constructed payload:", payload);

    // Build the POST URL. (Ensure that SHOP_ID is set in your environment variables.)
    const shopId = process.env.SHOP_ID;
    if (!shopId) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "SHOP_ID is not defined in environment variables." })
      };
    }
    const postUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings`;
    const postResponse = await fetch(postUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": `Bearer ${accessToken}`
      },
      body: urlEncodedBody.toString()
    });
    if (!postResponse.ok) {
      const errorText = await postResponse.text();
      console.error("Error duplicating listing. POST failed:", errorText);
      return { statusCode: postResponse.status, body: errorText };
    }
    const newListingData = await postResponse.json();
    console.log("Duplicated listing data:", newListingData);
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