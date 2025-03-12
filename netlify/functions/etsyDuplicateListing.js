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

    // Build GET request URL to fetch original listing details.
    const etsyGetUrl = `https://api.etsy.com/v3/application/listings/${listingId}`;
    const getResponse = await fetch(etsyGetUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId,
      },
    });
    console.log("GET response status:", getResponse.status);
    if (!getResponse.ok) {
      const errorText = await getResponse.text();
      console.error("GET request failed:", errorText);
      return {
        statusCode: getResponse.status,
        body: JSON.stringify({ error: "GET request failed", details: errorText }),
      };
    }

    const listingData = await getResponse.json();
    console.log("Listing data fetched:", listingData);

    // Use a static placeholder price for the overall listing (i.e. $1.00)
    const staticPrice = 1.00;

    // Build the creation payload.
    // We keep the basic fields and include inventory if available.
    // For inventory, we transform each product's offerings to use a static placeholder price,
    // and we set properties to an empty array (skipping invalid keys).
    let inventoryPayload = null;
    if (
      listingData.inventory &&
      listingData.inventory.products &&
      Array.isArray(listingData.inventory.products) &&
      listingData.inventory.products.length > 0
    ) {
      inventoryPayload = {
        products: listingData.inventory.products.map((product) => {
          return {
            sku: product.sku || "",
            // Map each offering, setting price to a placeholder value (you may adjust as needed)
            offerings: (product.offerings || []).map((offering) => {
              return {
                // Use a static placeholder price (e.g., $1.00) for each offering.
                // Alternatively, you could compute a float value from the original price object.
                price: staticPrice,
                quantity: offering.quantity || 0,
                is_enabled: offering.is_enabled !== undefined ? offering.is_enabled : true,
              };
            }),
            // Remove the invalid keys from properties – using an empty array as a placeholder.
            properties: [],
          };
        }),
      };
    }

    const payload = {
      quantity: listingData.quantity || 1,
      title: listingData.title || "Duplicated Listing",
      description: listingData.description || "",
      price: staticPrice, // overall listing price placeholder
      who_made: listingData.who_made || "i_did",
      when_made: listingData.when_made || "made_to_order",
      taxonomy_id: listingData.taxonomy_id || 0,
      shipping_profile_id: listingData.shipping_profile_id,
      return_policy_id: listingData.return_policy_id,
      tags: listingData.tags || [],
      materials: listingData.materials || [],
      skus: listingData.skus || [],
      style: listingData.style || [],
      has_variations: listingData.has_variations || false,
      is_customizable: listingData.is_customizable || false,
      is_personalizable: listingData.is_personalizable || false,
      // Attach transformed inventory payload if available.
      inventory: inventoryPayload,
    };

    console.log("Creation payload for new listing:", payload);

    // Retrieve SHOP_ID from environment variables.
    const shopId = process.env.SHOP_ID;
    if (!shopId) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "SHOP_ID environment variable is not set." }),
      };
    }
    const etsyPostUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings`;

    // Make POST request to duplicate the listing.
    const postResponse = await fetch(etsyPostUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId,
      },
      body: JSON.stringify(payload),
    });
    console.log("POST response status:", postResponse.status);
    if (!postResponse.ok) {
      const errorText = await postResponse.text();
      console.error("Error duplicating listing. POST failed:", errorText);
      return {
        statusCode: postResponse.status,
        body: JSON.stringify({ error: "Error duplicating listing", details: errorText }),
      };
    }
    const newListingData = await postResponse.json();
    console.log("New listing created:", newListingData);

    // If you need to update the inventory separately (if the listing creation endpoint doesn't accept inventory),
    // you would call a separate Netlify function here (e.g., updateInventory(newListingData.listing_id, inventoryPayload)).
    // For now, we return the new listing data.
    return {
      statusCode: 200,
      body: JSON.stringify(newListingData),
    };
  } catch (error) {
    console.error("Exception in etsyDuplicateListing:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
};