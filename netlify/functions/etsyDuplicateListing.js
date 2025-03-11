const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  try {
    // Extract listingId and token from query parameters.
    const { listingId, token } = event.queryStringParameters;
    if (!listingId || !token) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing listingId or token" })
      };
    }
    console.log("Received listingId:", listingId);
    console.log("Received token:", token);

    // Retrieve CLIENT_ID from environment variables; used as x-api-key.
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
        "x-api-key": clientId
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

    // Compute price value: if price is an object with amount/divisor, compute the float.
    let priceValue;
    if (listingData.price && typeof listingData.price === "object" &&
        listingData.price.amount && listingData.price.divisor) {
      priceValue = listingData.price.amount / listingData.price.divisor;
    } else if (listingData.price == null || listingData.price === "") {
      priceValue = 0.00;
    } else {
      priceValue = parseFloat(listingData.price);
    }
    const formattedPrice = parseFloat(priceValue.toFixed(2));

    // Build the initial payload using available fields.
    const payload = {
      quantity: listingData.quantity || 1,
      title: listingData.title || "Duplicated Listing",
      description: listingData.description || "",
      price: formattedPrice,
      who_made: listingData.who_made || "i_did",
      when_made: listingData.when_made || "made_to_order",
      taxonomy_id: listingData.taxonomy_id || 0,
      shipping_profile_id: listingData.shipping_profile_id, // Required for physical listings.
      return_policy_id: listingData.return_policy_id,       // Required.
      tags: listingData.tags || [],
      materials: listingData.materials || [],
      // Initially set skus and variations from listingData (if any)
      skus: listingData.skus || [],
      style: listingData.style || [],
      has_variations: (typeof listingData.has_variations === "boolean") ? listingData.has_variations : false,
      is_customizable: (typeof listingData.is_customizable === "boolean") ? listingData.is_customizable : false,
      is_personalizable: (typeof listingData.is_personalizable === "boolean") ? listingData.is_personalizable : false
    };

    // Variable to hold inventory data (if variations exist)
    let inventoryPayloadData = null;
    if (payload.has_variations) {
      const inventoryUrl = `https://api.etsy.com/v3/application/listings/${listingId}/inventory`;
      const inventoryResponse = await fetch(inventoryUrl, {
        method: "GET",
        headers: {
          "Authorization": `Bearer ${token}`,
          "x-api-key": clientId
        }
      });
      if (!inventoryResponse.ok) {
        const invErrorText = await inventoryResponse.text();
        console.error("Error fetching inventory:", invErrorText);
      } else {
        const inventoryData = await inventoryResponse.json();
        console.log("Inventory data fetched:", inventoryData);
        if (inventoryData.inventory) {
          // Merge SKU and variations details.
          payload.skus = inventoryData.inventory.skus || listingData.skus || [];
          payload.variations = inventoryData.inventory.variations || listingData.variations || [];
          // Optionally, you can include products if needed.
          inventoryPayloadData = inventoryData.inventory;
        }
      }
    }

    console.log("Payload for new listing:", payload);

    // Retrieve SHOP_ID from environment variables.
    const shopId = process.env.SHOP_ID;
    if (!shopId) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "SHOP_ID environment variable is not set." })
      };
    }
    const etsyPostUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings`;

    // Make POST request to duplicate the listing.
    const postResponse = await fetch(etsyPostUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId
      },
      body: JSON.stringify(payload)
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

    // If the listing had variations and we retrieved inventory data, update the new listing's inventory.
    if (payload.has_variations && inventoryPayloadData && newListingData.listing_id) {
      const newListingId = newListingData.listing_id;
      // Build an inventory payload; adjust as needed based on your original inventory data.
      const inventoryPayload = {
        products: inventoryPayloadData.products || [],
        variations: inventoryPayloadData.variations || [],
        skus: payload.skus  // Using the SKUs we got earlier.
      };
      const inventoryUpdateUrl = `https://api.etsy.com/v3/application/listings/${newListingId}/inventory`;
      console.log("Updating inventory for duplicated listing using payload:", inventoryPayload);
      const inventoryUpdateResponse = await fetch(inventoryUpdateUrl, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${token}`,
          "x-api-key": clientId
        },
        body: JSON.stringify(inventoryPayload)
      });
      console.log("Inventory update response status:", inventoryUpdateResponse.status);
      if (!inventoryUpdateResponse.ok) {
        const invErrorText = await inventoryUpdateResponse.text();
        console.error("Error updating inventory for duplicated listing:", invErrorText);
        // Note: You might choose to return an error here or simply log it.
      } else {
        console.log("Inventory updated successfully for duplicated listing.");
      }
    }

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