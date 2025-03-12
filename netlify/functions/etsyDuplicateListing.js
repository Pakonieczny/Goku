const fetch = require("node-fetch");

// Helper function to recursively remove invalid keys (e.g., "scale_name") from an object
function removeInvalidKeys(obj) {
  if (Array.isArray(obj)) {
    return obj.map(removeInvalidKeys);
  } else if (obj !== null && typeof obj === "object") {
    const newObj = {};
    for (const key in obj) {
      // Remove keys that are not allowed in the inventory update
      if (key === "scale_name") continue;
      newObj[key] = removeInvalidKeys(obj[key]);
    }
    return newObj;
  }
  return obj;
}

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

    // Convert price: if price is an object with amount/divisor, compute the float value.
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

    // Build payload for duplicating the listing with allowed fields.
    const creationPayload = {
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
      skus: listingData.skus || [],
      style: listingData.style || [],
      has_variations: (typeof listingData.has_variations === "boolean") ? listingData.has_variations : false,
      is_customizable: (typeof listingData.is_customizable === "boolean") ? listingData.is_customizable : false,
      is_personalizable: (typeof listingData.is_personalizable === "boolean") ? listingData.is_personalizable : false
    };

    console.log("Creation payload for new listing:", creationPayload);

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
      body: JSON.stringify(creationPayload)
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

    // Now update inventory if the original listing has inventory details.
    if (listingData.inventory) {
      // Clean the inventory payload by removing invalid keys (e.g., "scale_name").
      const cleanedInventoryPayload = removeInvalidKeys(listingData.inventory);
      console.log("Cleaned inventory payload:", cleanedInventoryPayload);

      const etsyInventoryUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${newListingData.listing_id}/inventory`;

      const inventoryResponse = await fetch(etsyInventoryUrl, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${token}`,
          "x-api-key": clientId
        },
        body: JSON.stringify(cleanedInventoryPayload)
      });
      console.log("PUT inventory update response status:", inventoryResponse.status);
      if (!inventoryResponse.ok) {
        const inventoryErrorText = await inventoryResponse.text();
        console.error("Error updating inventory. PUT failed:", inventoryErrorText);
        return {
          statusCode: inventoryResponse.status,
          body: JSON.stringify({ error: "Error updating inventory", details: inventoryErrorText })
        };
      }
      const updatedInventoryData = await inventoryResponse.json();
      console.log("Inventory updated:", updatedInventoryData);
      // Optionally, merge the inventory update result with the new listing data.
      newListingData.inventory = updatedInventoryData;
    } else {
      console.log("No inventory data found in original listing; skipping inventory update.");
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