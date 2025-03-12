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

    // Determine price for the top-level listing:
    // If there are variations, we set a default minimal price.
    let formattedPrice;
    if (listingData.has_variations) {
      formattedPrice = 0.20;
      console.log("Listing has variations; setting default top-level price:", formattedPrice);
    } else {
      if (listingData.price && typeof listingData.price === "object" &&
          listingData.price.amount && listingData.price.divisor) {
        formattedPrice = parseFloat((listingData.price.amount / listingData.price.divisor).toFixed(2));
      } else if (listingData.price == null || listingData.price === "") {
        formattedPrice = 0.20;
      } else {
        formattedPrice = parseFloat(listingData.price);
      }
      console.log("Computed price for duplicate (no variations):", formattedPrice);
    }

    // Build inventory payload if available.
    let newInventory = null;
    if (listingData.inventory && Array.isArray(listingData.inventory)) {
      // Map over each variation.
      newInventory = listingData.inventory.map(variation => {
        // Ensure the variation price is a float.
        let variationPrice = variation.price;
        if (typeof variationPrice === "object" && variationPrice.amount && variationPrice.divisor) {
          variationPrice = parseFloat((variationPrice.amount / variationPrice.divisor).toFixed(2));
        } else {
          variationPrice = parseFloat(variationPrice);
        }
        // Build a new variation object that omits disallowed keys.
        // Here we assume allowed keys: sku, price, quantity, is_enabled (optional), and properties.
        return {
          sku: variation.sku || "",
          price: variationPrice,
          quantity: variation.quantity || 0,
          // If variation has a flag for enabled/disabled, include it.
          is_enabled: typeof variation.is_enabled === "boolean" ? variation.is_enabled : true,
          // Copy the properties array as-is (assuming it is structured correctly).
          properties: variation.properties || []
        };
      });
      console.log("New inventory payload constructed:", newInventory);
    }

    // Build payload for duplicating the listing.
    const payload = {
      quantity: listingData.quantity || 1,
      title: listingData.title || "Duplicated Listing",
      description: listingData.description || "",
      price: formattedPrice,
      who_made: listingData.who_made || "i_did",
      when_made: listingData.when_made || "made_to_order",
      taxonomy_id: listingData.taxonomy_id || 0,
      shipping_profile_id: listingData.shipping_profile_id, // Must be present for physical listings.
      return_policy_id: listingData.return_policy_id,       // Must be present.
      tags: listingData.tags || [],
      materials: listingData.materials || [],
      skus: listingData.skus || [],
      style: listingData.style || [],
      has_variations: listingData.has_variations || false,
      is_customizable: listingData.is_customizable || false,
      is_personalizable: listingData.is_personalizable || false,
      processing_min: listingData.processing_min || null,
      processing_max: listingData.processing_max || null
      // Note: We'll add the inventory in a separate step if needed.
    };

    console.log("Creation payload for new listing:", payload);

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

    // If inventory data (variations) is available, attempt a separate inventory update call.
    if (newInventory) {
      // Use a separate endpoint for updating inventory.
      const listingIdNew = newListingData.listing_id;
      const etsyInventoryUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${listingIdNew}/inventory`;
      // Build the inventory update payload.
      const inventoryPayload = {
        products: newInventory // 'products' is the expected key for the array of variations.
      };

      console.log("Attempting inventory update for listing:", listingIdNew);
      let inventoryResponse = await fetch(etsyInventoryUrl, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${token}`,
          "x-api-key": clientId
        },
        body: JSON.stringify(inventoryPayload)
      });
      console.log("Inventory update response status:", inventoryResponse.status);
      if (!inventoryResponse.ok) {
        const invErrorText = await inventoryResponse.text();
        console.error("Inventory update failed:", invErrorText);
        // Optionally, you can implement retry logic here.
      } else {
        const updatedInventory = await inventoryResponse.json();
        console.log("Inventory updated successfully:", updatedInventory);
      }
    } else {
      console.log("No inventory data available to update.");
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