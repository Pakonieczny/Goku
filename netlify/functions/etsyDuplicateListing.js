const fetch = require("node-fetch");

// Helper function to compute price from an object, or default to placeholder if invalid.
function computePrice(price) {
  if (Array.isArray(price)) {
    // If price is an array, we return the static placeholder.
    return 1.00;
  }
  if (price && typeof price === "object" && price.amount && price.divisor) {
    return parseFloat((price.amount / price.divisor).toFixed(2));
  }
  if (price) {
    let parsed = parseFloat(price);
    return isNaN(parsed) ? 1.00 : parsed;
  }
  return 1.00; // fallback placeholder
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

    // Retrieve CLIENT_ID and SHOP_ID from environment variables.
    const clientId = process.env.CLIENT_ID;
    const shopId = process.env.SHOP_ID;
    if (!clientId) {
      return { statusCode: 500, body: JSON.stringify({ error: "CLIENT_ID env variable not set" }) };
    }
    if (!shopId) {
      return { statusCode: 500, body: JSON.stringify({ error: "SHOP_ID env variable not set" }) };
    }
    console.log("Using CLIENT_ID:", clientId.slice(0, 5) + "*****");
    console.log("Using SHOP_ID:", shopId);

    // Step 1: Fetch the original listing details.
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
    const originalListing = await getResponse.json();
    console.log("Original listing data fetched:", originalListing);

    // Step 1b: If inventory is not part of the listing, try fetching it separately.
    let inventoryData = originalListing.inventory;
    if (!inventoryData || !inventoryData.products) {
      console.log("No inventory data in listing response; attempting separate fetch...");
      const inventoryUrl = `https://api.etsy.com/v3/application/listings/${listingId}/inventory`;
      const inventoryResponse = await fetch(inventoryUrl, {
        method: "GET",
        headers: {
          "Authorization": `Bearer ${token}`,
          "x-api-key": clientId
        }
      });
      console.log("Inventory GET response status:", inventoryResponse.status);
      if (inventoryResponse.ok) {
        inventoryData = await inventoryResponse.json();
        console.log("Fetched inventory data:", inventoryData);
      } else {
        console.warn("Unable to fetch inventory data:", await inventoryResponse.text());
      }
    }

    // Step 2: Create the new listing with a static price placeholder.
    const creationPayload = {
      quantity: originalListing.quantity || 1,
      title: originalListing.title || "Duplicated Listing",
      description: originalListing.description || "",
      price: 1.00,  // Static placeholder – variations will define actual prices.
      who_made: originalListing.who_made || "i_did",
      when_made: originalListing.when_made || "made_to_order",
      taxonomy_id: originalListing.taxonomy_id || 0,
      shipping_profile_id: originalListing.shipping_profile_id,
      return_policy_id: originalListing.return_policy_id,
      tags: originalListing.tags || [],
      materials: originalListing.materials || []
      // Note: Inventory/variation details are not included in this payload.
    };

    console.log("Creation payload for new listing:", creationPayload);

    const etsyPostUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings`;
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
    const newListing = await postResponse.json();
    console.log("New listing created:", newListing);
    const newListingId = newListing.listing_id;
    if (!newListingId) {
      return { statusCode: 500, body: JSON.stringify({ error: "New listing ID not returned" }) };
    }

    // Step 3: If inventory data is available, transform and update inventory for the new listing.
    if (!inventoryData || !inventoryData.products) {
      console.log("No inventory data available to update.");
      return { statusCode: 200, body: JSON.stringify(newListing) };
    }

    // Transform inventory data.
    let transformedProducts = [];
    for (let product of inventoryData.products) {
      let newProduct = {
        sku: product.sku || "",
        offerings: []
      };

      if (product.offerings && Array.isArray(product.offerings)) {
        for (let offering of product.offerings) {
          // Compute price: if offering.price is an array, default to static placeholder.
          let priceValue = computePrice(offering.price);
          newProduct.offerings.push({
            price: priceValue,
            quantity: offering.quantity || 0,
            is_enabled: offering.is_enabled !== false
          });
        }
      }

      // Map property values from the original listing.
      if (product.property_values && Array.isArray(product.property_values)) {
        newProduct.property_values = [];
        for (let prop of product.property_values) {
          if (prop.property_name && prop.values && prop.values.length > 0) {
            newProduct.property_values.push({
              property_id: prop.property_id,
              value_id: (prop.value_ids && prop.value_ids.length > 0) ? prop.value_ids[0] : null,
              name: prop.property_name,
              value: prop.values[0]
            });
          }
        }
      }
      transformedProducts.push(newProduct);
    }
    const inventoryPayload = { products: transformedProducts };
    console.log("Transformed inventory payload:", JSON.stringify(inventoryPayload));

    // Step 4: Update the inventory for the new listing.
    const newInventoryUrl = `https://api.etsy.com/v3/application/listings/${newListingId}/inventory`;
    const updateResponse = await fetch(newInventoryUrl, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId
      },
      body: JSON.stringify(inventoryPayload)
    });
    console.log("Inventory update response status:", updateResponse.status);
    if (!updateResponse.ok) {
      const errorText = await updateResponse.text();
      console.error("Error updating inventory with PUT:", errorText);
      return {
        statusCode: updateResponse.status,
        body: JSON.stringify({ error: "Error updating inventory", details: errorText })
      };
    }
    const inventoryUpdateResult = await updateResponse.json();
    console.log("Inventory update result:", inventoryUpdateResult);

    // Return the new listing details along with the inventory update result.
    return {
      statusCode: 200,
      body: JSON.stringify({ newListing, inventory: inventoryUpdateResult })
    };
  } catch (error) {
    console.error("Exception in etsyDuplicateListing:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};