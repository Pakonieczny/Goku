const fetch = require("node-fetch");

// Helper function to compute price as a float if price is provided as an object.
function computePrice(price) {
  if (price && typeof price === "object" && price.amount && price.divisor) {
    return parseFloat((price.amount / price.divisor).toFixed(2));
  }
  if (price) {
    return parseFloat(price);
  }
  return 1.00; // fallback static placeholder
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
    console.log("Using CLIENT_ID:", clientId.slice(0,5) + "*****");
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

    // Step 2: Create the new listing.
    // Use a static placeholder for price (1.00) since variations will determine the actual prices.
    const creationPayload = {
      quantity: originalListing.quantity || 1,
      title: originalListing.title || "Duplicated Listing",
      description: originalListing.description || "",
      price: 1.00,  // static placeholder
      who_made: originalListing.who_made || "i_did",
      when_made: originalListing.when_made || "made_to_order",
      taxonomy_id: originalListing.taxonomy_id || 0,
      shipping_profile_id: originalListing.shipping_profile_id, // required for physical listings
      return_policy_id: originalListing.return_policy_id,       // required for physical listings
      tags: originalListing.tags || [],
      materials: originalListing.materials || []
      // Note: Inventory data (variations, SKUs, etc.) is not included here.
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

    // Step 3: Update the inventory (variations, SKUs, etc.) for the new listing.
    // The inventory update is done via a separate call.
    // (If original listing inventory data is not included in the GET response,
    // you may need to fetch it from the inventory endpoint of the original listing.)
    if (!originalListing.inventory || !originalListing.inventory.products) {
      console.log("No inventory data available in original listing.");
      return { statusCode: 200, body: JSON.stringify(newListing) };
    }

    // Transform the original inventory data to the format expected by Etsy.
    let transformedProducts = [];
    for (let product of originalListing.inventory.products) {
      let newProduct = {
        sku: product.sku || "",
        offerings: [],
        properties: []
      };
      if (product.offerings && Array.isArray(product.offerings)) {
        for (let offering of product.offerings) {
          // Compute a float price from offering.price
          let priceValue = computePrice(offering.price);
          // Ensure that priceValue is a float and meets the minimum threshold (e.g., $0.20)
          if (isNaN(priceValue) || priceValue < 0.20) {
            priceValue = 1.00; // fallback static price if computed price is invalid
          }
          newProduct.offerings.push({
            price: priceValue,
            quantity: offering.quantity || 0,
            is_enabled: offering.is_enabled !== false
          });
        }
      }
      if (product.property_values && Array.isArray(product.property_values)) {
        for (let prop of product.property_values) {
          if (prop.property_name && prop.values && prop.values.length > 0) {
            newProduct.properties.push({
              name: prop.property_name,
              value: prop.values[0] // take the first value
            });
          }
        }
      }
      transformedProducts.push(newProduct);
    }
    const inventoryPayload = { products: transformedProducts };
    console.log("Transformed inventory payload:", inventoryPayload);

    // Now, update the inventory for the new listing.
    // The endpoint is: PUT https://api.etsy.com/v3/application/listings/{newListingId}/inventory
    const inventoryUrl = `https://api.etsy.com/v3/application/listings/${newListingId}/inventory`;
    const updateResponse = await fetch(inventoryUrl, {
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