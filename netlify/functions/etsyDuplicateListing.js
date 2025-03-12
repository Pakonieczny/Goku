const fetch = require("node-fetch");

// Helper: delay in milliseconds
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Fetch the original listing’s inventory.
 */
async function fetchOriginalInventory(listingId, token, clientId) {
  const inventoryUrl = `https://api.etsy.com/v3/application/listings/${listingId}/inventory`;
  const invResponse = await fetch(inventoryUrl, {
    method: "GET",
    headers: {
      "Authorization": `Bearer ${token}`,
      "x-api-key": clientId
    }
  });
  if (!invResponse.ok) {
    const errorText = await invResponse.text();
    console.error("Failed to fetch original inventory:", invResponse.status, errorText);
    return null;
  }
  const invData = await invResponse.json();
  console.log("Fetched original inventory data:", JSON.stringify(invData));
  return invData;
}

/**
 * Transform the fetched inventory data to include only allowed keys.
 * Each product’s offerings will have a float price.
 */
function transformInventory(invData) {
  if (!invData || !Array.isArray(invData.products)) return null;
  const newProducts = invData.products.map(prod => {
    const newOfferings = Array.isArray(prod.offerings)
      ? prod.offerings.map(offering => {
          let offeringPrice;
          if (
            offering.price &&
            typeof offering.price === "object" &&
            offering.price.amount &&
            offering.price.divisor
          ) {
            offeringPrice = parseFloat((offering.price.amount / offering.price.divisor).toFixed(2));
          } else if (Array.isArray(offering.price)) {
            offeringPrice = parseFloat(offering.price[0]);
          } else {
            offeringPrice = parseFloat(offering.price);
          }
          // Return only allowed keys. Do not include disallowed keys like offering_id.
          return {
            price: isNaN(offeringPrice) ? 0.20 : offeringPrice, // Must be above min price (e.g. $0.20)
            quantity: offering.quantity || 0,
            is_enabled: (typeof offering.is_enabled === "boolean") ? offering.is_enabled : true
          };
        })
      : [];
    return {
      sku: prod.sku || "",
      offerings: newOfferings,
      properties: prod.property_values || [] // Map property_values to properties (if needed)
    };
  });
  return { products: newProducts };
}

/**
 * Attempt to update the inventory of the new listing using a PUT request.
 * If the PUT returns 404, try a POST request to create the inventory resource.
 */
async function updateInventoryResource(shopId, newListingId, token, clientId, inventoryPayload) {
  const inventoryEndpoint = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${newListingId}/inventory`;

  // First try PUT update
  console.log("Attempting PUT inventory update...");
  let response = await fetch(inventoryEndpoint, {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${token}`,
      "x-api-key": clientId
    },
    body: JSON.stringify(inventoryPayload)
  });
  console.log("PUT inventory update response status:", response.status);
  if (response.ok) {
    const data = await response.json();
    console.log("Inventory updated successfully via PUT:", data);
    return data;
  }
  
  // If PUT returns 404, try POST to create the inventory resource.
  if (response.status === 404) {
    console.warn("PUT returned 404. Inventory resource not found. Waiting 5 seconds before trying POST...");
    await delay(5000);
    console.log("Attempting POST inventory creation...");
    response = await fetch(inventoryEndpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId
      },
      body: JSON.stringify(inventoryPayload)
    });
    console.log("POST inventory creation response status:", response.status);
    if (response.ok) {
      const data = await response.json();
      console.log("Inventory created successfully via POST:", data);
      return data;
    } else {
      const errorText = await response.text();
      console.error("POST inventory creation failed:", errorText);
      throw new Error(`Inventory creation failed: ${response.status} - ${errorText}`);
    }
  }
  
  // For other errors, log and throw.
  const errorText = await response.text();
  console.error("Inventory update failed:", errorText);
  throw new Error(`Inventory update failed: ${response.status} - ${errorText}`);
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

    // Retrieve CLIENT_ID from environment variables.
    const clientId = process.env.CLIENT_ID;
    if (!clientId) {
      console.error("CLIENT_ID environment variable is not set.");
    } else {
      console.log("Using CLIENT_ID:", clientId.slice(0, 5) + "*****");
    }

    // Fetch the original listing details.
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
    console.log("Listing data fetched:", JSON.stringify(listingData));

    // Determine top-level price.
    let formattedPrice;
    if (listingData.has_variations) {
      formattedPrice = 0.20; // default minimal price for listings with variations
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

    // Build the creation payload for the new listing (inventory details will be updated separately if variations exist).
    const creationPayload = {
      quantity: listingData.quantity || 1,
      title: listingData.title || "Duplicated Listing",
      description: listingData.description || "",
      price: formattedPrice,
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
      processing_min: listingData.processing_min || null,
      processing_max: listingData.processing_max || null
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

    // Create the new listing.
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

    // If the original listing has variations, update the new listing's inventory.
    if (listingData.has_variations) {
      const originalInventory = await fetchOriginalInventory(listingId, token, clientId);
      if (!originalInventory) {
        console.warn("No inventory data fetched from original listing.");
      } else {
        const newInventoryPayload = transformInventory(originalInventory);
        console.log("Transformed inventory payload:", JSON.stringify(newInventoryPayload));
        console.log("Waiting 5 seconds before updating inventory...");
        await delay(5000);
        try {
          await updateInventoryResource(shopId, newListingData.listing_id, token, clientId, newInventoryPayload);
        } catch (invError) {
          console.error("Final inventory update error:", invError);
        }
      }
    } else {
      console.log("Listing does not have variations; no inventory update needed.");
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