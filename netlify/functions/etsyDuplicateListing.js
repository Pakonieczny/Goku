const fetch = require("node-fetch");

// Helper to convert a price field into a float value
function transformPrice(priceField) {
  if (typeof priceField === "object" && priceField.amount && priceField.divisor) {
    return parseFloat((priceField.amount / priceField.divisor).toFixed(2));
  } else if (typeof priceField === "string" || typeof priceField === "number") {
    return parseFloat(priceField);
  } else {
    return 1.00; // fallback placeholder
  }
}

// Transform the inventory data for updating the listing
function transformInventoryData(inventoryData) {
  if (!inventoryData || !inventoryData.products) return null;
  const transformedProducts = inventoryData.products.map((product) => {
    // Transform property_values: keep only property_id and value_ids.
    const transformedProperties = (product.property_values || []).map(pv => ({
      property_id: pv.property_id,
      value_ids: pv.value_ids || []
    }));
    
    // Transform offerings: ensure price is a float.
    const transformedOfferings = (product.offerings || []).map(offering => ({
      price: transformPrice(offering.price),
      quantity: offering.quantity,
      is_enabled: offering.is_enabled
    }));
    
    return {
      sku: product.sku,
      offerings: transformedOfferings,
      // Use 'properties' key instead of 'property_values'
      properties: transformedProperties
    };
  });

  return {
    products: transformedProducts,
    price_on_property: [],
    quantity_on_property: [],
    sku_on_property: []
  };
}

// Function to update inventory data with retries.
// It first attempts a PUT update; if a 404 is returned, it waits and then tries a POST.
async function updateInventory(newListingId, inventoryData, token, clientId) {
  if (!inventoryData) {
    console.log("No inventory data available to update.");
    return;
  }

  const transformedInventory = transformInventoryData(inventoryData);
  if (!transformedInventory) {
    console.log("Transformed inventory is empty.");
    return;
  }

  const inventoryUrl = `https://api.etsy.com/v3/application/listings/${newListingId}/inventory`;
  const headers = {
    "Content-Type": "application/json",
    "Authorization": `Bearer ${token}`,
    "x-api-key": clientId,
  };

  // Attempt PUT update with up to 3 retries
  const maxRetries = 3;
  let attempt = 0;
  let response;
  while (attempt < maxRetries) {
    console.log(`Attempting PUT inventory update, attempt ${attempt + 1}...`);
    response = await fetch(inventoryUrl, {
      method: "PUT",
      headers,
      body: JSON.stringify(transformedInventory),
    });
    console.log("PUT inventory update response status:", response.status);
    if (response.ok) {
      const data = await response.json();
      console.log("Inventory updated successfully via PUT:", data);
      return data;
    } else if (response.status === 404) {
      console.warn("PUT returned 404. Inventory resource not found. Waiting 5 seconds before trying POST...");
      await new Promise(resolve => setTimeout(resolve, 5000));
      break; // break out of retry loop to try POST
    } else {
      const errorText = await response.text();
      console.error("PUT inventory update attempt failed:", errorText);
      attempt++;
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }

  // Attempt POST creation if PUT did not succeed
  console.log("Attempting POST inventory creation...");
  response = await fetch(inventoryUrl, {
    method: "POST",
    headers,
    body: JSON.stringify(transformedInventory),
  });
  console.log("POST inventory creation response status:", response.status);
  if (response.ok) {
    const data = await response.json();
    console.log("Inventory created successfully via POST:", data);
    return data;
  } else {
    const errorText = await response.text();
    console.error("Error creating inventory with POST:", errorText);
    throw new Error(`Inventory creation failed: ${response.status} - ${errorText}`);
  }
};

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

    // Use a static placeholder price of $1.00 for the duplicated listing.
    const staticPrice = 1.00;

    // Build the payload for duplicating the listing.
    // Do not include inventory in this payload; it will be updated separately.
    const payload = {
      quantity: listingData.quantity || 1,
      title: listingData.title || "Duplicated Listing",
      description: listingData.description || "",
      price: staticPrice,
      who_made: listingData.who_made || "i_did",
      when_made: listingData.when_made || "made_to_order",
      taxonomy_id: listingData.taxonomy_id || 0,
      shipping_profile_id: listingData.shipping_profile_id, // required for physical listings
      return_policy_id: listingData.return_policy_id,       // required field
      tags: listingData.tags || [],
      materials: listingData.materials || [],
      skus: listingData.skus || [],
      style: listingData.style || [],
      has_variations: listingData.has_variations || false,
      is_customizable: listingData.is_customizable || false,
      is_personalizable: listingData.is_personalizable || false,
      // Do not include inventory here; it will be updated in a separate call.
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

    // --- INVENTORY UPDATE STEP ---
    // Retrieve inventory data from original listing (try from listing details first, then inventory endpoint)
    let inventoryData = listingData.inventory;
    if (!inventoryData) {
      console.warn("No inventory data available in original listing; attempting to fetch from inventory endpoint...");
      const etsyInventoryUrl = `https://api.etsy.com/v3/application/listings/${listingId}/inventory`;
      const inventoryResponse = await fetch(etsyInventoryUrl, {
        method: "GET",
        headers: {
          "Authorization": `Bearer ${token}`,
          "x-api-key": clientId,
        },
      });
      if (inventoryResponse.ok) {
        inventoryData = await inventoryResponse.json();
        console.log("Fetched inventory data from inventory endpoint:", inventoryData);
      } else {
        console.warn("No inventory data available from inventory endpoint. Status:", inventoryResponse.status);
      }
    } else {
      console.log("Original inventory data from listing details:", inventoryData);
    }

    // Update the new listing's inventory if available.
    if (inventoryData) {
      try {
        const inventoryUpdateResult = await updateInventory(newListingData.listing_id, inventoryData, token, clientId);
        console.log("Final inventory update result:", inventoryUpdateResult);
      } catch (inventoryError) {
        console.error("Final inventory update error:", inventoryError);
      }
    } else {
      console.log("No inventory data to update.");
    }

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