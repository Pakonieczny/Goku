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

// Transform the inventory data for updating the listing.
// We remove disallowed keys (e.g. product_id, is_deleted, scale_name) and
// keep only sku, offerings (with price, quantity, is_enabled) and properties (with property_id and value_ids).
function transformInventoryData(inventoryData) {
  if (!inventoryData || !inventoryData.products) return null;
  const transformedProducts = inventoryData.products.map((product) => {
    const transformedProperties = (product.property_values || []).map(pv => ({
      property_id: pv.property_id,
      // We omit value_id, name, and value because Etsy API does not accept these keys.
      // Instead, you might need to map these to the correct structure per Etsy’s docs.
      // For now, we assume that only property_id and an array of value_ids are required.
      value_ids: pv.value_ids || []
    }));
    
    const transformedOfferings = (product.offerings || []).map(offering => ({
      price: transformPrice(offering.price),
      quantity: offering.quantity,
      is_enabled: offering.is_enabled
      // We omit offering_id and is_deleted from the payload.
    }));
    
    return {
      sku: product.sku,
      offerings: transformedOfferings,
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
}

exports.handler = async function (event, context) {
  try {
    // Expecting query parameters: listingId (new listing ID), originalListingId, token.
    const { listingId, originalListingId, token } = event.queryStringParameters;
    if (!listingId || !originalListingId || !token) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing listingId, originalListingId, or token" }),
      };
    }
    console.log("New listing ID for inventory update:", listingId);
    console.log("Original listing ID:", originalListingId);
    console.log("Received token:", token);

    // Retrieve CLIENT_ID from environment variables.
    const clientId = process.env.CLIENT_ID;
    if (!clientId) {
      console.error("CLIENT_ID environment variable is not set.");
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "CLIENT_ID environment variable is not set." }),
      };
    }
    console.log("Using CLIENT_ID:", clientId.slice(0, 5) + "*****");

    // Fetch original inventory data from the original listing.
    const etsyInventoryUrl = `https://api.etsy.com/v3/application/listings/${originalListingId}/inventory`;
    console.log("Fetching inventory from original listing:", etsyInventoryUrl);
    const inventoryResponse = await fetch(etsyInventoryUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId,
      },
    });
    console.log("GET inventory response status:", inventoryResponse.status);
    let inventoryData;
    if (inventoryResponse.ok) {
      inventoryData = await inventoryResponse.json();
      console.log("Fetched original inventory data:", JSON.stringify(inventoryData));
    } else {
      const errorText = await inventoryResponse.text();
      console.error("Failed to fetch inventory data from original listing:", errorText);
      return {
        statusCode: inventoryResponse.status,
        body: JSON.stringify({ error: "Failed to fetch original inventory data", details: errorText }),
      };
    }

    // Attempt to update the inventory of the new listing using the transformed inventory data.
    const result = await updateInventory(listingId, inventoryData, token, clientId);
    console.log("Final inventory update result:", result);
    return {
      statusCode: 200,
      body: JSON.stringify(result),
    };
  } catch (error) {
    console.error("Exception in updateInventory handler:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
};