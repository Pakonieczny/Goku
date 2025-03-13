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
// We keep allowed keys for each product: sku, offerings, and properties.
// In this version, we omit keys that cause errors (like product_id, is_deleted, scale_name, etc.).
function transformInventoryData(inventoryData) {
  if (!inventoryData || !inventoryData.products) return null;
  const transformedProducts = inventoryData.products.map((product) => {
    // Transform property_values into a "properties" array; include only property_id and value_ids.
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
      properties: transformedProperties
    };
  });

  return {
    products: transformedProducts,
    // Additional keys as required by Etsy can be added here if needed.
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
    // Extract newListingId and originalListingId, plus token from query parameters.
    const { newListingId, originalListingId, token } = event.queryStringParameters;
    if (!newListingId || !originalListingId || !token) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing newListingId, originalListingId or token" }),
      };
    }
    console.log("Received newListingId:", newListingId);
    console.log("Received originalListingId:", originalListingId);
    console.log("Received token:", token);

    // Retrieve CLIENT_ID from environment variables.
    const clientId = process.env.CLIENT_ID;
    if (!clientId) {
      console.error("CLIENT_ID environment variable is not set.");
    } else {
      console.log("Using CLIENT_ID:", clientId.slice(0, 5) + "*****");
    }

    // Attempt to fetch original inventory data from the listing details.
    let inventoryData;
    // First, check if the original listing includes an 'inventory' object.
    const etsyGetListingUrl = `https://api.etsy.com/v3/application/listings/${originalListingId}`;
    const listingResponse = await fetch(etsyGetListingUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId,
      },
    });
    if (listingResponse.ok) {
      const listingData = await listingResponse.json();
      inventoryData = listingData.inventory;
      console.log("Original inventory data from listing details:", inventoryData);
    } else {
      console.warn("Failed to fetch listing details for inventory. Status:", listingResponse.status);
    }

    // If not available, try fetching from the inventory endpoint.
    if (!inventoryData) {
      console.warn("No inventory data available in listing details; fetching from inventory endpoint...");
      const etsyInventoryUrl = `https://api.etsy.com/v3/application/listings/${originalListingId}/inventory`;
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
    }

    if (!inventoryData) {
      console.log("No inventory data to update.");
      return {
        statusCode: 200,
        body: JSON.stringify({ message: "No inventory data available to update." }),
      };
    }

    // Update the new listing's inventory using our helper function.
    try {
      const inventoryUpdateResult = await updateInventory(newListingId, inventoryData, token, clientId);
      console.log("Final inventory update result:", inventoryUpdateResult);
      return {
        statusCode: 200,
        body: JSON.stringify(inventoryUpdateResult),
      };
    } catch (inventoryError) {
      console.error("Final inventory update error:", inventoryError);
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Inventory update failed", details: inventoryError.message }),
      };
    }
  } catch (error) {
    console.error("Exception in updateInventory handler:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
};