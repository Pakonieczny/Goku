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
// This function removes disallowed keys (such as product_id, is_deleted, scale_name, etc.)
// and maps property_values to a simplified structure.
function transformInventoryData(inventoryData) {
  if (!inventoryData || !inventoryData.products) return null;
  const transformedProducts = inventoryData.products.map((product) => {
    // For properties, we keep only property_id and value_ids.
    const transformedProperties = (product.property_values || []).map(pv => ({
      property_id: pv.property_id,
      // Note: According to Etsy’s API, keys like value_id, name, and value are not allowed.
      // We’re only keeping property_id and value_ids.
      value_ids: pv.value_ids || []
    }));
    
    // For offerings, ensure the price is a float.
    const transformedOfferings = (product.offerings || []).map(offering => ({
      price: transformPrice(offering.price),
      quantity: offering.quantity,
      is_enabled: offering.is_enabled
      // Note: Do not include keys like offering_id, is_deleted.
    }));
    
    return {
      sku: product.sku,
      offerings: transformedOfferings,
      properties: transformedProperties
    };
  });

  // Etsy requires additional arrays even if empty.
  return {
    products: transformedProducts,
    price_on_property: [],
    quantity_on_property: [],
    sku_on_property: []
  };
}

exports.handler = async function (event, context) {
  try {
    // Extract listingId and token from query parameters.
    const { listingId, token } = event.queryStringParameters;
    if (!listingId || !token) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing listingId or token" })
      };
    }
    console.log("updateInventory: Received listingId:", listingId);
    console.log("updateInventory: Received token:", token.slice(0, 5) + "...");
    
    // Retrieve CLIENT_ID from environment variables.
    const clientId = process.env.CLIENT_ID;
    if (!clientId) {
      console.error("updateInventory: CLIENT_ID environment variable is not set.");
      return { statusCode: 500, body: JSON.stringify({ error: "CLIENT_ID not set" }) };
    }
    console.log("updateInventory: Using CLIENT_ID:", clientId.slice(0, 5) + "*****");
    
    // Parse the original inventory data from the request body.
    const inventoryData = JSON.parse(event.body);
    console.log("updateInventory: Original inventory data:", JSON.stringify(inventoryData));
    
    // Transform the inventory data to match the Etsy API specifications.
    const transformedInventory = transformInventoryData(inventoryData);
    console.log("updateInventory: Transformed inventory data:", JSON.stringify(transformedInventory));
    
    if (!transformedInventory) {
      console.warn("updateInventory: No valid inventory data to update.");
      return { statusCode: 400, body: JSON.stringify({ error: "No valid inventory data" }) };
    }
    
    // Build the inventory update URL for the new listing.
    const inventoryUrl = `https://api.etsy.com/v3/application/listings/${listingId}/inventory`;
    const headers = {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${token}`,
      "x-api-key": clientId
    };
    
    // First attempt: PUT inventory update
    console.log("updateInventory: Attempting PUT inventory update...");
    let response = await fetch(inventoryUrl, {
      method: "PUT",
      headers,
      body: JSON.stringify(transformedInventory)
    });
    console.log("updateInventory: PUT response status:", response.status);
    if (response.ok) {
      const data = await response.json();
      console.log("updateInventory: Inventory updated successfully via PUT:", data);
      return { statusCode: 200, body: JSON.stringify(data) };
    } else if (response.status === 404) {
      console.warn("updateInventory: PUT returned 404 - inventory resource not found. Will try POST after delay.");
      await new Promise(resolve => setTimeout(resolve, 5000));
    } else {
      const errorText = await response.text();
      console.error("updateInventory: PUT update attempt failed:", errorText);
      // Continue to POST attempt even if PUT failed for reasons other than 404.
    }
    
    // Second attempt: POST inventory creation
    console.log("updateInventory: Attempting POST inventory creation...");
    response = await fetch(inventoryUrl, {
      method: "POST",
      headers,
      body: JSON.stringify(transformedInventory)
    });
    console.log("updateInventory: POST response status:", response.status);
    if (response.ok) {
      const data = await response.json();
      console.log("updateInventory: Inventory created successfully via POST:", data);
      return { statusCode: 200, body: JSON.stringify(data) };
    } else {
      const errorText = await response.text();
      console.error("updateInventory: Error creating inventory with POST:", errorText);
      return { statusCode: response.status, body: errorText };
    }
  } catch (error) {
    console.error("updateInventory: Exception:", error);
    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};