const fetch = require("node-fetch");

// Helper to convert a price field into a float value
function transformPrice(priceField) {
  if (typeof priceField === "object" && priceField.amount && priceField.divisor) {
    const computed = priceField.amount / priceField.divisor;
    console.log(`transformPrice: Converted ${JSON.stringify(priceField)} to ${computed}`);
    return parseFloat(computed.toFixed(2));
  } else if (typeof priceField === "string" || typeof priceField === "number") {
    const parsed = parseFloat(priceField);
    console.log(`transformPrice: Parsed ${priceField} to ${parsed}`);
    return parsed;
  } else {
    console.log("transformPrice: Fallback to 1.00 for", priceField);
    return 1.00; // fallback placeholder
  }
}

// Transform the inventory data for updating the listing.
// This function removes disallowed keys (e.g., product_id, is_deleted, scale_name, etc.)
// and maps property_values to a simplified structure.
function transformInventoryData(inventoryData) {
  if (!inventoryData || !inventoryData.products) {
    console.warn("transformInventoryData: No products found in inventoryData");
    return null;
  }
  const transformedProducts = inventoryData.products.map((product, index) => {
    console.log(`transformInventoryData: Processing product index ${index} with sku: ${product.sku}`);
    // Transform property_values: keep only property_id and value_ids.
    const transformedProperties = (product.property_values || []).map(pv => {
      console.log(`transformInventoryData: Processing property_value: ${JSON.stringify(pv)}`);
      return {
        property_id: pv.property_id,
        value_ids: pv.value_ids || []
      };
    });
    
    // Transform offerings: ensure price is a float.
    const transformedOfferings = (product.offerings || []).map((offering, oIndex) => {
      const priceFloat = transformPrice(offering.price);
      console.log(`transformInventoryData: Offering index ${oIndex} price transformed to: ${priceFloat}`);
      return {
        price: priceFloat,
        quantity: offering.quantity,
        is_enabled: offering.is_enabled
        // Do not include keys such as offering_id, is_deleted.
      };
    });
    
    return {
      sku: product.sku,
      offerings: transformedOfferings,
      // Use 'properties' key instead of 'property_values'
      properties: transformedProperties
    };
  });

  const transformedInventory = {
    products: transformedProducts,
    price_on_property: [],
    quantity_on_property: [],
    sku_on_property: []
  };
  console.log("transformInventoryData: Final transformed inventory:", JSON.stringify(transformedInventory, null, 2));
  return transformedInventory;
}

// Function to update inventory data with retries.
// It first attempts a PUT update; if a 404 is returned, it waits and then tries a POST.
async function updateInventory(newListingId, inventoryData, token, clientId) {
  if (!inventoryData) {
    console.warn("updateInventory: No inventory data available to update.");
    return;
  }

  const transformedInventory = transformInventoryData(inventoryData);
  if (!transformedInventory) {
    console.warn("updateInventory: Transformed inventory is empty.");
    return;
  }

  const inventoryUrl = `https://api.etsy.com/v3/application/listings/${newListingId}/inventory`;
  const headers = {
    "Content-Type": "application/json",
    "Authorization": `Bearer ${token}`,
    "x-api-key": clientId
  };

  let attempt = 0;
  const maxRetries = 3;
  let response;

  // Try PUT update with retries
  while (attempt < maxRetries) {
    console.log(`updateInventory: Attempting PUT inventory update, attempt ${attempt + 1}...`);
    try {
      response = await fetch(inventoryUrl, {
        method: "PUT",
        headers,
        body: JSON.stringify(transformedInventory)
      });
    } catch (err) {
      console.error(`updateInventory: Network error on PUT attempt ${attempt + 1}:`, err);
      attempt++;
      await new Promise(resolve => setTimeout(resolve, 2000));
      continue;
    }

    console.log("updateInventory: PUT response status:", response.status);
    if (response.ok) {
      const data = await response.json();
      console.log("updateInventory: Inventory updated successfully via PUT:", data);
      return data;
    } else if (response.status === 404) {
      console.warn("updateInventory: PUT returned 404 - inventory resource not found. Breaking out to try POST.");
      break;
    } else {
      const errorText = await response.text();
      console.error(`updateInventory: PUT update attempt ${attempt + 1} failed:`, errorText);
      attempt++;
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }

  // If PUT did not succeed, attempt POST creation
  console.log("updateInventory: Attempting POST inventory creation...");
  try {
    response = await fetch(inventoryUrl, {
      method: "POST",
      headers,
      body: JSON.stringify(transformedInventory)
    });
  } catch (err) {
    console.error("updateInventory: Network error during POST inventory creation:", err);
    throw new Error("Network error during POST inventory creation");
  }
  console.log("updateInventory: POST response status:", response.status);
  if (response.ok) {
    const data = await response.json();
    console.log("updateInventory: Inventory created successfully via POST:", data);
    return data;
  } else {
    const errorText = await response.text();
    console.error("updateInventory: Error creating inventory with POST:", errorText);
    throw new Error(`Inventory creation failed: ${response.status} - ${errorText}`);
  }
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
    console.log("updateInventory handler: Received listingId:", listingId);
    console.log("updateInventory handler: Received token:", token.slice(0, 5) + "...");
    
    // Retrieve CLIENT_ID from environment variables.
    const clientId = process.env.CLIENT_ID;
    if (!clientId) {
      console.error("updateInventory handler: CLIENT_ID environment variable is not set.");
      return { statusCode: 500, body: JSON.stringify({ error: "CLIENT_ID not set" }) };
    }
    console.log("updateInventory handler: Using CLIENT_ID:", clientId.slice(0, 5) + "*****");
    
    // Parse the original inventory data from the request body.
    let inventoryData;
    try {
      inventoryData = JSON.parse(event.body);
    } catch (parseError) {
      console.error("updateInventory handler: Error parsing request body:", parseError);
      return { statusCode: 400, body: JSON.stringify({ error: "Invalid JSON in request body" }) };
    }
    console.log("updateInventory handler: Original inventory data:", JSON.stringify(inventoryData, null, 2));
    
    // Call the updateInventory function to update/create the new listing's inventory.
    const updateResult = await updateInventory(listingId, inventoryData, token, clientId);
    console.log("updateInventory handler: Final inventory update result:", updateResult);
    
    return {
      statusCode: 200,
      body: JSON.stringify(updateResult)
    };
  } catch (error) {
    console.error("updateInventory handler: Exception:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};