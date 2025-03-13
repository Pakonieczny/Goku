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
// This function removes disallowed keys (e.g. product_id, is_deleted, scale_name)
// and converts the original property_values to a properties array with only property_id and value_ids.
function transformInventoryData(inventoryData) {
  if (!inventoryData || !inventoryData.products) return null;
  const transformedProducts = inventoryData.products.map((product) => {
    const transformedProperties = (product.property_values || []).map(pv => ({
      property_id: pv.property_id,
      value_ids: pv.value_ids || []
    }));
    
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
    price_on_property: [],
    quantity_on_property: [],
    sku_on_property: []
  };
}

// Function to update inventory data with retries.
// First it tries a PUT update. If a 404 is returned (resource not found), it waits 5 seconds and then attempts a POST.
async function updateInventory(newListingId, originalListingId, token, clientId) {
  // Fetch original inventory data from the original listing.
  const originalInventoryUrl = `https://api.etsy.com/v3/application/listings/${originalListingId}/inventory`;
  console.log("Fetching original inventory data from:", originalInventoryUrl);
  const originalInventoryResponse = await fetch(originalInventoryUrl, {
    method: "GET",
    headers: {
      "Authorization": `Bearer ${token}`,
      "x-api-key": clientId,
    },
  });
  console.log("Original inventory GET response status:", originalInventoryResponse.status);
  if (!originalInventoryResponse.ok) {
    const errorText = await originalInventoryResponse.text();
    throw new Error(`Failed to fetch original inventory: ${originalInventoryResponse.status} - ${errorText}`);
  }
  const originalInventoryData = await originalInventoryResponse.json();
  console.log("Original inventory data fetched:", JSON.stringify(originalInventoryData));

  // Transform the inventory data to the format expected by Etsy.
  const transformedInventory = transformInventoryData(originalInventoryData);
  if (!transformedInventory) {
    throw new Error("Transformed inventory data is empty.");
  }
  console.log("Transformed inventory data:", JSON.stringify(transformedInventory));

  // Build the URL for inventory update on the new listing.
  const inventoryUrl = `https://api.etsy.com/v3/application/listings/${newListingId}/inventory`;
  const headers = {
    "Content-Type": "application/json",
    "Authorization": `Bearer ${token}`,
    "x-api-key": clientId,
  };

  // Attempt PUT update with up to 3 retries.
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
      break; // Break out of the retry loop to try POST.
    } else {
      const errorText = await response.text();
      console.error("PUT inventory update attempt failed:", errorText);
      attempt++;
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }

  // Attempt POST creation if PUT did not succeed.
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
    // Expect query parameters: listingId (new listing ID), originalListingId, and token.
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

    // Update the new listing's inventory using the original listing's inventory data.
    const result = await updateInventory(listingId, originalListingId, token, clientId);
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