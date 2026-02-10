const fetch = require("node-fetch");

// Helper: Convert a price field into a float value
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
// This function iterates over each product and for each product, it creates a new property_values array
// that only contains allowed keys: property_id and value_ids. It also transforms the offerings array to ensure
// that the price is a float.
function transformInventoryData(inventoryData) {
  if (!inventoryData || !inventoryData.products) return null;
  const transformedProducts = inventoryData.products.map((product) => {
    // Transform property_values: only include property_id and value_ids.
    const transformedPropertyValues = (product.property_values || []).map(pv => {
      return {
        property_id: pv.property_id,
        value_ids: Array.isArray(pv.value_ids) ? pv.value_ids : []
      };
    });

    // Transform offerings: ensure price is a float.
    const transformedOfferings = (product.offerings || []).map(offering => {
      return {
        price: transformPrice(offering.price),
        quantity: offering.quantity,
        is_enabled: offering.is_enabled
      };
    });

    return {
      sku: product.sku,
      offerings: transformedOfferings,
      property_values: transformedPropertyValues
    };
  });

  return {
    products: transformedProducts,
    price_on_property: inventoryData.price_on_property || [],
    quantity_on_property: inventoryData.quantity_on_property || [],
    sku_on_property: inventoryData.sku_on_property || []
  };
}

// Function to update inventory data with retries.
// It first attempts a PUT update; if a 404 (resource not found) is returned,
// it waits 5 seconds and then attempts a POST to create the inventory resource.
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
      break; // Exit retry loop to attempt POST creation
    } else {
      const errorText = await response.text();
      console.error("PUT inventory update attempt failed:", errorText);
      attempt++;
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }

  // Attempt POST inventory creation if PUT did not succeed.
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
    // Extract query parameters: new listingId, originalListingId, and token.
    const { listingId, originalListingId, token } = event.queryStringParameters;
    if (!listingId || !originalListingId || !token) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing listingId, originalListingId, or token" }),
      };
    }
    console.log("Received new listingId:", listingId);
    console.log("Received original listingId:", originalListingId);
    console.log("Received token:", token);

    // Retrieve CLIENT_ID from environment variables.
    const clientId =
      process.env.CLIENT_ID ||
      process.env.ETSY_CLIENT_ID ||
      process.env.ETSY_API_KEY ||
      process.env.API_KEY;
    if (!clientId) {
      console.error("Missing Etsy app key env var for x-api-key header.");
      console.log("Env presence:", {
        CLIENT_ID: !!process.env.CLIENT_ID,
        ETSY_CLIENT_ID: !!process.env.ETSY_CLIENT_ID,
        ETSY_API_KEY: !!process.env.ETSY_API_KEY,
        API_KEY: !!process.env.API_KEY,
      });
      return {
        statusCode: 500,
        body: JSON.stringify({
          error: "Missing Etsy app key env var for x-api-key header.",
          checked: ["CLIENT_ID", "ETSY_CLIENT_ID", "ETSY_API_KEY", "API_KEY"],
        }),
      };
    }
    console.log("Using Etsy app key (masked):", String(clientId).slice(0, 5) + "*****");

    // Fetch original inventory data from Etsy using the original listing ID.
    const etsyInventoryUrl = `https://api.etsy.com/v3/application/listings/${originalListingId}/inventory`;
    console.log("Fetching original inventory data from:", etsyInventoryUrl);
    const inventoryResponse = await fetch(etsyInventoryUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId,
      },
    });
    console.log("Inventory fetch response status:", inventoryResponse.status);
    let inventoryData;
    if (inventoryResponse.ok) {
      inventoryData = await inventoryResponse.json();
      console.log("Fetched original inventory data:", JSON.stringify(inventoryData));
    } else {
      const errorText = await inventoryResponse.text();
      console.error("Failed to fetch original inventory data:", errorText);
      return {
        statusCode: inventoryResponse.status,
        body: JSON.stringify({ error: "Failed to fetch original inventory data", details: errorText }),
      };
    }

    // Update the new listing's inventory.
    try {
      const updateResult = await updateInventory(listingId, inventoryData, token, clientId);
      console.log("Final inventory update result:", updateResult);
      return {
        statusCode: 200,
        body: JSON.stringify(updateResult),
      };
    } catch (inventoryError) {
      console.error("Final inventory update error:", inventoryError);
      return {
        statusCode: 500,
        body: JSON.stringify({ error: inventoryError.message }),
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