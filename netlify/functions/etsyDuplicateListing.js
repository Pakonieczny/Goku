const fetch = require("node-fetch");

// Helper function to transform price from an object to a float.
function transformPrice(priceField) {
  if (typeof priceField === "object" && priceField.amount && priceField.divisor) {
    return parseFloat((priceField.amount / priceField.divisor).toFixed(2));
  } else if (typeof priceField === "string" || typeof priceField === "number") {
    return parseFloat(priceField);
  } else {
    return 1.00; // fallback placeholder
  }
}

// Transform the inventory payload by iterating over products and offerings.
function transformInventoryData(inventoryData) {
  if (!inventoryData || !inventoryData.products) return null;

  const transformedProducts = inventoryData.products.map((product) => {
    // Create a new product object copying allowed fields
    const newProduct = {
      sku: product.sku,
      // For each offering, transform price to a float and keep allowed keys
      offerings: product.offerings.map((offering) => ({
        // Remove disallowed keys (e.g. offering_id, is_deleted) by only sending what is allowed:
        price: transformPrice(offering.price),
        quantity: offering.quantity,
        is_enabled: offering.is_enabled,
      })),
      // Instead of passing the full properties array (which may contain invalid keys),
      // we send an empty array or placeholders as needed.
      properties: [] 
    };
    return newProduct;
  });

  // Construct the final payload. (Note: Etsy may require additional fields like
  // price_on_property, quantity_on_property, etc. For now we pass our products array.)
  return {
    products: transformedProducts,
    // You can add additional arrays as needed here.
    price_on_property: [],
    quantity_on_property: [],
    sku_on_property: []
  };
}

// Function to update inventory via PUT; if 404, try POST creation.
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

  // Attempt PUT update
  let response = await fetch(inventoryUrl, {
    method: "PUT",
    headers,
    body: JSON.stringify(transformedInventory),
  });
  console.log("PUT inventory update response status:", response.status);
  if (response.ok) {
    const data = await response.json();
    console.log("Inventory updated successfully:", data);
    return data;
  } else if (response.status === 404) {
    // Inventory resource not found; attempt POST to create inventory
    console.warn("Inventory resource not found via PUT; attempting POST creation after delay...");
    await new Promise((resolve) => setTimeout(resolve, 5000));
    response = await fetch(inventoryUrl, {
      method: "POST",
      headers,
      body: JSON.stringify(transformedInventory),
    });
    console.log("POST inventory creation response status:", response.status);
    if (response.ok) {
      const data = await response.json();
      console.log("Inventory created successfully:", data);
      return data;
    } else {
      const errorText = await response.text();
      throw new Error(`Inventory creation failed: ${response.status} - ${errorText}`);
    }
  } else {
    const errorText = await response.text();
    throw new Error(`Inventory update failed: ${response.status} - ${errorText}`);
  }
}

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

    // Retrieve the original listing details.
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

    // Static placeholder price for duplicated listing (used for the listing creation call)
    const staticPrice = 1.00;

    // Build the payload for duplicating the listing.
    // Note: We do not pass the inventory data in this call.
    const payload = {
      quantity: listingData.quantity || 1,
      title: listingData.title || "Duplicated Listing",
      description: listingData.description || "",
      price: staticPrice,
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
      // Do not include inventory here; it will be updated separately.
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
    // Since inventory isn’t passed during listing creation,
    // fetch original inventory data separately.
    let inventoryData = listingData.inventory; // May be null if not included.
    if (!inventoryData) {
      // Optionally, try fetching inventory data from the separate endpoint.
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
        console.warn("No inventory data available from inventory endpoint.");
      }
    } else {
      console.log("Original inventory data from listing details:", inventoryData);
    }

    // If inventory data is available, update the new listing’s inventory.
    if (inventoryData) {
      try {
        // Attempt to update inventory with retries.
        const updateResult = await updateInventory(newListingData.listing_id, inventoryData, token, clientId);
        console.log("Final inventory update result:", updateResult);
      } catch (inventoryError) {
        console.error("Final inventory update error:", inventoryError);
        // You can choose to return an error here or continue.
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