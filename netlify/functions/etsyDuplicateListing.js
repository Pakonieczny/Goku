const fetch = require("node-fetch");

// Remove keys that Etsy’s API does not allow.
function removeInvalidKeys(obj) {
  if (Array.isArray(obj)) {
    return obj.map(removeInvalidKeys);
  } else if (obj !== null && typeof obj === "object") {
    const newObj = {};
    for (const key in obj) {
      // Remove these keys entirely:
      if (["scale_name", "product_id", "is_deleted", "offering_id"].includes(key)) continue;
      newObj[key] = removeInvalidKeys(obj[key]);
    }
    return newObj;
  }
  return obj;
}

// Traverse the object and fix any "price" fields that are arrays.
function cleanInventoryData(data) {
  if (data === null || typeof data !== "object") return data;
  if (Array.isArray(data)) {
    return data.map(cleanInventoryData);
  }
  const newObj = {};
  for (const key in data) {
    let value = data[key];
    // If the key is "price" and its value is an array, convert it.
    if (key === "price" && Array.isArray(value)) {
      if (value.length > 0 && typeof value[0] === "object" && value[0].amount && value[0].divisor) {
         const computedPrice = value[0].amount / value[0].divisor;
         newObj[key] = parseFloat(computedPrice.toFixed(2));
         continue;
      } else if (value.length > 0 && typeof value[0] === "number") {
         newObj[key] = value[0];
         continue;
      } else {
         // If no valid price data, skip the key.
         continue;
      }
    }
    newObj[key] = cleanInventoryData(value);
  }
  return newObj;
}

// Helper: Retry inventory update (PUT) with delays.
async function retryInventoryUpdate(url, token, clientId, inventoryPayload, retries = 5, delayMs = 5000) {
  let lastError = null;
  for (let attempt = 0; attempt < retries; attempt++) {
    console.log(`Inventory update attempt ${attempt + 1}`);
    const response = await fetch(url, {
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
      return await response.json();
    } else {
      const errorText = await response.text();
      console.error("PUT inventory update attempt failed:", errorText);
      lastError = errorText;
      if (response.status === 404) {
        console.log("Inventory resource not found; waiting before retrying...");
        await new Promise(resolve => setTimeout(resolve, delayMs));
      } else {
        break;
      }
    }
  }
  throw new Error("Inventory update failed after all retries: " + lastError);
}

// Helper: Create inventory via POST if PUT fails.
async function createInventory(url, token, clientId, inventoryPayload) {
  console.log("Attempting POST inventory creation...");
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${token}`,
      "x-api-key": clientId
    },
    body: JSON.stringify(inventoryPayload)
  });
  console.log("POST inventory creation response status:", response.status);
  if (!response.ok) {
    const errorText = await response.text();
    throw new Error("Error creating inventory with POST: " + errorText);
  }
  return await response.json();
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

    // Build GET request URL to fetch original listing details.
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
    console.log("Listing data fetched:", listingData);

    // Calculate a base price from the listing (if available).
    let basePrice = 0.00;
    if (listingData.price) {
      if (typeof listingData.price === "object" && listingData.price.amount && listingData.price.divisor) {
        basePrice = listingData.price.amount / listingData.price.divisor;
      } else {
        basePrice = parseFloat(listingData.price);
      }
      basePrice = parseFloat(basePrice.toFixed(2));
    }
    console.log("Calculated base price:", basePrice);

    // Build payload for duplicating the listing.
    const creationPayload = {
      quantity: listingData.quantity || 1,
      title: listingData.title || "Duplicated Listing",
      description: listingData.description || "",
      price: basePrice, // Use the calculated base price.
      who_made: listingData.who_made || "i_did",
      when_made: listingData.when_made || "made_to_order",
      taxonomy_id: listingData.taxonomy_id || 0,
      shipping_profile_id: listingData.shipping_profile_id,
      return_policy_id: listingData.return_policy_id,
      tags: listingData.tags || [],
      materials: listingData.materials || [],
      skus: listingData.skus || [],
      style: listingData.style || [],
      has_variations: (typeof listingData.has_variations === "boolean") ? listingData.has_variations : false,
      is_customizable: (typeof listingData.is_customizable === "boolean") ? listingData.is_customizable : false,
      is_personalizable: (typeof listingData.is_personalizable === "boolean") ? listingData.is_personalizable : false
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

    // Create duplicate listing via POST.
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
      console.log("Listing has variations; fetching inventory data...");
      const inventoryUrl = `https://api.etsy.com/v3/application/listings/${listingId}/inventory`;
      const invResponse = await fetch(inventoryUrl, {
        method: "GET",
        headers: {
          "Authorization": `Bearer ${token}`,
          "x-api-key": clientId
        }
      });
      let inventoryPayload = null;
      if (invResponse.ok) {
        const invData = await invResponse.json();
        console.log("Original inventory data:", invData);
        // Clean the inventory data to remove invalid keys and convert any array price values.
        inventoryPayload = cleanInventoryData(removeInvalidKeys(invData));
      } else {
        const invErrorText = await invResponse.text();
        console.error("Error fetching inventory data:", invErrorText);
      }

      if (inventoryPayload) {
        const etsyInventoryUrl = `https://api.etsy.com/v3/application/listings/${newListingData.listing_id}/inventory`;
        try {
          const updatedInventory = await retryInventoryUpdate(etsyInventoryUrl, token, clientId, inventoryPayload);
          console.log("Inventory updated via PUT:", updatedInventory);
          newListingData.inventory = updatedInventory;
        } catch (err) {
          console.error("PUT inventory update failed after retries:", err.message);
          try {
            const createdInventory = await createInventory(etsyInventoryUrl, token, clientId, inventoryPayload);
            console.log("Inventory created via POST:", createdInventory);
            newListingData.inventory = createdInventory;
          } catch (postErr) {
            console.error("Error creating inventory with POST:", postErr.message);
            return {
              statusCode: 500,
              body: JSON.stringify({ error: "Error updating inventory", details: postErr.message })
            };
          }
        }
      } else {
        console.log("No inventory data available; skipping inventory update.");
      }
    } else {
      console.log("Listing does not have variations; skipping inventory update.");
    }

    return { statusCode: 200, body: JSON.stringify(newListingData) };
  } catch (error) {
    console.error("Exception in etsyDuplicateListing:", error);
    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};