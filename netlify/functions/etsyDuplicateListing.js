const fetch = require("node-fetch");

// --- Utility Functions ---

// Recursively remove keys that Etsy’s API rejects from an object
function cleanObject(obj) {
  if (Array.isArray(obj)) {
    return obj.map(cleanObject);
  } else if (obj && typeof obj === "object") {
    const newObj = {};
    for (const key in obj) {
      // Remove keys that are not allowed in the inventory update payload
      const invalidKeys = ["scale_name", "product_id", "is_deleted", "offering_id"];
      if (invalidKeys.includes(key)) continue;
      newObj[key] = cleanObject(obj[key]);
    }
    return newObj;
  }
  return obj;
}

// Transform price fields in the inventory payload
function transformInventoryPayload(payload) {
  if (payload.products && Array.isArray(payload.products)) {
    payload.products.forEach((product, prodIndex) => {
      // Check if product.price exists and is an array
      if (product.price && Array.isArray(product.price) && product.price.length > 0) {
        console.log(`Product ${prodIndex}: transforming product.price from array ${JSON.stringify(product.price)}`);
        product.price = parseFloat(product.price[0]);
      }
      if (product.offerings && Array.isArray(product.offerings)) {
        product.offerings.forEach((offering, offIndex) => {
          if (offering.price && Array.isArray(offering.price) && offering.price.length > 0) {
            console.log(`Product ${prodIndex}, Offering ${offIndex}: transforming offering.price from array ${JSON.stringify(offering.price)}`);
            offering.price = parseFloat(offering.price[0]);
          } else if (typeof offering.price === "string") {
            console.log(`Product ${prodIndex}, Offering ${offIndex}: parsing offering.price string "${offering.price}"`);
            offering.price = parseFloat(offering.price);
          }
        });
      }
    });
  }
  return payload;
}

// --- Main Handler ---
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

    // Retrieve CLIENT_ID from environment variables; used as x-api-key.
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

    // Build the creation payload for the new listing.
    // If the listing has variations, we omit the top-level price.
    let creationPayload = {
      quantity: listingData.quantity || 1,
      title: listingData.title || "Duplicated Listing",
      description: listingData.description || "",
      who_made: listingData.who_made || "i_did",
      when_made: listingData.when_made || "made_to_order",
      taxonomy_id: listingData.taxonomy_id || 0,
      shipping_profile_id: listingData.shipping_profile_id, // required for physical listings
      return_policy_id: listingData.return_policy_id,       // required
      tags: listingData.tags || [],
      materials: listingData.materials || [],
      skus: listingData.skus || [],
      style: listingData.style || [],
      has_variations: (typeof listingData.has_variations === "boolean") ? listingData.has_variations : false,
      is_customizable: (typeof listingData.is_customizable === "boolean") ? listingData.is_customizable : false,
      is_personalizable: (typeof listingData.is_personalizable === "boolean") ? listingData.is_personalizable : false
    };

    // Only include a base price if the listing does NOT have variations.
    if (!listingData.has_variations) {
      let basePrice = 0.00;
      if (listingData.price && typeof listingData.price === "object" &&
          listingData.price.amount && listingData.price.divisor) {
        basePrice = listingData.price.amount / listingData.price.divisor;
      } else if (listingData.price && typeof listingData.price !== "object") {
        basePrice = parseFloat(listingData.price);
      }
      basePrice = parseFloat(basePrice.toFixed(2));
      creationPayload.price = basePrice;
      console.log("Calculated base price (no variations):", basePrice);
    } else {
      console.log("Listing has variations; omitting top-level price.");
    }

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

    // Create the duplicated listing.
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

    // If the original listing has variations, attempt to update the inventory.
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
        // Clean and transform the inventory payload.
        inventoryPayload = cleanObject(invData);
        inventoryPayload = transformInventoryPayload(inventoryPayload);
        console.log("Transformed inventory payload:", inventoryPayload);
      } else {
        const invErrorText = await invResponse.text();
        console.error("Error fetching inventory data:", invErrorText);
      }

      if (inventoryPayload) {
        const etsyInventoryUrl = `https://api.etsy.com/v3/application/listings/${newListingData.listing_id}/inventory`;

        // Retry inventory update in case the resource is not yet ready.
        async function retryInventoryUpdate(url, payload, retries = 5, delayMs = 5000) {
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
              body: JSON.stringify(payload)
            });
            console.log("PUT inventory update response status:", response.status);
            if (response.ok) {
              return await response.json();
            } else {
              const errorText = await response.text();
              console.error(`PUT inventory update attempt ${attempt + 1} failed:`, errorText);
              lastError = errorText;
              // If 404, wait and retry.
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

        try {
          const updatedInventory = await retryInventoryUpdate(etsyInventoryUrl, inventoryPayload);
          console.log("Inventory updated via PUT:", updatedInventory);
          newListingData.inventory = updatedInventory;
        } catch (err) {
          console.error("PUT inventory update failed after retries:", err.message);
          return {
            statusCode: 500,
            body: JSON.stringify({ error: "Error updating inventory", details: err.message })
          };
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