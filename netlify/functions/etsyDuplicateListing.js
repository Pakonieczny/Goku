const fetch = require("node-fetch");

// Wait helper
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Fetch the original listing’s inventory.
 */
async function fetchOriginalInventory(listingId, token, clientId) {
  const inventoryUrl = `https://api.etsy.com/v3/application/listings/${listingId}/inventory`;
  const invResponse = await fetch(inventoryUrl, {
    method: "GET",
    headers: {
      "Authorization": `Bearer ${token}`,
      "x-api-key": clientId
    }
  });
  if (!invResponse.ok) {
    const errorText = await invResponse.text();
    console.error("Failed to fetch original inventory:", invResponse.status, errorText);
    return null;
  }
  const invData = await invResponse.json();
  console.log("Fetched original inventory data:", JSON.stringify(invData));
  return invData;
}

/**
 * Transform the fetched inventory data so that only allowed keys remain.
 * In Etsy’s inventory update payload, each product should have an “offerings” array.
 * We remove disallowed keys (like product_id, is_deleted, scale_name, offering_id, etc.)
 * and we convert the price in each offering to a float.
 */
function transformInventory(invData) {
  if (!invData || !Array.isArray(invData.products)) return null;
  const newProducts = invData.products.map(prod => {
    // Process offerings array for each product.
    let newOfferings = [];
    if (Array.isArray(prod.offerings)) {
      newOfferings = prod.offerings.map(offering => {
        let offeringPrice;
        // If price is an object with amount and divisor, calculate the float.
        if (offering.price && typeof offering.price === "object" && offering.price.amount && offering.price.divisor) {
          offeringPrice = parseFloat((offering.price.amount / offering.price.divisor).toFixed(2));
        } else if (Array.isArray(offering.price)) {
          // If price is mistakenly an array, take the first element and parse it.
          offeringPrice = parseFloat(offering.price[0]);
        } else {
          // Otherwise, try parsing directly.
          offeringPrice = parseFloat(offering.price);
        }
        return {
          price: isNaN(offeringPrice) ? 0.20 : offeringPrice,  // Etsy requires a valid float and above min price (e.g. $0.20)
          quantity: offering.quantity || 0,
          is_enabled: (typeof offering.is_enabled === "boolean") ? offering.is_enabled : true
          // Do not include disallowed keys like offering_id, scale_name, etc.
        };
      });
    }
    return {
      sku: prod.sku || "",
      offerings: newOfferings,
      properties: prod.properties || []
    };
  });
  return { products: newProducts };
}

/**
 * Attempt to update the inventory on the new listing.
 * This function uses a PUT call and includes simple retry logic.
 */
async function updateNewListingInventory(shopId, newListingId, token, clientId, inventoryPayload) {
  const etsyInventoryUrlNew = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${newListingId}/inventory`;
  const maxRetries = 3;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    console.log(`Inventory update attempt ${attempt}`);
    const invUpdateResponse = await fetch(etsyInventoryUrlNew, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId
      },
      body: JSON.stringify(inventoryPayload)
    });
    console.log("PUT inventory update response status:", invUpdateResponse.status);
    if (invUpdateResponse.ok) {
      const updatedInventory = await invUpdateResponse.json();
      console.log("Inventory updated successfully:", updatedInventory);
      return updatedInventory;
    } else {
      const invErrorText = await invUpdateResponse.text();
      console.error(`PUT inventory update attempt ${attempt} failed:`, invErrorText);
      await delay(5000); // wait 5 seconds before retrying
    }
  }
  throw new Error("Inventory update failed after all retries");
};

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

    // Fetch main listing details.
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
    console.log("Listing data fetched:", JSON.stringify(listingData));

    // Determine top-level price.
    // If the listing has variations, set a default minimal price (Etsy requires a price even if variations exist).
    let formattedPrice;
    if (listingData.has_variations) {
      formattedPrice = 0.20; // default minimal price for listings with variations
      console.log("Listing has variations; setting default top-level price:", formattedPrice);
    } else {
      if (listingData.price && typeof listingData.price === "object" &&
          listingData.price.amount && listingData.price.divisor) {
        formattedPrice = parseFloat((listingData.price.amount / listingData.price.divisor).toFixed(2));
      } else if (listingData.price == null || listingData.price === "") {
        formattedPrice = 0.20;
      } else {
        formattedPrice = parseFloat(listingData.price);
      }
      console.log("Computed price for duplicate (no variations):", formattedPrice);
    }

    // Build creation payload for new listing.
    // We omit detailed inventory here because variations will be updated in a separate call.
    const creationPayload = {
      quantity: listingData.quantity || 1,
      title: listingData.title || "Duplicated Listing",
      description: listingData.description || "",
      price: formattedPrice,
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
      processing_min: listingData.processing_min || null,
      processing_max: listingData.processing_max || null
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

    // Create new listing.
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

    // If the listing has variations, update its inventory.
    if (listingData.has_variations) {
      // Fetch the original inventory data.
      const originalInventory = await fetchOriginalInventory(listingId, token, clientId);
      if (!originalInventory) {
        console.warn("No inventory data fetched from original listing.");
      } else {
        // Transform inventory data to match Etsy’s requirements.
        const newInventoryPayload = transformInventory(originalInventory);
        console.log("Transformed inventory payload:", JSON.stringify(newInventoryPayload));
        // Wait briefly to allow the new listing’s inventory resource to be created.
        console.log("Waiting 5 seconds before updating inventory...");
        await delay(5000);
        // Update the new listing’s inventory.
        const newListingId = newListingData.listing_id;
        const etsyInventoryUrlNew = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${newListingId}/inventory`;
        const invUpdateResponse = await fetch(etsyInventoryUrlNew, {
          method: "PUT",
          headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${token}`,
            "x-api-key": clientId
          },
          body: JSON.stringify(newInventoryPayload)
        });
        console.log("Inventory update response status:", invUpdateResponse.status);
        if (!invUpdateResponse.ok) {
          const invErrorText = await invUpdateResponse.text();
          console.error("Inventory update failed:", invErrorText);
          // Optionally: implement retry logic here.
        } else {
          const updatedInventory = await invUpdateResponse.json();
          console.log("Inventory updated successfully:", updatedInventory);
        }
      }
    } else {
      console.log("Listing does not have variations; no inventory update needed.");
    }

    return {
      statusCode: 200,
      body: JSON.stringify(newListingData)
    };
  } catch (error) {
    console.error("Exception in etsyDuplicateListing:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};