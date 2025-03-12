const fetch = require("node-fetch");

const RETRY_DELAY = 5000; // 5 seconds delay between retries
const MAX_RETRIES = 3;

async function updateInventory(newListingId, token, clientId, inventoryPayload) {
  const inventoryUrl = `https://api.etsy.com/v3/application/listings/${newListingId}/inventory`;
  let retries = 0;
  while (retries < MAX_RETRIES) {
    console.log(`Attempting PUT inventory update (attempt ${retries + 1})...`);
    const putResponse = await fetch(inventoryUrl, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId
      },
      body: JSON.stringify(inventoryPayload)
    });
    console.log("PUT inventory update response status:", putResponse.status);
    if (putResponse.ok) {
      return await putResponse.json();
    } else {
      const errorText = await putResponse.text();
      console.error("PUT inventory update attempt failed:", errorText);
      if (putResponse.status === 404) {
        console.log("Inventory resource not found; waiting before retrying...");
      }
      retries++;
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
    }
  }
  throw new Error("Inventory update failed after all retries");
}

async function createInventory(newListingId, token, clientId, inventoryPayload) {
  const inventoryUrl = `https://api.etsy.com/v3/application/listings/${newListingId}/inventory`;
  console.log("Attempting POST inventory creation...");
  const postResponse = await fetch(inventoryUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${token}`,
      "x-api-key": clientId
    },
    body: JSON.stringify(inventoryPayload)
  });
  console.log("POST inventory creation response status:", postResponse.status);
  if (postResponse.ok) {
    return await postResponse.json();
  } else {
    const errorText = await postResponse.text();
    throw new Error("Error creating inventory with POST: " + errorText);
  }
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

    // Build payload for duplicating the listing.
    // Note: We are intentionally omitting a top-level "price" field because when variations exist, price comes from each offering.
    const creationPayload = {
      quantity: listingData.quantity || 1,
      title: listingData.title || "Duplicated Listing",
      description: listingData.description || "",
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

    // Create the new listing.
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

    // Prepare inventory payload if original listing has inventory data.
    let inventoryPayload = null;
    if (listingData.inventory && listingData.inventory.products) {
      inventoryPayload = {
        products: listingData.inventory.products.map(product => {
          return {
            sku: product.sku || "",
            offerings: product.offerings.map(offering => ({
              // Convert price to a float (Etsy expects a single float value).
              price: parseFloat((offering.price.amount / offering.price.divisor).toFixed(2)),
              quantity: offering.quantity,
              is_enabled: offering.is_enabled
            })),
            // Map property_values to properties while excluding disallowed keys.
            properties: (product.property_values || []).map(prop => ({
              name: prop.property_name,
              value: (prop.values && prop.values[0]) ? prop.values[0] : ""
            }))
          };
        })
      };
    } else {
      console.log("No inventory data present in original listing.");
    }

    if (inventoryPayload) {
      console.log("Transformed inventory payload:", inventoryPayload);
      // Wait before attempting inventory update.
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));

      let inventoryUpdateResult;
      try {
        inventoryUpdateResult = await updateInventory(newListingData.listing_id, token, clientId, inventoryPayload);
        console.log("Inventory update result:", inventoryUpdateResult);
      } catch (updateError) {
        console.error("Error updating inventory. Attempting inventory creation...");
        try {
          inventoryUpdateResult = await createInventory(newListingData.listing_id, token, clientId, inventoryPayload);
          console.log("Inventory creation result:", inventoryUpdateResult);
        } catch (creationError) {
          console.error("Error creating inventory with POST:", creationError);
          throw creationError;
        }
      }
    } else {
      console.log("No inventory payload to update.");
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