const fetch = require("node-fetch");

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

    // Retrieve CLIENT_ID and SHOP_ID from environment variables.
    const clientId = process.env.CLIENT_ID;
    const shopId = process.env.SHOP_ID;
    if (!clientId) {
      console.error("CLIENT_ID environment variable is not set.");
    }
    if (!shopId) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "SHOP_ID environment variable is not set." })
      };
    }
    console.log("Using CLIENT_ID:", clientId.slice(0, 5) + "*****");
    console.log("Using SHOP_ID:", shopId);

    // --- Step 1: Fetch original listing details ---
    const etsyGetUrl = `https://api.etsy.com/v3/application/listings/${listingId}`;
    const getResponse = await fetch(etsyGetUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId
      }
    });
    console.log("GET listing response status:", getResponse.status);
    if (!getResponse.ok) {
      const errorText = await getResponse.text();
      console.error("GET listing request failed:", errorText);
      return {
        statusCode: getResponse.status,
        body: JSON.stringify({ error: "GET listing request failed", details: errorText })
      };
    }
    const listingData = await getResponse.json();
    console.log("Listing data fetched:", listingData);

    // --- Step 2: Fetch inventory details for the listing (SKUs, variations, etc.) ---
    const etsyInventoryUrl = `https://api.etsy.com/v3/application/listings/${listingId}/inventory`;
    let inventoryData = {};
    const inventoryResponse = await fetch(etsyInventoryUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId
      }
    });
    console.log("GET inventory response status:", inventoryResponse.status);
    if (inventoryResponse.ok) {
      inventoryData = await inventoryResponse.json();
      console.log("Inventory data fetched:", inventoryData);
      // Process each product: remove invalid keys.
      if (inventoryData.products && Array.isArray(inventoryData.products)) {
        inventoryData.products = inventoryData.products.map(product => {
          let newProduct = { ...product };
          // Remove keys that Etsy may not accept on update.
          delete newProduct.product_id;
          delete newProduct.is_deleted;
          delete newProduct.scale_name;
          // Process offerings if available.
          if (newProduct.offerings && Array.isArray(newProduct.offerings)) {
            newProduct.offerings = newProduct.offerings.map(offering => {
              let newOffering = { ...offering };
              delete newOffering.scale_name;
              return newOffering;
            });
          }
          return newProduct;
        });
      }
    } else {
      const invError = await inventoryResponse.text();
      console.error("GET inventory request failed:", invError);
      // Continue with empty inventory if not available.
    }

    // --- Step 3: Process price ---
    let priceValue;
    if (listingData.price && typeof listingData.price === "object" &&
        listingData.price.amount && listingData.price.divisor) {
      priceValue = listingData.price.amount / listingData.price.divisor;
    } else if (listingData.price == null || listingData.price === "") {
      priceValue = 0.00;
    } else {
      priceValue = parseFloat(listingData.price);
    }
    const formattedPrice = parseFloat(priceValue.toFixed(2));

    // --- Step 4: Build payload for duplicating the listing ---
    const payload = {
      quantity: listingData.quantity || 1,
      title: listingData.title || "Duplicated Listing",
      description: listingData.description || "",
      price: formattedPrice,
      who_made: listingData.who_made || "i_did",
      when_made: listingData.when_made || "made_to_order",
      taxonomy_id: listingData.taxonomy_id || 0,
      shipping_profile_id: listingData.shipping_profile_id, // required for physical listings.
      return_policy_id: listingData.return_policy_id,       // required for physical listings.
      tags: listingData.tags || [],
      materials: listingData.materials || [],
      personalization_instructions: listingData.personalization_instructions || "",
      style: listingData.style || [],
      // Use inventory data if available; otherwise fallback to listingData.
      skus: (inventoryData && inventoryData.products && inventoryData.products.length > 0)
             ? inventoryData.products.map(product => product.sku || "")
             : (listingData.skus || []),
      variations: (inventoryData && inventoryData.products && inventoryData.products.length > 0)
             ? inventoryData.products.map(product => product.variations || [])
             : [],
      has_variations: (typeof listingData.has_variations === "boolean") ? listingData.has_variations : false,
      is_customizable: (typeof listingData.is_customizable === "boolean") ? listingData.is_customizable : false,
      is_personalizable: (typeof listingData.is_personalizable === "boolean") ? listingData.is_personalizable : false
    };

    console.log("Payload for new listing:", payload);

    // --- Step 5: Create the new (duplicated) listing ---
    const etsyPostUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings`;
    const postResponse = await fetch(etsyPostUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId
      },
      body: JSON.stringify(payload)
    });
    console.log("POST listing response status:", postResponse.status);
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

    // Extract new listing ID (may be under listing_id or id depending on response format)
    const newListingId = newListingData.listing_id || newListingData.id;
    if (!newListingId) {
      throw new Error("New listing ID not found in response.");
    }

    // --- Step 6: Update inventory for the new listing ---
    if (inventoryData && Object.keys(inventoryData).length > 0) {
      const etsyInventoryUpdateUrl = `https://api.etsy.com/v3/application/listings/${newListingId}/inventory`;
      const putResponse = await fetch(etsyInventoryUpdateUrl, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${token}`,
          "x-api-key": clientId
        },
        body: JSON.stringify(inventoryData)
      });
      console.log("PUT inventory update response status:", putResponse.status);
      if (!putResponse.ok) {
        const putError = await putResponse.text();
        console.error("Error updating inventory. PUT failed:", putError);
        // Optionally, you can return error details or continue.
      } else {
        const updatedInventory = await putResponse.json();
        console.log("Inventory updated for new listing:", updatedInventory);
        newListingData.updated_inventory = updatedInventory;
      }
    } else {
      console.log("No inventory data available to update for new listing.");
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