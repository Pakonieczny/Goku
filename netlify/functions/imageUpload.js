const fetch = require("node-fetch");

exports.handler = async function (event, context) {
  try {
    // Parse the incoming JSON body.
    const { listingId, token, fileName, dataURL, rank } = JSON.parse(event.body);
    if (!listingId || !token || !fileName || !dataURL) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing required parameters." }),
      };
    }
    console.log("Image upload initiated for listingId:", listingId);
    console.log("File name:", fileName);
    console.log("Rank:", rank || 1);

    // Retrieve SHOP_ID and CLIENT_ID from environment variables.
    const shopId = process.env.SHOP_ID;
    const clientId = process.env.CLIENT_ID;
    if (!shopId) {
      console.error("SHOP_ID environment variable is not set.");
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "SHOP_ID environment variable is not set." }),
      };
    }
    if (!clientId) {
      console.error("CLIENT_ID environment variable is not set.");
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "CLIENT_ID environment variable is not set." }),
      };
    }
    console.log("Using SHOP_ID:", shopId);
    console.log("Using CLIENT_ID:", clientId.slice(0, 5) + "*****");

    // Construct the Etsy API endpoint URL for uploading the image.
    const uploadUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${listingId}/images`;
    console.log("Uploading image to URL:", uploadUrl);

    // Prepare the payload.
    // Now, we send the full dataURL (including MIME type) instead of splitting it.
    const payload = {
      file: dataURL, // dataURL includes "data:image/jpeg;base64," or similar
      name: fileName,
      rank: rank || 1,
    };
    console.log("Upload payload:", JSON.stringify(payload, null, 2));

    // Make the POST request to upload the image, including the x-api-key header.
    const response = await fetch(uploadUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId,
      },
      body: JSON.stringify(payload),
    });
    console.log("Image upload response status:", response.status);
    if (!response.ok) {
      const errorText = await response.text();
      console.error("Error uploading image:", errorText);
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: errorText }),
      };
    }
    const data = await response.json();
    console.log("Image uploaded successfully:", data);
    return {
      statusCode: 200,
      body: JSON.stringify(data),
    };

  } catch (error) {
    console.error("Exception in imageUpload handler:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
};