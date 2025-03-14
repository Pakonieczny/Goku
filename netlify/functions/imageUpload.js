const fetch = require("node-fetch");
const FormData = require("form-data");

exports.handler = async function (event, context) {
  try {
    // Parse the incoming JSON body.
    const body = JSON.parse(event.body);
    const { listingId, token, fileName, dataURL, rank } = body;
    if (!listingId || !token || !fileName || !dataURL) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing required parameters: listingId, token, fileName, or dataURL" }),
      };
    }
    console.log("Received listingId:", listingId);
    console.log("Received token:", token.slice(0, 5) + "*****");
    console.log("File Name:", fileName);
    console.log("Rank:", rank || 1);

    // Retrieve CLIENT_ID and SHOP_ID from environment variables.
    const clientId = process.env.CLIENT_ID;
    const shopId = process.env.SHOP_ID;
    if (!clientId) {
      console.error("CLIENT_ID environment variable is not set.");
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "CLIENT_ID environment variable is not set." }),
      };
    }
    if (!shopId) {
      console.error("SHOP_ID environment variable is not set.");
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "SHOP_ID environment variable is not set." }),
      };
    }
    console.log("Using CLIENT_ID:", clientId.slice(0, 5) + "*****");
    console.log("Using SHOP_ID:", shopId);

    // Construct the Etsy image upload endpoint URL.
    const imageUploadUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${listingId}/images`;
    console.log("Image Upload URL:", imageUploadUrl);

    // Extract MIME type from dataURL if available.
    let mimeType = "application/octet-stream"; // default fallback
    let base64Data = dataURL;
    if (dataURL.startsWith("data:")) {
      const matches = dataURL.match(/^data:([^;]+);base64,(.*)$/);
      if (matches && matches.length === 3) {
        mimeType = matches[1];
        base64Data = matches[2];
      }
    }
    console.log("Determined MIME type:", mimeType);

    // Build the multipart/form-data payload using FormData.
    const form = new FormData();
    form.append("fileName", fileName);
    form.append("file", Buffer.from(base64Data, "base64"), {
      filename: fileName,
      contentType: mimeType,
    });
    form.append("rank", rank || 1);
    console.log("FormData boundary:", form.getBoundary());

    // Build request headers. form.getHeaders() adds the proper Content-Type with boundary.
    const headers = {
      "Authorization": `Bearer ${token}`,
      "x-api-key": clientId,
      ...form.getHeaders(),
    };
    console.log("Image upload request headers:", headers);

    // Make the POST request to the Etsy image upload endpoint.
    const response = await fetch(imageUploadUrl, {
      method: "POST",
      headers,
      body: form,
    });

    console.log("Image upload response status:", response.status);
    if (!response.ok) {
      const errorText = await response.text();
      console.error("Error uploading image. POST failed:", errorText);
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: "Error uploading image", details: errorText }),
      };
    }

    const responseData = await response.json();
    console.log("Image uploaded successfully:", responseData);
    return {
      statusCode: 200,
      body: JSON.stringify(responseData),
    };

  } catch (error) {
    console.error("Exception in imageUpload handler:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
};