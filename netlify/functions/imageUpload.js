const fetch = require("node-fetch");
const FormData = require("form-data");

exports.handler = async function (event, context) {
  try {
    // Parse the request body (assuming JSON format)
    const body = JSON.parse(event.body);
    const { listingId, token, fileName, dataURL, rank } = body;
    if (!listingId || !token || !fileName || !dataURL) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing required parameters: listingId, token, fileName, or dataURL" })
      };
    }
    console.log("Received listingId:", listingId);
    console.log("Received token:", token.slice(0, 5) + "*****");
    console.log("File Name:", fileName);
    console.log("Rank:", rank || 1);

    // Retrieve CLIENT_ID from environment variables.
    const clientId = process.env.CLIENT_ID;
    if (!clientId) {
      console.error("CLIENT_ID environment variable is not set.");
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "CLIENT_ID environment variable is not set." })
      };
    }
    console.log("Using CLIENT_ID:", clientId.slice(0, 5) + "*****");

    // Construct the Etsy image upload endpoint URL.
    // According to Etsy API v3 docs, the endpoint for listing images is:
    // POST https://api.etsy.com/v3/application/listings/{listingId}/images
    const imageUploadUrl = `https://api.etsy.com/v3/application/listings/${listingId}/images`;
    console.log("Image Upload URL:", imageUploadUrl);

    // Build the multipart/form-data payload using FormData.
    const form = new FormData();
    form.append("fileName", fileName);
    // Convert the dataURL (base64 string) to a Buffer.
    // (Assuming dataURL is the base64 portion only – if dataURL includes the data URI prefix, remove it first.)
    form.append("file", Buffer.from(dataURL, "base64"), fileName);
    form.append("rank", rank || 1);

    // Log the payload keys (the FormData body itself cannot be directly stringified)
    console.log("FormData payload keys:", [...form.keys()]);

    // Build headers. Note that form.getHeaders() includes the proper Content-Type with boundary.
    const headers = {
      "Authorization": `Bearer ${token}`,
      "x-api-key": clientId,
      ...form.getHeaders()
    };
    console.log("Image upload request headers:", headers);

    // Make the POST request to the image upload endpoint.
    const response = await fetch(imageUploadUrl, {
      method: "POST",
      headers,
      body: form
    });

    console.log("Image upload response status:", response.status);
    if (!response.ok) {
      const errorText = await response.text();
      console.error("Error uploading image. POST failed:", errorText);
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: "Error uploading image", details: errorText })
      };
    }

    const responseData = await response.json();
    console.log("Image uploaded successfully:", responseData);
    return {
      statusCode: 200,
      body: JSON.stringify(responseData)
    };

  } catch (error) {
    console.error("Exception in imageUpload handler:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};