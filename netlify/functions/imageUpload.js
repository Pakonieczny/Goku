const fetch = require("node-fetch");
const FormData = require("form-data");

exports.handler = async function (event, context) {
  try {
    // Parse the JSON body
    const body = JSON.parse(event.body);
    const { listingId, token, fileName, dataURL, rank } = body;

    // Validate required parameters
    if (!listingId || !token || !fileName || !dataURL) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing required parameters: listingId, token, fileName, or dataURL" }),
      };
    }
    console.log("Received parameters:", { listingId, fileName, rank });

    // Extract base64 content from the dataURL (assumes format "data:image/jpeg;base64,...")
    const parts = dataURL.split(",");
    if (parts.length < 2) {
      throw new Error("Invalid dataURL format");
    }
    const base64Data = parts[1];
    const buffer = Buffer.from(base64Data, "base64");

    // Create a FormData object to send the file
    const form = new FormData();
    // Append the file with a filename and content type (adjust contentType as needed)
    form.append("file", buffer, {
      filename: fileName,
      contentType: "image/jpeg",
    });
    // Append rank if provided (optional)
    if (rank !== undefined) {
      form.append("rank", rank);
    }

    // Build the Etsy API endpoint for image upload
    const endpoint = `https://api.etsy.com/v3/application/listings/${listingId}/images`;
    console.log("Uploading image to endpoint:", endpoint);

    // Retrieve CLIENT_ID from environment variables
    const clientId = process.env.CLIENT_ID;
    if (!clientId) {
      console.error("CLIENT_ID environment variable is not set.");
    } else {
      console.log("Using CLIENT_ID:", clientId.slice(0, 5) + "*****");
    }

    // Build headers. Note: FormData sets its own Content-Type header.
    const headers = {
      "Authorization": `Bearer ${token}`,
      "x-api-key": clientId,
    };
    console.log("Request headers:", headers);

    // Make the POST request to upload the image
    const response = await fetch(endpoint, {
      method: "POST",
      headers: headers,
      body: form,
    });
    console.log("Image upload response status:", response.status);

    const respText = await response.text();
    if (!response.ok) {
      console.error("Error uploading image:", respText);
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: respText }),
      };
    }
    console.log("Image uploaded successfully:", respText);
    return {
      statusCode: 200,
      body: respText,
    };
  } catch (error) {
    console.error("Exception in imageUpload handler:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
};