const multipart = require("lambda-multipart-parser");
const FormData = require("form-data");
const fetch = require("node-fetch");

exports.handler = async function (event, context) {
  try {
    // Parse the multipart/form-data from the event using lambda-multipart-parser.
    // Ensure your Netlify function is configured with rawBody enabled.
    const result = await multipart.parse(event);
    console.log("Parsed result:", result);

    // Extract required fields from the parsed result.
    const { listingId, token, fileName, rank } = result.fields;
    if (!listingId || !token || !fileName || !rank) {
      console.error("Missing required fields: listingId, token, fileName, or rank.");
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing required fields: listingId, token, fileName, or rank" }),
      };
    }

    // Retrieve environment variables.
    const clientId = process.env.CLIENT_ID;
    const shopId = process.env.SHOP_ID;
    if (!clientId || !shopId) {
      console.error("Missing environment variables: CLIENT_ID or SHOP_ID.");
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing environment variables" }),
      };
    }

    // Build the Etsy image upload endpoint URL.
    const imageUploadUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${listingId}/images`;
    console.log("Image Upload URL:", imageUploadUrl);

    // Ensure a file was provided. Assume the file is in result.files[0].
    const file = result.files[0];
    if (!file) {
      console.error("File not provided in form data.");
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "File not provided" }),
      };
    }
    console.log("File details:", file);

    // Create a new FormData payload using the form-data package.
    const formData = new FormData();
    // Append the file from the Buffer.
    formData.append("file", file.content, {
      filename: file.filename || file.originalFilename,
      contentType: file.contentType || file.mimetype,
    });
    // Append the rank field.
    formData.append("rank", rank);

    // Log the keys in the FormData for verification.
    console.log("FormData keys:", Array.from(formData.keys()));

    // Send the POST request to Etsy.
    const response = await fetch(imageUploadUrl, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId,
        ...formData.getHeaders(),
      },
      body: formData,
    });

    console.log("Image upload response status:", response.status);
    const responseText = await response.text();
    console.log("Response text:", responseText);

    if (!response.ok) {
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: responseText }),
      };
    }

    let jsonResponse;
    try {
      jsonResponse = JSON.parse(responseText);
    } catch (parseError) {
      console.error("Error parsing response JSON:", parseError);
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Error parsing response JSON" }),
      };
    }

    return {
      statusCode: 200,
      body: JSON.stringify(jsonResponse),
    };

  } catch (error) {
    console.error("Exception in imageUpload handler:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
};