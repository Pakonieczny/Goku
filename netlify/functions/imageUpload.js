const fetch = require("node-fetch");
const formidable = require("formidable");
const FormData = require("form-data");
const fs = require("fs").promises;

exports.handler = async function (event, context) {
  try {
    // Only allow POST requests
    if (event.httpMethod !== "POST") {
      return {
        statusCode: 405,
        body: JSON.stringify({ error: "Method Not Allowed" }),
      };
    }

    // If the body is base64 encoded, decode it
    const buffer = event.isBase64Encoded
      ? Buffer.from(event.body, "base64")
      : Buffer.from(event.body, "utf8");

    // Use formidable to parse the incoming multipart/form-data
    const form = new formidable.IncomingForm();
    // Wrap the form.parse in a promise
    const { fields, files } = await new Promise((resolve, reject) => {
      form.parse({ headers: event.headers, body: buffer }, (err, fields, files) => {
        if (err) {
          reject(err);
        } else {
          resolve({ fields, files });
        }
      });
    });

    // Extract required fields from the parsed form data
    const { listingId, token, fileName, rank } = fields;
    if (!listingId || !token || !fileName || !rank) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing required fields (listingId, token, fileName, or rank)" }),
      };
    }

    // Ensure the file was uploaded
    if (!files.file) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "No file uploaded" }),
      };
    }
    const fileField = files.file;
    // Read the file data from the temporary file path
    const fileData = await fs.readFile(fileField.path);
    console.log("File data length:", fileData.length);

    // Retrieve environment variables
    const clientId = process.env.CLIENT_ID;
    const shopId = process.env.SHOP_ID;
    if (!clientId || !shopId) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing CLIENT_ID or SHOP_ID environment variables" }),
      };
    }

    // Build the FormData to send to the Etsy API
    const formData = new FormData();
    formData.append("file", fileData, fileName);
    formData.append("listingId", listingId);
    formData.append("rank", rank);

    // Construct the Etsy API endpoint URL for image upload.
    const etsyUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${listingId}/images`;

    // Log the outgoing details for troubleshooting
    console.log("Etsy Image Upload URL:", etsyUrl);
    console.log("Outgoing Headers:", {
      "Authorization": `Bearer ${token}`,
      "x-api-key": clientId,
      ...formData.getHeaders(),
    });

    // Make the POST request to the Etsy API
    const response = await fetch(etsyUrl, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId,
        ...formData.getHeaders(),
      },
      body: formData,
    });

    console.log("Image upload response status:", response.status);
    const resultText = await response.text();
    console.log("Image upload response text:", resultText);

    if (!response.ok) {
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: resultText }),
      };
    }

    return {
      statusCode: 200,
      body: resultText,
    };

  } catch (error) {
    console.error("Exception in imageUpload handler:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
};