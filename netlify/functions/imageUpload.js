const fetch = require("node-fetch");
const formidable = require("formidable");
const { Readable } = require("stream");
const fs = require("fs");
const FormData = require("form-data");

exports.handler = async (event, context) => {
  try {
    // Check for a Content-Type header
    const contentType = event.headers["content-type"] || event.headers["Content-Type"];
    if (!contentType) {
      throw new Error("Missing content-type header");
    }

    // Convert the event body into a Buffer. Netlify sends the body as base64 if binary.
    const bodyBuffer = event.isBase64Encoded
      ? Buffer.from(event.body, "base64")
      : Buffer.from(event.body);

    // Create a readable stream from the bodyBuffer
    const stream = new Readable();
    stream.push(bodyBuffer);
    stream.push(null);

    // Use formidable to parse the incoming form data.
    const form = formidable({ multiples: false });
    const { fields, files } = await new Promise((resolve, reject) => {
      form.parse(stream, (err, fields, files) => {
        if (err) reject(err);
        else resolve({ fields, files });
      });
    });

    console.log("Parsed fields:", fields);
    console.log("Parsed files:", files);

    // Extract required fields from the parsed data.
    const { listingId, token, fileName, rank } = fields;
    if (!listingId || !token || !fileName || !rank) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: "Missing required parameters: listingId, token, fileName, rank",
        }),
      };
    }

    // Get the file from the parsed files (assume the field name is "file")
    const file = files.file;
    if (!file) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "No file provided" }),
      };
    }
    console.log("File details:", file);

    // Build FormData for the Etsy API upload.
    // We use the file stream directly.
    const formData = new FormData();
    formData.append("image", fs.createReadStream(file.filepath), {
      filename: fileName,
      contentType: file.mimetype,
    });
    formData.append("rank", rank);

    // Retrieve CLIENT_ID and SHOP_ID from environment variables.
    const clientId = process.env.CLIENT_ID;
    const shopId = process.env.SHOP_ID;
    if (!clientId || !shopId) {
      throw new Error("Missing CLIENT_ID or SHOP_ID in environment variables");
    }

    // Construct the Etsy API URL for image upload.
    const imageUploadUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${listingId}/images`;
    console.log("Image Upload URL:", imageUploadUrl);
    console.log("FormData headers:", formData.getHeaders());

    // Make the POST request to Etsy’s API using the binary file.
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
    console.log("Image upload response text:", responseText);

    if (!response.ok) {
      return {
        statusCode: response.status,
        body: JSON.stringify({
          error: "Error uploading image",
          details: responseText,
        }),
      };
    }

    return {
      statusCode: 200,
      body: responseText,
    };
  } catch (error) {
    console.error("Exception in imageUpload handler:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
};