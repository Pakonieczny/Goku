const fs = require("fs");
const Formidable = require("formidable");
const FormData = require("form-data");
const fetch = require("node-fetch");

exports.handler = async function (event, context) {
  try {
    // Ensure that the raw body is available (Netlify functions must be configured to include it)
    if (!event.body) {
      throw new Error("No request body found");
    }
    
    console.log("Starting form parsing...");
    // Create a new instance of formidable.IncomingForm
    const form = new Formidable.IncomingForm();
    // Wrap parsing in a Promise so we can await it
    const { fields, files } = await new Promise((resolve, reject) => {
      form.parse(
        { headers: event.headers, body: event.body },
        (err, fields, files) => {
          if (err) return reject(err);
          resolve({ fields, files });
        }
      );
    });

    console.log("Parsed fields:", fields);
    console.log("Parsed files:", files);

    // Get required parameters from parsed fields
    const listingId = fields.listingId;
    const token = fields.token;
    const fileName = fields.fileName;
    const rank = fields.rank || "1";

    if (!listingId || !token || !fileName) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: "Missing required parameters: listingId, token, or fileName"
        }),
      };
    }

    // Ensure the file exists in the parsed files (assume the field is named "file")
    const fileData = files.file;
    if (!fileData) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "No file provided" }),
      };
    }
    
    // Read the file as a Buffer (do not convert to base64)
    const fileBuffer = fs.readFileSync(fileData.filepath);
    console.log("File data length:", fileBuffer.length);

    // Prepare the FormData to send to Etsy.
    // The field name must be "image" as per Etsy’s API
    const formData = new FormData();
    formData.append("image", fileBuffer, {
      filename: fileName,
      contentType: fileData.mimetype,
    });
    // Append the rank parameter as well
    formData.append("rank", rank);

    // Retrieve CLIENT_ID and SHOP_ID from environment variables.
    const clientId = process.env.CLIENT_ID;
    const shopId = process.env.SHOP_ID;
    if (!clientId || !shopId) {
      throw new Error("Missing CLIENT_ID or SHOP_ID environment variables");
    }
    console.log("Using CLIENT_ID:", clientId.slice(0, 5) + "*****");
    console.log("Using SHOP_ID:", shopId);

    // Construct the Etsy API endpoint URL for uploading images.
    const uploadUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${listingId}/images`;
    console.log("Image Upload URL:", uploadUrl);

    // Use formData.getHeaders() to get the correct Content-Type header with boundary
    const headers = {
      "Authorization": `Bearer ${token}`,
      "x-api-key": clientId,
      ...formData.getHeaders(),
    };

    console.log("Image upload request headers:", headers);

    // Make the POST request to Etsy's API
    const response = await fetch(uploadUrl, {
      method: "POST",
      headers: headers,
      body: formData,
    });

    console.log("Image upload response status:", response.status);
    const responseText = await response.text();
    if (!response.ok) {
      console.error("Error uploading image. Response:", responseText);
      return {
        statusCode: response.status,
        body: JSON.stringify({
          error: "Error uploading image",
          details: responseText,
        }),
      };
    }

    console.log("Image uploaded successfully. Response:", responseText);
    return {
      statusCode: 200,
      body: responseText,
    };

  } catch (err) {
    console.error("Exception in imageUpload handler:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message }),
    };
  }
};