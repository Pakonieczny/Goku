const Formidable = require("formidable");
const streamifier = require("streamifier");
const fetch = require("node-fetch");

exports.handler = async function (event, context) {
  try {
    // Check if the incoming request body is base64 encoded.
    const buffer = event.isBase64Encoded
      ? Buffer.from(event.body, "base64")
      : Buffer.from(event.body);
      
    // Create a stream from the buffer.
    const reqStream = streamifier.createReadStream(buffer);

    // Create a new instance of Formidable.IncomingForm.
    const form = new Formidable.IncomingForm();
    form.keepExtensions = true;

    // Parse the stream. Formidable expects a Node.js request-like object.
    const parsed = await new Promise((resolve, reject) => {
      form.parse(reqStream, (err, fields, files) => {
        if (err) {
          return reject(err);
        }
        resolve({ fields, files });
      });
    });

    // Log parsed fields and files for troubleshooting.
    console.log("Parsed fields:", parsed.fields);
    console.log("Parsed files:", parsed.files);

    // Ensure required fields are provided.
    const { listingId, token, fileName, rank } = parsed.fields;
    if (!listingId || !token || !fileName) {
      throw new Error("Missing one or more required parameters: listingId, token, fileName");
    }

    // Assume that the uploaded file is in parsed.files.file (or adjust based on your form field name)
    const fileData = parsed.files.file;
    if (!fileData) {
      throw new Error("No valid image file provided.");
    }

    // Determine MIME type – Formidable should provide the mimetype.
    const mimeType = fileData.mimetype || "application/octet-stream";
    console.log("Determined MIME type:", mimeType);

    // Create a new FormData object to send to Etsy.
    // (Using the "form-data" package)
    const FormData = require("form-data");
    const formData = new FormData();
    formData.append("listingId", listingId);
    formData.append("fileName", fileName);
    formData.append("rank", rank || "1");
    formData.append("file", fileData.filepath ? require("fs").createReadStream(fileData.filepath) : fileData, {
      contentType: mimeType,
      filename: fileName
    });

    // Log a portion of the FormData (note that FormData objects cannot be fully stringified).
    console.log("FormData prepared with keys:", Array.from(formData.keys()));

    // Retrieve CLIENT_ID and SHOP_ID from environment variables.
    const clientId = process.env.CLIENT_ID;
    const shopId = process.env.SHOP_ID;
    if (!clientId || !shopId) {
      throw new Error("Missing CLIENT_ID or SHOP_ID environment variable");
    }
    console.log("Using CLIENT_ID:", clientId.slice(0, 5) + "*****");
    console.log("Using SHOP_ID:", shopId);

    // Build the Etsy image upload endpoint URL.
    const etsyImageUploadUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${listingId}/images`;
    console.log("Image Upload URL:", etsyImageUploadUrl);

    // Make the POST request to Etsy.
    const response = await fetch(etsyImageUploadUrl, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId,
        // Note: Do not manually set the Content-Type header when using formData;
        // let formData set it including the boundary.
      },
      body: formData
    });

    console.log("Image upload response status:", response.status);
    const responseText = await response.text();
    if (!response.ok) {
      console.error("Error uploading image. POST failed:", responseText);
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: responseText })
      };
    }

    // Try to parse response as JSON.
    let responseData;
    try {
      responseData = JSON.parse(responseText);
    } catch (jsonError) {
      responseData = responseText;
    }
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