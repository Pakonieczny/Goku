const Formidable = require("formidable");
const streamifier = require("streamifier");
const fetch = require("node-fetch");
const fs = require("fs");

exports.handler = async function (event, context) {
  try {
    // Ensure we have a content-type header in the event.
    const incomingContentType =
      (event.headers && (event.headers["content-type"] || event.headers["Content-Type"])) || "";
    if (!incomingContentType) {
      throw new Error("Missing content-type header in the request.");
    }
    console.log("Incoming Content-Type:", incomingContentType);

    // Convert the event body to a Buffer.
    const buffer = event.isBase64Encoded
      ? Buffer.from(event.body, "base64")
      : Buffer.from(event.body);

    // Create a stream from the buffer.
    const reqStream = streamifier.createReadStream(buffer);
    // Attach headers to the stream (include both content-length and content-type).
    reqStream.headers = {
      "content-length": buffer.length,
      "content-type": incomingContentType,
    };

    // Use Formidable to parse the incoming multipart/form-data.
    const form = new Formidable.IncomingForm();
    form.keepExtensions = true;

    const parsed = await new Promise((resolve, reject) => {
      form.parse(reqStream, (err, fields, files) => {
        if (err) {
          return reject(err);
        }
        resolve({ fields, files });
      });
    });

    console.log("Parsed fields:", parsed.fields);
    console.log("Parsed files:", parsed.files);

    // Extract required fields from parsed data.
    const { listingId, token, fileName, rank } = parsed.fields;
    if (!listingId || !token || !fileName) {
      throw new Error("Missing required parameters: listingId, token, or fileName");
    }

    // Assume that the uploaded file is available as parsed.files.file.
    const fileData = parsed.files.file;
    if (!fileData) {
      throw new Error("No valid image file provided.");
    }

    // Determine MIME type from the parsed file data.
    const mimeType = fileData.mimetype || "application/octet-stream";
    console.log("Determined MIME type:", mimeType);

    // Create a FormData instance using the "form-data" package.
    const FormData = require("form-data");
    const formData = new FormData();
    formData.append("listingId", listingId);
    formData.append("fileName", fileName);
    formData.append("rank", rank || "1");

    // Append the file data.
    if (fileData.filepath) {
      // If Formidable saved the file to a temporary path, use a stream.
      formData.append("file", fs.createReadStream(fileData.filepath), {
        contentType: mimeType,
        filename: fileName,
      });
    } else {
      // Otherwise, assume fileData is a Buffer.
      formData.append("file", fileData, {
        contentType: mimeType,
        filename: fileName,
      });
    }

    // Instead of logging keys (which causes an error), just log that FormData is ready.
    console.log("FormData prepared.");

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
        // Do not set Content-Type manually; formData will set it.
      },
      body: formData,
    });

    console.log("Image upload response status:", response.status);
    const responseText = await response.text();
    if (!response.ok) {
      console.error("Error uploading image. POST failed:", responseText);
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: responseText }),
      };
    }

    let responseData;
    try {
      responseData = JSON.parse(responseText);
    } catch (jsonError) {
      responseData = responseText;
    }
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