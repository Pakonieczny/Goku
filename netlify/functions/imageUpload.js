const fetch = require("node-fetch");
const multipart = require("parse-multipart-data");
const FormData = require("form-data");

exports.handler = async function (event, context) {
  try {
    // Ensure the content-type header is present
    const contentType = event.headers["content-type"] || event.headers["Content-Type"];
    if (!contentType) {
      console.error("Missing content-type header.");
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing content-type header" }),
      };
    }
    console.log("Content-Type:", contentType);

    // Convert the event body to a Buffer (decode from base64 if necessary)
    const bodyBuffer = event.isBase64Encoded
      ? Buffer.from(event.body, "base64")
      : Buffer.from(event.body, "utf8");

    // Parse the multipart form data
    const boundary = multipart.getBoundary(contentType);
    if (!boundary) {
      throw new Error("Unable to determine boundary from content-type header.");
    }
    const parts = multipart.parse(bodyBuffer, boundary);
    console.log("Parsed parts:", parts);

    // Separate fields and files from parsed parts
    const fields = {};
    const files = {};
    parts.forEach(part => {
      // If part.filename exists, it's a file; otherwise, it's a field
      if (part.filename) {
        files[part.name] = part;
      } else {
        fields[part.name] = part.data.toString();
      }
    });

    console.log("Parsed fields:", fields);
    console.log("Parsed files:", files);

    // Extract expected fields
    const { listingId, token, fileName, rank } = fields;
    if (!listingId || !token || !fileName || !rank) {
      console.error("Missing one or more required fields: listingId, token, fileName, rank.");
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing required fields: listingId, token, fileName, or rank" }),
      };
    }

    // Ensure file part exists
    const filePart = files["file"];
    if (!filePart) {
      console.error("Missing file part.");
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing file part" }),
      };
    }

    // Log a substring and length of the file data (for debugging)
    const fileDataLength = filePart.data.length;
    console.log(`File data length: ${fileDataLength} bytes`);
    console.log(`File data preview: ${filePart.data.toString("base64").substring(0, 50)}...`);

    // Build a FormData instance to send the file to Etsy
    const formData = new FormData();
    formData.append("file", filePart.data, { filename: fileName, contentType: filePart.type });
    formData.append("rank", rank);

    // Retrieve CLIENT_ID and SHOP_ID from environment variables
    const clientId = process.env.CLIENT_ID;
    const shopId = process.env.SHOP_ID;
    if (!clientId || !shopId) {
      console.error("Missing CLIENT_ID or SHOP_ID environment variables.");
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing CLIENT_ID or SHOP_ID environment variables" }),
      };
    }
    console.log("Using CLIENT_ID:", clientId.slice(0, 5) + "*****");
    console.log("Using SHOP_ID:", shopId);

    // Construct the Etsy API endpoint URL for image upload.
    const imageUploadUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${listingId}/images`;
    console.log("Image Upload URL:", imageUploadUrl);

    // Log the FormData headers (for troubleshooting)
    const formHeaders = formData.getHeaders();
    console.log("FormData headers:", formHeaders);

    // Make the POST request to upload the image
    const response = await fetch(imageUploadUrl, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId,
        ...formHeaders
      },
      body: formData,
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

    const respData = await response.json();
    console.log("Image uploaded successfully:", respData);
    return {
      statusCode: 200,
      body: JSON.stringify(respData),
    };

  } catch (error) {
    console.error("Exception in imageUpload handler:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
};