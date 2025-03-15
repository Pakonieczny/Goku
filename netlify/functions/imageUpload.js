// /netlify/functions/imageUpload.js

const formidable = require("formidable");
const { Readable } = require("stream");
const FormData = require("form-data");
const fetch = require("node-fetch");
const fs = require("fs");

exports.handler = async function (event, context) {
  try {
    console.log("Received event headers:", event.headers);

    if (!event.headers["content-length"]) {
      const len = Buffer.byteLength(
        event.body,
        event.isBase64Encoded ? "base64" : "utf8"
      );
      event.headers["content-length"] = len;
      console.log("Computed content-length:", len);
    }

    // Convert the event body to a Buffer
    const bodyBuffer = event.isBase64Encoded
      ? Buffer.from(event.body, "base64")
      : Buffer.from(event.body, "utf8");
    const req = new Readable();
    req._read = () => {};
    req.push(bodyBuffer);
    req.push(null);
    req.headers = event.headers;

    console.log("Starting form parsing...");

    // Parse the multipart/form-data
    const form = formidable({ multiples: false });
    const parseForm = () =>
      new Promise((resolve, reject) => {
        form.parse(req, (err, fields, files) => {
          if (err) return reject(err);
          resolve({ fields, files });
        });
      });

    const { fields, files } = await parseForm();
    console.log("Parsed fields:", fields);
    console.log("Parsed files:", files);

    if (!files.file) {
      throw new Error("No file provided in the upload");
    }
    const file = files.file;
    console.log("File details:", {
      originalFilename: file.originalFilename,
      mimetype: file.mimetype,
      size: file.size,
    });

    const formData = new FormData();
    // Append the actual binary data
    if (file.filepath) {
      formData.append("image", fs.createReadStream(file.filepath), {
        filename: file.originalFilename,
        contentType: file.mimetype,
      });
    } else if (file.data) {
      formData.append("image", file.data, {
        filename: file.originalFilename,
        contentType: file.mimetype,
      });
    } else {
      throw new Error("No valid file data available.");
    }

    // Include listing_id, fileName, rank
    formData.append("listing_id", fields.listingId);
    formData.append("fileName", fields.fileName);
    formData.append("rank", fields.rank);

    // If we have alt_text, append it
    if (fields.alt_text && fields.alt_text.trim().length > 0) {
      formData.append("alt_text", fields.alt_text.trim());
      console.log("Adding alt_text:", fields.alt_text.trim());
    }

    const clientId = process.env.CLIENT_ID;
    const shopId = process.env.SHOP_ID;
    if (!clientId || !shopId) {
      throw new Error("CLIENT_ID and/or SHOP_ID environment variables are not set.");
    }
    const imageUploadUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${fields.listingId}/images`;
    console.log("Image Upload URL:", imageUploadUrl);

    const response = await fetch(imageUploadUrl, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${fields.token}`,
        "x-api-key": clientId,
        ...formData.getHeaders(),
      },
      body: formData,
    });

    console.log("Image upload response status:", response.status);
    const responseText = await response.text();
    console.log("Image upload response:", responseText);

    if (!response.ok) {
      throw new Error(`Error uploading image: ${response.status} - ${responseText}`);
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