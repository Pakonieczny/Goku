// /netlify/functions/imageUpload.js

const formidable = require("formidable");
const { Readable } = require("stream");
const FormData = require("form-data");
const fetch = require("node-fetch");
const fs = require("fs");

exports.handler = async function (event, context) {
  try {
    console.log("Received event headers:", event.headers || {});

    // Ensure content-length exists (some providers omit it on base64 bodies)
    if (!event.headers?.["content-length"]) {
      const len = Buffer.byteLength(
        event.body || "",
        event.isBase64Encoded ? "base64" : "utf8"
      );
      event.headers = event.headers || {};
      event.headers["content-length"] = len;
      console.log("Computed content-length:", len);
    }

    // Convert the event body to a readable stream for formidable
    const bodyBuffer = event.isBase64Encoded
      ? Buffer.from(event.body || "", "base64")
      : Buffer.from(event.body || "", "utf8");

    const req = new Readable();
    req._read = () => {};
    req.push(bodyBuffer);
    req.push(null);
    req.headers = event.headers || {};

    console.log("Starting form parsing...");

    // Parse multipart/form-data
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

    // Basic presence checks for the uploaded file
    if (!files || !files.file) {
      return { statusCode: 400, body: JSON.stringify({ error: "No file provided in the upload" }) };
    }
    const file = files.file;
    console.log("File details:", {
      originalFilename: file.originalFilename,
      mimetype: file.mimetype,
      size: file.size,
    });

    // Env sanity
    const clientId = process.env.CLIENT_ID;
    const clientSecret =
      process.env.CLIENT_SECRET ||
      process.env.ETSY_CLIENT_SECRET ||
      process.env.ETSY_SHARED_SECRET;
    const shopId = process.env.SHOP_ID;
    if (!clientId || !shopId) {
      console.error("Missing envs:", { hasClientId: !!clientId, hasShopId: !!shopId });
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "CLIENT_ID and/or SHOP_ID environment variables are not set." }),
      };
    }
    if (!clientSecret) {
      console.error("Missing Etsy shared secret env var for x-api-key header.");
      console.log("Env presence:", {
        CLIENT_SECRET: !!process.env.CLIENT_SECRET,
        ETSY_CLIENT_SECRET: !!process.env.ETSY_CLIENT_SECRET,
        ETSY_SHARED_SECRET: !!process.env.ETSY_SHARED_SECRET,
      });
      return {
        statusCode: 500,
        body: JSON.stringify({
          error: "Missing Etsy shared secret env var for x-api-key header.",
          checked: ["CLIENT_SECRET", "ETSY_CLIENT_SECRET", "ETSY_SHARED_SECRET"],
        }),
      };
    }

    const xApiKey = `${String(clientId).trim()}:${String(clientSecret).trim()}`;

    // Input validation from fields
    const listingId = (fields.listingId || "").toString().trim();
    const token = (fields.token || "").toString().trim();
    const rank = (fields.rank ?? "").toString().trim();
    let altText = (fields.alt_text || "").toString();

    if (!listingId) {
      return { statusCode: 400, body: JSON.stringify({ error: "Missing listingId" }) };
    }
    if (!token) {
      return { statusCode: 401, body: JSON.stringify({ error: "Missing access token" }) };
    }

    // Prepare FormData with only Etsy-supported fields
    const formData = new FormData();

    // Binary image body
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
      return { statusCode: 400, body: JSON.stringify({ error: "No valid file data available" }) };
    }

    // Optional: rank (1..10 typically). Send only if present
    if (rank !== "") {
      formData.append("rank", rank);
    }

    // Optional: alt_text (clamp to ≤250 chars for safety)
    if (altText && altText.trim().length > 0) {
      altText = altText.trim().slice(0, 250);
      formData.append("alt_text", altText);
      console.log("Adding alt_text (clamped):", altText);
    }

    // Build Etsy endpoint
    const imageUploadUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${encodeURIComponent(
      listingId
    )}/images`;
    console.log("Image Upload URL:", imageUploadUrl);

    // POST to Etsy
    const response = await fetch(imageUploadUrl, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "x-api-key": xApiKey,
        ...formData.getHeaders(),
      },
      body: formData,
    });

    const responseText = await response.text();
    console.log("Image upload response status:", response.status);
    console.log("Image upload response:", responseText);

    // Forward Etsy's status/body transparently (don't mask as 500)
    if (!response.ok) {
      return {
        statusCode: response.status,
        body: responseText || JSON.stringify({ error: "Etsy image upload failed" }),
      };
    }

    return {
      statusCode: 200,
      body: responseText,
    };
  } catch (error) {
    console.error("Exception in imageUpload handler:", error);
    // True server-side failures only
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
};