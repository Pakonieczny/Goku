const formidable = require("formidable");
const { Readable } = require("stream");
const FormData = require("form-data");
const fetch = require("node-fetch");

exports.handler = async function (event, context) {
  try {
    console.log("Received event headers:", event.headers);
    // Ensure content-length is provided (compute if missing)
    if (!event.headers["content-length"]) {
      if (event.isBase64Encoded && event.body) {
        event.headers["content-length"] = Buffer.byteLength(event.body, "base64");
        console.log("Computed content-length (base64):", event.headers["content-length"]);
      } else if (event.body) {
        event.headers["content-length"] = Buffer.byteLength(event.body, "utf8");
        console.log("Computed content-length (utf8):", event.headers["content-length"]);
      }
    }

    // Convert the event.body to a Buffer (using base64 if applicable)
    let bodyBuffer;
    if (event.isBase64Encoded) {
      bodyBuffer = Buffer.from(event.body, "base64");
    } else {
      bodyBuffer = Buffer.from(event.body, "utf8");
    }
    // Create a readable stream from the buffer
    const stream = Readable.from(bodyBuffer);

    // Use Formidable to parse the multipart form-data.
    // We simulate a request by passing a minimal object with headers and our stream.
    const form = formidable({ multiples: false });
    // Promisify form parsing:
    const parseForm = () =>
      new Promise((resolve, reject) => {
        form.parse({ headers: event.headers, pipe: stream }, (err, fields, files) => {
          if (err) {
            return reject(err);
          }
          resolve({ fields, files });
        });
      });

    const { fields, files } = await parseForm();
    console.log("Parsed fields:", fields);
    console.log("Parsed files:", files);

    // Ensure we have a file uploaded
    if (!files.file) {
      throw new Error("No file provided in the upload");
    }
    const file = files.file;
    console.log("File details:", {
      originalFilename: file.originalFilename,
      mimetype: file.mimetype,
      size: file.size
    });

    // Now create a FormData instance (from the form-data module)
    const formData = new FormData();
    // Append the file data as binary (using file.data which is a Buffer)
    formData.append("image", file.data, {
      filename: file.originalFilename,
      contentType: file.mimetype
    });
    // Append the additional parameters required by Etsy:
    formData.append("listing_id", fields.listingId);
    formData.append("fileName", fields.fileName);
    formData.append("rank", fields.rank);
    // (token is not sent in the FormData but used in the headers)

    console.log("FormData keys:", Array.from(formData.keys()));

    // Prepare Etsy API parameters:
    const clientId = process.env.CLIENT_ID;
    const shopId = process.env.SHOP_ID;
    if (!clientId || !shopId) {
      throw new Error("CLIENT_ID and/or SHOP_ID environment variables are not set.");
    }
    const imageUploadUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${fields.listingId}/images`;
    console.log("Image Upload URL:", imageUploadUrl);
    console.log("Uploading photo with FormData:");
    console.log("listingId:", fields.listingId);
    console.log("token:", fields.token);
    console.log("fileName:", fields.fileName);
    console.log("rank:", fields.rank);

    // Make the POST request to Etsy API.
    const response = await fetch(imageUploadUrl, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${fields.token}`,
        "x-api-key": clientId,
        // Pass along the FormData headers (includes the proper content-type with boundary)
        ...formData.getHeaders()
      },
      body: formData
    });

    console.log("Image upload response status:", response.status);
    const responseText = await response.text();
    console.log("Image upload response:", responseText);

    if (!response.ok) {
      throw new Error(`Error uploading image: ${response.status} - ${responseText}`);
    }

    return {
      statusCode: 200,
      body: responseText
    };
  } catch (error) {
    console.error("Exception in imageUpload handler:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};