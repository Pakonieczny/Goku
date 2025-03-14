const FormData = require("form-data");
const fs = require("fs");
const fetch = require("node-fetch");
const { IncomingForm } = require("formidable");

exports.handler = async function (event, context) {
  try {
    // Parse the incoming request using formidable
    return new Promise((resolve, reject) => {
      const form = new IncomingForm();
      form.parse(event, async (err, fields, files) => {
        if (err) {
          console.error("Error parsing form:", err);
          return resolve({
            statusCode: 400,
            body: JSON.stringify({ error: "Error parsing form" }),
          });
        }

        console.log("Parsed fields:", fields);
        console.log("Parsed files:", files);

        const { listingId, token, fileName, rank } = fields;
        if (!listingId || !token || !fileName || !rank) {
          console.error("Missing required fields.");
          return resolve({
            statusCode: 400,
            body: JSON.stringify({ error: "Missing required fields: listingId, token, fileName, or rank" }),
          });
        }

        const clientId = process.env.CLIENT_ID;
        const shopId = process.env.SHOP_ID;
        if (!clientId || !shopId) {
          console.error("Missing environment variables (CLIENT_ID or SHOP_ID).");
          return resolve({
            statusCode: 500,
            body: JSON.stringify({ error: "Missing environment variables" }),
          });
        }

        // Build the Etsy image upload URL
        const imageUploadUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${listingId}/images`;
        console.log("Image Upload URL:", imageUploadUrl);

        // Ensure a file was provided
        const file = files.file;
        if (!file) {
          console.error("File not provided in form data.");
          return resolve({
            statusCode: 400,
            body: JSON.stringify({ error: "File not provided" }),
          });
        }

        // Create a new FormData payload using the "form-data" package
        const formData = new FormData();
        // Append the file: use createReadStream from fs with the temporary file path
        formData.append("file", fs.createReadStream(file.filepath), {
          filename: file.originalFilename,
          contentType: file.mimetype
        });
        // Append rank (Etsy expects rank as a field)
        formData.append("rank", rank);

        // Log the FormData keys (for debugging purposes)
        console.log("FormData keys:", Array.from(formData.keys()));

        // Make the POST request to the Etsy API for image upload
        const response = await fetch(imageUploadUrl, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${token}`,
            "x-api-key": clientId,
            ...formData.getHeaders()
          },
          body: formData
        });

        console.log("Image upload response status:", response.status);
        const responseText = await response.text();
        console.log("Response text:", responseText);

        if (!response.ok) {
          return resolve({
            statusCode: response.status,
            body: JSON.stringify({ error: responseText }),
          });
        }

        let jsonResponse;
        try {
          jsonResponse = JSON.parse(responseText);
        } catch (parseError) {
          console.error("Error parsing response JSON:", parseError);
          return resolve({
            statusCode: 500,
            body: JSON.stringify({ error: "Error parsing response JSON" }),
          });
        }

        return resolve({
          statusCode: 200,
          body: JSON.stringify(jsonResponse),
        });
      });
    });
  } catch (error) {
    console.error("Exception in imageUpload handler:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
};