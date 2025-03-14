// imageUpload.js
const Busboy = require('busboy').default || require('busboy');
const FormData = require("form-data");
const fetch = require("node-fetch");

exports.handler = async function (event, context) {
  return new Promise((resolve, reject) => {
    try {
      console.log("Received event headers:", event.headers);
      // Create a new Busboy instance using the headers from the event.
      const busboy = new Busboy({ headers: event.headers });
      
      let listingId, token, fileName, rank;
      let fileBuffer = Buffer.alloc(0);

      // Parse field data.
      busboy.on("field", (fieldname, val) => {
        console.log(`Field [${fieldname}]: ${val}`);
        if (fieldname === "listingId") {
          listingId = val;
        } else if (fieldname === "token") {
          token = val;
        } else if (fieldname === "fileName") {
          fileName = val;
        } else if (fieldname === "rank") {
          rank = val;
        }
      });

      // Accumulate file data.
      busboy.on("file", (fieldname, file, filename, encoding, mimetype) => {
        console.log(`Receiving file [${fieldname}]: filename: ${filename}, encoding: ${encoding}, mimetype: ${mimetype}`);
        file.on("data", (data) => {
          fileBuffer = Buffer.concat([fileBuffer, data]);
        });
        file.on("end", () => {
          console.log(`Finished receiving file [${fieldname}]. Total size: ${fileBuffer.length} bytes`);
        });
      });

      busboy.on("finish", async () => {
        try {
          // Check that all required parameters are present.
          if (!listingId || !token || !fileName || !fileBuffer.length) {
            const errorMessage = "Missing required parameters: listingId, token, fileName, or file data";
            console.error(errorMessage);
            resolve({
              statusCode: 400,
              body: JSON.stringify({ error: errorMessage }),
            });
            return;
          }
          
          // Log a substring of the fileBuffer (base64) for verification.
          const fileDataBase64 = fileBuffer.toString("base64");
          console.log("File data (first 50 chars base64):", fileDataBase64.substring(0, 50));
          
          // Prepare FormData for the image upload.
          const form = new FormData();
          form.append("file", fileBuffer, { filename: fileName });
          form.append("listingId", listingId);
          form.append("token", token);
          if (rank) form.append("rank", rank);

          // Retrieve CLIENT_ID and SHOP_ID from environment variables.
          const clientId = process.env.CLIENT_ID;
          const shopId = process.env.SHOP_ID;
          if (!clientId || !shopId) {
            const errMsg = "CLIENT_ID and/or SHOP_ID environment variables are not set.";
            console.error(errMsg);
            resolve({
              statusCode: 500,
              body: JSON.stringify({ error: errMsg }),
            });
            return;
          }
          
          // Construct the Etsy API endpoint URL for image upload.
          const etsyImageUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${listingId}/images`;
          console.log("Image Upload URL:", etsyImageUrl);

          // Prepare headers (FormData will set the correct content-type with boundary).
          const headers = {
            "Authorization": `Bearer ${token}`,
            "x-api-key": clientId,
            ...form.getHeaders()
          };
          console.log("Image upload request headers:", headers);

          // Make the POST request to Etsy.
          const response = await fetch(etsyImageUrl, {
            method: "POST",
            headers,
            body: form
          });

          console.log("Image upload response status:", response.status);
          if (!response.ok) {
            const errorText = await response.text();
            console.error("Error uploading image. POST failed:", errorText);
            resolve({
              statusCode: response.status,
              body: JSON.stringify({ error: errorText })
            });
          } else {
            const responseData = await response.json();
            console.log("Image uploaded successfully:", responseData);
            resolve({
              statusCode: 200,
              body: JSON.stringify(responseData)
            });
          }
        } catch (err) {
          console.error("Exception in Busboy finish handler:", err);
          resolve({
            statusCode: 500,
            body: JSON.stringify({ error: err.message })
          });
        }
      });

      // Write the body to busboy.
      busboy.end(Buffer.from(event.body, event.isBase64Encoded ? "base64" : "utf8"));
    } catch (error) {
      console.error("Exception in handler:", error);
      resolve({
        statusCode: 500,
        body: JSON.stringify({ error: error.message })
      });
    }
  });
};