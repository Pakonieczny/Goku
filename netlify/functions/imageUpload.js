const fetch = require("node-fetch");
const Busboy = require("busboy");
const FormData = require("form-data");

exports.handler = async function (event, context) {
  // Wrap in a Promise because Busboy is event‐driven
  return new Promise((resolve, reject) => {
    try {
      if (event.httpMethod !== "POST") {
        resolve({
          statusCode: 405,
          body: JSON.stringify({ error: "Method Not Allowed" }),
        });
        return;
      }

      console.log("Incoming request headers:", event.headers);

      const busboy = new Busboy({ headers: event.headers });
      let listingId, token, fileName, rank;
      let fileBuffer = Buffer.alloc(0);

      busboy.on("field", (fieldname, val) => {
        if (fieldname === "listingId") {
          listingId = val;
          console.log("Parsed field listingId:", listingId);
        }
        if (fieldname === "token") {
          token = val;
          console.log("Parsed field token:", token.substring(0, 10) + "...");
        }
        if (fieldname === "fileName") {
          fileName = val;
          console.log("Parsed field fileName:", fileName);
        }
        if (fieldname === "rank") {
          rank = val;
          console.log("Parsed field rank:", rank);
        }
      });

      busboy.on("file", (fieldname, file, filename, encoding, mimetype) => {
        console.log(`Receiving file [${fieldname}]: ${filename} (${mimetype})`);
        file.on("data", (data) => {
          fileBuffer = Buffer.concat([fileBuffer, data]);
        });
        file.on("end", () => {
          console.log(`Finished reading file ${filename}, size: ${fileBuffer.length} bytes`);
        });
      });

      busboy.on("finish", async () => {
        console.log("Finished parsing form data.");
        // Validate required fields
        if (!listingId || !token || !fileName || fileBuffer.length === 0) {
          resolve({
            statusCode: 400,
            body: JSON.stringify({ error: "Missing required parameters" }),
          });
          return;
        }

        // Retrieve CLIENT_ID and SHOP_ID from environment variables.
        const clientId = process.env.CLIENT_ID;
        const shopId = process.env.SHOP_ID;
        if (!clientId || !shopId) {
          resolve({
            statusCode: 500,
            body: JSON.stringify({ error: "Missing CLIENT_ID or SHOP_ID in environment" }),
          });
          return;
        }

        // Construct FormData for the outgoing request
        let form = new FormData();
        // Etsy expects a parameter 'listing_id' (string or number)
        form.append("listing_id", listingId);
        // Include file name and rank
        form.append("file_name", fileName);
        form.append("rank", rank || "1");
        // Append the file (we assume image/jpeg; adjust if necessary)
        form.append("file", fileBuffer, { filename: fileName, contentType: "image/jpeg" });

        // Log FormData headers (includes boundary)
        console.log("Outgoing FormData headers:", form.getHeaders());

        // Construct the Etsy image upload URL
        const imageUploadUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${listingId}/images`;
        console.log("Image Upload URL:", imageUploadUrl);

        try {
          const response = await fetch(imageUploadUrl, {
            method: "POST",
            headers: {
              // The form-data package sets its own Content-Type (with boundary)
              ...form.getHeaders(),
              "Authorization": `Bearer ${token}`,
              "x-api-key": clientId,
            },
            body: form,
          });

          // Read the response as text (because sometimes non-JSON data might be returned)
          const responseText = await response.text();
          console.log("Image upload response status:", response.status);
          console.log("Image upload response text:", responseText);

          if (!response.ok) {
            resolve({
              statusCode: response.status,
              body: JSON.stringify({ error: responseText }),
            });
          } else {
            let data;
            try {
              data = JSON.parse(responseText);
            } catch (parseError) {
              data = { message: responseText };
            }
            console.log("Image uploaded successfully:", data);
            resolve({
              statusCode: 200,
              body: JSON.stringify(data),
            });
          }
        } catch (uploadError) {
          console.error("Exception during image upload:", uploadError);
          resolve({
            statusCode: 500,
            body: JSON.stringify({ error: uploadError.message }),
          });
        }
      });

      // Write the body to Busboy. If the incoming event.body is base64-encoded, decode it.
      busboy.write(event.body, event.isBase64Encoded ? "base64" : "binary");
      busboy.end();
    } catch (err) {
      console.error("Exception in handler:", err);
      resolve({
        statusCode: 500,
        body: JSON.stringify({ error: err.message }),
      });
    }
  });
};