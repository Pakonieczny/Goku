const FormData = require("form-data");
const fetch = require("node-fetch");

exports.handler = async function (event, context) {
  try {
    // Assume you have parsed fields and files (e.g., using formidable or a similar parser)
    const { listingId, token, fileName, rank } = parsedFields; // parsedFields from your parser
    const fileData = parsedFiles.file.data; // parsedFiles from your parser
    
    console.log("Received listingId:", listingId);
    console.log("File Name:", fileName);
    console.log("Rank:", rank);
    console.log("File data length:", fileData.length);

    const clientId = process.env.CLIENT_ID;
    const shopId = process.env.SHOP_ID;
    if (!clientId || !shopId) {
      throw new Error("Missing CLIENT_ID or SHOP_ID in environment variables.");
    }
    
    const imageUploadUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${listingId}/images`;
    console.log("Image Upload URL:", imageUploadUrl);
    
    // Construct the FormData using the 'form-data' package.
    const form = new FormData();
    // IMPORTANT: Use the key 'image' as required by Etsy’s API.
    form.append("image", fileData, {
      filename: fileName,
      contentType: "image/jpeg"
    });
    // You may also need to include other fields if required by the API, e.g. rank:
    form.append("rank", rank);
    
    console.log("FormData prepared with headers:", form.getHeaders());
    
    const response = await fetch(imageUploadUrl, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${token}`,
        "x-api-key": clientId,
        // Do not set 'Content-Type' manually; let form-data set it with the boundary.
        ...form.getHeaders()
      },
      body: form
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
    
    const responseData = JSON.parse(responseText);
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