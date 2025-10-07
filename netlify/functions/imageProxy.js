const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  try {
    const imageUrl = event.queryStringParameters.url;
    if (!imageUrl) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing 'url' parameter" }),
      };
    }
    // Fetch the external image.
    const response = await fetch(imageUrl);
    if (!response.ok) {
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: "Failed to fetch image" }),
      };
    }
    const buffer = await response.buffer();
    return {
      statusCode: 200,
      headers: {
        "Content-Type": response.headers.get("content-type") || "image/jpeg",
        "Access-Control-Allow-Origin": "*",
      },
      body: buffer.toString("base64"),
      isBase64Encoded: true,
    };
  } catch (error) {
    console.error("Error in imageProxy:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
};