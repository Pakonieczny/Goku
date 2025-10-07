// getEtsyCredentials.js
exports.handler = async function(event, context) {
  try {
    const CLIENT_ID = process.env.CLIENT_ID;
    const REDIRECT_URI = process.env.REDIRECT_URI;
    if (!CLIENT_ID || !REDIRECT_URI) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing required environment variables" })
      };
    }
    return {
      statusCode: 200,
      body: JSON.stringify({ CLIENT_ID, REDIRECT_URI })
    };
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};