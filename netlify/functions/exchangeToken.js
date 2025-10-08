const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  try {
    // Retrieve query parameters from Netlify (passed via event.queryStringParameters)
    const code = event.queryStringParameters.code;
    const codeVerifier = event.queryStringParameters.code_verifier;

    // Retrieve environment variables for Etsy OAuth
    const CLIENT_ID = process.env.CLIENT_ID;
    const CLIENT_SECRET = process.env.CLIENT_SECRET;
    const REDIRECT_URI = process.env.REDIRECT_URI;

    // Build the request parameters for the Etsy token exchange
    const params = new URLSearchParams({
      grant_type: "authorization_code",
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      code: code,
      redirect_uri: REDIRECT_URI,
      code_verifier: codeVerifier
    });

    const response = await fetch("https://api.etsy.com/v3/public/oauth/token", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body: params
    });

    const data = await response.json();

    return {
      statusCode: response.status,
      body: JSON.stringify(data)
    };
  } catch (error) {
    console.error("Error in exchangeToken:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};