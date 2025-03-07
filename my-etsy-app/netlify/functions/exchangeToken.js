// netlify/functions/exchangeToken.js
const fetch = require("node-fetch");

exports.handler = async (event, context) => {
  const { code, code_verifier } = event.queryStringParameters || {};

  if (!code || !code_verifier) {
    return {
      statusCode: 400,
      body: JSON.stringify({ error: "Missing code or code_verifier" })
    };
  }

  const params = new URLSearchParams();
  params.append("grant_type", "authorization_code");
  params.append("client_id", process.env.CLIENT_ID);
  params.append("client_secret", process.env.CLIENT_SECRET);
  params.append("code", code);
  params.append("redirect_uri", process.env.REDIRECT_URI);
  params.append("code_verifier", code_verifier);

  try {
    const tokenResponse = await fetch("https://api.etsy.com/v3/public/oauth/token", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body: params
    });
    const tokenData = await tokenResponse.json();
    return {
      statusCode: tokenResponse.ok ? 200 : tokenResponse.status,
      body: JSON.stringify(tokenData)
    };
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};