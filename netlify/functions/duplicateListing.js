const fetch = require('node-fetch');

exports.handler = async function(event, context) {
  try {
    const accessToken = event.headers['access-token'] || event.headers['Access-Token'];
    if (!accessToken) {
      return { statusCode: 400, body: JSON.stringify({ error: "Missing access token" }) };
    }
    const body = JSON.parse(event.body);
    const payload = body.payload;
    // Use SHOP_ID from an environment variable (set in Netlify dashboard) or hardcode it here:
    const SHOP_ID = process.env.SHOP_ID || "YOUR_SHOP_ID";
    const etsyUrl = `https://api.etsy.com/v3/application/shops/${SHOP_ID}/listings`;
    const response = await fetch(etsyUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": `Bearer ${accessToken}`
      },
      body: new URLSearchParams(payload)
    });
    const data = await response.json();
    return {
      statusCode: response.status,
      body: JSON.stringify(data)
    };
  } catch (error) {
    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};