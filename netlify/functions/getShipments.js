// netlify/functions/getShipments.js
const fetch = require('node-fetch');

exports.handler = async function(event, context) {
  const clientId = process.env.CHIT_CHATS_CLIENT_ID;
  const accessToken = process.env.CHIT_CHATS_ACCESS_TOKEN;

  const url = `https://chitchats.com/api/v1/clients/${clientId}/shipments`;

  try {
    const response = await fetch(url, {
      headers: {
        'Authorization': accessToken,
        'Content-Type': 'application/json'
      }
    });

    if (!response.ok) {
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: response.statusText })
      };
    }

    const data = await response.json();

    return {
      statusCode: 200,
      body: JSON.stringify(data)
    };
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Internal Server Error' })
    };
  }
};