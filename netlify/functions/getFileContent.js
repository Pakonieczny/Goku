// netlify/functions/getFileContent.js
const fetch = require('node-fetch');

exports.handler = async function(event) {
  try {
    const { fileId } = event.queryStringParameters;
    if (!fileId) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing fileId parameter" })
      };
    }
    
    const response = await fetch(`https://api.openai.com/v1/files/${fileId}/content`, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`,
        "Content-Type": "application/json"
      }
    });

    if (!response.ok) {
      const errText = await response.text();
      return {
        statusCode: response.status,
        body: errText
      };
    }

    const content = await response.text();
    return {
      statusCode: 200,
      body: JSON.stringify({ content })
    };
  } catch (error) {
    console.error("Error in getFileContent:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};