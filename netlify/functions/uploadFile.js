// netlify/functions/uploadFile.js
const fetch = require("node-fetch");
const FormData = require("form-data");

exports.handler = async (event, context) => {
  try {
    // Expect a JSON body with: file (Base64-encoded), fileName, and purpose
    const { file, fileName, purpose } = JSON.parse(event.body);
    if (!file || !fileName || !purpose) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing file, fileName, or purpose in request body" })
      };
    }

    // Convert Base64 string to a Buffer
    const buffer = Buffer.from(file, "base64");

    const form = new FormData();
    form.append("file", buffer, {
      filename: fileName,
      contentType: "application/octet-stream"
    });
    form.append("purpose", purpose);

    const response = await fetch("https://api.openai.com/v1/files", {
      method: "POST",
      headers: {
        // Let form-data set the Content-Type header including the boundary
        "Authorization": "Bearer " + process.env.OPENAI_API_KEY
      },
      body: form
    });
    const data = await response.json();
    return {
      statusCode: response.ok ? 200 : response.status,
      body: JSON.stringify(data)
    };
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};