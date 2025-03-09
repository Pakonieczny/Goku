const fetch = require("node-fetch");
const FormData = require("form-data");

exports.handler = async (event, context) => {
  try {
    // Expect a JSON body with: file (Base64-encoded), fileName, and purpose
    const { file, fileName } = JSON.parse(event.body);
    
    // Use the allowed purpose "fine-tune" for uploading files to OpenAI.
    const purpose = "fine-tune";
    
    if (!file || !fileName || !purpose) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing file, fileName, or purpose in request body" })
      };
    }

    // Convert Base64 string to a Buffer
    const buffer = Buffer.from(file, "base64");

    // Create a new FormData instance and append fields
    const form = new FormData();
    form.append("file", buffer, {
      filename: fileName,
      contentType: "application/octet-stream"
    });
    form.append("purpose", purpose);

    // Make a POST request to OpenAI's file upload endpoint using the API key from environment variables.
    const response = await fetch("https://api.openai.com/v1/files", {
      method: "POST",
      headers: {
        // Do not set the Content-Type header here; FormData will set it including the proper boundary.
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