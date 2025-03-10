const FormData = require("form-data");

exports.handler = async function(event, context) {
  try {
    const { file, fileName, purpose } = JSON.parse(event.body);
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error("Missing OPENAI_API_KEY environment variable");
    
    const form = new FormData();
    const buffer = Buffer.from(file, "base64");
    form.append("file", buffer, { filename: fileName });
    form.append("purpose", purpose);
    
    const response = await fetch("https://api.openai.com/v1/files", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        ...form.getHeaders()
      },
      body: form
    });
    
    if (!response.ok) {
      const errorData = await response.text();
      throw new Error(`Upload failed with status ${response.status}: ${errorData}`);
    }
    
    const data = await response.json();
    return {
      statusCode: 200,
      body: JSON.stringify(data)
    };
  } catch (error) {
    console.error("Exception in uploadFile function:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};