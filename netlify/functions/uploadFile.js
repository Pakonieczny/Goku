const FormData = require("form-data");

exports.handler = async function(event, context) {
  try {
    console.log("uploadFile function invoked with event:", event);
    const { file, fileName, purpose } = JSON.parse(event.body);
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error("Missing OPENAI_API_KEY environment variable");
    
    const buffer = Buffer.from(file, "base64");
    console.log("File buffer created for", fileName);
    
    const form = new FormData();
    form.append("file", buffer, { filename: fileName });
    form.append("purpose", purpose);
    
    console.log("Form headers:", form.getHeaders());
    
    const response = await fetch("https://api.openai.com/v1/files", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        ...form.getHeaders()
      },
      body: form
    });
    
    if (!response.ok) {
      const errorText = await response.text();
      console.error(`Upload failed with status ${response.status}: ${errorText}`);
      throw new Error(`Upload failed with status ${response.status}: ${errorText}`);
    }
    
    const data = await response.json();
    console.log("File uploaded successfully:", data);
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