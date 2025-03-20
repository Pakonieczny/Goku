const fetch = require("node-fetch");
const FormData = require("form-data");

exports.handler = async function(event, context) {
  try {
    if (!event.body || event.body.trim() === "") {
      throw new Error("No request body provided");
    }
    const payload = JSON.parse(event.body);
    const { file, fileName, purpose } = payload;
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error("Missing OPENAI_API_KEY environment variable");
    if (!file || !file.trim()) {
      throw new Error("No file content provided in request body");
    }
    
    // Create a Buffer from the provided base64 file content.
    const buffer = Buffer.from(file, "base64");
    console.log(`File "${fileName}" loaded. Buffer length: ${buffer.length} bytes`);
    
    // Determine content type based on file extension.
    let contentType = "text/csv"; // default for CSV files
    if (fileName.toLowerCase().endsWith(".docx")) {
      contentType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
    } else if (fileName.toLowerCase().endsWith(".doc")) {
      contentType = "application/msword";
    }
    
    // Prepare form-data.
    const form = new FormData();
    form.append("file", buffer, { filename: fileName, contentType });
    form.append("purpose", purpose);
    
    console.log("FormData prepared. Headers:", form.getHeaders());
    
    const response = await fetch("https://api.openai.com/v1/files", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        ...form.getHeaders()
      },
      body: form
    });
    
    const responseText = await response.text();
    console.log("Response text from OpenAI API:", responseText);
    
    let data;
    try {
      data = JSON.parse(responseText);
    } catch (parseError) {
      throw new Error(`Failed to parse response JSON: ${responseText}`);
    }
    
    if (!response.ok) {
      throw new Error(`Upload failed with status ${response.status}: ${JSON.stringify(data)}`);
    }
    
    console.log("File uploaded successfully:", data);
    return {
      statusCode: response.status,
      body: JSON.stringify(data)
    };
  } catch (error) {
    console.error("Exception in uploadFile function:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message, stack: error.stack })
    };
  }
};