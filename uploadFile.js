const fetch = require("node-fetch");
const FormData = require("form-data");

exports.handler = async function(event, context) {
  try {
    console.log("uploadFile function invoked");
    const body = JSON.parse(event.body);
    const { file, fileName, purpose } = body;
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error("Missing OPENAI_API_KEY environment variable");

    if (!file || !file.trim()) {
      throw new Error("File content is empty");
    }
    
    // Create a Buffer from the base64 file content.
    const buffer = Buffer.from(file, "base64");
    console.log(`File "${fileName}" loaded. Buffer length: ${buffer.length} bytes`);
    
    // Determine the content type based on file extension.
    let contentType = "text/csv"; // default for CSV
    if (fileName.toLowerCase().endsWith(".docx")) {
      contentType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
    } else if (fileName.toLowerCase().endsWith(".doc")) {
      contentType = "application/msword";
    }
    
    // Prepare form-data.
    const form = new FormData();
    form.append("file", buffer, { filename: fileName, contentType: contentType });
    form.append("purpose", purpose);
    
    console.log("FormData prepared. Headers:", form.getHeaders());
    
    // Call the OpenAI file upload endpoint.
    const response = await fetch("https://api.openai.com/v1/files", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        ...form.getHeaders()
      },
      body: form
    });
    
    const responseText = await response.text();
    console.log("OpenAI API response text:", responseText);
    
    try {
      const data = JSON.parse(responseText);
      if (!response.ok) {
        throw new Error(`Upload failed with status ${response.status}: ${JSON.stringify(data)}`);
      }
      return {
        statusCode: response.status,
        body: JSON.stringify(data)
      };
    } catch (parseError) {
      throw new Error(`Upload failed with status ${response.status}: ${responseText}`);
    }
  } catch (error) {
    console.error("Exception in uploadFile function:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};