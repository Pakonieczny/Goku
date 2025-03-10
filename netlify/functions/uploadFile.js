const fetch = require("node-fetch");
const FormData = require("form-data");

exports.handler = async function(event, context) {
  try {
    console.log("uploadFile function invoked with event:", event);
    const { file, fileName, purpose } = JSON.parse(event.body);
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error("Missing OPENAI_API_KEY environment variable");

    // Convert the provided base64 file string into a Buffer.
    const buffer = Buffer.from(file, "base64");
    console.log("File buffer created for", fileName);

    // Determine the correct content type based on file extension.
    let contentType = "text/csv"; // default for .csv files
    if (fileName.toLowerCase().endsWith(".docx")) {
      contentType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
    } else if (fileName.toLowerCase().endsWith(".doc")) {
      contentType = "application/msword";
    }
    // Create FormData and append the file and purpose.
    const form = new FormData();
    form.append("file", buffer, { filename: fileName, contentType: contentType });
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

    const text = await response.text();
    try {
      const data = JSON.parse(text);
      if (!response.ok) {
        console.error("Error uploading file:", data);
        throw new Error(`Upload failed with status ${response.status}: ${JSON.stringify(data)}`);
      }
      console.log("File uploaded successfully:", data);
      return {
        statusCode: response.status,
        body: JSON.stringify(data)
      };
    } catch (e) {
      console.error("Error parsing response:", e, text);
      throw new Error(`Upload failed with status ${response.status}: ${text}`);
    }
  } catch (error) {
    console.error("Exception in uploadFile function:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};