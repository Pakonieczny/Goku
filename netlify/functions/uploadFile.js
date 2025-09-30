// netlify/functions/uploadFile.js
const fetch = require("node-fetch");
const FormData = require("form-data");

exports.handler = async function (event, context) {
  try {
    if (!event.body || event.body.trim() === "") {
      return { statusCode: 400, body: JSON.stringify({ error: "No request body provided" }) };
    }

    let payload;
    try {
      payload = JSON.parse(event.body);
    } catch {
      return { statusCode: 400, body: JSON.stringify({ error: "Invalid JSON body" }) };
    }

    const { file, fileName, purpose } = payload || {};
    const apiKey = process.env.OPENAI_API_KEY;

    if (!apiKey) {
      return { statusCode: 500, body: JSON.stringify({ error: "Missing OPENAI_API_KEY environment variable" }) };
    }
    if (!file || !String(file).trim()) {
      return { statusCode: 400, body: JSON.stringify({ error: "No file content provided in request body" }) };
    }

    // Support both raw base64 and data URLs ("data:...;base64,xxxx")
    const base64 = String(file).includes(",") ? String(file).split(",").pop() : String(file);

    // Create a Buffer from the provided base64 file content.
    const buffer = Buffer.from(base64, "base64");
    if (!buffer.length) {
      return { statusCode: 400, body: JSON.stringify({ error: "Decoded file is empty" }) };
    }
    console.log(`File "${fileName || "upload.csv"}" loaded. Buffer length: ${buffer.length} bytes`);

    // Netlify Functions request-body practical ceiling ~10MB — be nice and fail early with 413.
    if (buffer.length > Math.floor(9.5 * 1024 * 1024)) {
      return {
        statusCode: 413,
        body: JSON.stringify({
          error: `File too large for Netlify proxy (~10MB). Size=${(buffer.length / 1024 / 1024).toFixed(2)} MB`,
        }),
      };
    }

    // Determine content type based on file extension (guard missing fileName)
    let contentType = "text/csv"; // default for CSV files
    const lower = (fileName || "").toLowerCase();
    if (lower.endsWith(".docx")) {
      contentType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
    } else if (lower.endsWith(".doc")) {
      contentType = "application/msword";
    } else if (lower.endsWith(".txt")) {
      contentType = "text/plain";
    }

    // Prepare form-data.
    const form = new FormData();
    form.append("file", buffer, { filename: fileName || "upload.csv", contentType });

    // Normalize purpose for modern Files API usage
    const finalPurpose = purpose === "user_data" ? "assistants" : (purpose || "assistants");
    form.append("purpose", finalPurpose);

    console.log("FormData prepared. Headers:", form.getHeaders());

    const response = await fetch("https://api.openai.com/v1/files", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        ...form.getHeaders(),
      },
      body: form,
    });

    const responseText = await response.text();
    console.log("Response text from OpenAI API:", responseText);

    // Try JSON parse; if not JSON, pass raw text back.
    let data = null;
    try {
      data = JSON.parse(responseText);
    } catch {}

    if (!response.ok) {
      // Do NOT mask upstream errors as 500—surface the real status/body
      return {
        statusCode: response.status,
        body: data ? JSON.stringify(data) : responseText,
      };
    }

    console.log("File uploaded successfully");
    return { statusCode: 200, body: data ? JSON.stringify(data) : responseText };
  } catch (error) {
    console.error("Exception in uploadFile function:", error);
    // Only truly unexpected errors hit this path.
    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};