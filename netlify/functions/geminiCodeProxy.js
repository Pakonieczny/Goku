/* netlify/functions/geminiCodeProxy.js */
const fetch = require("node-fetch"); // Ensure node-fetch is in your package.json

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };
  if (event.httpMethod !== "POST") return { statusCode: 405, headers: CORS, body: "Method Not Allowed" };

  try {
    const { prompt, files } = JSON.parse(event.body);
    const apiKey = process.env.GEMINI_API_KEY;

    if (!apiKey) throw new Error("Missing GEMINI_API_KEY");
    if (!prompt) throw new Error("Missing instructions");

    // Construct the context payload for Gemini
    let fileContext = "Here are the current project files:\n\n";
    for (const [path, content] of Object.entries(files)) {
      fileContext += `--- FILE: ${path} ---\n${content}\n\n`;
    }

    const systemInstruction = `
      You are an expert game development AI. 
      The user will provide project files and a modification request.
      You must respond ONLY with a valid JSON object. Do not use markdown code blocks like \`\`\`json.
      
      The JSON format must be EXACTLY:
      {
        "message": "A short, 1-2 sentence explanation of what you changed.",
        "updatedFiles": [
          { "path": "folder/filename.ext", "content": "THE_ENTIRE_UPDATED_FILE_CONTENT" }
        ]
      }
      Only include files in 'updatedFiles' that actually need to be changed.
    `;

    const body = {
      contents: [{ role: "user", parts: [{ text: systemInstruction + fileContext + "\nUser Request: " + prompt }] }],
      generationConfig: {
        responseMimeType: "application/json", // Forces Gemini to return valid JSON
        temperature: 0.2 // Keep it focused and deterministic
      }
    };

    const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-pro-preview:generateContent`;
    
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-goog-api-key": apiKey },
      body: JSON.stringify(body),
    });

    const data = await res.json();
    
    if (!res.ok) throw new Error(data.error?.message || "Gemini API error");

    const responseText = data.candidates?.[0]?.content?.parts?.[0]?.text;
    if (!responseText) throw new Error("Empty response from Gemini");

    return {
      statusCode: 200,
      headers: CORS,
      body: responseText // It's already JSON thanks to responseMimeType
    };

  } catch (error) {
    console.error("Code Proxy Error:", error);
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: error.message }) };
  }
};