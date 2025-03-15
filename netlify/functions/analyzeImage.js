// /netlify/functions/analyzeImage.js

const formidable = require("formidable");
const { Readable } = require("stream");
const fs = require("fs");
const fetch = require("node-fetch");

/**
 * This serverless function:
 *  - Receives a multipart/form-data POST with:
 *     "imageFile" => the actual binary file
 *     "rules" => optional text instructions from the user
 *  - Converts the uploaded file to base64
 *  - Passes it to the GPT-4 vision endpoint in the recommended "image_url" format
 * 
 * Requirements:
 *  - OPENAI_API_KEY in your environment
 *  - Access to a GPT-4 model with vision
 */

exports.handler = async function(event, context) {
  try {
    if (!event.body) {
      throw new Error("No body in request.");
    }

    // Compute or set content-length if needed
    if (!event.headers["content-length"]) {
      const length = Buffer.byteLength(
        event.body,
        event.isBase64Encoded ? "base64" : "utf8"
      );
      event.headers["content-length"] = length;
    }

    // Convert the event body into a Buffer
    const bodyBuffer = event.isBase64Encoded
      ? Buffer.from(event.body, "base64")
      : Buffer.from(event.body, "utf8");

    // Create a fake request stream for formidable
    const req = new Readable();
    req._read = () => {};
    req.push(bodyBuffer);
    req.push(null);
    req.headers = event.headers;

    // Parse multipart form data
    const form = formidable({ multiples: false });
    const parseForm = () =>
      new Promise((resolve, reject) => {
        form.parse(req, (err, fields, files) => {
          if (err) return reject(err);
          resolve({ fields, files });
        });
      });
    const { fields, files } = await parseForm();

    if (!files.imageFile) {
      throw new Error("No 'imageFile' found in form data.");
    }

    // Optional text instructions from user
    const rules = fields.rules || "No special instructions provided.";

    // Read file, convert to base64
    const fileBuffer = fs.readFileSync(files.imageFile.filepath);
    const base64Image = fileBuffer.toString("base64");

    // Determine MIME type from file
    const mimeType = files.imageFile.mimetype || "image/jpeg";  // fallback

    // Construct messages array: the official Chat Completions format for images
    const messages = [
      {
        role: "user",
        content: [
          {
            type: "text",
            text: `Analyze this image. ${rules}`
          },
          {
            type: "image_url",
            image_url: {
              // If desired: detail: "high" or "low" or "auto"
              url: `data:${mimeType};base64,${base64Image}`
            }
          }
        ]
      }
    ];

    const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
    if (!OPENAI_API_KEY) {
      throw new Error("Missing OPENAI_API_KEY in environment.");
    }

    // Use a GPT-4 vision-enabled model. Example: "gpt-4-lens", "gpt-4o-latest" if it supports images
    const openAiPayload = {
      model: "gpt-4-mini", 
      messages: messages,
      temperature: 0.6,
      max_tokens: 200
    };

    const openAiResp = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${OPENAI_API_KEY}`
      },
      body: JSON.stringify(openAiPayload)
    });

    if (!openAiResp.ok) {
      const errData = await openAiResp.text();
      throw new Error(`OpenAI error: ${openAiResp.status} - ${errData}`);
    }

    const openAiJson = await openAiResp.json();
    let metadata = "";
    if (openAiJson.choices && openAiJson.choices.length > 0) {
      metadata = openAiJson.choices[0].message.content || "";
    } else {
      metadata = "No analysis returned from GPT-4 vision.";
    }

    return {
      statusCode: 200,
      body: JSON.stringify({ metadata })
    };
  } catch (err) {
    console.error("Error in analyzeImage.js:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message })
    };
  }
};
