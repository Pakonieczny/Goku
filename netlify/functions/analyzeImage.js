// /netlify/functions/analyzeImage.js
//
// Note: This function now strictly handles the analysis of the image and returns metadata.
// The embedding of metadata into the image's EXIF is handled on the client side by metadataHandler.js.

const formidable = require("formidable");
const { Readable } = require("stream");
const fs = require("fs");
const fetch = require("node-fetch");

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

    // Create a fake request stream
    const req = new Readable();
    req._read = () => {};
    req.push(bodyBuffer);
    req.push(null);
    req.headers = event.headers;

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

    const rules = fields.rules || "No special instructions provided.";

    // Read file from disk, convert to base64
    const fileBuffer = fs.readFileSync(files.imageFile.filepath);
    const base64Image = fileBuffer.toString("base64");

    // Determine MIME type from the upload
    const mimeType = files.imageFile.mimetype || "image/jpeg";

    // Construct the Chat Completions messages array
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

    // Must be a GPT-4 model with vision
    const openAiPayload = {
      model: "gpt-4o-mini", // replace with the correct model you have access to
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