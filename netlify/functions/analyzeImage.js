// /netlify/functions/analyzeImage.js

const formidable = require("formidable");
const { Readable } = require("stream");
const fs = require("fs");
const fetch = require("node-fetch");

/**
 * This serverless function accepts multipart form-data containing:
 *  - "image" (the actual binary file)
 *  - optional "rules" field (the user’s analysis instructions)
 * Then it reads that file into memory, converts it to base64,
 * and calls the GPT-4 text endpoint with a prompt that includes
 * the base64 string. GPT-4 tries to interpret the image content.
 * 
 * WARNING: This is an unconventional approach. For larger images,
 * you can easily exceed prompt size limits. GPT-4 with vision is not
 * a publicly available official endpoint. So treat this as a 
 * demonstration of the concept rather than a production solution.
 */

exports.handler = async function(event, context) {
  try {
    // Ensure we have an event body
    if (!event.body) {
      throw new Error("No body in request.");
    }

    // Compute / set content-length if needed
    if (!event.headers["content-length"]) {
      const length = Buffer.byteLength(
        event.body,
        event.isBase64Encoded ? "base64" : "utf8"
      );
      event.headers["content-length"] = length;
      console.log("Computed content-length:", length);
    }

    // Convert the event body into a Buffer
    const bodyBuffer = event.isBase64Encoded
      ? Buffer.from(event.body, "base64")
      : Buffer.from(event.body, "utf8");

    // Create a fake readable stream to feed to formidable
    const req = new Readable();
    req._read = () => {};
    req.push(bodyBuffer);
    req.push(null);
    req.headers = event.headers;

    // Parse multipart form data
    const form = formidable({ multiples: false });
    const parseForm = () => new Promise((resolve, reject) => {
      form.parse(req, (err, fields, files) => {
        if (err) reject(err);
        else resolve({ fields, files });
      });
    });

    const { fields, files } = await parseForm();

    // We'll have "image" in files
    const file = files.image;
    if (!file) {
      throw new Error("No 'image' file found in form data.");
    }
    console.log("Received file:", file.originalFilename);

    const rules = fields.rules || "No special instructions provided.";

    // 1) Read the image file from disk, convert to base64
    const fileBuffer = fs.readFileSync(file.filepath);
    const base64Image = fileBuffer.toString("base64");

    // 2) Construct a prompt that includes the base64
    //    WARNING: If the image is large, this can easily exceed GPT-4's max tokens.
    const systemPrompt = `
You are GPT-4 with some minimal vision-like capability, analyzing an image that is included in base64 form.
Consider the user-provided rules for how to interpret or describe the image.
Then produce a short textual description focusing on relevant details only. 
Do not literally dump the base64 in your final output. 
Remember: The image is in base64 text, but your job is to interpret or summarize it as best you can.
----

Rules from user:
${rules}
----
`;

    const userPrompt = `
Here is the image in base64 format (only partial GPT-4 "vision" simulation):
${base64Image}
Please analyze or describe it in 1-3 sentences, focusing on jewelry details if possible.
`;

    // 3) Send the combined prompt to the OpenAI chat completions endpoint
    const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
    if (!OPENAI_API_KEY) {
      throw new Error("Missing OPENAI_API_KEY environment variable.");
    }

    const openAiPayload = {
      model: "gpt-4",       // or your GPT-4 variant, e.g. 'gpt-4o-latest' if that’s your naming
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userPrompt }
      ],
      temperature: 0.6
    };

    // 4) Call OpenAI
    const openAiResp = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${OPENAI_API_KEY}`
      },
      body: JSON.stringify(openAiPayload)
    });

    if (!openAiResp.ok) {
      const errData = await openAiResp.json();
      throw new Error(`OpenAI error: ${openAiResp.status} - ${JSON.stringify(errData)}`);
    }
    const openAiJson = await openAiResp.json();

    // 5) Extract the text from the response
    let metadata = "";
    if (openAiJson.choices && openAiJson.choices.length > 0) {
      metadata = openAiJson.choices[0].message.content || "";
    } else {
      metadata = "No analysis returned from GPT-4.";
    }

    // Return the metadata in JSON
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