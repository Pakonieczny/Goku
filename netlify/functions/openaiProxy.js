const fetch = require("node-fetch");

exports.handler = async (event, context) => {
  try {
    console.log("openaiProxy received event:", event);

    // Parse the incoming payload
    let payload = JSON.parse(event.body);

    // Force the model to "gpt-4o-mini"
    payload.model = "gpt-4o-mini";

    // If the payload does not already contain a "messages" array,
    // and an "image" field is provided, then construct the proper messages array.
    if (!payload.messages) {
      if (payload.image) {
        // Use the provided prompt if it exists; otherwise, default to a standard prompt.
        const promptText = payload.prompt || "Describe this image.";
        // Use the provided detail level or default to "high"
        const detail = payload.detail || "high";

        // Construct the required messages array.
        payload.messages = [
          {
            role: "user",
            content: [
              { type: "text", text: promptText },
              {
                type: "image_url",
                image_url: {
                  url: payload.image,
                  detail: detail
                }
              }
            ]
          }
        ];
      } else {
        throw new Error("Missing required parameter: messages or image");
      }
    }

    // Remove top-level keys that are not expected by the API.
    delete payload.prompt;
    delete payload.image;
    delete payload.detail;

    console.log("Final payload to be sent:", JSON.stringify(payload, null, 2));

    // Retrieve the OpenAI API key from the environment.
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      throw new Error("Missing OPENAI_API_KEY environment variable");
    }

    // Define the OpenAI endpoint for chat completions (vision-capable)
    const endpoint = "https://api.openai.com/v1/chat/completions";
    console.log("Forwarding request to OpenAI endpoint:", endpoint);

    // Forward the request exactly as built.
    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`
      },
      body: JSON.stringify(payload)
    });

    const data = await response.json();
    if (!response.ok) {
      console.error("OpenAI API error:", data);
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: data })
      };
    }

    console.log("OpenAI API response:", data);
    return {
      statusCode: 200,
      body: JSON.stringify(data)
    };
  } catch (error) {
    console.error("Error in openaiProxy function:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};