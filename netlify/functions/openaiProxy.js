exports.handler = async function(event, context) {
  try {
    const payload = JSON.parse(event.body);
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error("Missing OPENAI_API_KEY environment variable");

    // Route by payload shape: use Responses API when caller sends `input`
    const wantsResponses =
      payload && (payload.input !== undefined ||
                  payload.max_output_tokens !== undefined ||
                  payload.reasoning !== undefined ||
                  payload.verbosity !== undefined);

    const endpoint = wantsResponses
      ? "https://api.openai.com/v1/responses"
      : "https://api.openai.com/v1/chat/completions";

    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`
      },
      body: JSON.stringify(payload)
    });

    if (!response.ok) {
      // Pass through OpenAI's JSON as-is so the frontend can read error.error.message
      const raw = await response.text();
      try {
        const errJson = JSON.parse(raw);
        return { statusCode: response.status, body: JSON.stringify(errJson) };
      } catch {
        // Fallback if upstream didn't return JSON
        return {
          statusCode: response.status,
          body: JSON.stringify({ error: { message: raw || "Upstream error", status: response.status } })
        };
      }
    }

    const data = await response.json();
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