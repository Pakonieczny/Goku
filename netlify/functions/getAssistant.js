let assistantCache = null;

exports.handler = async function(event, context) {
  // If we already have an assistant ID cached, return it.
  if (assistantCache) {
    return {
      statusCode: 200,
      body: JSON.stringify({ assistant_id: assistantCache })
    };
  }

  const apiKey = process.env.OPENAI_API_KEY;
  const createPayload = {
    model: "gpt-4o",  // Change as needed
    name: "Dynamic Assistant",
    instructions: "You are an assistant that can help with Etsy listings.",
    tools: [{ type: "file_search" }],
    top_p: 1.0,
    temperature: 1.0,
    response_format: "auto"
  };

  try {
    const response = await fetch("https://api.openai.com/v1/assistants", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`,
        "OpenAI-Beta": "assistants=v2"
      },
      body: JSON.stringify(createPayload)
    });
    const data = await response.json();
    if (!response.ok) {
      return {
        statusCode: response.status,
        body: JSON.stringify(data)
      };
    }
    assistantCache = data.id;
    return {
      statusCode: 200,
      body: JSON.stringify({ assistant_id: assistantCache })
    };
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};