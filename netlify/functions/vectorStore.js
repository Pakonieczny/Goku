exports.handler = async function(event, context) {
  try {
    // Parse the incoming payload from the request body.
    const payload = JSON.parse(event.body);
    // Get the API key from environment variables.
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      throw new Error("Missing OPENAI_API_KEY environment variable");
    }
    
    // Make a POST request to OpenAI's vector stores endpoint.
    const response = await fetch("https://api.openai.com/v1/beta/vector_stores", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`
      },
      body: JSON.stringify(payload)
    });
    
    // If the response is not OK, try to extract the error message.
    if (!response.ok) {
      let errorData;
      try {
        errorData = await response.json();
      } catch (parseError) {
        errorData = { error: "Failed to parse error response" };
      }
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: errorData })
      };
    }
    
    // Parse and return the successful response data.
    const data = await response.json();
    return {
      statusCode: 200,
      body: JSON.stringify(data)
    };
    
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};