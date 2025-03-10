exports.handler = async function(event, context) {
  try {
    // Log the incoming event for debugging purposes
    console.log("vectorStore function invoked with event:", event);

    // Parse the payload from the request body.
    const payload = JSON.parse(event.body);
    
    // Retrieve the API key from environment variables.
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      throw new Error("Missing OPENAI_API_KEY environment variable");
    }
    
    // Make the API call to OpenAI's vector_stores endpoint.
    const response = await fetch("https://api.openai.com/v1/beta/vector_stores", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`
      },
      body: JSON.stringify(payload)
    });
    
    // Check if the response is not OK.
    if (!response.ok) {
      let errorData;
      try {
        errorData = await response.json();
      } catch (parseError) {
        errorData = { error: "Failed to parse error response" };
      }
      console.error("Error response from OpenAI:", errorData);
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: errorData })
      };
    }
    
    // Parse the successful response.
    const data = await response.json();
    console.log("Successfully created vector store:", data);
    return {
      statusCode: 200,
      body: JSON.stringify(data)
    };
    
  } catch (error) {
    console.error("Exception in vectorStore function:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};