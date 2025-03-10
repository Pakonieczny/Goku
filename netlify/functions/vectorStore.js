exports.handler = async function(event, context) {
  try {
    console.log("vectorStore function invoked with event:", event);
    const payload = JSON.parse(event.body);
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error("Missing OPENAI_API_KEY environment variable");

    const response = await fetch("https://api.openai.com/v1/vector_stores", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`
      },
      body: JSON.stringify(payload)
    });

    if (!response.ok) {
      const contentType = response.headers.get("content-type");
      let errorData;
      if (contentType && contentType.includes("application/json")) {
        errorData = await response.json();
      } else {
        const rawText = await response.text();
        errorData = { error: "Non-JSON error response", raw: rawText };
      }
      console.error("Error response from OpenAI:", errorData);
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: errorData })
      };
    }

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