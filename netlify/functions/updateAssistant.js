exports.handler = async function(event, context) {
  try {
    const payload = JSON.parse(event.body);
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error("Missing OPENAI_API_KEY environment variable");
    
    const assistantId = process.env.ASSISTANT_ID;
    if (!assistantId) throw new Error("Missing ASSISTANT_ID environment variable");
    
    const response = await fetch(`https://api.openai.com/v1/assistants/${assistantId}`, {
      method: "PATCH",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`
      },
      body: JSON.stringify(payload)
    });
    
    if (!response.ok) {
      const errorData = await response.json();
      throw new Error("Error updating assistant: " + JSON.stringify(errorData));
    }
    
    const data = await response.json();
    return {
      statusCode: 200,
      body: JSON.stringify(data)
    };
  } catch (error) {
    console.error("Exception in updateAssistant function:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};