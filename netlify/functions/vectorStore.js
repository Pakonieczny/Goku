// netlify/functions/vectorStore.js
exports.handler = async function(event) {
  try {
    console.log("vectorStore function invoked with event:", event);
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error("Missing OPENAI_API_KEY environment variable");

    // Parse the request payload from the front end.
    const incomingPayload = JSON.parse(event.body || "{}");

    // Decide on action: if the incoming payload includes documents, we assume "create".
    const action = incomingPayload.action || (incomingPayload.documents ? "create" : "query");

    if (action === "create") {
      // Validate that we have a non-empty array of documents.
      if (!incomingPayload.documents || 
          !Array.isArray(incomingPayload.documents) || 
          incomingPayload.documents.length === 0) {
        throw new Error("documents must be a non-empty array when creating a vector store.");
      }

      // Build the payload as expected by the OpenAI vector store API.
      // Each document should be an object with a "text" property.
      const payload = {
        name: incomingPayload.name || "CSV Vector Store",
        documents: incomingPayload.documents, // Expected to be like: [{ text: "CSV file content" }, ...]
        model: "text-embedding-ada-002"
      };

      console.log("Creating vector store with payload:", payload);

      const response = await fetch("https://api.openai.com/v1/vector_stores", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${apiKey}`
        },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        console.error("Error response from OpenAI vector_stores create:", errorData);
        return {
          statusCode: response.status,
          body: JSON.stringify({ error: errorData })
        };
      }

      const data = await response.json();
      console.log("Successfully created/updated vector store:", data);
      return {
        statusCode: 200,
        body: JSON.stringify(data)
      };

    } else if (action === "query") {
      // Query mode: ensure that store_id is provided.
      const storeId = incomingPayload.store_id;
      if (!storeId) {
        throw new Error("Missing 'store_id' for vector store query.");
      }
      const queryObj = {
        query: incomingPayload.query || "",
        top_k: incomingPayload.topK || 5
      };

      console.log(`Querying vector store ${storeId} with:`, queryObj);

      const response = await fetch(`https://api.openai.com/v1/vector_stores/${storeId}/query`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${apiKey}`
        },
        body: JSON.stringify(queryObj)
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        console.error("Error response from OpenAI vector_stores query:", errorData);
        return {
          statusCode: response.status,
          body: JSON.stringify({ error: errorData })
        };
      }

      const data = await response.json();
      console.log("Vector store query success. Matches:", data);
      return {
        statusCode: 200,
        body: JSON.stringify(data)
      };

    } else {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Unknown action. Provide 'documents' or set action='query'." })
      };
    }
  } catch (error) {
    console.error("Exception in vectorStore function:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};