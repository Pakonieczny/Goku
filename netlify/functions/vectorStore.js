// netlify/functions/vectorStore.js
exports.handler = async function(event) {
  try {
    console.log("vectorStore function invoked with event:", event);
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error("Missing OPENAI_API_KEY environment variable");

    // Parse the incoming payload
    const payload = JSON.parse(event.body || "{}");

    // Force "create" action if file_ids is provided and non-empty; otherwise use provided action or default to "query"
    const action = (payload.file_ids && Array.isArray(payload.file_ids) && payload.file_ids.length > 0)
      ? "create"
      : (payload.action || "query");

    if (action === "create") {
      if (!payload.file_ids || !Array.isArray(payload.file_ids) || payload.file_ids.length === 0) {
        throw new Error("file_ids must be a non-empty array when creating a vector store.");
      }
      // Build the payload using "file_ids"
      const newPayload = {
        name: payload.name || "CSV Vector Store",
        file_ids: payload.file_ids, // Correct parameter for file IDs
        model: "text-embedding-ada-002"
      };
      console.log("Creating vector store with payload:", newPayload);

      const response = await fetch("https://api.openai.com/v1/vector_stores", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${apiKey}`
        },
        body: JSON.stringify(newPayload)
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
      // For query mode, ensure that a store_id is provided.
      const storeId = payload.store_id;
      if (!storeId) {
        throw new Error("Missing 'store_id' for vector store query.");
      }
      const queryObj = {
        query: payload.query || "",
        top_k: payload.topK || 5
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
        body: JSON.stringify({ error: "Unknown action. Provide 'file_ids' or set action='query'." })
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