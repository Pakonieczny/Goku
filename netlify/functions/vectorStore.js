// netlify/functions/vectorStore.js
exports.handler = async function(event) {
  try {
    console.log("vectorStore function invoked with event:", event);
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error("Missing OPENAI_API_KEY environment variable");

    // Parse the request payload
    const payload = JSON.parse(event.body || "{}");

    // Validate the payload for create action:
    if (payload.file_ids) {
      if (!Array.isArray(payload.file_ids) || payload.file_ids.length === 0) {
        throw new Error("file_ids must be a non-empty array when creating a vector store.");
      }
    }

    // Explicitly set action if not provided.
    payload.action = payload.action || (payload.file_ids ? "create" : "query");

    if (payload.action === "create") {
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

    } else if (payload.action === "query") {
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