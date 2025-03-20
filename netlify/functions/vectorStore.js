//
// netlify/functions/vectorStore.js
//
exports.handler = async function(event) {
  try {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error("Missing OPENAI_API_KEY environment variable");

    // Parse the incoming request body
    const payload = JSON.parse(event.body || "{}");

    // If file_ids are provided, assume a creation action; otherwise, default to query
    const action = (payload.file_ids && Array.isArray(payload.file_ids) && payload.file_ids.length > 0)
      ? "create"
      : (payload.action || "query");

    if (action === "create") {
      // Creating (or updating) a vector store by attaching file_ids
      if (!payload.file_ids || !Array.isArray(payload.file_ids) || payload.file_ids.length === 0) {
        throw new Error("file_ids must be a non-empty array when creating a vector store.");
      }

      // Build the request body for the vector store creation
      // You can also include "chunking_strategy" or "metadata" if you like.
      const newPayload = {
        name: payload.name || "CSV Vector Store",
        file_ids: payload.file_ids
      };

      console.log("Creating vector store with payload:", newPayload);

      const response = await fetch("https://api.openai.com/v1/vector_stores", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${apiKey}`,
          // Required to access the beta endpoints:
          "OpenAI-Beta": "assistants=v2"
        },
        body: JSON.stringify(newPayload)
      });

      // If creation fails, log and return the error
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        console.error("Error response from OpenAI vector_stores create:", errorData);
        return {
          statusCode: response.status,
          body: JSON.stringify({ error: errorData })
        };
      }

      // On success, return the vector store object
      const data = await response.json();
      console.log("Successfully created/updated vector store:", data);
      return {
        statusCode: 200,
        body: JSON.stringify(data)
      };

    } else if (action === "query") {
      // For a vector store query, we expect a store_id
      const storeId = payload.store_id;
      if (!storeId) {
        throw new Error("Missing 'store_id' for vector store query.");
      }

      /*
        The official docs show the search endpoint as:
          POST /v1/vector_stores/{vector_store_id}/search
        The request body can include:
          "query": string or array
          "filters": object (optional)
          "max_num_results": number (1..50)
          "rewrite_query": boolean (default false)
          "ranking_options": object (optional)
      */

      // We'll map your existing payload.topK to max_num_results
      const searchObj = {
        query: payload.query || "",
        max_num_results: payload.topK || 10, // or any default you prefer
        rewrite_query: false // optional
      };

      // If the user wants to pass filters or ranking_options, allow that
      if (payload.filters) {
        searchObj.filters = payload.filters;
      }
      if (payload.ranking_options) {
        searchObj.ranking_options = payload.ranking_options;
      }

      console.log(`Querying vector store ${storeId} with:`, searchObj);

      const response = await fetch(`https://api.openai.com/v1/vector_stores/${storeId}/search`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${apiKey}`,
          // Required to access the beta endpoints:
          "OpenAI-Beta": "assistants=v2"
        },
        body: JSON.stringify(searchObj)
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        console.error("Error response from OpenAI vector_stores query:", errorData);
        return {
          statusCode: response.status,
          body: JSON.stringify({ error: errorData })
        };
      }

      // Return the search results
      const data = await response.json();
      console.log("Vector store query success. Matches:", data);
      return {
        statusCode: 200,
        body: JSON.stringify(data)
      };

    } else {
      // Unrecognized action
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: "Unknown action. Provide 'file_ids' for creation or use 'query' with a store_id."
        })
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