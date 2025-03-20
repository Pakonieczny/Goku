// netlify/functions/vectorStore.js
exports.handler = async function(event) {
  try {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error("Missing OPENAI_API_KEY environment variable");

    // Parse the incoming request body
    const payload = JSON.parse(event.body || "{}");

    // If file_ids are provided, assume a creation action; otherwise, default to query
    // but we also have new custom actions: "list_files" and "clear_csv_files".
    let action = "query";
    if (payload.file_ids && Array.isArray(payload.file_ids) && payload.file_ids.length > 0) {
      action = "create";
    } else if (payload.action) {
      action = payload.action;
    }

    // Common helper for fetch calls to OpenAI
    async function openAIRequest(url, method = "GET", body = null) {
      const headers = {
        "Authorization": `Bearer ${apiKey}`,
        "OpenAI-Beta": "assistants=v2"
      };
      if (body) headers["Content-Type"] = "application/json";
      const response = await fetch(url, {
        method,
        headers,
        body: body ? JSON.stringify(body) : undefined
      });
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        console.error(`Error from OpenAI ${method} ${url}:`, errorData);
        return {
          statusCode: response.status,
          body: JSON.stringify({ error: errorData })
        };
      }
      const data = await response.json();
      return {
        statusCode: 200,
        body: JSON.stringify(data)
      };
    }

    if (action === "create") {
      // Creating (or updating) a vector store by attaching file_ids
      if (!payload.file_ids || !Array.isArray(payload.file_ids) || payload.file_ids.length === 0) {
        throw new Error("file_ids must be a non-empty array when creating a vector store.");
      }

      // Build the request body for the vector store creation
      const newPayload = {
        name: payload.name || "CSV Vector Store",
        file_ids: payload.file_ids
      };

      console.log("Creating vector store with payload:", newPayload);

      // POST /v1/vector_stores
      const url = "https://api.openai.com/v1/vector_stores";
      const result = await openAIRequest(url, "POST", newPayload);
      console.log("Result from create vector store:", result);
      return result;

    } else if (action === "query") {
      // For a vector store query, we expect a store_id
      const storeId = payload.store_id;
      if (!storeId) {
        throw new Error("Missing 'store_id' for vector store query.");
      }
      // POST /v1/vector_stores/{storeId}/search
      const searchObj = {
        query: payload.query || "",
        max_num_results: payload.topK || 10, 
        rewrite_query: false
      };
      if (payload.filters) {
        searchObj.filters = payload.filters;
      }
      if (payload.ranking_options) {
        searchObj.ranking_options = payload.ranking_options;
      }

      const url = `https://api.openai.com/v1/vector_stores/${storeId}/search`;
      console.log(`Querying vector store ${storeId} with:`, searchObj);
      const result = await openAIRequest(url, "POST", searchObj);
      console.log("Result from query vector store:", result);
      return result;

    } else if (action === "list_files") {
      // Custom action: list the files in a given vector store
      const storeId = payload.store_id;
      if (!storeId) {
        throw new Error("Missing 'store_id' for listing files.");
      }
      // GET /v1/vector_stores/{storeId}/files
      const url = `https://api.openai.com/v1/vector_stores/${storeId}/files`;
      console.log(`Listing files in vector store ${storeId}...`);
      const result = await openAIRequest(url, "GET");
      console.log("Result from listing vector store files:", result);
      return result;

    } else if (action === "clear_csv_files") {
      // Custom action: list all files, then remove CSV ones
      const storeId = payload.store_id;
      if (!storeId) {
        throw new Error("Missing 'store_id' for clearing CSV files.");
      }

      // 1) List the files
      const listUrl = `https://api.openai.com/v1/vector_stores/${storeId}/files`;
      const listResult = await openAIRequest(listUrl, "GET");
      if (listResult.statusCode !== 200) {
        return listResult;
      }
      const listData = JSON.parse(listResult.body);
      if (!listData.data || !Array.isArray(listData.data)) {
        return {
          statusCode: 200,
          body: JSON.stringify({ status: "No files found." })
        };
      }
      // 2) For each file that was originally CSV, delete it
      const files = listData.data;
      let deletedIds = [];
      for (let f of files) {
        // If the original filename ends with ".csv" or if name is .txt from our CSV flow
        // We'll just check if the "filename" has ".csv" or ".txt" to consider it a CSV
        const originalFilename = f.filename || "";
        if (originalFilename.toLowerCase().endsWith(".csv") || originalFilename.toLowerCase().endsWith(".txt")) {
          // DELETE /v1/vector_stores/{storeId}/files/{file_id}
          const deleteUrl = `https://api.openai.com/v1/vector_stores/${storeId}/files/${f.id}`;
          console.log("Deleting file from store:", deleteUrl);
          const deleteResult = await openAIRequest(deleteUrl, "DELETE");
          // We won't stop if there's an error; just keep going
          if (deleteResult.statusCode === 200) {
            deletedIds.push(f.id);
          }
        }
      }
      return {
        statusCode: 200,
        body: JSON.stringify({ status: "csv_files_cleared", deleted_file_ids: deletedIds })
      };

    } else {
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: "Unknown action. Provide 'file_ids' for creation, 'query' with store_id, 'list_files' with store_id, or 'clear_csv_files' with store_id."
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