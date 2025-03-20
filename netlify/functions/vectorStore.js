// netlify/functions/vectorStore.js
exports.handler = async function(event) {
  try {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error("Missing OPENAI_API_KEY environment variable");

    // Parse the incoming request body
    const payload = JSON.parse(event.body || "{}");

    // Determine action:
    // - "create": if file_ids is provided
    // - "query": if no file_ids and no specific action
    // - Also support custom actions: "list_files", "clear_csv_files", "update_file_attributes"
    let action = "query";
    if (payload.file_ids && Array.isArray(payload.file_ids) && payload.file_ids.length > 0) {
      action = "create";
    } else if (payload.action) {
      action = payload.action;
    }

    // Helper for making OpenAI API requests with beta header
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
      // Creating a vector store by attaching file_ids
      if (!payload.file_ids || !Array.isArray(payload.file_ids) || payload.file_ids.length === 0) {
        throw new Error("file_ids must be a non-empty array when creating a vector store.");
      }
      const newPayload = {
        name: payload.name || "CSV Vector Store",
        file_ids: payload.file_ids
      };

      console.log("Creating vector store with payload:", newPayload);
      const url = "https://api.openai.com/v1/vector_stores";
      const result = await openAIRequest(url, "POST", newPayload);
      console.log("Result from create vector store:", result);
      return result;

    } else if (action === "query") {
      // Query a vector store (POST /v1/vector_stores/{storeId}/search)
      const storeId = payload.store_id;
      if (!storeId) {
        throw new Error("Missing 'store_id' for vector store query.");
      }
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
      // List files in a given vector store: GET /v1/vector_stores/{storeId}/files
      const storeId = payload.store_id;
      if (!storeId) {
        throw new Error("Missing 'store_id' for listing files.");
      }
      const url = `https://api.openai.com/v1/vector_stores/${storeId}/files`;
      console.log(`Listing files in vector store ${storeId}...`);
      const result = await openAIRequest(url, "GET");
      console.log("Result from listing vector store files:", result);
      return result;

    } else if (action === "clear_csv_files") {
      // Clear CSV files: list files then delete those with .csv or .txt filenames
      const storeId = payload.store_id;
      if (!storeId) {
        throw new Error("Missing 'store_id' for clearing CSV files.");
      }
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
      const files = listData.data;
      let deletedIds = [];
      for (let f of files) {
        const originalFilename = (f.metadata && f.metadata.name) || "";
        if (originalFilename.toLowerCase().endsWith(".csv") || originalFilename.toLowerCase().endsWith(".txt")) {
          const deleteUrl = `https://api.openai.com/v1/vector_stores/${storeId}/files/${f.id}`;
          console.log("Deleting file from store:", deleteUrl);
          const deleteResult = await openAIRequest(deleteUrl, "DELETE");
          if (deleteResult.statusCode === 200) {
            deletedIds.push(f.id);
          }
        }
      }
      return {
        statusCode: 200,
        body: JSON.stringify({ status: "csv_files_cleared", deleted_file_ids: deletedIds })
      };

    } else if (action === "update_file_attributes") {
      // Update a vector store file's attributes
      const storeId = payload.store_id;
      const fileId = payload.file_id;
      const attributes = payload.attributes;
      if (!storeId || !fileId || !attributes) {
        throw new Error("Missing store_id, file_id or attributes for update_file_attributes");
      }
      const url = `https://api.openai.com/v1/vector_stores/${storeId}/files/${fileId}`;
      console.log("Updating file attributes for:", url, "with", attributes);
      const result = await openAIRequest(url, "POST", { attributes });
      return result;

    } else {
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: "Unknown action. Provide 'file_ids' for creation, 'query' with store_id, 'list_files' with store_id, 'clear_csv_files' with store_id, or 'update_file_attributes' with store_id, file_id, and attributes."
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