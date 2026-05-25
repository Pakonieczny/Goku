/**
 * netlify/functions/visionWarehouse.js
 *
 * Demonstrates:
 *   - createCorpus
 *   - uploadAndImport
 *   - analyzeCorpus
 *   - createIndex
 *   - deployIndex
 *   - search
 *
 * Pulls only GCP_PROJECT_NUMBER from environment variables, so you don't have
 * to pass or hard-code your project number. You still provide corpusId, etc.
 *
 * Required ENV Vars in Netlify:
 *   GCP_PROJECT_NUMBER
 *   GCP_PROJECT_ID
 *   GCP_CLIENT_EMAIL
 *   GCP_PRIVATE_KEY        (with actual newlines, or escaped \n replaced)
 *   GCP_BUCKET_NAME
 *   (optional) GCP_LOCATION (defaults to "us-central1")
 */

const WAREHOUSE_API_ROOT = "https://warehouse-visionai.googleapis.com/v1";

// 0) Build the serviceAccount from environment vars
const serviceAccount = {
  client_email: process.env.GCP_CLIENT_EMAIL,
  private_key: process.env.GCP_PRIVATE_KEY
    ? process.env.GCP_PRIVATE_KEY.replace(/\\n/g, "\n")
    : "",
  project_id: process.env.GCP_PROJECT_ID
};

const projectNumber = process.env.GCP_PROJECT_NUMBER; // e.g. "123456789012"
const bucketName = process.env.GCP_BUCKET_NAME;
const locationId = process.env.GCP_LOCATION || "us-central1";

// 1) Auth for Warehouse
const { GoogleAuth } = require("google-auth-library");
const auth = new GoogleAuth({
  credentials: serviceAccount,
  scopes: ["https://www.googleapis.com/auth/cloud-platform"]
});

async function getAccessToken() {
  const client = await auth.getClient();
  const token = await client.getAccessToken();
  return token.token || token;
}

// 2) Google Cloud Storage client
const { Storage } = require("@google-cloud/storage");
const gcsStorage = new Storage({
  projectId: serviceAccount.project_id,
  credentials: serviceAccount
});

/**
 * Helper: Upload base64 data (data:image/xxx;base64,...) to GCS
 * and return a "gs://bucket/file" URI.
 */
async function uploadBase64ToGCS(base64Data, objectKey) {
  const match = base64Data.match(/^data:(?<mime>[^;]+);base64,(?<base64>.+)$/);
  if (!match || !match.groups) {
    throw new Error("Invalid base64 data URL");
  }
  const mimeType = match.groups.mime;
  const rawBase64 = match.groups.base64;
  const fileBuffer = Buffer.from(rawBase64, "base64");

  const fileRef = gcsStorage.bucket(bucketName).file(objectKey);
  await fileRef.save(fileBuffer, {
    contentType: mimeType,
    resumable: false,
    public: false
  });

  return `gs://${bucketName}/${objectKey}`;
}

// 3) Netlify Handler: Switch on the six Warehouse API actions
const fetch = require("node-fetch"); // For Node < 18

exports.handler = async (event, context) => {
  try {
    const body = JSON.parse(event.body || "{}");
    const action = body.action;

    switch (action) {
      //-----------------------------------------------------------
      // A) createCorpus
      //-----------------------------------------------------------
      case "createCorpus": {
        /**
         * To create a brand-new corpus, you'd still pass:
         * {
         *   "action": "createCorpus",
         *   "displayName": "My test corpus",
         *   "description": "whatever"
         * }
         * We'll still use GCP_PROJECT_NUMBER from env. 
         */
        const displayName = body.displayName || "My Image Warehouse";
        const description = body.description || "No description provided";
        const url = `${WAREHOUSE_API_ROOT}/projects/${projectNumber}/locations/${locationId}/corpora`;

        const token = await getAccessToken();
        const reqBody = {
          display_name: displayName,
          description: description,
          type: "IMAGE",
          search_capability_setting: {
            search_capabilities: { type: "EMBEDDING_SEARCH" }
          }
        };

        const resp = await fetch(url, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(reqBody)
        });

        if (!resp.ok) {
          const txt = await resp.text();
          throw new Error(`createCorpus error: ${resp.status} => ${txt}`);
        }
        const data = await resp.json();
        return json200(data);
      }

      //-----------------------------------------------------------
      // B) uploadAndImport
      //-----------------------------------------------------------
      case "uploadAndImport": {
        /**
         * Instead of passing full "corpusName" here, 
         * we only pass "corpusId" in the request body. 
         * We'll build the entire resource path ourselves using
         * GCP_PROJECT_NUMBER, locationId, and corpusId.
         *
         * Example usage:
         * {
         *   "action": "uploadAndImport",
         *   "corpusId": "8216373799488087483",
         *   "assetId": "someUniqueAssetId",
         *   "base64Image": "data:image/png;base64,iVBORw0K..."
         * }
         */
        const { corpusId, assetId, base64Image } = body;
        if (!corpusId || !assetId || !base64Image) {
          throw new Error(
            "uploadAndImport requires corpusId, assetId, and base64Image"
          );
        }

        // Build the full resource name for the corpus
        const corpusName = `projects/${projectNumber}/locations/${locationId}/corpora/${corpusId}`;

        // 1) Upload image to GCS
        const objectKey = `tempAssets/${assetId}_${Date.now()}.jpg`;
        const gsUri = await uploadBase64ToGCS(base64Image, objectKey);
        console.log("Uploaded to GCS =>", gsUri);

        // 2) createAsset in the corpus
        const token = await getAccessToken();
        const url = `${WAREHOUSE_API_ROOT}/${corpusName}/assets?asset_id=${encodeURIComponent(assetId)}`;

        // We'll stick with the "asset_schema" approach from the earlier fix:
        const reqBody = {
          asset: {
            display_name: assetId,
            media_type: "MEDIA_TYPE_IMAGE",
            asset_schema: {
              // If you want to detect whether it's image/png or image/jpeg 
              // from the base64, you'd parse that out. Here we just assume PNG.
              mime_type: "image/png",
              gcs_uri: gsUri
            }
          }
        };

        const resp = await fetch(url, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(reqBody)
        });

        if (!resp.ok) {
          const txt = await resp.text();
          console.log("FULL error body =>", txt);
          throw new Error(`uploadAndImport: createAsset error: ${resp.status} => ${txt}`);
        }

        const data = await resp.json();
        return json200({
          message: "Asset creation success!",
          gcsUri: gsUri,
          data: data
        });
      }

      //-----------------------------------------------------------
      // C) analyzeCorpus
      //-----------------------------------------------------------
      case "analyzeCorpus": {
        /**
         * Similar to uploadAndImport, we can build corpusName from corpusId.
         * Example usage:
         * {
         *   "action": "analyzeCorpus",
         *   "corpusId": "8216373799488087483"
         * }
         */
        const { corpusId } = body;
        if (!corpusId) {
          throw new Error("analyzeCorpus requires corpusId");
        }
        const corpusName = `projects/${projectNumber}/locations/${locationId}/corpora/${corpusId}`;
        const token = await getAccessToken();
        const url = `${WAREHOUSE_API_ROOT}/${corpusName}:analyze`;

        const reqBody = { name: corpusName };

        const resp = await fetch(url, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(reqBody)
        });

        if (!resp.ok) {
          const txt = await resp.text();
          throw new Error(`analyzeCorpus error: ${resp.status} => ${txt}`);
        }
        const data = await resp.json();
        return json200(data);
      }

      //-----------------------------------------------------------
      // D) createIndex
      //-----------------------------------------------------------
      case "createIndex": {
        /**
         * We'll do the same approach: pass corpusId, build corpusName ourselves.
         * Example usage:
         * {
         *   "action": "createIndex",
         *   "corpusId": "8216373799488087483",
         *   "displayName": "MyIndex",
         *   "description": "Optional desc"
         * }
         */
        const { corpusId, displayName, description } = body;
        if (!corpusId) {
          throw new Error("createIndex requires corpusId");
        }
        const corpusName = `projects/${projectNumber}/locations/${locationId}/corpora/${corpusId}`;

        const dn = displayName || "MyIndex";
        const desc = description || "No description";
        const url = `${WAREHOUSE_API_ROOT}/${corpusName}/indexes`;
        const token = await getAccessToken();

        const reqBody = {
          display_name: dn,
          description: desc
        };

        const resp = await fetch(url, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(reqBody)
        });

        if (!resp.ok) {
          const txt = await resp.text();
          throw new Error(`createIndex error: ${resp.status} => ${txt}`);
        }
        const data = await resp.json();
        return json200(data);
      }

      //-----------------------------------------------------------
      // E) deployIndex
      //-----------------------------------------------------------
      case "deployIndex": {
        /**
         * This one references an existing index name. 
         * Possibly looks like:
         * {
         *   "action": "deployIndex",
         *   "indexName": "projects/123456789012/locations/us-central1/corpora/8216373799488087483/indexes/00000"
         * }
         * We'll keep it straightforward: user must pass the full indexName.
         */
        const { indexName } = body;
        if (!indexName) {
          throw new Error("deployIndex requires indexName");
        }
        const token = await getAccessToken();
        const endpointUrl = `${WAREHOUSE_API_ROOT}/projects/${projectNumber}/locations/${locationId}/indexEndpoints`;

        // Create index endpoint
        const epResp = await fetch(endpointUrl, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify({ display_name: "MyIndexEndpoint" })
        });

        if (!epResp.ok) {
          const txt = await epResp.text();
          throw new Error(`createIndexEndpoint error: ${epResp.status} => ${txt}`);
        }
        const epData = await epResp.json();
        return json200({
          message: "Index endpoint creation (LRO). Next step is deploying your index to it.",
          epData: epData
        });
      }

      //-----------------------------------------------------------
      // F) search
      //-----------------------------------------------------------
      case "search": {
        /**
         * For searching, you pass the full indexEndpointName. 
         * Because that's how Warehouse identifies the deployed index resource:
         *
         * e.g. "projects/123456789012/locations/us-central1/indexEndpoints/1234567890"
         *
         * And optionally textQuery or imageQueryBase64:
         * {
         *   "action": "search",
         *   "indexEndpointName": "...",
         *   "textQuery": "search string"
         *   OR
         *   "imageQueryBase64": "data:image/png;base64,iVBORw0K..."
         * }
         */
        const { indexEndpointName, textQuery, imageQueryBase64 } = body;
        if (!indexEndpointName) {
          throw new Error("search requires indexEndpointName");
        }
        const token = await getAccessToken();
        const url = `${WAREHOUSE_API_ROOT}/${indexEndpointName}:searchIndexEndpoint`;

        const reqBody = {};
        if (textQuery) {
          reqBody.text_query = textQuery;
        }
        if (imageQueryBase64) {
          reqBody.image_query = { input_image: imageQueryBase64 };
        }

        const resp = await fetch(url, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(reqBody)
        });

        if (!resp.ok) {
          const txt = await resp.text();
          throw new Error(`search error: ${resp.status} => ${txt}`);
        }
        const data = await resp.json();
        return json200(data);
      }

      //-----------------------------------------------------------
      // Unknown action
      //-----------------------------------------------------------
      default:
        return json400({ error: `Unknown action => ${action}` });
    }
  } catch (err) {
    console.error("Vision Warehouse function error =>", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message })
    };
  }
};

function json200(obj) {
  return { statusCode: 200, body: JSON.stringify(obj) };
}
function json400(obj) {
  return { statusCode: 400, body: JSON.stringify(obj) };
}