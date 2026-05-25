/* netlify/functions/cherryRelay.js
   ─────────────────────────────────────────────────────────────────────────
   Fetches a temp zip from Firebase Storage and streams it back to the
   Cherry3D Viewer, bypassing the CORS restriction that blocks the viewer
   from fetching directly from firebasestorage.googleapis.com.

   Called by cherry-viewer.goldenspike.app with:
     GET /.netlify/functions/cherryRelay?path=cherry_relay/filename.zip

   Security:
   - Only serves files under the cherry_relay/ prefix
   - File must exist in Firebase Storage (404 otherwise)
   - No auth required on read — matches the Firebase Storage rule
   ───────────────────────────────────────────────────────────────────────── */

const admin = require("./firebaseAdmin");

exports.handler = async (event) => {
  const CORS_HEADERS = {
    "Access-Control-Allow-Origin":  "https://cherry-viewer.goldenspike.app",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
  };

  // Handle preflight
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: CORS_HEADERS, body: "" };
  }

  if (event.httpMethod !== "GET") {
    return { statusCode: 405, headers: CORS_HEADERS, body: "Method Not Allowed" };
  }

  const filePath = event.queryStringParameters?.path;

  // Validate — only allow cherry_relay/ prefix
  if (!filePath || !filePath.startsWith("cherry_relay/")) {
    return {
      statusCode: 400,
      headers: CORS_HEADERS,
      body: JSON.stringify({ error: "Missing or invalid path. Must be under cherry_relay/." }),
    };
  }

  try {
    const bucket = admin.storage().bucket(
      process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app"
    );

    const file = bucket.file(filePath);

    // Check the file exists
    const [exists] = await file.exists();
    if (!exists) {
      return {
        statusCode: 404,
        headers: CORS_HEADERS,
        body: JSON.stringify({ error: "Relay file not found. It may have already been cleaned up." }),
      };
    }

    // Download into a buffer and return as base64
    const [contents] = await file.download();

    return {
      statusCode: 200,
      headers: {
        ...CORS_HEADERS,
        "Content-Type":        "application/zip",
        "Content-Disposition": `attachment; filename="${filePath.split("/").pop()}"`,
        "Cache-Control":       "no-store",
      },
      body:            contents.toString("base64"),
      isBase64Encoded: true,
    };

  } catch (err) {
    console.error("[cherryRelay] Error:", err.message);
    return {
      statusCode: 500,
      headers: CORS_HEADERS,
      body: JSON.stringify({ error: err.message }),
    };
  }
};
