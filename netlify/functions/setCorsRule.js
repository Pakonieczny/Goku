/**
 * Netlify Function  →  GET /.netlify/functions/setCorsRule
 * One-shot: writes the CORS JSON to gs://gokudatabase.appspot.com
 */
const { Storage } = require("@google-cloud/storage");

/* reuse the same service-account details you already load in firebaseAdmin.js */
const creds = {
  client_email: process.env.FIREBASE_CLIENT_EMAIL,
  private_key : process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, "\n"),
  project_id  : process.env.FIREBASE_PROJECT_ID
};

exports.handler = async () => {
  try {
    const storage = new Storage({ credentials: creds });
    await storage.bucket("gokudatabase.firebasestorage.app")
      .setCorsConfiguration([{
        origin        : ["https://shipping-1.goldenspike.app"],
        method        : ["GET","POST","PUT","DELETE","HEAD","OPTIONS"],
        responseHeader: ["Content-Type","Authorization"],
        maxAgeSeconds : 3600
      }]);

    return {
      statusCode: 200,
      body: "✅ CORS rule applied – you can delete this function now."
    };
  } catch (err) {
    console.error("CORS update failed:", err);
    return {
      statusCode: 500,
      body: "❌ CORS update failed – check function logs."
    };
  }
};