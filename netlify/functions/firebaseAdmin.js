const admin = require("firebase-admin");

const serviceAccount = {
  type: "service_account",
  project_id: process.env.FIREBASE_PROJECT_ID,
  private_key_id: process.env.FIREBASE_PRIVATE_KEY_ID,
  private_key: process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, "\n"),
  client_email: process.env.FIREBASE_CLIENT_EMAIL,
  client_id: process.env.FIREBASE_CLIENT_ID,
  auth_uri: process.env.FIREBASE_AUTH_URI,
  token_uri: process.env.FIREBASE_TOKEN_URI,
  auth_provider_x509_cert_url: process.env.FIREBASE_AUTH_PROVIDER_X509_CERT_URL,
  client_x509_cert_url: process.env.FIREBASE_CLIENT_X509_CERT_URL,
  universe_domain: process.env.FIREBASE_UNIVERSE_DOMAIN
};

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

/* ─── ensure CORS rule (runs once per cold-start) ───────── */
if (!process.env.CORS_SET) {
  const { Storage } = require("@google-cloud/storage");
  new Storage({ credentials: serviceAccount })
    .bucket("gokudatabase.firebasestorage.app")
    .setCorsConfiguration([{
      origin        : [
        "https://shipping-1.goldenspike.app",
        "https://design-message.goldenspike.app",
        "https://design-message-1.goldenspike.app"
      ],
      method        : ["GET","POST","PUT","DELETE","HEAD","OPTIONS"],
      responseHeader: ["Content-Type","Authorization"],
      maxAgeSeconds : 3600
    }])
    .then(() => console.log("CORS confirmed"))
    .catch(err => console.error("CORS error:", err));
  process.env.CORS_SET = "1";   // prevent repeats on warm invokes
}

module.exports = admin;