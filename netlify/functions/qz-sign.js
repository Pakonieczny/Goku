/* netlify/functions/qz-sign.js */
const crypto = require("crypto");
const PRIV = process.env.QZ_PRIVATE_KEY_PEM; // Load your RSA private key (PEM)

exports.handler = async (event) => {
  if (event.httpMethod !== "POST") return { statusCode: 405, body: "Method Not Allowed" };
  const { toSign } = JSON.parse(event.body || "{}");
  if (!toSign) return { statusCode: 400, body: "toSign required" };
  const sign = crypto.createSign("sha256");
  sign.update(toSign); sign.end();
  const signature = sign.sign(PRIV, "base64");
  return { statusCode: 200, body: signature };
};