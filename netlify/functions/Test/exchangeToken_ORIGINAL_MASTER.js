// netlify/functions/exchangeToken.js
// A single function that auto-detects your domain from request headers.
// If "sorting.goldenspike.app" => uses that redirect
// else if "goldenspike.app" => uses that
// else => defaults to goldenspike (you can invert this default if you wish).

const fetch  = require("node-fetch");
const crypto = require("crypto");

// We only allow these two final domains:
const SORTING_DOMAIN      = "https://sorting.goldenspike.app";
const SORTING2_DOMAIN      = "https://sorting-2.goldenspike.app";
const GOLDENSPIKE_DOMAIN  = "https://goldenspike.app";
const DESIGN_DOMAIN       = "https://design.goldenspike.app";
const DESIGNMESSAGE_DOMAIN       = "https://design-message.goldenspike.app";
const DESIGNMESSAGE1_DOMAIN       = "https://design-message-1.goldenspike.app";
// ─── new assembly domains ───────────────────────────────────────────
const ASSEMBLY1_DOMAIN   = "https://assembly-1.goldenspike.app";
const ASSEMBLY2_DOMAIN   = "https://assembly-2.goldenspike.app";
const ASSEMBLY3_DOMAIN   = "https://assembly-3.goldenspike.app";
const ASSEMBLY4_DOMAIN   = "https://assembly-4.goldenspike.app";

// ─── new shipping domains ───────────────────────────────────────────
const SHIPPING1_DOMAIN   = "https://shipping-1.goldenspike.app";
const SHIPPING2_DOMAIN   = "https://shipping-2.goldenspike.app";
const SHIPPING3_DOMAIN   = "https://shipping-3.goldenspike.app";

// ─── new weld + design sub-domains ──────────────────────────────────
const WELD1_DOMAIN       = "https://weld-1.goldenspike.app";
const DESIGN1_DOMAIN     = "https://design-1.goldenspike.app";

/* 🆕 global CORS constants */
const CORS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
};

// Helpers for PKCE:
function generateRandomString(length) {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let text = "";
  for (let i = 0; i < length; i++) {
    text += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return text;
}

function generateCodeChallenge(codeVerifier) {
  const hash = crypto.createHash("sha256").update(codeVerifier).digest();
  return hash.toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

// Helper to auto-detect domain from the request's host header
function pickDomainFromHost(event) {
  // 'x-forwarded-host' is typical under Netlify; fallback to Host if needed
  const host = (event.headers["x-forwarded-host"] || event.headers["host"] || "").toLowerCase(); // normalized
  console.log("exchangeToken => Detected host:", host);

  // normalize query param
  const param = (event.queryStringParameters || {}).redirect_domain || "";
  const paramLower = param.trim().toLowerCase(); // normalized
  
  /* ── query-param overrides ───────────────────────────────────────── */
  switch (paramLower) {
    case "sorting":      return SORTING_DOMAIN;
    case "sorting-2":    return SORTING2_DOMAIN;  
    case "weld-1":       return WELD1_DOMAIN;
    case "design-1":     return DESIGN1_DOMAIN;
    case "design-message":     return DESIGNMESSAGE_DOMAIN;
    case "design-message-1":     return DESIGNMESSAGE1_DOMAIN;
    case "assembly-1":   return ASSEMBLY1_DOMAIN;
    case "assembly-2":   return ASSEMBLY2_DOMAIN;
    case "assembly-3":   return ASSEMBLY3_DOMAIN;
    case "assembly-4":   return ASSEMBLY4_DOMAIN;
    case "shipping-1":   return SHIPPING1_DOMAIN;
    case "shipping-2":   return SHIPPING2_DOMAIN;
    case "shipping-3":   return SHIPPING3_DOMAIN;
    case "goldenspike":  return GOLDENSPIKE_DOMAIN;
    case "design":       return DESIGN_DOMAIN;
    default:
      break; // fall through to host
  }

  /* ── host header autodetect ──────────────────────────────────────── */
  if (host.includes("sorting.goldenspike.app"))      return SORTING_DOMAIN;
  if (host.includes("sorting-2.goldenspike.app"))    return SORTING2_DOMAIN;
  if (host.includes("assembly-1.goldenspike.app"))   return ASSEMBLY1_DOMAIN;
  if (host.includes("assembly-2.goldenspike.app"))   return ASSEMBLY2_DOMAIN;
  if (host.includes("assembly-3.goldenspike.app"))   return ASSEMBLY3_DOMAIN;
  if (host.includes("assembly-4.goldenspike.app"))   return ASSEMBLY4_DOMAIN;
  if (host.includes("shipping-1.goldenspike.app"))   return SHIPPING1_DOMAIN;
  if (host.includes("shipping-2.goldenspike.app"))   return SHIPPING2_DOMAIN;
  if (host.includes("shipping-3.goldenspike.app"))   return SHIPPING3_DOMAIN;
  if (host.includes("weld-1.goldenspike.app"))       return WELD1_DOMAIN;
  if (host.includes("design-1.goldenspike.app"))     return DESIGN1_DOMAIN;
  if (host.includes("design.goldenspike.app"))       return DESIGN_DOMAIN;
  if (host.includes("design-message.goldenspike.app"))       return DESIGNMESSAGE_DOMAIN;
  if (host.includes("design-message-1.goldenspike.app"))       return DESIGNMESSAGE1_DOMAIN;
  if (host.includes("goldenspike.app"))              return GOLDENSPIKE_DOMAIN;

  /* ── fallback ────────────────────────────────────────────────────── */
  return GOLDENSPIKE_DOMAIN;
}

exports.handler = async function(event) {

  /* 🆕 quick reply for pre-flight requests */
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  try {
    const query = event.queryStringParameters || {};
    const code = query.code;
    const codeVerifier = query.code_verifier;

    // Decide which domain to use
    const finalRedirectUri = pickDomainFromHost(event);
    console.log("exchangeToken => finalRedirectUri:", finalRedirectUri);

    // If no code => begin OAuth PKCE handshake
    if (!code) {
      const newCodeVerifier = generateRandomString(64);
      const codeChallenge   = generateCodeChallenge(newCodeVerifier);
      const CLIENT_ID       = process.env.CLIENT_ID;
      if (!CLIENT_ID) {
        return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: "Server config error" }) };
      }

      const scope = "listings_w listings_r transactions_r transactions_w";
      const state = "randomState123";
      const oauthUrl =
        "https://www.etsy.com/oauth/connect" +
        `?response_type=code&client_id=${CLIENT_ID}` +
        `&redirect_uri=${encodeURIComponent(finalRedirectUri)}` +
        `&scope=${encodeURIComponent(scope)}` +
        `&state=${state}` +
        `&code_challenge=${encodeURIComponent(codeChallenge)}` +
        `&code_challenge_method=S256`;

      return { statusCode: 302, headers: { ...CORS, Location: oauthUrl }, body: "" };
    }

    // Require code_verifier
    if (!codeVerifier) {
      return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: "Missing code_verifier param" }) };
    }

    // Exchange token
    const CLIENT_ID     = process.env.CLIENT_ID;
    const CLIENT_SECRET = process.env.CLIENT_SECRET;
    if (!CLIENT_ID || !CLIENT_SECRET) {
      return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: "Server creds missing" }) };
    }

    const params = new URLSearchParams({
      grant_type: "authorization_code",
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      code,
      redirect_uri: finalRedirectUri,
      code_verifier: codeVerifier
    });

    const resp  = await fetch("https://api.etsy.com/v3/public/oauth/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: params
    });
    const data  = await resp.json();
    if (!resp.ok) {
      return { statusCode: resp.status, headers: CORS, body: JSON.stringify(data) };
    }

    const finalUrl = `${finalRedirectUri}?access_token=${encodeURIComponent(data.access_token)}`;
    return { statusCode: 302, headers: { ...CORS, Location: finalUrl }, body: "" };

  } catch (err) {
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: err.message }) };
  }
};