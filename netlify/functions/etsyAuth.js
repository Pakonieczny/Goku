// lib/etsyAuth.js — server-side Etsy OAuth token manager (auto-refresh)
 const fetch = require("node-fetch");
 const admin = require("firebase-admin");
 
 if (!admin.apps.length) {
   // Uses GOOGLE_APPLICATION_CREDENTIALS or Netlify env-injected credentials
   admin.initializeApp({ credential: admin.credential.applicationDefault() });
 }
 const db = admin.firestore();
 
 const DOC_PATH = "config/etsy/oauth";
 
 async function readToken() {
   const snap = await db.doc(DOC_PATH).get();
   return snap.exists ? snap.data() : null;
 }
 
 async function saveToken(tok) {
   await db.doc(DOC_PATH).set(tok, { merge: true });
 }
 
 async function refreshWith(refresh_token) {
   const res = await fetch("https://api.etsy.com/v3/public/oauth/token", {
      method: "POST",
     headers: { "Content-Type": "application/x-www-form-urlencoded" },
     body: new URLSearchParams({
       grant_type: "refresh_token",
       client_id : process.env.CLIENT_ID,     // Etsy App keystring
       refresh_token
     })
   });
   if (!res.ok) {
     const t = await res.text();
     throw new Error(`Etsy refresh failed: ${res.status} ${t}`);
   }
   const json = await res.json(); // { access_token, expires_in, refresh_token, ... }
   const expires_at = Date.now() + Math.max(0, (json.expires_in - 90)) * 1000; // refresh ~90s early
   const stored = {
     access_token : json.access_token,
     refresh_token: json.refresh_token || refresh_token, // Etsy rotates; keep old if absent
     expires_at
   };
   await saveToken(stored);
   return stored;
 }
 
 async function getValidEtsyAccessToken() {
   let tok = await readToken();
   if (!tok || !tok.refresh_token) throw new Error("No Etsy refresh token in store.");
   const needs = !tok.expires_at || (Date.now() >= tok.expires_at - 120000);
   if (needs) tok = await refreshWith(tok.refresh_token);
   return tok.access_token;
 }
 
 module.exports = { getValidEtsyAccessToken, refreshWith, readToken, saveToken };