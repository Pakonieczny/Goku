 const fetch = require("node-fetch");
 
 exports.handler = async (event) => {
   if (event.httpMethod === "OPTIONS") {
     return { statusCode: 200, headers: { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers":"Content-Type", "Access-Control-Allow-Methods":"POST,OPTIONS" }, body: "ok" };
   }
   try {
     const { refresh_token } = JSON.parse(event.body || "{}");
     if (!refresh_token) return { statusCode: 400, body: JSON.stringify({ error: "Missing refresh_token" }) };
 
     const CLIENT_ID     = process.env.CLIENT_ID;
     const CLIENT_SECRET = process.env.CLIENT_SECRET;
     if (!CLIENT_ID || !CLIENT_SECRET) {
       return { statusCode: 500, body: JSON.stringify({ error: "Server creds missing" }) };
     }
 
     const params = new URLSearchParams({
       grant_type    : "refresh_token",
       client_id     : CLIENT_ID,
       client_secret : CLIENT_SECRET,
       refresh_token
     });
 
     const r = await fetch("https://api.etsy.com/v3/public/oauth/token", {
       method: "POST",
       headers: { "Content-Type": "application/x-www-form-urlencoded" },
       body: params
     });
     const data = await r.json();
     if (!r.ok) return { statusCode: r.status, body: JSON.stringify(data) };
  
     const issued_at = Math.floor(Date.now()/1000);
     return { statusCode: 200, body: JSON.stringify({ ...data, issued_at }) };
   } catch (err) {
     return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
   }
 };