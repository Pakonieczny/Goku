/* netlify/functions/testChitChatsClient.js */
const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "GET,OPTIONS"
};

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  try {
    const BASE         = process.env.CHIT_CHATS_BASE_URL || "https://chitchats.com/api/v1";
    const CLIENT_ID    = process.env.CHIT_CHATS_CLIENT_ID;
    const ACCESS_TOKEN = process.env.CHIT_CHATS_ACCESS_TOKEN;
    if (!CLIENT_ID || !ACCESS_TOKEN) {
      return { statusCode: 500, headers: CORS, body: JSON.stringify({ success: false, error: "Missing CHIT_CHATS_CLIENT_ID or CHIT_CHATS_ACCESS_TOKEN" }) };
    }

    const authH = { "Authorization": ACCESS_TOKEN, "Content-Type": "application/json; charset=utf-8" };
    const resp  = await fetch(`${BASE}/clients/${encodeURIComponent(CLIENT_ID)}`, { headers: authH });
    const txt   = await resp.text();
    let data; try { data = JSON.parse(txt); } catch { data = txt; }

    if (!resp.ok) return { statusCode: resp.status, headers: CORS, body: JSON.stringify({ success: false, error: data }) };
    return { statusCode: 200, headers: CORS, body: JSON.stringify({ success: true, data }) };

  } catch (err) {
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ success: false, error: err.message }) };
  }
};