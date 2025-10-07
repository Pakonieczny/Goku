/* netlify/functions/chitChatSearch.js */
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
    const params   = event.queryStringParameters || {};
    const orderId  = (params.orderId  || "").trim();   // Etsy receipt or your internal order id
    const tracking = (params.tracking || "").trim();   // CC shipment id OR carrier tracking code
    const q        = (params.q        || "").trim();   // arbitrary search string
    const status   = (params.status   || "").trim();   // e.g. ready, in_transit
    const limit    = String(params.limit || 100);
    const page     = String(params.page  || 1);

    const BASE         = process.env.CHIT_CHATS_BASE_URL || "https://chitchats.com/api/v1";
    const CLIENT_ID    = process.env.CHIT_CHATS_CLIENT_ID;
    const ACCESS_TOKEN = process.env.CHIT_CHATS_ACCESS_TOKEN;
    if (!CLIENT_ID || !ACCESS_TOKEN) {
      return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: "Missing CHIT_CHATS_CLIENT_ID or CHIT_CHATS_ACCESS_TOKEN" }) };
    }

    const baseShip = `${BASE}/clients/${encodeURIComponent(CLIENT_ID)}/shipments`;
    const authH = { "Authorization": ACCESS_TOKEN, "Content-Type": "application/json; charset=utf-8" };

    const getJSON = async (url) => {
      const r = await fetch(url, { headers: authH });
      const txt = await r.text();
      let data; try { data = JSON.parse(txt); } catch { data = txt; }
      if (!r.ok) throw new Error(typeof data === "string" ? data : JSON.stringify(data));
      return data;
    };

    const shape = (s) => ({
      id: s.id,
      order_id: s.order_id,
      status: s.status,
      batch_id: s.batch_id,
      carrier_tracking_code: s.carrier_tracking_code,
      tracking_url: s.tracking_url,
      postage_label_pdf_url: s.postage_label_pdf_url
    });

    // (i) Exact orderId via full-text q filter, then exact match on order_id
    if (orderId) {
      const url = `${baseShip}?limit=1000&q=${encodeURIComponent(orderId)}${status ? `&status=${encodeURIComponent(status)}` : ""}`;
      const list = await getJSON(url);
      const exact = Array.isArray(list) ? list.filter(s => String(s.order_id) === orderId) : [];
      return { statusCode: 200, headers: CORS, body: JSON.stringify({ success: true, matchType: "byOrderId", shipments: exact.map(shape) }) };
    }

    // (ii) Tracking: try direct shipment id, then match carrier_tracking_code via q
    if (tracking) {
      let out = [];
      if (/^[A-Za-z0-9]{8,14}$/.test(tracking)) {
        try {
          const s = await getJSON(`${baseShip}/${encodeURIComponent(tracking)}`);
          if (s && s.id) out.push(shape(s));
        } catch { /* ignore and fall back */ }
      }
      if (!out.length) {
        const url  = `${baseShip}?limit=1000&q=${encodeURIComponent(tracking)}`;
        const list = await getJSON(url);
        const match = Array.isArray(list) ? list.filter(s => (s.carrier_tracking_code || "").toUpperCase() === tracking.toUpperCase()) : [];
        out = match.map(shape);
      }
      return { statusCode: 200, headers: CORS, body: JSON.stringify({ success: true, matchType: "byTracking", shipments: out }) };
    }

    // (iii) Generic listing/search for UI fallbacks
    const url = `${baseShip}?limit=${encodeURIComponent(limit)}&page=${encodeURIComponent(page)}`
              + (q ? `&q=${encodeURIComponent(q)}` : "")
              + (status ? `&status=${encodeURIComponent(status)}` : "");
    const list = await getJSON(url);
    return { statusCode: 200, headers: CORS, body: JSON.stringify({ success: true, matchType: "byQuery", shipments: (Array.isArray(list) ? list : []).map(shape) }) };

  } catch (err) {
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ success: false, error: err.message }) };
  }
};