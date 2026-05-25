// netlify/functions/ssGetGiftMessage.js
// Node 18 runtime (CommonJS)

const fetch = require("node-fetch");   // bundled by Netlify

exports.handler = async (event) => {
  const orderNumber = event.queryStringParameters?.orderNumber;
  if (!orderNumber)
    return { statusCode: 400, body: "Missing orderNumber" };

  // ── ShipStation Basic-Auth ──
  const { SS_API_KEY, SS_API_SECRET } = process.env;
  const AUTH = "Basic " + Buffer.from(`${SS_API_KEY}:${SS_API_SECRET}`).toString("base64");
  const base = "https://ssapi.shipstation.com";

  // helper: query ShipStation endpoint, return first order or null
  const query = async (url) => {
    const r = await fetch(url, { headers: { Authorization: AUTH } });
    if (r.status === 404) return null;           // nothing found at this URL
    if (!r.ok) {
      const txt = await r.text();
      throw new Error(`ShipStation ${r.status}: ${txt || r.statusText}`);
    }
    const j = await r.json();
    return j.orders?.[0] || null;
  };

  try {
    // 1️⃣  Try orderNumber field first, then fall back to customerOrderId
    let order =
      await query(`${base}/orders?orderNumber=${encodeURIComponent(orderNumber)}`) ||
      await query(`${base}/orders?customerOrderId=${encodeURIComponent(orderNumber)}`);

    // 2️⃣  Final fallback: advancedsearch (matches partials, prefixes, etc.)
    if (!order) {
      order = await query(
        `${base}/orders/advancedsearch?orderNumber=${encodeURIComponent(orderNumber)}&page=1&pageSize=1`
      );
    }

    if (!order)
      return { statusCode: 404, body: "Order not found in ShipStation" };

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        giftMessage: order.giftMessage?.trim() || "",
        giftFrom:
          order.giftMessageFrom?.trim() ||
          order.billTo?.name?.trim() ||
          ""
      })
    };
  } catch (err) {
    console.error("[ssGetGiftMessage]", err);
    return { statusCode: 500, body: err.message };
  }
};