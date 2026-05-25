// netlify/functions/trackOrderProxy.js
// Proxy → ShipStation “Mark as Shipped” and notify marketplace (Etsy)
//
// ENV VARS required in this Netlify site:
//   SS_API_KEY, SS_API_SECRET
//
// Docs: mark-as-shipped requires orderId; resolve it from orderNumber first.
// https://www.shipstation.com/docs/api/orders/mark-as-shipped/
// https://www.shipstation.com/docs/api/orders/list-orders/

const SS_BASE = "https://ssapi.shipstation.com";
const { SS_API_KEY = "", SS_API_SECRET = "" } = process.env;

const SS_AUTH = Buffer.from(`${SS_API_KEY}:${SS_API_SECRET}`).toString("base64");
const SS_HEADERS_JSON = {
  Authorization: `Basic ${SS_AUTH}`,
  "Content-Type": "application/json",
  Accept: "application/json",
};

exports.handler = async (event) => {
  try {
    if (event.httpMethod === "GET") {
      return { statusCode: 200, body: JSON.stringify({ ok: true, fn: "trackOrderProxy" }) };
    }
    if (event.httpMethod !== "POST") {
      return { statusCode: 405, body: JSON.stringify({ error: "Method Not Allowed" }) };
    }

    // Parse body, tolerate both your field names and ShipStation’s.
    let input = {};
    try { input = JSON.parse(event.body || "{}"); }
    catch { return { statusCode: 400, body: JSON.stringify({ error: "Invalid JSON body" }) }; }

    const orderNumber    = input.orderNumber ?? input.receiptId ?? input.receipt_id ?? "";
    const trackingNumber = input.trackingNumber ?? input.tracking ?? "";
    let   carrierCode    = (input.carrierCode ?? input.carrier ?? "").toLowerCase();
    const shipDate       = input.shipDate ?? new Date().toISOString().slice(0,10); // YYYY-MM-DD

    if (!orderNumber || !trackingNumber || !carrierCode) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing orderNumber/receiptId, trackingNumber/tracking, or carrierCode/carrier" })
      };
    }

    // Normalize carrier → allow "chitchats" / "usps" / etc.
    // ShipStation accepts codes like "usps", "ups", "fedex", or "other".
    if (carrierCode === "chitchats" || carrierCode === "chit_chats") {
      carrierCode = "other";
    }

    // 1) Resolve orderId from orderNumber (exact match preferred).
    const listUrl  = `${SS_BASE}/orders?orderNumber=${encodeURIComponent(orderNumber)}`;
    const listResp = await fetch(listUrl, { method: "GET", headers: SS_HEADERS_JSON });
    const listText = await listResp.text();
    let listJson   = {};
    try { listJson = JSON.parse(listText); } catch {}
    const orders   = Array.isArray(listJson?.orders) ? listJson.orders : [];
    const exact    = orders.find(o => String(o.orderNumber) === String(orderNumber));
    const hit      = exact || orders[0];
    const orderId  = hit?.orderId;

    if (!orderId) {
      return {
        statusCode: 404,
        body: JSON.stringify({
          error: `ShipStation order not found for orderNumber='${orderNumber}'`,
          hint : "Verify the order exists in ShipStation or push it first, then retry."
        })
      };
    }

    // 2) Mark as shipped (notify marketplace).
    const markBody = {
      orderId,
      carrierCode,
      shipDate,
      trackingNumber,
      notifyCustomer    : false,
      notifySalesChannel: true
    };

    const markResp = await fetch(`${SS_BASE}/orders/markasshipped`, {
      method : "POST",
      headers: SS_HEADERS_JSON,
      body   : JSON.stringify(markBody)
    });

    const upstreamText = await markResp.text();

    // Pass through success; upgrade empty-error statuses to JSON with context
    if (markResp.ok) {
      return { statusCode: 200, body: upstreamText || JSON.stringify({ ok:true, orderId, orderNumber }) };
    } else {
      let upstream;
      try { upstream = JSON.parse(upstreamText); } catch { upstream = { raw: upstreamText }; }
      return {
        statusCode: markResp.status || 502,
        body: JSON.stringify({
          error  : upstream?.message || upstream?.error || "ShipStation mark-as-shipped failed",
          status : markResp.status,
          details: upstream,
          orderId, orderNumber
        })
      };
    }
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message || String(err) }) };
  }
};