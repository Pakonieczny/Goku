/*  netlify/functions/firebaseOrders.js  */
const admin = require("./firebaseAdmin");
const db    = admin.firestore();

/* 🆕 global CORS headers */
const CORS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
};

exports.handler = async (event, context) => {

  /* 🆕 instant response for pre-flight */
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  try {
    const method = event.httpMethod;

    /* ───────────────────────── POST ───────────────────────── */
    if (method === "POST") {
      const body = JSON.parse(event.body || "{}");

      const {
        orderNumber,
        orderNumField,
        clientName,
        britesMessages,
        shippingLabelTimestamps,
        employeeName,
        newMessage,
        staffNote             
      } = body;

      if (!orderNumber) {
        return {
          statusCode: 400,
          headers: CORS,
          body: JSON.stringify({ error: "No orderNumber provided" })
        };
      }

      /* 1️⃣  Handle live-chat messages */
      if (typeof newMessage === "string" && newMessage.trim() !== "") {
        await db
          .collection("Brites_Orders")
          .doc(orderNumber)
          .collection("messages")
          .add({
            text       : newMessage.trim(),
            senderName : employeeName || "Staff",
            senderRole : "staff",
            timestamp  : admin.firestore.FieldValue.serverTimestamp()
          });

        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({ success: true, message: "Chat doc added." })
        };
      }

      /* 2️⃣  Merge order-level fields (only those actually present) */
      const dataToStore = {};
      if (orderNumField           !== undefined) dataToStore["Order Number"]              = orderNumField;
      if (clientName              !== undefined) dataToStore["Client Name"]               = clientName;
      if (britesMessages          !== undefined) dataToStore["Brites Messages"]           = britesMessages;
      if (shippingLabelTimestamps !== undefined) dataToStore["Shipping Label Timestamps"] = shippingLabelTimestamps;
      if (employeeName            !== undefined) dataToStore["Employee Name"]             = employeeName;
      if (staffNote               !== undefined) dataToStore["Staff Note"]                = staffNote;

      if (Object.keys(dataToStore).length === 0) {
        return {
          statusCode: 200,
          headers: CORS,
          body: JSON.stringify({ success: true, message: "Nothing to update." })
        };
      }

      await db
        .collection("Brites_Orders")
        .doc(orderNumber)
        .set(dataToStore, { merge: true });

      return {
        statusCode: 200,
        headers: CORS,
        body: JSON.stringify({
          success: true,
          message: `Order doc ${orderNumber} created/updated.`
        })
      };
    }

/* ───────────────────────── GET (replace this block) ──────────────── */
if (method === "GET") {

  /* 🆕  bulk query:  ?staffNotes=1  →  array of order IDs with Staff Note */
  if (event.queryStringParameters?.staffNotes === "1") {
    const snap = await db.collection("Brites_Orders")
                         .where("Staff Note", "!=", "")
                         .select()            // ids only
                         .get();
    return {
      statusCode: 200,
      headers: CORS,
      body: JSON.stringify({
        success      : true,
        orderNumbers : snap.docs.map(d => d.id)
      })
    };
  }

  /* ── single-order fetch (legacy path) ── */
  const { orderId } = event.queryStringParameters || {};
  if (!orderId) {                         //  ← no longer tripped by staffNotes call
    return { statusCode: 400, headers: CORS,
             body: JSON.stringify({ success:false, msg:"orderId required" }) };
  }

  const docSnap = await db.collection("Brites_Orders").doc(orderId).get();

  /* 200 ➜ quieter “not found”, caller checks success flag instead of .ok */
  if (!docSnap.exists) {
    return {
      statusCode: 200,
      headers: CORS,
      body: JSON.stringify({ success: false, notFound: true })
    };
  }

  return {
    statusCode: 200,
    headers: CORS,
    body: JSON.stringify({ success: true, data: docSnap.data() })
  };
}

    /* ─────────────────────── fallback ─────────────────────── */
    return {
      statusCode: 405,
      headers: CORS,
      body: JSON.stringify({ error: "Method Not Allowed" })
    };

  } catch (error) {
    console.error("Error in firebaseOrders function:", error);
    return {
      statusCode: 500,
      headers: CORS,
      body: JSON.stringify({ error: error.message })
    };
  }
};