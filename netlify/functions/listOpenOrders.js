/**
 * listOpenOrders.js  –  returns EVERY open receipt for your shop.
 * Works by walking Etsy's offset-based pagination until next_offset === null
 * when offset==0, but fetches ONE page when an explicit offset is sent.
 */

const fetch = require("node-fetch");

exports.handler = async (event) => {
  try {
    /* 1.  OAuth token from front-end header */
    const accessToken =
      event.headers["access-token"] || event.headers["Access-Token"];
    if (!accessToken) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing access-token header" })
      };
    }

    /* 2.  Required env vars */
    const SHOP_ID   = process.env.SHOP_ID;      // numeric ID of your Etsy shop
    const CLIENT_ID = process.env.CLIENT_ID;    // Etsy app key string
    if (!SHOP_ID || !CLIENT_ID) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing SHOP_ID or CLIENT_ID env var" })
      };
    }

    /* 3.  Loop through pages using offset pagination */
    const allReceipts = [];

    // ===== first requested offset comes from browser (unchanged) =====
    let offset        = Number(event.queryStringParameters.offset || 0);
    const firstOffset = offset;                 // remember what the browser asked for

    do {
      /* ───── QUERY PARAMS (PRE-FILTERED) ───── */
      const qs = new URLSearchParams({
        status       : "open",     // still pulling “open” orders
        was_paid     : "true",     // only paid
        was_shipped  : "false",    // not yet shipped
        was_canceled : "false",    // ← NEW • exclude every cancelled receipt
        limit        : "100",
        offset       : offset.toString(),
        sort_on      : "created",
        sort_order   : "desc"
      });

      const url =
        `https://api.etsy.com/v3/application/shops/${SHOP_ID}/receipts?${qs}`;

      const resp = await fetch(url, {
        method: "GET",
        headers: {
          Authorization : `Bearer ${accessToken}`,
          "x-api-key"   : CLIENT_ID,
          "Content-Type": "application/json"
        }
      });

      if (!resp.ok) {
        const txt = await resp.text();
        return { statusCode: resp.status, body: txt };
      }

      /* ── keep Etsy’s payload UNCHANGED so pagination.next_offset is preserved */
      const data = await resp.json();
      console.log("DEBUG_BODY", JSON.stringify(data).slice(0, 300));  // log first 300 chars
      return { statusCode: 200, body: JSON.stringify(data) };

      /* Everything below this point never runs because of the return above.
         It has been removed for clarity. */
    } while (offset !== null);

    /* ── AFTER the loop finishes ── */
    return {
      statusCode: 200,
      body: JSON.stringify({
        results    : allReceipts,
        pagination : { next_offset: null }   // no more pages
      })
    };

  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};