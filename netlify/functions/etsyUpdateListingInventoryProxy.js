// netlify/functions/etsyUpdateListingInventoryProxy.js
const fetch = require("node-fetch");

exports.handler = async (event) => {
  try {
    if (event.httpMethod !== "POST") {
      return { statusCode: 405, body: "Method Not Allowed" };
    }

    const accessToken = event.headers["access-token"] || event.headers["Access-Token"];
    const clientId    = process.env.CLIENT_ID;
    if (!accessToken) return { statusCode: 400, body: JSON.stringify({ error: "Missing access token" }) };
    if (!clientId)    return { statusCode: 500, body: JSON.stringify({ error: "Missing CLIENT_ID" }) };

    const body = JSON.parse(event.body || "{}");
    const listingId = body.listing_id;
    const items     = Array.isArray(body.items) ? body.items : [];

    if (!listingId) return { statusCode: 400, body: JSON.stringify({ error: "Missing listing_id" }) };
    if (!items.length) return { statusCode: 400, body: JSON.stringify({ error: "No items provided" }) };

    // 1) Fetch current inventory (we PUT the full document back with SKU edits)
    const getUrl = `https://openapi.etsy.com/v3/application/listings/${listingId}/inventory`;
    const getResp = await fetch(getUrl, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "x-api-key": clientId,
        "Content-Type": "application/json"
      }
    });
    const inv = await getResp.json();
    if (!getResp.ok) {
      return { statusCode: getResp.status, body: JSON.stringify(inv) };
    }

    // 2) Determine Variant 1 (first product) and apply that SKU consistently to ALL variants
    const list = Array.isArray(inv.products) ? inv.products : [];
    if (!list.length) {
      return { statusCode: 400, body: JSON.stringify({ error: "No variants found for this listing" }) };
    }
    const targetPid = Number(list[0].product_id); // Variant 1
    const requested = Array.isArray(items) ? items.find(i => Number(i.product_id) === targetPid) : null;
    if (!requested) {
      return { statusCode: 400, body: JSON.stringify({ error: "Missing Variant 1 in items (product_id of first variant required)" }) };
    }
    const newSku = String(requested.sku || "").trim();

    // Enforce SKU consistency across ALL products to satisfy Etsy API rule
    const updated = list.map(p => {
      p.sku = newSku;
      return p;
    });

    // Helper: normalize Etsy Money formats to decimal number
    function toDecimalPrice(price) {
      // v3 GET may return an object or array of Money; PUT wants a decimal
      const m = Array.isArray(price) ? price[0] : price;
      if (m && typeof m === "object" && m.amount != null) {
        const amt = Number(m.amount);
        const div = Number(m.divisor || 100);
        return +(amt / div).toFixed(2);
      }
      // already a number/string; let API accept it
      return typeof price === "string" ? Number(price) : price;
    }

    // 3) Build sanitized products array for PUT (remove invalid keys)
    const products = updated.map(p => ({
      sku: p.sku || "",
      // keep only allowed property_values fields
      property_values: Array.isArray(p.property_values) ? p.property_values.map(v => ({
        property_id: v.property_id,
        property_name: v.property_name ?? undefined,
        scale_id: v.scale_id ?? null,
        value_ids: Array.isArray(v.value_ids) ? v.value_ids : [],
        values: Array.isArray(v.values) ? v.values : []
      })) : [],
      // keep only allowed offerings fields and convert price
      offerings: Array.isArray(p.offerings) ? p.offerings.map(o => ({
        quantity: Number(o.quantity ?? o.available_quantity ?? 0),
        is_enabled: o.is_enabled !== false,
        price: toDecimalPrice(o.price)
      })) : []
    }));

    // 4) PUT back to Etsy (include *_on_property if present)
    const putUrl = `https://openapi.etsy.com/v3/application/listings/${listingId}/inventory`;
    const putResp = await fetch(putUrl, {
      method: "PUT",
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "x-api-key": clientId,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        products,
        price_on_property: inv.price_on_property || [],
        quantity_on_property: inv.quantity_on_property || [],
        sku_on_property: inv.sku_on_property || []
      })
    });
    const result = await putResp.json();
    if (!putResp.ok) {
      return { statusCode: putResp.status, body: JSON.stringify(result) };
    }

    return { statusCode: 200, body: JSON.stringify({ ok: true, listing_id: listingId }) };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};