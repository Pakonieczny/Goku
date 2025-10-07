// netlify/functions/updateListingInventory.js
const fetch = require("node-fetch");

exports.handler = async (event) => {
  try {
    const listingId = event.queryStringParameters?.listingId;
    const token =
      event.queryStringParameters?.token ||
      event.headers["access-token"] ||
      event.headers["Access-Token"] ||
      event.headers["authorization"]?.replace(/^Bearer\s+/i, "");
      const clientId =
      event.headers?.["x-api-key"] ||
      event.headers?.["X-Api-Key"] ||
      process.env.CLIENT_ID ||
      process.env.ETSY_CLIENT_ID;

    if (!listingId) return { statusCode: 400, body: JSON.stringify({ error: "Missing listingId parameter" }) };
    if (!token) return { statusCode: 400, body: JSON.stringify({ error: "Missing access token" }) };
    if (!clientId) return { statusCode: 500, body: JSON.stringify({ error: "CLIENT_ID not set" }) };

    // Body: { sku: "Horse_18789" }
    let body = {};
    try { body = event.body ? JSON.parse(event.body) : {}; } catch {}
    const desiredSku = String(body.sku || "").trim();

    // 1) GET current inventory (so we can send the full, sanitized products array back)
    const invUrl = `https://openapi.etsy.com/v3/application/listings/${encodeURIComponent(listingId)}/inventory`;
    const commonHeaders = {
      Accept: "application/json",
      Authorization: `Bearer ${token}`,
      "x-api-key": clientId
    };
    const invRes = await fetch(invUrl, { method: "GET", headers: commonHeaders });

    let products = [];
    if (invRes.ok) {
      const inv = await invRes.json();
      const srcProducts = inv?.products || inv?.results?.products || [];
      // 2) sanitize products per Etsy docs (remove IDs, convert Money -> decimal, drop is_deleted, etc.)
      products = srcProducts.map((p, idx) => {
        const toDecimal = (price) => {
          if (price == null) return undefined;
          if (typeof price === "object" && price.amount != null && price.divisor) {
            return Number(price.amount) / Number(price.divisor);
          }
          return Number(price);
        };
        const offerings = (p.offerings || []).map(o => ({
          price: toDecimal(o.price),
          quantity: o.quantity,
          is_enabled: o.is_enabled !== false
        }));
        const property_values = (p.property_values || []).map(v => ({
          property_id: v.property_id,
          scale_id: v.scale_id ?? null,
          value_ids: v.value_ids || [],
          values: v.values || []
        }));
        // Put the generated SKU on the first product without one (or overwrite first product)
        const sku = (idx === 0 && desiredSku) ? desiredSku : (p.sku || "");
        return { sku, offerings, property_values };
      });
    } else {
      // No inventory yet? Create a minimal single-product inventory with your SKU.
      products = [{
        sku: desiredSku || undefined,
        property_values: [],
        offerings: [{ price: 1.0, quantity: 187, is_enabled: true }]
      }];
    }

    const payload = {
      products,
      price_on_property: [],
      quantity_on_property: [],
      sku_on_property: []
    };

    // 3) PUT updated inventory (this is where the SKU actually gets stored)
    const putRes = await fetch(invUrl, {
      method: "PUT",
      headers: { ...commonHeaders, "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    const text = await putRes.text();
    let data; try { data = JSON.parse(text); } catch { data = { raw: text }; }

    if (!putRes.ok) {
      return { statusCode: putRes.status, body: JSON.stringify({ error: "Error updating inventory", details: data }) };
    }
    return { statusCode: 200, body: JSON.stringify({ ok: true, inventory: data }) };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};