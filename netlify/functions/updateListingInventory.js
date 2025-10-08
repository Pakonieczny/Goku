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

    // Body: { sku: "Horse_18789" }  -> when provided, enforce uniform SKU across all products
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

    // Hoist on_property arrays so they exist even if GET fails (avoids ReferenceError)
    let price_on_property = [];
    let quantity_on_property = [];
    let sku_on_property = [];
    let readiness_state_on_property = [];

    let products = [];
    let globalReadyId;
    let usesProcessingProfiles = false;

    // SKU policy variables
    let skuMode = "keep";     // "uniform" | "vary" | "none" | "keep"
    let uniformSku;           // set when skuMode === "uniform"

    if (invRes.ok) {
      const inv = await invRes.json();
      const srcProducts = inv?.products || inv?.results?.products || [];

      // Preserve on_property arrays (v3 returns them at top level or under results)
      price_on_property            = inv?.price_on_property            ?? inv?.results?.price_on_property            ?? [];
      quantity_on_property         = inv?.quantity_on_property         ?? inv?.results?.quantity_on_property         ?? [];
      sku_on_property              = inv?.sku_on_property              ?? inv?.results?.sku_on_property              ?? [];
      readiness_state_on_property  = inv?.readiness_state_on_property  ?? inv?.results?.readiness_state_on_property  ?? [];

      // Detect a global readiness_state_id from any offering
      (function detectGlobalReadyId() {
        const prods = inv?.products || inv?.results?.products || [];
        for (const pr of prods) {
          for (const o of (pr?.offerings || [])) {
            const idNum = Number(o?.readiness_state_id);
            if (Number.isFinite(idNum)) {
              globalReadyId = idNum;
              return;
            }
          }
        }
      })();
      usesProcessingProfiles = (Array.isArray(readiness_state_on_property) && readiness_state_on_property.length > 0) || (globalReadyId != null);

      // Decide SKU policy
      const skuVaries = Array.isArray(sku_on_property) && sku_on_property.length > 0;
      const existingSkus = srcProducts.map(p => (p.sku || "").trim()).filter(Boolean);
      const firstExisting = existingSkus[0];

      if (desiredSku) {
        // Caller wants one SKU everywhere: enforce uniform + clear sku_on_property
        skuMode = "uniform";
        uniformSku = desiredSku;
        sku_on_property = [];
      } else if (skuVaries) {
        // Listing declares SKU varies by property; keep per-variant SKUs
        skuMode = "vary";
      } else {
        // No sku_on_property => must be consistent, so unify or drop all
        if (existingSkus.length === 0) {
          skuMode = "none"; // consistent "no sku" across all products
        } else {
          skuMode = "uniform";
          uniformSku = firstExisting; // unify to first non-empty
        }
      }

      // 2) sanitize products per Etsy docs (remove IDs, convert Money -> decimal, drop is_deleted, etc.)
      products = srcProducts.map((p) => {
        const toDecimal = (price) => {
          if (price == null) return undefined;
          if (typeof price === "object" && price.amount != null && price.divisor) {
            return Number(price.amount) / Number(price.divisor);
          }
          return Number(price);
        };

        const offeringsSrc = Array.isArray(p.offerings) ? p.offerings : [];
        let offerings = offeringsSrc
          .map(o => {
            const raw = toDecimal(o.price);
            const price = Number.isFinite(raw) ? Number(raw.toFixed(2)) : undefined;
            // Preserve readiness_state_id; if missing but listing uses processing profiles, fall back to globalReadyId
            const rsidRaw = o?.readiness_state_id;
            const rsidNum = Number(rsidRaw);
            const readiness_state_id = Number.isFinite(rsidNum)
              ? rsidNum
              : (usesProcessingProfiles ? globalReadyId : undefined);
            const base = {
              price,
              quantity: (o.quantity == null ? 1 : o.quantity),
              is_enabled: o.is_enabled !== false
            };
            if (readiness_state_id != null) base.readiness_state_id = readiness_state_id;
            return base;
          })
          .filter(o => Number.isFinite(o.price));

        // Ensure at least one valid offering
        if (offerings.length === 0) {
          const fallbackRaw = toDecimal(offeringsSrc[0]?.price);
          const fallbackPrice = Number.isFinite(fallbackRaw) ? Number(fallbackRaw.toFixed(2)) : 1.0;
          const fallback = {
            price: fallbackPrice,
            quantity: offeringsSrc[0]?.quantity ?? 187,
            is_enabled: true
          };
          if (usesProcessingProfiles && globalReadyId != null) fallback.readiness_state_id = globalReadyId;
          offerings = [fallback];
        }

        const property_values = (p.property_values || [])
          .map(v => {
            // Keep a non-empty property_name for each spec (esp. custom prop_id 513).
            const name =
              (typeof v.property_name === "string" && v.property_name.trim()) ||
              (typeof v.property_name_formatted === "string" && v.property_name_formatted.trim()) ||
              undefined;

            const out = {
              property_id: v.property_id,
              property_name: name
            };
            if (v.scale_id != null) out.scale_id = v.scale_id;
            if (Array.isArray(v.value_ids)) out.value_ids = v.value_ids.filter(Boolean);
            if (Array.isArray(v.values))    out.values    = v.values.filter(Boolean);

            if (!out.property_name && Number(v.property_id) === 513) {
              out.property_name = "Custom";
            }
            return out;
          })
          .filter(v =>
            v.property_id &&
            v.property_name &&
            ((Array.isArray(v.value_ids) && v.value_ids.length > 0) ||
             (Array.isArray(v.values)    && v.values.length    > 0))
          );

        // Build product with the correct SKU policy
        const prod = { offerings, property_values };
        if (skuMode === "uniform") {
          prod.sku = uniformSku;
        } else if (skuMode === "vary") {
          // keep variant-specific sku as-is if present
          const keep = (p.sku || "").trim();
          if (keep) prod.sku = keep;
          // If missing SKU on some variants while varying, omit; Etsy allows differing/blank when sku_on_property declares variance
        } else if (skuMode === "none") {
          // omit sku for everyone (consistent "no sku")
        } else {
          // "keep" (should not occur, but safe fallback)
          const keep = (p.sku || "").trim();
          if (keep) prod.sku = keep;
        }
        return prod;
      });
    } else {
      // No inventory yet? Create a minimal single-product inventory with your SKU policy
      const baseOffering = { price: 1.0, quantity: 187, is_enabled: true };
      const minimal = { property_values: [], offerings: [baseOffering] };
      if (desiredSku) {
        minimal.sku = desiredSku;
      }
      products = [minimal];
      // Ensure consistency at top-level
      if (desiredSku) sku_on_property = [];
    }

    // Build payload with only non-empty *_on_property arrays
    const payload = { products };
    if (Array.isArray(price_on_property) && price_on_property.length) {
      payload.price_on_property = price_on_property;
    }
    if (Array.isArray(quantity_on_property) && quantity_on_property.length) {
      payload.quantity_on_property = quantity_on_property;
    }
    if (Array.isArray(sku_on_property) && sku_on_property.length) {
      payload.sku_on_property = sku_on_property;
    }
    if (Array.isArray(readiness_state_on_property) && readiness_state_on_property.length) {
      payload.readiness_state_on_property = readiness_state_on_property;
    }

    // 3) PUT updated inventory (this is where the SKU actually gets stored)
    const putRes = await fetch(invUrl, {
      method: "PUT",
      headers: { ...commonHeaders, "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    const text = await putRes.text();
    let data; try { data = JSON.parse(text); } catch { data = { raw: text }; }

    if (!putRes.ok) {
      const msg = data?.error || data?.message || data?.raw || "Error updating inventory";
      return { statusCode: putRes.status, body: JSON.stringify({ error: msg, details: data }) };
    }
    return { statusCode: 200, body: JSON.stringify({ ok: true, inventory: data }) };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};