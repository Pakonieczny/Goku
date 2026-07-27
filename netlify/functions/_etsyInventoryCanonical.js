// netlify/functions/_etsyInventoryCanonical.js
// Shared canonicalization, snapshot hashing, health scanning and
// dependency-map derivation for Etsy listing inventory.
//
// Used by BOTH etsyListingInventoryDetailProxy (read) and
// etsyUpdateListingInventoryProxy (write + read-back verification) so the
// snapshot hash the console stores on load is guaranteed to be computed by
// the exact same code that checks it before a write. Do not fork this logic.

const crypto = require("crypto");

/* ---------- Money / number normalization ---------- */

function toDecimalPrice(price) {
  // Etsy v3 GET returns Money objects ({amount, divisor}); PUT wants decimals.
  const m = Array.isArray(price) ? price[0] : price;
  if (m && typeof m === "object" && m.amount != null) {
    const amt = Number(m.amount);
    const div = Number(m.divisor || 100);
    if (!Number.isFinite(amt) || !div) return null;
    return Math.round((amt / div) * 100) / 100;
  }
  const n = typeof m === "string" ? Number(m) : m;
  return Number.isFinite(n) ? Math.round(n * 100) / 100 : null;
}

function toQuantity(offering) {
  const q = Number(offering?.quantity ?? offering?.available_quantity ?? 0);
  return Number.isInteger(q) && q >= 0 ? q : 0;
}

/* ---------- Canonical product form ---------- */

function firstOffering(p) {
  return (p?.offerings || [])[0] || {};
}

function comboKey(p) {
  return (p?.property_values || [])
    .map(v => Number(v.property_id) + ":" + (v.values || []).map(String).join("/"))
    .sort()
    .join("|");
}

// Reduce a raw Etsy product (from GET or from the console draft) to the
// fields that constitute buyer-visible pricing state. Ids assigned by Etsy
// (product_id, offering_id, value_ids) are intentionally excluded: Etsy
// regenerates them on every PUT, so including them would make read-back
// verification and staleness hashing impossible.
function canonicalProduct(p) {
  const o = firstOffering(p);
  const enabled = o.is_enabled !== false;
  return {
    key: comboKey(p),
    properties: (p.property_values || [])
      .map(v => ({
        property_id: Number(v.property_id),
        property_name: String(v.property_name || ""),
        values: (v.values || []).map(String)
      }))
      .sort((a, b) => a.property_id - b.property_id),
    sku: String(p.sku || "").trim(),
    price: toDecimalPrice(o.price),
    // A disabled offering's stock count is meaningless; Etsy's canonical
    // "unavailable" representation is quantity 0 + disabled, so normalize.
    quantity: enabled ? toQuantity(o) : 0,
    is_enabled: enabled
  };
}

function canonicalInventory(inv) {
  const products = (inv?.products || []).map(canonicalProduct)
    .sort((a, b) => a.key.localeCompare(b.key));
  const norm = arr => [...new Set((arr || []).map(Number))].sort((a, b) => a - b);
  return {
    products,
    price_on_property: norm(inv?.price_on_property),
    quantity_on_property: norm(inv?.quantity_on_property),
    sku_on_property: norm(inv?.sku_on_property)
  };
}

function snapshotHash(inv) {
  return crypto.createHash("sha256")
    .update(JSON.stringify(canonicalInventory(inv)))
    .digest("hex");
}

/* ---------- Pricing health (server-side, mirrors console rules) ---------- */

function pricingHealth(inv) {
  const issues = [];
  const canon = canonicalInventory(inv);
  const seen = new Set();
  const prices = [];
  let enabled = 0, disabled = 0, missingSku = 0;

  if (!canon.products.length) {
    issues.push({ severity: "error", message: "Listing has no inventory products." });
  }
  for (const p of canon.products) {
    if (seen.has(p.key)) {
      issues.push({ severity: "error", message: "Duplicate combination: " + (p.key || "base product") });
    }
    seen.add(p.key);
    if (!p.sku) missingSku++;
    if (p.is_enabled) enabled++; else disabled++;
    if (!Number.isFinite(p.price) || p.price <= 0) {
      issues.push({ severity: "error", message: "Invalid price on " + (p.key || "base product") });
    } else {
      prices.push(p.price);
    }
    if (!Number.isInteger(p.quantity) || p.quantity < 0) {
      issues.push({ severity: "error", message: "Invalid quantity on " + (p.key || "base product") });
    }
  }
  if (canon.products.length && enabled === 0) {
    issues.push({ severity: "error", message: "Every offering is disabled." });
  }
  if (disabled) {
    issues.push({ severity: "warning", message: disabled + " offering" + (disabled === 1 ? " is" : "s are") + " disabled." });
  }
  if (missingSku) {
    issues.push({ severity: "info", message: missingSku + " combination" + (missingSku === 1 ? " has" : "s have") + " no SKU." });
  }
  return {
    issues,
    error_count: issues.filter(x => x.severity === "error").length,
    warning_count: issues.filter(x => x.severity === "warning").length,
    enabled_count: enabled,
    disabled_count: disabled,
    product_count: canon.products.length,
    min_price: prices.length ? Math.min(...prices) : null,
    max_price: prices.length ? Math.max(...prices) : null
  };
}

/* ---------- Dependency-map derivation (*_on_property) ---------- */

// Etsy rejects a PUT when a field varies across a property that is not
// declared in the matching *_on_property array. Deriving the arrays from the
// matrix itself guarantees the declaration always matches reality.
// Property IDs in the order they appear inside the products themselves.
// Etsy requires *_on_property arrays to follow this exact order.
function productPropertyOrder(inv) {
  const order = [];
  for (const p of (inv?.products || [])) {
    for (const v of (p.property_values || [])) {
      const id = Number(v.property_id);
      if (!order.includes(id)) order.push(id);
    }
  }
  return order;
}

function deriveOnProperty(inv) {
  const canon = canonicalInventory(inv);
  const propIds = productPropertyOrder(inv);

  const fieldValue = {
    price: p => (Number.isFinite(p.price) ? p.price.toFixed(2) : "?"),
    quantity: p => String(p.quantity),
    sku: p => p.sku
  };

  const out = { price: [], quantity: [], sku: [] };
  for (const id of propIds) {
    const groups = new Map();
    for (const p of canon.products) {
      const key = p.properties
        .filter(v => v.property_id !== id)
        .map(v => v.property_id + ":" + v.values.join("/"))
        .join("|");
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(p);
    }
    for (const field of ["price", "quantity", "sku"]) {
      if (out[field].includes(id)) continue;
      for (const rows of groups.values()) {
        const vals = new Set(rows.map(fieldValue[field]));
        if (vals.size > 1) { out[field].push(id); break; }
      }
    }
  }
  // Partial-matrix post-check: group-wise derivation can miss variation when
  // combinations don't overlap (e.g. charm-only rows exist solely with the
  // no-chain tier). Etsy's hard rule is that a field must be constant across
  // all products sharing the same values on its declared properties. Verify
  // that; if any group still varies, the declaration is provably
  // insufficient — escalate that field to all property ids (always valid).
  for (const field of ["price", "quantity", "sku"]) {
    const declared = out[field];
    const groups = new Map();
    let conflict = false;
    for (const p of canon.products) {
      const gkey = p.properties
        .filter(v => declared.includes(v.property_id))
        .map(v => v.property_id + ":" + v.values.join("/"))
        .join("|");
      const val = fieldValue[field](p);
      if (groups.has(gkey) && groups.get(gkey) !== val) { conflict = true; break; }
      groups.set(gkey, val);
    }
    if (conflict) out[field] = [...propIds];
  }
  return {
    price_on_property: out.price,
    quantity_on_property: out.quantity,
    sku_on_property: out.sku
  };
}

/* ---------- Read-back verification ---------- */

// Compares the inventory the operator intended to write against what Etsy
// now serves. Returns { verified, differences: [exact human-readable diffs] }.
function verifyAgainst(intendedInv, freshInv) {
  const want = canonicalInventory(intendedInv);
  const got = canonicalInventory(freshInv);
  const differences = [];
  const detail = []; // structured, for callers that need to react programmatically (e.g. auto-retry logic)

  const gotMap = new Map(got.products.map(p => [p.key, p]));
  const wantMap = new Map(want.products.map(p => [p.key, p]));

  for (const p of want.products) {
    const g = gotMap.get(p.key);
    if (!g) { differences.push("Missing on Etsy after update: " + (p.key || "base product")); detail.push({ key: p.key, field: "missing" }); continue; }
    if (g.price !== p.price) { differences.push((p.key || "base product") + " price is " + g.price + ", expected " + p.price); detail.push({ key: p.key, field: "price" }); }
    if (g.quantity !== p.quantity) { differences.push((p.key || "base product") + " quantity is " + g.quantity + ", expected " + p.quantity); detail.push({ key: p.key, field: "quantity" }); }
    if (g.sku !== p.sku) { differences.push((p.key || "base product") + " SKU is \"" + g.sku + "\", expected \"" + p.sku + "\""); detail.push({ key: p.key, field: "sku" }); }
    if (g.is_enabled !== p.is_enabled) { differences.push((p.key || "base product") + " enabled state is " + g.is_enabled + ", expected " + p.is_enabled); detail.push({ key: p.key, field: "is_enabled", expected: p.is_enabled }); }
  }
  for (const g of got.products) {
    if (!wantMap.has(g.key)) { differences.push("Unexpected combination on Etsy after update: " + (g.key || "base product")); detail.push({ key: g.key, field: "unexpected" }); }
  }
  return { verified: differences.length === 0, differences, detail };
}

module.exports = {
  productPropertyOrder,
  toDecimalPrice,
  canonicalInventory,
  snapshotHash,
  pricingHealth,
  deriveOnProperty,
  verifyAgainst,
  comboKey
};
