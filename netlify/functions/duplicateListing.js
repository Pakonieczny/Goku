// netlify/functions/duplicateListing.js
const fetch = require("node-fetch");
const FormData = require("form-data");

const API_BASE = "https://openapi.etsy.com/v3/application";

function bearer(event) {
  const h = event.headers || {};
  const auth = h.authorization || h.Authorization || "";
  if (auth.startsWith("Bearer ")) return auth;
  if (process.env.ETSY_ACCESS_TOKEN) return "Bearer " + process.env.ETSY_ACCESS_TOKEN;
  throw new Error("Missing Authorization bearer token.");
}
function clientId(event) {
  const h = event.headers || {};
  return (
    h["x-api-key"] || h["X-Api-Key"] ||
    h["client-id"] || h["Client-Id"] ||
    process.env.CLIENT_ID || process.env.ETSY_CLIENT_ID || ""
  );
}
function baseHeaders(token, xApiKey, extra) {
  const h = { Authorization: token, "x-api-key": xApiKey, Accept: "application/json" };
  return Object.assign(h, extra || {});
}

// ---------- helpers ----------
function deriveQuantity(srcData, inventory) {
  const q1 = (srcData && (srcData.quantity ?? srcData.data?.quantity));
  if (Number.isInteger(q1) && q1 > 0) return q1;
  try {
    if (Array.isArray(inventory)) {
      let maxQ = 0;
      for (const p of inventory) {
        const offs = p?.offerings || [];
        for (const o of offs) {
          const q = o?.quantity ?? o?.available_quantity ?? 0;
          if (Number.isInteger(q) && q > maxQ) maxQ = q;
        }
      }
      if (maxQ > 0) return maxQ;
    }
  } catch {}
  return 187;
}

// cents for createDraftListing's price
function derivePrice(srcData, inventory) {
  const toCents = (v) => {
    if (v == null) return null;
    if (typeof v === "object") {
      const a = v.amount ?? v.cents ?? v.value;
      return Number.isFinite(a) ? a : null;
    }
    const n = Number(v);
    if (!Number.isFinite(n)) return null;
    return String(v).includes(".") ? Math.round(n * 100) : Math.round(n);
  };
  const p1 = toCents(srcData && (srcData.price ?? srcData.data?.price));
  if (p1 && p1 > 0) return p1;

  let min = Infinity;
  try {
    if (Array.isArray(inventory)) {
      for (const p of inventory) {
        for (const o of (p?.offerings || [])) {
          const c = toCents(o?.price);
          if (c && c > 0 && c < min) min = c;
        }
      }
    }
  } catch {}
  if (min !== Infinity) return min;
  return 1000; // $10.00 default
}

// convert Money object -> decimal for updateListingInventory
function toDecimalPrice(price) {
  if (price == null) return undefined;
  if (typeof price === "number") return price;
  if (typeof price === "string") {
    const n = Number(price);
    return Number.isFinite(n) ? n : undefined;
  }
  if (typeof price === "object") {
    const amount = price.amount ?? price.cents ?? price.value;
    const divisor = price.divisor ?? 100;
    if (Number.isFinite(amount) && Number.isFinite(divisor) && divisor > 0) {
      // keep exact to 2 decimals
      return Math.round((amount / divisor) * 100) / 100;
    }
  }
  return undefined;
}

async function jget(url, token, xApiKey) {
  const r = await fetch(url, { headers: baseHeaders(token, xApiKey) });
  const t = await r.text();
  let data; try { data = t ? JSON.parse(t) : null; } catch { data = { raw: t }; }
  if (!r.ok) throw new Error(`[GET ${url}] ${r.status} — ${data?.error || data?.message || t}`);
  return data;
}
async function jpost(url, token, xApiKey, body) {
  const r = await fetch(url, {
    method: "POST",
    headers: baseHeaders(token, xApiKey, { "Content-Type": "application/json" }),
    body: JSON.stringify(body || {})
  });
  const t = await r.text();
  let data; try { data = t ? JSON.parse(t) : null; } catch { data = { raw: t }; }
  if (!r.ok) throw new Error(`[POST ${url}] ${r.status} — ${data?.error || data?.message || t}`);
  return data;
}
async function jpatch(url, token, xApiKey, body) {
  const r = await fetch(url, {
    method: "PATCH",
    headers: baseHeaders(token, xApiKey, { "Content-Type": "application/json" }),
    body: JSON.stringify(body || {})
  });
  const t = await r.text();
  let data; try { data = t ? JSON.parse(t) : null; } catch { data = { raw: t }; }
  if (!r.ok) throw new Error(`[PATCH ${url}] ${r.status} — ${data?.error || data?.message || t}`);
  return data;
}
async function jput(url, token, xApiKey, body) {
  const r = await fetch(url, {
    method: "PUT",
    headers: baseHeaders(token, xApiKey, { "Content-Type": "application/json" }),
    body: JSON.stringify(body || {})
  });
  const t = await r.text();
  let data; try { data = t ? JSON.parse(t) : null; } catch { data = { raw: t }; }
  if (!r.ok) throw new Error(`[PUT ${url}] ${r.status} — ${data?.error || data?.message || t}`);
  return data;
}

// some Etsy endpoints still prefer x-www-form-urlencoded (e.g., creating readiness profile)
async function formPost(url, token, xApiKey, data) {
  const enc = new URLSearchParams();
  Object.entries(data || {}).forEach(([k, v]) => {
    if (v !== undefined && v !== null) enc.append(k, String(v));
  });
  const r = await fetch(url, {
    method: "POST",
    headers: baseHeaders(token, xApiKey, { "Content-Type": "application/x-www-form-urlencoded" }),
    body: enc.toString()
  });
  const t = await r.text();
  let json; try { json = t ? JSON.parse(t) : null; } catch { json = { raw: t }; }
  if (!r.ok) throw new Error(`[POST ${url}] ${r.status} — ${json?.error || json?.message || t}`);
  return json;
}

async function uploadImageFromUrl(imageUrl, token, xApiKey, shop_id, listing_id, rank) {
  const imgRes = await fetch(imageUrl);
  if (!imgRes.ok) throw new Error(`Download image failed: ${imageUrl} ${imgRes.status}`);
  const buf = Buffer.from(await imgRes.arrayBuffer());

  const fd = new FormData();
  fd.append("image", buf, { filename: `clone_${listing_id}_${rank}.jpg` });
  fd.append("rank", String(rank));

  const up = await fetch(`${API_BASE}/shops/${shop_id}/listings/${listing_id}/images`, {
    method: "POST",
    headers: { ...baseHeaders(token, xApiKey), ...fd.getHeaders() },
    body: fd
  });
  const txt = await up.text();
  let json; try { json = JSON.parse(txt); } catch { json = { raw: txt }; }
  if (!up.ok) throw new Error(`[UPLOAD image rank ${rank}] ${up.status} — ${json?.error || json?.message || txt}`);
  return json;
}

async function uploadDigitalFileFromUrl(fileUrl, token, xApiKey, shop_id, listing_id, nameHint) {
  const fRes = await fetch(fileUrl);
  if (!fRes.ok) throw new Error(`Download file failed: ${fileUrl} ${fRes.status}`);
  const buf = Buffer.from(await fRes.arrayBuffer());

  const fd = new FormData();
  fd.append("name", nameHint || "download");
  fd.append("file", buf, { filename: nameHint || "download.bin" });

  const up = await fetch(`${API_BASE}/shops/${shop_id}/listings/${listing_id}/files`, {
    method: "POST",
    headers: { ...baseHeaders(token, xApiKey), ...fd.getHeaders() },
    body: fd
  });
  const txt = await up.text();
  let json; try { json = JSON.parse(txt); } catch { json = { raw: txt }; }
  if (!up.ok) throw new Error(`[UPLOAD file ${nameHint || ""}] ${up.status} — ${json?.error || json?.message || txt}`);
  return json;
}

function isDigitalListing(srcData, filesList) {
  const t = (srcData.type || srcData.listing_type || srcData.data?.type || "").toString().toLowerCase();
  const digitalFlag = !!(srcData.is_digital ?? srcData.data?.is_digital);
  if (digitalFlag) return true;
  if (t === "download" || t === "digital") return true;
  return Array.isArray(filesList) && filesList.length > 0;
}

// Resolve a readiness_state_id to satisfy physical listing creation.
// Priority: copy from source inventory (majority id) -> pick an existing shop definition -> (last resort) create a default definition.
async function resolveReadinessStateId({ token, xApiKey, shop_id, sourceId }) {
  // 1) Try to read from source inventory with legacy=false
  try {
    const inv = await jget(`${API_BASE}/listings/${sourceId}/inventory?legacy=false`, token, xApiKey);
    const prods = inv?.products || inv?.data?.products || [];
    const counts = new Map();
    for (const p of prods) {
      for (const o of (p?.offerings || [])) {
        const rid = o?.readiness_state_id;
        if (rid) counts.set(rid, (counts.get(rid) || 0) + 1);
      }
    }
    if (counts.size) {
      return [...counts.entries()].sort((a, b) => b[1] - a[1])[0][0];
    }
  } catch (e) {
    console.warn("readiness from inventory warning:", e.message);
  }

  // 2) Try any existing processing profile in the shop
  try {
    const defs = await jget(`${API_BASE}/shops/${shop_id}/readiness-state-definitions`, token, xApiKey);
    const list = defs?.results || defs?.data || [];
    if (Array.isArray(list) && list.length) {
      const preferred =
        list.find(d => (d.readiness_state || d.data?.readiness_state) === "ready_to_ship") || list[0];
      return preferred.readiness_state_id || preferred.data?.readiness_state_id;
    }
  } catch (e) {
    console.warn("readiness definitions read warning:", e.message);
  }

  // 3) As a last resort, create a minimal profile
  try {
    const created = await formPost(
      `${API_BASE}/shops/${shop_id}/readiness-state-definitions`,
      token,
      xApiKey,
      {
        readiness_state: "ready_to_ship",
        min_processing_time: 1,
        max_processing_time: 3,
        processing_time_unit: "days"
      }
    );
    return created?.readiness_state_id || created?.data?.readiness_state_id;
  } catch (e) {
    console.warn("create readiness-state-definition warning:", e.message);
  }

  return null;
}

exports.handler = async (event) => {
  try {
    if (event.httpMethod !== "POST") {
      return { statusCode: 405, body: JSON.stringify({ error: "Method Not Allowed" }) };
    }
    const token = bearer(event);
    const xApiKey = clientId(event);
    if (!xApiKey) {
      return { statusCode: 400, body: JSON.stringify({ error: "Missing x-api-key (CLIENT_ID). Add header X-Api-Key or set env CLIENT_ID." }) };
    }

    const body = JSON.parse(event.body || "{}");
    const sourceId = String(body.listing_id || body.listingId || "").trim();
    if (!/^\d{10}$/.test(sourceId)) {
      return { statusCode: 400, body: JSON.stringify({ error: "Missing or invalid listing_id" }) };
    }

    // 1) Read source listing core (use legacy=false so readiness fields appear where applicable)
    const src = await jget(`${API_BASE}/listings/${sourceId}?legacy=false`, token, xApiKey);
    const srcData = src || {};
    const shop_id = String(srcData.shop_id || srcData.data?.shop_id || srcData.results?.[0]?.shop_id || "").trim();
    if (!shop_id) throw new Error("Could not resolve shop_id from source listing.");

    // 1a) Images
    const img = await jget(`${API_BASE}/listings/${sourceId}/images`, token, xApiKey);
    const images = img?.results || img?.data || [];

    // 1b) Inventory/variations (use legacy=false for readiness ids)
    const inv = await jget(`${API_BASE}/listings/${sourceId}/inventory?legacy=false`, token, xApiKey);
    const inventory = inv?.products || inv?.data?.products || [];

    // 1c) Properties
    let props = [];
    try {
      const p = await jget(`${API_BASE}/listings/${sourceId}/properties`, token, xApiKey);
      props = p?.results || p?.data || [];
    } catch (e) {
      console.warn("properties read warning:", e.message);
    }

    // 1d) Translations
    let translations = [];
    try {
      const tr = await jget(`${API_BASE}/listings/${sourceId}/translations`, token, xApiKey);
      translations = tr?.results || tr?.data || [];
    } catch (e) {
      console.warn("translations read warning:", e.message);
    }

    // 1e) Digital files (if any)
    let files = [];
    try {
      const f = await jget(`${API_BASE}/listings/${sourceId}/files`, token, xApiKey);
      files = f?.results || f?.data || [];
    } catch (e) { /* not digital or none */ }

    const isDigital = isDigitalListing(srcData, files);

    // Resolve readiness_state_id for physical copies
    let readinessId = null;
    if (!isDigital) {
      readinessId = await resolveReadinessStateId({ token, xApiKey, shop_id, sourceId });
      if (!readinessId) {
        throw new Error("No readiness_state_id available for physical listing creation.");
      }
    }

    // 2) Create a new DRAFT listing (use legacy=false and include readiness_state_id for physical)
    const qty = deriveQuantity(srcData, inventory);
    const core = {
      title: srcData.title || srcData.data?.title || "",
      description: srcData.description || srcData.data?.description || "",
      taxonomy_id: srcData.taxonomy_id || srcData.data?.taxonomy_id,
      who_made: srcData.who_made || srcData.data?.who_made,
      when_made: srcData.when_made || srcData.data?.when_made,
      is_supply: !!(srcData.is_supply ?? srcData.data?.is_supply),
      tags: srcData.tags || srcData.data?.tags || [],
      materials: srcData.materials || srcData.data?.materials || [],
      quantity: qty,
      price: derivePrice(srcData, inventory), // cents (integer) per createDraftListing
      shipping_profile_id: srcData.shipping_profile_id || srcData.data?.shipping_profile_id,
      return_policy_id: srcData.return_policy_id || srcData.data?.return_policy_id,
      shop_section_id: srcData.shop_section_id || srcData.data?.shop_section_id,
      is_personalizable: !!(srcData.is_personalizable ?? srcData.data?.is_personalizable),
      personalization_is_required: !!(srcData.personalization_is_required ?? srcData.data?.personalization_is_required),
      personalization_char_count_max: srcData.personalization_char_count_max || srcData.data?.personalization_char_count_max,
      personalization_instructions: srcData.personalization_instructions || srcData.data?.personalization_instructions,
      state: "draft",
      should_auto_renew: !!(srcData.should_auto_renew ?? srcData.data?.should_auto_renew)
    };
    if (!isDigital && readinessId) core.readiness_state_id = readinessId;

    const created = await jpost(`${API_BASE}/shops/${shop_id}/listings?legacy=false`, token, xApiKey, core);
    const newListingId = created?.listing_id || created?.data?.listing_id || created?.results?.[0]?.listing_id;
    if (!newListingId) throw new Error("Draft creation succeeded but no new listing_id in response.");

    // 3) Re-upload images preserving order
    if (Array.isArray(images) && images.length) {
      for (let i = 0; i < images.length; i++) {
        const r = images[i]?.rank || i + 1;
        const url =
          images[i]?.url_fullxfull ||
          images[i]?.url_o_fullxfull ||
          images[i]?.url_570xN ||
          images[i]?.url_300x300 ||
          images[i]?.url;
        if (url) await uploadImageFromUrl(url, token, xApiKey, shop_id, newListingId, r);
      }
    }

    // 4) Restore inventory/variations — strip readonly, normalize prices to decimals, carry readiness_state_id
    if (Array.isArray(inventory) && inventory.length) {
      const products = inventory.map(p => {
        const copy = JSON.parse(JSON.stringify(p || {}));

        // remove read-only/noisy fields per docs
        delete copy.product_id;
        delete copy.offering_id;
        delete copy.scale_name;
        delete copy.is_deleted;
        delete copy.value_pairs;

        if (Array.isArray(copy.offerings)) {
          copy.offerings = copy.offerings.map(o => {
            const oo = { ...o };
            delete oo.offering_id;

            // normalize quantity/enable flags
            if (oo.available_quantity != null && oo.quantity == null) {
              oo.quantity = oo.available_quantity;
              delete oo.available_quantity;
            }

            // price must be decimal for inventory updates
            const dec = toDecimalPrice(oo.price);
            if (Number.isFinite(dec)) oo.price = dec; else delete oo.price;

            // ensure readiness_state_id present (copy exact if provided; else fall back to listing-level)
            if (!isDigital) {
              const rid = o?.readiness_state_id || readinessId;
              if (rid) oo.readiness_state_id = rid;
            }
            return oo;
          });
        }
        return copy;
      });

      await jput(`${API_BASE}/listings/${newListingId}/inventory?legacy=false`, token, xApiKey, { products });
    }

    // 5) Restore listing-level properties/attributes (best-effort)
    if (Array.isArray(props) && props.length) {
      for (const pr of props) {
        const property_id = pr.property_id || pr.data?.property_id;
        if (!property_id) continue;
        const payload = {
          value_ids: pr.value_ids || pr.data?.value_ids || [],
          values: pr.values || pr.data?.values || [],
          scale_id: pr.scale_id || pr.data?.scale_id || null
        };
        try {
          await jput(`${API_BASE}/listings/${newListingId}/properties/${property_id}`, token, xApiKey, payload);
        } catch (e) {
          console.warn(`property ${property_id} set warning:`, e.message);
        }
      }
    }

    // 6) Restore translations (best-effort)
    if (Array.isArray(translations) && translations.length) {
      for (const tr of translations) {
        const lang = tr.language || tr.data?.language;
        if (!lang) continue;
        const payload = {
          title: tr.title || tr.data?.title || core.title,
          description: tr.description || tr.data?.description || core.description,
          tags: tr.tags || tr.data?.tags || core.tags
        };
        try {
          await jput(`${API_BASE}/listings/${newListingId}/translations/${lang}`, token, xApiKey, payload);
        } catch (e) {
          console.warn(`translation ${lang} set warning:`, e.message);
        }
      }
    }

    // 7) Digital files (if any) — re-upload
    if (Array.isArray(files) && files.length) {
      for (const f of files) {
        const fileUrl = f.url || f.file_url || f.download_url;
        const nameHint = f.name || f.file_name || "download.bin";
        if (fileUrl) await uploadDigitalFileFromUrl(fileUrl, token, xApiKey, shop_id, newListingId, nameHint);
      }
    }

    // ✅ success
    return {
      statusCode: 200,
      body: JSON.stringify({
        ok: true,
        new_listing_id: newListingId,
        copied: {
          images: Array.isArray(images) ? images.length : 0,
          products: Array.isArray(inventory) ? inventory.length : 0,
          properties: Array.isArray(props) ? props.length : 0,
          translations: Array.isArray(translations) ? translations.length : 0,
          files: Array.isArray(files) ? files.length : 0
        }
      })
    };
  } catch (e) {
    console.error("duplicateListing failed:", e);
    return { statusCode: 500, body: JSON.stringify({ error: e.message }) };
  }
};