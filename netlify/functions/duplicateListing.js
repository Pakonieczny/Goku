// netlify/functions/duplicateListing.js
const fetch = require("node-fetch");
const FormData = require("form-data");

const API_BASE = "https://openapi.etsy.com/v3/application";

function bearer(event) {
  const h = event.headers || {};
  const auth = h.authorization || h.Authorization || "";
  if (auth.startsWith("Bearer ")) return auth;
  // Fallback to env token if you use server-held tokens:
  if (process.env.ETSY_ACCESS_TOKEN) return "Bearer " + process.env.ETSY_ACCESS_TOKEN;
  throw new Error("Missing Authorization bearer token.");
}

async function jget(url, token) {
  const r = await fetch(url, { headers: { Authorization: token } });
  const t = await r.text();
  let data; try { data = t ? JSON.parse(t) : null; } catch { data = { raw: t }; }
  if (!r.ok) throw new Error(`[GET ${url}] ${r.status} — ${data?.error || data?.message || t}`);
  return data;
}
async function jpost(url, token, body) {
  const r = await fetch(url, {
    method: "POST",
    headers: { Authorization: token, "Content-Type": "application/json" },
    body: JSON.stringify(body || {})
  });
  const t = await r.text();
  let data; try { data = t ? JSON.parse(t) : null; } catch { data = { raw: t }; }
  if (!r.ok) throw new Error(`[POST ${url}] ${r.status} — ${data?.error || data?.message || t}`);
  return data;
}
async function jpatch(url, token, body) {
  const r = await fetch(url, {
    method: "PATCH",
    headers: { Authorization: token, "Content-Type": "application/json" },
    body: JSON.stringify(body || {})
  });
  const t = await r.text();
  let data; try { data = t ? JSON.parse(t) : null; } catch { data = { raw: t }; }
  if (!r.ok) throw new Error(`[PATCH ${url}] ${r.status} — ${data?.error || data?.message || t}`);
  return data;
}
async function jput(url, token, body) {
  const r = await fetch(url, {
    method: "PUT",
    headers: { Authorization: token, "Content-Type": "application/json" },
    body: JSON.stringify(body || {})
  });
  const t = await r.text();
  let data; try { data = t ? JSON.parse(t) : null; } catch { data = { raw: t }; }
  if (!r.ok) throw new Error(`[PUT ${url}] ${r.status} — ${data?.error || data?.message || t}`);
  return data;
}

async function uploadImageFromUrl(imageUrl, token, shop_id, listing_id, rank) {
  // Fetch binary and re-upload to the new listing
  const imgRes = await fetch(imageUrl);
  if (!imgRes.ok) throw new Error(`Download image failed: ${imageUrl} ${imgRes.status}`);
  const buf = Buffer.from(await imgRes.arrayBuffer());

  const fd = new FormData();
  fd.append("image", buf, { filename: `clone_${listing_id}_${rank}.jpg` });
  fd.append("rank", String(rank));

  const up = await fetch(`${API_BASE}/shops/${shop_id}/listings/${listing_id}/images`, {
    method: "POST",
    headers: { Authorization: token, ...fd.getHeaders() },
    body: fd
  });
  const txt = await up.text();
  let json; try { json = JSON.parse(txt); } catch { json = { raw: txt }; }
  if (!up.ok) throw new Error(`[UPLOAD image rank ${rank}] ${up.status} — ${json?.error || json?.message || txt}`);
  return json;
}

async function uploadDigitalFileFromUrl(fileUrl, token, shop_id, listing_id, nameHint) {
  const fRes = await fetch(fileUrl);
  if (!fRes.ok) throw new Error(`Download file failed: ${fileUrl} ${fRes.status}`);
  const buf = Buffer.from(await fRes.arrayBuffer());

  const fd = new FormData();
  fd.append("name", nameHint || "download");
  fd.append("file", buf, { filename: nameHint || "download.bin" });

  const up = await fetch(`${API_BASE}/shops/${shop_id}/listings/${listing_id}/files`, {
    method: "POST",
    headers: { Authorization: token, ...fd.getHeaders() },
    body: fd
  });
  const txt = await up.text();
  let json; try { json = JSON.parse(txt); } catch { json = { raw: txt }; }
  if (!up.ok) throw new Error(`[UPLOAD file ${nameHint || ""}] ${up.status} — ${json?.error || json?.message || txt}`);
  return json;
}

exports.handler = async (event) => {
  try {
    if (event.httpMethod !== "POST") {
      return { statusCode: 405, body: JSON.stringify({ error: "Method Not Allowed" }) };
    }
    const token = bearer(event);
    const body = JSON.parse(event.body || "{}");
    const sourceId = String(body.listing_id || body.listingId || "").trim();
    if (!/^\d{10}$/.test(sourceId)) {
      return { statusCode: 400, body: JSON.stringify({ error: "Missing or invalid listing_id" }) };
    }

    // 1) Read source listing core
    const src = await jget(`${API_BASE}/listings/${sourceId}`, token);
    const srcData = src || {};
    const shop_id = String(srcData.shop_id || srcData.data?.shop_id || srcData.results?.[0]?.shop_id || "").trim();
    if (!shop_id) throw new Error("Could not resolve shop_id from source listing.");

    // 1a) Read images (for re-upload)
    const img = await jget(`${API_BASE}/listings/${sourceId}/images`, token);
    const images = img?.results || img?.data || [];

    // 1b) Read inventory/variations
    const inv = await jget(`${API_BASE}/listings/${sourceId}/inventory`, token);
    const inventory = inv?.products || inv?.data?.products || [];

    // 1c) Read listing-level properties/attributes
    let props = [];
    try {
      const p = await jget(`${API_BASE}/listings/${sourceId}/properties`, token);
      props = p?.results || p?.data || [];
    } catch (e) {
      // Some shops have minimal attributes; proceed
      console.warn("properties read warning:", e.message);
    }

    // 1d) Read translations (best-effort)
    let translations = [];
    try {
      const tr = await jget(`${API_BASE}/listings/${sourceId}/translations`, token);
      translations = tr?.results || tr?.data || [];
    } catch (e) {
      console.warn("translations read warning:", e.message);
    }

    // 1e) Digital files (if any)
    let files = [];
    try {
      const f = await jget(`${API_BASE}/listings/${sourceId}/files`, token);
      files = f?.results || f?.data || [];
    } catch (e) {
      // Not digital or no files
    }

    // 2) Create a new DRAFT listing with core fields copied
    const core = {
      title: srcData.title || srcData.data?.title || "",
      description: srcData.description || srcData.data?.description || "",
      taxonomy_id: srcData.taxonomy_id || srcData.data?.taxonomy_id,
      who_made: srcData.who_made || srcData.data?.who_made,
      when_made: srcData.when_made || srcData.data?.when_made,
      is_supply: !!(srcData.is_supply ?? srcData.data?.is_supply),
      tags: srcData.tags || srcData.data?.tags || [],
      materials: srcData.materials || srcData.data?.materials || [],
      // profiles / sections
      shipping_profile_id: srcData.shipping_profile_id || srcData.data?.shipping_profile_id,
      return_policy_id: srcData.return_policy_id || srcData.data?.return_policy_id,
      shop_section_id: srcData.shop_section_id || srcData.data?.shop_section_id,
      // personalization
      is_personalizable: !!(srcData.is_personalizable ?? srcData.data?.is_personalizable),
      personalization_is_required: !!(srcData.personalization_is_required ?? srcData.data?.personalization_is_required),
      personalization_char_count_max: srcData.personalization_char_count_max || srcData.data?.personalization_char_count_max,
      personalization_instructions: srcData.personalization_instructions || srcData.data?.personalization_instructions,
      // keep it DRAFT
      state: "draft",
      should_auto_renew: !!(srcData.should_auto_renew ?? srcData.data?.should_auto_renew)
    };

    const created = await jpost(`${API_BASE}/shops/${shop_id}/listings`, token, core);
    const newListingId =
      created?.listing_id || created?.data?.listing_id || created?.results?.[0]?.listing_id;
    if (!newListingId) throw new Error("Draft creation succeeded but no new listing_id in response.");

    // 3) Re-upload images preserving order
    if (Array.isArray(images) && images.length) {
      for (let i = 0; i < images.length; i++) {
        const r = images[i]?.rank || i + 1;
        // Prefer the largest available URL; fall back to any url
        const url =
          images[i]?.url_fullxfull ||
          images[i]?.url_o_fullxfull ||
          images[i]?.url_570xN ||
          images[i]?.url_300x300 ||
          images[i]?.url;
        if (url) {
          await uploadImageFromUrl(url, bearer(event), shop_id, newListingId, r);
        }
      }
    }

    // 4) Restore inventory/variations (must strip readonly IDs)
    if (Array.isArray(inventory) && inventory.length) {
      const products = inventory.map(p => {
        const copy = JSON.parse(JSON.stringify(p || {}));
        delete copy.product_id;
        delete copy.offering_id;
        if (Array.isArray(copy.offerings)) {
          copy.offerings = copy.offerings.map(o => {
            const oo = { ...o };
            delete oo.offering_id;
            // normalize price (v3 accepts decimal string)
            if (oo.price && typeof oo.price === "object" && oo.price.amount) {
              // amount is in minor units; fall back to provided "amount" if "amount" not present
              const cents = oo.price.amount || oo.price.cents || 0;
              const cur = oo.price.currency_code || "USD";
              oo.price = { amount: cents, currency_code: cur };
            }
            return oo;
          });
        }
        return copy;
      });

      await jput(`${API_BASE}/listings/${newListingId}/inventory`, token, { products });
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
          await jput(`${API_BASE}/listings/${newListingId}/properties/${property_id}`, token, payload);
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
          await jput(`${API_BASE}/listings/${newListingId}/translations/${lang}`, token, payload);
        } catch (e) {
          console.warn(`translation ${lang} set warning:`, e.message);
        }
      }
    }

    // 7) Digital files (if any) — re-upload
    if (Array.isArray(files) && files.length) {
      for (const f of