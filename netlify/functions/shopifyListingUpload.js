// netlify/functions/shopifyListingUpload.js
// ---------------------------------------------------------------------------
// SHOPIFY LISTING UPLOAD PIPELINE for the Etsy Listing Generator app.
//
// The Index.html generator produces a finished listing (title, description,
// tags, photos with alt text, category). This function turns that into a LIVE
// Shopify product:
//
//   1. Builds the full variant matrix + prices from the canonical Brites
//      pricing scheme (Pricing_Scheme_v2) — tier randomly assigned per
//      product, category-specific option structure, catalog-canonical option
//      names ("Metal Choice") and metal value strings.
//   2. Creates the product atomically with productSet (options + variants +
//      prices + SKUs in one mutation; made-to-order: untracked inventory,
//      policy CONTINUE).
//   3. Attaches all photos via productCreateMedia (checks mediaUserErrors),
//      alt text from the generator's per-photo metadata.
//   4. Publishes to the Online Store publication → instantly live.
//   5. Adds the product to matching MANUAL collections (theme keywords vs
//      collection titles). Smart collections pick the product up on their own
//      via productType/tags/title.
//
// DAILY VARIANT BUDGET (critical): Shopify caps stores with 50K+ variants at
// 1,000 NEW VARIANTS PER DAY. Every request is therefore queued in Firestore
// first, and a budget-aware drain uploads jobs until the daily allowance is
// spent; the remainder uploads automatically on subsequent days' drains.
//
// HTTP API (called by Index.html):
//   POST {op:"enqueue", payload:{...}}   -> queue job + immediate drain pass
//   GET  ?op=drain                       -> drain queue within today's budget
//   GET  ?op=status                      -> queue + budget snapshot
//   POST {op:"retryFailed"}              -> requeue all FAILED jobs
//
// SCHEDULING: every enqueue self-drains, the app pings ?op=drain on load and
// every 30 min while open, and the day rolls over automatically (dateKey in
// America/Toronto). For fully unattended multi-day drains, point any external
// cron (e.g. cron-job.org) at ?op=drain hourly.
//
// ENV (this Netlify site): SHOPIFY_STORE=brites-jewelry.myshopify.com,
// SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET, SHOPIFY_API_VERSION? (2025-10)
// plus the FIREBASE_* vars already configured for firebaseAdmin.js.
// ---------------------------------------------------------------------------
"use strict";

const admin = require("./firebaseAdmin");
const db = admin.firestore();

const API_VERSION = process.env.SHOPIFY_API_VERSION || "2025-10";
const DAILY_VARIANT_LIMIT = 1000;   // Shopify hard limit (50K+ variant stores)
const DAILY_SAFETY_CEILING = 950;   // leave headroom for manual admin work
const DRAIN_TIME_BUDGET_MS = 18000; // stay under Netlify's 26s cap
const QCOL = "Shopify_Upload_Queue";
const META = "Shopify_Upload_Meta";
const VENDOR = "Brites Jewelry";

/* ------------------------------ Shopify auth ------------------------------ */

let _token = null, _tokenExp = 0;
async function getToken() {
  if (_token && Date.now() < _tokenExp - 60000) return _token;
  const store = process.env.SHOPIFY_STORE;
  const res = await fetch(`https://${store}/admin/oauth/access_token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "client_credentials",
      client_id: process.env.SHOPIFY_CLIENT_ID,
      client_secret: process.env.SHOPIFY_CLIENT_SECRET
    })
  });
  const text = await res.text();
  if (!res.ok) throw new Error("Token request failed (" + res.status + "): " + text);
  const data = JSON.parse(text);
  _token = data.access_token;
  _tokenExp = Date.now() + (Number(data.expires_in || 3600) * 1000);
  return _token;
}

async function gql(query, variables, _attempt) {
  const store = process.env.SHOPIFY_STORE;
  const token = await getToken();
  const res = await fetch(`https://${store}/admin/api/${API_VERSION}/graphql.json`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Shopify-Access-Token": token },
    body: JSON.stringify({ query, variables: variables || {} })
  });
  const body = await res.json().catch(() => ({}));
  if (res.status === 429 || (body.errors || []).some(e => (e.extensions || {}).code === "THROTTLED")) {
    if ((_attempt || 0) < 4) { await new Promise(r => setTimeout(r, 1500 * ((_attempt || 0) + 1))); return gql(query, variables, (_attempt || 0) + 1); }
  }
  if (body.errors && body.errors.length) throw new Error("GraphQL: " + JSON.stringify(body.errors).slice(0, 500));
  return body.data;
}

/* ------------------------- PRICING (Pricing_Scheme_v2) ------------------------- */
// Canonical metal strings — MUST match the catalog exactly.
const M = {
  SS: "Sterling Silver",
  GF: "14k Gold Filled",
  RG: "14k Rose Gold Filled",
  SG: "14k Solid Gold"
};
// Option names: "Metal Choice" is the standardized catalog option name.
const OPT = { metal: "Metal Choice", engrave: "Engraving", length: "Chain Length", size: "Hoop Size", ctype: "Charm Type" };

// Tables indexed [tier-1]. Verbatim from Pricing_Scheme_v2.
const P = {
  stud: { // Material -> [T1,T2,T3]
    [M.SS]: [45, 48, 51], [M.GF]: [49, 52, 55], [M.RG]: [52, 55, 58], [M.SG]: [201, 216, 231]
  },
  huggie: { // no Rose Gold; 14K only 11mm
    [M.SS]: [51, 54, 57], [M.GF]: [56, 59, 62], [M.SG]: [307, 330, 353]
  },
  beady: { // [material][engrave? "E":"N"][length] -> [T1,T2,T3]; no Rose Gold; lengths 14/16/18
    [M.SS]: { N: { 14: [68, 72, 76], 16: [74, 78, 82], 18: [76, 80, 84] },
              E: { 14: [78, 82, 86], 16: [83, 88, 93], 18: [85, 90, 95] } },
    [M.GF]: { N: { 14: [76, 80, 84], 16: [82, 87, 92], 18: [84, 89, 94] },
              E: { 14: [85, 90, 95], 16: [92, 97, 102], 18: [93, 99, 105] } },
    [M.SG]: { N: { 14: [392, 422, 452], 16: [435, 468, 501], 18: [448, 482, 516] },
              E: { 14: [411, 442, 473], 16: [454, 488, 522], 18: [467, 502, 537] } }
  },
  regular: { // [material][engrave] -> [T1,T2,T3]
    [M.SS]: { N: [54, 57, 60], E: [64, 67, 70] },
    [M.GF]: { N: [59, 62, 65], E: [68, 72, 76] },
    [M.RG]: { N: [62, 65, 68], E: [71, 75, 79] },
    [M.SG]: { N: [254, 262, 280], E: [273, 282, 301] }
  },
  charm: { // [material][charmType] -> [T1,T2,T3]
    [M.SS]: { "Necklace Charm": [35, 37, 39], "Charm + Engrave": [44, 47, 50], "Huggie Charm": [37, 39, 41] },
    [M.GF]: { "Necklace Charm": [40, 42, 44], "Charm + Engrave": [49, 52, 55], "Huggie Charm": [43, 45, 47] },
    [M.RG]: { "Necklace Charm": [40, 42, 44], "Charm + Engrave": [49, 52, 55], "Huggie Charm": [43, 45, 47] },
    [M.SG]: { "Necklace Charm": [154, 166, 178], "Charm + Engrave": [170, 182, 195], "Huggie Charm": [145, 156, 167] }
  }
};

const MSFX = { [M.SS]: "SS", [M.GF]: "GF", [M.RG]: "RG", [M.SG]: "SG" };

// Category (Listing-Generator folder prefix) -> builder + productType + type tag.
// Bracelets have no dedicated table in the scheme; documented fallback is the
// Regular Necklace table (same structure: Metal x Engraving).
function buildMatrix(category, tier, baseSku) {
  const t = tier - 1;
  const sku = (s) => (baseSku ? `${baseSku}-${s}` : undefined);
  const cat = String(category || "").toLowerCase();

  if (cat.startsWith("stud")) {
    return {
      productType: "Earrings", typeTag: "be-ptype-earrings",
      optionOrder: [OPT.metal],
      variants: [M.SS, M.GF, M.RG, M.SG].map(m => ({
        options: { [OPT.metal]: m }, price: P.stud[m][t], sku: sku(MSFX[m])
      }))
    };
  }
  if (cat.startsWith("hoop") || cat.startsWith("huggie")) {
    const v = [];
    for (const m of [M.SS, M.GF, M.SG]) {
      for (const size of ["8.5mm", "11mm"]) {
        if (m === M.SG && size === "8.5mm") continue; // invalid combo per scheme
        v.push({ options: { [OPT.metal]: m, [OPT.size]: size }, price: P.huggie[m][t], sku: sku(`${MSFX[m]}-${size === "8.5mm" ? "85" : "11"}`) });
      }
    }
    return { productType: "Earrings", typeTag: "be-ptype-earrings", optionOrder: [OPT.metal, OPT.size], variants: v };
  }
  if (cat.startsWith("beady")) {
    const v = [];
    for (const m of [M.SS, M.GF, M.SG]) {
      for (const e of ["No", "Yes"]) {
        for (const len of [14, 16, 18]) {
          v.push({
            options: { [OPT.metal]: m, [OPT.engrave]: e, [OPT.length]: `${len}"` },
            price: P.beady[m][e === "Yes" ? "E" : "N"][len][t],
            sku: sku(`${MSFX[m]}-${e === "Yes" ? "E" : "N"}-${len}`)
          });
        }
      }
    }
    return { productType: "Necklace", typeTag: "be-ptype-necklace", optionOrder: [OPT.metal, OPT.engrave, OPT.length], variants: v };
  }
  if (cat.startsWith("charm")) {
    const v = [];
    for (const m of [M.SS, M.GF, M.RG, M.SG]) {
      for (const ct of ["Necklace Charm", "Charm + Engrave", "Huggie Charm"]) {
        v.push({ options: { [OPT.metal]: m, [OPT.ctype]: ct }, price: P.charm[m][ct][t], sku: sku(`${MSFX[m]}-${ct === "Necklace Charm" ? "NC" : ct === "Charm + Engrave" ? "CE" : "HC"}`) });
      }
    }
    return { productType: "Charm", typeTag: "be-ptype-charm", optionOrder: [OPT.metal, OPT.ctype], variants: v };
  }
  // regular necklace + bracelets (fallback table)
  const isBracelet = cat.startsWith("bracelet");
  const v = [];
  for (const m of [M.SS, M.GF, M.RG, M.SG]) {
    for (const e of ["No", "Yes"]) {
      v.push({ options: { [OPT.metal]: m, [OPT.engrave]: e }, price: P.regular[m][e === "Yes" ? "E" : "N"][t], sku: sku(`${MSFX[m]}-${e === "Yes" ? "E" : "N"}`) });
    }
  }
  return {
    productType: isBracelet ? "Bracelet" : "Necklace",
    typeTag: isBracelet ? "be-ptype-bracelet" : "be-ptype-necklace",
    optionOrder: [OPT.metal, OPT.engrave], variants: v
  };
}

/* ------------------------------ helpers ------------------------------ */

function torontoDateKey() {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "America/Toronto", year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date());
}

// Reserve `n` variants from today's budget atomically. Returns true if granted.
async function reserveBudget(n) {
  const ref = db.collection(META).doc("budget");
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const key = torontoDateKey();
    let used = 0;
    if (snap.exists && snap.data().dateKey === key) used = snap.data().used || 0;
    if (used + n > DAILY_SAFETY_CEILING) return false;
    tx.set(ref, { dateKey: key, used: used + n, limit: DAILY_VARIANT_LIMIT, ceiling: DAILY_SAFETY_CEILING }, { merge: true });
    return true;
  });
}
async function refundBudget(n) { // on failed create, give the variants back
  const ref = db.collection(META).doc("budget");
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const key = torontoDateKey();
    if (snap.exists && snap.data().dateKey === key) {
      tx.set(ref, { used: Math.max(0, (snap.data().used || 0) - n) }, { merge: true });
    }
  });
}
async function budgetSnapshot() {
  const snap = await db.collection(META).doc("budget").get();
  const key = torontoDateKey();
  const used = (snap.exists && snap.data().dateKey === key) ? (snap.data().used || 0) : 0;
  return { dateKey: key, used, ceiling: DAILY_SAFETY_CEILING, remaining: Math.max(0, DAILY_SAFETY_CEILING - used) };
}

// Online Store publication id, cached in Firestore (stable per shop).
async function onlineStorePublicationId() {
  const ref = db.collection(META).doc("publication");
  const snap = await ref.get();
  if (snap.exists && snap.data().id) return snap.data().id;
  const d = await gql(`query { publications(first: 25) { nodes { id catalog { title } } } }`);
  const nodes = (d.publications && d.publications.nodes) || [];
  const hit = nodes.find(n => /online store/i.test(((n.catalog || {}).title) || "")) || nodes[0];
  if (!hit) throw new Error("No publications found on shop");
  await ref.set({ id: hit.id, title: ((hit.catalog || {}).title) || "" });
  return hit.id;
}

// All collections (id, title, smart?) cached 6h — used for manual-collection placement.
async function collectionsCatalog() {
  const ref = db.collection(META).doc("collections");
  const snap = await ref.get();
  if (snap.exists && (Date.now() - (snap.data().at || 0)) < 6 * 3600 * 1000) return snap.data().list || [];
  const list = [];
  let cursor = null;
  for (let page = 0; page < 4; page++) {
    const d = await gql(`query($cursor: String) {
      collections(first: 100, after: $cursor) {
        nodes { id title handle ruleSet { rules { column } } }
        pageInfo { hasNextPage endCursor }
      }
    }`, { cursor });
    for (const n of d.collections.nodes || []) {
      list.push({ id: n.id, title: n.title, handle: n.handle, smart: !!n.ruleSet });
    }
    if (!d.collections.pageInfo.hasNextPage) break;
    cursor = d.collections.pageInfo.endCursor;
  }
  await ref.set({ at: Date.now(), list });
  return list;
}

function matchManualCollections(all, themeKeywords, title) {
  const kws = (themeKeywords || []).map(k => String(k).toLowerCase().trim()).filter(Boolean);
  const titleLc = String(title || "").toLowerCase();
  const hits = [];
  for (const c of all) {
    if (c.smart) continue; // smart collections self-populate via their rules
    const ct = c.title.toLowerCase();
    const hit = kws.some(k => ct.includes(k) || k.includes(ct)) ||
      ct.split(/[^a-z]+/).some(w => w.length > 3 && titleLc.includes(w));
    if (hit) hits.push(c);
  }
  return hits.slice(0, 3);
}

/* ------------------------- semantic theme expansion (AI) ------------------------- */
// Second AI pass: given the charm/listing context AND the store's real
// collection titles, find ALL likely meanings — not just the obvious one.
// A flower charm is also: birth flower, nature, garden, floral, botanical.
// A paw charm is also: dog, cat, pet, animal lover. Returns expanded meaning
// keywords (merged into product tags so smart Tag/Title collection rules can
// match) plus the exact collection titles that genuinely apply (used for
// manual-collection placement). Uses OPENAI_API_KEY (already configured for
// openaiProxy on this site). Fails soft: on any error the pipeline continues
// with the original themeKeywords only.
async function expandThemes(p, collectionTitles) {
  try {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) return null;
    const prompt = `A handmade jewelry product is being filed into store collections.

PRODUCT:
- Title: ${p.title}
- Jewelry category: ${String(p.category || "").replace(/_/g, " ")}
- Known theme keywords: ${(p.themeKeywords || []).join(", ") || "(none)"}
- Tags: ${(p.tags || []).slice(0, 15).join(", ")}

STORE COLLECTIONS (exact titles):
${collectionTitles.map(t => "- " + t).join("\n")}

TASK: Identify ALL likely meanings and audiences for this charm/design — not just the obvious one. Think laterally: a Flower charm is also a Birth Flower, Nature Lovers, garden, botanical item; a Bee is also insect, nature, honey, spring; a Paw is dog, cat, pet, animal lover; an Anchor is nautical, sea, navy, sailor; a Stethoscope is medical, nurse, doctor, healthcare worker. Include recipient/occasion angles when strongly implied (e.g. mom, graduation, bridesmaid).

Return STRICT JSON only, no markdown:
{"meanings": ["8-15 single lowercase words or short phrases covering every plausible meaning"], "collections": ["EXACT titles from the list above that genuinely fit — empty array if none"]}
Only include collection titles copied verbatim from the list. Do not force weak matches.`;
    const res = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": "Bearer " + apiKey },
      body: JSON.stringify({
        model: process.env.OPENAI_EXPANSION_MODEL || process.env.OPENAI_MODEL || "gpt-5.5",
        messages: [
          { role: "system", content: "Return STRICT JSON only." },
          { role: "user", content: prompt }
        ],
        reasoning_effort: "low",
        max_completion_tokens: 700
      })
    });
    if (!res.ok) return null;
    const data = await res.json();
    const raw = ((data.choices || [])[0] || {}).message?.content || "";
    const parsed = JSON.parse(raw.replace(/```json|```/g, "").trim());
    const meanings = Array.isArray(parsed.meanings) ? parsed.meanings.map(m => String(m).toLowerCase().trim()).filter(Boolean).slice(0, 15) : [];
    const titleSet = new Set(collectionTitles);
    const collections = Array.isArray(parsed.collections) ? parsed.collections.filter(t => titleSet.has(t)).slice(0, 5) : [];
    return { meanings, collections };
  } catch (e) { return null; }
}

/* ------------------------------ product creation ------------------------------ */

async function createShopifyProduct(job) {
  const p = job.payload;
  const matrix = job.matrix; // computed at enqueue

  // Semantic expansion first, so meanings land in tags (smart collections
  // with Tag/Title rules then match on their own) and AI-picked collections
  // feed the manual placement below.
  let allCollections = [];
  try { allCollections = await collectionsCatalog(); } catch (e) {}
  const expansion = await expandThemes(p, allCollections.map(c => c.title));
  const expandedMeanings = (expansion && expansion.meanings) || [];
  const aiPickedTitles = new Set((expansion && expansion.collections) || []);
  job.expandedMeanings = expandedMeanings;
  job.aiPickedCollections = Array.from(aiPickedTitles);

  const valuesByName = {};
  matrix.optionOrder.forEach(n => valuesByName[n] = []);
  const setVariants = matrix.variants.map(v => {
    const ov = matrix.optionOrder.map(n => {
      const val = String(v.options[n]);
      if (valuesByName[n].indexOf(val) < 0) valuesByName[n].push(val);
      return { optionName: n, name: val };
    });
    const variant = {
      optionValues: ov,
      price: String(v.price),
      inventoryPolicy: "CONTINUE",
      inventoryItem: { tracked: false }
    };
    if (v.sku) variant.sku = v.sku;
    return variant;
  });
  const productOptions = matrix.optionOrder.map((n, idx) => ({
    name: n, position: idx + 1, values: valuesByName[n].map(val => ({ name: val }))
  }));

  const tags = Array.from(new Set([
    ...(p.tags || []),
    ...expandedMeanings,          // secondary meanings -> smart Tag rules match
    matrix.typeTag,
    "auto-listed"
  ].map(t => String(t).trim()).filter(Boolean))).slice(0, 60);

  const input = {
    title: p.title,
    descriptionHtml: p.descriptionHtml || "",
    vendor: VENDOR,
    productType: p.productType || matrix.productType,
    status: "ACTIVE",
    tags,
    productOptions,
    variants: setVariants
  };

  const setRes = await gql(`mutation($input: ProductSetInput!) {
    productSet(synchronous: true, input: $input) {
      product { id handle title variantsCount { count } }
      userErrors { field message }
    }
  }`, { input });
  const errs = (setRes.productSet && setRes.productSet.userErrors) || [];
  if (errs.length) throw new Error("productSet: " + errs.map(e => e.message).join("; "));
  const product = setRes.productSet.product;

  // Media — always mediaUserErrors, never userErrors, on product media mutations.
  const images = (p.images || []).filter(im => im && im.src && /^https:\/\//i.test(im.src)).slice(0, 10);
  if (images.length) {
    const media = images.map(im => ({
      originalSource: im.src,
      alt: (im.alt || p.title || "").slice(0, 500),
      mediaContentType: "IMAGE"
    }));
    const mRes = await gql(`mutation($productId: ID!, $media: [CreateMediaInput!]!) {
      productCreateMedia(productId: $productId, media: $media) {
        media { ... on MediaImage { id } }
        mediaUserErrors { field message }
      }
    }`, { productId: product.id, media });
    const mErrs = (mRes.productCreateMedia && mRes.productCreateMedia.mediaUserErrors) || [];
    if (mErrs.length) job.mediaWarnings = mErrs.map(e => e.message);
  }

  // Publish to Online Store — instantly live.
  const pubId = await onlineStorePublicationId();
  const pubRes = await gql(`mutation($id: ID!, $input: [PublicationInput!]!) {
    publishablePublish(id: $id, input: $input) { userErrors { field message } }
  }`, { id: product.id, input: [{ publicationId: pubId }] });
  const pubErrs = (pubRes.publishablePublish && pubRes.publishablePublish.userErrors) || [];
  if (pubErrs.length) job.publishWarnings = pubErrs.map(e => e.message);

  // Manual collections (smart ones self-populate from productType/tags/title).
  let addedCollections = [];
  try {
    const all = allCollections.length ? allCollections : await collectionsCatalog();
    // Union: AI-picked exact titles + keyword matcher (original themes PLUS
    // expanded meanings), manual collections only — smart ones self-populate.
    const kwHits = matchManualCollections(all, [...(p.themeKeywords || []), ...expandedMeanings], p.title);
    const byId = {};
    for (const c of all) if (!c.smart && aiPickedTitles.has(c.title)) byId[c.id] = c;
    for (const c of kwHits) byId[c.id] = c;
    const hits = Object.values(byId).slice(0, 4);
    for (const c of hits) {
      const aRes = await gql(`mutation($id: ID!, $productIds: [ID!]!) {
        collectionAddProducts(id: $id, productIds: $productIds) { userErrors { field message } }
      }`, { id: c.id, productIds: [product.id] });
      const aErrs = (aRes.collectionAddProducts && aRes.collectionAddProducts.userErrors) || [];
      if (!aErrs.length) addedCollections.push(c.title);
    }
  } catch (e) { job.collectionWarnings = [String(e.message || e)]; }

  return { productId: product.id, handle: product.handle, variantsCreated: (product.variantsCount || {}).count || setVariants.length, addedCollections };
}

/* ------------------------------ queue + drain ------------------------------ */

function validatePayload(p) {
  const errors = [];
  if (!p || typeof p !== "object") return ["missing payload"];
  if (!p.title || String(p.title).trim().length < 3) errors.push("title required");
  if (!p.category) errors.push("category required");
  if (!Array.isArray(p.images) || !p.images.length) errors.push("at least one image URL required");
  else if (!p.images.some(im => /^https:\/\//i.test(im && im.src || ""))) errors.push("images must be https URLs");
  return errors;
}

async function enqueue(payload) {
  const errors = validatePayload(payload);
  if (errors.length) return { ok: false, errors };
  const tier = 1 + Math.floor(Math.random() * 3); // random tier per scheme
  const matrix = buildMatrix(payload.category, tier, (payload.sku || "").trim().toUpperCase() || null);
  const doc = {
    status: "QUEUED",
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    createdAtMs: Date.now(),
    attempts: 0,
    tier,
    variantCount: matrix.variants.length,
    matrix,
    payload: {
      category: payload.category,
      title: String(payload.title).trim().slice(0, 255),
      descriptionHtml: String(payload.descriptionHtml || "").slice(0, 60000),
      tags: (payload.tags || []).slice(0, 40),
      themeKeywords: (payload.themeKeywords || []).slice(0, 12),
      productType: payload.productType || null,
      sku: (payload.sku || "").trim().toUpperCase() || null,
      images: (payload.images || []).slice(0, 10).map(im => ({ src: String(im.src || ""), alt: String(im.alt || "").slice(0, 500) })),
      source: payload.source || "index-app"
    }
  };
  const ref = await db.collection(QCOL).add(doc);
  return { ok: true, jobId: ref.id, tier, variantCount: matrix.variants.length };
}

async function drain() {
  const t0 = Date.now();
  const out = { uploaded: [], skipped: 0, failed: [], stoppedFor: null };
  const snap = await db.collection(QCOL).where("status", "==", "QUEUED").orderBy("createdAtMs", "asc").limit(30).get();
  for (const docSnap of snap.docs) {
    if (Date.now() - t0 > DRAIN_TIME_BUDGET_MS) { out.stoppedFor = "time"; break; }
    const job = { id: docSnap.id, ...docSnap.data() };
    const granted = await reserveBudget(job.variantCount);
    if (!granted) { out.stoppedFor = "budget"; break; }
    await docSnap.ref.set({ status: "UPLOADING", startedAtMs: Date.now() }, { merge: true });
    try {
      const result = await createShopifyProduct(job);
      await docSnap.ref.set({
        status: "DONE", completedAtMs: Date.now(),
        productId: result.productId, handle: result.handle,
        variantsCreated: result.variantsCreated, addedCollections: result.addedCollections,
        expandedMeanings: job.expandedMeanings || null, aiPickedCollections: job.aiPickedCollections || null,
        mediaWarnings: job.mediaWarnings || null, publishWarnings: job.publishWarnings || null,
        collectionWarnings: job.collectionWarnings || null
      }, { merge: true });
      out.uploaded.push({ jobId: job.id, handle: result.handle, variants: result.variantsCreated });
    } catch (e) {
      await refundBudget(job.variantCount);
      await docSnap.ref.set({
        status: "FAILED", attempts: (job.attempts || 0) + 1,
        error: String(e && e.message || e).slice(0, 800), failedAtMs: Date.now()
      }, { merge: true });
      out.failed.push({ jobId: job.id, error: String(e && e.message || e).slice(0, 200) });
    }
  }
  return out;
}

async function status() {
  const [q, u, f, d, budget] = await Promise.all([
    db.collection(QCOL).where("status", "==", "QUEUED").count().get(),
    db.collection(QCOL).where("status", "==", "UPLOADING").count().get(),
    db.collection(QCOL).where("status", "==", "FAILED").count().get(),
    db.collection(QCOL).where("status", "==", "DONE").count().get(),
    budgetSnapshot()
  ]);
  return {
    queued: q.data().count, uploading: u.data().count,
    failed: f.data().count, done: d.data().count, budget
  };
}

async function retryFailed() {
  const snap = await db.collection(QCOL).where("status", "==", "FAILED").limit(100).get();
  let n = 0;
  for (const s of snap.docs) { await s.ref.set({ status: "QUEUED", error: null }, { merge: true }); n++; }
  return { requeued: n };
}

/* ------------------------------ handler ------------------------------ */

const HEADERS = {
  "Content-Type": "application/json",
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type"
};
const out = (status, obj) => ({ statusCode: status, headers: HEADERS, body: JSON.stringify(obj) });

exports.handler = async (event) => {
  try {
    if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: HEADERS, body: "" };
    const q = (event && event.queryStringParameters) || {};
    let body = {};
    if (event.httpMethod === "POST") { try { body = JSON.parse(event.body || "{}"); } catch (_) {} }
    const op = body.op || q.op || "status";

    if (op === "enqueue") {
      const res = await enqueue(body.payload);
      if (!res.ok) return out(400, res);
      const drained = await drain(); // immediate attempt within today's budget
      return out(200, { ...res, drained, budget: await budgetSnapshot() });
    }
    if (op === "drain") return out(200, { drained: await drain(), budget: await budgetSnapshot() });
    if (op === "status") return out(200, await status());
    if (op === "retryFailed") { const r = await retryFailed(); const drained = await drain(); return out(200, { ...r, drained }); }
    return out(400, { error: "Unknown op: " + op });
  } catch (e) {
    return out(500, { error: String(e && e.message || e) });
  }
};
