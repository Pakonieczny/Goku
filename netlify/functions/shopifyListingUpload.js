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
// CREDENTIAL PIGGYBACK SUPPORT ------------------------------------------
// The Shopify Admin credentials live on the goldenspike Netlify site (used
// by shopifyEditor.js). Rather than duplicating secrets onto this site,
// gql() transparently proxies through that site's "gqlProxy" action when
// no local SHOPIFY_* env vars exist. Override the endpoint with
// SHOPIFY_PROXY_BASE if the credential site ever moves; if that site has
// EDIT_PASSCODE enabled, mirror it here as SHOPIFY_PROXY_PASSCODE.
const SHOPIFY_PROXY_BASE =
  process.env.SHOPIFY_PROXY_BASE ||
  "https://goldenspike.app/.netlify/functions/shopifyEditor";
const hasLocalShopifyCreds = () =>
  !!(process.env.SHOPIFY_STORE && process.env.SHOPIFY_CLIENT_ID && process.env.SHOPIFY_CLIENT_SECRET);

async function gqlViaProxy(query, variables, _attempt) {
  const headers = { "Content-Type": "application/json" };
  const pass = process.env.SHOPIFY_PROXY_PASSCODE || process.env.EDIT_PASSCODE;
  if (pass) headers["X-Edit-Passcode"] = pass;
  let res;
  try {
    res = await fetch(SHOPIFY_PROXY_BASE, {
      method: "POST",
      headers,
      body: JSON.stringify({ action: "gqlProxy", query, variables: variables || {} })
    });
  } catch (e) {
    throw new Error(`Shopify credential-proxy request to ${SHOPIFY_PROXY_BASE} failed at the network level: ${e && e.message}.`);
  }
  const j = await res.json().catch(() => ({}));
  if (res.status === 429 && (_attempt || 0) < 4) {
    await new Promise(r => setTimeout(r, 1500 * ((_attempt || 0) + 1)));
    return gqlViaProxy(query, variables, (_attempt || 0) + 1);
  }
  if (res.status === 401) {
    throw new Error("Shopify credential-proxy rejected the call (401): the credential site has EDIT_PASSCODE enabled — set SHOPIFY_PROXY_PASSCODE on this site to the same value.");
  }
  if (res.status === 400 && /Unknown op|Unknown action|Missing 'query'/i.test((j && j.error) || "")) {
    throw new Error("Shopify credential-proxy error: the credential site is running an older shopifyEditor.js without the gqlProxy action — deploy the updated shopifyEditor.js there.");
  }
  if (!res.ok || j.ok === false) {
    throw new Error(`Shopify credential-proxy error (${res.status}): ${(j && j.error) || "unknown"}`);
  }
  return j.data;
}
// ------------------------------------------------------------------------

// DIAGNOSTIC FIX: with SHOPIFY_STORE unset, the token fetch targeted
// "https://undefined/..." and every job failed with undici's generic
// "fetch failed" — useless in the panel. Validate the env up front and
// name exactly what's missing, and wrap both fetches so network errors
// say WHICH request to WHICH host failed.
function requireShopifyEnv() {
  const missing = [];
  if (!process.env.SHOPIFY_STORE) missing.push("SHOPIFY_STORE");
  if (!process.env.SHOPIFY_CLIENT_ID) missing.push("SHOPIFY_CLIENT_ID");
  if (!process.env.SHOPIFY_CLIENT_SECRET) missing.push("SHOPIFY_CLIENT_SECRET");
  if (missing.length) {
    throw new Error(
      "Missing Netlify env var(s) on THIS site: " + missing.join(", ") +
      ". Add them in Site settings → Environment variables (SHOPIFY_STORE looks like brites-jewelry.myshopify.com), then redeploy."
    );
  }
}
async function getToken() {
  if (_token && Date.now() < _tokenExp - 60000) return _token;
  requireShopifyEnv();
  const store = process.env.SHOPIFY_STORE;
  let res;
  try {
    res = await fetch(`https://${store}/admin/oauth/access_token`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        grant_type: "client_credentials",
        client_id: process.env.SHOPIFY_CLIENT_ID,
        client_secret: process.env.SHOPIFY_CLIENT_SECRET
      })
    });
  } catch (e) {
    throw new Error(`Shopify token request to https://${store}/ failed at the network level: ${e && e.message}. Check SHOPIFY_STORE spelling and site connectivity.`);
  }
  const text = await res.text();
  if (!res.ok) throw new Error("Token request failed (" + res.status + "): " + text);
  const data = JSON.parse(text);
  _token = data.access_token;
  _tokenExp = Date.now() + (Number(data.expires_in || 3600) * 1000);
  return _token;
}

async function gql(query, variables, _attempt) {
  // CREDENTIAL PIGGYBACK: when this site has no SHOPIFY_* env vars, route
  // every GraphQL call through the sibling Netlify site that already holds
  // the credentials (shopifyEditor.js's "gqlProxy" action). Direct mode is
  // used automatically whenever local creds exist, so adding the env vars
  // here later switches back with zero code changes.
  if (!hasLocalShopifyCreds()) return gqlViaProxy(query, variables, _attempt);
  const store = process.env.SHOPIFY_STORE;
  const token = await getToken();
  let res;
  try {
    res = await fetch(`https://${store}/admin/api/${API_VERSION}/graphql.json`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Shopify-Access-Token": token },
      body: JSON.stringify({ query, variables: variables || {} })
    });
  } catch (e) {
    throw new Error(`Shopify GraphQL request to https://${store}/ failed at the network level: ${e && e.message}.`);
  }
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

/* ---------------- COLLECTION ALLOWLIST (canonical, user-defined) ----------------
 * ONLY these collections may ever be targeted, AI-picked, manually added, or
 * displayed. The Shopify store contains other collections (legacy, internal,
 * menu parents like "Profession & Passion") — the pipeline must ignore them
 * completely. Edit this list when the storefront's collection set changes. */
const ALLOWED_COLLECTIONS = [
  // Animals & Nature
  "Animal Lovers", "Bird Lovers", "Floral & Flower Lovers", "Insects & Butterflies", "Beach & Ocean",
  // Symbols & Style
  "Celestial", "Heart Jewelry", "Cross & Religious", "Western & Cowboy", "Food & Fruits", "Mythical Dragon",
  // Profession
  "Sports & Athletics", "Gifts for Teachers", "Gifts for Graduates", "Music & Musicians", "Dancers",
  "Nurses & Doctors", "Firefighters", "Police Officers", "Pilots & Aviation", "Military & Veterans",
  // Jewelry
  "Necklaces", "Charms Only", "Earrings", "Bracelets", "Rings",
  // Chains & Charms
  "Beady Chain Necklaces", "Bar & Engraved", "Disc & Coin Charms"
];
const ALLOWED_SET = new Set(ALLOWED_COLLECTIONS.map(t => t.toLowerCase().trim()));
const isAllowedCollection = (title) => ALLOWED_SET.has(String(title || "").toLowerCase().trim());

// All collections (id, title, smart?) cached 6h — used for manual-collection placement.
async function collectionsCatalog() {
  const ref = db.collection(META).doc("collections");
  const snap = await ref.get();
  const cached = snap.exists ? (snap.data().list || []) : [];
  const cacheHasRules = cached.some(c => c.smart && Array.isArray(c.rules));
  if (snap.exists && cacheHasRules && (Date.now() - (snap.data().at || 0)) < 6 * 3600 * 1000) return cached;
  const list = [];
  let cursor = null;
  for (let page = 0; page < 4; page++) {
    const d = await gql(`query($cursor: String) {
      collections(first: 100, after: $cursor) {
        nodes { id title handle ruleSet { appliedDisjunctively rules { column relation condition } } }
        pageInfo { hasNextPage endCursor }
      }
    }`, { cursor });
    for (const n of d.collections.nodes || []) {
      list.push({
        id: n.id, title: n.title, handle: n.handle, smart: !!n.ruleSet,
        disjunctive: n.ruleSet ? !!n.ruleSet.appliedDisjunctively : null,
        rules: n.ruleSet ? (n.ruleSet.rules || []).map(r => ({
          column: String(r.column || "").toLowerCase(),
          relation: String(r.relation || "").toLowerCase(),
          condition: String(r.condition || "")
        })) : null
      });
    }
    if (!d.collections.pageInfo.hasNextPage) break;
    cursor = d.collections.pageInfo.endCursor;
  }
  await ref.set({ at: Date.now(), list });
  return list;
}

/* ---------------- SMART-MATCH: satisfy smart-collection rules ----------------
 * Smart collections only self-populate when the product LITERALLY satisfies
 * their rules — and rules like TAG equals "necklace" are exact-match, so a
 * product tagged "beady necklace" misses the Necklaces collection entirely.
 * This planner decides which smart collections SHOULD apply (AI picks, theme
 * keywords, and structural type matches) and returns the exact extra tags
 * needed to satisfy each target's rules. Rules we cannot force (TITLE, TYPE
 * mismatches, price) are only counted as satisfied when already true. */
function planSmartCollections(all, ctx) {
  const title = String(ctx.title || "").toLowerCase();
  const productType = String(ctx.productType || "").toLowerCase();
  const typeTag = String(ctx.typeTag || "").toLowerCase();
  const vendor = String(ctx.vendor || "").toLowerCase();
  const baseTags = new Set((ctx.tags || []).map(t => String(t).toLowerCase().trim()));
  const kws = (ctx.keywords || []).map(k => String(k).toLowerCase().trim()).filter(Boolean);
  const aiTitles = new Set((ctx.aiPickedTitles || []).map(t => String(t)));
  const catWords = String(ctx.category || "").toLowerCase().split(/[^a-z]+/).filter(w => w.length > 3);

  const extraTags = new Set();
  const expected = [];

  const ruleState = (r, pendingTags) => {
    // → "true" (already satisfied) | "forcible" (satisfiable by adding a tag)
    //   | "false" (cannot be satisfied)
    const cond = r.condition.toLowerCase();
    const has = (t) => baseTags.has(t) || pendingTags.has(t);
    if (r.column === "tag") {
      if (has(cond)) return "true";
      return "forcible"; // equals AND contains are both satisfied by adding the exact tag
    }
    if (r.column === "type") {
      if (r.relation === "equals") return productType === cond ? "true" : "false";
      if (r.relation === "contains") return productType.includes(cond) ? "true" : "false";
      return "false";
    }
    if (r.column === "title") {
      if (r.relation === "equals") return title === cond ? "true" : "false";
      if (r.relation === "contains") return title.includes(cond) ? "true" : "false";
      return "false";
    }
    if (r.column === "vendor") {
      if (r.relation === "equals") return vendor === cond ? "true" : "false";
      if (r.relation === "contains") return vendor.includes(cond) ? "true" : "false";
      return "false";
    }
    return "false"; // price/weight/inventory etc. — never force, never assume
  };

  // SIBLING DISCRIMINATION: categories that share a productType (both
  // necklace categories are "Necklace"; both earring categories are
  // "Earrings") are only distinguishable by category. A collection whose
  // title carries a sibling qualifier ("Beady …", "Stud …", "Hoop …") must
  // be HARD-VETOED for any product from a different sibling category —
  // fuzzy word overlap ("necklace" ⊂ "Beady Chain Necklaces") and even an
  // AI pick must never put a Regular necklace into a Beady collection.
  const SIBLING_QUALIFIERS = [
    { word: "beady", category: "Beady_Necklace" },
    { word: "stud",  category: "Stud_Earrings"  },
    { word: "hoop",  category: "Hoop_Earrings"  }
  ];
  const productCategory = String(ctx.category || "");
  const vetoed = (collectionTitleLc) => SIBLING_QUALIFIERS.some(q =>
    new RegExp("\\b" + q.word + "\\b", "i").test(collectionTitleLc) && productCategory !== q.category);

  for (const c of all) {
    if (!c.smart || !Array.isArray(c.rules) || !c.rules.length) continue;
    const ct = c.title.toLowerCase();
    if (vetoed(ct)) continue;

    // Is this collection a TARGET for the product?
    const themed = aiTitles.has(c.title) ||
      kws.some(k => ct.includes(k) || k.includes(ct)) ||
      ct.split(/[^a-z]+/).some(w => w.length > 3 && title.includes(w));
    const structural = c.rules.some(r => {
      const cond = r.condition.toLowerCase();
      if (r.column === "type") return productType && (cond === productType || productType.includes(cond) || cond.includes(productType));
      if (r.column === "tag") return cond === typeTag || (catWords.length && catWords.some(w => cond === w));
      return false;
    });
    if (!themed && !structural) continue;

    // Can we satisfy its rules?
    const pending = new Set();
    const states = c.rules.map(r => ({ r, s: ruleState(r, pending) }));
    if (c.disjunctive !== false) {
      // ANY rule: prefer one that's already true; else force the cheapest tag.
      if (states.some(x => x.s === "true")) { expected.push(c.title); continue; }
      const f = states.find(x => x.s === "forcible");
      if (f) { extraTags.add(f.r.condition); expected.push(c.title); }
    } else {
      // ALL rules must hold.
      if (states.some(x => x.s === "false")) continue;
      states.forEach(x => { if (x.s === "forcible") extraTags.add(x.r.condition); });
      expected.push(c.title);
    }
  }
  return { extraTags: Array.from(extraTags), expectedSmart: expected };
}

// Ground truth: which collections does Shopify say the product is in?
// Smart membership recomputes shortly after tags land — retry once on empty.
async function verifyProductCollections(productId) {
  const q = `query($id: ID!) { product(id: $id) { collections(first: 50) { nodes { title } } } }`;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const d = await gql(q, { id: productId });
      const titles = (((d || {}).product || {}).collections || {}).nodes ? d.product.collections.nodes.map(n => n.title) : [];
      if (titles.length || attempt === 1) return titles;
    } catch (e) {
      if (attempt === 1) { console.warn("verifyProductCollections failed:", e.message); return []; }
    }
    await new Promise(r => setTimeout(r, 4000));
  }
  return [];
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
  try {
    // ALLOWLIST GATE: everything downstream — the AI's pickable titles, the
    // smart-rule planner, the manual-collection matcher — sees ONLY the
    // canonical collections. Anything else in the store is invisible here.
    allCollections = (await collectionsCatalog()).filter(c => isAllowedCollection(c.title));
  } catch (e) {}
  const expansion = await expandThemes(p, allCollections.map(c => c.title));
  const expandedMeanings = (expansion && expansion.meanings) || [];
  const aiPickedTitles = new Set(((expansion && expansion.collections) || []).filter(isAllowedCollection));
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

  // SMART-MATCH: derive the exact tags each targeted smart collection's
  // rules demand, so membership is deterministic instead of hoping the AI
  // tags happen to exact-match a rule condition. Planned tags go FIRST so
  // the 60-tag cap can never trim them away.
  let smartPlan = { extraTags: [], expectedSmart: [] };
  try {
    // The CATEGORY is user-driven (chosen when files were uploaded to
    // Firebase via the folder prefix) — the least ambiguous signal we
    // have. Feed it into targeting directly so structural collections
    // ("Beady Chain Necklaces") are guaranteed by the classification
    // alone, independent of what the AI title/keywords happen to say.
    const catPhrase = String(p.category || "").replace(/_/g, " ").toLowerCase().trim();
    const catKeywords = catPhrase ? [catPhrase, ...catPhrase.split(/\s+/).filter(w => w.length > 3)] : [];
    smartPlan = planSmartCollections(allCollections, {
      title: p.title, category: p.category,
      productType: p.productType || matrix.productType,
      typeTag: matrix.typeTag, vendor: VENDOR,
      tags: [...(p.tags || []), ...expandedMeanings, matrix.typeTag],
      keywords: [...(p.themeKeywords || []), ...expandedMeanings, ...catKeywords],
      aiPickedTitles: Array.from(aiPickedTitles)
    });
    if (smartPlan.extraTags.length) console.log("Smart-match tags added:", smartPlan.extraTags.join(", "));
    job.expectedSmartCollections = smartPlan.expectedSmart;
  } catch (e) { console.warn("planSmartCollections failed (continuing):", e.message); }

  // The category itself becomes a tag ("beady necklace" / "regular
  // necklace") — deterministic, user-driven, and lets store rules key on
  // the exact category phrase without depending on AI output.
  const categoryTag = String(p.category || "").replace(/_/g, " ").toLowerCase().trim();

  const tags = Array.from(new Set([
    ...smartPlan.extraTags,       // rule-satisfying tags — never trimmed
    categoryTag,
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

  // IDEMPOTENT RESUME: persist the productId the moment creation succeeds.
  // Previously it was only saved on FULL success, so a failure in any later
  // stage (e.g. publish dying on a missing read_publications scope) left an
  // orphaned product on Shopify — and every "Retry failed" created another
  // copy. A retried job that already carries a productId now SKIPS creation
  // and media (both would duplicate) and resumes at publish + collections
  // (both safe to repeat).
  let product;
  if (job.productId) {
    product = { id: job.productId, handle: job.handle || null, variantsCount: null };
    console.log(`Resuming job ${job.id}: product ${job.productId} already created — skipping productSet/media.`);
  } else {
    const setRes = await gql(`mutation($input: ProductSetInput!) {
      productSet(synchronous: true, input: $input) {
        product { id handle title variantsCount { count } }
        userErrors { field message }
      }
    }`, { input });
    const errs = (setRes.productSet && setRes.productSet.userErrors) || [];
    if (errs.length) throw new Error("productSet: " + errs.map(e => e.message).join("; "));
    product = setRes.productSet.product;
    job.productId = product.id;
    job.handle = product.handle;
    try {
      await db.collection(QCOL).doc(job.id).set({ productId: product.id, handle: product.handle }, { merge: true });
    } catch (e) { console.warn("Couldn't persist productId for resume:", e.message); }

    // Media — always mediaUserErrors, never userErrors, on product media mutations.
    // Fresh-create only: productCreateMedia APPENDS, so re-running it on a
    // resumed job would duplicate the product's images.
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

      // PIN GALLERY ORDER. productCreateMedia processes images
      // asynchronously — final positions follow processing completion, not
      // the input array, so a slow image lands later regardless of where it
      // was submitted. The created-media list in the response mirrors input
      // order, so declare that order explicitly — the Shopify equivalent of
      // Etsy's per-image rank, part of the upload itself.
      const mediaIds = ((mRes.productCreateMedia && mRes.productCreateMedia.media) || [])
        .map(m => m && m.id).filter(Boolean);
      if (mediaIds.length > 1) {
        const moves = mediaIds.map((id, idx) => ({ id, newPosition: String(idx) }));
        const oRes = await gql(`mutation($id: ID!, $moves: [MoveInput!]!) {
          productReorderMedia(id: $id, moves: $moves) {
            job { id }
            mediaUserErrors { field message }
          }
        }`, { id: product.id, moves });
        const oErrs = (oRes.productReorderMedia && oRes.productReorderMedia.mediaUserErrors) || [];
        if (oErrs.length) job.mediaWarnings = [...(job.mediaWarnings || []), ...oErrs.map(e => e.message)];
      }
    }
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
    const all = allCollections.length
      ? allCollections
      : (await collectionsCatalog()).filter(c => isAllowedCollection(c.title));
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

  // Ground-truth verification: what does Shopify ACTUALLY show? The panel
  // displays this instead of what we merely attempted.
  let collectionsActual = [];
  try {
    // Ground truth, filtered to the allowlist: a legacy smart collection's
    // own rules may still capture the product inside Shopify, but per store
    // policy those are ignored — never displayed, never counted.
    collectionsActual = (await verifyProductCollections(product.id)).filter(isAllowedCollection);
  } catch (_) {}

  // "Complete the set" cross-links: register the new product with existing
  // same-charm listings (both directions). Never fatal — the product is live
  // either way, and the linkage can be re-run.
  let setLinks = null;
  try {
    setLinks = await ensureSetLinks(product, job);
    console.log(`Set links for ${product.handle}:`, JSON.stringify(setLinks));
  } catch (e) {
    job.setLinkWarnings = [String((e && e.message) || e)];
    console.warn("ensureSetLinks failed (non-fatal):", e && e.message);
  }

  return {
    productId: product.id, handle: product.handle,
    variantsCreated: (product.variantsCount || {}).count || setVariants.length,
    addedCollections, collectionsActual, setLinks,
    expectedSmartCollections: job.expectedSmartCollections || []
  };
}

/* ---------------------------- charm set linking ---------------------------- */
// The PDP "Complete the set" module reads product metafield brites.set:
// a JSON array of partners [{h: handle, f: form, t: short title}]. The
// verified master (charmSetsData.js on goldenspike) is an offline snapshot —
// live truth is the metafields. New products must register themselves there:
// forward (new -> partners) and backward (each partner -> new).

const SET_FORM_BY_CATEGORY = {
  Beady_Necklace: "Necklace", Necklace: "Necklace", Necklaces: "Necklace",
  Stud_Earrings: "Stud Earrings", Hoop_Earrings: "Huggies", Huggies: "Huggies",
  Bracelet: "Bracelet", Bracelets: "Bracelet", Ring: "Ring", Rings: "Ring",
  Charm_Only: "Charm Only", Charms: "Charm Only"
};

// Order matters: "earring" contains "ring", huggie/hoop beat stud.
function setFormFromTitle(title) {
  const t = String(title || "").toLowerCase();
  if (/\bhuggies?\b|\bhoops?\b/.test(t)) return "Huggies";
  if (/\bearrings?\b|\bstuds?\b/.test(t)) return "Stud Earrings";
  if (/\bbracelets?\b/.test(t)) return "Bracelet";
  if (/\bnecklaces?\b|\bpendants?\b/.test(t)) return "Necklace";
  if (/\brings?\b/.test(t)) return "Ring";
  if (/\bcharms?\b/.test(t)) return "Charm Only";
  return "";
}

// Words that describe the FORM or are filler — everything left is the charm
// subject ("Pinecone", "Flying Swallow", "Map of Palestine").
const SET_STOP_WORDS = new Set([
  "charm", "charms", "necklace", "necklaces", "pendant", "pendants", "beady",
  "chain", "stud", "studs", "earring", "earrings", "huggie", "huggies",
  "hoop", "hoops", "bracelet", "bracelets", "ring", "rings", "disc", "bar",
  "on", "a", "an", "the", "for", "in", "of", "with", "and", "or", "to",
  "dainty", "cute", "tiny", "mini", "small", "little", "whimsical", "kawaii",
  "handcrafted", "adorable", "elegant", "delicate", "gold", "silver", "14k",
  "lovers", "lover", "everyday", "layering", "gift", "giving", "birthdays",
  "moms", "mothers", "day", "collectors", "friends", "jewelry"
]);

// Words that mark the FORM/STRUCTURE part of a title. In this catalog's
// title grammar the charm subject always LEADS ("Paintbrush Charm Beady
// Necklace for Teachers", "Flying Swallow Charm Stud Earrings") — so the
// subject is everything BEFORE the first form word, and everything after
// (format, chain, audience like "for Teachers") is noise by construction.
const SET_FORM_MARKERS = new Set([
  "charm", "charms", "necklace", "necklaces", "pendant", "pendants",
  "earring", "earrings", "stud", "studs", "huggie", "huggies", "hoop",
  "hoops", "bracelet", "bracelets", "ring", "rings", "beady", "chain",
  "disc", "bar", "jewelry"
]);
const SET_FILLERS = new Set([
  "a", "an", "the", "of", "and", "or", "with", "for", "on", "in", "to",
  "dainty", "cute", "tiny", "mini", "small", "little", "whimsical",
  "kawaii", "adorable", "elegant", "delicate", "handcrafted", "gold",
  "silver", "14k", "silhouette", "motif", "shaped", "style", "styled"
]);

function setSubjectTokens(title) {
  const raw = String(title || "").toLowerCase().replace(/[^a-z0-9\s-]/g, " ")
    .split(/[\s-]+/).filter(Boolean);
  const cut = raw.findIndex(w => SET_FORM_MARKERS.has(w));
  const leading = (cut > 0 ? raw.slice(0, cut) : raw).filter(w => !SET_FILLERS.has(w));
  if (leading.length && cut > 0) return leading;
  // Fallback (form word leads or nothing survives): full stopword strip.
  return raw.filter(w => !SET_FORM_MARKERS.has(w) && !SET_FILLERS.has(w) && !SET_STOP_WORDS.has(w));
}

// The map's t values are short ("Elephant Charm Necklace"): cut the raw
// title at the first filler joint, cap at 6 words.
function setShortTitle(title) {
  let t = String(title || "").split(/\s+(?:on|for|in|with)\s+/i)[0];
  return t.split(/\s+/).slice(0, 6).join(" ").trim();
}

// Zoomed vision input — the SAME template crop the client pipeline applies
// before its vision calls (window.AI_ZOOM_TEMPLATE / cropImageForAI in
// Index.html): zoom 2, centered horizontally, center shifted +8% of height
// downward. The catalog's photography places the charm ~1/3 up from the
// bottom, so this window reliably holds the full charm at double
// magnification — the judged detail (cutouts, engraving, silhouette) is
// twice the resolution of the raw frame. Exact port of the client math:
// crop w/h = dim/zoom; origin centered + offset*dim, clamped; longest side
// capped at 1024; JPEG q85. Any failure falls back to the original URL so a
// crop hiccup can never break the judgment (same policy as the client).
const AI_ZOOM_TEMPLATE = { zoom: 2, offsetX: 0, offsetY: 0.08 };

function zoomCropRect(W, H, tpl) {
  const zoom = Math.max(1, Number(tpl.zoom) || 2);
  const cw = W / zoom, ch = H / zoom;
  let sx = (W - cw) / 2 + (Number(tpl.offsetX) || 0) * W;
  let sy = (H - ch) / 2 + (Number(tpl.offsetY) || 0) * H;
  sx = Math.max(0, Math.min(W - cw, sx));
  sy = Math.max(0, Math.min(H - ch, sy));
  return { sx: Math.round(sx), sy: Math.round(sy), cw: Math.round(cw), ch: Math.round(ch) };
}

async function zoomImageForAI(url) {
  const sharp = require("sharp"); // already a project dependency
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 20000);
  let buf;
  try {
    const r = await fetch(url, { signal: controller.signal });
    if (!r.ok) throw new Error("image fetch HTTP " + r.status);
    buf = Buffer.from(await r.arrayBuffer());
  } finally { clearTimeout(timer); }
  const meta = await sharp(buf).metadata();
  const W = meta.width, H = meta.height;
  if (!W || !H) throw new Error("image has no dimensions");
  const { sx, sy, cw, ch } = zoomCropRect(W, H, AI_ZOOM_TEMPLATE);
  let pipe = sharp(buf).extract({ left: sx, top: sy, width: cw, height: ch });
  const MAX = 1024;
  if (Math.max(cw, ch) > MAX) {
    pipe = pipe.resize({ width: cw >= ch ? MAX : null, height: ch > cw ? MAX : null });
  }
  const out = await pipe.jpeg({ quality: 85 }).toBuffer();
  return "data:image/jpeg;base64," + out.toString("base64");
}

// Visual verification — IDENTICAL criteria, prompt, model and gating to
// verifyCharmSets-background.js (the Set Matcher pipeline). Title matching
// above only proposes CANDIDATES; nothing reaches brites.set without the
// same charm-first visual confirmation the verified master was built with.
async function judgeCharms(imgs) {
  const model = process.env.OPENAI_VISION_MODEL || "gpt-5.4-mini";
  const content = [{ type: "text", text:
    "You are an expert jeweler matching CHARMS across product photos from ONE fine-jewelry store. The ONLY thing being matched is the charm itself. Report ONLY what is actually visible.\n\n" +
    "STEP 1 — FIND THE CHARM AND ZOOM IN. Photos may be zoomed out, lifestyle shots, or show the charm small on a model, on a chain, or beside other jewelry. LOCATE the charm in each photo and examine it as if magnified to full frame. IGNORE everything else: the chain and its style, the mounting (hoop/stud/necklace/ring/bracelet), the model, hands, props, background, lighting. A charm on a necklace, the same charm on a hoop earring, and the same charm photographed alone are ALL the same charm — format differences must NEVER cause a FALSE.\n\n" +
    "STEP 2 — MATCH THE CHARM WITH EXTREME PRECISION on these criteria, in order of importance:\n" +
    "1. TYPE/SUBJECT: what the charm depicts (which animal, symbol, letter, object). A penguin is not a sheep; a music note is not a treble clef; an alien head is not a UFO.\n" +
    "2. OUTLINE/SILHOUETTE: the exact outer shape and pose/orientation. Full-body vs head-only, upright vs side profile, wings folded vs spread = DIFFERENT charms even when the subject matches.\n" +
    "3. ENGRAVING, CUTOUTS & SURFACE DETAIL: cut-out holes, pierced patterns, engraved lines, stones, textures, added elements (leaves, stars, rays, banners). An open-outline star and a solid star are DIFFERENT. A plain crescent and a crescent with rays are DIFFERENT.\n" +
    "4. PROPORTIONS: relative dimensions of the charm's features.\n" +
    "5. SYMBOLIC MEANING where one clearly applies (zodiac sign/constellation, birth flower, Norse Mjolnir/Valknut/Vegvisir/Yggdrasil/runes): the meaning must match exactly — Leo is not Scorpio.\n\n" +
    "ACCEPTABLE differences (never cause FALSE): metal color/finish, charm size/scale, jewelry format and mounting, chain style, photo angle or lighting, shown singly vs as a pair.\n\n" +
    "TASK: Photo 1 is the REFERENCE charm. " + (imgs[0].form ? "Expected formats in order: " + imgs.map(i => i.form || "?").join(", ") + ". " : "") +
    "For EACH subsequent photo: zoom in on its charm and decide same_charm: TRUE only if it is the SAME charm design by ALL criteria above — same subject, same silhouette/pose, same cutouts/engraving/details, same meaning — merely worn or mounted differently. When the charm is too small, blurry, or hidden to verify the details, use confidence low and judge from what is genuinely visible.\n" +
    'Reply with ONLY a JSON array, one entry per photo starting from photo 2: [{"photo":2,"same_charm":true,"charm":"<meaning, e.g. Leo (zodiac)>","charm_detail":"<literal depiction incl. silhouette + cutouts>","confidence":"high|medium|low","reason":"short"}]' }];
  // Template-zoom every image (reference AND candidates) before judgment —
  // in parallel, each falling back to its raw URL if the crop fails. The
  // zoom is MANDATORY for accuracy, so its outcome is reported per image:
  // an un-zoomed judgment must be visible, never silent.
  const zoomReport = { ok: 0, failed: 0, failures: [] };
  const zoomed = await Promise.all(imgs.map(async (im) => {
    try { const u = await zoomImageForAI(im.url); zoomReport.ok++; return u; }
    catch (e) {
      zoomReport.failed++;
      zoomReport.failures.push(`${im.handle}: ${String((e && e.message) || e).slice(0, 90)}`);
      console.warn("zoom crop fallback for", im.handle, "-", e && e.message);
      return im.url;
    }
  }));
  zoomed.forEach(u => content.push({ type: "image_url", image_url: { url: u, detail: "high" } }));

  const payload = { model, messages: [{ role: "user", content }] };
  if (/^(gpt-5|o\d)/.test(model)) {
    payload.max_completion_tokens = 1500;
    payload.reasoning_effort = "low";
  } else {
    payload.max_tokens = 700;
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 60000);
  let upstream;
  try {
    upstream = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${process.env.OPENAI_API_KEY}` },
      body: JSON.stringify(payload), signal: controller.signal
    });
  } catch (err) {
    throw new Error(err && err.name === "AbortError" ? "OpenAI timeout after 60s" : (err && err.message) || String(err));
  } finally { clearTimeout(timer); }
  const text = await upstream.text().catch(() => "");
  let json = null; try { json = JSON.parse(text); } catch (_) {}
  if (!upstream.ok) throw new Error((json && json.error && json.error.message) || text || ("OpenAI HTTP " + upstream.status));
  const raw = ((json.choices || [])[0] || {}).message;
  const out = (raw && raw.content ? raw.content : "").replace(/```json|```/g, "").trim();
  let verdicts = null;
  try { verdicts = JSON.parse(out); } catch { verdicts = null; }
  return { verdicts, zoomReport };
}

// Expand the charm subject with catalog-vocabulary synonyms for SEARCH
// recall only (measured live: subject "porcine" found 0 of the pig family,
// which is titled "Pig …"). Failure-safe: on any error the base subject is
// used alone.
async function expandSubjectSynonyms(subject, title) {
  try {
    const model = process.env.OPENAI_VISION_MODEL || "gpt-5.4-mini";
    const payload = { model, messages: [{ role: "user", content:
      `A jewelry catalog names the same charm subject with different words across product titles. Subject words: ${JSON.stringify(subject)} (from the title ${JSON.stringify(String(title || ""))}). Reply with ONLY a JSON array of 0-4 additional lowercase single words that other product titles for the SAME subject would likely use — common names and synonyms (e.g. "porcine" -> ["pig","piggy"], "canine" -> ["dog","puppy"]). No adjectives, no style words, no repeats of the given words. [] if none apply.` }] };
    if (/^(gpt-5|o\d)/.test(model)) { payload.max_completion_tokens = 500; payload.reasoning_effort = "low"; }
    else { payload.max_tokens = 100; }
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 15000);
    let r;
    try {
      r = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${process.env.OPENAI_API_KEY}` },
        body: JSON.stringify(payload), signal: controller.signal
      });
    } finally { clearTimeout(timer); }
    const j = await r.json();
    const out = String((((j.choices || [])[0] || {}).message || {}).content || "").replace(/```json|```/g, "").trim();
    const arr = JSON.parse(out);
    if (!Array.isArray(arr)) return [];
    return arr.map(w => String(w).toLowerCase().trim())
      .filter(w => /^[a-z0-9]{2,20}$/.test(w) && !subject.includes(w)).slice(0, 4);
  } catch (e) {
    console.warn("subject synonym expansion skipped:", (e && e.message) || e);
    return [];
  }
}

// Semantic similarity between the charm subject and candidate titles —
// ONE embeddings call per run (subject + up to ~60 titles). Handles
// plurals, synonyms and phrasing natively; deterministic per model. This
// is the candidacy RANKER — vision remains the truth gate.
async function rankBySemantics(subjectPhrase, titles) {
  const inputs = [subjectPhrase].concat(titles);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 20000);
  let r;
  try {
    r = await fetch("https://api.openai.com/v1/embeddings", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${process.env.OPENAI_API_KEY}` },
      body: JSON.stringify({ model: "text-embedding-3-small", input: inputs }),
      signal: controller.signal
    });
  } finally { clearTimeout(timer); }
  const j = await r.json();
  if (!r.ok) throw new Error((j.error && j.error.message) || ("embeddings HTTP " + r.status));
  const vecs = (j.data || []).sort((a, b) => a.index - b.index).map(d => d.embedding);
  if (vecs.length !== inputs.length) throw new Error("embeddings shape mismatch");
  const dot = (a, b) => { let s = 0; for (let i = 0; i < a.length; i++) s += a[i] * b[i]; return s; };
  const norm = (a) => Math.sqrt(dot(a, a));
  const ref = vecs[0], refN = norm(ref);
  return titles.map((t, i) => dot(ref, vecs[i + 1]) / (refN * norm(vecs[i + 1]) || 1));
}

async function ensureSetLinks(product, job) {
  // Job docs nest the listing data under .payload (see the create function's
  // own `const p = job.payload`). Tolerate both shapes.
  const p = (job && job.payload) || job || {};
  const myForm = SET_FORM_BY_CATEGORY[p.category] || setFormFromTitle(p.title);
  const subject = setSubjectTokens(p.title);
  if (!myForm || !subject.length) return { partners: [], reason: "no form/subject", debug: { myForm, subject, title: p.title } };

  // Candidate pool: catalog search on the subject phrase. Precision does
  // NOT come from the search (the whole-word title filter below enforces
  // it) — so use the broadest query first, and a fallback ladder in case a
  // syntax is rejected by this API path (measured live: "title:owl"
  // returned 0 nodes while the catalog holds many owls).
  // Multi-word subjects MUST search with OR: Shopify ANDs space-separated
  // terms, and families name the same charm inconsistently ("Horned Goat"
  // vs "Capricorn Goat" vs "Goat Head") — measured live: "horned goat"
  // returned 2 of a 24-product goat family. Recall is the search's job;
  // ranking (match count) and the vision check keep precision.
  const subjectKey = subject.join("_").replace(/[.\/]/g, "-");
  let storedSyn = [];
  try {
    const synDoc = await db.collection("Brites_Editor_Meta").doc("setSubjectSynonyms").get();
    storedSyn = ((synDoc.exists && synDoc.data()) || {})[subjectKey] || [];
  } catch (e) { console.warn("synonym store read skipped:", e && e.message); }
  const fresh = await expandSubjectSynonyms(subject, p.title);
  const synonyms = Array.from(new Set(storedSyn.concat(fresh)));
  if (fresh.some(w => !storedSyn.includes(w))) {
    db.collection("Brites_Editor_Meta").doc("setSubjectSynonyms")
      .set({ [subjectKey]: synonyms }, { merge: true })
      .catch(e => console.warn("synonym store write skipped:", e && e.message));
  }
  const searchWords = subject.concat(synonyms);
  const attempts = [
    searchWords.join(" OR "),                         // unscoped, OR across all vocabulary
    subject.join(" "),                                // unscoped AND (fallback)
    subject.map(w => `title:${w}*`).join(" "),        // scoped with wildcard
    subject.map(w => `title:${w}`).join(" ")          // scoped exact token
  ].filter((q, i, a) => a.indexOf(q) === i);
  let nodes = [], queryUsed = "";
  for (const q of attempts) {
    const d = await gql(`query($q: String!) {
      products(first: 100, query: $q) {
        nodes { id handle title featuredImage { url } metafield(namespace: "brites", key: "set") { value } }
      }
    }`, { q });
    nodes = ((d.products || {}).nodes) || [];
    queryUsed = q;
    if (nodes.length) break;
  }
  // Funnel counters — every empty result names its stage.
  const debug = { subject, synonyms, myForm, query: queryUsed, nodes: nodes.length,
    titleMatched: 0, otherForm: 0, withImage: 0,
    sampleNodes: nodes.slice(0, 5).map(n => `${n.handle} [${setFormFromTitle(n.title)}]`) };

  // Title matching only PROPOSES candidates — the vision check is what
  // decides truth. Multi-word subjects name the same charm inconsistently
  // across formats ("Citrus Fruit Necklace" vs "Citrus Charm Huggie"), so
  // requiring every word starves the judge. Any shared subject word
  // qualifies; candidates matching MORE words rank first so full matches
  // get judge slots ahead of partial ones.
  // SEMANTIC candidacy: rank every search hit by embedding similarity to
  // the charm subject. No plural rules, no stopword patches — "Dancers"
  // scores near "dancer", "Piggy Bank" near "pig", while "Chocolate Bar"
  // (a tag-recall accident) scores low and drops out. Word matching
  // survives only as the emergency fallback if the embeddings call fails.
  const pool = nodes.filter(n => n.handle !== product.handle);
  const subjectPhrase = subject.join(" ") + " charm jewelry";
  let titleMatched;
  try {
    const scores = await rankBySemantics(subjectPhrase, pool.map(n => String(n.title || "")));
    titleMatched = pool.map((n, i) => ({ ...n, sim: scores[i] }))
      .filter(n => n.sim >= 0.35)
      .sort((a, b) => b.sim - a.sim);
    debug.semantic = { threshold: 0.35,
      top: titleMatched.slice(0, 6).map(n => `${n.handle} ${n.sim.toFixed(2)}`),
      cut: pool.length - titleMatched.length };
  } catch (e) {
    console.warn("semantic ranking fallback to word match:", e && e.message);
    debug.semantic = { error: String((e && e.message) || e).slice(0, 120) };
    const wordRe = (w) => new RegExp(`\\b${w.replace(/[.*+?^$()|[\]\\]/g, "\\$&")}s?\\b`);
    titleMatched = pool
      .map(n => ({ ...n, matches: searchWords.reduce((k, w) => k + (wordRe(w).test(String(n.title || "").toLowerCase()) ? 1 : 0), 0) }))
      .filter(n => n.matches > 0)
      .sort((a, b) => b.matches - a.matches);
  }
  debug.titleMatched = titleMatched.length;
  const candidates = titleMatched
    .map(n => ({ ...n, form: setFormFromTitle(n.title) }))
    .filter(n => n.form && n.form !== myForm);
  debug.otherForm = candidates.length;

  // Judge up to 8 candidates (diverse forms first) — visual confirmation
  // decides what a "partner" is, exactly as in the Set Matcher pipeline.
  const byForm = {};
  for (const c of candidates) { (byForm[c.form] = byForm[c.form] || []).push(c); }
  const toJudge = [];
  for (const form of Object.keys(byForm)) toJudge.push(byForm[form][0]);
  for (const c of candidates) {
    if (toJudge.length >= 12) break;
    if (!toJudge.includes(c)) toJudge.push(c);
  }
  const judgeable = toJudge.filter(c => c.featuredImage && c.featuredImage.url);
  debug.withImage = judgeable.length;
  // Reference image: the charm CLOSE-UP (gallery slot 3 by catalog
  // convention) — on model shots the charm is tiny even at 2x zoom and the
  // model must guess its identity (measured: citrus read as "fruit with
  // leaves" one run and "slice disc" the next, admitting a kiwi/avocado).
  // Fall back to the first image when no third exists.
  const refUrl = (((p.images || [])[2]) || {}).src || (((p.images || [])[0]) || {}).src;
  debug.referenceImage = (((p.images || [])[2]) || {}).src ? "closeup (slot 3)" : "featured (slot 1)";
  if (!judgeable.length || !refUrl) return { partners: [], reason: refUrl ? "no judgeable candidates" : "no reference image", debug };

  const imgs = [{ handle: product.handle, url: refUrl, form: myForm }]
    .concat(judgeable.map(c => ({ handle: c.handle, url: c.featuredImage.url, form: c.form })));
  const { verdicts, zoomReport } = await judgeCharms(imgs);
  debug.zoom = zoomReport; // ok = images judged at template zoom (2x charm detail)
  if (!Array.isArray(verdicts)) throw new Error("charm judge returned no parseable verdict");
  // Mirror the verifier's gating: only an explicit same_charm true admits a
  // link (the background pruner removes on explicit false; for NEW links we
  // require positive confirmation — absence of a verdict is not a match).
  const confirmedHandles = new Set();
  const audit = [];
  verdicts.forEach((v, i) => {
    const cand = judgeable[(Number(v && v.photo) || (i + 2)) - 2];
    if (!cand) return;
    audit.push(`${cand.handle}: ${v.same_charm ? "MATCH" : "no"} (${v.confidence || "?"}) ${v.reason || ""}`.slice(0, 160));
    if (v.same_charm === true) confirmedHandles.add(cand.handle);
  });
  // The pair ledger is authoritative: a pair marked ok:false (nightly
  // verifier prune OR manual unlink) can never be re-admitted by a rerun.
  let vetoPairs = {};
  try {
    const ledger = await db.collection("Brites_Editor_Meta").doc("charmVerifyState").get();
    vetoPairs = ((ledger.exists && ledger.data()) || {}).pairs || {};
  } catch (e) { console.warn("pair ledger read skipped:", e && e.message); }
  const pairKeyOf = (a, b) => [a, b].sort().join("|").replace(/[.\/]/g, "_");
  const vetoed = (h) => { const v = vetoPairs[pairKeyOf(product.handle, h)]; return v && v.ok === false; };
  const partners = judgeable.filter(c => {
    if (!confirmedHandles.has(c.handle)) return false;
    if (vetoed(c.handle)) { audit.push(`${c.handle}: vetoed by pair ledger (previously pruned/unlinked)`); return false; }
    return true;
  }).slice(0, 8);
  if (!partners.length) return { partners: [], reason: "no visual matches", audit, debug };

  const myEntry = { h: product.handle, f: myForm, t: setShortTitle(p.title) };
  // MERGE with the product's existing set: previously confirmed partners are
  // kept (pruning is the nightly verifier's job, never a rerun's side
  // effect), newly confirmed ones append, deduped by handle, capped at 8.
  let existingSet = [];
  try { existingSet = JSON.parse((product.metafield && product.metafield.value) || "[]"); } catch (_) {}
  if (!Array.isArray(existingSet)) existingSet = [];
  existingSet = existingSet.filter(e => e && e.h && e.h !== product.handle);
  const mergedForward = existingSet.slice();
  for (const c of partners) {
    if (mergedForward.length >= 8) break;
    if (!mergedForward.some(e => e.h === c.handle)) {
      mergedForward.push({ h: c.handle, f: c.form, t: setShortTitle(c.title) });
    }
  }
  const metafields = [{
    ownerId: product.id, namespace: "brites", key: "set", type: "json",
    value: JSON.stringify(mergedForward)
  }];
  // Backward links: append the new product to each partner's set (dedup, cap 4).
  for (const p of partners) {
    let cur = [];
    try { cur = JSON.parse((p.metafield && p.metafield.value) || "[]"); } catch (_) {}
    if (!Array.isArray(cur)) cur = [];
    if (cur.some(e => e && e.h === myEntry.h) || cur.length >= 8) continue;
    metafields.push({
      ownerId: p.id, namespace: "brites", key: "set", type: "json",
      value: JSON.stringify(cur.concat([myEntry]))
    });
  }
  const r = await gql(`mutation($m: [MetafieldsSetInput!]!) {
    metafieldsSet(metafields: $m) { userErrors { field message } }
  }`, { m: metafields });
  const ue = ((r.metafieldsSet || {}).userErrors) || [];
  if (ue.length) throw new Error("set metafieldsSet: " + ue[0].message);

  // ---- Firebase: the storefront's live set source + the pipeline ledger ----
  // 1) Brites_Set_Links/{handle}: one doc per product holding its confirmed
  //    partners [{h,f,t}] — written for the new product AND every backlinked
  //    partner, so the site's Set section loads the fresh links immediately.
  // 2) Brites_Editor_Meta/charmVerifyState.pairs: the same pair-verdict
  //    ledger the verifier writes, so the console/CSV counts these links as
  //    CONFIRMED (visually verified here, same criteria) instead of pending.
  const fbWrites = [];
  const batch = db.batch();
  batch.set(db.collection("Brites_Set_Links").doc(product.handle), {
    partners: mergedForward, source: "upload-verified",
    updatedAt: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true });
  fbWrites.push(product.handle);
  for (const m of metafields.slice(1)) {
    const p = partners.find(x => x.id === m.ownerId);
    if (!p) continue;
    batch.set(db.collection("Brites_Set_Links").doc(p.handle), {
      partners: JSON.parse(m.value), source: "upload-verified",
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    fbWrites.push(p.handle);
  }
  const pairVerdicts = {};
  for (const p of partners) {
    const key = [product.handle, p.handle].sort().join("|").replace(/[.\/]/g, "_");
    const a = audit.find(l => l.startsWith(p.handle + ":")) || "";
    pairVerdicts["pairs." + key] = { ok: true, charm: "", reason: ("upload-verified: " + a).slice(0, 200) };
  }
  batch.set(db.collection("Brites_Editor_Meta").doc("charmVerifyState"), {}, { merge: true });
  await batch.commit();
  if (Object.keys(pairVerdicts).length) {
    await db.collection("Brites_Editor_Meta").doc("charmVerifyState").update(pairVerdicts)
      .catch(e => console.warn("charmVerifyState pair merge skipped:", e && e.message));
  }

  return {
    partners: mergedForward.map(e => ({ h: e.h, f: e.f })),
    newlyConfirmed: partners.map(c => ({ h: c.handle, f: c.form })),
    backLinked: metafields.length - 1, firebase: fbWrites, audit, debug
  };
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
  const prices = matrix.variants.map(v => v.price);
  const doc = {
    priceMin: Math.min.apply(null, prices),
    priceMax: Math.max.apply(null, prices),
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
  // FIX: `where("status","==") + orderBy("createdAtMs")` requires a Firestore
  // COMPOSITE index (equality filter + order on a different field), which
  // doesn't exist in this project — every drain died with
  // "9 FAILED_PRECONDITION: The query requires an index". A single-field
  // filter needs no composite index, so fetch a generous batch and FIFO-sort
  // in memory instead. The queue is small (daily budget caps it), so up to
  // 150 tiny docs per drain is negligible read cost — and zero console setup.
  const snap = await db.collection(QCOL).where("status", "==", "QUEUED").limit(150).get();
  const queuedDocs = snap.docs
    .slice()
    .sort((a, b) => (a.data().createdAtMs || 0) - (b.data().createdAtMs || 0))
    .slice(0, 30);
  for (const docSnap of queuedDocs) {
    if (Date.now() - t0 > DRAIN_TIME_BUDGET_MS) { out.stoppedFor = "time"; break; }
    const job = { id: docSnap.id, ...docSnap.data() };

    // CONCURRENCY GUARD: drains fire from four triggers (enqueue, the
    // 30-min in-app timer, the Drain button, retryFailed) and possibly
    // multiple open tabs. Two overlapping drains could both read this doc
    // as QUEUED and both create the product. Claim it QUEUED→UPLOADING in
    // a transaction; whoever loses the race skips the job entirely.
    const claimed = await db.runTransaction(async (tx) => {
      const fresh = await tx.get(docSnap.ref);
      if (!fresh.exists || fresh.data().status !== "QUEUED") return false;
      tx.set(docSnap.ref, { status: "UPLOADING", startedAtMs: Date.now() }, { merge: true });
      return true;
    });
    if (!claimed) { out.skipped++; continue; }

    const granted = await reserveBudget(job.variantCount);
    if (!granted) {
      // Give the job back to the queue — budget denial isn't a failure.
      await docSnap.ref.set({ status: "QUEUED", startedAtMs: null }, { merge: true });
      out.stoppedFor = "budget"; break;
    }
    try {
      const result = await createShopifyProduct(job);
      await docSnap.ref.set({
        status: "DONE", completedAtMs: Date.now(),
        productId: result.productId, handle: result.handle,
        variantsCreated: result.variantsCreated, addedCollections: result.addedCollections,
        collectionsActual: result.collectionsActual || [],
        expectedSmartCollections: result.expectedSmartCollections || [],
        expandedMeanings: job.expandedMeanings || null, aiPickedCollections: job.aiPickedCollections || null,
        mediaWarnings: job.mediaWarnings || null, publishWarnings: job.publishWarnings || null,
        collectionWarnings: job.collectionWarnings || null,
        setLinks: result.setLinks || null, setLinkWarnings: job.setLinkWarnings || null
      }, { merge: true });
      out.uploaded.push({ jobId: job.id, handle: result.handle, variants: result.variantsCreated,
        setLinks: result.setLinks || null });
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

// Last N jobs, newest first — everything the UI panel shows per listing:
// title, tier, variant count, price range, collections, live handle, error.
async function recentJobs(n) {
  const snap = await db.collection(QCOL).orderBy("createdAtMs", "desc").limit(n || 12).get();
  return snap.docs.map(d => {
    const j = d.data();
    return {
      id: d.id, status: j.status,
      title: (j.payload || {}).title || "",
      category: (j.payload || {}).category || "",
      tier: j.tier, variantCount: j.variantCount,
      priceMin: j.priceMin || null, priceMax: j.priceMax || null,
      handle: j.handle || null,
      addedCollections: j.addedCollections || [],
      aiPickedCollections: j.aiPickedCollections || [],
      collectionsActual: j.collectionsActual || [],
      expandedMeanings: (j.expandedMeanings || []).slice(0, 8),
      error: j.error || null,
      createdAtMs: j.createdAtMs || null, completedAtMs: j.completedAtMs || null
    };
  });
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
    if (op === "status") {
      const res = await status();
      if (q.recent) res.recent = await recentJobs(Number(q.recent) || 12);
      return out(200, res);
    }
    if (op === "retryFailed") { const r = await retryFailed(); const drained = await drain(); return out(200, { ...r, drained }); }
    if (op === "unlinkSet") {
      // Remove a WRONG set link in both directions and record ok:false in
      // the pair ledger so no future run can re-admit it.
      // Body: { op: "unlinkSet", handle, partner } (bare handles or URLs).
      const clean = (x) => String(x || "").trim()
        .replace(/^https?:\/\/[^\/]+/i, "").replace(/^\/?products\//i, "")
        .replace(/^\//, "").split(/[?#]/)[0].trim();
      const hA = clean(body.handle), hB = clean(body.partner);
      if (!hA || !hB) return out(400, { ok: false, error: "Need handle and partner" });
      const q = await gql(`query($a: String!, $b: String!) {
        a: productByHandle(handle: $a) { id handle metafield(namespace: "brites", key: "set") { value } }
        b: productByHandle(handle: $b) { id handle metafield(namespace: "brites", key: "set") { value } }
      }`, { a: hA, b: hB });
      if (!q.a || !q.b) return out(404, { ok: false, error: "Product not found: " + (!q.a ? hA : hB) });
      const parse = (n) => { try { const v = JSON.parse((n.metafield && n.metafield.value) || "[]"); return Array.isArray(v) ? v : []; } catch { return []; } };
      const aSet = parse(q.a).filter(e => e && e.h !== hB);
      const bSet = parse(q.b).filter(e => e && e.h !== hA);
      const r = await gql(`mutation($m: [MetafieldsSetInput!]!) {
        metafieldsSet(metafields: $m) { userErrors { field message } }
      }`, { m: [
        { ownerId: q.a.id, namespace: "brites", key: "set", type: "json", value: JSON.stringify(aSet) },
        { ownerId: q.b.id, namespace: "brites", key: "set", type: "json", value: JSON.stringify(bSet) }
      ]});
      const ue = ((r.metafieldsSet || {}).userErrors) || [];
      if (ue.length) return out(500, { ok: false, error: "metafieldsSet: " + ue[0].message });
      const batch = db.batch();
      batch.set(db.collection("Brites_Set_Links").doc(hA), { partners: aSet, updatedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
      batch.set(db.collection("Brites_Set_Links").doc(hB), { partners: bSet, updatedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
      await batch.commit();
      const key = [hA, hB].sort().join("|").replace(/[.\/]/g, "_");
      await db.collection("Brites_Editor_Meta").doc("charmVerifyState")
        .update({ ["pairs." + key]: { ok: false, charm: "", reason: "manual unlink" } })
        .catch(async () => {
          await db.collection("Brites_Editor_Meta").doc("charmVerifyState")
            .set({ pairs: { [key]: { ok: false, charm: "", reason: "manual unlink" } } }, { merge: true });
        });
      return out(200, { ok: true, unlinked: [hA, hB], remaining: { [hA]: aSet.length, [hB]: bSet.length } });
    }
    if (op === "relinkSet") {
      // Run set matching + vision verification + writes for an EXISTING
      // product (backlog tool, and recovery for uploads that predate the
      // set-linking feature). Body: { op: "relinkSet", handle }.
      // Accept a bare handle, a /products/... path, or a full product URL.
      const handle = String(body.handle || "").trim()
        .replace(/^https?:\/\/[^\/]+/i, "").replace(/^\/?products\//i, "")
        .replace(/^\//, "").split(/[?#]/)[0].trim();
      if (!handle) return out(400, { ok: false, error: "Missing handle" });
      const d = await gql(`query($h: String!) {
        productByHandle(handle: $h) { id handle title featuredImage { url } images(first: 3) { nodes { url } } metafield(namespace: "brites", key: "set") { value } }
      }`, { h: handle });
      const prod = d.productByHandle;
      if (!prod) return out(404, { ok: false, error: "No product with handle " + handle });
      if (!prod.featuredImage || !prod.featuredImage.url) {
        return out(422, { ok: false, error: "Product has no featured image to use as the reference" });
      }
      // Job-shaped input: title drives subject + form, featured image is
      // the vision reference (same framing convention as the catalog).
      const gallery = (((prod.images || {}).nodes) || []).map(n => ({ src: n.url }));
      const jobShaped = { payload: { title: prod.title, images: gallery.length ? gallery : [{ src: prod.featuredImage.url }] } };
      try {
        const setLinks = await ensureSetLinks(prod, jobShaped);
        return out(200, { ok: true, handle: prod.handle, setLinks });
      } catch (e) {
        return out(500, { ok: false, handle: prod.handle, error: String((e && e.message) || e) });
      }
    }
    return out(400, { error: "Unknown op: " + op });
  } catch (e) {
    return out(500, { error: String(e && e.message || e) });
  }
};
