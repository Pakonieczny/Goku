/*  netlify/functions/etsyMailListingCreator-background.js
 *
 *  Closes the Custom Brites sales loop end-to-end. Triggered by
 *  etsyMailListingCreatorCron when it claims a thread that the sales
 *  agent has flagged customerAccepted: true.
 *
 *  Flow per invocation:
 *    1. Load thread + draft, validate inputs, idempotency check
 *    2. Resolve the family's template listing (necklace / huggie / stud)
 *    3. Read template structural defaults (shipping_profile_id, taxonomy_id,
 *       readiness_state_id, etc.) from Etsy
 *    4. Generate title + description + tags with Claude
 *    5. Create a brand-new draft listing on Etsy
 *       (POST /shops/{shop}/listings?legacy=false  —  price in DECIMAL)
 *    6. Upload customer reference photos as listing images (multipart)
 *       Falls back to the template's image if the customer never sent one
 *    7. Set inventory: SKU, decimal price, quantity, readiness_state_id
 *       (PUT /listings/{id}/inventory?legacy=false  —  price in DECIMAL)
 *    8. Publish the listing (PATCH state=active, x-www-form-urlencoded)
 *    9. Send the live URL to the customer via etsyMailDraftSend.enqueue
 *   10. Write idempotency markers + audit row
 *
 *  Background functions on Netlify get a 15-minute timeout (vs 26s sync),
 *  which is plenty for ~12-15 Etsy API calls including 1-10 image uploads.
 *
 *  Spec source of truth: CUSTOM_LISTING_AUTOMATION_SPEC.md §4–§6
 */

"use strict";

const fetch     = require("node-fetch");
const FormData  = require("form-data");

const admin     = require("./firebaseAdmin");
const meter     = require("./_etsyApiMeter");
const {
  etsyFetch,
  getValidEtsyAccessToken,
  SHOP_ID
} = require("./_etsyMailEtsy");
const { callClaudeRaw } = require("./_etsyMailAnthropic");
const { requireExtensionAuth, CORS } = require("./_etsyMailAuth");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

// ─── constants ───────────────────────────────────────────────────────────

const THREADS_COLL  = "EtsyMail_Threads";
const DRAFTS_COLL   = "EtsyMail_Drafts";
const AUDIT_COLL    = "EtsyMail_Audit";
const CONFIG_COLL   = "EtsyMail_Config";
const SALES_COLL    = "EtsyMail_SalesContext";   // v4.3 — reset on completion
const TEMPLATES_DOC = "listingTemplates";

const ALLOWED_FAMILIES = ["necklace", "huggie", "stud"];
const MANUAL_LISTING_DEFAULT_PRICE_USD = Math.max(1, Number(process.env.ETSYMAIL_MANUAL_LISTING_DEFAULT_PRICE_USD || 1));

// v3.31: default Sonnet 4.6 (was Sonnet 4.6). Listing creation fires
// rarely (once per closed sale) but each call generates title +
// description + tags — three structured-text tasks Sonnet handles
// cleanly. The original sales-agent default chain remains as a
// secondary fallback so if you set ETSYMAIL_SALES_MODEL globally to
// (say) Opus you can opt this back into Opus without a code edit.
const AI_MODEL =
  process.env.ETSYMAIL_LISTING_CREATOR_MODEL ||
  process.env.ETSYMAIL_SALES_MODEL ||
  "claude-sonnet-4-6";

const MAX_IMAGES_PER_LISTING = 10;   // Etsy's hard cap

// ─── small helpers ───────────────────────────────────────────────────────

/** Resolve the deployed base URL for inter-function calls. Mirrors the
 *  pattern used by etsyMailAutoPipeline-background.js so dev / preview /
 *  production all work without code changes. */
function functionsBase() {
  return process.env.URL
      || process.env.DEPLOY_URL
      || process.env.NETLIFY_BASE_URL
      || "http://localhost:8888";
}

function pickFamily(thread) {
  // Prefer the explicit field if the sales agent wrote it (post-§7.6 deploy),
  // otherwise fall back to the family inside lastResolverResult. Defensive
  // because §2.1 of the spec says the agent writes acceptedQuoteFamily but
  // the §7.6 sales-agent change is described as "one-line"; the production-
  // ready interpretation writes both, so we try both here.
  const direct = String(thread.acceptedQuoteFamily || "").toLowerCase().trim();
  if (direct) return direct;
  const fromResolver = String(
    thread.lastResolverResult && thread.lastResolverResult.family || ""
  ).toLowerCase().trim();
  return fromResolver;
}

function clampStr(s, n) {
  return String(s == null ? "" : s).slice(0, n);
}


function positiveMoney(v) {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function firstPositiveMoney(...vals) {
  for (const v of vals) {
    const n = positiveMoney(v);
    if (n != null) return n;
  }
  return null;
}

function extractMoneyFromText(text) {
  const s = String(text || "");
  const matches = [...s.matchAll(/\$\s*([0-9]{1,5})(?:\.([0-9]{1,2}))?/g)];
  if (!matches.length) return null;
  // Use the last quoted amount in the conversation. In sales threads, the
  // latest dollar amount is normally the current offer the customer accepted.
  const m = matches[matches.length - 1];
  const dollars = Number(m[1]);
  const cents = m[2] ? Number("0." + m[2].padEnd(2, "0")) : 0;
  const total = dollars + cents;
  return Number.isFinite(total) && total > 0 ? total : null;
}

function normalizeManualFamily(v) {
  const f = String(v || "").toLowerCase().trim();
  if (ALLOWED_FAMILIES.includes(f)) return f;
  if (/\bstud\b|studs|post\s*earrings?/.test(f)) return "stud";
  if (/\bhuggie\b|huggies|hoops?|earrings?/.test(f)) return "huggie";
  if (/necklace|chain|pendant|charm/.test(f)) return "necklace";
  return "";
}

function inferFamilyFromConversation(text) {
  const s = String(text || "").toLowerCase();
  if (/\bstud\b|studs|post\s*earrings?/.test(s)) return "stud";
  if (/\bhuggie\b|huggies|hoops?|earrings?/.test(s)) return "huggie";
  if (/necklace|chain|pendant|charm/.test(s)) return "necklace";
  return "necklace";
}

function buildManualResolverResult({ family, priceUsd, sales, threadContext, inferredPrice }) {
  const existing = (sales && (sales.lastResolverResult || sales._lastResolverResult)) || null;
  if (existing && typeof existing === "object") {
    return {
      ...existing,
      success : existing.success !== false,
      family  : normalizeManualFamily(existing.family) || family,
      total   : positiveMoney(existing.total) || priceUsd,
      manualGenerated: true,
      manualFallback : true
    };
  }

  const brief = threadContext
    .map(m => String(m.text || "").trim())
    .filter(Boolean)
    .slice(-4)
    .join(" | ")
    .slice(0, 500);

  return {
    success: true,
    family,
    quantity: 1,
    total: priceUsd,
    subtotal: priceUsd,
    perPieceAfterModifier: priceUsd,
    bulkTier: null,
    rush: null,
    shippingSummary: null,
    escalations: [],
    manualGenerated: true,
    manualFallback: true,
    missingFieldsAllowed: true,
    priceWasInferred: !!inferredPrice,
    lineItems: [
      {
        code: "MANUAL-CUSTOM",
        label: brief || "Manual custom listing generated from the conversation",
        priceUsd,
        priceQuote: true,
        isModifier: false
      }
    ]
  };
}

// ─── 1. Load thread data ─────────────────────────────────────────────────

// ─── 1a. Manual listing fallback loader ─────────────────────────────────────

async function loadManualThreadData(threadId) {
  const threadRef = db.collection(THREADS_COLL).doc(threadId);
  const draftRef  = db.collection(DRAFTS_COLL).doc(`draft_${threadId}`);
  const salesRef  = db.collection(SALES_COLL).doc(threadId);

  const [threadSnap, draftSnap, salesSnap] = await Promise.all([
    threadRef.get(), draftRef.get(), salesRef.get()
  ]);
  if (!threadSnap.exists) throw new Error(`Thread not found: ${threadId}`);

  const thread = threadSnap.data() || {};
  const draft  = draftSnap.exists ? (draftSnap.data() || {}) : {};
  const sales  = salesSnap.exists ? (salesSnap.data() || {}) : {};
  const threadContext = await loadThreadContext(threadId);
  const contextText = [
    thread.subject,
    thread.snippet,
    thread.lastMessageText,
    draft.text,
    ...(threadContext || []).map(m => m && m.text)
  ].filter(Boolean).join("\n");

  const family =
    normalizeManualFamily(thread.acceptedQuoteFamily) ||
    normalizeManualFamily(thread.lastResolverResult && thread.lastResolverResult.family) ||
    normalizeManualFamily(sales.family) ||
    normalizeManualFamily(sales.knownFacts && sales.knownFacts.family) ||
    normalizeManualFamily(sales.lastResolverResult && sales.lastResolverResult.family) ||
    inferFamilyFromConversation(contextText);

  const inferredTextPrice = extractMoneyFromText(contextText);
  const priceUsd = firstPositiveMoney(
    thread.acceptedQuoteUsd,
    thread.lastResolverResult && thread.lastResolverResult.total,
    sales.totalQuotedUsd,
    sales.acceptedQuoteUsd,
    sales.knownFacts && sales.knownFacts.acceptedPriceUsd,
    sales.lastResolverResult && sales.lastResolverResult.total,
    sales._lastResolverResult && sales._lastResolverResult.total,
    inferredTextPrice,
    MANUAL_LISTING_DEFAULT_PRICE_USD
  );

  const lastResolverResult = thread.lastResolverResult ||
    buildManualResolverResult({ family, priceUsd, sales, threadContext, inferredPrice: inferredTextPrice != null });

  const preparedThread = {
    ...thread,
    customerAccepted   : true,
    acceptedQuoteUsd   : priceUsd,
    acceptedQuoteFamily: family,
    lastResolverResult,
    manualListingMode  : true
  };

  if (!preparedThread.etsyConversationUrl && /^etsy_conv_\d+$/.test(threadId)) {
    preparedThread.etsyConversationUrl = `https://www.etsy.com/your/conversations/${threadId.replace(/^etsy_conv_/, "")}`;
  }

  const referenceAttachments = await collectThreadImageAttachments(threadId, draft);

  await threadRef.set({
    acceptedQuoteUsd      : priceUsd,
    acceptedQuoteFamily   : family,
    lastResolverResult,
    customListingStatus   : "creating",
    customListingManualMode: true,
    customListingManualRequestedAt: FV.serverTimestamp(),
    updatedAt             : FV.serverTimestamp()
  }, { merge: true });

  return { thread: preparedThread, draft, referenceAttachments, family };
}

async function loadThreadData(threadId) {
  const threadRef = db.collection(THREADS_COLL).doc(threadId);
  const draftRef  = db.collection(DRAFTS_COLL).doc(`draft_${threadId}`);

  const [threadSnap, draftSnap] = await Promise.all([threadRef.get(), draftRef.get()]);
  if (!threadSnap.exists) throw new Error(`Thread not found: ${threadId}`);

  const thread = threadSnap.data();
  const draft  = draftSnap.exists ? draftSnap.data() : {};

  // Validate the bits we depend on. These are "invalid" errors on purpose
  // — isTerminalError() classifies them as terminal so we don't spin.
  if (!thread.customerAccepted)     throw new Error(`Thread not accepted (invalid input): ${threadId}`);

  // v5.30.1 — Salvage path for missing acceptedQuoteUsd.
  //
  // ROOT CAUSE: the sales agent (etsyMailSalesAgent-background.js)
  // historically wrote `acceptedQuoteUsd: null` if the customer's
  // acceptance turn didn't re-emit the price. Example: agent quotes
  // $42 on turn 3; customer replies "yes, let's do it" on turn 4; AI
  // parses customer_accepted=true on turn 4 but quoted_total_usd is
  // undefined (no number in the customer's "yes"). The agent wrote
  // null to acceptedQuoteUsd while still queuing customListingStatus,
  // and we landed here failing validation with "No accepted quote on
  // thread (invalid input)".
  //
  // RECOVERY: the most-recent resolver result IS the quote the
  // customer accepted (only resolver-produced quotes get sent to the
  // customer in the first place, and acceptance follows the last
  // quote). If thread.lastResolverResult has a positive total, adopt
  // it and persist back so subsequent code paths see consistent state.
  //
  // The salesAgent fix in v5.30.1 prevents new threads from entering
  // this state; this salvage path recovers threads already stuck.
  if (!thread.acceptedQuoteUsd) {
    const lrr = thread.lastResolverResult;
    const salvageTotal = (lrr && lrr.success && typeof lrr.total === "number" && lrr.total > 0)
      ? lrr.total
      : null;
    if (salvageTotal == null) {
      throw new Error(`No accepted quote on thread (invalid input): ${threadId}`);
    }
    console.warn(`[listingCreator] ${threadId}: acceptedQuoteUsd missing — salvaging $${salvageTotal} from lastResolverResult.total`);
    thread.acceptedQuoteUsd = salvageTotal;
    // Persist back so the inbox + future retries see consistent state.
    try {
      await threadRef.set({
        acceptedQuoteUsd        : salvageTotal,
        // Audit fields so it's obvious in Firestore what happened.
        acceptedQuoteSalvagedAt : FV.serverTimestamp(),
        acceptedQuoteSalvagedFrom: "lastResolverResult.total"
      }, { merge: true });
    } catch (e) {
      console.warn(`[listingCreator] ${threadId}: salvage write-back failed (non-fatal): ${e.message}`);
    }
  }

  if (!thread.lastResolverResult)   throw new Error(`No resolver result on thread (invalid input): ${threadId}`);
  if (!thread.etsyConversationUrl)  throw new Error(`No conversation URL on thread (invalid input): ${threadId}`);

  const family = pickFamily(thread);
  if (!family) throw new Error(`No family on thread (invalid input): ${threadId}`);
  if (!ALLOWED_FAMILIES.includes(family)) {
    throw new Error(`Unknown product family (invalid input): ${family}`);
  }

  // v4.3.3 — IMAGE COLLECTION OVERHAUL.
  //
  // Earlier versions trusted draft.referenceAttachments to carry the
  // customer's reference photos through to the worker. That path was
  // unreliable for two reasons:
  //
  //   1. The URLs in referenceAttachments are the original Etsy CDN
  //      URLs (i.etsystatic.com/...). These typically can't be fetched
  //      by a backend worker — Etsy returns 403/404 or HTML challenge
  //      pages for unauthenticated server-side requests. We were silently
  //      falling back to the template stock photo every time.
  //   2. Type-filtering on referenceAttachments dropped images whose
  //      contentType wasn't preserved through the scrape pipeline.
  //
  // The reliable source: when a customer sends a photo via Etsy
  // messaging, etsyMailMirrorImage uploads the binary into Firebase
  // Storage and writes the storage path onto the message document as
  // `storageImagePaths`. These are bytes we own — fetchable, durable,
  // independent of Etsy's CDN auth model.
  //
  // We now scan the entire thread's messages subcollection for inbound
  // messages with storageImagePaths, generate fresh signed URLs from
  // those storage paths (signed URLs only live 7 days; the original
  // mirror response's URL is long gone by the time the worker runs),
  // and feed those into the upload step.
  //
  // Fallback chain if no Storage-mirrored images:
  //   1. draft.referenceAttachments (legacy path — kept for older
  //      threads that pre-date the mirror integration; will likely fail
  //      to fetch but at least gives the worker something to try)
  //   2. Template listing's images (last-resort stock photo fallback —
  //      worker logs a warning when this happens)
  // v4.3.3 — Skip the Storage scan entirely on resume when images
  // were already uploaded in a prior worker run (customListingImagesAt
  // is set). The image-upload step itself short-circuits in that case
  // (resumeImagesAt branch in the main flow), so spending Storage calls
  // and signing URLs only to throw them away is pure waste — and adds
  // 1-3s to every resume invocation.
  const imagesAlreadyUploaded = !!thread.customListingImagesAt;
  const referenceAttachments = imagesAlreadyUploaded
    ? []
    : await collectThreadImageAttachments(threadId, draft);

  return { thread, draft, referenceAttachments, family };
}

/**
 * Scan messages collection for inbound messages with storageImagePaths,
 * generate signed URLs for each, return as attachment list with the
 * MOST RECENT customer reference image first (rank=1 on Etsy).
 *
 * Why most-recent-first: Etsy assigns rank=1 to the cover/primary photo.
 * In a sales conversation the customer's *latest* reference image
 * reflects their final intent (Joanna sent an initials photo first,
 * then changed her mind to a baseball — the baseball is what we want
 * as the primary). Earlier reference images are kept as secondary
 * photos for context.
 *
 * Returns [] if no mirrored images found OR if Storage isn't configured.
 * Caller falls back to legacy / template image in that case.
 */
async function collectThreadImageAttachments(threadId, draft) {
  // v4.3.3 — Wait briefly for the Chrome-extension mirror flow to
  // finish before scanning. Etsy's DOM scrape returns image URLs to
  // the extension, which then calls etsyMailMirrorImage once per image.
  // If the customer accepts within seconds of sending a photo (rare
  // but real — Joanna's case), the mirror calls may still be in flight
  // when this worker runs. Without the wait, we'd scan, see no
  // storageImagePaths, fall back to the template, and produce a
  // listing with the wrong primary photo.
  //
  // Strategy: scan once. If any inbound message has storageMirrorState
  // === "pending" AND has imageUrls, the mirror flow is in progress —
  // wait a few seconds and re-scan. Cap at three attempts (≈12s total)
  // so we never block the worker indefinitely on a stuck or failed
  // mirror.
  const MIRROR_WAIT_MS    = 4000;
  const MAX_MIRROR_WAITS  = 3;

  let attempt = 0;
  while (attempt <= MAX_MIRROR_WAITS) {
    const result = await scanMessagesForMirroredImages(threadId);

    // If we found any mirrored images, return them — don't keep waiting
    // even if some messages are still pending (partial coverage is
    // better than blocking).
    if (result.attachments.length > 0) return result.attachments;

    // No mirrored images found AND something is still pending? Wait.
    // Otherwise (no pending, no mirrored) fall through to legacy.
    if (!result.anyPending) break;

    if (attempt < MAX_MIRROR_WAITS) {
      console.log(`[listingCreator] mirror-pending detected for ${threadId}; waiting ${MIRROR_WAIT_MS}ms (attempt ${attempt + 1}/${MAX_MIRROR_WAITS})...`);
      await new Promise(r => setTimeout(r, MIRROR_WAIT_MS));
    }
    attempt++;
  }

  // Legacy fallback: try draft.referenceAttachments. May still fail at
  // upload time if URLs are unfetchable Etsy CDN, but at least we tried.
  console.warn(`[listingCreator] no mirrored images available for ${threadId} after ${attempt} attempt(s); falling back to draft.referenceAttachments`);
  const looksLikeImageUrl = (u) => /\.(jpe?g|png|webp|gif|bmp|tiff?|heic)(\?|$)/i.test(u || "");
  return Array.isArray(draft.referenceAttachments)
    ? draft.referenceAttachments.filter(a => {
        if (!a || !a.url) return false;
        if (a.type === "image" || /^image\//.test(a.type || "")) return true;
        if (looksLikeImageUrl(a.url)) return true;
        if (["latest_inbound", "thread_history", "sales_context"].includes(a.source)) return true;
        return false;
      })
    : [];
}

/**
 * Single scan over the thread's inbound messages: returns mirrored
 * attachments AND a flag indicating whether any inbound message has
 * pending mirror state (i.e. the extension hasn't finished yet).
 *
 * Used by collectThreadImageAttachments to decide whether to wait
 * and re-scan or to fall through to the legacy path.
 */
async function scanMessagesForMirroredImages(threadId) {
  try {
    // Use the same composite-index avoidance pattern as autoPipeline:
    // pull recent messages by timestamp DESC (single-field auto-index)
    // and filter direction in JS. Scanning newest-first means the first
    // mirrored image we encounter becomes rank 1 in the final list —
    // exactly the order Etsy displays photos in (most-recent customer
    // reference = primary photo).
    const msgsSnap = await db
      .collection(`${THREADS_COLL}/${threadId}/messages`)
      .orderBy("timestamp", "desc")
      .limit(200)   // sane cap; conversations rarely exceed this
      .get();

    if (msgsSnap.empty) {
      return { attachments: [], anyPending: false };
    }

    const bucket = admin.storage().bucket();
    const attachments = [];
    const seenPaths = new Set();
    let anyPending = false;

    for (const msgDoc of msgsSnap.docs) {
      if (attachments.length >= MAX_IMAGES_PER_LISTING) break;
      const m = msgDoc.data() || {};
      // Filter direction in JS — see comment above on index avoidance.
      if (m.direction !== "inbound") continue;

      // Track pending state — used by the caller to decide whether to
      // wait and retry. Pending = the message had imageUrls (so a
      // mirror was expected) but storageImagePaths is still empty.
      const hasImageUrls = Array.isArray(m.imageUrls) && m.imageUrls.length > 0;
      const paths        = Array.isArray(m.storageImagePaths) ? m.storageImagePaths : [];
      if (hasImageUrls && paths.length === 0 && m.storageMirrorState === "pending") {
        anyPending = true;
      }

      for (const p of paths) {
        if (attachments.length >= MAX_IMAGES_PER_LISTING) break;
        if (typeof p !== "string" || !p.trim() || seenPaths.has(p)) continue;
        seenPaths.add(p);

        // Generate a fresh signed URL. The URL returned by the mirror
        // function long ago (7-day signed URL) has likely expired and
        // wasn't persisted on the message anyway. Signing is local
        // (the SDK uses the service account's private key from
        // FIREBASE_PRIVATE_KEY env) — no IAM round-trip.
        try {
          const file = bucket.file(p);
          const [signedUrl] = await file.getSignedUrl({
            action : "read",
            expires: Date.now() + 60 * 60 * 1000   // 1 hour — well past upload step's runtime
          });
          const basename = p.split("/").pop() || `ref_${attachments.length + 1}.jpg`;
          attachments.push({
            url     : signedUrl,
            type    : "image",
            source  : "storage_mirror",
            filename: basename,
            storagePath: p,
            messageId: msgDoc.id,
            messageTs: m.timestamp && m.timestamp.toMillis ? m.timestamp.toMillis() : null
          });
        } catch (e) {
          console.warn(`[listingCreator] Signed URL failed for ${p}:`, e.message);
          // Continue — partial success is better than total failure.
        }
      }
    }

    if (attachments.length > 0) {
      console.log(`[listingCreator] gathered ${attachments.length} mirrored image(s) from thread ${threadId} messages (newest first)`);
    }
    return { attachments, anyPending };
  } catch (e) {
    console.warn(`[listingCreator] message scan failed for ${threadId}:`, e.message);
    return { attachments: [], anyPending: false };
  }
}

async function loadThreadContext(threadId) {
  // Last 10 messages, oldest first, for the description-generator prompt.
  const snap = await db.collection(`${THREADS_COLL}/${threadId}/messages`)
    .orderBy("timestamp", "desc")
    .limit(10)
    .get();
  return snap.docs.map(d => {
    const m = d.data();
    return {
      direction: m.direction || "unknown",
      text     : clampStr(m.text, 600)
    };
  }).reverse();
}

// ─── 2. Template resolution (one per family) ────────────────────────────

async function resolveTemplateListingId(family) {
  const cfg = await db.collection(CONFIG_COLL).doc(TEMPLATES_DOC).get();
  if (!cfg.exists) {
    throw new Error("Listing templates not configured (invalid setup): missing EtsyMail_Config/listingTemplates");
  }
  const entry = cfg.data()[family];
  // Accept either schema:
  //   1. Flat string: { necklace: "1094504461" }
  //      — written by the dashboard Settings UI (saveListingTemplates)
  //        and the recommended setup format from SETUP.md.
  //   2. Map: { necklace: { listingId: "1094504461", ... } }
  //      — older schema that supports per-family extra metadata. Kept
  //        for forward compat; nothing currently writes it.
  let listingId = null;
  if (typeof entry === "string") {
    listingId = entry.trim();
  } else if (entry && typeof entry === "object" && entry.listingId) {
    listingId = String(entry.listingId).trim();
  }
  if (!listingId) {
    throw new Error(`No template configured for family (invalid setup): ${family}`);
  }
  return listingId;
}

async function readTemplateListing(templateListingId) {
  // legacy=false so readiness_state_id appears in the inventory payload
  meter.bumpSimple("creator.templateListing");
  meter.bumpSimple("creator.templateInventory");
  const [listing, inventory] = await Promise.all([
    etsyFetch(`/listings/${templateListingId}`, { query: { legacy: false } }),
    etsyFetch(`/listings/${templateListingId}/inventory`, { query: { legacy: false } })
  ]);

  // Surface missing required fields with a clearer message than Etsy's
  // 400 ("listing.taxonomy_id: must not be null") — the operator sees
  // these in needsOperatorReviewReason on the thread doc.
  if (!listing.taxonomy_id) {
    throw new Error(`Template ${templateListingId} has no taxonomy_id (invalid setup)`);
  }
  if (!listing.shipping_profile_id) {
    throw new Error(`Template ${templateListingId} has no shipping_profile_id (invalid setup)`);
  }
  if (!listing.return_policy_id) {
    throw new Error(`Template ${templateListingId} has no return_policy_id (invalid setup)`);
  }

  // Walk the inventory to find any readiness_state_id (templates with a
  // processing profile expose it on every offering; we just need one).
  let readinessStateId = null;
  for (const p of (inventory.products || [])) {
    for (const o of (p.offerings || [])) {
      const n = Number(o.readiness_state_id);
      if (Number.isFinite(n) && n > 0) { readinessStateId = n; break; }
    }
    if (readinessStateId) break;
  }

  return {
    taxonomyId       : listing.taxonomy_id,
    shippingProfileId: listing.shipping_profile_id,
    returnPolicyId   : listing.return_policy_id,
    shopSectionId    : listing.shop_section_id  || null,
    whoMade          : listing.who_made   || "i_did",
    whenMade         : listing.when_made  || "made_to_order",
    isSupply         : !!listing.is_supply,
    materials        : Array.isArray(listing.materials) ? listing.materials : [],
    readinessStateId
  };
}

async function resolveReadinessStateIdFallback() {
  // Used only if the template doesn't have a readiness_state_id set.
  // Reads any existing processing profile from the shop. We never
  // auto-create one — that's an operator setup problem.
  meter.bumpSimple("creator.readinessDefs");
  const defs = await etsyFetch(`/shops/${SHOP_ID}/readiness-state-definitions`);
  const list = Array.isArray(defs.results) ? defs.results
            : Array.isArray(defs)         ? defs
            : [];
  if (!list.length) {
    throw new Error("No readiness_state_id available (invalid setup): configure a processing profile on the template listing");
  }
  const preferred = list.find(d => d.readiness_state === "ready_to_ship") || list[0];
  return Number(preferred.readiness_state_id);
}

// ─── 3. AI-generated title + description + tags ─────────────────────────

const TITLE_DESC_SYSTEM_PROMPT =
`You are generating Etsy listing content for a custom-order that a customer just accepted. Output JSON only with three fields: "title", "description", "tags".

CONSTRAINTS:
- title: max 140 characters, plain text, no emojis. Should be specific to this custom order (e.g., "Custom 10mm Sterling Silver Necklace Charm with Baseball Design, 16in Chain").
- description: 200-500 words, plain text, no markdown. Describe what was ordered (the spec), shipping/processing info, materials, customization details from the conversation. Mention this is a custom order made for a specific buyer.
- tags: array of up to 13 strings, each max 20 characters, lowercase, single words or short phrases (e.g., "custom necklace", "sterling silver", "baseball charm").

VOICE RULES:
- No em-dashes or en-dashes anywhere.
- "We" not "I" (Custom Brites is a shop, not a one-person operation).
- No service-script clichés ("absolutely", "happy to", "say the word", "lock it in").
- No specific timeframes for delivery.

Output ONLY valid JSON, no other text, no markdown fences.`;

function extractTextFromClaudeResponse(resp) {
  const blocks = Array.isArray(resp.content) ? resp.content : [];
  const out = [];
  for (const b of blocks) {
    if (b && b.type === "text" && typeof b.text === "string") out.push(b.text);
  }
  return out.join("").trim();
}

async function generateListingContent({ family, lastResolverResult, threadContext }) {
  const userPrompt =
`Product family: ${family}

Accepted spec (structured):
${JSON.stringify(lastResolverResult, null, 2)}

Recent conversation (last 10 messages, oldest first):
${threadContext.map(m => `[${m.direction}] ${m.text}`).join("\n")}

Output the JSON object now.`;

  const resp = await callClaudeRaw({
    model      : AI_MODEL,
    maxTokens  : 1500,
    system     : TITLE_DESC_SYSTEM_PROMPT,
    messages   : [{ role: "user", content: userPrompt }],
    useThinking: false   // single-shot generation, no need for thinking
  });

  const raw = extractTextFromClaudeResponse(resp);
  // Strip accidental markdown fences just in case
  const cleaned = raw
    .replace(/^```(?:json)?\s*/i, "")
    .replace(/\s*```\s*$/i, "")
    .trim();

  let parsed;
  try { parsed = JSON.parse(cleaned); }
  catch (e) {
    throw new Error(`Claude returned non-JSON listing content (invalid response): ${cleaned.slice(0, 200)}`);
  }

  // Validate + clamp to Etsy limits
  const title = clampStr(parsed.title, 140).trim();
  if (!title) throw new Error("Generated title is empty (invalid response)");

  const description = clampStr(parsed.description, 102400).trim();
  if (!description) throw new Error("Generated description is empty (invalid response)");

  // Etsy V3 tag rules: letters, numbers, hyphens, and spaces only;
  // ≤20 chars; ≤13 tags. We strip anything else so a stray "necklace!"
  // or "20%-off" doesn't get rejected at createDraftListing time.
  const tags = (Array.isArray(parsed.tags) ? parsed.tags : [])
    .map(t => String(t == null ? "" : t)
                .toLowerCase()
                .replace(/[^a-z0-9\- ]+/g, "")
                .replace(/\s+/g, " ")
                .trim()
                .slice(0, 20))
    .filter(Boolean)
    .slice(0, 13);

  return { title, description, tags };
}

// ─── 4. Create the draft listing ────────────────────────────────────────

async function createDraftListing({ title, description, tags, priceUsd, template }) {
  const priceDecimal = Number(priceUsd);
  if (!Number.isFinite(priceDecimal) || priceDecimal <= 0) {
    throw new Error(`Invalid price (invalid input): ${priceUsd}`);
  }

  let readinessStateId = template.readinessStateId;
  if (!readinessStateId) readinessStateId = await resolveReadinessStateIdFallback();

  const body = {
    title,
    description,
    // v4.3.2 — Etsy's createDraftListing (legacy=false) takes price as
    // a DECIMAL number (e.g. 42 for $42.00), NOT cents. Earlier code
    // sent priceCents (4200) which Etsy interpreted as $4,200.00. This
    // is well-documented: https://developer.etsy.com/documentation/tutorials/listings/
    // "change the price array in offerings to be a decimal value".
    price             : priceDecimal,
    quantity          : 1,
    who_made          : template.whoMade  || "i_did",
    when_made         : template.whenMade || "made_to_order",
    taxonomy_id       : template.taxonomyId,           // required, validated upstream
    shipping_profile_id: template.shippingProfileId,   // required, validated upstream
    return_policy_id  : template.returnPolicyId,       // required, validated upstream
    is_supply         : !!template.isSupply,
    materials         : template.materials || [],
    tags              : Array.isArray(tags) ? tags : [],
    is_personalizable : false,             // custom listings: details live in the description
    state             : "draft",
    should_auto_renew : false,
    readiness_state_id: readinessStateId
  };
  // shop_section_id is optional — only include if the template defines one
  if (template.shopSectionId) body.shop_section_id = template.shopSectionId;

  meter.bumpSimple("creator.createDraft");
  const created = await etsyFetch(
    `/shops/${SHOP_ID}/listings`,
    { method: "POST", query: { legacy: false }, body }
  );

  const listingId = created && (created.listing_id || created.results?.[0]?.listing_id);
  if (!listingId) {
    throw new Error(`createDraftListing returned no listing_id: ${JSON.stringify(created).slice(0, 300)}`);
  }
  return String(listingId);
}

// ─── 5. Upload reference photos ──────────────────────────────────────────

async function uploadOneImage({ accessToken, listingId, imageUrl, rank, altText, filename }) {
  const imgRes = await fetch(imageUrl);
  if (!imgRes.ok) {
    throw new Error(`Image fetch failed (${imgRes.status}): ${imageUrl}`);
  }
  const buf = Buffer.from(await imgRes.arrayBuffer());

  const form = new FormData();
  form.append("image", buf, { filename: filename || `ref_${rank}.jpg` });
  form.append("rank", String(rank));
  if (altText) form.append("alt_text", clampStr(altText, 250));

  const url =
    `https://api.etsy.com/v3/application/shops/${SHOP_ID}/listings/${encodeURIComponent(listingId)}/images`;

  // METER — this is a direct fetch (not via etsyFetch), so it has its own
  // full outcome tracking (not just attempt-tag).
  const _meterToken = meter.bump("creator.imageUpload");
  let res;
  try {
    res = await fetch(url, {
      method : "POST",
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "x-api-key"  : `${process.env.CLIENT_ID}:${process.env.CLIENT_SECRET || process.env.ETSY_SHARED_SECRET}`,
        ...form.getHeaders()
      },
      body: form
    });
  } catch (err) {
    _meterToken.failNet();
    throw err;
  }
  _meterToken.fromHttp(res.status);

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Etsy image upload ${res.status}: ${text.slice(0, 300)}`);
  }
}

async function getTemplateImageUrls(templateListingId) {
  meter.bumpSimple("creator.templateImages");
  const data = await etsyFetch(`/listings/${templateListingId}/images`);
  const results = Array.isArray(data.results) ? data.results : [];
  return results
    .map(r => ({
      url     : r.url_fullxfull || r.url_570xN || r.url_300x300,
      type    : "image",
      source  : "template",
      filename: `template_${r.rank || 1}.jpg`
    }))
    .filter(x => x.url);
}

async function uploadReferenceImages(listingId, referenceAttachments, templateListingId) {
  // CRITICAL: Etsy refuses to publish a listing with zero images. If the
  // customer never sent a photo, fall back to the template's image so the
  // listing is at least purchasable. Operator can hot-swap later.
  let images = referenceAttachments;
  let usedFallback = false;
  if (!images.length) {
    console.warn("[listingCreator] No reference photos. Using template image fallback.");
    images = await getTemplateImageUrls(templateListingId);
    usedFallback = true;
  }
  if (!images.length) {
    throw new Error("No images available (invalid setup): template has no images and customer sent none");
  }

  const queued = images.slice(0, MAX_IMAGES_PER_LISTING);
  let success = 0;
  let lastError = null;

  for (let i = 0; i < queued.length; i++) {
    const img  = queued[i];
    const rank = i + 1;
    try {
      // Refresh the token per-image so a long upload sequence (10 images,
      // each multi-MB) doesn't run out of token mid-loop. The token cache
      // makes this cheap when no refresh is needed.
      const accessToken = await getValidEtsyAccessToken();
      await uploadOneImage({
        accessToken,
        listingId,
        imageUrl: img.url,
        rank,
        altText : img.filename || "Custom order reference",
        filename: img.filename
      });
      success++;
    } catch (e) {
      lastError = e;
      console.error(`[listingCreator] Image upload failed (rank ${rank}):`, e.message);
      // Don't throw — partial-success > total-failure. We only abort if 0 succeed.
    }
  }

  if (success === 0) {
    throw new Error(`All image uploads failed: ${lastError ? lastError.message : "unknown"}`);
  }

  return { uploaded: success, attempted: queued.length, usedFallback };
}

// ─── 6. Set inventory (SKU + decimal price + readiness_state_id) ────────

function buildSku(thread, threadId) {
  const family = String(pickFamily(thread) || "x").charAt(0).toUpperCase();
  const price  = String(Math.round(Number(thread.acceptedQuoteUsd))).padStart(3, "0");
  const tail   = String(threadId).slice(-6);
  return `CUSTOM-${family}-${price}-${tail}`;
}

async function setInventory(listingId, { priceUsd, readinessStateId, sku }) {
  // Read current inventory (createDraftListing's `price` field already
  // initialized a single product/offering — we sanitize and add SKU).
  meter.bumpSimple("creator.inventoryGet");
  const inv = await etsyFetch(`/listings/${listingId}/inventory`, { query: { legacy: false } });
  const srcProducts = Array.isArray(inv.products) ? inv.products : [];
  if (!srcProducts.length) {
    throw new Error("Listing has no products (invalid state): createDraftListing did not initialize inventory");
  }

  // Money object on GET → decimal on PUT. updateListingInventory.js does
  // the same dance — that's the closest reference for this conversion.
  const toDecimal = (price) => {
    if (price == null) return Number(priceUsd);
    if (typeof price === "object" && price.amount != null) {
      const div = Number(price.divisor || 100);
      return Number(price.amount) / (div > 0 ? div : 100);
    }
    const n = Number(price);
    return Number.isFinite(n) ? n : Number(priceUsd);
  };

  const products = srcProducts.map(p => {
    const offerings = (Array.isArray(p.offerings) && p.offerings.length ? p.offerings : [{}]).map(o => {
      const decimal = toDecimal(o.price);
      const offering = {
        price      : Number(decimal.toFixed(2)),
        quantity   : 1,
        is_enabled : true
      };
      if (readinessStateId != null) offering.readiness_state_id = Number(readinessStateId);
      return offering;
    });

    return {
      sku            : sku,
      property_values: [],   // custom listings have no variations
      offerings
    };
  });

  meter.bumpSimple("creator.inventoryPut");
  await etsyFetch(`/listings/${listingId}/inventory`, {
    method: "PUT",
    query : { legacy: false },
    body  : { products }
  });
}

// ─── 7. Publish the listing ──────────────────────────────────────────────

async function publishListing(listingId) {
  // PATCH /shops/{shop_id}/listings/{id} expects x-www-form-urlencoded.
  // The shared etsyFetch() helper sends JSON, so we go direct here.
  // (Same approach used by updateListing.js in the reference project.)
  const accessToken = await getValidEtsyAccessToken();
  const url =
    `https://api.etsy.com/v3/application/shops/${SHOP_ID}/listings/${encodeURIComponent(listingId)}`;

  const form = new URLSearchParams();
  form.append("state", "active");

  // METER — direct fetch needs full outcome tracking.
  const _meterToken = meter.bump("creator.publishPatch");
  let res;
  try {
    res = await fetch(url, {
      method : "PATCH",
      headers: {
        Authorization : `Bearer ${accessToken}`,
        "x-api-key"   : `${process.env.CLIENT_ID}:${process.env.CLIENT_SECRET || process.env.ETSY_SHARED_SECRET}`,
        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
        Accept        : "application/json"
      },
      body: form.toString()
    });
  } catch (err) {
    _meterToken.failNet();
    throw err;
  }
  _meterToken.fromHttp(res.status);

  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text }; }

  if (!res.ok) {
    throw new Error(`publishListing ${res.status}: ${text.slice(0, 400)}`);
  }

  // The PATCH response usually includes `url`. If for some reason it doesn't,
  // do one extra GET — better than failing the whole flow on a missing field.
  if (!data.url) {
    try {
      meter.bumpSimple("creator.postPublishGet");
      const fresh = await etsyFetch(`/listings/${listingId}`);
      if (fresh && fresh.url) data.url = fresh.url;
    } catch (e) {
      console.warn("[listingCreator] post-publish GET to recover url failed:", e.message);
    }
  }

  if (!data.url) {
    throw new Error(`Published listing has no url field (invalid response): ${JSON.stringify(data).slice(0, 200)}`);
  }
  return data;
}

// ─── 8. Send the URL to the customer (via the existing send pipeline) ───

function buildListingDeliveryMessage(family, listingUrl) {
  // Voice rules per CUSTOM_LISTING_AUTOMATION_SPEC.md §6.7 / sales.md:
  //   - "we" not "I"
  //   - no em / en dashes
  //   - no specific timeframes
  //   - no service-script clichés ("absolutely", "happy to", "lock it in")
  //   - plain text, no markdown
  const f = ALLOWED_FAMILIES.includes(family) ? family : "custom";
  return (
    `Here's the custom listing for your ${f}: ${listingUrl}

Once you check out, we'll get to work on your order. Let us know if you have any questions.`
  );
}

async function sendListingUrlToCustomer({ threadId, etsyConversationUrl, listingUrl, family, listingId }) {
  // v4.3.17 — Build the exact customer-facing reply up front and persist
  // it onto the thread while staking the at-most-once claim. The inbox UI
  // uses these fields as a visible operator contract: the listing URL was
  // created and the same text is what auto-send / manual Send via Etsy will
  // deliver.
  const text = buildListingDeliveryMessage(family, listingUrl);
  const draftId = "draft_" + threadId;

  // v4.3.4 — AT-MOST-ONCE GUARD. Prevent duplicate sends to Etsy.
  //
  // The earlier worker-resume design (v4.3) made sendListingUrlToCustomer
  // safe to re-call IF the underlying enqueue path were truly idempotent —
  // i.e., it would refuse to re-send a draft that already went through.
  // It isn't: etsyMailDraftSend's enqueue path with `force: true`
  // overwrites a `sent` draft back to `queued`, and the extension's
  // send loop happily delivers it AGAIN. From the customer's view, two
  // identical "Here's the custom listing for your necklace..." messages
  // arrive. The bug surfaces in three real-world scenarios:
  //
  //   1. The worker runs once, succeeds, marks the thread terminal.
  //      Then the customer re-affirms ("yes, perfect!"). The agent
  //      classifies acceptance again, sets customListingStatus="queued",
  //      cron claims, worker re-runs, sendListingUrlToCustomer fires
  //      again, customer gets a duplicate.
  //   2. The operator manually retries by clearing customListingStatus
  //      back to "queued" in Firestore (e.g. for testing, or because
  //      the failure modes earlier in this conversation pushed them
  //      to retry by hand).
  //   3. The cron's stuck-sweep reclaims a "creating" thread that
  //      appeared wedged but had actually completed — the worker's
  //      terminal idempotency check in main() catches the customListing
  //      Status==="created" case BEFORE this function is called, but if
  //      that field was clobbered (re-acceptance writes "queued"), the
  //      check passes and we land here.
  //
  // The atomic claim below uses customListingSentAt as the source of
  // truth: the field is set ONCE on the first successful send and
  // never overwritten. A second invocation reads this field inside a
  // transaction, sees it set, and bails — no enqueue, no audit, no
  // customer-facing duplicate.
  const threadRef = db.collection(THREADS_COLL).doc(threadId);
  const claim = await db.runTransaction(async (tx) => {
    const snap = await tx.get(threadRef);
    if (!snap.exists) return { proceed: false, reason: "thread_missing" };
    const d = snap.data();
    if (d.customListingSentAt) {
      return {
        proceed     : false,
        reason      : "already_sent",
        sentAt      : d.customListingSentAt && d.customListingSentAt.toMillis ? d.customListingSentAt.toMillis() : null,
        sentListingId: d.customListingSentListingId || null
      };
    }
    // Stake our claim. Writing customListingSentAt BEFORE the actual
    // enqueue is intentional — a crash between this write and the
    // enqueue would leave the customer un-messaged. That's a recoverable
    // failure (operator can manually clear customListingSentAt and
    // retry) and is strictly preferable to its inverse: a duplicate
    // send to the customer, which is unrecoverable.
    tx.update(threadRef, {
      customListingSentAt        : FV.serverTimestamp(),
      customListingSentListingId : String(listingId),
      customListingReplyText     : text,
      customListingReplyDraftId  : draftId,
      customListingReplyStatus   : "queued",
      customListingReplyQueuedAt : FV.serverTimestamp(),
      updatedAt                  : FV.serverTimestamp()
    });
    return { proceed: true };
  });

  if (!claim.proceed) {
    console.log(`[listingCreator] sendListingUrlToCustomer skipped for ${threadId}: ${claim.reason}` +
      (claim.sentAt ? ` (sent ${new Date(claim.sentAt).toISOString()}, listing ${claim.sentListingId})` : ""));
    return { skipped: true, reason: claim.reason };
  }

  const res = await fetch(`${functionsBase()}/.netlify/functions/etsyMailDraftSend`, {
    method : "POST",
    headers: {
      "Content-Type": "application/json",
      // Forward the shared secret if it's set. enqueue itself doesn't
      // require it (per etsyMailDraftSend.js), but other ops do, and
      // including it is harmless.
      ...(process.env.ETSYMAIL_EXTENSION_SECRET
        ? { "X-EtsyMail-Secret": process.env.ETSYMAIL_EXTENSION_SECRET }
        : {})
    },
    body: JSON.stringify({
      op                  : "enqueue",
      threadId,
      etsyConversationUrl,
      text,
      employeeName        : "system:listing-creator",
      // The message text is a static template (built from buildListingDeliveryMessage),
      // not LLM output, so generatedByAI:false is the correct semantic.
      // The bg fn DOES use Claude for title/description/tags upstream, but
      // that's listing content — separate from the customer-facing message.
      // Recording the model anyway is useful forensics for "which deploy
      // generated this listing's content".
      aiMeta              : {
        generatedByAI: false,
        model        : AI_MODEL,
        source       : "listing_creator",
        listingId    : String(listingId),
        listingUrl,
        family
      },
      // Explicitly mark this as an automated listing-creator send. Do not
      // let etsyMailDraftSend infer "manual" just because generatedByAI is
      // false; the customer-facing text is template-generated, but the send
      // is still system-owned.
      sendOrigin          : "auto",
      // v4.3.4 — Was force:true, now force:false. The at-most-once
      // claim above is the authoritative guard against duplicate sends;
      // we don't need (and don't want) enqueue to overwrite a prior
      // queued/sent draft. If enqueue refuses because something is
      // already queued, we surface that as an error — better to fail
      // loudly than to clobber.
      force               : false
    })
  });

  const responseText = await res.text();
  if (!res.ok) {
    // Roll back the claim so a manual retry can re-send. We've not
    // actually delivered to the customer; nothing else has read
    // customListingSentAt yet (it's been milliseconds), so clearing
    // is safe.
    try {
      await threadRef.update({
        customListingSentAt        : FV.delete(),
        customListingSentListingId : FV.delete(),
        customListingReplyStatus   : FV.delete(),
        customListingReplyQueuedAt : FV.delete(),
        customListingReplyEnqueuedAt: FV.delete(),
        updatedAt                  : FV.serverTimestamp()
      });
    } catch (e) {
      console.warn(`[listingCreator] sentAt rollback failed (non-fatal): ${e.message}`);
    }
    throw new Error(`enqueue send failed (${res.status}): ${responseText.slice(0, 300)}`);
  }

  try {
    let enqueueJson = null;
    try { enqueueJson = responseText ? JSON.parse(responseText) : null; } catch {}
    await threadRef.set({
      customListingReplyText      : text,
      customListingReplyDraftId   : (enqueueJson && enqueueJson.draftId) || draftId,
      customListingReplyStatus    : "queued",
      customListingReplyEnqueuedAt: FV.serverTimestamp(),
      updatedAt                   : FV.serverTimestamp()
    }, { merge: true });
  } catch (e) {
    console.warn(`[listingCreator] reply visibility write-back failed (non-fatal): ${e.message}`);
  }

  return { skipped: false, text, draftId };
}

// ─── 9. Idempotency markers ──────────────────────────────────────────────

async function markManualSuccess({ threadId, listingId, listingUrl, generated, imagesUploaded, salesSynopsis, family, priceUsd }) {
  const threadRef = db.collection(THREADS_COLL).doc(threadId);
  const draftId = "draft_" + threadId;
  const replyText = buildListingDeliveryMessage(family, listingUrl);

  await Promise.all([
    threadRef.set({
      customListingId          : String(listingId),
      customListingUrl         : listingUrl,
      customListingCreatedAt   : FV.serverTimestamp(),
      customListingStatus      : "created",
      customListingManualMode  : true,
      customListingManualCreatedAt: FV.serverTimestamp(),
      customListingReplyText   : replyText,
      customListingReplyDraftId: draftId,
      customListingReplyStatus : "draft_ready",
      customListingError       : FV.delete(),
      customListingErrorAt     : FV.delete(),
      customListingErrorCount  : FV.delete(),
      acceptedQuoteUsd         : priceUsd,
      acceptedQuoteFamily      : family,
      salesSynopsis            : salesSynopsis || buildStructuredSalesSynopsis({ thread: { acceptedQuoteUsd: priceUsd, acceptedQuoteFamily: family }, family, listingId, listingUrl }),
      needsOperatorReview      : false,
      needsOperatorReviewReason: null,
      updatedAt                : FV.serverTimestamp()
    }, { merge: true }),
    db.collection(DRAFTS_COLL).doc(draftId).set({
      draftId,
      threadId,
      status                  : "draft",
      text                    : replyText,
      generatedByAI           : false,
      generatedBySalesAgent   : false,
      customListingManualMode : true,
      customListingId         : String(listingId),
      customListingUrl        : listingUrl,
      customListingReplyStatus: "draft_ready",
      aiMeta: {
        generatedByAI: false,
        source       : "manual_listing_creator",
        listingId    : String(listingId),
        listingUrl,
        family
      },
      updatedAt               : FV.serverTimestamp(),
      createdAt               : FV.serverTimestamp()
    }, { merge: true })
  ]);

  await db.collection(AUDIT_COLL).add({
    threadId,
    eventType : "custom_listing_manual_created",
    actor     : "operator:manual-listing-button",
    payload   : {
      listingId      : String(listingId),
      listingUrl,
      priceUsd,
      family,
      imagesUploaded,
      titlePreview   : generated ? clampStr(generated.title, 140) : "(resumed — no fresh title)"
    },
    createdAt : FV.serverTimestamp()
  });
}

async function markSuccess({ threadId, listingId, listingUrl, generated, imagesUploaded, salesSynopsis, isResume }) {
  const threadRef = db.collection(THREADS_COLL).doc(threadId);

  await threadRef.update({
    // Listing-pipeline fields
    customListingId         : String(listingId),
    customListingUrl        : listingUrl,
    customListingCreatedAt  : FV.serverTimestamp(),
    customListingStatus     : "created",
    customListingReplyStatus: "queued",
    customListingError      : FV.delete(),
    customListingErrorAt    : FV.delete(),
    customListingErrorCount : FV.delete(),

    // v4.3 — TERMINAL SALES STATUS. The sales agent treats "sales_completed"
    // as a terminal status (TERMINAL_THREAD_STATUSES in the agent code) and
    // skips processing on threads in this state. The dashboard's "Completed
    // Sales" menu queries threads where status == "sales_completed".
    status                  : "sales_completed",
    salesCompletedAt        : FV.serverTimestamp(),

    // v4.3 — Operator-facing synopsis. Generated at completion so the next
    // person reviewing the thread sees the full sale at a glance instead of
    // scrolling the entire conversation. Falls back to a structured summary
    // if the Claude call fails — markSuccess never blocks on synopsis.
    salesSynopsis           : salesSynopsis,

    needsOperatorReview     : false,
    needsOperatorReviewReason: null,
    updatedAt               : FV.serverTimestamp()
  });

  // v4.3 — RESET SalesContext.stage so a future inbound on this thread
  // (e.g. "thanks, when will it ship?") doesn't re-route to the sales
  // agent. The autoPipeline's path (a) STATEFUL keys off
  // ACTIVE_SALES_STAGES = {discovery, spec, quote, revision,
  // pending_close_approval}. We set stage to "completed" — outside that
  // set — so loadActiveSalesContextStage returns null, and the follow-up
  // falls through to the regular customer-service draft pipeline. The
  // rest of SalesContext (accumulated spec, quote history) is preserved
  // for forensic value.
  //
  // Wrapped in try/catch because a SalesContext write failure shouldn't
  // unwind the whole completion — the listing is live, the customer has
  // the URL, the synopsis is saved. At worst the operator sees one stray
  // sales-agent reply on a follow-up.
  try {
    await db.collection(SALES_COLL).doc(threadId).set({
      stage           : "completed",
      stageCompletedAt: FV.serverTimestamp(),
      completionReason: "sale_closed",
      completedListingId: String(listingId),
      updatedAt       : FV.serverTimestamp()
    }, { merge: true });
  } catch (e) {
    console.warn(`[listingCreator] SalesContext reset failed for ${threadId} (non-fatal):`, e.message);
  }

  await db.collection(AUDIT_COLL).add({
    threadId,
    eventType : "custom_listing_created",
    actor     : "listing-creator",
    payload   : {
      listingId      : String(listingId),
      listingUrl,
      titlePreview   : generated ? clampStr(generated.title, 140) : "(resumed — no fresh title)",
      imagesUploaded : imagesUploaded.uploaded,
      imagesAttempted: imagesUploaded.attempted,
      usedTemplateImageFallback: imagesUploaded.usedFallback,
      resumed        : !!isResume,
      synopsisChars  : (salesSynopsis || "").length
    },
    createdAt: FV.serverTimestamp()
  }).catch(e => console.warn("[listingCreator] audit write failed (non-fatal):", e.message));
}

// ─── 9b. Mid-flow persistence (resumability across crashes) ─────────────

/** Persist customListingId IMMEDIATELY after Etsy creates the draft. The
 *  reason: every subsequent step (image upload, inventory, publish, send)
 *  can take seconds-to-minutes, and any crash in that window leaves a
 *  draft listing on Etsy that the next retry can't match to its source
 *  thread. Once this write lands, the worker's resume path on the next
 *  attempt sees customListingId populated and skips createDraftListing.
 *
 *  Trade-off: if THIS Firestore write fails, we still have an orphan
 *  draft on Etsy (the listing exists, the ID isn't saved). That's the
 *  same failure mode we had before this fix — no regression. The win
 *  is that for the much more common Etsy-API-call-fails-mid-flow case,
 *  we no longer create a second listing on retry. */
async function persistListingIdEarly(threadId, listingId) {
  await db.collection(THREADS_COLL).doc(threadId).update({
    customListingId            : String(listingId),
    customListingDraftCreatedAt: FV.serverTimestamp(),
    updatedAt                  : FV.serverTimestamp()
  });
}

/** Persist that image upload completed. Used by the resume path so a
 *  retry doesn't re-upload duplicates onto an existing draft listing. */
async function persistImagesUploaded(threadId, imagesUploaded) {
  await db.collection(THREADS_COLL).doc(threadId).update({
    customListingImagesAt     : FV.serverTimestamp(),
    customListingImagesCount  : Number(imagesUploaded.uploaded || 0),
    customListingUsedFallback : !!imagesUploaded.usedFallback,
    updatedAt                 : FV.serverTimestamp()
  });
}

// ─── 9c. Sales synopsis (operator-facing summary at completion) ─────────

const SYNOPSIS_SYSTEM_PROMPT =
`You are writing a compact operator handoff after a custom Etsy listing was created.

Output 3 to 5 bullet points only. No paragraphs. No preamble. Each bullet must be short and operational.

Include only:
• final item/spec
• price and product family
• listing status/link or listing id
• reference images or special production notes, only if relevant
• one follow-up caution, only if needed

Do not retell the full conversation. Do not include customer-service script language. Do not mention shipping timelines.`;

function buildStructuredSalesSynopsis({ thread, family, listingId, listingUrl }) {
  const lr = thread.lastResolverResult || {};
  const price = thread.acceptedQuoteUsd || lr.total || lr.totalUsd || null;
  const items = Array.isArray(lr.lineItems) ? lr.lineItems : [];
  const itemText = items.length
    ? items.map(li => li.label || li.name || li.description || li.code || JSON.stringify(li)).filter(Boolean).join("; ")
    : (thread.acceptedQuoteFamily || family || "custom order");
  const photoCount = thread.customListingImagesCount;

  const bullets = [];
  bullets.push(`• Final spec: ${clampStr(itemText || "custom order", 180)}`);
  bullets.push(`• Price/family: ${price ? `$${price}` : "price on listing"}${family ? ` · ${family}` : ""}`);
  bullets.push(`• Listing: ${listingUrl || (listingId ? `Etsy listing ${listingId}` : "created")}`);
  if (photoCount != null) bullets.push(`• Reference images: ${photoCount}`);
  bullets.push("• Follow-up: review listing details before production if the order has manual or inferred specs.");
  return bullets.slice(0, 5).join("\n");
}

function compactSynopsisText(text, fallback) {
  const raw = String(text || "").trim();
  const source = raw || String(fallback || "").trim();
  if (!source) return "";

  const lines = source
    .split(/\n+/)
    .map(line => line.trim())
    .filter(Boolean)
    .map(line => line.replace(/^[•\-*]\s*/, ""))
    .filter(Boolean);

  const useful = lines.length > 1
    ? lines
    : source.split(/(?<=[.!?])\s+/).map(x => x.trim()).filter(Boolean);

  return useful.slice(0, 5).map(line => `• ${clampStr(line.replace(/\s+/g, " "), 180)}`).join("\n");
}

async function generateSalesSynopsis({ thread, family, listingId, listingUrl, fullThreadContext }) {
  const customer = thread.customerName || thread.buyerName || thread.etsyUsername || "the customer";
  const fallback = buildStructuredSalesSynopsis({ thread, family, listingId, listingUrl });
  const userPrompt =
`Customer: ${customer}
Etsy username: ${thread.etsyUsername || "n/a"}
Conversation subject: ${thread.subject || "n/a"}

Product family: ${family}
Final accepted price (USD): ${thread.acceptedQuoteUsd}
Listing created: ${listingUrl} (id ${listingId})
Reference photos provided: ${(thread.customListingImagesCount != null) ? thread.customListingImagesCount : "n/a"}

Accepted spec (structured):
${JSON.stringify(thread.lastResolverResult || {}, null, 2)}

Full conversation (oldest first, last 30 messages):
${fullThreadContext.map(m => `[${m.direction}] ${m.text}`).join("\n")}

Write the compact bullet handoff now.`;

  try {
    const resp = await callClaudeRaw({
      model      : AI_MODEL,
      maxTokens  : 320,
      system     : SYNOPSIS_SYSTEM_PROMPT,
      messages   : [{ role: "user", content: userPrompt }],
      useThinking: false
    });
    const text = extractTextFromClaudeResponse(resp).trim();
    if (text) return compactSynopsisText(text, fallback);
  } catch (e) {
    console.warn("[listingCreator] synopsis generation failed:", e.message);
  }

  // Fallback: compact structured handoff. Synopsis should never block listing
  // completion, and it should never become a full conversation narrative.
  return fallback;
}

/** Wider context for the synopsis (last 30 messages, oldest first).
 *  Distinct from loadThreadContext, which only pulls 10 — that's tuned
 *  for the listing-content prompt, not a complete conversation summary. */
async function loadFullThreadContext(threadId) {
  const snap = await db.collection(`${THREADS_COLL}/${threadId}/messages`)
    .orderBy("timestamp", "desc")
    .limit(30)
    .get();
  return snap.docs.map(d => {
    const m = d.data();
    return {
      direction: m.direction || "unknown",
      text     : clampStr(m.text, 800)
    };
  }).reverse();
}

// ─── 10. Failure tracking ────────────────────────────────────────────────

function isTerminalError(err) {
  const msg = String((err && err.message) || err).toLowerCase();
  // Retryable: transient network and rate-limit signals
  if (msg.includes("etimedout") || msg.includes("econnreset") || msg.includes("enotfound")) return false;
  if (msg.includes("rate limit") || msg.includes(" 429") || msg.includes("429:")) return false;
  if (msg.includes(" 502") || msg.includes(" 503") || msg.includes(" 504")) return false;
  // Terminal: validation, bad input, auth, missing config
  if (msg.includes("invalid input") || msg.includes("invalid setup") || msg.includes("invalid response")) return true;
  if (msg.includes("invalid state")) return true;
  if (msg.includes(" 401") || msg.includes(" 403") || msg.includes("not found")) return true;
  // Default: terminal — better to escalate than burn API quota in a loop.
  return true;
}

async function markFailure({ threadId, err }) {
  const errMsg = clampStr((err && err.message) || err, 500);
  const terminal = isTerminalError(err);
  console.error(`[listingCreator] failed ${threadId}: ${errMsg}`, err && err.stack ? err.stack.split("\n").slice(0, 5).join("\n") : "");

  try {
    await db.collection(THREADS_COLL).doc(threadId).update({
      // "queued" → eligible for cron retry on the next tick.
      // "failed" → stuck pending operator (needsOperatorReview=true).
      // We don't use null for retryable — the cron's primary query keys
      // off customListingStatus == "queued", and using null would orphan
      // the thread out of the indexed query path.
      customListingStatus      : terminal ? "failed" : "queued",
      customListingError       : errMsg,
      customListingErrorAt     : FV.serverTimestamp(),
      customListingErrorCount  : FV.increment(1),

      // v4.3 — CRITICAL: clear customListingStartedAt so the cron's
      // tryClaim can distinguish "worker exited with error" from
      // "worker still running". Without this clear, a re-acceptance
      // turn that overwrites customListingStatus from "creating" back
      // to "queued" while the worker is mid-flow would race with the
      // cron's claim — see SETUP.md "race protection". With this
      // clear, a fresh startedAt unambiguously means "worker still
      // in-flight", and the cron's "queued"-path freshness check
      // safely skips.
      customListingStartedAt   : FV.delete(),

      needsOperatorReview      : terminal,
      needsOperatorReviewReason: terminal ? `listing_creation_failed: ${errMsg}` : null,
      updatedAt                : FV.serverTimestamp()
    });
  } catch (e) {
    console.error("[listingCreator] markFailure write failed:", e.message);
  }

  try {
    await db.collection(AUDIT_COLL).add({
      threadId,
      eventType: "custom_listing_failed",
      actor    : "listing-creator",
      payload  : { error: errMsg, terminal },
      createdAt: FV.serverTimestamp()
    });
  } catch (e) {
    console.warn("[listingCreator] audit write failed (non-fatal):", e.message);
  }
}

// ─── handler ─────────────────────────────────────────────────────────────

exports.handler = meter.wrapHandler(async function (event) {
  const tStart = Date.now();
  let threadId = null;

  // CORS preflight (consistency with the rest of the codebase — even
  // though browsers shouldn't be hitting this endpoint, the Chrome
  // extension and inbox UI assume CORS is enabled on every fn).
  if (event && event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  // Auth: require the shared extension secret. The cron forwards it via
  // X-EtsyMail-Secret. Operators invoking manually for debug also need
  // it. Without this, anyone who knows or guesses a thread id could
  // POST here and force-trigger listing creation. requireExtensionAuth
  // fails closed in production but warns-only in dev/preview.
  const auth = requireExtensionAuth(event || {});
  if (!auth.ok) return auth.response;

  try {
    const body = event && event.body ? JSON.parse(event.body) : {};
    const manualMode = body.manual === true || body.manualCreate === true || body.mode === "manual";
    threadId = String(body.threadId || "").trim();
    const validThreadId = manualMode
      ? /^etsy_conv_[A-Za-z0-9_-]+$/.test(threadId)
      : /^etsy_conv_\d+$/.test(threadId);
    if (!threadId || !validThreadId) {
      return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: "Invalid threadId" }) };
    }

    // 1. Load thread + draft + family. Manual mode intentionally accepts
    // incomplete sales context and fills the minimum Etsy-required fields
    // from the conversation/SalesContext so the operator can edit the live
    // listing afterward.
    const { thread, referenceAttachments, family } = manualMode
      ? await loadManualThreadData(threadId)
      : await loadThreadData(threadId);

    const attempts = Number(thread.customListingAttempts || 0);
    if (attempts > 1) {
      console.warn(
        `[listingCreator] Retry detected for ${threadId} (attempt #${attempts}, ` +
        `errorCount=${Number(thread.customListingErrorCount || 0)}).`
      );
    }

    // 2. TERMINAL idempotency check. If the thread is already in the
    //    "sales_completed" terminal status, the listing is fully done.
    //    Bail safely (defense in depth — cron's tryClaim should have
    //    cleaned up before firing us, but if we got here anyway, don't
    //    re-process). customListingStatus="created" is the same signal
    //    in the listing-pipeline state machine.
    if (!manualMode && (thread.status === "sales_completed" || thread.customListingStatus === "created")) {
      console.log(`[listingCreator] thread ${threadId} already terminal (status=${thread.status}, customListingStatus=${thread.customListingStatus}), bailing.`);
      // Best-effort: align customListingStatus to "created" if it drifted.
      if (thread.customListingStatus !== "created") {
        await db.collection(THREADS_COLL).doc(threadId).update({
          customListingStatus: "created",
          updatedAt          : FV.serverTimestamp()
        }).catch(() => { /* non-fatal */ });
      }
      return {
        statusCode: 200,
        headers   : CORS,
        body: JSON.stringify({ ok: true, alreadyCreated: true, listingId: thread.customListingId || null })
      };
    }

    // 3. RESUME detection. v4.3 — customListingId is now persisted right
    //    after createDraftListing succeeds, so a retry can resume from
    //    where it left off instead of creating a duplicate listing.
    //
    //    State signals:
    //      thread.customListingId          → step 5 (createDraftListing) done
    //      thread.customListingImagesAt    → step 6 (uploadReferenceImages) done
    //
    //    Steps 7 (inventory PUT), 8 (publish PATCH), and 9 (enqueue send)
    //    are idempotent on Etsy's side (PUT replaces, PATCH state=active
    //    is no-op if already active, enqueue uses deterministic draftId
    //    with force=true). So we always run those — no resume sentinel
    //    needed for them.
    const resumeListingId = thread.customListingId ? String(thread.customListingId) : null;
    const resumeImagesAt  = thread.customListingImagesAt || null;
    const isResume        = !!resumeListingId;

    if (isResume) {
      console.warn(
        `[listingCreator] Resuming for ${threadId}: ` +
        `listingId=${resumeListingId}, imagesAlreadyUploaded=${!!resumeImagesAt}. ` +
        `Skipping createDraftListing${resumeImagesAt ? " and uploadReferenceImages" : ""}.`
      );
    }

    // 4. Resolve template (always — needed for inventory readiness_state_id
    //    and as the image fallback source).
    const templateListingId = await resolveTemplateListingId(family);
    const template = await readTemplateListing(templateListingId);

    // 5. Generate AI listing content + create draft (skipped on resume).
    let newListingId, generated;
    if (isResume) {
      newListingId = resumeListingId;
      generated    = null;   // not needed downstream — only the title was used (for audit)
    } else {
      const threadContext = await loadThreadContext(threadId);
      generated = await generateListingContent({
        family,
        lastResolverResult: thread.lastResolverResult,
        threadContext
      });
      newListingId = await createDraftListing({
        title      : generated.title,
        description: generated.description,
        tags       : generated.tags,
        priceUsd   : thread.acceptedQuoteUsd,
        template
      });
      console.log(`[listingCreator] draft created for ${threadId}: listing=${newListingId}`);

      // CRITICAL — persist the listing id immediately. If anything below
      // fails or crashes, the next retry sees customListingId set and
      // resumes instead of creating a duplicate.
      await persistListingIdEarly(threadId, newListingId);
    }

    // 6. Upload reference photos (skipped on resume if already done).
    let imagesUploaded;
    if (resumeImagesAt) {
      imagesUploaded = {
        uploaded    : Number(thread.customListingImagesCount || 0),
        attempted   : Number(thread.customListingImagesCount || 0),
        usedFallback: !!thread.customListingUsedFallback,
        resumed     : true
      };
    } else {
      imagesUploaded = await uploadReferenceImages(
        newListingId,
        referenceAttachments,
        templateListingId
      );
      // Mark images done so a later crash doesn't re-upload duplicates.
      await persistImagesUploaded(threadId, imagesUploaded);
    }

    // 7. Set inventory (idempotent — always run).
    const sku = buildSku(thread, threadId);
    let readinessStateId = template.readinessStateId;
    if (!readinessStateId) readinessStateId = await resolveReadinessStateIdFallback();
    await setInventory(newListingId, {
      priceUsd        : thread.acceptedQuoteUsd,
      readinessStateId,
      sku
    });

    // 8. Publish (idempotent — PATCH state=active is a no-op if already active).
    const published   = await publishListing(newListingId);
    const listingUrl  = published.url;
    console.log(`[listingCreator] published ${threadId}: ${listingUrl}`);

    // 9 + 9b. v4.3.2 — Send the URL to the customer AND generate the
    // sales synopsis IN PARALLEL. They're independent operations:
    //   - sendListingUrlToCustomer just enqueues a draft via
    //     etsyMailDraftSend; the customer receives it via the Chrome
    //     extension's send loop.
    //   - generateSalesSynopsis is an LLM call that produces operator-
    //     facing summary text.
    // Running them serially added 5-15s for the synopsis call after the
    // customer message was already sent. Now they're awaited together.
    //
    // Both must succeed (or the synopsis fall back to its structured
    // template) before markSuccess writes the completion state.

    const freshThread = (await db.collection(THREADS_COLL).doc(threadId).get()).data() || thread;
    const fullThreadContext = await loadFullThreadContext(threadId);

    let salesSynopsis;
    if (manualMode) {
      salesSynopsis = await generateSalesSynopsis({
        thread: freshThread, family, listingId: newListingId, listingUrl, fullThreadContext
      });
      await markManualSuccess({
        threadId,
        listingId     : newListingId,
        listingUrl,
        generated,
        imagesUploaded,
        salesSynopsis,
        family,
        priceUsd      : thread.acceptedQuoteUsd
      });
    } else {
      const [, generatedSynopsis] = await Promise.all([
        sendListingUrlToCustomer({
          threadId,
          etsyConversationUrl: thread.etsyConversationUrl,
          listingUrl,
          family,
          listingId: newListingId
        }),
        generateSalesSynopsis({
          thread: freshThread, family, listingId: newListingId, listingUrl, fullThreadContext
        })
      ]);
      salesSynopsis = generatedSynopsis;

      // 10. Success markers + audit. This write also flips thread.status to
      //     "sales_completed" — a terminal status the sales agent honors,
      //     and the dashboard's "Completed Sales" menu filters on.
      await markSuccess({
        threadId,
        listingId     : newListingId,
        listingUrl,
        generated,
        imagesUploaded,
        salesSynopsis,
        isResume
      });
    }

    return {
      statusCode: 200,
      headers   : CORS,
      body: JSON.stringify({
        ok        : true,
        threadId,
        listingId : String(newListingId),
        listingUrl,
        resumed   : isResume,
        manual    : !!manualMode,
        synopsisChars: (salesSynopsis || "").length,
        elapsedMs : Date.now() - tStart
      })
    };

  } catch (err) {
    if (threadId) await markFailure({ threadId, err });
    return {
      statusCode: 500,
      headers   : CORS,
      body: JSON.stringify({ ok: false, error: clampStr(err.message, 500), threadId })
    };
  }
});
