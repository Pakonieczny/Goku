/*  netlify/functions/etsyMailSnapshot.js
 *
 *  Ingest endpoint for the Chrome extension's Etsy thread scrapes.
 *
 *  Flow:
 *    1. Extension scrapes an Etsy conversation and POSTs a structured snapshot.
 *    2. This function finds (or creates) a thread doc keyed by etsyConversationId.
 *    3. For each scraped message, it dedupes by contentHash and writes only new ones.
 *    4. It updates the thread's lastSyncedAt, lastScrapedDomHash, customer/username if newly learned,
 *       and advances status detected_from_gmail → etsy_scraped.
 *    5. Writes an audit event.
 *
 *  POST body shape (from extension):
 *    {
 *      scrapedAt: <ms>,
 *      etsyConversationId: <string>,
 *      etsyConversationUrl: <string>,
 *      threadDomHash: <sha1 of full DOM>,
 *      participants: [{ name, etsyUsername, role }, ...],
 *      subject: <string or null>,
 *      messages: [
 *        {
 *          senderName, senderRole,      // role: 'customer' | 'staff'
 *          timestampMs,                  // ms since epoch
 *          text,
 *          imageUrls: [<etsy CDN urls>], // to be mirrored
 *          attachmentUrls: [...],
 *          contentHash: <sha1(senderName + timestampMs + normalizedText)>,
 *          domSelector: <optional debug>
 *        }, ...
 *      ],
 *      session: { etsyLoggedIn: <bool>, etsyUsername: <string or null> }
 *    }
 */

const admin = require("./firebaseAdmin");
const { requireExtensionAuth, CORS } = require("./_etsyMailAuth");
const db  = admin.firestore();
const FV  = admin.firestore.FieldValue;

const THREADS_COLL = "EtsyMail_Threads";
const AUDIT_COLL   = "EtsyMail_Audit";

function json(statusCode, body) { return { statusCode, headers: CORS, body: JSON.stringify(body) }; }
function bad(msg, code = 400)    { return json(code, { error: msg }); }

async function writeAudit({ threadId, eventType, actor, payload }) {
  await db.collection(AUDIT_COLL).add({
    threadId: threadId || null,
    draftId : null,
    eventType,
    actor   : actor || "system:extension",
    payload : payload || {},
    createdAt: FV.serverTimestamp()
  });
}

function normalize(text = "") {
  return String(text).toLowerCase().replace(/\s+/g, " ").trim();
}

function pickCustomer(participants) {
  if (!Array.isArray(participants)) return null;
  return participants.find(p => p && p.role === "customer") || null;
}

/**
 * v3.1 — Defensive decoder for JSON-stringified Unicode escape sequences.
 *
 * Some upstream code path (in the Chrome scraper, by the look of the
 * field shape — names + subjects only) is calling JSON.stringify on
 * scraped strings and storing the result, which turns "Caitríona" into
 * the literal six-character string `Caitr\u00edona` (a real backslash,
 * then "u00ed", then "ona"). Operators see the literal escape sequence
 * in the inbox UI instead of the accented character.
 *
 * We can't reach into the extension to fix it at the source, so we
 * decode defensively here at the ingest boundary. Behavior:
 *
 *   - Input has no backslash-u sequences → returned unchanged.
 *   - Each `\uXXXX` matched is replaced with the actual character it
 *     represents, BUT only when the resulting code point is >= 0x80
 *     (non-ASCII). This avoids "decoding" sequences that are real
 *     backslash content (`a literal \u0041` from a documentation
 *     string, etc.) and only fixes the bug we're actually seeing,
 *     which is non-ASCII characters that got round-tripped through
 *     JSON.stringify.
 *   - Surrogate pairs (`\uD83D\uDE00` = 😀) are joined and decoded as a
 *     single code point so emoji and astral-plane characters survive.
 *   - Idempotent: running it twice on a clean string is a no-op.
 *
 * Returns the input unchanged for non-string types, null, or undefined.
 */
function unmangleEscapedUnicode(s) {
  if (typeof s !== "string" || s.length === 0) return s;
  // Fast path — no `\u` sequences, nothing to do.
  if (s.indexOf("\\u") === -1) return s;

  // Two-pass approach:
  //   Pass 1 — surrogate pairs `\uHHHH\uHHHH` where the first is a
  //            high surrogate (D800-DBFF) and the second a low
  //            surrogate (DC00-DFFF). These represent astral-plane
  //            code points (e.g. emoji) and must be decoded together.
  //   Pass 2 — single `\uHHHH` escapes for non-ASCII BMP characters.
  let out = s.replace(
    /\\u([dD][89aAbB][0-9a-fA-F]{2})\\u([dD][c-fC-F][0-9a-fA-F]{2})/g,
    (_m, hi, lo) => {
      const high = parseInt(hi, 16);
      const low  = parseInt(lo, 16);
      try {
        return String.fromCodePoint(((high - 0xD800) << 10) + (low - 0xDC00) + 0x10000);
      } catch {
        return _m;   // leave as-is on any parse failure
      }
    }
  );
  out = out.replace(/\\u([0-9a-fA-F]{4})/g, (m, hex) => {
    const cp = parseInt(hex, 16);
    // Only decode non-ASCII. Below 0x80 we leave the literal alone —
    // it might be intentional content (e.g. a docstring showing JSON
    // syntax). Empirically the bug only produces non-ASCII escapes
    // because ASCII characters don't get JSON-escaped in the first place.
    if (cp < 0x80) return m;
    try {
      return String.fromCharCode(cp);
    } catch {
      return m;
    }
  });
  return out;
}

/**
 * Walk an object/array and apply unmangleEscapedUnicode to every string
 * field. Used to clean the `participants` array before storing.
 * Recursion-safe (won't follow circular references — uses a Set).
 */
function unmangleObjectStrings(obj, _seen) {
  if (obj == null) return obj;
  if (typeof obj === "string") return unmangleEscapedUnicode(obj);
  if (typeof obj !== "object") return obj;
  if (!_seen) _seen = new Set();
  if (_seen.has(obj)) return obj;
  _seen.add(obj);
  if (Array.isArray(obj)) {
    return obj.map(v => unmangleObjectStrings(v, _seen));
  }
  const out = {};
  for (const k of Object.keys(obj)) {
    out[k] = unmangleObjectStrings(obj[k], _seen);
  }
  return out;
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };
  if (event.httpMethod !== "POST")     return json(405, { error: "Method Not Allowed" });

  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return bad("Invalid JSON"); }

  // v3.1 — Defensive unmangle of every string field BEFORE we destructure.
  // The Chrome scraper has a known bug where some non-ASCII customer
  // names + subjects arrive as literal `\uXXXX` escape sequences (six
  // characters) instead of the actual Unicode character (one character).
  // unmangleObjectStrings walks the body recursively and decodes any
  // such sequence found in any string field. Idempotent on clean data.
  // See unmangleEscapedUnicode above for the full rationale.
  body = unmangleObjectStrings(body);

  // v3.2 — threadExists lookup. Lets the Chrome scraper ask, before it
  // commits to scrolling through a long Etsy conversation, "do we
  // already have this thread?" If yes, the scraper skips Phase 1
  // (which triggers Etsy's load-more to pull the entire history) and
  // sends only the bubbles that are visible without any scrolling.
  // Those visible bubbles always include the newest message (Etsy's
  // conversation page opens at the bottom of the thread). The snapshot
  // ingest's content-based dedup (senderRole + normalizedText +
  // tsMinute) handles any overlap between visible-already-known
  // bubbles and genuinely new ones, even if multiple new messages
  // arrived between scrapes.
  //
  // For a fresh thread we've never seen, this op returns exists:false
  // and the scraper falls back to its existing full Phase 1 scroll.
  //
  // v3.4 — Also return exists:false when the thread DOC exists but has
  // no messages (messageCount === 0 or null). This handles the "clean
  // rescrape" workflow where an operator wiped a polluted thread's
  // messages subcollection via deleteSub but the parent doc remained.
  // Without this, the scraper would see exists:true, do an incremental
  // scrape, and only capture the visible bottom of the thread — leaving
  // the upper history permanently missing.
  //
  // Returns: { exists }
  if (body.op === "threadExists") {
    const convId = body.etsyConversationId;
    if (!convId) return bad("threadExists requires etsyConversationId");
    const threadId = `etsy_conv_${convId}`;
    try {
      const tSnap = await db.collection(THREADS_COLL).doc(threadId).get();
      if (!tSnap.exists) return json(200, { exists: false });
      const data = tSnap.data() || {};
      const mc = typeof data.messageCount === "number" ? data.messageCount : null;
      // Empty thread → treat as not existing so the scraper does a full pass
      if (mc === 0 || mc === null) {
        return json(200, { exists: false, reason: "thread_doc_present_but_empty" });
      }
      return json(200, { exists: true });
    } catch (err) {
      console.error("threadExists failed:", err);
      // Fail open so the scraper falls back to its full-scrape path
      // rather than blocking on a transient backend error.
      return json(200, { exists: false, error: err.message });
    }
  }

  const {
    etsyConversationId,
    etsyConversationUrl,
    threadDomHash,
    scrapedAt,
    participants = [],
    subject = null,
    messages = [],
    session = {},
    // v0.9.38 — Etsy's new in-thread heading. Optional; older scrapers
    // and odd thread types (no related order) may omit it.
    conversationHeading = null
  } = body;

  if (!etsyConversationId) return bad("Missing etsyConversationId");
  if (!Array.isArray(messages)) return bad("messages must be an array");

  const threadId = `etsy_conv_${etsyConversationId}`;
  const tRef = db.collection(THREADS_COLL).doc(threadId);

  try {
    // ─── 1) Load or create the thread doc ───
    const tSnap = await tRef.get();
    const now = FV.serverTimestamp();
    const customer = pickCustomer(participants);

    let threadExisted = tSnap.exists;
    let currentStatus = tSnap.exists ? (tSnap.data().status || null) : null;

    const threadPatch = {
      etsyConversationId,
      etsyConversationUrl: etsyConversationUrl || null,
      lastScrapedDomHash : threadDomHash || null,
      lastSyncedAt       : now,
      updatedAt          : now
    };
    if (subject) threadPatch.subject = subject;
    if (customer) {
      if (customer.name)          threadPatch.customerName = customer.name;
      if (customer.etsyUsername)  threadPatch.etsyUsername = customer.etsyUsername;
      // NEW — buyer metadata useful for M3 customer panel
      if (customer.peopleUrl)     threadPatch.buyerPeopleUrl = customer.peopleUrl;
      if (customer.avatarUrl)     threadPatch.buyerAvatarUrl = customer.avatarUrl;
      if (customer.buyerUserId)   threadPatch.buyerUserId = String(customer.buyerUserId);
      if (typeof customer.isRepeatBuyer === "boolean") {
        threadPatch.buyerIsRepeatBuyer = customer.isRepeatBuyer;
      }
    }

    // v0.9.38 — Persist Etsy's conversation-heading info if the scraper
    // surfaced it. Per-field rather than blob so missing fields don't
    // clobber previously-captured ones (e.g. if Etsy briefly removes
    // the order link from the heading, we keep the last-seen orderId).
    if (conversationHeading && typeof conversationHeading === "object") {
      if (conversationHeading.orderId) {
        threadPatch.etsyOrderId = String(conversationHeading.orderId);
      }
      if (conversationHeading.categoryBadge) {
        threadPatch.etsyHeadingBadge = String(conversationHeading.categoryBadge);
      }
      if (conversationHeading.title) {
        threadPatch.etsyHeadingTitle = String(conversationHeading.title);
      }
      if (conversationHeading.viewOrderUrl) {
        threadPatch.etsyViewOrderUrl = String(conversationHeading.viewOrderUrl);
      }
    }

    // Advance status on first successful scrape
    const advanceable = ["detected_from_gmail", "pending_etsy_scrape", null, undefined];
    if (advanceable.includes(currentStatus)) {
      threadPatch.status = "etsy_scraped";
    }

    if (!threadExisted) {
      // Create fresh
      const initial = {
        threadId,
        etsyConversationId,
        etsyConversationUrl : etsyConversationUrl || null,
        gmailMessageId      : null,
        gmailThreadId       : null,
        gmailReceivedAt     : null,
        customerName        : (customer && customer.name) || "Unknown",
        customerEmail       : null,
        etsyUsername        : (customer && customer.etsyUsername) || null,
        linkedOrderId       : null,
        linkedListingIds    : [],
        status              : "etsy_scraped",
        category            : null,
        confidence          : null,
        needsHumanReview    : true,
        aiDraftStatus       : "none",
        latestDraftId       : null,
        lastInboundAt       : null,
        lastOutboundAt      : null,
        lastSyncedAt        : now,
        lastScrapedDomHash  : threadDomHash || null,
        assignedTo          : null,
        tags                : [],
        riskFlags           : [],
        messageCount        : 0,
        unread              : true,
        lastReadAt          : null,
        subject             : subject || null,
        createdAt           : now,
        updatedAt           : now,
        // v4.3.16 — Buyer metadata. Previously these fields were only
        // written via threadPatch (the `merge: true` path for existing
        // threads), so on a FIRST scrape — the precise moment when we
        // most need them — they were dropped. Now mirror them into
        // the initial doc so they're present from creation.
        buyerUserId         : (customer && customer.buyerUserId) ? String(customer.buyerUserId) : null,
        buyerPeopleUrl      : (customer && customer.peopleUrl) || null,
        buyerAvatarUrl      : (customer && customer.avatarUrl) || null,
        buyerIsRepeatBuyer  : !!(customer && customer.isRepeatBuyer),
        // v0.9.38 — Mirror Etsy's conversation-heading metadata into
        // initial-create too so first-scrape threads carry it. Older
        // scrapers send conversationHeading=null and these stay null.
        etsyOrderId         : (conversationHeading && conversationHeading.orderId)
                              ? String(conversationHeading.orderId) : null,
        etsyHeadingBadge    : (conversationHeading && conversationHeading.categoryBadge) || null,
        etsyHeadingTitle    : (conversationHeading && conversationHeading.title) || null,
        etsyViewOrderUrl    : (conversationHeading && conversationHeading.viewOrderUrl) || null
      };
      await tRef.set(initial, { merge: false });
    } else {
      await tRef.set(threadPatch, { merge: true });
    }

    // ─── 2) Dedupe + upsert messages ───
    // We fetch existing hashes AND their current timestamps so we can UPDATE
    // a stored message's timestamp if the scraper now provides a better one
    // (e.g., scraper v0.3+ extracts real per-message Date: headers that
    // earlier scrapes missed).
    //
    // v3.3 — Also build a CONTENT-based dedup index. The existing
    // contentHash includes the bubble's position in the full thread,
    // which doesn't survive an incremental scrape: if a returning
    // customer sends a new message, the incremental scrape sees only
    // the bottom ~10 visible bubbles, so previously-stored bubbles get
    // assigned different positions in the scrape than they had in
    // storage. Different positions → different hashes → false negatives
    // on the dedup check → duplicate copies of every old bubble inserted.
    //
    // The content-based index avoids that by keying on:
    //   - role (staff/customer/shop_owner → folded into staff vs customer)
    //   - either normalizedText (text bubbles) OR sorted imageUrls
    //     (image bubbles), since image bubbles have empty text and would
    //     otherwise collide with each other
    //   - tsMinute (timestamp rounded to the minute; matches Etsy's
    //     minute-level UI precision)
    //
    // Both indexes are checked for every incoming bubble: a match in
    // EITHER means we already have it. Old bubbles in storage match
    // via the content index; new bubbles match either way.
    const existingSnap = await tRef.collection("messages")
      .select("contentHash", "senderRole", "normalizedText", "text", "imageUrls", "timestamp", "messageType")
      .limit(2000).get();
    const existingByHash    = new Map();   // hash → { docId, currentTsMs }
    const existingByContent = new Map();   // contentKey → { docId, currentTsMs }


    // Normalize the role vocabulary. Snapshot writes "customer"/"staff".
    // Optimistic-message writes "shop_owner" (treated as staff equivalent).
    // Anything else falls back to customer (safer default — content match
    // on an unknown role won't accidentally suppress a real customer
    // bubble; the worst case is one extra dedupe miss).
    function normalizeRole(senderRole) {
      if (senderRole === "staff" || senderRole === "shop_owner") return "staff";
      return "customer";
    }

    // Build a stable content-fingerprint for a bubble. For text bubbles
    // the fingerprint is the normalized text. For image bubbles the
    // fingerprint is the sorted, joined imageUrls (matches the in-scraper
    // pattern at processImageAttachment: `IMAGE:${urlKey}`).
    // Falls back to text → URLs in that order to handle borderline docs.
    // Extract the stable image identifier from an Etsy CDN URL so we
    // can dedupe image bubbles across scrapes even if the URL format
    // varies slightly (CDN domain rotation, size suffix changes like
    // _5760xN vs _fullxfull, cache-busting query params). Etsy's
    // messaging-image URLs have one of these shapes:
    //
    //   https://i.etsystatic.com/iiii/icm/iap/<ID>/<filename>_<size>.jpg
    //   https://i.etsystatic.com/<numeric>/r/il/<hash>/<id>/il_<size>.<id>_<rand>.jpg
    //
    // The identifier segment is preceded by /icm/ or /il/. We pick the
    // last segment before the filename — that's where Etsy's image
    // identifier consistently lives across URL variations. Falls back
    // to the full URL if the pattern doesn't match, so we never
    // dedupe LESS aggressively than today.
    function extractEtsyImageId(url) {
      const str = String(url || "");
      if (!str) return "";
      // Strip query string and fragment
      const clean = str.split(/[?#]/)[0];
      // Take the last 1-2 path segments; the actual ID is usually the
      // second-to-last segment (the folder containing the image file).
      // e.g. ".../icm/iap/abc123def/foo_fullxfull.jpg" → "abc123def"
      const parts = clean.split("/").filter(Boolean);
      if (parts.length >= 2) {
        const candidate = parts[parts.length - 2];
        // Reject obvious non-ID segments like "icm", "iap", "iusa"
        if (candidate.length >= 6 && !/^(icm|iap|iusa|r|il)$/i.test(candidate)) {
          return candidate;
        }
      }
      // Fallback: use the entire cleaned URL. Still works when both
      // sides of the dedup comparison see the same exact URL.
      return clean;
    }

    function bubbleFingerprint({ text, normalizedText, imageUrls, messageType }) {
      const txt = normalizedText || normalize(text || "");
      if (txt) return `T:${txt}`;
      if (Array.isArray(imageUrls) && imageUrls.length) {
        const ids = imageUrls
          .filter(Boolean)
          .map(extractEtsyImageId)
          .filter(Boolean)
          .sort();
        if (ids.length) return `I:${ids.join("|")}`;
      }
      // Empty bubble (no text, no images). Should never happen in
      // practice but if it does, fold to a sentinel so the key is at
      // least valid (won't collide with real bubbles since real ones
      // always have one or the other).
      return "E:empty";
    }

    function contentKey(senderRole, fp, tsMs) {
      const role = normalizeRole(senderRole);
      const minute = tsMs != null ? Math.floor(tsMs / 60000) : "";
      return `${role}|${fp}|${minute}`;
    }

    existingSnap.forEach(d => {
      const data = d.data() || {};
      const currentTsMs = data.timestamp && typeof data.timestamp.toMillis === "function"
        ? data.timestamp.toMillis()
        : null;
      if (data.contentHash) {
        existingByHash.set(data.contentHash, { docId: d.id, currentTsMs });
      }
      // Also index by content. Skip docs without enough info to
      // fingerprint reliably (no text AND no images AND no timestamp).
      if (currentTsMs != null && data.senderRole) {
        const fp = bubbleFingerprint({
          text          : data.text,
          normalizedText: data.normalizedText,
          imageUrls     : data.imageUrls,
          messageType   : data.messageType
        });
        if (fp !== "E:empty") {
          existingByContent.set(
            contentKey(data.senderRole, fp, currentTsMs),
            { docId: d.id, currentTsMs }
          );
        }
      }
    });

    let newest_inbound_ms  = null;
    let newest_outbound_ms = null;
    let newestAny_ms       = null;
    const toInsert = [];
    const toUpdate = [];

    // Rough heuristic: a "scrape-time fallback" timestamp is one within a
    // few seconds of scrapedAt. If the existing stored timestamp looks like
    // a fallback AND the new one doesn't, update it.
    const scrapeTimeMs = typeof scrapedAt === "number" ? scrapedAt : Date.now();
    const FALLBACK_WINDOW_MS = 120 * 1000;  // 2 minutes
    function looksLikeFallbackTs(tsMs) {
      return tsMs != null && Math.abs(tsMs - scrapeTimeMs) < FALLBACK_WINDOW_MS;
    }

    for (const m of messages) {
      if (!m || !m.contentHash) continue;

      const direction = m.senderRole === "staff" ? "outbound" : "inbound";
      const ts = typeof m.timestampMs === "number" ? m.timestampMs : null;
      if (ts != null) {
        newestAny_ms = Math.max(newestAny_ms || 0, ts);
        if (direction === "inbound")  newest_inbound_ms  = Math.max(newest_inbound_ms  || 0, ts);
        if (direction === "outbound") newest_outbound_ms = Math.max(newest_outbound_ms || 0, ts);
      }

      // v3.3 — Check both dedup indexes. Hash match is the fast path
      // (full-scrape case). Content match handles the incremental case
      // where the same bubble's hash differs because its position in
      // the scrape doesn't match its position in storage.
      let existing = existingByHash.get(m.contentHash);
      if (!existing) {
        const fp = bubbleFingerprint({
          text          : m.text,
          normalizedText: null,                // recomputed from text
          imageUrls     : m.imageUrls,
          messageType   : m.messageType
        });
        if (fp !== "E:empty") {
          existing = existingByContent.get(contentKey(m.senderRole, fp, ts));
        }
      }

      if (existing) {
        // Candidate for timestamp update: we have a new ts, it's different,
        // and either we stored nothing or what we stored looks like a fallback.
        if (ts != null) {
          const stored = existing.currentTsMs;
          const storedLooksFallback = looksLikeFallbackTs(stored);
          const newLooksFallback    = looksLikeFallbackTs(ts);
          const storedMissingOrBad  = stored == null || storedLooksFallback;
          if (storedMissingOrBad && !newLooksFallback && stored !== ts) {
            toUpdate.push({
              docId: existing.docId,
              patch: {
                timestamp : admin.firestore.Timestamp.fromMillis(ts),
                updatedAt : now
              }
            });
          }
        }
        continue;   // already exists, don't re-insert
      }

      // Sanitize listing cards — accept only expected fields, drop anything weird
      const listingCards = Array.isArray(m.listingCards)
        ? m.listingCards.map(c => ({
            listingId        : String(c.listingId || ""),
            listingUrl       : String(c.listingUrl || ""),
            title            : String(c.title || ""),
            thumbnailUrl     : String(c.thumbnailUrl || ""),
            priceText        : String(c.priceText || ""),
            originalPriceText: String(c.originalPriceText || ""),
            shippingText    : String(c.shippingText || "")
          })).filter(c => c.listingId && c.listingUrl)
        : [];

      toInsert.push({
        source            : "etsy",
        direction,
        senderName        : m.senderName || "Unknown",
        senderRole        : m.senderRole || "customer",
        timestamp         : ts ? admin.firestore.Timestamp.fromMillis(ts) : now,
        text              : m.text || "",
        normalizedText    : normalize(m.text),
        contentHash       : m.contentHash,
        messageType       : m.messageType || "text",     // "text" | "image" | future types
        imageUrls         : Array.isArray(m.imageUrls) ? m.imageUrls : [],
        thumbnailUrls     : Array.isArray(m.thumbnailUrls) ? m.thumbnailUrls : [],
        listingCards,                                    // NEW: structured Etsy listing previews
        storageImagePaths : [],
        storageMirrorState: Array.isArray(m.imageUrls) && m.imageUrls.length ? "pending" : "none",
        attachmentUrls    : Array.isArray(m.attachmentUrls) ? m.attachmentUrls : [],
        etsyDomSelector   : m.domSelector || null,
        createdAt         : now
      });
    }

    // Write inserts (new messages)
    let writtenCount = 0;
    for (let i = 0; i < toInsert.length; i += 400) {
      const batch = db.batch();
      const chunk = toInsert.slice(i, i + 400);
      for (const m of chunk) {
        const mRef = tRef.collection("messages").doc(`etsy_${m.contentHash}`);
        batch.set(mRef, m, { merge: false });
      }
      await batch.commit();
      writtenCount += chunk.length;
    }

    // Write timestamp updates (existing messages with better timestamps now available)
    let updatedCount = 0;
    for (let i = 0; i < toUpdate.length; i += 400) {
      const batch = db.batch();
      const chunk = toUpdate.slice(i, i + 400);
      for (const u of chunk) {
        const mRef = tRef.collection("messages").doc(u.docId);
        batch.set(mRef, u.patch, { merge: true });
      }
      await batch.commit();
      updatedCount += chunk.length;
    }

    // ─── 3) Update thread tail timestamps + message count ───
    if (writtenCount > 0) {
      const tailPatch = { updatedAt: now };
      tailPatch.messageCount = FV.increment(writtenCount);
      if (newest_inbound_ms != null) {
        tailPatch.lastInboundAt = admin.firestore.Timestamp.fromMillis(newest_inbound_ms);
        tailPatch.unread = true;
      }
      if (newest_outbound_ms != null) {
        tailPatch.lastOutboundAt = admin.firestore.Timestamp.fromMillis(newest_outbound_ms);
      }

      // ─── v1.3: image_attached risk flag ──────────────────────────
      // If any of the newly-inserted messages carry images, mark the
      // thread so the inbox UI's "with image" filter can find it.
      // arrayUnion is idempotent — re-marking an already-marked thread
      // is a no-op. We don't bother removing the flag if all images
      // get deleted later because that's exceedingly rare and the
      // UX cost of a stale flag is low.
      const anyImageMessage = toInsert.some(m =>
        (Array.isArray(m.imageUrls) && m.imageUrls.length > 0) ||
        m.messageType === "image"
      );
      if (anyImageMessage) {
        tailPatch.riskFlags = FV.arrayUnion("image_attached");
      }

      // ─── v1.3: searchableText denormalized field ────────────────
      // Maintains a lowercased, normalized concatenation of:
      //   - thread metadata (customer name, etsy username, subject)
      //   - the message bodies of recent messages (incremental: we
      //     append newly-inserted message text to the existing field
      //     and truncate from the front to keep the most recent ~6KB)
      //
      // This is what etsyMailSearch.js queries for substring matches.
      // Keeping it on the thread doc means search is a single
      // collection scan, no subcollection joins.
      //
      // The 6KB cap protects against runaway growth — a thread with
      // hundreds of messages would otherwise grow unbounded. Recent
      // messages are most relevant to search, so dropping oldest
      // first is the right trade-off.
      const newTextChunks = toInsert
        .map(m => normalize(m.text))
        .filter(Boolean);

      // v1.10: searchableText must exist on every thread for the inbox
      // to search message bodies. Pre-v1.10 it was only built/updated
      // when new messages arrived — threads scraped once before this
      // logic existed, OR threads that haven't received new activity
      // since v1.3, never got the field. Search would silently miss
      // them.
      //
      // Now: rebuild searchableText whenever EITHER:
      //   (a) new messages arrived (incremental — append + truncate, fast)
      //   (b) the field is missing on the existing thread doc
      //       (one-time backfill from the messages subcollection)
      //
      // Case (b) is a single subcollection read per thread, runs at most
      // once per thread (next scrape sees the field populated and skips).
      const prevSnap = (tSnap && tSnap.data && tSnap.data()) || {};
      const hasField = !!prevSnap.searchableText;
      const SEARCHABLE_MAX = 6000;

      const buildMeta = (truncatedBody) => {
        const metaParts = [
          threadPatch.customerName  || prevSnap.customerName  || "",
          threadPatch.etsyUsername  || prevSnap.etsyUsername  || "",
          threadPatch.subject       || prevSnap.subject       || "",
          prevSnap.linkedOrderId    || ""
        ].map(s => normalize(String(s))).filter(Boolean);
        return (metaParts.join(" ") + " " + truncatedBody).trim();
      };

      if (newTextChunks.length > 0) {
        // Case (a): incremental update
        const prevMessageText = prevSnap.searchableMessageText || "";
        const combined = (prevMessageText + " " + newTextChunks.join(" ")).trim();
        const truncated = combined.length > SEARCHABLE_MAX
          ? combined.slice(combined.length - SEARCHABLE_MAX)
          : combined;
        tailPatch.searchableMessageText = truncated;
        tailPatch.searchableText = buildMeta(truncated);
      } else if (!hasField) {
        // Case (b): one-time backfill. Read the existing messages
        // subcollection (most recent 50, plenty for the 6KB cap) and
        // build the field from scratch. After this scrape the field
        // exists and the snapshot returns to incremental updates.
        try {
          const msgsSnap = await tRef.collection("messages")
            .orderBy("timestamp", "desc")
            .limit(50)
            .get();
          const allText = msgsSnap.docs
            .map(d => normalize((d.data() || {}).text || ""))
            .filter(Boolean)
            .reverse()                // back to chronological order
            .join(" ");
          const truncated = allText.length > SEARCHABLE_MAX
            ? allText.slice(allText.length - SEARCHABLE_MAX)
            : allText;
          tailPatch.searchableMessageText = truncated;
          tailPatch.searchableText = buildMeta(truncated);
        } catch (backfillErr) {
          console.warn("snapshot: searchableText backfill failed for", threadId, "—", backfillErr.message);
          // Fall back to metadata-only so at least metadata search works
          tailPatch.searchableText = buildMeta("");
        }
      }

      await tRef.set(tailPatch, { merge: true });
    }

    // ─── 4) Session / login-required detection ───
    // v1.2: This MUST run BEFORE the auto-pipeline trigger. If Etsy is
    // logged out, we know the send pipeline can't deliver — pushing the
    // thread to Needs Review and skipping the AI call avoids burning
    // an Opus call for a draft we can't actually send.
    //
    // Status: route to pending_human_review (the v1.1+ visible folder).
    // The legacy hold_login_required is no longer in the rail.
    const etsyLoggedOut = session && session.etsyLoggedIn === false;
    if (etsyLoggedOut) {
      await tRef.set({
        status   : "pending_human_review",
        updatedAt: now
      }, { merge: true });
      await writeAudit({
        threadId,
        eventType: "held",
        actor: "system:extension",
        payload: { reason: "etsy_login_required" }
      });
    }

    // ─── 5) Trigger auto-reply pipeline ─────────────────────────
    // If a new inbound message landed AND the Etsy session is logged in,
    // fire the auto-reply pipeline as a Netlify -background function.
    // It:
    //   - generates an AI draft via etsyMailDraftReply
    //   - reads the AI's self-rated confidence
    //   - applies deterministic veto rules (refund, cancel, legal, etc.)
    //   - either auto-enqueues for send (high confidence + no vetoes)
    //     OR routes the thread to "Needs review" (low confidence,
    //     vetoed, or kill-switch active)
    //
    // Why -background: the AI draft step takes 10-60 seconds with Sonnet
    // 4.6 + tool calls. Netlify's standard 10s function timeout is too
    // tight; the -background suffix unlocks 15 minutes and decouples
    // the response from completion (Netlify returns 202 immediately).
    //
    // We AWAIT the fetch (with a 5-second AbortSignal) so the snapshot
    // function doesn't return before the trigger has been dispatched.
    // Netlify -background returns 202 within ~50-200ms typically; the
    // 5s timeout is generous safety. Any error is swallowed — the
    // scrape ingest must succeed independently of auto-reply.
    //
    // The on/off flag and confidence threshold live in
    // EtsyMail_Config/autoPipeline (read by the pipeline itself, cached
    // 15s). Snapshot stays dumb — every new inbound triggers, and the
    // pipeline decides whether to act.
    const hasNewInbound = writtenCount > 0 && newest_inbound_ms != null;
    if (hasNewInbound && !etsyLoggedOut) {
      const baseUrl = process.env.URL
                   || process.env.DEPLOY_URL
                   || "http://localhost:8888";
      const headers = { "Content-Type": "application/json" };
      if (process.env.ETSYMAIL_EXTENSION_SECRET) {
        headers["X-EtsyMail-Secret"] = process.env.ETSYMAIL_EXTENSION_SECRET;
      }
      // AbortSignal.timeout requires Node 18+. All Netlify Functions
      // run on Node 18+ by default, but fall back gracefully if absent
      // (older bundlers can be missing the static method).
      const signal = (typeof AbortSignal !== "undefined" && AbortSignal.timeout)
        ? AbortSignal.timeout(5000)
        : undefined;
      try {
        const res = await fetch(`${baseUrl}/.netlify/functions/etsyMailAutoPipeline-background`, {
          method : "POST",
          headers,
          body   : JSON.stringify({
            threadId,
            employeeName: "system:auto-pipeline"
          }),
          signal
        });
        // 202 = Netlify accepted the background invocation. 200 is fine
        // too (e.g., if someone runs the function synchronously in dev).
        // Anything else is a smoke signal.
        if (res.status !== 202 && !res.ok) {
          console.warn("autoPipeline trigger non-2xx:", res.status, threadId);
        }
      } catch (e) {
        console.warn("autoPipeline trigger failed:", e.message, threadId);
        // Don't propagate — scrape ingest must succeed independently.
      }
    }

    // ─── 5) Audit ───
    await writeAudit({
      threadId,
      eventType: threadExisted ? "scrape_succeeded" : "thread_created_from_scrape",
      actor    : "system:extension",
      payload  : {
        newMessageCount      : writtenCount,
        updatedMessageCount  : updatedCount,
        totalMessagesScraped : messages.length,
        threadDomHash        : threadDomHash || null,
        scrapedAt            : scrapedAt || null
      }
    });

    // ─── 6) Collect image mirror jobs (if any) ───
    // Return a list of {messageId, imageUrls} pairs the extension can pass
    // to etsyMailMirrorImage, one call per image. Keeps Storage uploads out
    // of this hot path.
    const imagesToMirror = [];
    for (const m of toInsert) {
      if (!m.imageUrls || !m.imageUrls.length) continue;
      imagesToMirror.push({
        messageDocId: `etsy_${m.contentHash}`,
        imageUrls   : m.imageUrls
      });
    }

    // ─── 7) Trigger per-buyer order sync (v4.4.1 — fire-and-forget) ───
    // Closes the gap where manual scrapes left brand-new customers (or
    // recently-active ones) without an EtsyMail_Customers doc until the
    // next scheduled etsyMailSync cron. Triggering directly off the
    // snapshot here makes manual and auto-pipeline paths equivalent:
    // both refresh the customer's order data immediately after a scrape.
    //
    // v4.4.1 — Also pass receiptId+threadId so sync-background's new
    // targeted-hydrate path (ensureReceiptMirroredById) can pull a
    // specific receipt directly from Etsy if it's not yet in the mirror.
    // Without this, a help-request thread on a brand-new order ends up
    // with "No purchase history" in the customer panel because:
    //   - the mirror cron hasn't picked the receipt up yet, AND
    //   - the buyer-sync's mirror query returns 0 results, AND
    //   - the customer doc gets written with orderCount=0 (or worse, not
    //     at all when buyerUserId never came through on the scrape).
    // Passing receiptId resolves buyerUserId from the receipt itself if
    // the scrape didn't capture it, and guarantees the specific order
    // the customer is writing about is present in the mirror before the
    // aggregation runs.
    //
    // Async-invoke into etsyMailSync-background via Netlify's background
    // dispatch URL. We do NOT await — order sync is independent of the
    // snapshot's primary responsibility (saving messages) and shouldn't
    // block the response. Errors are logged but never bubbled up.
    const buyerForSync = (customer && customer.buyerUserId) ? String(customer.buyerUserId) : null;

    // Pull a structured order ID from the scraped conversation heading
    // (Etsy's "Help request" / "Help with order n.°" banner). Falls back
    // to the field that just got written to the thread doc.
    const receiptForSync =
         (body && body.conversationHeading && body.conversationHeading.orderId)
      || threadPatch.etsyOrderId
      || null;
    const receiptIdValid = receiptForSync && /^\d+$/.test(String(receiptForSync))
      ? String(receiptForSync) : null;

    // Fire the trigger whenever we have EITHER signal. With only
    // buyerUserId: standard mirror-aggregation path. With only receiptId:
    // targeted hydrate resolves buyer from the receipt, then aggregates.
    // With both: belt-and-suspenders, the hydrate confirms the receipt
    // and may correct a stale buyerUserId.
    if (buyerForSync || receiptIdValid) {
      const fnHost = process.env.URL || process.env.DEPLOY_PRIME_URL || null;
      if (fnHost) {
        const syncUrl = `${fnHost}/.netlify/functions/etsyMailSync-background`;
        const syncBody = { mode: "buyer", threadId };
        if (buyerForSync)   syncBody.buyerUserId = buyerForSync;
        if (receiptIdValid) syncBody.receiptId   = receiptIdValid;

        require("node-fetch")(syncUrl, {
          method : "POST",
          headers: { "Content-Type": "application/json" },
          body   : JSON.stringify(syncBody)
        }).catch(err => {
          console.warn(
            `buyer sync trigger failed for buyerUserId=${buyerForSync || "(none)"} receiptId=${receiptIdValid || "(none)"}:`,
            err.message || err
          );
        });
        console.log(
          `[snapshot] queued buyer sync — buyerUserId=${buyerForSync || "(unresolved)"}` +
          ` receiptId=${receiptIdValid || "(none)"} threadId=${threadId}`
        );
      } else {
        console.warn(
          `[snapshot] no fnHost (URL/DEPLOY_PRIME_URL) — skipping buyer sync trigger ` +
          `(buyerUserId=${buyerForSync || "(none)"}, receiptId=${receiptIdValid || "(none)"})`
        );
      }
    } else {
      console.log(
        `[snapshot] no buyer-sync signal available for thread ${threadId} ` +
        `(no buyerUserId from scrape, no orderId in conversation heading) — skipping trigger`
      );
    }

    return json(200, {
      success         : true,
      threadId,
      threadExisted,
      newMessages     : writtenCount,
      updatedMessages : updatedCount,
      totalScanned    : messages.length,
      imagesToMirror
    });

  } catch (err) {
    console.error("etsyMailSnapshot error:", err);
    await writeAudit({
      threadId: threadId,
      eventType: "scrape_failed",
      actor: "system:extension",
      payload: { error: err.message }
    }).catch(()=>{});
    return json(500, { error: err.message || String(err) });
  }
};
