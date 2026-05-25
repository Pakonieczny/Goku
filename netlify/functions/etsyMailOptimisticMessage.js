/**
 * etsyMailOptimisticMessage.js — v0.9.3
 *
 * Writes a synthetic "I just sent this" message into a thread's message
 * subcollection so the operator sees their outbound message immediately,
 * without waiting for the next M2 scrape.
 *
 * Lifecycle:
 *   1. Inbox sees draft status flip to sent / sent_text_only / sent_unverified
 *   2. Inbox calls op=insert with the draft's text + attachments
 *   3. We write a doc keyed `optim_<draftId>` with localOptimistic: true
 *   4. Inbox renderer dedupes: if a real outbound message with matching
 *      text exists, the optimistic one is hidden from the rendered list
 *      (it stays in Firestore as a ghost; cheap to leave there)
 *
 * Why a dedicated function (not a firestoreProxy extension):
 *   - Schema is fixed; we don't want general write access from the inbox
 *   - Auth-gated like every other M5 endpoint
 *   - Deterministic doc id (optim_<draftId>) so re-inserts overwrite
 *
 * Endpoint: POST /.netlify/functions/etsyMailOptimisticMessage
 *   { op: "insert", threadId, draftId, text, employeeName, attachments? }
 *   { op: "delete", threadId, draftId }
 *
 * Auth: requireExtensionAuth (same secret as the other M5 endpoints).
 */
const admin = require("./firebaseAdmin");
const { requireExtensionAuth, CORS } = require("./_etsyMailAuth");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const THREADS_COLL = "EtsyMail_Threads";

function json(statusCode, body) {
  return { statusCode, headers: CORS, body: JSON.stringify(body) };
}
function bad(msg, code = 400) { return json(code, { error: msg }); }
function ok(payload) { return json(200, { success: true, ...payload }); }

/** Build an outbound message doc that visually matches a real M2-scraped
 *  outbound. The renderer pivots on `direction === "outbound"` for bubble
 *  placement; everything else is for fidelity. */
function buildOptimisticDoc({ draftId, text, employeeName, attachments }) {
  const atts = Array.isArray(attachments) ? attachments : [];
  const images   = atts.filter(a => a && (a.type === "image" || a.type === "tracking_image"));
  const listings = atts.filter(a => a && a.type === "listing");

  // Image fields shaped like real outbound messages so renderThreadMessages
  // shows thumbnails + falls through to mirror URL when storagePath exists.
  const imageUrls         = images.map(a => a.proxyUrl   || null).filter(Boolean);
  const storageImagePaths = images.map(a => a.storagePath || null);
  const thumbnailUrls     = images.map(a => a.proxyUrl   || null).filter(Boolean);

  // Listing cards: minimal shape that matches what the inbox's renderer
  // already handles (renderListingCard reads listingId, listingUrl,
  // listingTitle, thumbnail, price).
  const listingCards = listings.map(a => ({
    listingId   : a.listingId    || null,
    listingUrl  : a.listingUrl   || (a.listingId ? `https://www.etsy.com/listing/${a.listingId}` : null),
    listingTitle: a.listingTitle || null,
    thumbnail   : a.thumbnail    || null,
    price       : a.price        || null
  }));

  const nowMs = Date.now();
  return {
    // What the renderer reads
    direction       : "outbound",
    source          : "owner",
    senderName      : employeeName || "Shop owner",
    senderRole      : "shop_owner",
    text            : String(text || ""),
    // v0.9.8 — `timestamp` MUST be a Firestore Timestamp, not a raw
    // Number. Firestore range queries compare values by type first,
    // then value. The fetchMessagesNow delta query in firestoreProxy
    // does:
    //   q.where("timestamp", ">", Timestamp.fromMillis(sinceMs))
    // If we wrote `timestamp` as a Number, the optimistic message was
    // EXCLUDED from delta query results — because in Firestore's mixed-
    // type ordering, Numbers compare differently than Timestamps. The
    // operator's just-sent message would silently never come back from
    // the delta fetch, so the in-memory state.messages didn't update,
    // and mobile's render-skip cache (keyed on message count + last id)
    // matched the previous render and skipped repainting.
    //
    // Symptom: send a message on mobile, see "Sent ✓" toast, but the
    // sent bubble never appears in the conversation. Closing and
    // reopening the browser forced a FULL fetch (which doesn't use the
    // since-cursor) so the message reappeared.
    //
    // Snapshot writes use Timestamp.fromMillis(ts); match that exactly.
    timestamp       : admin.firestore.Timestamp.fromMillis(nowMs),
    createdAt       : FV.serverTimestamp(),
    imageUrls,
    storageImagePaths,
    thumbnailUrls,
    listingCards,
    // Markers used for dedupe + cleanup
    localOptimistic     : true,
    optimisticDraftId   : draftId,
    optimisticInsertedAt: FV.serverTimestamp(),
    // Stable hash so the renderer can do a fast text-match dedupe
    // without trimming/normalizing on every render.
    optimisticTextKey   : String(text || "").trim().toLowerCase().slice(0, 200)
  };
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }
  if (event.httpMethod !== "POST") {
    return json(405, { error: "Method Not Allowed" });
  }

  // v0.9.1 #1: auth required (inbox forwards X-EtsyMail-Secret).
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return bad("Invalid JSON body"); }

  const { op, threadId, draftId } = body;
  if (!op) return bad("Missing op");
  if (!threadId || !/^etsy_conv_\d+$/.test(String(threadId))) {
    return bad("threadId must match etsy_conv_<digits>");
  }
  if (!draftId) return bad("Missing draftId");

  const docId = "optim_" + String(draftId);
  const ref = db.collection(THREADS_COLL).doc(String(threadId))
                .collection("messages").doc(docId);

  try {
    if (op === "insert") {
      const { text, employeeName = null, attachments = [] } = body;
      const doc = buildOptimisticDoc({
        draftId, text, employeeName, attachments
      });
      // Deterministic ID + merge:false so a second insert overwrites
      // (e.g. operator hits Send again on the same draftId).
      await ref.set(doc, { merge: false });
      return ok({ docId, threadId, inserted: true });
    }

    if (op === "delete") {
      await ref.delete();
      return ok({ docId, threadId, deleted: true });
    }

    return bad(`Unknown op '${op}'`);

  } catch (err) {
    console.error("etsyMailOptimisticMessage error:", err);
    return json(500, { error: err.message || String(err) });
  }
};

// v0.9.7: also export the doc-shape helper so backend callers (specifically
// etsyMailDraftSend.enqueue) can produce identical optimistic-message docs
// without duplicating the field list. Keeping the shape in one place means
// future renderer changes (e.g. new dedupe key) only need updating here.
exports.buildOptimisticDoc = buildOptimisticDoc;
