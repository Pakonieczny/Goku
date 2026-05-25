/*  netlify/functions/etsyMailDraftAttachment.js
 *
 *  Accepts an image upload from the M5 composer (drag-drop or paste from
 *  clipboard) and stores it in Firebase Storage under a draft-scoped path.
 *  Returns a same-origin proxy URL the composer chip renders inline AND
 *  the extension fetches later to re-inject into Etsy's compose box.
 *
 *  Why a separate function instead of reusing etsyMailMirrorImage:
 *    - Mirror accepts a URL to an Etsy-hosted image, downloads it server-side.
 *    - This function accepts arbitrary bytes from the operator's browser.
 *    - Mirror attaches the result to a message doc; this one doesn't touch
 *      the thread — the attachment reference lives on the draft doc.
 *    - Mirror has an Etsy-host allowlist; this one needs no allowlist.
 *
 *  Upload mechanics:
 *    Netlify functions have a 6MB request body cap. We accept base64-encoded
 *    image bytes rather than multipart, because multipart in Lambdas is
 *    painful (no native parser; busboy+streams add fragility). The composer
 *    reads the File with FileReader.readAsDataURL, strips the data-URL
 *    prefix, and POSTs { filename, contentType, bytesBase64, threadId }.
 *
 *  POST body:
 *    {
 *      threadId     : "etsy_conv_1651714855",
 *      filename     : "screenshot.png",            // original filename (best-effort)
 *      contentType  : "image/png",                 // declared by browser
 *      bytesBase64  : "iVBORw0KGgo...",            // raw image bytes, base64
 *      fromPaste    : true | false,                // UI hint; no business logic
 *      draftId      : "draft_etsy_conv_...",       // optional — if provided,
 *                                                    attachment record is
 *                                                    added to the draft
 *                                                    immediately (saves a
 *                                                    follow-up firestoreProxy
 *                                                    round-trip from UI)
 *    }
 *
 *  Response:
 *    {
 *      success      : true,
 *      attachmentId : "att_1234abcd",              // used as chip key + send-pipeline ref
 *      storagePath  : "etsymail/drafts/etsy_conv_.../att_....png",
 *      proxyUrl     : "/.netlify/functions/etsyMailImage?path=...",
 *      contentType  : "image/png",
 *      bytes        : 12345,
 *      filename     : "screenshot.png",
 *      widthPx      : null,    // reserved — not computed (would need sharp)
 *      heightPx     : null
 *    }
 *
 *  Validation:
 *    - contentType must be an image/* type we accept (png, jpeg, gif, webp).
 *      Etsy's compose accepts png/jpeg; we pass others through but the
 *      send step may fail — UI shows a warning if type isn't png/jpeg.
 *    - Total bytes after base64 decode must be ≤ 5 MB (Etsy's compose cap
 *      is larger but our transport layer has a 6 MB body limit, and base64
 *      inflates by ~33%).
 *    - threadId must match /^etsy_conv_\d+$/ — keeps the storage namespace
 *      clean and prevents arbitrary path writes.
 */

const crypto = require("crypto");
const admin  = require("./firebaseAdmin");
const { requireExtensionAuth, CORS } = require("./_etsyMailAuth");

const db     = admin.firestore();
const bucket = admin.storage().bucket();
const FV     = admin.firestore.FieldValue;

const DRAFTS_COLL = "EtsyMail_Drafts";

const MAX_DECODED_BYTES = 5 * 1024 * 1024;   // 5 MB after base64 decode
const ALLOWED_TYPES = new Set([
  "image/png", "image/jpeg", "image/jpg", "image/gif", "image/webp"
]);

function json(statusCode, body) {
  return { statusCode, headers: CORS, body: JSON.stringify(body) };
}
function bad(msg, code = 400) { return json(code, { error: msg }); }

function extFromContentType(ct) {
  switch ((ct || "").toLowerCase()) {
    case "image/png":  return "png";
    case "image/jpeg":
    case "image/jpg":  return "jpg";
    case "image/gif":  return "gif";
    case "image/webp": return "webp";
    default:           return "bin";
  }
}

function sanitizeFilename(name) {
  if (!name) return "attachment";
  // Strip path components, limit length, keep letters/numbers/dash/underscore/dot
  return String(name)
    .replace(/[\\/]/g, "")
    .replace(/[^a-zA-Z0-9._-]/g, "_")
    .slice(0, 80) || "attachment";
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }
  if (event.httpMethod !== "POST") {
    return json(405, { error: "Method Not Allowed" });
  }

  // v0.9.1 #1: auth required. Inbox forwards X-EtsyMail-Secret from
  // localStorage('etsymail_secret'). If env var unset, requireExtensionAuth
  // falls through (dev mode) — same as every other M5 endpoint.
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body;
  try { body = JSON.parse(event.body || "{}"); }
  catch { return bad("Invalid JSON body"); }

  const {
    threadId,
    filename,
    contentType,
    bytesBase64,
    fromPaste = false,
    draftId   = null
  } = body;

  // ── Input validation ───────────────────────────────────────────────
  if (!threadId || !/^etsy_conv_\d+$/.test(String(threadId))) {
    return bad("threadId must match etsy_conv_<digits>");
  }
  if (!contentType || typeof contentType !== "string") {
    return bad("Missing contentType");
  }
  if (!ALLOWED_TYPES.has(contentType.toLowerCase())) {
    return bad(`Unsupported contentType '${contentType}'. Accepted: ${[...ALLOWED_TYPES].join(", ")}`);
  }
  if (!bytesBase64 || typeof bytesBase64 !== "string") {
    return bad("Missing bytesBase64");
  }

  // ── Decode + size check ────────────────────────────────────────────
  let buf;
  try {
    // Strip any accidental data-URL prefix (the UI should strip this
    // client-side but defend against it here)
    const cleaned = bytesBase64.replace(/^data:[^;]+;base64,/, "");
    buf = Buffer.from(cleaned, "base64");
  } catch (e) {
    return bad("Invalid base64 payload");
  }
  if (!buf.length) return bad("Empty payload after decode");
  if (buf.length > MAX_DECODED_BYTES) {
    return bad(`Image too large: ${buf.length} bytes (max ${MAX_DECODED_BYTES})`);
  }

  // ── Upload to Firebase Storage ─────────────────────────────────────
  const attachmentId = "att_" + crypto.randomBytes(6).toString("hex");
  const ext          = extFromContentType(contentType);
  const safeName     = sanitizeFilename(filename || `paste.${ext}`);
  // Path shape mirrors the existing etsyMailMirrorImage: etsymail/<scope>/.../file
  // so etsyMailImage.js (which requires the "etsymail/" prefix) can serve it.
  const storagePath  = `etsymail/drafts/${threadId}/${attachmentId}.${ext}`;

  try {
    const file = bucket.file(storagePath);
    await file.save(buf, {
      metadata: {
        contentType,
        cacheControl: "public, max-age=31536000",
        metadata: {
          threadId,
          attachmentId,
          originalFilename: safeName,
          fromPaste: String(!!fromPaste),
          uploadedAt: String(Date.now())
        }
      },
      resumable: false
    });
  } catch (err) {
    console.error(`etsyMailDraftAttachment upload failed (${storagePath}):`, err);
    return json(500, { error: `Storage upload failed: ${err.message || err}` });
  }

  // Build the same-origin proxy URL the UI and extension will hit.
  const proxyUrl = `/.netlify/functions/etsyMailImage?path=${encodeURIComponent(storagePath)}`;

  // Build the attachment record shape we'll use in the draft document.
  const attachmentRecord = {
    attachmentId,
    type        : "image",
    source      : fromPaste ? "paste" : "upload",
    storagePath,
    proxyUrl,
    contentType,
    bytes       : buf.length,
    filename    : safeName,
    createdAt   : FV.serverTimestamp()
  };

  // ── Optionally stash the attachment on the draft doc ──────────────
  // The UI can manage its own attachments list in memory and persist on
  // send, but if the operator provides draftId we write through now so
  // a refresh doesn't lose the upload. We use arrayUnion so concurrent
  // uploads don't clobber each other.
  if (draftId) {
    try {
      // arrayUnion with an object uses deep equality — two uploads get
      // unique attachmentIds so they're always distinct.
      //
      // We can't put a serverTimestamp inside arrayUnion (Firestore
      // forbids sentinels inside array elements), so use a client-ish
      // ms timestamp for createdAt on the array copy.
      const arrayRecord = {
        ...attachmentRecord,
        createdAt: Date.now()
      };
      await db.collection(DRAFTS_COLL).doc(String(draftId)).set({
        draftAttachments: FV.arrayUnion(arrayRecord),
        updatedAt       : FV.serverTimestamp()
      }, { merge: true });
    } catch (err) {
      // Non-fatal: upload succeeded; the UI can persist later.
      console.warn(`etsyMailDraftAttachment: draft patch failed for ${draftId}:`, err.message);
    }
  }

  return json(200, {
    success     : true,
    attachmentId,
    storagePath,
    proxyUrl,
    contentType,
    bytes       : buf.length,
    filename    : safeName,
    widthPx     : null,  // reserved; would require sharp to compute
    heightPx    : null
  });
};
