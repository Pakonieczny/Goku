/*  netlify/functions/etsyMailMirrorImage.js
 *
 *  Mirrors a single Etsy image URL into Firebase Storage and records
 *  the storage path on the message document.
 *
 *  Called by the Chrome extension once per image returned by etsyMailSnapshot's
 *  `imagesToMirror` array. Keeping image uploads off the ingest path means a
 *  slow image fetch never blocks the thread snapshot from appearing in the UI.
 *
 *  POST body:
 *    {
 *      threadId:     "etsy_conv_12345",
 *      messageDocId: "etsy_<contentHash>",
 *      imageUrl:     "https://i.etsystatic.com/..."
 *    }
 *
 *  Response:
 *    { success: true, storagePath: "etsymail/<threadId>/<messageDocId>/<hash>.jpg",
 *      publicUrl: "https://..." (signed, 7 days) }
 */

const fetch = require("node-fetch");
const crypto = require("crypto");
const admin = require("./firebaseAdmin");
const { requireExtensionAuth, CORS } = require("./_etsyMailAuth");

const db     = admin.firestore();
const bucket = admin.storage().bucket();
const FV     = admin.firestore.FieldValue;

const THREADS_COLL = "EtsyMail_Threads";

function json(statusCode, body) { return { statusCode, headers: CORS, body: JSON.stringify(body) }; }
function bad(msg, code = 400)    { return json(code, { error: msg }); }

function extFromContentType(ct = "") {
  if (/png/i.test(ct)) return "png";
  if (/gif/i.test(ct)) return "gif";
  if (/webp/i.test(ct)) return "webp";
  if (/svg/i.test(ct)) return "svg";
  return "jpg";
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };
  if (event.httpMethod !== "POST")     return json(405, { error: "Method Not Allowed" });

  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return bad("Invalid JSON"); }

  const { threadId, messageDocId, imageUrl } = body;
  if (!threadId || !messageDocId || !imageUrl) return bad("Missing threadId, messageDocId, or imageUrl");
  if (!/^https?:\/\//i.test(imageUrl)) return bad("imageUrl must be http(s)");

  // Safety: only mirror known Etsy image hosts. Stops the function being used
  // as an arbitrary URL fetcher.
  const allowedHosts = /(etsystatic\.com|etsystatic\.net|i\.etsystatic\.com|img\.etsystatic\.com|etsy\.com)$/i;
  try {
    const u = new URL(imageUrl);
    const host = u.hostname;
    if (!allowedHosts.test(host)) {
      return bad(`Host ${host} not in Etsy image allowlist`);
    }
  } catch {
    return bad("imageUrl is not a valid URL");
  }

  try {
    // Fetch the image server-side (bypasses browser CORS).
    const imgRes = await fetch(imageUrl);
    if (!imgRes.ok) return bad(`Upstream fetch failed: ${imgRes.status}`, 502);
    const ct = imgRes.headers.get("content-type") || "image/jpeg";
    const buf = await imgRes.buffer();

    // Build a stable path. Hash-named so the same image uploaded twice doesn't duplicate.
    const hash = crypto.createHash("sha1").update(imageUrl).digest("hex").slice(0, 16);
    const ext  = extFromContentType(ct);
    const storagePath = `etsymail/${threadId}/${messageDocId}/${hash}.${ext}`;

    // Upload to Storage. predefinedAcl removed — bucket policy controls ACLs.
    const file = bucket.file(storagePath);
    await file.save(buf, {
      metadata: {
        contentType: ct,
        cacheControl: "public, max-age=31536000",
        metadata: { sourceUrl: imageUrl, mirroredAt: String(Date.now()) }
      },
      resumable: false
    });

    // Signed URL valid 7 days. The operator UI can render this; if you want
    // long-term public URLs, switch to `file.makePublic()` and use the gs:// path.
    const [signedUrl] = await file.getSignedUrl({
      action: "read",
      expires: Date.now() + 7 * 24 * 60 * 60 * 1000
    });

    // Record on the message doc
    const msgRef = db.collection(THREADS_COLL).doc(threadId).collection("messages").doc(messageDocId);
    await msgRef.set({
      storageImagePaths  : FV.arrayUnion(storagePath),
      storageMirrorState : "mirrored",
      updatedAt          : FV.serverTimestamp()
    }, { merge: true });

    return json(200, {
      success    : true,
      storagePath,
      publicUrl  : signedUrl,
      bytes      : buf.length,
      contentType: ct
    });

  } catch (err) {
    console.error("etsyMailMirrorImage error:", err);
    // Record failure state on the message so the UI can show "mirror failed"
    try {
      await db.collection(THREADS_COLL).doc(threadId).collection("messages").doc(messageDocId)
        .set({ storageMirrorState: "failed", updatedAt: FV.serverTimestamp() }, { merge: true });
    } catch {}
    return json(500, { error: err.message || String(err) });
  }
};
