/* netlify/functions/etsyMailCollateralUpload.js
 *
 * Owner-only upload endpoint for Sales Agent collateral.
 * Stores files in Firebase Storage under:
 *   etsymail/collateral/<kind>/<collateralId>/<timestamp>_<filename>
 *
 * Then upserts EtsyMail_Collateral/<collateralId> with the public Firebase
 * download URL the sales agent can reference through get_collateral().
 */

const crypto = require("crypto");

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const { requireOwner, logUnauthorized } = require("./_etsyMailRoles");

const db = admin.firestore();
const bucket = admin.storage().bucket();
const FV = admin.firestore.FieldValue;

const COLLATERAL_COLL = "EtsyMail_Collateral";
const AUDIT_COLL = "EtsyMail_Audit";

const VALID_KINDS = new Set(["line_sheet", "product_card", "lookbook", "image_set", "terms"]);
const ALLOWED_TYPES = new Set([
  "application/pdf",
  "image/png",
  "image/jpeg",
  "image/jpg",
  "image/webp",
  "image/gif"
]);
const MAX_UPLOAD_BYTES = 10 * 1024 * 1024;

function json(statusCode, body) {
  return { statusCode, headers: { ...CORS, "Content-Type": "application/json" }, body: JSON.stringify(body) };
}
function bad(msg, code = 400, extra = {}) { return json(code, { error: msg, ...extra }); }
function ok(body) { return json(200, { success: true, ...body }); }

function sanitizeDocId(raw) {
  const id = String(raw || "").trim()
    .toLowerCase()
    .replace(/[^a-z0-9_\-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 120);
  return id || null;
}

function sanitizeFileName(raw) {
  const name = String(raw || "collateral").trim()
    .replace(/[\\/\0<>:"|?*]+/g, "_")
    .replace(/\s+/g, "_")
    .slice(0, 160);
  return name || "collateral";
}

function inferExt(contentType, filename) {
  const fromName = String(filename || "").split(".").pop();
  if (fromName && fromName.length <= 8 && fromName !== filename) return fromName.toLowerCase();
  if (contentType === "application/pdf") return "pdf";
  if (contentType === "image/jpeg" || contentType === "image/jpg") return "jpg";
  if (contentType === "image/png") return "png";
  if (contentType === "image/webp") return "webp";
  if (contentType === "image/gif") return "gif";
  return "bin";
}

function normalizeKeywords(raw) {
  if (Array.isArray(raw)) {
    return raw.map(k => String(k).trim().toLowerCase()).filter(k => k.length >= 2 && k.length <= 60).slice(0, 30);
  }
  return String(raw || "")
    .split(",")
    .map(k => k.trim().toLowerCase())
    .filter(k => k.length >= 2 && k.length <= 60)
    .slice(0, 30);
}

function firebaseDownloadUrl(bucketName, storagePath, token) {
  return "https://firebasestorage.googleapis.com/v0/b/" + encodeURIComponent(bucketName) +
    "/o/" + encodeURIComponent(storagePath) +
    "?alt=media&token=" + encodeURIComponent(token);
}

async function writeAudit({ eventType, actor, payload = {}, outcome = "success", ruleViolations = [] }) {
  try {
    await db.collection(AUDIT_COLL).add({
      threadId: null,
      draftId: null,
      eventType,
      actor: actor || "system:collateralUpload",
      payload,
      createdAt: FV.serverTimestamp(),
      outcome,
      ruleViolations
    });
  } catch (e) {
    console.warn("collateral upload audit write failed:", e.message);
  }
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }
  if (event.httpMethod !== "POST") return json(405, { error: "Method Not Allowed" });

  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return bad("Invalid JSON body"); }

  const actor = body.actor || "operator";
  const ownerCheck = await requireOwner(actor);
  if (!ownerCheck.ok) {
    await logUnauthorized({
      actor,
      eventType: "collateral_upload_unauthorized",
      payload: { reason: ownerCheck.reason, name: body.name, category: body.category, kind: body.kind }
    });
    return json(403, { error: "Owner role required", reason: ownerCheck.reason });
  }

  const name = String(body.name || "").trim().slice(0, 200);
  const category = String(body.category || "").trim().toLowerCase().slice(0, 80);
  const kind = String(body.kind || "line_sheet").trim();
  const description = String(body.description || "").trim().slice(0, 1500);
  const keywords = normalizeKeywords(body.keywords);
  const contentType = String(body.contentType || "").trim().toLowerCase();
  const filename = sanitizeFileName(body.filename || name || "collateral");
  const bytesBase64 = String(body.bytesBase64 || "");

  if (!name) return bad("name required");
  if (!category) return bad("category required");
  if (!VALID_KINDS.has(kind)) return bad("kind invalid", 422, { allowed: Array.from(VALID_KINDS) });
  if (!ALLOWED_TYPES.has(contentType)) return bad("Unsupported file type", 415, { allowed: Array.from(ALLOWED_TYPES) });
  if (!bytesBase64) return bad("bytesBase64 required");

  let buf;
  try { buf = Buffer.from(bytesBase64, "base64"); }
  catch { return bad("Invalid base64 payload"); }

  if (!buf.length) return bad("Uploaded file is empty");
  if (buf.length > MAX_UPLOAD_BYTES) return bad("File too large", 413, { maxBytes: MAX_UPLOAD_BYTES });

  const id = sanitizeDocId(body.id || `${category}_${kind}_${name}`);
  if (!id) return bad("collateral id could not be generated");

  const ext = inferExt(contentType, filename);
  const baseName = filename.includes(".") ? filename : `${filename}.${ext}`;
  const safeKind = sanitizeDocId(kind) || "collateral";
  const token = crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString("hex");
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const storagePath = `etsymail/collateral/${safeKind}/${id}/${stamp}_${baseName}`;
  const file = bucket.file(storagePath);

  try {
    await file.save(buf, {
      resumable: false,
      contentType,
      metadata: {
        cacheControl: "public, max-age=31536000",
        metadata: {
          firebaseStorageDownloadTokens: token,
          uploadedBy: actor,
          collateralId: id
        }
      }
    });

    const url = firebaseDownloadUrl(bucket.name, storagePath, token);
    const ref = db.collection(COLLATERAL_COLL).doc(id);
    const snap = await ref.get();
    const doc = {
      id,
      name,
      category,
      kind,
      url,
      description,
      keywords,
      active: body.active === false ? false : true,
      storagePath,
      storageBucket: bucket.name,
      fileName: baseName,
      contentType,
      bytes: buf.length,
      approvedBy: actor,
      approvedAt: FV.serverTimestamp(),
      uploadedBy: actor,
      uploadedAt: FV.serverTimestamp(),
      lastUpdatedBy: actor,
      updatedAt: FV.serverTimestamp()
    };
    if (!snap.exists) {
      doc.createdBy = actor;
      doc.createdAt = FV.serverTimestamp();
      doc.lastUsedAt = null;
    }

    await ref.set(doc, { merge: true });

    await writeAudit({
      eventType: "collateral_uploaded",
      actor,
      payload: { id, category, kind, storagePath, fileName: baseName, bytes: buf.length }
    });

    return ok({
      id,
      item: { id, name, category, kind, url, description, keywords, active: doc.active, storagePath, fileName: baseName, contentType, bytes: buf.length },
      storagePath,
      url
    });
  } catch (err) {
    console.error("collateral upload failed:", err);
    await writeAudit({
      eventType: "collateral_upload_failed",
      actor,
      payload: { id, category, kind, filename: baseName, error: err.message },
      outcome: "failure",
      ruleViolations: ["UPLOAD_FAILED"]
    });
    return json(500, { error: err.message || String(err) });
  }
};
