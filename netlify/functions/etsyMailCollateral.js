/*  netlify/functions/etsyMailCollateral.js
 *
 *  v2.0 Step 2.5 — Curated collateral retrieval.
 *
 *  This is the smallest, lowest-risk piece in the v2.0 plan: pure
 *  retrieval of operator-curated URLs (line sheets, product cards,
 *  lookbooks, image sets, terms PDFs). The AI never uploads anything;
 *  the owner uploads files manually (e.g., to Firebase Storage or
 *  any external host) and registers the URL here.
 *
 *  ═══ FOUR OPS ═══════════════════════════════════════════════════════════
 *
 *  POST { op: "search", category?, kind?, keywords?, limit? }
 *      AI tool path. Returns matches by category + kind + optional
 *      keyword overlap. Both roles + the agent can call this.
 *
 *  POST { op: "list", includeInactive? }
 *      UI catalog browser. Both roles can read.
 *
 *  POST { op: "create", actor, item }
 *      OWNER-ONLY. Registers a new collateral entry pointing at an
 *      already-uploaded URL.
 *
 *  POST { op: "update", actor, id, patch }
 *      OWNER-ONLY. Edits metadata or marks active:false.
 *
 *  ═══ COLLATERAL SHAPE ═════════════════════════════════════════════════
 *
 *    EtsyMail_Collateral/{id} = {
 *      id,
 *      category    : "necklace" | "ring" | "wedding" | ...,
 *      kind        : "line_sheet" | "product_card" | "lookbook" |
 *                    "image_set" | "terms" |
 *                    "fit_reference" | "metal_comparison" |
 *                    "care_instructions" | "bracelet_sizing",
 *      name        : "<short display title>",
 *      url         : "<https://...>",
 *      description : "<one-paragraph blurb shown to the AI>",
 *      keywords    : ["<extra match terms>"],
 *      active      : true,
 *      lastUsedAt  : Timestamp | null,    // updated by search() when AI uses it
 *      approvedBy  : "<employeeName>",
 *      approvedAt  : Timestamp,
 *      createdBy, createdAt, updatedAt, lastUpdatedBy
 *    }
 *
 *  ═══ EXPORTED HELPER ══════════════════════════════════════════════════
 *
 *    module.exports.searchCollateral({ category, kind, keywords, limit })
 *
 *  Direct-import path for etsyMailSalesAgent (matches Step 1's
 *  searchListings pattern; no HTTP round-trip from agent's tool loop).
 */

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const { requireOwner, logUnauthorized } = require("./_etsyMailRoles");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

// v2.5 — Storage bucket for direct file uploads. The bucket is initialized
// in firebaseAdmin.js via FIREBASE_STORAGE_BUCKET; admin.storage().bucket()
// with no args returns the default. The Admin SDK bypasses Storage rules,
// so the user's fallback `allow read, write: if false` does not block our
// writes from this server-side path.
const bucket = admin.storage().bucket();

const COLLATERAL_COLL = "EtsyMail_Collateral";
const AUDIT_COLL      = "EtsyMail_Audit";

const VALID_KINDS = new Set([
  "line_sheet", "product_card", "lookbook", "image_set", "terms",
  // v5.24 — Sales agent auto-attachment kinds. The prompt/agent already
  // emits these for care, metals, fit, and bracelet sizing cards; the
  // collateral admin API must accept them too.
  "fit_reference", "metal_comparison", "care_instructions", "bracelet_sizing"
]);

const SAFE_FIELDS = new Set([
  "category", "kind", "name", "url", "description", "keywords", "active",
  // v2.5: storage metadata for files uploaded through op:"upload". Saved
  // alongside the doc so deleteFile can clean up the underlying GCS object
  // when an entry is deactivated/removed. Storing both fields together
  // avoids ambiguity — `url` is what the AI/customer sees, `storagePath`
  // is what we use to reach the file via the Admin SDK.
  "storagePath", "storageBucket", "uploadedFilename", "uploadedContentType",
  "uploadedSizeBytes"
]);

const SEARCH_DEFAULT_LIMIT = 5;
const SEARCH_MAX_LIMIT     = 20;

// v2.5 — Upload limits + allowlist
//
// Netlify functions have a 6 MB request body cap. Base64 inflates payloads
// by ~33%, so the largest raw file we can accept is ~4.5 MB. We cap at
// 4_500_000 to leave headroom for JSON envelope overhead.
//
// Allowlist is intentionally narrow: collateral is line sheets, product
// cards, lookbooks, image sets, terms PDFs. Anything else is suspicious
// (executables, archives, office docs that customers can't open inline).
const UPLOAD_MAX_BYTES = 4_500_000;
const UPLOAD_ALLOWED_CONTENT_TYPES = new Set([
  "image/jpeg", "image/png", "image/webp", "image/gif",
  "application/pdf"
]);
const UPLOAD_PATH_PREFIX = "etsymail-collateral";

// ─── Helpers ────────────────────────────────────────────────────────────

function json(statusCode, body) {
  return { statusCode, headers: { ...CORS, "Content-Type": "application/json" }, body: JSON.stringify(body) };
}
function bad(msg, code = 400) { return json(code, { error: msg }); }
function ok(body)             { return json(200, { ...body }); }

async function writeAudit({ eventType, actor = "system:collateral", payload = {},
                            outcome = "success", ruleViolations = [] }) {
  try {
    await db.collection(AUDIT_COLL).add({
      threadId: null, draftId: null,
      eventType, actor, payload,
      createdAt: FV.serverTimestamp(),
      outcome, ruleViolations
    });
  } catch (e) {
    console.warn("collateral audit write failed:", e.message);
  }
}

/** Trim a stored collateral doc to the shape returned to the AI / UI.
 *  The AI gets `description` (so it can decide whether to reference it)
 *  but not internal fields like `approvedBy` (no value to the AI). */
function trimForCaller(doc) {
  // v4.3.12 — Expose the storage-mirror fields that exist when the
  // collateral was uploaded through the system (op:"upload"). The
  // agent uses these to construct an `image` attachment on its draft
  // for line-sheet sends — without `storagePath` + `uploadedContentType`,
  // normalizeAttachments in etsyMailDraftSend rejects the entry.
  // Entries created via op:"create" (external URL, no upload) will
  // not have these fields; the agent treats those as link-only.
  //
  // v5.0.2 BUGFIX — field-name mismatch.
  // The upload pipeline (uploadCollateralFile, line ~400) writes the
  // MIME type as `uploadedContentType`. The previous version of this
  // function read `doc.contentType` and exposed it as `out.contentType`
  // — neither field name matches what's on disk OR what the consumer
  // (agent's findAttachableForKind) checks for. Result: every
  // operator-uploaded line sheet was failing the agent's attachable-
  // entry check because uploadedContentType was undefined on the
  // trimmed result. Heidi-thread bug confirmed via audit row showing
  // lineSheetAttach.reason = "no_active_collateral_for_kind" while
  // a perfectly valid line sheet existed in Firestore.
  // Fix: read `uploadedContentType` (the actual persisted field) AND
  // also tolerate `contentType` for legacy/future flexibility, and
  // expose under both names so any downstream consumer works.
  const out = {
    id          : doc.id,
    category    : doc.category,
    kind        : doc.kind,
    name        : doc.name,
    url         : doc.url,
    description : doc.description || "",
    keywords    : doc.keywords || [],
    active      : doc.active !== false
  };
  if (doc.storagePath)        out.storagePath        = doc.storagePath;

  // Resolve content type from either field name. uploadedContentType is
  // what the upload pipeline writes; contentType is the older name kept
  // for compatibility.
  const resolvedContentType = doc.uploadedContentType || doc.contentType || null;
  if (resolvedContentType) {
    out.uploadedContentType = resolvedContentType;
    out.contentType         = resolvedContentType;   // legacy alias
  }

  // Filename: same dual-name treatment.
  const resolvedFilename = doc.uploadedFilename || doc.fileName || null;
  if (resolvedFilename) {
    out.uploadedFilename = resolvedFilename;
    out.fileName         = resolvedFilename;   // legacy alias
  }

  if (typeof doc.uploadedSizeBytes === "number") out.uploadedSizeBytes = doc.uploadedSizeBytes;
  if (typeof doc.bytes === "number")             out.bytes             = doc.bytes;
  return out;
}

function sanitizePatch(rawPatch) {
  if (!rawPatch || typeof rawPatch !== "object") {
    return { ok: false, reason: "PATCH_NOT_OBJECT" };
  }
  const patch = {};
  for (const k of Object.keys(rawPatch)) {
    if (!SAFE_FIELDS.has(k)) continue;
    patch[k] = rawPatch[k];
  }
  if ("category" in patch) {
    if (typeof patch.category !== "string" || !patch.category.trim()) {
      return { ok: false, reason: "category_REQUIRED" };
    }
    patch.category = patch.category.trim();
  }
  if ("kind" in patch) {
    if (!VALID_KINDS.has(patch.kind)) {
      return { ok: false, reason: "kind_INVALID", allowed: Array.from(VALID_KINDS) };
    }
  }
  if ("name" in patch) {
    if (typeof patch.name !== "string" || !patch.name.trim()) {
      return { ok: false, reason: "name_REQUIRED" };
    }
    patch.name = patch.name.trim().slice(0, 200);
  }
  if ("url" in patch) {
    if (typeof patch.url !== "string" || !/^https?:\/\//.test(patch.url)) {
      return { ok: false, reason: "url_MUST_BE_HTTP_OR_HTTPS" };
    }
    if (patch.url.length > 2000) {
      return { ok: false, reason: "url_TOO_LONG" };
    }
  }
  if ("description" in patch) {
    if (typeof patch.description !== "string") {
      return { ok: false, reason: "description_MUST_BE_STRING" };
    }
    patch.description = patch.description.slice(0, 1500);
  }
  if ("keywords" in patch) {
    if (!Array.isArray(patch.keywords)) {
      return { ok: false, reason: "keywords_MUST_BE_ARRAY" };
    }
    patch.keywords = patch.keywords
      .map(k => String(k).trim().toLowerCase())
      .filter(k => k.length >= 2 && k.length <= 60)
      .slice(0, 30);
  }
  if ("active" in patch) patch.active = patch.active === true;

  return { ok: true, patch };
}

// ─── Search — exported for direct-import by sales agent ────────────────

/** Return collateral matching category + optional kind + optional
 *  keyword overlap. Active items only. Sorted by:
 *    1. exact category match score (always positive — non-matches return 0)
 *    2. kind match (if kind specified)
 *    3. keyword overlap (count of keywords matching)
 *    4. lastUsedAt desc (recency tie-breaker)
 *
 *  Returns: { matches: [trimForCaller(doc)], count, totalScored }
 */
async function searchCollateral({ category, kind, keywords, limit } = {}) {
  const cap = Math.max(1, Math.min(parseInt(limit, 10) || SEARCH_DEFAULT_LIMIT, SEARCH_MAX_LIMIT));

  // We always filter on active==true. Either: also filter on category
  // (cheaper) OR scan all active and score in memory (more flexible).
  // Use a category prefilter when supplied; otherwise scan up to 200
  // active items.
  let q = db.collection(COLLATERAL_COLL).where("active", "==", true);
  if (category && typeof category === "string" && category.trim()) {
    q = q.where("category", "==", category.trim());
  }
  const snap = await q.limit(200).get();

  if (snap.empty) return { matches: [], count: 0, totalScored: 0 };

  const wantKeywords = Array.isArray(keywords)
    ? keywords.map(k => String(k).trim().toLowerCase()).filter(k => k.length >= 2)
    : [];

  const scored = [];
  snap.forEach(d => {
    const data = d.data() || {};
    let score = 1;   // base score for being active + (optionally) category-filtered

    if (kind && data.kind === kind) score += 5;
    else if (kind && data.kind !== kind) return;   // kind requested but mismatch → skip

    if (wantKeywords.length > 0) {
      const itemKeywords = (data.keywords || []).map(k => String(k).toLowerCase());
      const desc = String(data.description || "").toLowerCase();
      const name = String(data.name || "").toLowerCase();
      let kwHits = 0;
      for (const w of wantKeywords) {
        if (itemKeywords.includes(w)) kwHits += 2;
        else if (name.includes(w))    kwHits += 2;
        else if (desc.includes(w))    kwHits += 1;
      }
      score += kwHits;
    }

    scored.push({ score, doc: { id: d.id, ...data } });
  });

  scored.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    const at = a.doc.lastUsedAt && a.doc.lastUsedAt.toMillis ? a.doc.lastUsedAt.toMillis() : 0;
    const bt = b.doc.lastUsedAt && b.doc.lastUsedAt.toMillis ? b.doc.lastUsedAt.toMillis() : 0;
    return bt - at;
  });

  const matches = scored.slice(0, cap).map(s => trimForCaller(s.doc));

  // Best-effort: stamp lastUsedAt on the matches so the UI shows what's
  // being referenced. Skip on error — don't block search on a write.
  if (matches.length > 0) {
    try {
      const batch = db.batch();
      for (const m of matches) {
        batch.set(db.collection(COLLATERAL_COLL).doc(m.id),
                  { lastUsedAt: FV.serverTimestamp() }, { merge: true });
      }
      await batch.commit();
    } catch (e) {
      console.warn("collateral lastUsedAt update failed:", e.message);
    }
  }

  return { matches, count: matches.length, totalScored: scored.length };
}

// ─── v2.5: Direct file upload to Firebase Storage ──────────────────────
//
// Path layout:
//   etsymail-collateral/<random-id>-<sanitized-filename>
//
// The random ID prevents filename collisions and makes the URL non-
// guessable for casual enumeration (it's not a security boundary —
// public ACL is set explicitly below — but it's good hygiene).
//
// Files are made publicly readable via file.makePublic(). That sets
// the underlying GCS object ACL, so the resulting URL
//   https://storage.googleapis.com/<bucket>/<path>
// works without auth — bypassing the Firebase Storage rules layer
// entirely. This is the right call for collateral: line sheets,
// lookbooks, etc. are meant to be linkable from Etsy replies that
// customers open in any browser without any login.
//
// If you want to lock down a specific entry later, use op:"deleteFile"
// to remove the GCS object — the Firestore doc still records its
// metadata for audit but the URL stops resolving.

function sanitizeFilename(name) {
  // Strip path separators, collapse spaces/odd chars to underscore, cap
  // length. Keep the extension. Two reasons we keep the original name in
  // the path: (a) operators recognize what they uploaded when browsing
  // the bucket; (b) the GCS URL preserves a meaningful filename when
  // shared.
  const raw = String(name || "file").trim();
  const lastDot = raw.lastIndexOf(".");
  const stem = lastDot > 0 ? raw.slice(0, lastDot) : raw;
  const ext  = lastDot > 0 ? raw.slice(lastDot) : "";
  const safeStem = stem.replace(/[^a-zA-Z0-9._-]+/g, "_").slice(0, 60) || "file";
  const safeExt  = ext.replace(/[^a-zA-Z0-9.]+/g, "").slice(0, 8);
  return safeStem + safeExt;
}

function randomId(len = 12) {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let s = "";
  for (let i = 0; i < len; i++) s += chars[Math.floor(Math.random() * chars.length)];
  return s;
}

/** Upload a file to Firebase Storage. Returns the metadata block that
 *  the caller persists onto the collateral doc. */
async function uploadCollateralFile({ filename, contentType, bytesBase64 }) {
  if (!filename || typeof filename !== "string") {
    return { ok: false, code: 400, error: "filename required" };
  }
  if (!contentType || typeof contentType !== "string") {
    return { ok: false, code: 400, error: "contentType required" };
  }
  if (!UPLOAD_ALLOWED_CONTENT_TYPES.has(contentType)) {
    return {
      ok: false, code: 415,
      error: `contentType '${contentType}' not allowed. Permitted: ${[...UPLOAD_ALLOWED_CONTENT_TYPES].join(", ")}`
    };
  }
  if (!bytesBase64 || typeof bytesBase64 !== "string") {
    return { ok: false, code: 400, error: "bytesBase64 required" };
  }

  // Decode + size-check
  let buffer;
  try {
    buffer = Buffer.from(bytesBase64, "base64");
  } catch (e) {
    return { ok: false, code: 400, error: "bytesBase64 was not valid base64: " + e.message };
  }
  if (buffer.length === 0) {
    return { ok: false, code: 400, error: "decoded file was empty" };
  }
  if (buffer.length > UPLOAD_MAX_BYTES) {
    return {
      ok: false, code: 413,
      error: `file is ${buffer.length} bytes; max ${UPLOAD_MAX_BYTES} bytes (~4.5 MB after base64 decode)`
    };
  }

  // Build the storage path. Random ID prefix + sanitized filename.
  const safe = sanitizeFilename(filename);
  const storagePath = `${UPLOAD_PATH_PREFIX}/${randomId()}-${safe}`;
  const file = bucket.file(storagePath);

  // Write + make public. We set the contentType explicitly so the GCS
  // URL serves with the right MIME (Chrome sniffs but Safari doesn't).
  // cacheControl is generous — collateral rarely changes; if a file is
  // edited, you upload a new one with a new path.
  try {
    await file.save(buffer, {
      contentType,
      resumable: false,    // small files; resumable adds latency
      metadata: {
        cacheControl: "public, max-age=86400",
        contentType
      }
    });
    await file.makePublic();
  } catch (e) {
    return { ok: false, code: 500, error: "Storage write failed: " + e.message };
  }

  const url = `https://storage.googleapis.com/${bucket.name}/${storagePath}`;
  return {
    ok: true,
    url,
    storagePath,
    storageBucket: bucket.name,
    uploadedFilename: safe,
    uploadedContentType: contentType,
    uploadedSizeBytes: buffer.length
  };
}

/** Remove a file from Firebase Storage. Idempotent — if the file is
 *  already gone, returns success. Used when an operator removes a
 *  collateral entry whose file was uploaded through this endpoint. */
async function deleteCollateralFile(storagePath) {
  if (!storagePath || typeof storagePath !== "string") {
    return { ok: false, code: 400, error: "storagePath required" };
  }
  // Defense-in-depth: only delete files under our prefix. Stops a
  // misbehaving caller from passing a path that points into another
  // app's tree (game-generator-1, listing-generator-1, etc.).
  if (!storagePath.startsWith(UPLOAD_PATH_PREFIX + "/")) {
    return {
      ok: false, code: 400,
      error: `storagePath must start with '${UPLOAD_PATH_PREFIX}/'`
    };
  }

  const file = bucket.file(storagePath);
  try {
    await file.delete({ ignoreNotFound: true });
    return { ok: true, deleted: true, storagePath };
  } catch (e) {
    return { ok: false, code: 500, error: "Storage delete failed: " + e.message };
  }
}

// ─── Handler ───────────────────────────────────────────────────────────

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }
  if (event.httpMethod !== "POST") {
    return json(405, { error: "Method Not Allowed" });
  }

  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return bad("Invalid JSON body"); }

  const op = body.op;
  if (!op) return bad("op required");

  try {
    if (op === "search") {
      const result = await searchCollateral({
        category: body.category,
        kind    : body.kind,
        keywords: body.keywords,
        limit   : body.limit
      });
      return ok({ success: true, ...result });
    }

    if (op === "list") {
      const includeInactive = body.includeInactive === true;
      let q = db.collection(COLLATERAL_COLL);
      if (!includeInactive) q = q.where("active", "==", true);
      const snap = await q.limit(500).get();
      const items = [];
      snap.forEach(d => items.push({ id: d.id, ...d.data() }));
      return ok({ success: true, items, count: items.length });
    }

    if (op === "create") {
      const { actor, item } = body;
      if (!item) return bad("item required");

      const ownerCheck = await requireOwner(actor);
      if (!ownerCheck.ok) {
        await logUnauthorized({
          actor,
          eventType: "collateral_create_unauthorized",
          payload  : { reason: ownerCheck.reason, item }
        });
        return json(403, { error: "Owner role required", reason: ownerCheck.reason });
      }

      const clean = sanitizePatch(item);
      if (!clean.ok) return json(422, { error: "Item rejected: " + clean.reason });

      // Required fields for a new entry
      if (!clean.patch.category) return bad("category required");
      if (!clean.patch.kind)     return bad("kind required");
      if (!clean.patch.name)     return bad("name required");
      if (!clean.patch.url)      return bad("url required");
      if (!("active" in clean.patch)) clean.patch.active = true;

      const doc = {
        ...clean.patch,
        approvedBy   : actor,
        approvedAt   : FV.serverTimestamp(),
        createdBy    : actor,
        createdAt    : FV.serverTimestamp(),
        lastUpdatedBy: actor,
        updatedAt    : FV.serverTimestamp(),
        lastUsedAt   : null
      };
      const ref = await db.collection(COLLATERAL_COLL).add(doc);

      await writeAudit({
        eventType: "collateral_created",
        actor,
        payload  : { id: ref.id, kind: clean.patch.kind, category: clean.patch.category }
      });

      return ok({ success: true, id: ref.id });
    }

    if (op === "update") {
      const { actor, id, patch } = body;
      if (!id || !patch) return bad("id and patch required");

      const ownerCheck = await requireOwner(actor);
      if (!ownerCheck.ok) {
        await logUnauthorized({
          actor,
          eventType: "collateral_update_unauthorized",
          payload  : { id, reason: ownerCheck.reason, attemptedPatch: patch }
        });
        return json(403, { error: "Owner role required", reason: ownerCheck.reason });
      }

      const clean = sanitizePatch(patch);
      if (!clean.ok) return json(422, { error: "Patch rejected: " + clean.reason });

      const cleanPatch = clean.patch;
      cleanPatch.lastUpdatedBy = actor;
      cleanPatch.updatedAt     = FV.serverTimestamp();

      await db.collection(COLLATERAL_COLL).doc(id).set(cleanPatch, { merge: true });

      await writeAudit({
        eventType: "collateral_updated",
        actor,
        payload  : { id, patch: cleanPatch }
      });

      return ok({ success: true, id });
    }

    /* ─── v2.5: Upload a file to Firebase Storage ───────────────
     * Owner-only. Accepts { actor, filename, contentType, bytesBase64 }.
     * Returns { url, storagePath, storageBucket, uploadedFilename,
     * uploadedContentType, uploadedSizeBytes } — the caller (the
     * collateral form in the inbox UI) auto-fills its URL field with
     * `url` and persists the rest as part of the `create` op below.
     *
     * This op DOES NOT create a Firestore doc — it only stages the
     * file. The form then calls `create` (or `update`) with the URL
     * + storagePath alongside the user-entered metadata. Splitting
     * the concerns lets the operator change their mind: if they
     * upload a file then click Cancel, an orphaned object remains in
     * Storage but no Firestore doc was created. A follow-up GC pass
     * could sweep `etsymail-collateral/*` objects with no matching
     * Firestore doc, but that's a future cleanup; orphans are
     * harmless and tiny. */
    if (op === "upload") {
      const { actor, filename, contentType, bytesBase64 } = body;

      const ownerCheck = await requireOwner(actor);
      if (!ownerCheck.ok) {
        await logUnauthorized({
          actor,
          eventType: "collateral_upload_unauthorized",
          payload  : { reason: ownerCheck.reason, filename, contentType }
        });
        return json(403, { error: "Owner role required", reason: ownerCheck.reason });
      }

      const result = await uploadCollateralFile({ filename, contentType, bytesBase64 });
      if (!result.ok) {
        return json(result.code || 500, { error: result.error });
      }

      await writeAudit({
        eventType: "collateral_file_uploaded",
        actor,
        payload  : {
          storagePath: result.storagePath,
          storageBucket: result.storageBucket,
          filename: result.uploadedFilename,
          contentType: result.uploadedContentType,
          sizeBytes: result.uploadedSizeBytes
        }
      });

      return ok({
        success            : true,
        url                : result.url,
        storagePath        : result.storagePath,
        storageBucket      : result.storageBucket,
        uploadedFilename   : result.uploadedFilename,
        uploadedContentType: result.uploadedContentType,
        uploadedSizeBytes  : result.uploadedSizeBytes
      });
    }

    /* ─── v2.5: Delete a previously-uploaded file from Storage ──
     * Owner-only. Accepts { actor, storagePath }. Idempotent — if
     * the object is already gone, returns success. Path is required
     * to start with the collateral prefix so a malformed caller
     * can't reach files owned by other apps in the same bucket.
     *
     * Typical caller: the inbox UI's collateral row "delete" button
     * (a future enhancement) OR an audit cleanup task that wants
     * to free Storage when a collateral entry is permanently
     * removed. The Firestore doc itself is NOT deleted by this op
     * — use the `update` op with `active: false` for soft-delete,
     * or delete the doc through firestoreProxy. */
    if (op === "deleteFile") {
      const { actor, storagePath } = body;

      const ownerCheck = await requireOwner(actor);
      if (!ownerCheck.ok) {
        await logUnauthorized({
          actor,
          eventType: "collateral_deletefile_unauthorized",
          payload  : { reason: ownerCheck.reason, storagePath }
        });
        return json(403, { error: "Owner role required", reason: ownerCheck.reason });
      }

      const result = await deleteCollateralFile(storagePath);
      if (!result.ok) {
        return json(result.code || 500, { error: result.error });
      }

      await writeAudit({
        eventType: "collateral_file_deleted",
        actor,
        payload  : { storagePath: result.storagePath }
      });

      return ok({ success: true, storagePath: result.storagePath });
    }

    return bad(`Unknown op '${op}'`);

  } catch (err) {
    console.error("collateral error:", err);
    return json(500, { error: err.message || String(err), op });
  }
};

// Exposed for direct import by etsyMailSalesAgent (Step 2 + 3 use this).
module.exports.searchCollateral = searchCollateral;
