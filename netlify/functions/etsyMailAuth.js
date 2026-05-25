/*  netlify/functions/etsyMailAuth.js
 *
 *  v4.1 — Multi-operator auth endpoint.
 *
 *  Username + password login, 30-day Firestore-backed sessions, and
 *  owner-only operator management. Builds on the existing
 *  EtsyMail_Operators collection (which previously held only
 *  role/displayName) by adding password fields and introducing
 *  EtsyMail_Sessions for issued tokens.
 *
 *  ─── Operator schema (after this change) ───────────────────────────
 *
 *  EtsyMail_Operators/{username} = {
 *    username        : "paul",         // doc id; canonical login name
 *    displayName     : "Paul K",       // shown in UI, used in audit rows
 *    role            : "owner" | "operator",
 *    passwordHash    : <hex>,          // pbkdf2-sha512(password, salt, 200000, 64)
 *    salt            : <hex>,          // 32 random bytes per operator
 *    iterations      : 200000,
 *    digest          : "sha512",
 *    createdAt       : Timestamp,
 *    createdBy       : "<owner username>",
 *    lastLoginAt     : Timestamp | null,
 *    revokedAt       : Timestamp | null  // soft delete
 *  }
 *
 *  EtsyMail_Sessions/{token} = {
 *    username        : "paul",
 *    createdAt       : Timestamp,
 *    expiresAt       : Timestamp,      // createdAt + 30 days
 *    lastSeenAt      : Timestamp,      // bumped on each successful auth
 *    userAgent       : <string|null>   // best-effort browser fingerprint
 *  }
 *
 *  ─── Ops ────────────────────────────────────────────────────────────
 *
 *    POST { op:"login", username, password }
 *      Public (gated only by the X-EtsyMail-Secret bootstrap header).
 *      Returns { ok:true, sessionToken, username, displayName, role,
 *                expiresAtMs } or 401 with { error }.
 *
 *    POST { op:"logout" }
 *      Authenticated (X-EtsyMail-Session). Deletes the session doc and
 *      clears the role/session caches.
 *
 *    POST { op:"currentUser" }
 *      Authenticated. Returns who the session belongs to. Used by the
 *      front-end on page load to validate a stored token and populate
 *      operator state.
 *
 *    POST { op:"setMyPassword", currentPassword, newPassword }
 *      Authenticated. Any user can change their own password by
 *      proving knowledge of the current one. Doesn't require owner.
 *      All other sessions for that user are revoked.
 *
 *    POST { op:"listOperators" }
 *      Owner-only. Returns the operator roster minus password material.
 *
 *    POST { op:"addOperator", username, displayName, role, password }
 *      Owner-only. Creates an EtsyMail_Operators doc. role must be
 *      "owner" or "operator". Idempotent: if username exists and is
 *      revoked, the row is rehydrated; if username exists and is
 *      active, returns 409.
 *
 *    POST { op:"removeOperator", username }
 *      Owner-only. Soft-deletes (sets revokedAt). Active sessions for
 *      that user are deleted so they're booted within ~60s. Owner
 *      cannot remove the last remaining owner (returns 400).
 *
 *    POST { op:"resetOperatorPassword", username, newPassword }
 *      Owner-only. Sets a new password on behalf of an operator (e.g.
 *      they forgot it). Their other sessions are revoked.
 *
 *  ─── Security notes ────────────────────────────────────────────────
 *
 *    - All POSTs require the X-EtsyMail-Secret bootstrap header. Auth
 *      endpoints are NOT publicly reachable — without the dev-time
 *      secret, no operator can even attempt to log in. This is by
 *      design: it prevents probing usernames over the public internet.
 *    - Failed-login attempts are not rate-limited at this layer (could
 *      add later). Per-operator backoff would prevent brute force on
 *      small wordlists; relying on the secret as the outer gate is
 *      acceptable for a closed-team tool.
 *    - All password material flows through hashSecret/verifySecret
 *      below — same PBKDF2-SHA512 + per-secret salt + constant-time
 *      compare pattern used for the master-purge password.
 *    - Session tokens are 32 random bytes (256 bits) base64url-encoded.
 *      Treated as opaque by callers; not signed (no JWT). Trust comes
 *      from possession + Firestore lookup.
 */

"use strict";

const admin = require("./firebaseAdmin");
const crypto = require("crypto");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const {
  requireSession,
  requireOwnerSession,
  invalidateRoleCache,
  invalidateSessionCache,
  OPERATORS_COLL,
  SESSIONS_COLL
} = require("./_etsyMailRoles");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const AUDIT_COLL = "EtsyMail_Audit";

const PBKDF2_ITERATIONS = 200_000;
const PBKDF2_KEY_BYTES  = 64;
const PBKDF2_DIGEST     = "sha512";
const SALT_BYTES        = 32;

// 30-day session lifetime, expressed in ms.
const SESSION_LIFETIME_MS = 30 * 24 * 60 * 60 * 1000;

// ─── Helpers ────────────────────────────────────────────────────────────

function json(statusCode, body) {
  return {
    statusCode,
    headers: { ...CORS, "Content-Type": "application/json" },
    body: JSON.stringify(body)
  };
}
function bad(msg, code = 400) { return json(code, { error: msg }); }

/** Hash a secret with a fresh random salt. Same shape as the master-purge
 *  password helper in etsyMailThreads.js; duplicated here rather than
 *  imported because both files are reachable independently and we want
 *  to avoid an unnecessary cross-file dependency. */
function hashSecret(plaintext) {
  const salt = crypto.randomBytes(SALT_BYTES);
  const hash = crypto.pbkdf2Sync(
    String(plaintext),
    salt,
    PBKDF2_ITERATIONS,
    PBKDF2_KEY_BYTES,
    PBKDF2_DIGEST
  );
  return {
    hash       : hash.toString("hex"),
    salt       : salt.toString("hex"),
    iterations : PBKDF2_ITERATIONS,
    digest     : PBKDF2_DIGEST
  };
}

/** Constant-time compare a plaintext attempt against a stored
 *  { hash, salt, iterations, digest } record. Returns boolean. */
function verifySecret(plaintext, stored) {
  if (!stored || !stored.hash || !stored.salt) return false;
  const iterations = stored.iterations || PBKDF2_ITERATIONS;
  const digest     = stored.digest     || PBKDF2_DIGEST;
  let attempt;
  try {
    attempt = crypto.pbkdf2Sync(
      String(plaintext),
      Buffer.from(stored.salt, "hex"),
      iterations,
      PBKDF2_KEY_BYTES,
      digest
    );
  } catch {
    return false;
  }
  let storedBuf;
  try { storedBuf = Buffer.from(stored.hash, "hex"); }
  catch { return false; }
  if (attempt.length !== storedBuf.length) return false;
  return crypto.timingSafeEqual(attempt, storedBuf);
}

/** Generate a fresh session token: 32 random bytes, base64url. */
function generateSessionToken() {
  return crypto.randomBytes(32).toString("base64url");
}

/** Validate username syntax — lowercase alphanumeric + underscore + dash,
 *  3-32 chars. Keeps doc IDs Firestore-safe and prevents weird login
 *  collisions like trailing-whitespace usernames. */
function isValidUsername(s) {
  return typeof s === "string" && /^[a-z0-9_-]{3,32}$/.test(s);
}

async function writeAudit({ eventType, actor, payload = {} }) {
  try {
    await db.collection(AUDIT_COLL).add({
      threadId : null,
      draftId  : null,
      eventType,
      actor    : actor || "system:auth",
      payload,
      createdAt: FV.serverTimestamp()
    });
  } catch (e) {
    console.warn("auth audit write failed:", e.message);
  }
}

async function deleteSessionsForUser(username) {
  const snap = await db.collection(SESSIONS_COLL)
    .where("username", "==", username)
    .limit(500)
    .get();
  if (snap.empty) return 0;
  const batch = db.batch();
  for (const d of snap.docs) {
    batch.delete(d.ref);
    invalidateSessionCache(d.id);
  }
  await batch.commit();
  return snap.size;
}

// ─── Op handlers ────────────────────────────────────────────────────────

async function handleLogin(body) {
  const username = String(body.username || "").trim().toLowerCase();
  const password = String(body.password || "");
  const userAgent = String(body.userAgent || "").slice(0, 200) || null;

  if (!username || !password) return bad("Username and password required");

  const opSnap = await db.collection(OPERATORS_COLL).doc(username).get();
  if (!opSnap.exists) {
    // Generic message — don't leak whether the username exists.
    await writeAudit({ eventType: "login_failed", actor: username, payload: { reason: "no_such_user" } });
    return json(401, { error: "Invalid username or password" });
  }
  const op = opSnap.data() || {};
  if (op.revokedAt) {
    await writeAudit({ eventType: "login_failed", actor: username, payload: { reason: "revoked" } });
    return json(401, { error: "Invalid username or password" });
  }
  if (!op.passwordHash || !op.salt) {
    // This operator was created in the v4.0 era before passwords. Reject
    // with a clear message so the owner knows to set a password for them.
    await writeAudit({ eventType: "login_failed", actor: username, payload: { reason: "no_password_set" } });
    return json(401, { error: "No password configured. Ask the owner to set one." });
  }

  const ok = verifySecret(password, {
    hash: op.passwordHash, salt: op.salt,
    iterations: op.iterations, digest: op.digest
  });
  if (!ok) {
    await writeAudit({ eventType: "login_failed", actor: username, payload: { reason: "bad_password" } });
    return json(401, { error: "Invalid username or password" });
  }

  // Issue session.
  const token = generateSessionToken();
  const expiresAtMs = Date.now() + SESSION_LIFETIME_MS;
  const expiresAt = admin.firestore.Timestamp.fromMillis(expiresAtMs);

  await db.collection(SESSIONS_COLL).doc(token).set({
    username,
    createdAt : FV.serverTimestamp(),
    expiresAt,
    lastSeenAt: FV.serverTimestamp(),
    userAgent
  });
  await db.collection(OPERATORS_COLL).doc(username).set({
    lastLoginAt: FV.serverTimestamp()
  }, { merge: true });

  await writeAudit({
    eventType: "login_success",
    actor: username,
    payload: { tokenPrefix: token.slice(0, 8), userAgent }
  });

  return json(200, {
    ok          : true,
    sessionToken: token,
    username,
    displayName : op.displayName || username,
    role        : op.role || "operator",
    expiresAtMs
  });
}

async function handleLogout(event) {
  const token = (event.headers && (event.headers["x-etsymail-session"] || event.headers["X-EtsyMail-Session"])) || null;
  if (!token) return bad("No session token to log out");
  // Don't fail if the token is already gone — logout should be idempotent.
  const ref = db.collection(SESSIONS_COLL).doc(token);
  let username = null;
  try {
    const snap = await ref.get();
    if (snap.exists) username = (snap.data() || {}).username || null;
    if (snap.exists) await ref.delete();
  } catch (e) {
    console.warn("logout delete failed:", e.message);
  }
  invalidateSessionCache(token);
  await writeAudit({ eventType: "logout", actor: username || "unknown" });
  return json(200, { ok: true });
}

async function handleCurrentUser(event) {
  const sess = await requireSession(event);
  if (!sess.ok) return json(401, { error: "Not authenticated", reason: sess.reason });
  return json(200, {
    ok: true,
    username   : sess.username,
    displayName: sess.displayName,
    role       : sess.role
  });
}

async function handleSetMyPassword(event, body) {
  const sess = await requireSession(event);
  if (!sess.ok) return json(401, { error: "Not authenticated", reason: sess.reason });

  const cur = String(body.currentPassword || "");
  const next = String(body.newPassword || "");
  if (!cur || !next) return bad("currentPassword and newPassword required");
  if (next.length < 8) return bad("New password must be at least 8 characters");

  const ref = db.collection(OPERATORS_COLL).doc(sess.username);
  const snap = await ref.get();
  if (!snap.exists) return bad("Operator record missing", 404);
  const op = snap.data() || {};
  const verified = verifySecret(cur, {
    hash: op.passwordHash, salt: op.salt,
    iterations: op.iterations, digest: op.digest
  });
  if (!verified) {
    await writeAudit({ eventType: "password_change_failed", actor: sess.username });
    return json(401, { error: "Current password is incorrect" });
  }

  const fresh = hashSecret(next);
  await ref.set({
    passwordHash: fresh.hash,
    salt        : fresh.salt,
    iterations  : fresh.iterations,
    digest      : fresh.digest,
    passwordChangedAt: FV.serverTimestamp()
  }, { merge: true });

  // Revoke other sessions but keep THIS session live so the operator
  // doesn't get kicked out of the tab they're using.
  const currentToken = event.headers["x-etsymail-session"] || event.headers["X-EtsyMail-Session"];
  const others = await db.collection(SESSIONS_COLL)
    .where("username", "==", sess.username)
    .get();
  const batch = db.batch();
  let killed = 0;
  for (const d of others.docs) {
    if (d.id === currentToken) continue;
    batch.delete(d.ref);
    invalidateSessionCache(d.id);
    killed++;
  }
  if (killed) await batch.commit();

  await writeAudit({
    eventType: "password_changed",
    actor: sess.username,
    payload: { otherSessionsRevoked: killed }
  });
  return json(200, { ok: true, otherSessionsRevoked: killed });
}

async function handleListOperators(event) {
  const sess = await requireOwnerSession(event);
  if (!sess.ok) return json(403, { error: "Owner role required", reason: sess.reason });

  const snap = await db.collection(OPERATORS_COLL).get();
  const ops = [];
  for (const d of snap.docs) {
    const data = d.data() || {};
    ops.push({
      username    : d.id,
      displayName : data.displayName || d.id,
      role        : data.role || "operator",
      createdAt   : data.createdAt && data.createdAt.toMillis ? data.createdAt.toMillis() : null,
      lastLoginAt : data.lastLoginAt && data.lastLoginAt.toMillis ? data.lastLoginAt.toMillis() : null,
      revoked     : !!data.revokedAt,
      hasPassword : !!(data.passwordHash && data.salt)
    });
  }
  ops.sort((a, b) => (a.username || "").localeCompare(b.username || ""));
  return json(200, { ok: true, operators: ops });
}

async function handleAddOperator(event, body) {
  const sess = await requireOwnerSession(event);
  if (!sess.ok) return json(403, { error: "Owner role required", reason: sess.reason });

  const username = String(body.username || "").trim().toLowerCase();
  const displayName = String(body.displayName || "").trim() || username;
  const role = String(body.role || "operator").toLowerCase();
  const password = String(body.password || "");

  if (!isValidUsername(username)) {
    return bad("Username must be 3-32 chars, lowercase letters/digits/underscore/dash");
  }
  if (!["owner", "operator"].includes(role)) {
    return bad("role must be 'owner' or 'operator'");
  }
  if (password.length < 8) {
    return bad("Password must be at least 8 characters");
  }

  const ref = db.collection(OPERATORS_COLL).doc(username);
  const existing = await ref.get();
  if (existing.exists && !(existing.data() || {}).revokedAt) {
    return json(409, { error: `Operator '${username}' already exists` });
  }

  const fresh = hashSecret(password);
  // v4.1.1 — Use a full-overwrite set() (merge:false) to drop ANY
  // prior fields on this doc. The previous version mixed merge:false
  // with FV.delete(), which Firestore rejects ("FieldValue.delete()
  // can only be used in update() or set() with {merge:true}"). The
  // full-overwrite approach is cleaner anyway: a re-added operator
  // gets a fresh, well-formed record with no leftover fields from
  // any prior soft-revoked state.
  await ref.set({
    username,
    displayName,
    role,
    passwordHash: fresh.hash,
    salt        : fresh.salt,
    iterations  : fresh.iterations,
    digest      : fresh.digest,
    createdAt   : FV.serverTimestamp(),
    createdBy   : sess.username
    // (no revokedAt — by omission, the field is absent on the new doc)
  }, { merge: false });

  invalidateRoleCache(username);

  await writeAudit({
    eventType: "operator_added",
    actor: sess.username,
    payload: { username, displayName, role }
  });
  return json(200, { ok: true, username });
}

async function handleRemoveOperator(event, body) {
  const sess = await requireOwnerSession(event);
  if (!sess.ok) return json(403, { error: "Owner role required", reason: sess.reason });

  // v4.1.1 — Remove must be permissive about what username it accepts.
  // The doc ID in Firestore is whatever string was used to create the
  // record, which may include legacy entries with capitals or spaces
  // (e.g. an operator created out-of-band before username validation
  // existed, or via direct Firestore-console editing). The strict
  // validator is right for ADD (we want clean data going in) but
  // wrong for REMOVE (we need to clean out whatever's already there).
  // We only require:
  //   - non-empty after trim
  //   - <= 200 chars (Firestore doc ID limit is 1500 bytes; 200 chars
  //     is plenty and protects against absurd input)
  // We do NOT lowercase — Firestore doc IDs are case-sensitive, and
  // lowercasing the lookup would miss "Paul K" while finding "paul".
  const username = String(body.username || "").trim();
  if (!username) return bad("username required");
  if (username.length > 200) return bad("username too long");

  if (username === sess.username) {
    return bad("You cannot remove your own account");
  }

  const ref = db.collection(OPERATORS_COLL).doc(username);
  const snap = await ref.get();
  if (!snap.exists) return bad("No such operator", 404);
  const data = snap.data() || {};

  if (data.role === "owner") {
    // Don't let the last remaining owner be removed.
    const owners = await db.collection(OPERATORS_COLL).where("role", "==", "owner").get();
    const activeOwners = owners.docs.filter(d => !(d.data() || {}).revokedAt);
    // Count this doc as one of the active owners only if it's currently
    // active; if it's already revoked, removing it again doesn't reduce
    // the active-owner count.
    const isCurrentlyActive = !data.revokedAt;
    const activeCountAfter = activeOwners.length - (isCurrentlyActive ? 1 : 0);
    if (activeCountAfter < 1) {
      return bad("Cannot remove the last remaining active owner");
    }
  }

  // For malformed legacy docs (e.g. ones lacking passwordHash / created
  // outside the addOperator path), do a HARD delete instead of soft-
  // revoke. Soft-revoke leaves the row in the listOperators output
  // forever, which is what's frustrating you right now. Real operator
  // accounts that have been used (passwordHash present + a lastLoginAt)
  // get soft-revoked so the audit trail is preserved.
  const isLegacyMalformed = !data.passwordHash || !data.salt;
  if (isLegacyMalformed) {
    await ref.delete();
  } else {
    await ref.set({
      revokedAt: FV.serverTimestamp(),
      revokedBy: sess.username
    }, { merge: true });
  }

  invalidateRoleCache(username);
  const killedSessions = await deleteSessionsForUser(username);

  await writeAudit({
    eventType: "operator_removed",
    actor: sess.username,
    payload: { username, killedSessions, hardDeleted: isLegacyMalformed }
  });
  return json(200, { ok: true, killedSessions, hardDeleted: isLegacyMalformed });
}

async function handleResetOperatorPassword(event, body) {
  const sess = await requireOwnerSession(event);
  if (!sess.ok) return json(403, { error: "Owner role required", reason: sess.reason });

  // v4.1.1 — Same permissive-lookup rule as handleRemoveOperator.
  // We're operating on an existing doc by ID; that ID can be anything
  // the original creator put there. Strict validation belongs on the
  // add path, not on lookups against existing rows.
  const username = String(body.username || "").trim();
  if (!username) return bad("username required");
  if (username.length > 200) return bad("username too long");
  const newPassword = String(body.newPassword || "");
  if (newPassword.length < 8) return bad("New password must be at least 8 characters");

  const ref = db.collection(OPERATORS_COLL).doc(username);
  const snap = await ref.get();
  if (!snap.exists) return bad("No such operator", 404);

  const fresh = hashSecret(newPassword);
  await ref.set({
    passwordHash: fresh.hash,
    salt        : fresh.salt,
    iterations  : fresh.iterations,
    digest      : fresh.digest,
    passwordChangedAt: FV.serverTimestamp(),
    passwordChangedBy: sess.username
  }, { merge: true });

  const killed = await deleteSessionsForUser(username);

  await writeAudit({
    eventType: "operator_password_reset",
    actor: sess.username,
    payload: { username, killedSessions: killed }
  });
  return json(200, { ok: true, killedSessions: killed });
}

// ─── Handler ────────────────────────────────────────────────────────────

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };
  if (event.httpMethod !== "POST")    return bad("POST required", 405);

  // X-EtsyMail-Secret as outer gate. Without this, no auth attempt
  // even reaches the username/password check — protects against
  // public probing.
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return bad("Invalid JSON body"); }

  const op = String(body.op || "").toLowerCase();

  try {
    switch (op) {
      case "login":                  return await handleLogin(body);
      case "logout":                 return await handleLogout(event);
      case "currentuser":            return await handleCurrentUser(event);
      case "setmypassword":          return await handleSetMyPassword(event, body);
      case "listoperators":          return await handleListOperators(event);
      case "addoperator":            return await handleAddOperator(event, body);
      case "removeoperator":         return await handleRemoveOperator(event, body);
      case "resetoperatorpassword":  return await handleResetOperatorPassword(event, body);
      default:
        return bad(`Unknown op '${body.op}'`);
    }
  } catch (err) {
    console.error("etsyMailAuth unhandled error:", err);
    return json(500, { error: err.message || "Unknown error" });
  }
};
