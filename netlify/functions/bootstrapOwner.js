/*  netlify/functions/bootstrapOwner.js
 *
 *  v4.1 — One-shot owner-account bootstrap, callable from a browser.
 *
 *  Purpose: seed the very first owner account in EtsyMail_Operators so
 *  the new login flow has someone to authenticate. Without this, the
 *  inbox is locked behind a login modal that nobody can satisfy
 *  (chicken-and-egg: addOperator requires an owner session, but no
 *  owner session can exist until an owner exists).
 *
 *  ─── Self-disabling safety ──────────────────────────────────────────
 *
 *  This endpoint REFUSES to run once any owner exists with a password
 *  set. So it works exactly once — first call seeds the owner, all
 *  subsequent calls return 403 BOOTSTRAP_ALREADY_DONE. No need to
 *  delete the file after use, though you can if you want belt-and-
 *  suspenders security.
 *
 *  Outer gate is still X-EtsyMail-Secret like every other function.
 *  Without that header the request is rejected before we even check
 *  whether bootstrap is allowed. Combined with the self-disabling
 *  safety, this gives two layers of protection: even if the secret
 *  leaked, the endpoint can only ever be used once total.
 *
 *  ─── Usage from the browser ────────────────────────────────────────
 *
 *  Open the inbox (etsy-mail-1.html). The Settings → Inbox secret
 *  modal should auto-prompt you for the X-EtsyMail-Secret if it's
 *  not stored yet. Save the secret. THEN, in DevTools console:
 *
 *      await fetch("/.netlify/functions/bootstrapOwner", {
 *        method: "POST",
 *        headers: {
 *          "Content-Type": "application/json",
 *          "X-EtsyMail-Secret": localStorage.getItem("etsymail_secret")
 *        },
 *        body: JSON.stringify({
 *          username   : "paul",
 *          displayName: "Paul K",
 *          password   : "your-strong-password-here"
 *        })
 *      }).then(r => r.json());
 *
 *  Expected: {ok: true, username: "paul"}.
 *  After that you can sign in via the regular login modal, then add
 *  satellites via Settings → Operators.
 *
 *  ─── What it writes ─────────────────────────────────────────────────
 *
 *  EtsyMail_Operators/{username} = {
 *    username, displayName, role:"owner",
 *    passwordHash, salt, iterations, digest,
 *    createdAt, createdBy:"bootstrap-endpoint"
 *  }
 *
 *  Same shape etsyMailAuth.js writes for ordinary addOperator calls
 *  — login flow doesn't care which path created the doc.
 */

"use strict";

const admin = require("./firebaseAdmin");
const crypto = require("crypto");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const OPERATORS_COLL = "EtsyMail_Operators";

// Same PBKDF2 parameters as etsyMailAuth.js. Duplicated here rather
// than imported so this file can be deleted post-bootstrap without
// breaking anything. (etsyMailAuth never imports from us.)
const PBKDF2_ITERATIONS = 200_000;
const PBKDF2_KEY_BYTES  = 64;
const PBKDF2_DIGEST     = "sha512";
const SALT_BYTES        = 32;

function json(statusCode, body) {
  return {
    statusCode,
    headers: { ...CORS, "Content-Type": "application/json" },
    body: JSON.stringify(body)
  };
}

function hashSecret(plaintext) {
  const salt = crypto.randomBytes(SALT_BYTES);
  const hash = crypto.pbkdf2Sync(
    String(plaintext), salt, PBKDF2_ITERATIONS, PBKDF2_KEY_BYTES, PBKDF2_DIGEST
  );
  return {
    hash      : hash.toString("hex"),
    salt      : salt.toString("hex"),
    iterations: PBKDF2_ITERATIONS,
    digest    : PBKDF2_DIGEST
  };
}

function isValidUsername(s) {
  return typeof s === "string" && /^[a-z0-9_-]{3,32}$/.test(s);
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };
  if (event.httpMethod !== "POST")    return json(405, { error: "POST required" });

  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  // Self-disable: refuse if any owner with a password already exists.
  // Soft-deleted (revokedAt) owners don't count; their passwords are
  // still hashed but the doc isn't usable for login. We allow bootstrap
  // again in that edge case, on the theory that an all-revoked-owners
  // shop is functionally identical to a fresh shop.
  let bootstrapAllowed = true;
  try {
    const owners = await db.collection(OPERATORS_COLL)
      .where("role", "==", "owner")
      .get();
    for (const d of owners.docs) {
      const data = d.data() || {};
      if (data.passwordHash && !data.revokedAt) {
        bootstrapAllowed = false;
        break;
      }
    }
  } catch (e) {
    console.error("bootstrapOwner: pre-check failed:", e.message);
    return json(500, { error: "Couldn't verify bootstrap eligibility: " + e.message });
  }

  if (!bootstrapAllowed) {
    return json(403, {
      error  : "Bootstrap already done — an active owner with a password exists",
      reason : "BOOTSTRAP_ALREADY_DONE"
    });
  }

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return json(400, { error: "Invalid JSON body" }); }

  const username = String(body.username || "").trim().toLowerCase();
  const displayName = String(body.displayName || "").trim() || username;
  const password = String(body.password || "");

  if (!isValidUsername(username)) {
    return json(400, { error: "Username must be 3-32 chars, lowercase letters/digits/underscore/dash" });
  }
  if (password.length < 8) {
    return json(400, { error: "Password must be at least 8 characters" });
  }

  const ref = db.collection(OPERATORS_COLL).doc(username);
  const fresh = hashSecret(password);

  try {
    // Use set without merge so any prior partial doc gets fully
    // replaced. We've already verified there's no usable owner; if
    // a stale revoked record exists for this username, replacing it
    // is the right behavior.
    await ref.set({
      username,
      displayName,
      role        : "owner",
      passwordHash: fresh.hash,
      salt        : fresh.salt,
      iterations  : fresh.iterations,
      digest      : fresh.digest,
      createdAt   : FV.serverTimestamp(),
      createdBy   : "bootstrap-endpoint"
    });
  } catch (e) {
    console.error("bootstrapOwner: write failed:", e.message);
    return json(500, { error: "Couldn't write owner record: " + e.message });
  }

  return json(200, {
    ok: true,
    username,
    note: "Sign in at the inbox with this username. You can now (and should) delete bootstrapOwner.js from netlify/functions/."
  });
};
