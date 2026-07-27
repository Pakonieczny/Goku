/*  netlify/functions/_pricingResultStore.js
 *
 *  Firestore result channel for the background pricing function.
 *
 *  ═══ WHY THIS EXISTS ═══════════════════════════════════════════════════
 *
 *  etsyPricingApplyOne used to be a SYNCHRONOUS Netlify function. Its proxy
 *  chain always pays for a real verification GET (every payload creates
 *  disabled combinations), which works out to 5–8 Etsy round-trips plus two
 *  function cold starts — inside a 10-second platform budget. The code was
 *  lifted from a background function with a 13-minute budget; the budget
 *  didn't come with it.
 *
 *  It is now etsyPricingApplyOne-background.js (15-minute budget). But a
 *  background function returns 202 immediately with NO response body, so the
 *  browser can no longer read the outcome from the call itself. This module
 *  is the result channel: the background function writes its outcome to one
 *  Firestore doc per request, and etsyPricingApplyResult.js (a plain sync
 *  function — one Firestore read, well inside 10s) serves it to the browser,
 *  which polls until the doc appears.
 *
 *  Docs are keyed by the browser-generated request_id, so a poll can never
 *  pick up a STALE result from an earlier run against the same listing.
 *
 *  ═══ CREDENTIALS ═══════════════════════════════════════════════════════
 *
 *  Same env-based resolution etsyRateLimiter.js and _etsyApiUsage.js use:
 *  FIREBASE_SERVICE_ACCOUNT (single JSON var) or the split FIREBASE_* vars.
 *  Deliberately NOT require("./firebaseAdmin") — that file is not part of
 *  this site's bundle, and an unresolvable require breaks the whole deploy.
 *
 *  If credentials are absent getDb() returns null and callers degrade:
 *  pricing still applies to Etsy; only the result report is lost, and the
 *  browser's poll times out into "still running — check the Pricing Console".
 */

"use strict";

const RESULTS_COLL = "EtsyPricing_ApplyResults";

let _db;            // undefined = not tried yet; null = tried and unavailable
function getDb() {
  if (_db !== undefined) return _db;
  _db = null;
  try {
    let svc = null;
    if (process.env.FIREBASE_SERVICE_ACCOUNT) {
      svc = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
    } else {
      const { FIREBASE_PROJECT_ID, FIREBASE_CLIENT_EMAIL, FIREBASE_PRIVATE_KEY } = process.env;
      if (FIREBASE_PROJECT_ID && FIREBASE_CLIENT_EMAIL && FIREBASE_PRIVATE_KEY) {
        svc = {
          type: "service_account",
          project_id: FIREBASE_PROJECT_ID,
          client_email: FIREBASE_CLIENT_EMAIL,
          private_key: FIREBASE_PRIVATE_KEY.includes("\\n")
            ? FIREBASE_PRIVATE_KEY.replace(/\\n/g, "\n")
            : FIREBASE_PRIVATE_KEY,
        };
      }
    }
    if (svc) {
      const admin = require("firebase-admin");
      if (!admin.apps.length) {
        admin.initializeApp({ credential: admin.credential.cert(svc), projectId: svc.project_id });
      }
      _db = admin.firestore();
    } else {
      console.warn("[pricingResultStore] No Firebase credentials in env — pricing results cannot be reported back to the browser (pricing itself is unaffected).");
    }
  } catch (e) {
    _db = null;
    console.warn("[pricingResultStore] Firestore init failed — pricing results cannot be reported:", e && e.message);
  }
  return _db;
}

// Doc ids come from the browser (crypto.randomUUID or a timestamp fallback).
// Sanitise defensively: Firestore doc ids must be non-empty, without slashes.
function docIdFor(requestId, listingId) {
  const raw = String(requestId || listingId || "").replace(/[^A-Za-z0-9_.-]/g, "").slice(0, 120);
  return raw || null;
}

// Write the outcome. Never throws — result reporting must not be able to
// break the pricing work itself.
async function writeResult(requestId, listingId, result) {
  try {
    const db = getDb();
    const id = docIdFor(requestId, listingId);
    if (!db || !id) return false;
    await db.collection(RESULTS_COLL).doc(id).set({
      request_id: String(requestId || ""),
      listing_id: String(listingId || ""),
      done: true,
      at: Date.now(),
      result: result || {},
    });
    return true;
  } catch (e) {
    console.warn("[pricingResultStore] writeResult failed:", e && e.message);
    return false;
  }
}

async function readResult(requestId, listingId) {
  const db = getDb();
  const id = docIdFor(requestId, listingId);
  if (!db) return { unavailable: true };
  if (!id) return { pending: true };
  const snap = await db.collection(RESULTS_COLL).doc(id).get();
  if (!snap.exists) return { pending: true };
  const d = snap.data() || {};
  // request_id must match so a re-run against the same listing can never be
  // answered by the PREVIOUS run's doc.
  if (requestId && d.request_id && d.request_id !== String(requestId)) return { pending: true };
  return { done: true, at: d.at || null, result: d.result || {} };
}

module.exports = { getDb, writeResult, readResult, RESULTS_COLL };
