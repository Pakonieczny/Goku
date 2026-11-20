/* netlify/functions/etsyCheckNow.js — CHECK ETSY NOW (20 Nov 2026)
 *
 * The widget's "Check Etsy" button. Etsy never tells us when its daily limit frees up: its limit is a rolling
 * 24-hour window (developers.etsy.com/documentation/essentials/rate-limits), and the only way to learn where it
 * stands is to make a call and read the answer. So this function makes EXACTLY ONE small Etsy request per press:
 *
 *   - 2xx  → Etsy's own x-remaining-today / x-limit-per-day   → "Etsy: 1,240 calls left"
 *   - 429  → Etsy's retry-after (seconds)                      → "Etsy: try again in about 3h 10m"
 *
 * Guarantees, because every call spends the shop's quota:
 *   - one fetch, no retries (it does NOT go through etsyRateLimiter's retrying etsyFetch);
 *   - at most one check per MIN_GAP_MS for the whole shop (a Firestore claim in EtsyApi_Config/checkNow), so double
 *     clicks, several tabs or several computers cannot add calls — a press inside the gap gets the last result back;
 *   - operator passcode required (X-Edit-Passcode, same EDIT_PASSCODE rule as operatorAuth.js: unset = open console).
 *
 * The call is counted and its rate-limit headers recorded through _etsyApiUsage like any other Etsy call, so the
 * widget's KEY bar shows Etsy's fresh reading afterwards. It never turns the shop's stop switch on or off: resuming
 * Etsy stays a person's press of Resume Etsy in Brites Manager → Settings.
 */
"use strict";

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");
let usage = null;
try { usage = require("./_etsyApiUsage"); } catch (_) { usage = null; }

const ETSY_URL = "https://api.etsy.com/v3/application/listings/active?limit=1";
const MIN_GAP_MS = 60 * 1000;
const COLL = "EtsyApi_Config", DOC_ID = "checkNow";
const APP_ID = process.env.ETSY_APP_ID || "listing-generator";

const HEADERS = {
  "Content-Type": "application/json",
  "Cache-Control": "no-store",
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, X-Edit-Passcode",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};
const json = (statusCode, payload) => ({ statusCode, headers: HEADERS, body: JSON.stringify(payload) });

/* Constant-time compare (same as operatorAuth.js). */
function safeEqual(a, b) {
  a = String(a || ""); b = String(b || "");
  let diff = a.length ^ b.length;
  const n = Math.max(a.length, b.length);
  for (let i = 0; i < n; i++) diff |= a.charCodeAt(i % (a.length || 1)) ^ b.charCodeAt(i % (b.length || 1));
  return diff === 0;
}

const num = (v) => { if (v == null || v === "") return null; const n = Number(v); return Number.isFinite(n) ? n : null; };

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: HEADERS, body: "" };
  if (event.httpMethod !== "POST") return json(405, { ok: false, error: "POST only" });

  const expected = process.env.EDIT_PASSCODE || "";
  if (expected) {
    const h = event.headers || {};
    if (!safeEqual(h["x-edit-passcode"] || h["X-Edit-Passcode"] || "", expected)) return json(401, { ok: false, error: "unauthorized" });
  }

  const clientId = process.env.CLIENT_ID || process.env.ETSY_CLIENT_ID || process.env.ETSY_API_KEY || process.env.API_KEY;
  const clientSecret = process.env.CLIENT_SECRET || process.env.ETSY_CLIENT_SECRET || process.env.ETSY_SHARED_SECRET;
  if (!clientId || !clientSecret) return json(500, { ok: false, error: "Missing Etsy CLIENT_ID or CLIENT_SECRET environment variable" });

  const db = admin.firestore();
  const ref = db.collection(COLL).doc(DOC_ID);
  const now = Date.now();

  // Claim the shop-wide slot. Inside the gap: no Etsy call, hand back the last result.
  let claim;
  try {
    claim = await db.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      const d = snap.exists ? snap.data() || {} : {};
      const claimedAt = num(d.claimedAt) || 0;
      if (claimedAt && now - claimedAt < MIN_GAP_MS) return { busy: true, claimedAt, last: d.last || null };
      tx.set(ref, { claimedAt: now }, { merge: true });
      return { busy: false };
    });
  } catch (e) {
    console.error("etsyCheckNow: claim failed:", e && e.message);
    return json(503, { ok: false, error: "Could not reserve the check (cloud unavailable); no Etsy call was made." });
  }
  if (claim.busy) {
    return json(200, { ok: true, cached: true, nextAllowedAt: claim.claimedAt + MIN_GAP_MS, ...(claim.last || { kind: "pending", checkedAt: claim.claimedAt }) });
  }

  // The one Etsy request. No retries.
  let last;
  let res = null;
  try {
    res = await fetch(ETSY_URL, {
      method: "GET",
      headers: { Accept: "application/json", "x-api-key": `${String(clientId).trim()}:${String(clientSecret).trim()}` },
      timeout: 10000,
    });
  } catch (e) {
    last = { kind: "error", status: null, detail: String((e && e.message) || e).slice(0, 200), checkedAt: now };
  }

  if (usage) { try { usage.recordCall(APP_ID, res); await usage.flushNow(); } catch (_) {} }

  if (res) {
    const limit = num(res.headers.get("x-limit-per-day"));
    const remaining = num(res.headers.get("x-remaining-today"));
    const retryAfterSec = num(res.headers.get("retry-after"));
    if (res.status === 429) {
      last = { kind: "limited", status: 429, retryAfterSec, retryAt: retryAfterSec != null ? now + retryAfterSec * 1000 : null, limit, remaining, checkedAt: now };
    } else if (res.ok) {
      last = { kind: "ok", status: res.status, limit, remaining, checkedAt: now };
    } else {
      last = { kind: "error", status: res.status, limit, remaining, checkedAt: now };
    }
    try { await res.text(); } catch (_) {}   // (never returned: this endpoint only reports Etsy's quota answer)
  }

  try { await ref.set({ last }, { merge: true }); } catch (e) { console.warn("etsyCheckNow: result not saved:", e && e.message); }
  console.log(`[etsyCheckNow] one Etsy call → ${last.kind} status=${last.status} remaining=${last.remaining} retryAfter=${last.retryAfterSec}`);
  return json(200, { ok: true, cached: false, nextAllowedAt: now + MIN_GAP_MS, ...last });
};
