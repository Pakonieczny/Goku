// netlify/functions/_shared/etsyRateLimiter.js
// Distributed token-bucket limiter + resilient Etsy fetch with 429 handling.
// Uses Firestore if FIREBASE_SERVICE_ACCOUNT is provided; otherwise falls back to in-memory
// (in-memory helps single instance, Firestore makes it safe across many instances).

const fetch = require("node-fetch");

// ---- Config
const RATE_PER_SEC = 5;      // Etsy hard cap
const BURST        = 5;      // bucket capacity
const MAX_RETRIES  = 5;      // on 429 / contention
const JITTER_MS    = 50;     // random jitter to avoid thundering herd

// ---- Firestore (optional but recommended)
let useFirestore = false;
let db = null;

function buildSvcFromSplitEnv() {
  const {
    FIREBASE_PROJECT_ID,
    FIREBASE_CLIENT_EMAIL,
    FIREBASE_PRIVATE_KEY,
    FIREBASE_PRIVATE_KEY_ID,
    FIREBASE_CLIENT_ID,
    FIREBASE_AUTH_URI,
    FIREBASE_TOKEN_URI,
    FIREBASE_AUTH_PROVIDER_X509_CERT_URL,
    FIREBASE_UNIVERSE_DOMAIN,
    FIREBASE_CLIENT_X509_CERT_URL // optional, sometimes present
  } = process.env;

  // Minimum needed: project_id, client_email, private_key
  if (!FIREBASE_PROJECT_ID || !FIREBASE_CLIENT_EMAIL || !FIREBASE_PRIVATE_KEY) return null;

  // Fix escaped newlines in Netlify envs
  const pk = FIREBASE_PRIVATE_KEY.includes("\\n")
    ? FIREBASE_PRIVATE_KEY.replace(/\\n/g, "\n")
    : FIREBASE_PRIVATE_KEY;

  return {
    type: "service_account",
    project_id: FIREBASE_PROJECT_ID,
    private_key_id: FIREBASE_PRIVATE_KEY_ID,
    private_key: pk,
    client_email: FIREBASE_CLIENT_EMAIL,
    client_id: FIREBASE_CLIENT_ID,
    auth_uri: FIREBASE_AUTH_URI,
    token_uri: FIREBASE_TOKEN_URI,
    auth_provider_x509_cert_url: FIREBASE_AUTH_PROVIDER_X509_CERT_URL,
    client_x509_cert_url: FIREBASE_CLIENT_X509_CERT_URL,
    universe_domain: FIREBASE_UNIVERSE_DOMAIN
  };
}

try {
  let svc = null;

  if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    // Option A: single JSON var
    svc = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  } else {
    // Option B: split vars like in your screenshot
    svc = buildSvcFromSplitEnv();
  }

  if (svc) {
    const admin = require("firebase-admin");
    if (!admin.apps.length) {
      admin.initializeApp({ credential: admin.credential.cert(svc), projectId: svc.project_id });
    }
    db = admin.firestore();
    useFirestore = true;
  }
} catch (_) {
  useFirestore = false; // falls back to in-memory token-bucket
}

// ---- In-memory fallback (per instance)
const memState = { tokens: BURST, lastMs: Date.now() };

// Helpers
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const jitter = () => Math.floor(Math.random() * JITTER_MS);

function backoff(attempt) {
  // 200ms per token at 5 rps; exponential with cap
  const base = 200;
  const ms = Math.min(2000, base * Math.pow(2, attempt));
  return ms + jitter();
}

function parseRetryAfter(h) {
  if (!h) return null;
  const s = Number(h);
  if (Number.isFinite(s)) return Math.ceil(s * 1000);
  const when = Date.parse(h);
  return Number.isFinite(when) ? Math.max(0, when - Date.now()) : null;
}

// ---- Token bucket (Firestore-backed if available)
async function takeToken(bucket = "etsy-global") {
  const now = Date.now();

  if (!useFirestore) {
    // In-memory (per VM) — good safety net; Firestore is the robust path
    const elapsed = now - memState.lastMs;
    const refill  = (elapsed / 1000) * RATE_PER_SEC;
    memState.tokens = Math.min(BURST, memState.tokens + refill);
    memState.lastMs = now;

    if (memState.tokens >= 1) {
      memState.tokens -= 1;
      return;
    }
    const need = 1 - memState.tokens;
    const waitMs = Math.ceil((need / RATE_PER_SEC) * 1000) + jitter();
    await sleep(waitMs);
    return;
  }

  // Firestore transaction to make it safe across instances
  const ref = db.collection("rate_limits").doc(bucket);
  let attempt = 0;

  while (true) {
    attempt++;
    try {
      await db.runTransaction(async tx => {
        const snap = await tx.get(ref);
        const data = snap.exists ? snap.data() : { tokens: BURST, lastMs: now };
        const last = Number(data.lastMs || now);
        const tokensStored = Number.isFinite(data.tokens) ? data.tokens : BURST;

        const elapsed = Math.max(0, now - last);
        const refill  = (elapsed / 1000) * RATE_PER_SEC;
        let tokens    = Math.min(BURST, tokensStored + refill);

        if (tokens < 1) {
          // Not enough tokens → throw with recommended retry delay
          const need   = 1 - tokens;
          const waitMs = Math.ceil((need / RATE_PER_SEC) * 1000) + jitter();
          const err    = new Error("rate-limit-wait");
          err.waitMs   = waitMs;
          throw err;
        }

        tokens -= 1;
        tx.set(ref, { tokens, lastMs: now }, { merge: true });
      });
      return; // success
    } catch (e) {
      if (e && e.message === "rate-limit-wait") {
        await sleep(e.waitMs);
      } else {
        // Transaction contention/backoff
        if (attempt >= MAX_RETRIES) throw e;
        await sleep(backoff(attempt));
      }
    }
  }
}

// ---- Public Etsy fetch with gating + 429 retry
async function etsyFetch(url, init = {}, opts = {}) {
  const { bucket = "etsy-global", retries = MAX_RETRIES } = opts;

  let attempt = 0;
  while (true) {
    attempt++;
    await takeToken(bucket);

    const res = await fetch(url, init);

    if (res.status !== 429) return res;

    // 429 → respect Retry-After if present, else exponential backoff
    const ra = parseRetryAfter(res.headers.get("retry-after"));
    const waitMs = ra != null ? ra + jitter() : backoff(attempt);
    if (attempt >= retries) return res; // propagate the 429 payload to caller
    await sleep(waitMs);
  }
}

module.exports = { etsyFetch, takeToken };