/*  netlify/functions/_etsyMailGmail.js  (v1.4)
 *
 *  Shared Gmail API helpers for the EtsyMail system.
 *
 *  ═══ v1.4 CHANGE LOG ═══════════════════════════════════════════════════
 *
 *  URL FORMAT FIX: switched from constructing `/your/conversations/<id>`
 *  to `/messages/<id>`. Etsy's current canonical conversation URL format
 *  is `https://www.etsy.com/messages/<id>` — the older `/your/conversations/`
 *  path still works via redirect but produces an extra hop the scraper
 *  has to follow. Using the canonical format avoids that hop.
 *
 *  Also added /messages/<id> to the regex pattern set so we recognize
 *  the canonical form in tracker chains too.
 *
 *  ═══ v1.3 CHANGE LOG (retained) ════════════════════════════════════════
 *
 *  CRITICAL FIX: scan every redirect-chain Location header (not just the
 *  final URL) for a conversation id. Etsy's tracker chain looks like:
 *
 *      ablink.account.etsy.com  (SendGrid)
 *        → 302 → etsy.app.link/3p?$original_url=...etsy.com/conversations/<id>...  ← id here
 *        → 307 → etsy.com/?utm_content=...   (Branch.io desktop fallback STRIPS the original URL)
 *        → 200 → final = homepage
 *
 *  Branch.io's deep-link service (etsy.app.link) inspects User-Agent. On
 *  mobile it forwards the $original_url to the app; on desktop/server it
 *  sends users to the homepage with marketing UTM params, dropping the
 *  conversation URL entirely. So by the time we see the terminal 200,
 *  the conversation id is GONE from `current` — but it was sitting in
 *  the very first 302's Location header all along (URL-encoded as
 *  `$original_url=https%3A%2F%2Fwww%2Eetsy%2Ecom%2Fconversations%2F<id>`).
 *
 *  Fix: at every redirect hop, run findConversationIdInString on the
 *  Location value. If a conversation id is found, short-circuit and
 *  return — no point following the rest of the chain. This also makes
 *  the extractor faster since most emails resolve at hop=0 of cand=1
 *  (the "View message" tracker).
 *
 *  Per-tracker diagnostic logging from v1.2 retained — once we confirm
 *  this fix works in production for a few days, v1.4 will quiet the
 *  logs back down.
 *
 *  ═══ v1.2 CHANGE LOG (retained) ════════════════════════════════════════
 *
 *  DIAGNOSTIC: per-tracker logging added to extractEtsyConversationLink
 *  and followToFinalUrl. Previously the extractor silently returned null
 *  on any failure path, making it impossible to tell whether SendGrid
 *  was 403'ing, redirects were dead-ending at non-conversation URLs,
 *  or no candidates were found in the body. v1.2 logs:
 *    [gmail-extract msgId=…] candidates: <n> uni=<n> ss=<n>
 *    [gmail-extract msgId=…] try <i>/<n>: <truncated url>
 *    [gmail-tracker hop=<n>] status=<code> location=<truncated>
 *    [gmail-extract msgId=…] resolved → conv=<id>     (success)
 *    [gmail-extract msgId=…] no candidate resolved    (failure)
 *
 *  Once the live failure mode is identified from logs, v1.3 will switch
 *  back to silent operation (these logs are noisy at scale).
 *
 *  ═══ v1.1 CHANGE LOG (retained) ════════════════════════════════════════
 *
 *  Added redirect-following for Etsy's SendGrid click-tracking URLs.
 *  Etsy notification emails (from no-reply@account.etsy.com) wrap the
 *  "View message" link in `https://ablink.account.etsy.com/uni/ss/c/...`
 *  trackers — the conversation URL is NOT in the email body anywhere.
 *  We have to fetch the tracker URL with redirect:manual, read the
 *  Location header, and extract the conversation id from there.
 *
 *  extractEtsyConversationLink() became async because of this.
 *
 *  ═══ EXPORTS ═══════════════════════════════════════════════════════════
 *
 *    getValidGmailAccessToken()                  → Bearer access token
 *    gmailFetch(path, opts)                      → authenticated fetch
 *    listMessages({ q, pageToken })              → users.messages.list
 *    getMessage(id, { format })                  → users.messages.get
 *    extractEmailBodyText(message)               → plain+html text blob
 *    extractEtsyConversationLink(message) (async) → → { id, url } | null
 *    extractHeaderValue(headers, name)
 *    summarizeMessage(message)
 *
 *  ═══ ENV VARS ══════════════════════════════════════════════════════════
 *
 *    GMAIL_CLIENT_ID
 *    GMAIL_CLIENT_SECRET
 *
 *  Tokens at config/gmailOauth (Firestore).
 *  Required scope: https://mail.google.com/  (or .readonly minimum)
 */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const GMAIL_CLIENT_ID     = process.env.GMAIL_CLIENT_ID;
const GMAIL_CLIENT_SECRET = process.env.GMAIL_CLIENT_SECRET;

const OAUTH_DOC_PATH         = "config/gmailOauth";
const TOKEN_REFRESH_BUFFER_MS = 2 * 60 * 1000;
const GMAIL_API_BASE          = "https://gmail.googleapis.com/gmail/v1/users/me";
const GOOGLE_TOKEN_ENDPOINT   = "https://oauth2.googleapis.com/token";

// ─── OAuth ─────────────────────────────────────────────────────────────────

async function refreshGmailToken(oldRefreshToken) {
  if (!GMAIL_CLIENT_ID)     throw new Error("GMAIL_CLIENT_ID env var missing");
  if (!GMAIL_CLIENT_SECRET) throw new Error("GMAIL_CLIENT_SECRET env var missing");

  const res = await fetch(GOOGLE_TOKEN_ENDPOINT, {
    method : "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body   : new URLSearchParams({
      grant_type   : "refresh_token",
      client_id    : GMAIL_CLIENT_ID,
      client_secret: GMAIL_CLIENT_SECRET,
      refresh_token: oldRefreshToken
    })
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Gmail token refresh failed: ${res.status} ${body}`);
  }

  const data = await res.json();
  const expires_at = Date.now() + Math.max(0, (data.expires_in - 120)) * 1000;

  await db.doc(OAUTH_DOC_PATH).set({
    access_token : data.access_token,
    refresh_token: data.refresh_token || oldRefreshToken,
    expires_at,
    scope        : data.scope || null,
    token_type   : data.token_type || "Bearer",
    updatedAt    : FV.serverTimestamp()
  }, { merge: true });

  return data.access_token;
}

async function getValidGmailAccessToken() {
  const snap = await db.doc(OAUTH_DOC_PATH).get();
  if (!snap.exists) throw new Error(
    `Gmail OAuth not seeded at ${OAUTH_DOC_PATH}. Run etsyMailGmailSeedTokens first.`
  );
  const tok = snap.data();
  if (!tok.refresh_token) throw new Error(`No refresh_token in ${OAUTH_DOC_PATH}.`);

  const expiresAt = typeof tok.expires_at === "number" ? tok.expires_at : 0;
  if (!tok.access_token || expiresAt - Date.now() < TOKEN_REFRESH_BUFFER_MS) {
    return await refreshGmailToken(tok.refresh_token);
  }
  return tok.access_token;
}

// ─── Generic Gmail fetch ───────────────────────────────────────────────────

async function gmailFetch(path, opts = {}) {
  const token = await getValidGmailAccessToken();
  const url = path.startsWith("http") ? path : `${GMAIL_API_BASE}${path}`;

  const headers = {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
    ...(opts.headers || {})
  };

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 30000);

  let res;
  try {
    res = await fetch(url, { ...opts, headers, signal: controller.signal });
  } catch (err) {
    clearTimeout(timeoutId);
    if (err.name === "AbortError") throw new Error(`Gmail API timeout: ${url}`);
    throw err;
  }
  clearTimeout(timeoutId);

  if (res.status === 401 && !opts._retried) {
    const stale = await db.doc(OAUTH_DOC_PATH).get();
    if (stale.exists && stale.data().refresh_token) {
      await refreshGmailToken(stale.data().refresh_token);
      return gmailFetch(path, { ...opts, _retried: true });
    }
  }

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Gmail API ${res.status} on ${path}: ${text.slice(0, 300)}`);
  }
  return await res.json();
}

// ─── Gmail message ops ─────────────────────────────────────────────────────

async function listMessages({ q = "", pageToken = null, maxResults = 100 } = {}) {
  const params = new URLSearchParams();
  if (q) params.set("q", q);
  if (pageToken) params.set("pageToken", pageToken);
  if (maxResults) params.set("maxResults", String(maxResults));
  return await gmailFetch(`/messages?${params.toString()}`);
}

async function getMessage(id, { format = "full" } = {}) {
  return await gmailFetch(`/messages/${encodeURIComponent(id)}?format=${format}`);
}

function extractHeaderValue(headers = [], name = "") {
  if (!Array.isArray(headers)) return null;
  const lower = name.toLowerCase();
  for (const h of headers) {
    if (h && h.name && h.name.toLowerCase() === lower) return h.value;
  }
  return null;
}

function decodeBase64Url(s) {
  if (!s) return "";
  const b64 = String(s).replace(/-/g, "+").replace(/_/g, "/");
  const padded = b64 + "=".repeat((4 - (b64.length % 4)) % 4);
  try {
    return Buffer.from(padded, "base64").toString("utf-8");
  } catch {
    return "";
  }
}

function extractEmailBodyText(message) {
  const out = [];
  function walk(part) {
    if (!part) return;
    const mime = (part.mimeType || "").toLowerCase();
    if ((mime === "text/plain" || mime === "text/html") && part.body && part.body.data) {
      out.push(decodeBase64Url(part.body.data));
    }
    if (Array.isArray(part.parts)) {
      for (const sub of part.parts) walk(sub);
    }
  }
  walk(message && message.payload);
  return out.join("\n\n--BOUNDARY--\n\n");
}

// ─── Etsy conversation link extraction ─────────────────────────────────────
//
// Etsy emails come in two forms in the wild:
//
//   FORM A (legacy): the conversation URL appears directly in the email
//     body as `etsy.com/your/conversations/<id>` (or URL-encoded). Easy.
//
//   FORM B (current, observed Apr 2026): the body contains ONLY SendGrid
//     click-tracking URLs of shape `https://ablink.account.etsy.com/...`.
//     Each redirects (302) to the real destination only when followed.
//     The conversation URL is NOT in the body — we have to follow at
//     least one tracker to find it.
//
// Strategy:
//   1. Fast path: scan the body for direct conversation URLs. If found
//      (FORM A), return immediately — no network needed.
//   2. Tracker path (FORM B): collect every distinct ablink URL in the
//      body. Filter to ones that look like message-link candidates
//      (the `/uni/ss/c/` flavor, which Etsy uses for in-app deep links;
//      "/ss/c/" without "/uni/" is reserved for marketing footer/nav
//      links — they don't redirect to conversations). Follow each one
//      with redirect:manual until the Location header reveals a
//      conversation URL.
//   3. Cap follows at MAX_TRACKER_FOLLOWS so a malformed email can't
//      burn the function budget.
//
// Returns: { conversationId, conversationUrl } or null

const CONV_ID_PATTERNS = [
  /\/(?:your\/)?conversations\/(\d+)/,
  /\/your\/messages\/(?:buyer|thread)\/(\d+)/,
  /\/messages\/(\d+)/
];

const ETSY_TRACKER_HOST = "ablink.account.etsy.com";
const MAX_TRACKER_FOLLOWS  = 4;     // most "View message" emails have 1-3 candidates
const MAX_REDIRECT_HOPS    = 5;     // tracker → tracker → … → etsy.com
const TRACKER_FETCH_TIMEOUT_MS = 8000;

function decodePercentSafe(s) {
  // Decode %XX escapes only (not full URI). Leave invalid escapes alone.
  return s.replace(/%[0-9A-Fa-f]{2}/g, (m) => {
    try { return decodeURIComponent(m); } catch { return m; }
  });
}

function findConversationIdInString(s) {
  if (!s) return null;
  const decoded = decodePercentSafe(s);
  for (const re of CONV_ID_PATTERNS) {
    const m = decoded.match(re);
    if (m && m[1]) {
      const id = m[1];
      if (id.length >= 5 && id.length <= 15) return id;
    }
  }
  return null;
}

/**
 * Fetch a single URL with manual redirect handling; return the final
 * resolved URL (after following all 3xx hops up to MAX_REDIRECT_HOPS),
 * or null on failure / hop limit.
 *
 * IMPLEMENTATION NOTE: We use GET (not HEAD) with a real-looking
 * User-Agent. SendGrid's click trackers (ablink.*) return 403 on HEAD
 * requests — they only honor GET with a browser-like UA. We use
 * redirect:"manual" so we read the Location header without following
 * the body, keeping each hop cheap (response body never read or buffered
 * past the headers).
 */
/**
 * Fetch a single URL with manual redirect handling.
 *
 * v1.3 behavior change: at every redirect hop, the Location header is
 * scanned for a conversation id (including any URL-encoded $original_url
 * parameter). If a conversation id is found, this function short-circuits
 * and returns the constructed conversation URL immediately — it does not
 * continue following the chain.
 *
 * This is necessary because Etsy's redirect chain goes through Branch.io
 * (etsy.app.link), which strips the original URL on desktop user-agents
 * and sends the request to the homepage instead. By the time we'd reach
 * the terminal 200, the conversation id would be gone.
 *
 * Returns:
 *   { conversationId, conversationUrl, foundAtHop }  on success
 *   { finalUrl }                                     on terminal 2xx without conv id
 *   null                                             on failure / timeout / 4xx-5xx
 */
async function followToFinalUrl(startUrl, hopBudget = MAX_REDIRECT_HOPS, logTag = "") {
  let current = startUrl;
  for (let hop = 0; hop < hopBudget; hop++) {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), TRACKER_FETCH_TIMEOUT_MS);
    let res;
    try {
      res = await fetch(current, {
        method   : "GET",
        redirect : "manual",
        signal   : controller.signal,
        headers  : {
          "User-Agent"     : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
          "Accept"         : "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "en-US,en;q=0.9"
        }
      });
    } catch (e) {
      clearTimeout(t);
      console.log(`[gmail-tracker ${logTag} hop=${hop}] fetch error: ${e.message}`);
      return null;
    }
    clearTimeout(t);

    if (res.status >= 300 && res.status < 400) {
      const loc = res.headers.get("location");
      console.log(`[gmail-tracker ${logTag} hop=${hop}] status=${res.status} → ${loc ? loc.slice(0, 200) : "(no Location)"}`);
      if (!loc) return null;

      // ━━━ v1.3 FIX ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
      // Branch.io (etsy.app.link) is the second hop in Etsy's tracker
      // chain. It encodes the actual destination as a percent-encoded
      // URL in the `$original_url` query parameter. On desktop UAs it
      // then redirects to homepage instead, dropping that parameter.
      //
      // So we MUST extract the conversation id from the Location value
      // itself — at every hop — rather than waiting for the chain's
      // terminal URL. findConversationIdInString already calls
      // decodePercentSafe internally, so percent-encoded paths
      // (%2Fconversations%2F<id>) match correctly.
      //
      // Also scan the absolute URL we'd next request (relative redirects
      // resolved against current) for completeness.
      let resolvedNext;
      try {
        resolvedNext = new URL(loc, current).toString();
      } catch (urlErr) {
        console.log(`[gmail-tracker ${logTag} hop=${hop}] bad redirect URL: ${urlErr.message}`);
        return null;
      }

      const idAtHop = findConversationIdInString(loc) || findConversationIdInString(resolvedNext);
      if (idAtHop) {
        console.log(`[gmail-tracker ${logTag} hop=${hop}] short-circuit: conv id ${idAtHop} found in Location, no further follows`);
        return {
          conversationId : idAtHop,
          conversationUrl: `https://www.etsy.com/messages/${idAtHop}`,
          foundAtHop     : hop
        };
      }
      // ━━━ end v1.3 fix ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

      current = resolvedNext;
      continue;
    }

    if (res.status >= 200 && res.status < 300) {
      console.log(`[gmail-tracker ${logTag} hop=${hop}] terminal status=${res.status}, final=${current.slice(0, 200)}`);
      try { res.body && res.body.resume && res.body.resume(); } catch {}
      // Last-chance scan of the terminal URL itself (covers the legacy
      // form-A case where redirects deliver us straight to the
      // /your/conversations/<id> page).
      const idAtFinal = findConversationIdInString(current);
      if (idAtFinal) {
        return {
          conversationId : idAtFinal,
          conversationUrl: `https://www.etsy.com/messages/${idAtFinal}`,
          foundAtHop     : hop
        };
      }
      return { finalUrl: current };
    }

    let bodyPeek = "";
    try { bodyPeek = (await res.text()).slice(0, 200); } catch {}
    console.log(`[gmail-tracker ${logTag} hop=${hop}] terminal status=${res.status} body="${bodyPeek.replace(/\s+/g, " ")}"`);
    return null;
  }
  console.log(`[gmail-tracker ${logTag}] hop budget exhausted`);
  return null;
}

async function extractEtsyConversationLink(message) {
  const msgId = message && message.id ? String(message.id) : "?";
  const logTag = `msgId=${msgId}`;

  const body = extractEmailBodyText(message);
  if (!body) {
    console.log(`[gmail-extract ${logTag}] empty body`);
    return null;
  }

  // ── Fast path: try to find a direct conversation URL in the body ──
  const directId = findConversationIdInString(body);
  if (directId) {
    console.log(`[gmail-extract ${logTag}] direct match → conv=${directId}`);
    return {
      conversationId : directId,
      conversationUrl: `https://www.etsy.com/messages/${directId}`
    };
  }

  // ── Tracker path: collect ablink URLs and follow them ──
  const flat = body.replace(/=\r?\n/g, "");
  const trackerRegex = /https?:\/\/ablink\.account\.etsy\.com\/[^\s)<"']+/gi;

  const all = flat.match(trackerRegex) || [];

  const seen = new Set();
  const candidates = [];
  for (const u of all) {
    const cleaned = u.replace(/[).,!?;:'"]+$/, "");
    if (seen.has(cleaned)) continue;
    seen.add(cleaned);
    candidates.push(cleaned);
  }

  if (!candidates.length) {
    console.log(`[gmail-extract ${logTag}] no tracker URLs in body, body length=${body.length}`);
    return null;
  }

  const uniLinks  = candidates.filter(u => u.includes("/uni/ss/c/"));
  const ssLinks   = candidates.filter(u => !u.includes("/uni/ss/c/"));
  const ordered   = [...uniLinks, ...ssLinks].slice(0, MAX_TRACKER_FOLLOWS);

  console.log(`[gmail-extract ${logTag}] candidates: ${candidates.length} (uni=${uniLinks.length} ss=${ssLinks.length}), trying ${ordered.length}`);

  for (let i = 0; i < ordered.length; i++) {
    const trackerUrl = ordered[i];
    console.log(`[gmail-extract ${logTag}] try ${i + 1}/${ordered.length}: ${trackerUrl.slice(0, 100)}...`);
    const result = await followToFinalUrl(trackerUrl, MAX_REDIRECT_HOPS, `${logTag} cand=${i + 1}`);

    if (!result) {
      console.log(`[gmail-extract ${logTag}] try ${i + 1} → no resolution`);
      continue;
    }

    // v1.3 followToFinalUrl returns either:
    //   { conversationId, conversationUrl, foundAtHop }  ← short-circuited on conv id
    //   { finalUrl }                                     ← terminal 2xx, no conv id
    if (result.conversationId) {
      console.log(`[gmail-extract ${logTag}] resolved → conv=${result.conversationId} (atHop=${result.foundAtHop})`);
      return {
        conversationId : result.conversationId,
        conversationUrl: result.conversationUrl
      };
    }

    console.log(`[gmail-extract ${logTag}] try ${i + 1} → final=${(result.finalUrl || "").slice(0, 120)}`);
    console.log(`[gmail-extract ${logTag}] try ${i + 1} → final URL had no conversation id`);
  }

  console.log(`[gmail-extract ${logTag}] no candidate resolved to a conversation`);
  return null;
}

function summarizeMessage(message) {
  const headers = (message && message.payload && message.payload.headers) || [];
  const internalDateMs = message && message.internalDate
    ? parseInt(message.internalDate, 10)
    : null;
  return {
    gmailMessageId  : message && message.id ? String(message.id) : null,
    gmailThreadId   : message && message.threadId ? String(message.threadId) : null,
    snippet         : message && message.snippet ? String(message.snippet) : "",
    internalDateMs,
    from            : extractHeaderValue(headers, "From"),
    to              : extractHeaderValue(headers, "To"),
    subject         : extractHeaderValue(headers, "Subject"),
    messageIdHeader : extractHeaderValue(headers, "Message-ID") || extractHeaderValue(headers, "Message-Id"),
    dateHeader      : extractHeaderValue(headers, "Date")
  };
}

module.exports = {
  // Token management
  getValidGmailAccessToken,
  refreshGmailToken,
  // HTTP wrapper
  gmailFetch,
  // High-level Gmail ops
  listMessages,
  getMessage,
  // Parsing helpers
  extractHeaderValue,
  extractEmailBodyText,
  extractEtsyConversationLink,   // now async
  decodeBase64Url,
  summarizeMessage,
  // Tracker resolution (exposed for tests)
  followToFinalUrl,
  findConversationIdInString,
  // Constants
  OAUTH_DOC_PATH,
  GMAIL_API_BASE
};
