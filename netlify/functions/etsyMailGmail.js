/*  netlify/functions/etsyMailGmail.js
 *
 *  Status + manual-trigger endpoint for the Gmail watcher. Mirrors the
 *  shape of etsyMailSync.js exactly so any operator UI that knows how
 *  to display Etsy sync status can display Gmail watcher status with
 *  the same patterns.
 *
 *  ═══ ACTIONS ═══════════════════════════════════════════════════════════
 *
 *  GET /.netlify/functions/etsyMailGmail?action=status
 *      → No auth. Returns current watcher state. Use to poll progress
 *        from the inbox UI or a dashboard.
 *
 *  POST /.netlify/functions/etsyMailGmail?action=trigger
 *      body: { mode?: "incremental" | "full", query?: "...", windowDays?: 7 }
 *      → Auth via X-EtsyMail-Secret. Invokes etsyMailGmail-background
 *        and returns immediately with { invoked: true }. Poll status
 *        endpoint to see progress.
 *
 *  Scheduled invocations bypass this endpoint entirely — etsyMailGmailCron
 *  POSTs directly to etsyMailGmail-background.
 *
 *  ═══ STATUS RESPONSE ═══════════════════════════════════════════════════
 *
 *  {
 *    lastSyncInProgress              : bool,
 *    lastSyncCompletedAt             : iso-string | null,
 *    lastSyncStartedAt               : iso-string | null,
 *    lastSyncMode                    : "incremental" | "full" | null,
 *    lastSyncMessagesScanned         : number,
 *    lastSyncJobsEnqueued            : number,
 *    lastSyncThreadsCreated          : number,
 *    lastSyncThreadsLinked           : number,
 *    lastSyncSkippedNoLink           : number,
 *    lastSyncSkippedAlreadyProcessed : number,
 *    lastSyncErrors                  : number,
 *    lastSyncPagesFetched            : number,
 *    lastSyncQuery                   : string | null,
 *    lastSyncDurationMs              : number | null,
 *    lastSyncError                   : string | null,
 *    lastSyncErrorAt                 : iso-string | null,
 *    lastInternalDateMs              : number,
 *    lastInternalDateIso             : iso-string | null,
 *    oauthSeeded                     : bool,
 *    oauthEmailAddress               : string | null,
 *    oauthExpiresAt                  : iso-string | null
 *  }
 */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");
const { requireExtensionAuth, CORS } = require("./_etsyMailAuth");

const db = admin.firestore();

const SYNC_STATE_DOC = "EtsyMail_Config/gmailSyncState";
const OAUTH_DOC_PATH = "config/gmailOauth";

function json(statusCode, body) {
  return {
    statusCode,
    headers: { ...CORS, "Content-Type": "application/json" },
    body: JSON.stringify(body)
  };
}

function tsToIso(v) {
  if (!v) return null;
  if (v.toMillis) return new Date(v.toMillis()).toISOString();
  if (v instanceof Date) return v.toISOString();
  return null;
}

async function runStatus() {
  const [stateSnap, oauthSnap] = await Promise.all([
    db.doc(SYNC_STATE_DOC).get(),
    db.doc(OAUTH_DOC_PATH).get()
  ]);

  const state = stateSnap.exists ? stateSnap.data() : null;
  const oauth = oauthSnap.exists ? oauthSnap.data() : null;

  return {
    // ── Sync state ──
    lastSyncInProgress             : state ? !!state.lastSyncInProgress : false,
    lastSyncStartedAt              : state ? tsToIso(state.lastSyncStartedAt)   : null,
    lastSyncCompletedAt            : state ? tsToIso(state.lastSyncCompletedAt) : null,
    lastSyncMode                   : state ? (state.lastSyncMode || null)        : null,
    lastSyncMessagesScanned        : state ? (state.lastSyncMessagesScanned || 0) : 0,
    lastSyncJobsEnqueued           : state ? (state.lastSyncJobsEnqueued || 0) : 0,
    lastSyncThreadsCreated         : state ? (state.lastSyncThreadsCreated || 0) : 0,
    lastSyncThreadsLinked          : state ? (state.lastSyncThreadsLinked || 0) : 0,
    lastSyncSkippedNoLink          : state ? (state.lastSyncSkippedNoLink || 0) : 0,
    lastSyncSkippedAlreadyProcessed: state ? (state.lastSyncSkippedAlreadyProcessed || 0) : 0,
    lastSyncErrors                 : state ? (state.lastSyncErrors || 0) : 0,
    lastSyncPagesFetched           : state ? (state.lastSyncPagesFetched || 0) : 0,
    lastSyncQuery                  : state ? (state.lastSyncQuery || null) : null,
    lastSyncDurationMs             : state ? (state.lastSyncDurationMs || null) : null,
    lastSyncError                  : state ? (state.lastSyncError || null) : null,
    lastSyncErrorAt                : state ? tsToIso(state.lastSyncErrorAt) : null,
    lastInternalDateMs             : state ? (state.lastInternalDateMs || 0) : 0,
    lastInternalDateIso            : state && state.lastInternalDateMs
      ? new Date(state.lastInternalDateMs).toISOString()
      : null,

    // ── OAuth state ──
    oauthSeeded         : !!oauth,
    oauthEmailAddress   : oauth ? (oauth.emailAddress || null) : null,
    oauthExpiresAt      : oauth && oauth.expires_at
      ? new Date(oauth.expires_at).toISOString()
      : null,
    oauthExpired        : oauth && oauth.expires_at
      ? oauth.expires_at < Date.now()
      : null
  };
}

// Trigger the background function. Same pattern as etsyMailSync.js — we
// derive the site origin from the incoming request (so it works in both
// production and deploy previews) and POST. Netlify returns 202 and the
// background function runs detached for up to 15 min.
async function triggerBackgroundSync(event, body) {
  const host = event.headers["x-forwarded-host"] || event.headers.host;
  const proto = event.headers["x-forwarded-proto"] || "https";
  const siteOrigin = `${proto}://${host}`;
  const url = `${siteOrigin}/.netlify/functions/etsyMailGmail-background`;

  const res = await fetch(url, {
    method : "POST",
    headers: { "Content-Type": "application/json" },
    body   : JSON.stringify(body || {})
  });

  return { invoked: res.status >= 200 && res.status < 300, invocationStatus: res.status };
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: CORS, body: "" };
  }

  const qs = event.queryStringParameters || {};
  const action = qs.action || "status";

  try {
    if (action === "status") {
      const body = await runStatus();
      return json(200, body);
    }

    if (action === "trigger") {
      const auth = requireExtensionAuth(event);
      if (!auth.ok) return auth.response;

      // Parse trigger params from body + query string. Same shape as
      // etsyMailSync.js — body wins over query string when both supplied.
      let mode = "incremental";
      let query = null;
      let windowDays = null;
      if (event.httpMethod === "POST" && event.body) {
        try {
          const b = JSON.parse(event.body);
          if (b.mode === "full") mode = "full";
          if (typeof b.query === "string") query = b.query;
          if (typeof b.windowDays === "number") windowDays = b.windowDays;
        } catch {}
      }
      if (qs.mode === "full") mode = "full";
      if (qs.query) query = qs.query;
      if (qs.windowDays) windowDays = parseInt(qs.windowDays, 10);

      const result = await triggerBackgroundSync(event, { mode, query, windowDays });
      return json(202, {
        ok: true,
        mode,
        query     : query || null,
        windowDays: windowDays || null,
        ...result,
        nextStep: "Poll /.netlify/functions/etsyMailGmail?action=status to see progress"
      });
    }

    return json(400, { error: `Unknown action: ${action}. Use action=status or action=trigger.` });
  } catch (err) {
    console.error("etsyMailGmail error:", err);
    return json(500, { error: err.message || "Unknown error" });
  }
};
