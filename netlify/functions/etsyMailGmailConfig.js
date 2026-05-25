/*  netlify/functions/etsyMailGmailConfig.js
 *
 *  Owner-gated endpoint for the inbox UI's Gmail Watcher panel. Three ops:
 *
 *    GET  ?op=get
 *      → Returns { enabled, updatedAt, updatedBy, pullFromMs, pullFromIso,
 *                  syncState }
 *        Callable with the X-EtsyMail-Secret only — no role check on read.
 *
 *    POST { op:"set", enabled: bool, actor: "<operator name>" }
 *      → Owner-only. Writes EtsyMail_Config/gmailWatcher.enabled and
 *        appends an audit row. The cron picks up the new flag value within
 *        ≤1 minute (its next tick).
 *
 *      Side effect — "pull-from" watermark seed:
 *        When this op flips enabled from false → true AND
 *        gmailWatcher.pullFromMs is set, we ALSO write that value into
 *        EtsyMail_Config/gmailSyncState.lastInternalDateMs and then clear
 *        pullFromMs from gmailWatcher (one-shot consumption). The next
 *        cron tick's buildQuery() in etsyMailGmail-background.js then
 *        builds `after:<seconds>` from that watermark — so re-enabling the
 *        watcher pulls only emails newer than the operator-chosen instant
 *        instead of every email since the last shutdown.
 *
 *        Rationale: when an operator manually disables the watcher for
 *        days then re-enables it, the existing watermark would replay all
 *        emails that arrived during the off period — usually unwanted.
 *        The pull-from picker lets the operator pick a precise restart
 *        cursor (often "now" or "an hour ago") instead.
 *
 *    POST { op:"setPullFrom", pullFromMs: number|null, actor: "<name>" }
 *      → Owner-only. Stores (or clears) the pull-from cursor in
 *        EtsyMail_Config/gmailWatcher.pullFromMs without touching the
 *        enabled flag. Used by the settings modal so the operator can
 *        configure the cursor BEFORE clicking the topbar toggle.
 *        Validation:
 *          - pullFromMs must be a finite number ≤ Date.now() (no future)
 *          - pullFromMs === null clears any prior value
 *
 *  Why a dedicated endpoint instead of just using firestoreProxy:
 *    - Centralizes the role check (firestoreProxy enforces it too, but
 *      this gives the UI a clean single-purpose call site).
 *    - Returns a combined view of the toggle state AND the watcher's
 *      sync state in one round trip — the inbox panel needs both.
 *    - Audit row uses a watcher-specific eventType so operators can
 *      filter the audit log for "who toggled the watcher when."
 *    - Seeding the watermark on enable is a cross-doc transition
 *      (gmailWatcher → gmailSyncState) that needs to happen alongside
 *      the toggle audit row; doing it here keeps the contract atomic.
 *
 *  Auth model is identical to other operator-config endpoints:
 *    - X-EtsyMail-Secret required for all ops
 *    - Set / setPullFrom additionally require actor in
 *      EtsyMail_Operators with role="owner". Operators with
 *      role="operator" can READ the panel but the toggle button and
 *      pull-from picker are disabled for them.
 */

"use strict";

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const { requireOwner, logUnauthorized } = require("./_etsyMailRoles");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const WATCHER_CFG_DOC  = "EtsyMail_Config/gmailWatcher";
const SYNC_STATE_DOC   = "EtsyMail_Config/gmailSyncState";
const OAUTH_DOC_PATH   = "config/gmailOauth";
const AUDIT_COLL       = "EtsyMail_Audit";

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

async function readWatcherConfig() {
  const snap = await db.doc(WATCHER_CFG_DOC).get();
  if (!snap.exists) {
    return {
      enabled    : false,
      updatedAt  : null,
      updatedBy  : null,
      pullFromMs : null,
      pullFromIso: null
    };
  }
  const d = snap.data();
  // pullFromMs is the operator-configured "start the next sync from this
  // instant" cursor. It is one-shot — consumed and cleared the next time
  // the watcher transitions off→on (see op:"set" below). null means "no
  // override; use the existing watermark or the default initial window".
  const pullFromMs = (typeof d.pullFromMs === "number" && d.pullFromMs > 0)
    ? d.pullFromMs : null;
  return {
    enabled    : !!d.enabled,
    updatedAt  : tsToIso(d.updatedAt),
    updatedBy  : d.updatedBy || null,
    pullFromMs,
    pullFromIso: pullFromMs ? new Date(pullFromMs).toISOString() : null
  };
}

async function readSyncSnapshot() {
  const [stateSnap, oauthSnap] = await Promise.all([
    db.doc(SYNC_STATE_DOC).get(),
    db.doc(OAUTH_DOC_PATH).get()
  ]);
  const state = stateSnap.exists ? stateSnap.data() : null;
  const oauth = oauthSnap.exists ? oauthSnap.data() : null;
  return {
    oauthSeeded         : !!oauth,
    oauthEmailAddress   : oauth ? (oauth.emailAddress || null) : null,
    lastSyncCompletedAt : state ? tsToIso(state.lastSyncCompletedAt) : null,
    lastSyncInProgress  : state ? !!state.lastSyncInProgress : false,
    lastSyncMode        : state ? (state.lastSyncMode || null) : null,
    lastSyncMessagesScanned: state ? (state.lastSyncMessagesScanned || 0) : 0,
    lastSyncJobsEnqueued: state ? (state.lastSyncJobsEnqueued || 0) : 0,
    lastSyncErrors      : state ? (state.lastSyncErrors || 0) : 0,
    lastSyncError       : state ? (state.lastSyncError || null) : null
  };
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };

  // X-EtsyMail-Secret on every call (read AND write).
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  const qs = event.queryStringParameters || {};
  const op = (qs.op || "").toLowerCase() ||
             (event.httpMethod === "POST" ? safeBodyOp(event) : null) ||
             "get";

  try {
    if (op === "get") {
      const [cfg, sync] = await Promise.all([readWatcherConfig(), readSyncSnapshot()]);
      return json(200, { ok: true, ...cfg, syncState: sync });
    }

    if (op === "set") {
      if (event.httpMethod !== "POST") return json(405, { error: "POST required for op=set" });

      let body = {};
      try { body = JSON.parse(event.body || "{}"); }
      catch { return json(400, { error: "Invalid JSON body" }); }

      const { enabled, actor } = body;
      if (typeof enabled !== "boolean") {
        return json(400, { error: "Field 'enabled' must be a boolean" });
      }
      if (!actor || typeof actor !== "string") {
        return json(400, { error: "Field 'actor' (operator name) is required" });
      }

      // Owner-only — toggling the watcher affects every operator.
      const owner = await requireOwner(actor);
      if (!owner.ok) {
        await logUnauthorized({
          actor,
          eventType: "gmail_watcher_toggle_unauthorized",
          payload  : { attemptedEnabled: enabled, reason: owner.reason }
        });
        return json(403, { error: "Owner role required", reason: owner.reason });
      }

      // Read prior state up-front so we can detect the off→on edge and
      // pick up any operator-configured pull-from cursor.
      const priorCfgSnap = await db.doc(WATCHER_CFG_DOC).get();
      const priorCfg     = priorCfgSnap.exists ? priorCfgSnap.data() : {};
      const wasEnabled   = !!priorCfg.enabled;
      const isOffToOn    = enabled && !wasEnabled;
      const priorPullFromMs = (typeof priorCfg.pullFromMs === "number" && priorCfg.pullFromMs > 0)
        ? priorCfg.pullFromMs : null;

      // Persist the flag. On the off→on transition, also clear pullFromMs
      // (it's one-shot — see header for rationale). When transitioning
      // on→off or no transition, leave pullFromMs alone so the operator
      // can continue editing it via op=setPullFrom while disabled.
      const cfgPatch = {
        enabled,
        updatedAt: FV.serverTimestamp(),
        updatedBy: actor
      };
      if (isOffToOn && priorPullFromMs) {
        // Use admin.firestore.FieldValue.delete() to remove the field
        // rather than setting it to null — keeps the doc shape clean for
        // future readers and for any rules / queries that filter on
        // field presence.
        cfgPatch.pullFromMs = FV.delete();
      }
      await db.doc(WATCHER_CFG_DOC).set(cfgPatch, { merge: true });

      // Watermark seed on off→on transition. We write to gmailSyncState
      // so the next cron tick's buildQuery() picks it up via the
      // lastInternalDateMs path. No change required in the background
      // function — it already treats lastInternalDateMs as the cursor.
      let seededWatermarkMs = null;
      if (isOffToOn && priorPullFromMs) {
        seededWatermarkMs = priorPullFromMs;
        await db.doc(SYNC_STATE_DOC).set({
          lastInternalDateMs: priorPullFromMs,
          updatedAt         : FV.serverTimestamp()
        }, { merge: true });
      }

      // Audit. The seeded watermark is included in the payload so an
      // operator scanning the audit log can see why the next sync ran
      // from a particular instant.
      await db.collection(AUDIT_COLL).add({
        threadId : null,
        draftId  : null,
        eventType: enabled ? "gmail_watcher_enabled" : "gmail_watcher_disabled",
        actor,
        payload  : {
          enabled,
          ...(seededWatermarkMs ? {
            seededWatermarkMs,
            seededWatermarkIso: new Date(seededWatermarkMs).toISOString()
          } : {})
        },
        createdAt: FV.serverTimestamp()
      }).catch(()=>{});

      const [cfg, sync] = await Promise.all([readWatcherConfig(), readSyncSnapshot()]);
      return json(200, {
        ok: true,
        ...cfg,
        syncState: sync,
        // Echo the seed action back to the UI so the modal/topbar can
        // show a "watermark seeded to <iso>" toast on success.
        seededWatermarkMs,
        seededWatermarkIso: seededWatermarkMs
          ? new Date(seededWatermarkMs).toISOString() : null
      });
    }

    if (op === "setpullfrom") {
      // Operator-config endpoint: store the pull-from cursor without
      // touching the enabled flag. The settings modal calls this when
      // the operator picks a date/time and clicks save. The cursor is
      // applied the next time the watcher transitions off→on (see set).
      if (event.httpMethod !== "POST") return json(405, { error: "POST required for op=setPullFrom" });

      let body = {};
      try { body = JSON.parse(event.body || "{}"); }
      catch { return json(400, { error: "Invalid JSON body" }); }

      const { pullFromMs, actor } = body;
      if (!actor || typeof actor !== "string") {
        return json(400, { error: "Field 'actor' (operator name) is required" });
      }
      // null/undefined → clear. Otherwise must be a finite number ≤ now.
      const wantsClear = pullFromMs === null || typeof pullFromMs === "undefined";
      if (!wantsClear) {
        if (typeof pullFromMs !== "number" || !isFinite(pullFromMs) || pullFromMs <= 0) {
          return json(400, { error: "Field 'pullFromMs' must be a positive finite number (epoch ms) or null to clear" });
        }
        if (pullFromMs > Date.now()) {
          return json(400, { error: "pullFromMs cannot be in the future" });
        }
      }

      // Owner-only — same trust level as toggling.
      const owner = await requireOwner(actor);
      if (!owner.ok) {
        await logUnauthorized({
          actor,
          eventType: "gmail_watcher_pullfrom_unauthorized",
          payload  : { attemptedPullFromMs: wantsClear ? null : pullFromMs, reason: owner.reason }
        });
        return json(403, { error: "Owner role required", reason: owner.reason });
      }

      await db.doc(WATCHER_CFG_DOC).set({
        pullFromMs: wantsClear ? FV.delete() : pullFromMs,
        updatedAt : FV.serverTimestamp(),
        updatedBy : actor
      }, { merge: true });

      await db.collection(AUDIT_COLL).add({
        threadId : null,
        draftId  : null,
        eventType: "gmail_watcher_pullfrom_set",
        actor,
        payload  : wantsClear
          ? { cleared: true }
          : { pullFromMs, pullFromIso: new Date(pullFromMs).toISOString() },
        createdAt: FV.serverTimestamp()
      }).catch(()=>{});

      const [cfg, sync] = await Promise.all([readWatcherConfig(), readSyncSnapshot()]);
      return json(200, { ok: true, ...cfg, syncState: sync });
    }

    return json(400, { error: `Unknown op '${op}'. Use op=get, op=set, or op=setPullFrom.` });

  } catch (err) {
    console.error("etsyMailGmailConfig error:", err);
    return json(500, { error: err.message || "Unknown error" });
  }
};

// Small helper to peek at body.op without parsing twice on the GET path.
function safeBodyOp(event) {
  if (!event.body) return null;
  try {
    const b = JSON.parse(event.body);
    return b && typeof b.op === "string" ? b.op.toLowerCase() : null;
  } catch { return null; }
}
