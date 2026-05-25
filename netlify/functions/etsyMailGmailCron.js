/*  netlify/functions/etsyMailGmailCron.js  (v1.1)
 *
 *  Scheduled trigger for the Gmail watcher. v1.1 adds a Firestore config-flag
 *  gate: the cron reads EtsyMail_Config/gmailWatcher.enabled before doing
 *  anything. Operators can toggle the watcher on/off from the inbox UI (via
 *  etsyMailGmailConfig.js) without touching netlify.toml or redeploying.
 *
 *  ═══ DECISION TABLE ═══════════════════════════════════════════════════
 *
 *    enabled    | lastSyncInProgress | action
 *    -----------+--------------------+----------------------------------
 *    false      | any                | skip (operator turned it off)
 *    true       | true               | skip (don't double-trigger)
 *    true       | false              | POST to etsyMailGmail-background
 *    no doc     | any                | skip (default-off — fail closed)
 *
 *  ═══ WHY DEFAULT-OFF ══════════════════════════════════════════════════
 *
 *  Pre-v1.1 the cron always ran when scheduled. v1.1 ships with the cron
 *  schedule live in netlify.toml but the flag default-off, so the watcher
 *  stays dormant until an operator explicitly clicks "enable" in the
 *  inbox. This avoids the v1.0 footgun where any manual `mode:"full"`
 *  trigger could create a flood of detected_from_gmail thread shells
 *  before anyone realized what was happening.
 *
 *  Schedule lives in netlify.toml:
 *    [functions."etsyMailGmailCron"]
 *      schedule = "* * * * *"   # every 1 minute
 */

"use strict";

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

const db = admin.firestore();

const SYNC_STATE_DOC = "EtsyMail_Config/gmailSyncState";
const WATCHER_CFG_DOC = "EtsyMail_Config/gmailWatcher";

async function readDoc(path) {
  const snap = await db.doc(path).get();
  return snap.exists ? snap.data() : null;
}

exports.handler = async (event) => {
  try {
    // ── Gate 1: operator-controlled enabled flag ─────────────────────
    // Default-off. Reading the doc each tick is cheap (single Firestore
    // read) and means the toggle takes effect within ≤1 minute of the
    // operator clicking it — no redeploy, no env-var change.
    const cfg = await readDoc(WATCHER_CFG_DOC);
    const enabled = !!(cfg && cfg.enabled === true);

    if (!enabled) {
      console.log("etsyMailGmailCron: skipped (gmailWatcher.enabled=false)");
      return {
        statusCode: 200,
        body: JSON.stringify({ skipped: true, reason: "watcher disabled" })
      };
    }

    // ── Gate 2: don't double-trigger if a sync is already running ────
    // v1.1 — Stale-lock detection. The background watcher writes
    // lastSyncInProgress=true at start and false at end. If it crashes
    // mid-run (or gets killed at the 15-min Netlify timeout) the lock
    // stays true forever and the cron would skip every tick after that.
    // Treat anything older than 20 min as stale and override. Netlify's
    // max background function runtime is 15 min, so 20 min is a safe
    // "definitely dead" threshold.
    const state = await readDoc(SYNC_STATE_DOC);
    if (state && state.lastSyncInProgress) {
      const startedMs = state.lastSyncStartedAt && state.lastSyncStartedAt.toMillis
        ? state.lastSyncStartedAt.toMillis()
        : 0;
      const ageMin = startedMs ? (Date.now() - startedMs) / 60000 : 999;
      if (ageMin < 20) {
        console.log(`etsyMailGmailCron: skipped (sync in progress, started ${Math.round(ageMin)} min ago)`);
        return {
          statusCode: 200,
          body: JSON.stringify({
            skipped     : true,
            reason      : "sync already in progress",
            lockAgeMin  : Math.round(ageMin)
          })
        };
      }
      // Lock is stale (>= 20 min old). The next invocation we trigger
      // will overwrite lastSyncInProgress=true with its own fresh
      // timestamp; the background watcher's own writeSyncState at the
      // start of runIncremental does this unconditionally.
      console.warn(`etsyMailGmailCron: clearing stale lock (started ${Math.round(ageMin)} min ago) and proceeding`);
    }

    // ── Invoke the background fn ─────────────────────────────────────
    const siteOrigin = process.env.URL
                    || process.env.DEPLOY_URL
                    || process.env.NETLIFY_BASE_URL;
    if (!siteOrigin) {
      console.error("Missing URL/DEPLOY_URL env vars — cannot determine site origin");
      return { statusCode: 500, body: "No site origin available" };
    }

    const targetUrl = `${siteOrigin}/.netlify/functions/etsyMailGmail-background`;
    console.log(`etsyMailGmailCron: invoking ${targetUrl}`);

    // Fire-and-forget POST. Netlify returns 202 for background functions.
    const resp = await fetch(targetUrl, {
      method : "POST",
      headers: { "Content-Type": "application/json" },
      body   : JSON.stringify({ cronTriggered: true })
    });

    console.log(`etsyMailGmailCron: invocation returned ${resp.status}`);

    return {
      statusCode: 200,
      body: JSON.stringify({
        triggered       : true,
        invocationStatus: resp.status
      })
    };

  } catch (err) {
    console.error("etsyMailGmailCron error:", err);
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
