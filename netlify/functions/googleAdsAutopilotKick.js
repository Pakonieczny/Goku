// netlify/functions/googleAdsAutopilotKick.js
// ─────────────────────────────────────────────────────────────────────────────
// One file, three jobs (same scheduled+HTTP shape as returnPolicyLabeler.js):
//   1) SCHEDULED (hourly, x-nf-event:schedule) → decide which tasks are due and
//      POST them to googleAdsAutopilot-background. Cheap tasks hourly; heavy
//      tasks once daily; budget reallocation weekly.
//   2) GET  → serve the operator console (approval queue + kill switch + toggles).
//   3) POST → console actions (approve/reject/apply, kill/resume, dry-run, run-now),
//      guarded by EDIT_PASSCODE (same passcode the rest of the tooling uses).
//
// Only THIS function is scheduled (one netlify.toml entry). The worker and console
// are triggered, not scheduled — so the whole system adds exactly one cron line.
//
// ═══ WHY JOBS 2 AND 3 NOW LIVE IN googleAdsAutopilotApi.js ═══════════════════
//
// A scheduled Netlify function CANNOT be reached over HTTP. Netlify's own docs:
//
//     "You can't invoke scheduled functions directly with a URL."
//     "Scheduled functions only run on their schedule for published deploys
//      and ... you can't invoke them directly with a URL."
//
// Netlify answers any direct request to a scheduled function with a bare
// 403 — no body, no log line. So the moment this function got its cron entry,
// the console it also served became unreachable: every unlock attempt showed
// "HTTP 403" with nothing to explain it, and the passcode was never the issue
// (authed() returns 401, never 403).
//
// The three jobs are therefore SPLIT, with no logic duplicated:
//
//   this file                     the cron entry. Scheduled invocation only.
//   googleAdsAutopilotApi.js      the console HTML + the POST actions. NOT
//                                 scheduled, so HTTP works normally.
//
// handleAction, kick and CONSOLE_HTML are exported below and required by that
// file, so there is exactly one copy of each. DO NOT add a schedule entry for
// googleAdsAutopilotApi — that is the whole bug, reintroduced.
// ─────────────────────────────────────────────────────────────────────────────

const fetch = require("node-fetch");
const E = require("./googleAdsAutopilot");

let _fb = null;
function fb() {
  if (_fb !== null) return _fb;
  try { const admin = require("./firebaseAdmin"); _fb = { admin, db: admin.firestore(), FV: admin.firestore.FieldValue }; }
  catch (e) { _fb = false; }
  return _fb;
}
function baseUrl() { return process.env.URL || ("https://" + (process.env.SITE_NAME || "goldenspike") + ".netlify.app"); }
const HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, X-Edit-Passcode",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Content-Type": "application/json"
};
function ok(o) { return { statusCode: 200, headers: HEADERS, body: JSON.stringify(o) }; }
function authed(event, body) {
  const pass = process.env.EDIT_PASSCODE || "";
  if (!pass) return true; // if unset, console is open (set EDIT_PASSCODE to lock)
  const h = (event.headers && (event.headers["x-edit-passcode"] || event.headers["X-Edit-Passcode"])) || "";
  return h === pass || (body && body.passcode === pass);
}

/* ----------------------------- scheduled kick ----------------------------- */
async function decideTasks() {
  const now = new Date();
  const tasks = new Set(["anomaly", "monthly", "ceiling", "conversions"]); // cheap + high value every hour
  const dailyHour = Number(process.env.GADS_DAILY_HOUR || 8);   // UTC
  const weeklyDow = Number(process.env.GADS_WEEKLY_DOW || 1);   // 1=Mon
  const isDailySlot = now.getUTCHours() === dailyHour;
  let ranDaily = false, ranWeekly = false;
  const f = fb();
  const dayKey = now.toISOString().slice(0, 10);
  if (isDailySlot && f) {
    try {
      const ref = f.db.collection(E.COL.state).doc("cycle");
      const s = await ref.get(); const st = s.exists ? s.data() : {};
      if (st.lastDailyKey !== dayKey) {
        ["measure", "mine", "prune", "events"].forEach(t => tasks.add(t)); ranDaily = true;
        if (now.getUTCDay() === weeklyDow && st.lastWeeklyKey !== dayKey) { tasks.add("budgets"); ranWeekly = true; }
        await ref.set({ lastDailyKey: dayKey, lastWeeklyKey: ranWeekly ? dayKey : (st.lastWeeklyKey || null),
                        lastDecidedAt: f.FV.serverTimestamp() }, { merge: true });
      }
    } catch (e) {}
  } else if (isDailySlot && !f) {
    ["measure", "mine", "prune", "events"].forEach(t => tasks.add(t));
    if (now.getUTCDay() === weeklyDow) tasks.add("budgets");
  }
  return { tasks: [...tasks], ranDaily, ranWeekly };
}

async function kick() {
  const ctrl = await E.control();
  if (!ctrl.enabled) return { status: "dormant (kill switch off)" };
  const { tasks, ranDaily } = await decideTasks();
  // Once a day: reconcile the Best Sellers collection with the canonical Top-200 list. It runs as
  // a background-worker task (the sync makes a few hundred Shopify calls and would time out a
  // regular function); the report lands in Brites_Editor_Meta/bestSellersSyncReport.
  if (ranDaily) tasks.push("bestSellers");
  const res = await fetch(baseUrl() + "/.netlify/functions/googleAdsAutopilot-background", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ tasks, token: process.env.EDIT_PASSCODE || undefined })
  });
  return { status: "kicked", tasks, upstream: res.status };
}

/* ------------------------------- POST actions ------------------------------ */
async function handleAction(body) {
  const f = fb();
  const a = body.action;
  const ctrl = await E.control();
  if (a === "dashboard") return await E.dashboard();
  if (a === "metricsRange") { try { return await E.metricsRange({ start: body.start, end: body.end }); } catch (e) { return { error: e.message }; } }
  if (a === "keywordDiag") { try { return await E.keywordDiag({ keyword: body.keyword, geo: body.geo }); } catch (e) { return { ok: false, error: e.message }; } }
  if (a === "conversionHealth") { try { return await E.conversionHealth({ force: !!body.force }); } catch (e) { return { error: e.message }; } }
  if (a === "syncConversions") {
    try { const up = await E.uploadConversions({ ctrl }); const adj = await E.uploadConversionAdjustments({ ctrl }); const health = await E.conversionHealth({ force: true }); return { ok: true, uploaded: up, adjustments: adj, health }; }
    catch (e) { return { ok: false, error: e.message }; }
  }
  if (a === "kill")   { if (f) await f.db.collection(E.COL.control).doc("control").set({ enabled: false }, { merge: true }); return { enabled: false }; }
  if (a === "resume") { if (f) await f.db.collection(E.COL.control).doc("control").set({ enabled: true }, { merge: true });  return { enabled: true }; }
  if (a === "dryRun") { if (f) await f.db.collection(E.COL.control).doc("control").set({ dryRun: !!body.on }, { merge: true }); return { dryRun: !!body.on }; }
  if (a === "setControl") {
    const allow = ["maxDailyBudgetTotal","maxBudgetStepPct","budgetMoveApprovalPct","targetRoas",
                   "minConvForTargetTune","anomalySpendMultiple","autoApproveVettedTemplates","learningCooldownDays",
                   "defaultCountries","maxMonthlySpend","smartBidding"];
    const patch = {}; allow.forEach(k => { if (body.patch && body.patch[k] !== undefined) patch[k] = body.patch[k]; });
    if (patch.smartBidding !== undefined) patch.smartBidding = !!patch.smartBidding;
    if (patch.defaultCountries !== undefined) {
      patch.defaultCountries = [...new Set((Array.isArray(patch.defaultCountries) ? patch.defaultCountries : [])
        .map(x => String(x).replace(/\D/g, "")).filter(Boolean))];
    }
    if (patch.maxMonthlySpend !== undefined) {
      const n = Number(patch.maxMonthlySpend); patch.maxMonthlySpend = (isFinite(n) && n > 0) ? Math.round(n * 100) / 100 : 0;
    }
    if (f && Object.keys(patch).length) await f.db.collection(E.COL.control).doc("control").set(patch, { merge: true });
    return { patched: patch };
  }
  if (a === "reject") { if (f) await f.db.collection(E.COL.approvals).doc(body.id).set({ status: "REJECTED" }, { merge: true }); return { id: body.id, status: "REJECTED" }; }
  if (a === "approve" || a === "apply") {
    if (!f) return { error: "no firestore" };
    if (a === "approve") await f.db.collection(E.COL.approvals).doc(body.id).set({ status: "APPROVED" }, { merge: true });
    try { await E.applyApproval(body.id, ctrl); return { id: body.id, status: "APPLIED", dryRun: !!ctrl.dryRun }; }
    catch (e) { return { id: body.id, error: e.message }; }
  }
  if (a === "retryStuck") {
    try { return await E.retryStuckApprovals(ctrl); }
    catch (e) { return { error: e.message }; }
  }
  if (a === "setBudget") {
    try { return await E.setCampaignBudget(body.id, body.budget, { ctrl, budgetRes: body.budgetRes }); }
    catch (e) { return { ok: false, error: e.message }; }
  }
  // ---- Ad Doctor: Google Ads diagnostics + AI specialist verdicts ----
  if (a === "dailyStats")    { try { return await E.dailyStats({ start: body.start, end: body.end }); } catch (e) { return { error: e.message }; } }
  if (a === "diagnostics")    { try { return (await E.getDiagnostics()) || { empty: true }; } catch (e) { return { error: e.message }; } }
  if (a === "runDiagnostics") {
    // Heavy (multi-GAQL + LLM) — runs in the background worker to dodge the
    // 26s gateway timeout. The worker writes a per-run status doc so the
    // console can show the REAL failure instead of timing out blind.
    try {
      const runId = String(body.runId || Date.now());
      const res = await fetch(baseUrl() + "/.netlify/functions/googleAdsAutopilot-background", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tasks: ["diagnostics"], campaignId: body.campaignId || null, runId, token: process.env.EDIT_PASSCODE || undefined })
      });
      if (res.status >= 400) return { error: "background dispatch failed: HTTP " + res.status + " — is googleAdsAutopilot-background deployed?" };
      return { queued: true, runId, upstream: res.status };
    } catch (e) { return { error: e.message }; }
  }
  if (a === "pmaxPreview") {
    try { return await E.pmaxPreviewData({ handle: body.handle, titles: body.titles, n: body.n }); }
    catch (e) { return { error: e.message }; }
  }
  if (a === "pmaxBackfillImages") {
    // One-time (re-runnable) job: re-images already-ENABLED PMax campaigns with vision-
    // selected shots. Heavy (per-product vision + Shopify + Google Ads GAQL across every
    // asset group) — runs in the background worker; console polls gen_<genId> for progress
    // and the final { campaigns, assetGroups, queued, results } summary.
    try {
      const genId = String(body.genId || Date.now());
      const res = await fetch(baseUrl() + "/.netlify/functions/googleAdsAutopilot-background", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tasks: ["pmaxBackfillImages"], genId,
          campaignIds: Array.isArray(body.campaignIds) ? body.campaignIds.slice(0, 50) : [],
          token: process.env.EDIT_PASSCODE || undefined })
      });
      if (res.status >= 400) return { error: "background dispatch failed: HTTP " + res.status + " — is googleAdsAutopilot-background deployed?" };
      return { queued: true, genId, upstream: res.status };
    } catch (e) { return { error: e.message }; }
  }
  if (a === "pmaxUpgradeAdStrength") {
    // One-time (re-runnable) job: tops up headlines/long headlines/descriptions/sitelinks
    // on already-LIVE PMax campaigns without touching what's already there — the safe
    // alternative to deleting and relaunching. Heavy (GAQL scan + AI copy per campaign);
    // runs in the background worker, console polls gen_<genId> for progress.
    try {
      const genId = String(body.genId || Date.now());
      const res = await fetch(baseUrl() + "/.netlify/functions/googleAdsAutopilot-background", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tasks: ["pmaxUpgradeAdStrength"], genId,
          campaignIds: Array.isArray(body.campaignIds) ? body.campaignIds.slice(0, 50) : [],
          token: process.env.EDIT_PASSCODE || undefined })
      });
      if (res.status >= 400) return { error: "background dispatch failed: HTTP " + res.status + " — is googleAdsAutopilot-background deployed?" };
      return { queued: true, genId, upstream: res.status };
    } catch (e) { return { error: e.message }; }
  }
  if (a === "generatePmax") {
    // Merchant-catalog validation + multi-operation PMax build run in the background;
    // the console polls gen_<genId> via genStatus, same as Search generation.
    try {
      const genId = String(body.genId || Date.now());
      const res = await fetch(baseUrl() + "/.netlify/functions/googleAdsAutopilot-background", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tasks: ["pmaxGenerate"], genId, handle: body.handle, dailyBudget: body.dailyBudget, targetRoas: body.targetRoas, days: body.days,
          itemIds:Array.isArray(body.itemIds)?body.itemIds.slice(0,30):[],productTitles:Array.isArray(body.productTitles)?body.productTitles.slice(0,10):[],
          feedLabel:body.feedLabel||null,searchThemes:Array.isArray(body.searchThemes)?body.searchThemes.slice(0,25):[],offerDetails:Array.isArray(body.offerDetails)?body.offerDetails.slice(0,30):[],
          token:process.env.EDIT_PASSCODE||undefined })
      });
      if (res.status >= 400) return { error: "background dispatch failed: HTTP " + res.status };
      return { queued: true, genId };
    } catch (e) { return { error: e.message }; }
  }
  if (a === "diagRunStatus") {
    try { return (await E.getGenStatus("diag-" + String(body.runId))) || { pending: true }; }
    catch (e) { return { error: e.message }; }
  }
  if (a === "playbook") { try { return (await E.getPlaybook()) || { empty: true }; } catch (e) { return { error: e.message }; } }
  if (a === "distill") {
    // LLM-heavy -> background worker; the console polls the playbook doc.
    try {
      const res = await fetch(baseUrl() + "/.netlify/functions/googleAdsAutopilot-background", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tasks: ["distill"], token: process.env.EDIT_PASSCODE || undefined })
      });
      return { queued: true, upstream: res.status };
    } catch (e) { return { error: e.message }; }
  }
  if (a === "adReviewStatus") { try { return await E.adReviewStatus({ adIds: body.adIds }); } catch (e) { return { statuses: {}, error: e.message }; } }
  if (a === "remedyHistory") { try { return await E.remedyHistory({ limit: body.limit || 100 }); } catch (e) { return { items: [], error: e.message }; } }
  if (a === "applyRemedy")   { try { return await E.applyRemedy(body.campaignId, body.remedy, { ctrl }); } catch (e) { return { ok: false, error: e.message }; } }
  if (a === "applyRec")       { try { return await E.applyGoogleRecommendation(body.resourceName, { ctrl }); } catch (e) { return { ok: false, error: e.message }; } }
  if (a === "dismissRec")     { try { return await E.dismissGoogleRecommendation(body.resourceName); } catch (e) { return { ok: false, error: e.message }; } }
  if (a === "analyzeCampaign") {
    try { return await E.analyzeCampaign(body.id, { force: !!body.force }); }
    catch (e) { return { error: e.message }; }
  }
  if (a === "campaignTimeline") {
    try { return await E.campaignTimeline({ id: body.id }); }
    catch (e) { return { error: e.message }; }
  }
  if (a === "setStatus") {
    try { return await E.setCampaignStatus(body.id, body.status, { ctrl }); }
    catch (e) { return { ok: false, error: e.message }; }
  }
  if (a === "startNow") {
    try { return await E.startCampaignNow(body.id, { ctrl }); }
    catch (e) { return { ok: false, error: e.message }; }
  }
  if (a === "enforceCeiling") {
    try { return await E.enforceBudgetCeiling({ ctrl }); }
    catch (e) { return { ok: false, error: e.message }; }
  }
  if (a === "backfillOrders") {
    try { return await E.backfillOrders({ limit: Number(body.limit) || 100 }); }
    catch (e) { return { ok: false, error: e.message }; }
  }
  if (a === "monthlyGuard") {
    try { return await E.monthlySpendGuard({ ctrl }); }
    catch (e) { return { ok: false, error: e.message }; }
  }
  if (a === "countries") {
    try { return { ok: true, list: await E.listCountries({ force: !!body.force }) }; }
    catch (e) { return { ok: false, error: e.message }; }
  }
  if (a === "setCountries") {
    try { return await E.setCampaignCountries(body.id, body.countries || [], { ctrl }); }
    catch (e) { return { ok: false, error: e.message }; }
  }
  if (a === "setApprovalDates") {
    try { return await E.setApprovalDates(body.id, body.startDate, body.endDate); }
    catch (e) { return { ok: false, error: e.message }; }
  }
  if (a === "setApprovalCountries") {
    try { return await E.setApprovalCountries(body.id, body.countries || []); }
    catch (e) { return { ok: false, error: e.message }; }
  }
  if (a === "clearLedger") {
    try { return await E.clearLedger({}); }
    catch (e) { return { ok: false, error: e.message }; }
  }

  if (a === "opportunities") {
    try {
      if (body.force) {
        // The AI scan is slow and belongs in the 15-minute background worker. Create a
        // verifiable run record BEFORE dispatch so the console can distinguish “button worked”
        // from “worker never received it”, then pass the same runId through every layer.
        const runId = "opp-" + Date.now() + "-" + Math.random().toString(36).slice(2, 8);
        const dispatchAt = Date.now();
        const baseAudit = { schema: 2, engineVersion:E.OPPORTUNITY_ENGINE_VERSION||"12.2.0-opportunity-scan-observability", runId, status: "queued", startedAt: dispatchAt, completedAt: null, updatedAt: dispatchAt,
          summary: { total: 2, ok: 1, warning: 0, failed: 0, skipped: 0, running: 0, queued: 1 },
          checks: [
            { id:"console_request",category:"orchestration",label:"Console scan request",status:"ok",startedAt:dispatchAt,endedAt:dispatchAt,tookMs:0,detail:"Authenticated Opportunities scan request accepted by the API gateway." },
            { id:"background_worker",category:"orchestration",label:"Background worker dispatch",status:"queued",startedAt:dispatchAt,detail:"Dispatching the read-only scan to the Netlify background function." }
          ] };
        try { if (f) await Promise.all([
          f.db.collection(E.COL.state).doc("opportunities").set({ scanning: true, progress:{pct:1,label:"Dispatching background worker",detail:"run "+runId,at:Date.now()} }, { merge: true }),
          f.db.collection(E.COL.state).doc("opportunityScanAudit").set({ scanAudit:baseAudit }, { merge:true })
        ]); } catch (e) {}
        let bgStatus = null, bgError = null;
        try {
          const res = await fetch(baseUrl() + "/.netlify/functions/googleAdsAutopilot-background", {
            method: "POST", headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ tasks: ["scanOpportunities"], scanRunId: runId, token: process.env.EDIT_PASSCODE || undefined })
          });
          bgStatus = res.status;
          if (res.status >= 400) bgError = "background dispatch HTTP " + res.status;
        } catch (e) { bgError = e && e.message || String(e); }
        baseAudit.status = bgError ? "failed" : "running"; baseAudit.updatedAt = Date.now();
        const bg = baseAudit.checks[1]; bg.status = bgError ? "failed" : "ok"; bg.endedAt = Date.now(); bg.tookMs = bg.endedAt - dispatchAt; bg.httpStatus = bgStatus;
        bg.detail = bgError ? "The background worker did not accept the scan." : "Background worker accepted the scan; execution is now asynchronous."; bg.error = bgError;
        baseAudit.summary = { total:2, ok:bgError?1:2, warning:0, failed:bgError?1:0, skipped:0, running:0, queued:0 };
        if (bgError) baseAudit.completedAt = Date.now();
        try { if (f) await Promise.all([
          f.db.collection(E.COL.state).doc("opportunities").set({ scanning:!bgError, lastError:bgError||null, lastErrorAt:bgError?Date.now():null }, { merge:true }),
          f.db.collection(E.COL.state).doc("opportunityScanAudit").set({ scanAudit:baseAudit }, { merge:true })
        ]); } catch (e) {}
        const cur = await E.opportunitiesWithStatus({ cacheOnly: true });
        return Object.assign({}, cur, { scanning: !bgError, started: !bgError, runId, dispatchStatus:bgStatus, dispatchError:bgError });
      }
      // Never scan synchronously from the console — cache read only.
      return await E.opportunitiesWithStatus({ cacheOnly: true });
    } catch (e) { return { opportunities: [], error: e.message }; }
  }
  if (a === "collections") {
    try { return { collections: await E.getCollections({ force: !!body.force }) }; }
    catch (e) { return { collections: [], error: e.message }; }
  }
  if (a === "occasions") {
    try { return { occasions: await E.suggestOccasions(body.coll, { force: !!body.force }) }; }
    catch (e) { return { occasions: [], error: e.message }; }
  }
  if (a === "releaseOpportunity") {
    try { return await E.releaseOpportunity({ tag: body.tag }); }
    catch (e) { return { ok: false, error: e.message }; }
  }
  if (a === "generate") {
    // Keyword research + high-effort copy generation outruns the 26s gateway
    // -> background worker; the console polls genStatus for the outcome.
    try {
      const genId = String(Date.now()) + Math.random().toString(36).slice(2, 7);
      const res = await fetch(baseUrl() + "/.netlify/functions/googleAdsAutopilot-background", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tasks: ["generate"], genId,
          coll: body.coll, event: body.event, budget: body.budget,
          startDate: body.startDate, endDate: body.endDate, countries: body.countries,
          maxCpc: body.maxCpc, peakDate: body.peakDate, smartBidding: body.smartBidding,
          token: process.env.EDIT_PASSCODE || undefined })
      });
      return { queued: true, genId, upstream: res.status };
    } catch (e) { return { ok: false, reason: e.message }; }
  }
  if (a === "genStatus") {
    try { return (await E.getGenStatus(body.genId)) || { pending: true }; }
    catch (e) { return { pending: true, error: e.message }; }
  }
  if (a === "measureNow") {
    try { const snap = await E.measure(); return { ok: true, campaigns: Array.isArray(snap) ? snap.length : null, at: Date.now() }; }
    catch (e) { return { ok: false, error: e.message }; }
  }
  if (a === "runNow") {
    const tasks = Array.isArray(body.tasks) && body.tasks.length ? body.tasks
      : ["anomaly","conversions","measure","mine","prune","budgets","events"];
    const res = await fetch(baseUrl() + "/.netlify/functions/googleAdsAutopilot-background", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ tasks, token: process.env.EDIT_PASSCODE || undefined }) });
    return { status: "kicked", tasks, upstream: res.status };
  }
  return { error: "unknown action" };
}

/* --------------------------------- handler -------------------------------- */
//  SCHEDULED ONLY. The HTTP surface moved to googleAdsAutopilotApi.js because
//  Netlify refuses HTTP requests to a scheduled function (403, empty body).
//  The GET/POST branches are kept below the scheduled path purely so a LOCAL
//  `netlify dev` invocation still works; in production this handler is only
//  ever reached by the scheduler.
exports.handler = async (event) => {
  const scheduled = !!(event && event.headers &&
    (event.headers["x-nf-event"] === "schedule" || event.isScheduled));
  if (scheduled) {
    try { const out = await kick(); console.log("[gadsKick] scheduled:", JSON.stringify(out)); return ok(out); }
    catch (e) { console.error("[gadsKick] scheduled error", e.message); return ok({ status: "error", error: e.message }); }
  }
  return httpHandler(event);
};

/*  The HTTP behaviour, exported so googleAdsAutopilotApi.js is a two-line file
    rather than a second copy of anything. */
async function httpHandler(event) {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: HEADERS };

  // GET → console HTML
  if (event.httpMethod === "GET") {
    return { statusCode: 200, headers: { "Content-Type": "text/html; charset=utf-8" }, body: CONSOLE_HTML };
  }

  // POST → actions (auth required)
  let body = {}; try { body = JSON.parse(event.body || "{}"); } catch {}
  if (!authed(event, body)) return { statusCode: 401, headers: HEADERS, body: JSON.stringify({ error: "unauthorized" }) };
  try { return ok(await handleAction(body)); }
  catch (e) { return { statusCode: 500, headers: HEADERS, body: JSON.stringify({ error: e.message }) }; }
}

// Reused by googleAdsAutopilotApi.js — one implementation, two entry points.
module.exports.httpHandler = httpHandler;
module.exports.handleAction = handleAction;
module.exports.kick = kick;

/* ------------------------------ console (HTML) ----------------------------- */
const CONSOLE_HTML = `<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Brites · Ad Autopilot</title>
<style>
*{box-sizing:border-box}body{margin:0;font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:#faf9f7;color:#1a1a1a;padding:24px;max-width:1000px;margin:0 auto}
h1{font-family:Georgia,serif;font-size:24px;margin:0 0 2px}.sub{color:#777;font-size:13px;margin:0 0 18px}
.bar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;background:#fff;border:1px solid #ececec;border-radius:12px;padding:14px;margin-bottom:16px}
.pill{font-size:12px;font-weight:700;padding:4px 10px;border-radius:20px}
.on{background:#e7f6ec;color:#1c7c3f}.off{background:#fdeaea;color:#b42424}.dry{background:#fff5e6;color:#a06b2f}
button{font:inherit;font-size:13px;border:1px solid #1a1a1a;background:#1a1a1a;color:#fff;border-radius:8px;padding:7px 12px;cursor:pointer}
button.ghost{background:#fff;color:#1a1a1a}button.warn{background:#b42424;border-color:#b42424}
input{font:inherit;border:1px solid #ccc;border-radius:8px;padding:7px 10px}
.card{background:#fff;border:1px solid #ececec;border-radius:12px;padding:16px;margin-bottom:12px}
.q{border-left:3px solid #cdb98a;padding:10px 12px;margin:8px 0;background:#fcfbf9;border-radius:0 8px 8px 0}
.q .t{font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#9a8f00;font-weight:700}
.q .s{font-size:14px;margin:3px 0 8px}.muted{color:#888;font-size:12px}
pre{font-size:11px;white-space:pre-wrap;background:#f6f5f2;border-radius:8px;padding:10px;max-height:200px;overflow:auto}
h3{font-size:14px;margin:0 0 8px;letter-spacing:.04em}
</style></head><body>
<h1>Ad Autopilot</h1><p class="sub">Brites Jewelry · review queue, controls, and live status</p>
<div id="gate" class="card"><h3>Passcode</h3><input id="pc" type="password" placeholder="edit passcode" style="width:240px">
<button onclick="boot()">Unlock</button><span id="ge" class="muted"></span></div>
<div id="app" style="display:none">
  <div class="bar" id="ctrlbar"></div>
  <div class="card"><h3>Pending approvals</h3><div id="queue"><p class="muted">loading…</p></div></div>
  <div class="card"><h3>Run now</h3>
    <button class="ghost" onclick="run(['conversions'])">Upload conversions</button>
    <button class="ghost" onclick="run(['measure'])">Measure</button>
    <button class="ghost" onclick="run(['mine','prune'])">Mine + prune</button>
    <button class="ghost" onclick="run(['budgets'])">Reallocate budgets</button>
    <button class="ghost" onclick="run(['events'])">Generate due events</button>
    <span id="rs" class="muted"></span></div>
  <div class="card"><h3>Last campaign snapshot</h3><pre id="metrics">—</pre></div>
  <div class="card"><h3>Recent activity</h3><pre id="ledger">—</pre></div>
</div>
<script>
var PC="";
function api(action,extra){return fetch(location.pathname,{method:"POST",headers:{"Content-Type":"application/json","X-Edit-Passcode":PC},
  body:JSON.stringify(Object.assign({action:action,passcode:PC},extra||{}))}).then(function(r){return r.json()})}
function boot(){PC=document.getElementById("pc").value;api("dashboard").then(function(d){
  if(d.error){document.getElementById("ge").textContent=" "+d.error;return}
  document.getElementById("gate").style.display="none";document.getElementById("app").style.display="block";render(d)})}
function render(d){
  var c=d.control||{};var bar=document.getElementById("ctrlbar");
  bar.innerHTML='<span class="pill '+(c.enabled?'on':'off')+'">'+(c.enabled?'LIVE':'STOPPED')+'</span>'+
    '<span class="pill '+(c.dryRun?'dry':'on')+'">'+(c.dryRun?'DRY-RUN':'APPLYING')+'</span>'+
    '<span class="muted">ceiling $'+(c.maxDailyBudgetTotal)+'/day · step '+(c.maxBudgetStepPct)+'% · approve&gt;'+(c.budgetMoveApprovalPct)+'%</span>'+
    (c.enabled?'<button class="warn" onclick="act(\\'kill\\')">KILL</button>':'<button onclick="act(\\'resume\\')">Resume</button>')+
    '<button class="ghost" onclick="dry('+(!c.dryRun)+')">'+(c.dryRun?'Go live (apply)':'Switch to dry-run')+'</button>';
  var q=document.getElementById("queue");var p=d.pending||[];
  q.innerHTML=p.length?'':'<p class="muted">nothing waiting 🎉</p>';
  p.forEach(function(it){var el=document.createElement("div");el.className="q";
    el.innerHTML='<div class="t">'+it.type+(it.vetted?' · vetted':'')+'</div><div class="s">'+(it.summary||'')+'</div>'+
      '<button onclick="approve(\\''+it.id+'\\')">Approve</button> <button class="ghost" onclick="reject(\\''+it.id+'\\')">Reject</button>';
    q.appendChild(el)});
  document.getElementById("metrics").textContent=d.lastMetrics?JSON.stringify(d.lastMetrics,null,1):"—";
  document.getElementById("ledger").textContent=(d.recentLedger||[]).map(function(l){return (l.kind||'')+' '+(l.service||l.label||'')+' '+(l.ok?'ok':'ERR')+(l.validateOnly?' [dry]':'')}).join("\\n")||"—";
}
function refresh(){api("dashboard").then(render)}
function act(a){api(a).then(refresh)}
function dry(on){api("dryRun",{on:on}).then(refresh)}
function approve(id){api("approve",{id:id}).then(refresh)}
function reject(id){api("reject",{id:id}).then(refresh)}
function run(t){document.getElementById("rs").textContent=" running…";api("runNow",{tasks:t}).then(function(r){document.getElementById("rs").textContent=" "+(r.status||'')+" ["+t.join(',')+"]";setTimeout(refresh,4000)})}
</script></body></html>`;
