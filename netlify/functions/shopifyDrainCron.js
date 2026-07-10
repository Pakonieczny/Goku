// netlify/functions/shopifyDrainCron.js
//
// SCHEDULED DRAIN for the Shopify upload queue.
//
// Why: over-budget jobs stay QUEUED until something calls drain. The app
// already drains on enqueue, on the Drain/Retry buttons, and on a 30-minute
// in-app timer — but all of those require a browser tab to be open. This
// cron covers the remaining case: the tab is closed overnight, the daily
// variant budget resets at midnight (Toronto time), and queued jobs should
// start flowing without anyone touching the app.
//
// Wiring (netlify.toml in this repo's root — create the file if it doesn't
// exist yet):
//
//   [functions."shopifyDrainCron"]
//     schedule = "*/30 * * * *"
//
// Every 30 minutes it POSTs op=drain to this site's own
// shopifyListingUpload function. Drains are cheap no-ops when the queue is
// empty, and the transactional QUEUED→UPLOADING claim in drain() makes
// overlap with in-app drains harmless.
//
// process.env.URL is injected by Netlify automatically (the site's primary
// URL), so there is nothing to configure.

exports.handler = async () => {
  const base = process.env.URL || process.env.DEPLOY_PRIME_URL;
  if (!base) {
    console.warn("shopifyDrainCron: no site URL available (URL / DEPLOY_PRIME_URL unset); skipping.");
    return { statusCode: 200, body: JSON.stringify({ ok: false, skipped: "no site url" }) };
  }
  const endpoint = `${base.replace(/\/$/, "")}/.netlify/functions/shopifyDrain-background?op=drain`;
  try {
    const res = await fetch(endpoint);
    const j = await res.json().catch(() => ({}));
    const up = ((j.drained || {}).uploaded || []).length;
    const failed = ((j.drained || {}).failed || []).length;
    console.log(`shopifyDrainCron: drained — ${up} uploaded, ${failed} failed, stoppedFor=${(j.drained || {}).stoppedFor || "none"}, budget remaining=${(j.budget || {}).remaining}`);
    return { statusCode: 200, body: JSON.stringify({ ok: true, uploaded: up, failed, budget: j.budget || null }) };
  } catch (e) {
    console.error("shopifyDrainCron: drain call failed:", e && e.message);
    return { statusCode: 200, body: JSON.stringify({ ok: false, error: String(e && e.message || e) }) };
  }
};
