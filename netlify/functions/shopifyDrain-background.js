// /netlify/functions/shopifyDrain-background.js
//
// BACKGROUND DRAIN (15-minute budget). The synchronous drain path died at
// Netlify's ~26s limit once product creation gained inline vision
// set-matching — the gateway returned an HTML timeout page mid-job. All
// drains now run here: enqueue, the Drain/Retry buttons, and the cron all
// trigger this function and return immediately; the client reads results
// from job status polling.

const { drain, budgetSnapshot } = require("./shopifyListingUpload.js")._drain;

async function triggerContinuation() {
  const base = (process.env.URL || process.env.DEPLOY_PRIME_URL || process.env.DEPLOY_URL || "").replace(/\/$/, "");
  if (!base) {
    console.warn("shopifyDrain-background: cannot continue — no site URL.");
    return false;
  }
  try {
    const res = await fetch(base + "/.netlify/functions/shopifyDrain-background", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ source: "shopifyDrain-background-continuation" })
    });
    if (!res.ok) throw new Error("HTTP " + res.status);
    return true;
  } catch (e) {
    console.error("shopifyDrain-background continuation failed:", e && e.message);
    return false;
  }
}

exports.handler = async function () {
  try {
    const drained = await drain();
    const budget = await budgetSnapshot();
    console.log("shopifyDrain-background:",
      JSON.stringify({
        uploaded: (drained.uploaded || []).length,
        failed: (drained.failed || []).length,
        recovered: drained.recovered || 0,
        locked: !!drained.locked,
        stoppedFor: drained.stoppedFor || null,
        moreQueued: !!drained.moreQueued,
        budget
      }));

    // A single background invocation has a 15-minute hard limit. If the
    // controlled 12-minute drain window expires with work remaining, start
    // the next worker immediately instead of waiting for the cron safety net.
    if (drained.moreQueued && drained.stoppedFor === "time") {
      await triggerContinuation();
    }
  } catch (e) {
    console.error("shopifyDrain-background fatal:", e);
  }
  return { statusCode: 200, body: "done" };
};
