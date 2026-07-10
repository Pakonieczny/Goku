// /netlify/functions/shopifyDrain-background.js
//
// BACKGROUND DRAIN (15-minute budget). The synchronous drain path died at
// Netlify's ~26s limit once product creation gained inline vision
// set-matching — the gateway returned an HTML timeout page mid-job. All
// drains now run here: enqueue, the Drain/Retry buttons, and the cron all
// trigger this function and return immediately; the client reads results
// from job status polling.

const { drain, budgetSnapshot } = require("./shopifyListingUpload.js")._drain;

exports.handler = async function () {
  try {
    const drained = await drain();
    const budget = await budgetSnapshot();
    console.log("shopifyDrain-background:",
      JSON.stringify({ uploaded: (drained.uploaded || []).length, deferred: (drained.deferred || []).length, budget }));
  } catch (e) {
    console.error("shopifyDrain-background fatal:", e);
  }
  return { statusCode: 200, body: "done" };
};
