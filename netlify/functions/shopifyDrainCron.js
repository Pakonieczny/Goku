// netlify/functions/shopifyDrainCron.js
//
// Scheduled safety net for the Shopify upload queue. The interactive app
// triggers the background drain immediately after enqueue, while this function
// re-triggers it every 30 minutes so queued/retryable jobs do not depend on a
// browser tab remaining open.
//
// Netlify background functions acknowledge the trigger before their long work
// finishes. The actual outcome remains in the Firestore job documents and is
// displayed by the Listing Generator status panel.

exports.handler = async () => {
  const base = process.env.URL || process.env.DEPLOY_PRIME_URL;
  if (!base) {
    console.warn("shopifyDrainCron: no site URL available; skipping.");
    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
      body: JSON.stringify({ ok: false, skipped: "no site url" })
    };
  }

  const endpoint = `${base.replace(/\/$/, "")}/.netlify/functions/shopifyDrain-background`;
  try {
    const res = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ source: "shopifyDrainCron" })
    });

    if (!res.ok) {
      const detail = await res.text().catch(() => "");
      throw new Error(`background trigger HTTP ${res.status}${detail ? `: ${detail.slice(0, 300)}` : ""}`);
    }

    console.log(`shopifyDrainCron: background drain accepted (HTTP ${res.status}).`);
    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
      body: JSON.stringify({ ok: true, accepted: true, triggerStatus: res.status })
    };
  } catch (e) {
    console.error("shopifyDrainCron: background trigger failed:", e && e.message);
    // Return 500 so Netlify records the scheduled invocation as failed rather
    // than silently reporting success while the queue was never triggered.
    return {
      statusCode: 500,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
      body: JSON.stringify({ ok: false, error: String((e && e.message) || e) })
    };
  }
};
