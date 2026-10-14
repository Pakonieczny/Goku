// Scheduled safety net for the Shopify upload queue: every 15 minutes it asks the background drain to run, so the
// queue keeps draining (and resumes the moment the daily variant budget resets at midnight Toronto) even when no
// browser tab is open. The drain itself does nothing when the queue is empty or the day's budget is spent.
export default async () => {
  const base = process.env.URL || process.env.DEPLOY_PRIME_URL;
  if (!base) {
    console.warn("shopifyDrainSchedule: no site URL available; skipping.");
    return new Response(JSON.stringify({ ok: false, skipped: "no site url" }), { status: 200 });
  }
  try {
    const res = await fetch(`${base.replace(/\/$/, "")}/.netlify/functions/shopifyDrain-background`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ source: "shopifyDrainSchedule" })
    });
    console.log(`shopifyDrainSchedule: background drain triggered (HTTP ${res.status}).`);
    return new Response(JSON.stringify({ ok: res.ok, status: res.status }), { status: 200 });
  } catch (e) {
    console.error("shopifyDrainSchedule: trigger failed:", e && e.message);
    return new Response(JSON.stringify({ ok: false, error: String((e && e.message) || e) }), { status: 500 });
  }
};

export const config = { schedule: "*/15 * * * *" };
