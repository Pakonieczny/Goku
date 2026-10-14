"use strict";
// Pure helpers for the Shopify variant budget (no Firebase, no network), shared by
// shopifyListingUpload.js and its tests.
//
// Shopify limits new variants "per 24 hour period". The budget is therefore a ROLLING 24 h window: every reservation is
// kept with its time and stops counting exactly 24 h later, so the queue drains up to the ceiling in any 24 h and can
// never exceed what Shopify allows in any 24 h, whichever way Shopify resets its own counter.

const WINDOW_MS = 24 * 3600 * 1000;
const TZ = "America/Toronto";

function torontoDateKey(date) {
  return new Intl.DateTimeFormat("en-CA", { timeZone: TZ, year: "numeric", month: "2-digit", day: "2-digit" }).format(date || new Date());
}

// Start (ms) of the Toronto day containing `now` (only used to place the old calendar-day counter in the window once).
function torontoDayStartMs(now) {
  const parts = Object.fromEntries(
    new Intl.DateTimeFormat("en-CA", { timeZone: TZ, hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" })
      .formatToParts(new Date(now)).map((p) => [p.type, p.value])
  );
  return now - ((Number(parts.hour) % 24) * 3600 + Number(parts.minute) * 60 + Number(parts.second)) * 1000;
}

// entries: [{ t, n, j }] reservations (time, variants, job id). Drop what is older than 24 h.
function liveEntries(entries, now) {
  return (Array.isArray(entries) ? entries : []).filter((e) => e && Number(e.t) > now - WINDOW_MS && Number(e.n) > 0);
}
function usedIn(entries, now) {
  return liveEntries(entries, now).reduce((a, e) => a + Number(e.n), 0);
}
// The stored budget doc -> live entries. The old calendar-day doc {dateKey, used} is carried over once, placed at the
// start of its Toronto day (so it ages out at the old reset time instead of vanishing).
function entriesFromDoc(doc, now) {
  if (!doc) return [];
  if (Array.isArray(doc.entries)) return liveEntries(doc.entries, now);
  if (doc.dateKey && Number(doc.used) > 0) {
    const t = torontoDayStartMs(now);
    return doc.dateKey === torontoDateKey(new Date(now)) ? liveEntries([{ t, n: Number(doc.used), j: "migrated" }], now) : [];
  }
  return [];
}

// A job costs nothing more when its Shopify product already exists or its reservation is still in the window.
function jobFits(job, remaining, reservedIds) {
  const d = (job && job.data) || {};
  if (d.productId) return true;
  if (reservedIds && job.id != null && reservedIds.has(job.id)) return true;
  return Number(d.variantCount || 0) <= remaining;
}
// The first (oldest) queued job that fits what is left, so one large job at the head cannot leave the allowance unused.
function pickFit(jobs, remaining, reservedIds) {
  for (const j of jobs || []) if (jobFits(j, remaining, reservedIds)) return j;
  return null;
}
// Smallest variant count among jobs that would still need budget.
function smallestNeed(jobs, reservedIds) {
  let min = Infinity;
  for (const j of jobs || []) {
    const d = j.data || {};
    if (d.productId || (reservedIds && j.id != null && reservedIds.has(j.id))) continue;
    min = Math.min(min, Number(d.variantCount || 0));
  }
  return min === Infinity ? 0 : min;
}
// Milliseconds until `need` variants fit under `ceiling` (0 when they fit now): the moment enough old reservations age out.
function msUntilFits(entries, need, ceiling, now) {
  const live = liveEntries(entries, now).sort((a, b) => a.t - b.t);
  let free = ceiling - live.reduce((a, e) => a + Number(e.n), 0);
  if (free >= need) return 0;
  for (const e of live) {
    free += Number(e.n);
    if (free >= need) return Math.max(0, Number(e.t) + WINDOW_MS - now);
  }
  return 0;
}

module.exports = { WINDOW_MS, torontoDateKey, torontoDayStartMs, liveEntries, usedIn, entriesFromDoc, jobFits, pickFit, smallestNeed, msUntilFits };
