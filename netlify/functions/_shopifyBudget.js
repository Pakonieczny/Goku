"use strict";
// Pure helpers for the Shopify daily variant budget (no Firebase, no network), shared by
// shopifyListingUpload.js and its tests.

const TZ = "America/Toronto";

function torontoDateKey(date) {
  return new Intl.DateTimeFormat("en-CA", { timeZone: TZ, year: "numeric", month: "2-digit", day: "2-digit" }).format(date || new Date());
}

// Milliseconds until the budget day rolls over (midnight in Toronto). Off by at most an hour on the two daylight-saving days a year.
function msUntilTorontoMidnight(now) {
  const parts = Object.fromEntries(
    new Intl.DateTimeFormat("en-CA", { timeZone: TZ, hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" })
      .formatToParts(now || new Date()).map((p) => [p.type, p.value])
  );
  const h = Number(parts.hour) % 24, m = Number(parts.minute), s = Number(parts.second);
  return ((24 - h) * 3600 - m * 60 - s) * 1000;
}

// jobs: [{ data }] oldest first. A job costs nothing more when its Shopify product already exists or it already
// holds today's reservation; otherwise it needs its variantCount to fit in what is left today.
function jobFits(data, remaining, todayKey) {
  if (!data) return false;
  if (data.productId || data.budgetReservedDay === todayKey) return true;
  return Number(data.variantCount || 0) <= remaining;
}
// The first (oldest) queued job that fits today's remaining budget, so one large job at the head can no longer
// leave the rest of the day's allowance unused while smaller jobs wait behind it.
function pickFit(jobs, remaining, todayKey) {
  for (const j of jobs || []) if (jobFits(j.data, remaining, todayKey)) return j;
  return null;
}
// Smallest variant count among jobs that would still need budget (for "paused until tomorrow" messages).
function smallestNeed(jobs, todayKey) {
  let min = Infinity;
  for (const j of jobs || []) {
    const d = j.data || {};
    if (d.productId || d.budgetReservedDay === todayKey) continue;
    min = Math.min(min, Number(d.variantCount || 0));
  }
  return min === Infinity ? 0 : min;
}

module.exports = { torontoDateKey, msUntilTorontoMidnight, jobFits, pickFit, smallestNeed };
