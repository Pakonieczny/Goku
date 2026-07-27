/*  netlify/functions/etsyPricingApplyOne-background.js
 *
 *  POST { listing_id, request_id, queue_id?, category?, chain_type?, engraving?, access_token? }
 *
 *  Applies the canonical Brites pricing scheme to ONE listing — the same
 *  rebuild the Pricing Console performs, fired by the Listing Generator the
 *  moment it finishes uploading a set.
 *
 *  ═══ WHY THIS IS A BACKGROUND FUNCTION ═════════════════════════════════
 *
 *  The synchronous version had a 10-second platform budget, but its proxy
 *  chain always pays for a real verification GET (every payload creates
 *  disabled combinations), which works out to 5–8 Etsy round-trips plus two
 *  function cold starts — and the rate limiter can legitimately sleep for
 *  seconds on a 429. The sequence was lifted from the batch runner, which is
 *  a background function with a 13-minute budget; this restores that budget
 *  (Netlify gives "-background" functions 15 minutes).
 *
 *  The trade: Netlify answers a background invocation with 202 IMMEDIATELY
 *  and discards whatever the handler returns. The browser therefore cannot
 *  read the outcome from this call. Instead, EVERY exit path below funnels
 *  through finish(), which writes the outcome to one Firestore doc keyed by
 *  the browser-generated request_id; the browser polls the tiny synchronous
 *  etsyPricingApplyResult.js until that doc appears. Keying by request_id
 *  means a re-run against the same listing can never be answered by a stale
 *  doc from an earlier run.
 *
 *  The result payload shapes are IDENTICAL to what the synchronous version
 *  returned (ok / skipped / plan_error / not_verified+write_landed / error),
 *  so Index.html's handling branches did not change.
 *
 *  ═══ SEQUENCE ══════════════════════════════════════════════════════════
 *
 *    1. GET  etsyListingInventoryDetailProxy   — current inventory + hash
 *    2.      listingKindFor() picks the planner:
 *              regular / beady -> planStandardRebuild()     (metal x chain length)
 *              charm           -> planCharmListingRebuild() (metal x charm type)
 *              stud            -> planStudRebuild()         (metal only)
 *    3. POST etsyUpdateListingInventoryProxy   — staleness-checked write
 *    4.      require res.verified              — Etsy read-back must match
 *    5.      finish()                          — outcome → Firestore doc
 *
 *  ═══ WHAT IT WILL NOT DO ═══════════════════════════════════════════════
 *
 *  Hoop/Huggie earrings report skipped:true — Shopify has a huggie table but
 *  no Etsy hoop prices exist yet, so there is nothing to write.
 *
 *  An unknown source is NEVER assumed to be a necklace. listingKindFor() in
 *  the shared scheme module returns null and the request skips, because
 *  guessing a sheet is a silent money error rather than a visible failure.
 *
 *  Personalization (the engraving text box) is only attached to necklaces.
 *  A charm listing expresses engraving through its Charm Type dropdown and a
 *  stud listing has none, so forcing a REQUIRED field on either would block
 *  checkout for every buyer.
 */

"use strict";

// Node 18+ global fetch, matching etsyPricingBatch-background.js.
const { planStandardRebuild, planCharmListingRebuild, planStudRebuild,
        ENGRAVE_INSTRUCTIONS, listingKindFor } = require("./_etsyPricingScheme");
const { writeResult, getDb } = require("./_pricingResultStore");

// DEPLOY_PRIME_URL is the CURRENT deploy's URL; URL is always the production
// site. Preferring DEPLOY_PRIME_URL stops a branch/preview deploy from calling
// the PRODUCTION proxies and writing to live Etsy listings.
const SITE = (process.env.DEPLOY_PRIME_URL || process.env.URL || "").replace(/\/$/, "");
const FN = SITE + "/.netlify/functions";

// Per-call ceilings. Generous now that the budget is 15 minutes — these exist
// so ONE hung proxy call cannot eat the whole budget, not to squeeze under a
// 10-second cap the way the synchronous version had to.
const DETAIL_TIMEOUT_MS = Number(process.env.PRICING_DETAIL_TIMEOUT_MS || 60000);
const UPDATE_TIMEOUT_MS = Number(process.env.PRICING_UPDATE_TIMEOUT_MS || 180000);

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, Accept, access-token, Authorization",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};
const json = (statusCode, body) => ({
  statusCode,
  headers: { "Content-Type": "application/json", "Cache-Control": "no-store", ...CORS },
  body: JSON.stringify(body),
});

async function callFn(path, opts, timeoutMs) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), timeoutMs);
  let r;
  try {
    r = await fetch(FN + path, { ...opts, signal: ac.signal });
  } catch (e) {
    if (e && e.name === "AbortError") {
      const te = new Error("Timed out after " + timeoutMs + "ms calling " + path);
      te.timeout = true;
      throw te;
    }
    throw e;
  } finally {
    clearTimeout(timer);
  }
  const t = await r.text();
  let d;
  try { d = JSON.parse(t); } catch { d = { error: t.slice(0, 300) }; }
  if (!r.ok) { const e = new Error(d.error || ("HTTP " + r.status)); e.data = d; e.status = r.status; throw e; }
  return d;
}

// Best-effort mirror of what the batch runner persists: the pre-write
// inventory, so a generator-priced listing has the SAME one-click rollback
// state in EtsyPricing_Listings that a batch-priced one gets. Guarded on
// !original_saved exactly as the batch is; never allowed to fail the run.
async function saveRollbackState(listingId, res) {
  try {
    const db = getDb();
    if (!db || !res || !res.previous_inventory) return;
    const ref = db.collection("EtsyPricing_Listings").doc(String(listingId));
    const snap = await ref.get();
    const d = snap.exists ? (snap.data() || {}) : {};
    if (d.original_saved) return;
    await ref.set({
      original_inventory: res.previous_inventory,
      original_snapshot_hash: res.previous_snapshot_hash || null,
      original_saved: true,
    }, { merge: true });
  } catch (e) {
    console.warn("[applyOne-background] rollback-state save failed (non-fatal):", e && e.message);
  }
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };
  if (event.httpMethod !== "POST") return json(405, { ok: false, error: "Method not allowed" });

  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { body = {}; }

  const listingId = String(body.listing_id || body.listingId || "").trim();
  const requestId = String(body.request_id || body.requestId || "").trim();

  // Every outcome — including validation failures — goes through here, because
  // the 202 Netlify already sent tells the browser nothing. The return value
  // only matters when the handler is invoked directly (tests, non-background
  // deploys); Netlify discards it for background invocations.
  const finish = async (result) => {
    await writeResult(requestId, listingId, result);
    return json(200, result);
  };

  if (!listingId) return finish({ ok: false, retryable: false, reason: "error", error: "listing_id is required" });
  if (!SITE) {
    return finish({ ok: false, retryable: false, reason: "error",
      error: "Server misconfigured: neither DEPLOY_PRIME_URL nor URL is set, so this function cannot call its own proxies." });
  }

  const accessToken =
    body.access_token ||
    event.headers["access-token"] ||
    (event.headers.authorization || "").replace(/^Bearer\s+/i, "");
  if (!accessToken) return finish({ ok: false, retryable: false, reason: "error", error: "Missing Etsy access token" });

  // Which planner this listing needs, from the SAME resolver everywhere.
  // Unknown source = skip, never a guess.
  const explicit = String(body.chain_type || body.listing_kind || "").toLowerCase();
  let kind = ["beady", "regular", "charm", "stud"].includes(explicit) ? explicit : null;
  if (!kind) kind = listingKindFor(body.queue_id || body.queueId, body.category);
  if (kind === null) {
    return finish({
      ok: false,
      skipped: true,
      reason: "no_pricing_path",
      error: "No pricing path for this listing source (queue_id=" + (body.queue_id || body.queueId || "none") +
             ", category=" + (body.category || "none") + "). Necklaces, standalone Charms and Stud Earrings are " +
             "priced; Hoop/Huggie earrings are not, because no Etsy hoop price table exists yet.",
    });
  }
  const engraving = body.engraving !== false;

  const authHeaders = { "access-token": accessToken, "Content-Type": "application/json" };

  try {
    const detail = await callFn(
      "/etsyListingInventoryDetailProxy?listingId=" + encodeURIComponent(listingId) + "&inventory_only=1",
      { headers: authHeaders },
      DETAIL_TIMEOUT_MS
    );
    const products = detail && detail.inventory && detail.inventory.products;
    if (!products || !products.length) {
      return finish({ ok: false, skipped: true, reason: "no_inventory", error: "Listing has no inventory products to rebuild from." });
    }

    const plan = kind === "charm" ? planCharmListingRebuild(products)
               : kind === "stud"  ? planStudRebuild(products)
               : planStandardRebuild(products, kind, engraving);
    if (plan.error) {
      // NOT flagged `skipped` — Index.html treats `skipped` as "expected, say
      // nothing", and a plan error on a real necklace ("No metal dropdown
      // found", "Beady pricing covers only 14/16/18") is a genuine problem
      // that must be surfaced to the operator.
      return finish({ ok: false, skipped: false, reason: "plan_error", error: plan.error });
    }

    // Personalization is the engraving text box. Necklaces carry "+ Engrave"
    // metal values and need it. A charm listing expresses engraving through its
    // Charm Type dropdown and a stud listing has no engraving at all, so forcing
    // a REQUIRED personalization field on them would block checkout for every
    // buyer. Left untouched on those two.
    const wantsPers = engraving && (kind === "regular" || kind === "beady");
    const pers = wantsPers
      ? { enabled: true, required: true, max_chars: 1000, instructions: ENGRAVE_INSTRUCTIONS }
      : null;

    const res = await callFn("/etsyUpdateListingInventoryProxy", {
      method: "POST",
      headers: authHeaders,
      body: JSON.stringify({
        listing_id: Number(listingId),
        expected_snapshot_hash: detail.snapshot_hash,
        inventory: { products: plan.rows },
        auto_on_property: true,
        personalization: pers,
      }),
    }, UPDATE_TIMEOUT_MS);

    await saveRollbackState(listingId, res);

    if (!res.verified) {
      // The PUT has ALREADY landed on Etsy — only the read-back disagreed.
      // write_landed tells the browser to say "CHECK the listing before
      // re-running", never to retry blindly: the price tier is drawn at random
      // per listing, so a re-run would write DIFFERENT prices.
      return finish({
        ok: false,
        retryable: false,
        write_landed: true,
        reason: "not_verified",
        listing_id: listingId,
        error: (res.verification_error || "Etsy verification did not match after write.") +
               " The write was sent — re-check this listing in the Pricing Console before re-running it.",
      });
    }

    const health = res.fresh && res.fresh.pricing_health;
    return finish({
      ok: true,
      listing_id: listingId,
      chain_type: kind,
      listing_kind: kind,
      engraving: wantsPers,
      variants: plan.rows.length,
      min_price: health ? health.min_price ?? null : null,
      max_price: health ? health.max_price ?? null : null,
      snapshot_hash: (res.fresh && res.fresh.snapshot_hash) || null,
      // personalization reports whether the engraving text box actually got
      // attached — it can fail independently of the inventory write (e.g.
      // SHOP_ID unset) and used to be discarded, reporting a clean success.
      personalization: res.personalization || null,
      previous_snapshot_hash: res.previous_snapshot_hash || null,
    });
  } catch (e) {
    return finish({
      ok: false,
      retryable: false,
      reason: (e && e.timeout) ? "timeout" : "error",
      listing_id: listingId,
      error: (e && e.message) || String(e),
    });
  }
};
