/*  netlify/functions/firestoreProxy.js
 *
 *  Generic Firestore read/write proxy, so browser pages can use Firestore
 *  without loading the Firebase client SDK from the gstatic CDN.
 *
 *  This is NOT a universal Firestore client — it's a small, explicit surface
 *  designed for the EtsyMail operator inbox. It exposes:
 *
 *    GET  ?op=list&coll=<collection>&where=<field>,==,<value>&orderBy=<field>,desc&limit=N
 *    GET  ?op=get&coll=<collection>&id=<docId>
 *    GET  ?op=listSub&coll=<collection>&id=<docId>&sub=<subcollection>&orderBy=...&limit=N
 *    GET  ?op=counts&coll=<collection>&groupBy=<field>
 *    POST body:{ op:'set',  coll, id, data, merge? }
 *    POST body:{ op:'add',  coll, data }
 *    POST body:{ op:'addSub', coll, id, sub, data }
 *    POST body:{ op:'update', coll, id, data }
 *
 *  Collection allowlist prevents random Firestore access. Extend CALLABLE_COLLS
 *  when new collections come online.
 *
 *  Timestamps: the browser can pass { __serverTimestamp: true } and the proxy
 *  substitutes admin.firestore.FieldValue.serverTimestamp().
 */

const admin = require("./firebaseAdmin");
const { requireExtensionAuth } = require("./_etsyMailAuth");
const { requireOwner, requireAnyRole, requireSession, logUnauthorized } = require("./_etsyMailRoles");
const db    = admin.firestore();
const FV    = admin.firestore.FieldValue;

const CORS = {
  "Access-Control-Allow-Origin" : "*",
  // v1.5: include X-EtsyMail-Secret in allowed headers since the proxy
  // now requires it on every op. The inbox UI forwards it from
  // localStorage on every api() call.
  "Access-Control-Allow-Headers": "Content-Type,Authorization,X-EtsyMail-Secret",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
};

// Only these collections + their subcollections can be read/written through this proxy.
// Extend as new mail-system collections come online.
const CALLABLE_COLLS = new Set([
  "EtsyMail_Threads",
  "EtsyMail_Customers",
  "EtsyMail_Orders",
  "EtsyMail_Drafts",
  "EtsyMail_Audit",
  "EtsyMail_Jobs",
  "EtsyMail_Config",
  "EtsyMail_TrackingCache",          // M4 tracking-image cache (keyed by tracking code)
  "EtsyMail_TrackingJobs",            // M4 tracking-image async job status (keyed by jobId)
  // ─── v2.0 Step 1 ─────────────────────────────────────────────────────
  "EtsyMail_Listings",                // Etsy listings catalog mirror
  "EtsyMail_ListingsSync",            // catalog sync state (id: "global")
  "EtsyMail_IntentClassifications",   // per-thread intent cache
  // ─── v2.0 Step 2 ────────────────────────────────────────────────────
  "EtsyMail_SalesContext",            // per-thread sales-funnel state
  "EtsyMail_SalesPrompts",            // operator-tunable per-stage prompts
  "EtsyMail_Operators",               // role assignments (owner | operator)
  // ─── v2.0 Step 2.5 ──────────────────────────────────────────────────
  "EtsyMail_Collateral",              // owner-curated reference URLs
  // ─── v2.1 Option-sheet pricing (replaces v2.0 band model) ───────────
  "EtsyMail_OptionSheets",            // line-sheet docs per product family
  // ─── v2.2 Etsy shipping upgrades cache ──────────────────────────────
  "EtsyMail_ShippingUpgradesCache",    // synced from Etsy every 6h
  // ─── Diagnostic log for quota investigation ─────────────────────────
  "EtsyMail_DiagnosticLog",            // one doc per etsyMailSync-background / mirror-cron invocation
  // ─── Receipts mirror (May 2026) ─────────────────────────────────────
  // EtsyMail_Receipts is the local mirror of Etsy receipts, populated by
  // etsyMailReceiptsMirrorCron every 3 min. Read access from the dashboard
  // is useful for diagnostics + future analytics. Write access is owner-
  // gated since these docs are sourced from Etsy and should not be hand-
  // edited.
  "EtsyMail_Receipts"
  // ─── v2.0 Step 3 will add ───────────────────────────────────────────
  // "EtsyMail_CustomOrders",
  // "EtsyMail_CustomOrderTemplates"
]);
const CALLABLE_SUBS = new Set([
  "messages",
  // v4.3.5+ — round-history archive. Each round-2+ reset writes one
  // doc into EtsyMail_Threads/{threadId}/salesHistory describing the
  // prior completed sale. The dashboard's tabbed Sales conversation
  // card reads from this subcollection to render archived rounds
  // alongside the live SalesContext. Read-only from the dashboard
  // side; only autoPipeline writes here, server-side.
  "salesHistory"
]);

const OWNER_WRITE_COLLS = new Set([
  "EtsyMail_Operators",
  "EtsyMail_Config",
  "EtsyMail_OptionSheets",
  "EtsyMail_Collateral",
  "EtsyMail_SalesPrompts",
  "EtsyMail_ShippingUpgradesCache"
]);

// Operator-write collections: any registered role (owner or operator) may
// write through the proxy, but anonymous extension-secret-only callers
// cannot. This closes the gap where a leaked secret could freely mutate
// thread state or draft payloads without any identity on the audit trail.
//
// EtsyMail_Threads — operator may mirror a sales-stage advance; all other
//   thread-state writes come through etsyMailThreads (role-checked there).
// EtsyMail_Drafts  — operator may persist composer attachment lists; the
//   full draft lifecycle is managed by etsyMailDraftReply / etsyMailDraftSend.
const OPERATOR_WRITE_COLLS = new Set([
  "EtsyMail_SalesContext",
  "EtsyMail_Threads",
  "EtsyMail_Drafts"
]);

function json(statusCode, body) {
  return { statusCode, headers: CORS, body: JSON.stringify(body) };
}
function bad(msg, code = 400) { return json(code, { error: msg }); }
function ok(body) { return json(200, { success: true, ...body }); }

function assertColl(coll) {
  if (!CALLABLE_COLLS.has(coll)) throw new Error(`Collection '${coll}' not in allowlist`);
}
function assertSub(sub) {
  if (!CALLABLE_SUBS.has(sub)) throw new Error(`Subcollection '${sub}' not in allowlist`);
}

async function requireProxyWriteRole(coll, op, actor, event, payload = {}) {
  // v3.27 — Session-first actor resolution. The inbox passes a
  // client-supplied `actor` (derived from localStorage's employee
  // name), but that field is missing or stale in two real-world
  // cases: (1) the operator's session is logged out and the inbox
  // falls back to the literal string "operator" which isn't a
  // registered role, and (2) the operator's local employee_name
  // doesn't match their EtsyMail_Operators doc id. Both produce
  // "Registered operator role required" 403s.
  //
  // If the request carries a valid X-EtsyMail-Session header, we
  // derive the actor from the server-side session doc instead of
  // trusting whatever the client sent. Falls back to client-supplied
  // actor when no session is present — preserves the extension flow
  // which uses X-EtsyMail-Secret without sessions.
  //
  // Failure modes: if session resolution throws or returns ok:false
  // we still proceed with the client-supplied actor and let the
  // normal role check decide. We never UPGRADE a session — if the
  // session is invalid, the role check below sees whatever the
  // client claimed and likely 403s anyway. We never DOWNGRADE either;
  // the session check is purely a more-trustworthy source of actor.
  let effectiveActor = actor;
  if (event && event.headers && (event.headers["x-etsymail-session"] || event.headers["X-EtsyMail-Session"])) {
    try {
      const sess = await requireSession(event);
      if (sess.ok && sess.username) {
        effectiveActor = sess.username;
      }
    } catch (e) {
      console.warn("[firestoreProxy] session resolution failed (continuing with client actor):", e.message);
    }
  }

  if (OWNER_WRITE_COLLS.has(coll)) {
    const owner = await requireOwner(effectiveActor);
    if (!owner.ok) {
      await logUnauthorized({
        actor: effectiveActor,
        eventType: "firestore_proxy_write_unauthorized",
        payload: { coll, op, reason: owner.reason, clientActor: actor, ...payload }
      });
      return { ok: false, statusCode: 403, error: "Owner role required", reason: owner.reason };
    }
  } else if (OPERATOR_WRITE_COLLS.has(coll)) {
    const operator = await requireAnyRole(effectiveActor);
    if (!operator.ok) {
      await logUnauthorized({
        actor: effectiveActor,
        eventType: "firestore_proxy_write_unauthorized",
        payload: { coll, op, reason: operator.reason, clientActor: actor, ...payload }
      });
      return { ok: false, statusCode: 403, error: "Registered operator role required", reason: operator.reason };
    }
  }
  return { ok: true, effectiveActor };
}

/* Recursively substitute { __serverTimestamp:true } with server timestamps
 * and { __arrayUnion:[...] } / { __arrayRemove:[...] } with array ops.
 * Keeps raw dates and primitives alone. */
function hydrate(value) {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(hydrate);
  if (value.__serverTimestamp === true) return FV.serverTimestamp();
  if (Array.isArray(value.__arrayUnion))  return FV.arrayUnion(...value.__arrayUnion);
  if (Array.isArray(value.__arrayRemove)) return FV.arrayRemove(...value.__arrayRemove);
  if (typeof value.__increment === "number") return FV.increment(value.__increment);
  const out = {};
  for (const k of Object.keys(value)) out[k] = hydrate(value[k]);
  return out;
}

/* Convert Firestore doc data to JSON-safe form, turning Timestamps into ISO strings
 * with a {_ts: true, ms: <millis>} marker so the client can reformat. */
function serialize(value) {
  if (value === null || typeof value !== "object") return value;
  if (value && typeof value.toDate === "function" && typeof value.toMillis === "function") {
    return { _ts: true, ms: value.toMillis() };
  }
  if (Array.isArray(value)) return value.map(serialize);
  const out = {};
  for (const k of Object.keys(value)) out[k] = serialize(value[k]);
  return out;
}

const STRING_WHERE_FIELDS = new Set([
  // Etsy IDs look numeric but are stored as strings in Firestore. Do not
  // coerce these to Number or diagnostic/admin queries silently miss docs.
  "id",
  "receipt_id",
  "receiptId",
  "etsyOrderId",
  "buyer_user_id",
  "buyerUserId",
  "buyer_user_id_string",
  "customerId",
  "threadId",
  "conversationId",
  "etsyConversationId",
  "orderId"
]);

function parseWhere(raw) {
  // Accept repeated ?where=field,op,value. Booleans/null still coerce.
  // Numeric-looking Etsy IDs stay strings for known ID fields. For other
  // fields, callers can force a string with where=field,==,str:<value>.
  if (!raw) return [];
  const arr = Array.isArray(raw) ? raw : [raw];
  return arr.map(s => {
    const [field, op, ...rest] = String(s).split(",");
    let value = rest.join(",");
    if (value.startsWith("str:")) value = value.slice(4);
    else if (value === "true") value = true;
    else if (value === "false") value = false;
    else if (value === "null") value = null;
    else if (!STRING_WHERE_FIELDS.has(field) && /^-?\d+(\.\d+)?$/.test(value)) value = Number(value);
    return { field, op, value };
  });
}
function parseOrderBy(raw) {
  if (!raw) return [];
  const arr = Array.isArray(raw) ? raw : [raw];
  return arr.map(s => {
    const [field, dir = "asc"] = String(s).split(",");
    return { field, dir: dir === "desc" ? "desc" : "asc" };
  });
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };

  // ── v1.5: gate every op behind the shared secret ────────────────
  // Pre-v1.5, the proxy was unauthenticated. A public Netlify URL with
  // unauthenticated read AND write access to thread/draft/customer/audit
  // collections is a security incident waiting to happen — anyone could
  // wipe EtsyMail_Threads, rewrite searchableText fields with garbage,
  // or read every customer's order history.
  //
  // Now: every GET and POST requires X-EtsyMail-Secret. The inbox UI
  // forwards it from localStorage on every api() call (existing
  // behavior, no UI change needed). Functions calling the proxy
  // server-side forward it from process.env.ETSYMAIL_EXTENSION_SECRET.
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  try {
    const method = event.httpMethod;
    const qs     = event.queryStringParameters || {};
    // Support repeated params (Netlify folds them into multiValueQueryStringParameters)
    const mvqs   = event.multiValueQueryStringParameters || {};

    if (method === "GET") {
      const op = qs.op;
      if (!op) return bad("Missing op");

      /* ─── list ─────────────────────────────── */
      if (op === "list") {
        const coll = qs.coll;
        if (!coll) return bad("Missing coll");
        assertColl(coll);

        let q = db.collection(coll);

        const wheres = parseWhere(mvqs.where || qs.where);
        for (const w of wheres) q = q.where(w.field, w.op, w.value);

        const orders = parseOrderBy(mvqs.orderBy || qs.orderBy);
        for (const o of orders) q = q.orderBy(o.field, o.dir);

        // v5.32 — Delta sync support.
        //
        // When the client passes ?since=<millis>, we add an "updatedAt > since"
        // filter so the response contains only docs that have changed since
        // the cursor. This is the cornerstone of the dashboard's read-budget
        // strategy: instead of re-fetching the whole folder every poll, the
        // client fetches once (full) and then only deltas (typically 0–5
        // docs/minute) thereafter.
        //
        // Index posture:
        //   - On its own, "where(updatedAt,>,since).orderBy(updatedAt,desc)"
        //     uses Firestore's auto-built single-field index for updatedAt.
        //     No composite index required.
        //   - If the caller stacks since on top of additional where clauses
        //     (e.g. where=status,==,X), Firestore WILL require a composite
        //     index (status, updatedAt). That's a deliberate caller choice;
        //     we don't strip the other wheres because doing so would change
        //     semantics. The dashboard's delta path is therefore designed
        //     to send since BY ITSELF (no status where) and filter client-
        //     side. See fetchThreadListNow in etsy-mail-1.html.
        //
        // sinceField defaults to "updatedAt". A caller can override with
        // ?sinceField=otherField if the collection uses a different cursor
        // (e.g. "timestamp" for the messages subcollection — see listSub
        // below).
        if (qs.since != null && qs.since !== "") {
          const sinceMs = Number(qs.since);
          if (Number.isFinite(sinceMs) && sinceMs > 0) {
            const sinceField = String(qs.sinceField || "updatedAt");
            q = q.where(sinceField, ">", admin.firestore.Timestamp.fromMillis(sinceMs));
            // If the caller hasn't specified an orderBy on the cursor field,
            // add one — Firestore requires range filters to be the first
            // orderBy clause.
            const hasOrderOnCursor = orders.some(o => o.field === sinceField);
            if (!hasOrderOnCursor) {
              q = q.orderBy(sinceField, "asc");
            }
          }
        }

        const limit = Math.min(parseInt(qs.limit || "100", 10), 500);
        q = q.limit(limit);

        const snap = await q.get();
        return ok({ docs: snap.docs.map(d => ({ id: d.id, ...serialize(d.data()) })) });
      }

      /* ─── get ─────────────────────────────── */
      if (op === "get") {
        const { coll, id } = qs;
        if (!coll || !id) return bad("Missing coll or id");
        assertColl(coll);
        const snap = await db.collection(coll).doc(String(id)).get();
        if (!snap.exists) return ok({ exists: false, doc: null });
        return ok({ exists: true, doc: { id: snap.id, ...serialize(snap.data()) } });
      }

      /* ─── listSub ──────────────────────────── */
      if (op === "listSub") {
        const { coll, id, sub } = qs;
        if (!coll || !id || !sub) return bad("Missing coll, id, or sub");
        assertColl(coll);
        assertSub(sub);

        let q = db.collection(coll).doc(String(id)).collection(sub);
        const orders = parseOrderBy(mvqs.orderBy || qs.orderBy);
        for (const o of orders) q = q.orderBy(o.field, o.dir);

        // v5.32 — Delta sync support for subcollections. See block comment in
        // the "list" handler above. For the messages subcollection the cursor
        // is "timestamp" (messages are append-only with a timestamp field),
        // so the dashboard sends ?sinceField=timestamp. Default stays
        // "updatedAt" for any caller that doesn't override.
        if (qs.since != null && qs.since !== "") {
          const sinceMs = Number(qs.since);
          if (Number.isFinite(sinceMs) && sinceMs > 0) {
            const sinceField = String(qs.sinceField || "updatedAt");
            q = q.where(sinceField, ">", admin.firestore.Timestamp.fromMillis(sinceMs));
            const hasOrderOnCursor = orders.some(o => o.field === sinceField);
            if (!hasOrderOnCursor) {
              q = q.orderBy(sinceField, "asc");
            }
          }
        }

        const limit = Math.min(parseInt(qs.limit || "500", 10), 2000);
        q = q.limit(limit);

        const snap = await q.get();
        return ok({ docs: snap.docs.map(d => ({ id: d.id, ...serialize(d.data()) })) });
      }

      /* ─── counts ────────────────────────────
       * Minimal group-count: scans up to 2000 docs, groups by one field.
       * Good enough for sidebar badges; swap for count() aggregation later. */
      if (op === "counts") {
        const { coll, groupBy } = qs;
        if (!coll || !groupBy) return bad("Missing coll or groupBy");
        assertColl(coll);
        const snap = await db.collection(coll).select(groupBy).limit(2000).get();
        const counts = {};
        snap.forEach(d => {
          const v = (d.data() || {})[groupBy] || "unknown";
          counts[v] = (counts[v] || 0) + 1;
        });
        return ok({ counts });
      }

      return bad(`Unknown op '${op}'`);
    }

    if (method === "POST") {
      let body = {};
      try { body = JSON.parse(event.body || "{}"); }
      catch { return bad("Invalid JSON body"); }

      const { op } = body;
      if (!op) return bad("Missing op");

      /* ─── set ───────────────────────────────
       * Explicit doc id. With merge:true behaves like patch. */
      if (op === "set") {
        const { coll, id, data, merge = false, actor = null } = body;
        if (!coll || !id || !data) return bad("Missing coll, id, or data");
        assertColl(coll);
        const role = await requireProxyWriteRole(coll, op, actor, event, { id: String(id) });
        if (!role.ok) return json(role.statusCode, { error: role.error, reason: role.reason });
        await db.collection(coll).doc(String(id)).set(hydrate(data), { merge: !!merge });
        return ok({ id: String(id) });
      }

      /* ─── add ─────────────────────────────── */
      if (op === "add") {
        const { coll, data, actor = null } = body;
        if (!coll || !data) return bad("Missing coll or data");
        assertColl(coll);
        const role = await requireProxyWriteRole(coll, op, actor, event);
        if (!role.ok) return json(role.statusCode, { error: role.error, reason: role.reason });
        const ref = await db.collection(coll).add(hydrate(data));
        return ok({ id: ref.id });
      }

      /* ─── addSub ──────────────────────────── */
      if (op === "addSub") {
        const { coll, id, sub, data, actor = null } = body;
        if (!coll || !id || !sub || !data) return bad("Missing coll, id, sub, or data");
        assertColl(coll);
        assertSub(sub);
        const role = await requireProxyWriteRole(coll, op, actor, event, { id: String(id), sub });
        if (!role.ok) return json(role.statusCode, { error: role.error, reason: role.reason });
        const ref = await db.collection(coll).doc(String(id)).collection(sub).add(hydrate(data));
        return ok({ id: ref.id });
      }

      /* ─── updateSub ───────────────────────────
       * Patch a single sub-collection document. Used for one-time
       * timestamp corrections and other targeted edits where deleting
       * and re-adding the doc would be destructive. */
      if (op === "updateSub") {
        const { coll, id, sub, subId, patch, actor = null } = body;
        if (!coll || !id || !sub || !subId || !patch) {
          return bad("Missing coll, id, sub, subId, or patch");
        }
        assertColl(coll);
        assertSub(sub);
        const role = await requireProxyWriteRole(coll, op, actor, event, { id: String(id), sub });
        if (!role.ok) return json(role.statusCode, { error: role.error, reason: role.reason });
        await db.collection(coll).doc(String(id)).collection(sub).doc(String(subId)).update(hydrate(patch));
        return ok({ subId: String(subId) });
      }

      /* ─── update ──────────────────────────── */
      if (op === "update") {
        const { coll, id, data, actor = null } = body;
        if (!coll || !id || !data) return bad("Missing coll, id, or data");
        assertColl(coll);
        const role = await requireProxyWriteRole(coll, op, actor, event, { id: String(id) });
        if (!role.ok) return json(role.statusCode, { error: role.error, reason: role.reason });
        await db.collection(coll).doc(String(id)).update(hydrate(data));
        return ok({ id: String(id) });
      }

      /* ─── deleteSub ─────────────────────────
       * Nuke an ENTIRE subcollection (all docs under
       * coll/id/sub). Used for "clean rescrape" to wipe stale messages
       * from earlier scraper versions. Paginates in chunks of 400 to
       * stay under Firestore's batch limits. */
      if (op === "deleteSub") {
        const { coll, id, sub, confirm, actor = null } = body;
        if (!coll || !id || !sub) return bad("Missing coll, id, or sub");
        if (confirm !== true) return bad("Refusing to deleteSub without { confirm: true } flag");
        assertColl(coll);
        assertSub(sub);
        const role = await requireProxyWriteRole(coll, op, actor, event, { id: String(id), sub });
        if (!role.ok) return json(role.statusCode, { error: role.error, reason: role.reason });

        const subRef = db.collection(coll).doc(String(id)).collection(sub);
        let totalDeleted = 0;
        // Loop: grab 400 docs, batch-delete them, repeat until empty
        while (true) {
          const snap = await subRef.limit(400).get();
          if (snap.empty) break;
          const batch = db.batch();
          snap.docs.forEach(d => batch.delete(d.ref));
          await batch.commit();
          totalDeleted += snap.docs.length;
          if (snap.docs.length < 400) break;  // last page
        }

        // Reset the parent doc's messageCount so the thread shows accurate state
        try {
          await db.collection(coll).doc(String(id)).set({
            messageCount: 0,
            lastInboundAt: null,
            lastOutboundAt: null,
            updatedAt: admin.firestore.FieldValue.serverTimestamp()
          }, { merge: true });
        } catch (e) {
          console.warn("deleteSub: parent doc reset failed:", e.message);
        }

        return ok({ deleted: totalDeleted });
      }

      /* ─── deleteDoc ─────────────────────────
       * Delete a single top-level doc. */
      if (op === "deleteDoc") {
        const { coll, id, confirm, actor = null } = body;
        if (!coll || !id) return bad("Missing coll or id");
        if (confirm !== true) return bad("Refusing to deleteDoc without { confirm: true } flag");
        assertColl(coll);
        const role = await requireProxyWriteRole(coll, op, actor, event, { id: String(id) });
        if (!role.ok) return json(role.statusCode, { error: role.error, reason: role.reason });
        await db.collection(coll).doc(String(id)).delete();
        return ok({ id: String(id), deleted: true });
      }

      /* ─── deleteThread ──────────────────────
       * Owner-only. Completely purge a thread and every piece of data
       * tied to it across Firestore + Firebase Storage, so a subsequent
       * Etsy scrape of the same conversation creates a fresh record
       * with no carry-over state.
       *
       * What gets deleted:
       *   Firestore
       *     EtsyMail_Threads/{threadId} doc + messages subcollection
       *     EtsyMail_Drafts (every doc where threadId == this thread)
       *     EtsyMail_IntentClassifications/{threadId}
       *     EtsyMail_SalesContext/{threadId}
       *   Firebase Storage
       *     etsymail/{threadId}/**          (mirrored inbound images)
       *     etsymail/drafts/{threadId}/**   (composer / draft attachments)
       *
       * What is preserved:
       *   EtsyMail_Customers/{buyerUserId}  — keyed by buyer, shared across
       *     every thread that buyer ever opens. Deleting it would orphan
       *     other threads' customer panels.
       *   EtsyMail_Audit/*                  — audit trail is forensic; we
       *     write a `thread_deleted` row so the deletion itself is recorded.
       *   etsymail/tracking/<code>.png      — tracking snapshots are keyed
       *     by tracking code, not threadId; multiple threads can share one.
       *
       * The operation is best-effort and continues past per-step failures
       * — if Storage cleanup partially fails, the Firestore wipe still
       * completes. Counts of what was deleted are returned so the caller
       * can show an honest report (and the audit row records the same).
       *
       * Owner-only because this is multi-collection destructive — owner
       * is the only role permitted to write to ALL the affected
       * collections, and this is a higher bar than even the existing
       * deleteSub (which checks per-collection role). */
      if (op === "deleteThread") {
        const { threadId, confirm, actor = null } = body;
        if (!threadId || !/^etsy_conv_[a-zA-Z0-9_-]+$/.test(String(threadId))) {
          return bad("threadId must look like 'etsy_conv_<id>'");
        }
        if (confirm !== true) {
          return bad("Refusing to deleteThread without { confirm: true } flag");
        }

        // Owner-only role check (more restrictive than the per-collection
        // role tier — this op crosses owner-write and operator-write
        // collections, so we gate at the higher bar).
        const owner = await requireOwner(actor);
        if (!owner.ok) {
          await logUnauthorized({
            actor,
            eventType: "firestore_proxy_delete_thread_unauthorized",
            payload  : { threadId, reason: owner.reason }
          });
          return json(403, { error: "Owner role required", reason: owner.reason });
        }

        const tid = String(threadId);
        const counts = {
          messages           : 0,
          thread             : 0,
          drafts             : 0,
          intentClassifications: 0,
          salesContext       : 0,
          storageMirrored    : 0,
          storageDrafts      : 0
        };
        const errors = [];   // non-fatal; reported in the response

        // 1. Wipe the messages subcollection. Same paginated batch pattern
        //    as deleteSub above — Firestore's batch limit is 500 ops per
        //    commit, so we chunk at 400 to leave headroom.
        try {
          const subRef = db.collection("EtsyMail_Threads").doc(tid).collection("messages");
          while (true) {
            const snap = await subRef.limit(400).get();
            if (snap.empty) break;
            const batch = db.batch();
            snap.docs.forEach(d => batch.delete(d.ref));
            await batch.commit();
            counts.messages += snap.docs.length;
            if (snap.docs.length < 400) break;
          }
        } catch (e) {
          errors.push(`messages subcollection: ${e.message}`);
        }

        // 2. Drafts. Drafts carry threadId as a field (not the doc id),
        //    so we have to query for them. Single where() on equality is
        //    cheap and doesn't need a composite index.
        try {
          const draftsSnap = await db.collection("EtsyMail_Drafts")
            .where("threadId", "==", tid).get();
          if (!draftsSnap.empty) {
            const batch = db.batch();
            draftsSnap.docs.forEach(d => batch.delete(d.ref));
            await batch.commit();
            counts.drafts = draftsSnap.docs.length;
          }
        } catch (e) {
          errors.push(`drafts: ${e.message}`);
        }

        // 3. Intent classification (one doc keyed by threadId).
        try {
          const ref = db.collection("EtsyMail_IntentClassifications").doc(tid);
          const snap = await ref.get();
          if (snap.exists) {
            await ref.delete();
            counts.intentClassifications = 1;
          }
        } catch (e) {
          errors.push(`intent classification: ${e.message}`);
        }

        // 4. Sales context (one doc keyed by threadId, only present if
        //    the sales agent ever ran on this thread).
        try {
          const ref = db.collection("EtsyMail_SalesContext").doc(tid);
          const snap = await ref.get();
          if (snap.exists) {
            await ref.delete();
            counts.salesContext = 1;
          }
        } catch (e) {
          errors.push(`sales context: ${e.message}`);
        }

        // 5. The thread doc itself. Done LAST so a partial failure above
        //    leaves the thread visible in the UI for retry — better than
        //    half-deleted state with the parent gone but children alive.
        try {
          const ref = db.collection("EtsyMail_Threads").doc(tid);
          const snap = await ref.get();
          if (snap.exists) {
            await ref.delete();
            counts.thread = 1;
          }
        } catch (e) {
          errors.push(`thread doc: ${e.message}`);
        }

        // 6. Firebase Storage prefixes. bucket.deleteFiles({ prefix })
        //    is GCS's recursive delete by path prefix — efficient even
        //    for large message-image trees.
        try {
          const [files] = await admin.storage().bucket().getFiles({ prefix: `etsymail/${tid}/` });
          if (files.length > 0) {
            await admin.storage().bucket().deleteFiles({ prefix: `etsymail/${tid}/` });
            counts.storageMirrored = files.length;
          }
        } catch (e) {
          errors.push(`mirrored images: ${e.message}`);
        }
        try {
          const [files] = await admin.storage().bucket().getFiles({ prefix: `etsymail/drafts/${tid}/` });
          if (files.length > 0) {
            await admin.storage().bucket().deleteFiles({ prefix: `etsymail/drafts/${tid}/` });
            counts.storageDrafts = files.length;
          }
        } catch (e) {
          errors.push(`draft attachments: ${e.message}`);
        }

        // 7. Audit row. We always write this — even on partial failure —
        //    so there's a forensic record of the deletion attempt.
        try {
          await db.collection("EtsyMail_Audit").add({
            threadId  : tid,
            eventType : "thread_deleted",
            actor     : actor || "system",
            payload   : { counts, errors, op: "deleteThread" },
            createdAt : admin.firestore.FieldValue.serverTimestamp(),
            outcome   : errors.length === 0 ? "success" : "partial"
          });
        } catch (e) {
          // Don't fail the request because of audit failure — surface
          // it in the response instead.
          errors.push(`audit write: ${e.message}`);
        }

        return ok({
          threadId: tid,
          counts,
          errors,
          partial : errors.length > 0
        });
      }

      /* ─── reprocessThread ──────────────────
       * Owner-only. Force the auto-pipeline to re-evaluate an existing
       * thread that was already processed once (typical case: pipeline
       * settings — sales-mode, classifier — got flipped ON after the
       * thread was first scraped, and the idempotency lock prevents
       * automatic re-evaluation).
       *
       * Steps, in order:
       *   1. Validate threadId + verify thread doc exists
       *   2. Clear the idempotency lock (`lastAutoProcessedInboundAt`)
       *   3. Delete any cached classification doc — otherwise the next
       *      classifier call returns the stale label
       *   4. Call etsyMailIntentClassifier SYNCHRONOUSLY with the latest
       *      inbound text → capture the verdict for the response
       *   5. Trigger etsyMailAutoPipeline-background asynchronously to
       *      run the full pipeline (draft + sales routing) using the
       *      fresh classification
       *   6. Return a structured diagnostic so the UI can show what
       *      happened at each step — this is the single most useful
       *      thing for catching pipeline problems
       *
       * The synchronous classifier call is the diagnostic core. If it
       * throws, the response surfaces the exact error (e.g. "Anthropic
       * API quota exhausted" or "messageText empty") without forcing
       * the operator to dig through Netlify function logs. */
      if (op === "reprocessThread") {
        const { threadId, actor = null } = body;
        if (!threadId || !/^etsy_conv_[a-zA-Z0-9_-]+$/.test(String(threadId))) {
          return bad("threadId must look like 'etsy_conv_<id>'");
        }
        const owner = await requireOwner(actor);
        if (!owner.ok) {
          await logUnauthorized({
            actor,
            eventType: "firestore_proxy_reprocess_unauthorized",
            payload  : { threadId, reason: owner.reason }
          });
          return json(403, { error: "Owner role required", reason: owner.reason });
        }

        const tid = String(threadId);
        const stages = {};   // populated step-by-step for the response

        // Step 1 — Confirm the thread exists + assemble the recent
        // INBOUND BURST. The classifier is designed for one message but
        // works fine with the customer's last few inbounds concatenated:
        // a single follow-up nudge ("Please confirm?") in isolation
        // reads as `unclear`, but combined with the substantive earlier
        // messages in the same conversation flow it classifies correctly.
        // We fetch up to 50 most-recent docs (no composite index needed),
        // pick the latest 5 inbounds, reverse to chronological, and join
        // with newlines. 4000-char cap matches loadLatestInboundText.
        let latestText = null;
        let inboundCount = 0;
        let burstMessageCount = 0;
        try {
          const tDoc = await db.collection("EtsyMail_Threads").doc(tid).get();
          if (!tDoc.exists) return json(404, { error: "Thread not found", threadId: tid });
          stages.threadStatus = tDoc.data().status || null;

          const msgs = await db.collection("EtsyMail_Threads").doc(tid)
            .collection("messages")
            .orderBy("timestamp", "desc")
            .limit(50)
            .get();
          // Collect inbound texts newest-first, then reverse for the
          // classifier so it reads chronologically (oldest → newest).
          const recentInboundsNewestFirst = [];
          for (const d of msgs.docs) {
            const data = d.data();
            if (data.direction !== "inbound") continue;
            inboundCount++;
            if (recentInboundsNewestFirst.length < 5) {
              const t = String(data.text || "").trim();
              if (t) recentInboundsNewestFirst.push(t);
            }
          }
          if (recentInboundsNewestFirst.length > 0) {
            const chronological = recentInboundsNewestFirst.slice().reverse();
            latestText = chronological.join("\n\n").slice(0, 4000);
            burstMessageCount = chronological.length;
          }
        } catch (e) {
          return json(500, { error: "Could not load thread state: " + e.message });
        }

        if (!latestText) {
          return json(422, {
            error  : "No inbound message text on this thread",
            threadId: tid,
            stages : { ...stages, latestInboundFound: false, inboundCount }
          });
        }
        stages.latestInboundFound = true;
        stages.inboundCount       = inboundCount;
        stages.burstMessageCount  = burstMessageCount;
        stages.burstCharCount     = latestText.length;
        stages.inboundPreview     = latestText.slice(0, 120);

        // Step 2 — Clear the idempotency lock on the thread doc.
        try {
          await db.collection("EtsyMail_Threads").doc(tid).set({
            lastAutoProcessedInboundAt: null,
            updatedAt: admin.firestore.FieldValue.serverTimestamp()
          }, { merge: true });
          stages.idempotencyLockCleared = true;
        } catch (e) {
          stages.idempotencyLockCleared = false;
          stages.idempotencyLockError   = e.message;
        }

        // Step 3 — Delete the classification cache for this thread.
        try {
          const cacheRef = db.collection("EtsyMail_IntentClassifications").doc(tid);
          const snap = await cacheRef.get();
          if (snap.exists) {
            await cacheRef.delete();
            stages.classifierCacheCleared = true;
          } else {
            stages.classifierCacheCleared = "no cache to clear";
          }
        } catch (e) {
          stages.classifierCacheCleared = false;
          stages.classifierCacheError   = e.message;
        }

        // Step 4 — Synchronously call the classifier. Diagnostic core.
        try {
          const baseUrl = process.env.URL
                       || process.env.DEPLOY_URL
                       || "http://localhost:8888";
          const headers = { "Content-Type": "application/json" };
          if (process.env.ETSYMAIL_EXTENSION_SECRET) {
            headers["X-EtsyMail-Secret"] = process.env.ETSYMAIL_EXTENSION_SECRET;
          }
          const res = await fetch(`${baseUrl}/.netlify/functions/etsyMailIntentClassifier`, {
            method: "POST",
            headers,
            body  : JSON.stringify({
              threadId   : tid,
              messageText: latestText,
              actor      : actor || "system:reprocess",
              force      : true
            })
          });
          const text = await res.text();
          let parsed = null;
          try { parsed = JSON.parse(text); } catch { parsed = { raw: text }; }
          stages.classifierStatus = res.status;
          if (res.ok && parsed) {
            stages.classification     = parsed.classification || null;
            stages.classifyConfidence = parsed.confidence     || null;
            stages.classifySignals    = parsed.signals        || null;
          } else {
            stages.classifierError = (parsed && parsed.error) || `HTTP ${res.status}`;
            return json(200, {
              success : false,
              threadId: tid,
              stages,
              hint    : "Classifier returned non-2xx. Check Netlify logs for etsyMailIntentClassifier — likely an Anthropic API key, quota, or model-name issue."
            });
          }
        } catch (e) {
          stages.classifierError = e.message;
          return json(200, {
            success : false,
            threadId: tid,
            stages,
            hint    : "Could not reach the classifier function. Verify it's deployed under /.netlify/functions/etsyMailIntentClassifier."
          });
        }

        // Step 5 — Trigger the auto-pipeline asynchronously.
        try {
          const baseUrl = process.env.URL
                       || process.env.DEPLOY_URL
                       || "http://localhost:8888";
          const headers = { "Content-Type": "application/json" };
          if (process.env.ETSYMAIL_EXTENSION_SECRET) {
            headers["X-EtsyMail-Secret"] = process.env.ETSYMAIL_EXTENSION_SECRET;
          }
          const signal = (typeof AbortSignal !== "undefined" && AbortSignal.timeout)
            ? AbortSignal.timeout(5000) : undefined;
          const res = await fetch(`${baseUrl}/.netlify/functions/etsyMailAutoPipeline-background`, {
            method : "POST",
            headers,
            body   : JSON.stringify({
              threadId    : tid,
              employeeName: actor || "system:reprocess",
              forceRerun  : true
            }),
            signal
          });
          stages.pipelineTriggered     = res.status === 202 || res.ok;
          stages.pipelineTriggerStatus = res.status;
        } catch (e) {
          stages.pipelineTriggered    = false;
          stages.pipelineTriggerError = e.message;
        }

        // Step 6 — Audit row so the reprocess attempt is on the record.
        try {
          await db.collection("EtsyMail_Audit").add({
            threadId  : tid,
            eventType : "thread_reprocessed",
            actor     : actor || "system",
            payload   : { stages, op: "reprocessThread" },
            createdAt : admin.firestore.FieldValue.serverTimestamp()
          });
        } catch (e) {
          stages.auditWriteError = e.message;
        }

        return ok({
          threadId: tid,
          stages,
          summary : `Classified as ${stages.classification || "(failed)"}` +
                    (stages.classifyConfidence ? ` @ ${(stages.classifyConfidence * 100).toFixed(0)}%` : "") +
                    (stages.pipelineTriggered ? " — pipeline running in background, refresh in ~20s" : "")
        });
      }

      return bad(`Unknown op '${op}'`);
    }

    return json(405, { error: "Method Not Allowed" });

  } catch (err) {
    console.error("firestoreProxy error:", err);
    return json(500, { error: err.message || String(err) });
  }
};
