// /netlify/functions/relinkSet-background.js
//
// Background (15-minute) runner for set re-matching on an existing product.
// The synchronous relinkSet op dies at Netlify's ~26s sync limit on larger
// families (synonyms + searches + embeddings + 12+ zoom crops + chunked
// vision legitimately exceed it). Netlify invokes any function whose name
// ends in "-background" asynchronously: the POST returns 202 immediately
// and this runs to completion, writing its result to Firestore where the
// main function's op:"relinkResult" serves it to the console.
//
// Body: { handle } (bare handle or product URL)
// Result doc: Brites_Relink_Results/{handle} = { status, result?, error?, at }

const { runRelink, normalizeHandle, db, admin } = require("./shopifyListingUpload.js")._relink;

exports.handler = async function (event) {
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch (_) {}
  const handle = normalizeHandle(body.handle);
  if (!handle) {
    console.error("relinkSet-background: missing handle");
    return { statusCode: 400, body: "Missing handle" };
  }
  const ref = db.collection("Brites_Relink_Results").doc(handle);
  await ref.set({ status: "RUNNING", at: admin.firestore.FieldValue.serverTimestamp() }, { merge: false });
  try {
    const result = await runRelink(handle);
    await ref.set({ status: result.ok ? "DONE" : "ERROR", result,
      at: admin.firestore.FieldValue.serverTimestamp() });
    console.log("relinkSet-background:", handle, "→", result.ok ? "DONE" : "ERROR", (result.setLinks && result.setLinks.partners || []).length, "partners");
  } catch (e) {
    await ref.set({ status: "ERROR", error: String((e && e.message) || e),
      at: admin.firestore.FieldValue.serverTimestamp() });
    console.error("relinkSet-background fatal:", e);
  }
  return { statusCode: 200, body: "done" };
};
