// netlify/functions/uploadFile.js
// Uploads API (robust):
// 1) POST /v1/uploads                → create Upload
// 2) POST /v1/uploads/{id}/parts     → add binary Part(s) (<=64MB each)
// 3) POST /v1/uploads/{id}/complete  → returns a File object
//
// Tweaks vs prior version:
// - Send binary for each Part (field: "data", Content-Type: application/octet-stream)
//   → avoids proxy 400s seen with huge base64 text fields.
// - Fallback: if a Part returns 400/415, retry once with base64 field.
// - Smaller default chunk size (5 MiB) for friendlier proxies.
// - Returns upstream x-request-id(s) for each step.

const fetch = require("node-fetch");
const FormData = require("form-data");

// <= 64 MiB per OpenAI part; we choose smaller to be proxy-friendly.
const MAX_PART_BYTES = 5 * 1024 * 1024; // 5 MiB

exports.handler = async function (event) {
  try {
    if (!event.body || event.body.trim() === "") {
      return json(400, { error: "No request body provided" });
    }

    let payload;
    try {
      payload = JSON.parse(event.body);
    } catch {
      return json(400, { error: "Invalid JSON body" });
    }

    const { file, fileName, purpose } = payload || {};
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) return json(500, { error: "Missing OPENAI_API_KEY environment variable" });

    if (!file || !String(file).trim()) {
      return json(400, { error: "No file content provided in request body" });
    }

    // Support data URLs and raw base64
    const base64 = String(file).includes(",") ? String(file).split(",").pop() : String(file);
    const buffer = Buffer.from(base64, "base64");
    if (!buffer.length) return json(400, { error: "Decoded file is empty" });

    // Netlify JSON body practical ceiling ~10MB; fail fast.
    if (buffer.length > Math.floor(9.5 * 1024 * 1024)) {
      return json(413, {
        error: `File too large for Netlify proxy (~10MB body). Size=${(buffer.length / 1024 / 1024).toFixed(2)} MB`,
      });
    }

    const resolvedName = fileName || "upload.csv";
    const resolvedPurpose = purpose || "assistants";
    const contentType = detectMime(resolvedName);

    // ---------- 1) CREATE UPLOAD ----------
    const commonHeaders = { Authorization: `Bearer ${apiKey}` };
    if (process.env.OPENAI_ORG_ID)    commonHeaders["OpenAI-Organization"] = process.env.OPENAI_ORG_ID;
    if (process.env.OPENAI_PROJECT_ID) commonHeaders["OpenAI-Project"]      = process.env.OPENAI_PROJECT_ID;

    const createRes = await fetch("https://api.openai.com/v1/uploads", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...commonHeaders },
      body: JSON.stringify({
        purpose: resolvedPurpose,
        filename: resolvedName,
        bytes: buffer.length,
        mime_type: contentType,
      }),
    });
    const createReqId = createRes.headers.get("x-request-id") || null;
    const createText = await createRes.text();
    let createJson; try { createJson = JSON.parse(createText); } catch {}
    if (!createRes.ok) return passthrough(createRes.status, createJson || createText, createReqId);

    const uploadId = createJson?.id;
    if (!uploadId) {
      return json(502, { error: "Upload created without an id", request_id: createReqId, raw: createJson || createText });
    }

    // ---------- 2) ADD PARTS ----------
    const partIds = [];
    for (let offset = 0, idx = 0; offset < buffer.length; offset += MAX_PART_BYTES, idx++) {
      const end = Math.min(offset + MAX_PART_BYTES, buffer.length);
      const chunk = buffer.subarray(offset, end);

      // Primary attempt: send BINARY as a file field
      let partRes = await postPartBinary(uploadId, chunk, idx, commonHeaders);

      // Fallback once on 400/415: try base64 text field
      if (partRes.status === 400 || partRes.status === 415) {
        partRes = await postPartBase64(uploadId, chunk, idx, commonHeaders);
      }

      const partReqId = partRes.headers.get("x-request-id") || null;
      const partText = await partRes.text();
      let partJson; try { partJson = JSON.parse(partText); } catch {}

      if (!partRes.ok || !partJson?.id) {
        return passthrough(partRes.status, partJson || partText, partReqId, {
          upload_id: uploadId,
          failed_offset: offset,
        });
      }

      partIds.push({ id: partJson.id, reqId: partReqId });
    }

    // ---------- 3) COMPLETE UPLOAD ----------
    const completeRes = await fetch(`https://api.openai.com/v1/uploads/${encodeURIComponent(uploadId)}/complete`, {
      method: "POST",
      headers: { "Content-Type": "application/json", ...commonHeaders },
      body: JSON.stringify({ part_ids: partIds.map(p => p.id) }),
    });
    const completeReqId = completeRes.headers.get("x-request-id") || null;
    const completeText = await completeRes.text();
    let completeJson; try { completeJson = JSON.parse(completeText); } catch {}

    if (!completeRes.ok) {
      return passthrough(completeRes.status, completeJson || completeText, completeReqId, {
        upload_id: uploadId,
        part_ids: partIds.map(p => p.id),
      });
    }

    const fileObject = completeJson?.file || null;

    return json(200, {
      ok: true,
      file: fileObject,
      upload: {
        id: uploadId,
        filename: resolvedName,
        purpose: resolvedPurpose,
        bytes: buffer.length,
        mime_type: contentType,
        status: completeJson?.status,
      },
      request_ids: {
        create: createReqId,
        parts: partIds.map(p => p.reqId).filter(Boolean),
        complete: completeReqId,
      },
    });
  } catch (err) {
    console.error("Exception in uploadFile function:", err);
    return json(500, { error: err.message || String(err) });
  }
};

// ----- helpers -----
function json(statusCode, body) {
  return { statusCode, body: JSON.stringify(body) };
}

function passthrough(statusCode, upstreamBody, requestId, extra = {}) {
  if (typeof upstreamBody === "object" && upstreamBody !== null) {
    return json(statusCode, { ...upstreamBody, request_id: requestId, ...extra });
  }
  return json(statusCode, { error: upstreamBody, request_id: requestId, ...extra });
}

function detectMime(name) {
  const lower = String(name || "").toLowerCase();
  if (lower.endsWith(".csv")) return "text/csv";
  if (lower.endsWith(".txt")) return "text/plain";
  if (lower.endsWith(".pdf")) return "application/pdf";
  if (lower.endsWith(".docx")) return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
  if (lower.endsWith(".doc")) return "application/msword";
  if (lower.endsWith(".jsonl")) return "application/jsonl";
  return "application/octet-stream";
}

// Sends a Part as BINARY file content
async function postPartBinary(uploadId, chunk, idx, headers) {
  const form = new FormData();
  // name MUST be "data"; attach binary with a filename and octet-stream
  form.append("data", chunk, {
    filename: `part-${String(idx).padStart(5, "0")}.bin`,
    contentType: "application/octet-stream",
  });
  return fetch(`https://api.openai.com/v1/uploads/${encodeURIComponent(uploadId)}/parts`, {
    method: "POST",
    headers: { ...headers, ...form.getHeaders() },
    body: form,
  });
}

// Fallback: send base64 as a text field named "data"
async function postPartBase64(uploadId, chunk, idx, headers) {
  const form = new FormData();
  form.append("data", chunk.toString("base64"));
  return fetch(`https://api.openai.com/v1/uploads/${encodeURIComponent(uploadId)}/parts`, {
    method: "POST",
    headers: { ...headers, ...form.getHeaders() },
    body: form,
  });
}