// netlify/functions/uploadFile.js
// End-to-end upload via OpenAI Uploads API:
// 1) Create upload (/v1/uploads)
// 2) Add one or more parts (/v1/uploads/{id}/parts)
// 3) Complete upload (/v1/uploads/{id}/complete) -> returns a usable File
//
// Notes:
// - Each Part <= 64 MB; total upload <= ~8 GB (per OpenAI spec).
// - We send base64-encoded chunks in a multipart field named "data" (per API examples).
// - Returns the created File plus helpful request IDs for tracing.
// - Honors optional org/project headers if you set env vars: OPENAI_ORG_ID / OPENAI_PROJECT_ID.

const fetch = require("node-fetch");
const FormData = require("form-data");

// Tune chunk size well below the 64MB per-part cap.
const MAX_PART_BYTES = 20 * 1024 * 1024; // 20 MiB

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

    // Support both raw base64 and data URLs ("data:...;base64,xxxx")
    const base64 = String(file).includes(",") ? String(file).split(",").pop() : String(file);
    const buffer = Buffer.from(base64, "base64");
    if (!buffer.length) return json(400, { error: "Decoded file is empty" });

    const resolvedName = fileName || "upload.csv";
    const resolvedPurpose = purpose || "assistants"; // modern default for retrieval / file-search
    const contentType = detectMime(resolvedName);

    // Netlify Functions request-body practical ceiling ~10MB — fail fast to be kind to users.
    if (buffer.length > Math.floor(9.5 * 1024 * 1024)) {
      return json(413, {
        error: `File too large for Netlify proxy (~10MB body). Size=${(buffer.length / 1024 / 1024).toFixed(2)} MB`,
      });
    }

    // ---------- 1) CREATE UPLOAD ----------
    const createPayload = {
      purpose: resolvedPurpose,
      filename: resolvedName,
      bytes: buffer.length,
      mime_type: contentType,
    };

    const commonHeaders = {
      Authorization: `Bearer ${apiKey}`,
    };

    // Optional project/org scoping if you’ve set them
    if (process.env.OPENAI_ORG_ID) {
      commonHeaders["OpenAI-Organization"] = process.env.OPENAI_ORG_ID;
    }
    if (process.env.OPENAI_PROJECT_ID) {
      commonHeaders["OpenAI-Project"] = process.env.OPENAI_PROJECT_ID;
    }

    const createRes = await fetch("https://api.openai.com/v1/uploads", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...commonHeaders },
      body: JSON.stringify(createPayload),
    });

    const createReqId = createRes.headers.get("x-request-id") || null;
    const createText = await createRes.text();
    let createJson;
    try { createJson = JSON.parse(createText); } catch {}
    if (!createRes.ok) {
      return passthrough(createRes.status, createJson || createText, createReqId);
    }
    const uploadId = createJson?.id;
    if (!uploadId) {
      return json(502, { error: "Upload was created but no upload ID returned", create_request_id: createReqId, raw: createJson || createText });
    }

    // ---------- 2) ADD PARTS ----------
    const partIds = [];
    let offset = 0;
    while (offset < buffer.length) {
      const end = Math.min(offset + MAX_PART_BYTES, buffer.length);
      const chunk = buffer.subarray(offset, end);
      // API examples show sending a base64 string in a multipart field named "data".
      const form = new FormData();
      form.append("data", chunk.toString("base64"));

      const partRes = await fetch(`https://api.openai.com/v1/uploads/${encodeURIComponent(uploadId)}/parts`, {
        method: "POST",
        headers: { ...commonHeaders, ...form.getHeaders() },
        body: form,
      });

      const partReqId = partRes.headers.get("x-request-id") || null;
      const partText = await partRes.text();
      let partJson;
      try { partJson = JSON.parse(partText); } catch {}
      if (!partRes.ok) {
        return passthrough(partRes.status, partJson || partText, partReqId, {
          upload_id: uploadId,
          failed_offset: offset,
        });
      }
      if (!partJson?.id) {
        return json(502, {
          error: "Add-part succeeded without a part ID",
          upload_id: uploadId,
          part_response: partJson || partText,
          request_id: partReqId,
        });
      }
      partIds.push({ id: partJson.id, reqId: partReqId });
      offset = end;
    }

    // ---------- 3) COMPLETE UPLOAD ----------
    const completeRes = await fetch(`https://api.openai.com/v1/uploads/${encodeURIComponent(uploadId)}/complete`, {
      method: "POST",
      headers: { "Content-Type": "application/json", ...commonHeaders },
      body: JSON.stringify({ part_ids: partIds.map(p => p.id) }),
    });

    const completeReqId = completeRes.headers.get("x-request-id") || null;
    const completeText = await completeRes.text();
    let completeJson;
    try { completeJson = JSON.parse(completeText); } catch {}
    if (!completeRes.ok) {
      return passthrough(completeRes.status, completeJson || completeText, completeReqId, {
        upload_id: uploadId,
        part_ids: partIds.map(p => p.id),
      });
    }

    // The completed Upload includes a nested File object that’s now usable across the platform.
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

// ---------- helpers ----------
function json(statusCode, body) {
  return { statusCode, body: JSON.stringify(body) };
}

function passthrough(statusCode, upstreamBody, requestId, extra = {}) {
  // Surface upstream status/body transparently and include x-request-id for support.
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