// netlify/functions/openaiImageProxy.js (Listing-Generator-1)
//
// Goals:
// - Proxy OpenAI Images API from Netlify (keep API key server-side)
// - Never assume JSON (handle empty bodies + HTML error pages)
// - Add upstream timeout (so YOU get a clean error instead of a long hang/504)
// - Supports:
//    kind/mode: "generations" -> POST https://api.openai.com/v1/images/generations (JSON)
//    kind/mode: "edits"       -> POST https://api.openai.com/v1/images/edits (multipart)
//
// Browser payload example:
// {
//   kind: "edits",
//   model: "gpt-image-1.5",
//   prompt: "...",
//   n: 1,
//   size: "1024x1024",
//   input_image: "data:image/jpeg;base64,...",
//   mask_image:  "data:image/png;base64,..." // optional
// }

function respond(statusCode, obj) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
      "Access-Control-Allow-Methods": "POST, OPTIONS",
      "Cache-Control": "no-store",
    },
    body: JSON.stringify(obj),
  };
}

function clampInt(n, min, max, fallback) {
  const v = Number(n);
  if (!Number.isFinite(v)) return fallback;
  return Math.max(min, Math.min(max, Math.trunc(v)));
}

function readJsonBody(event) {
  try {
    return JSON.parse(event.body || "{}");
  } catch {
    return null;
  }
}

function pickMode(body) {
  const v = String(body?.kind || body?.mode || "").toLowerCase().trim();
  if (v === "generations" || v === "generation") return "generations";
  if (v === "edits" || v === "edit") return "edits";
  return "edits"; // your default use-case
}

function dataUrlToBuffer(dataUrl) {
  // data:image/png;base64,AAAA...
  const m = /^data:(.+?);base64,(.+)$/.exec(String(dataUrl || ""));
  if (!m) throw new Error("Invalid data URL (expected data:<mime>;base64,...)");
  const mime = m[1];
  const b64 = m[2];
  return { mime, buffer: Buffer.from(b64, "base64") };
}

async function readBodySafe(resp) {
  const text = await resp.text().catch(() => "");
  if (!text) return { text: "", json: null, isHtml: false };
  const isHtml = /^\s*</.test(text) && /<html|<!doctype/i.test(text);
  try {
    return { text, json: JSON.parse(text), isHtml };
  } catch {
    return { text, json: null, isHtml };
  }
}

async function fetchWithTimeout(url, options, timeoutMs) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(t);
  }
}

exports.handler = async (event) => {
  try {
    if (event.httpMethod === "OPTIONS") return respond(200, { ok: true });
    if (event.httpMethod !== "POST") return respond(405, { error: { message: "Method not allowed" } });

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) return respond(500, { error: { message: "Missing OPENAI_API_KEY environment variable" } });

    const body = readJsonBody(event);
    if (!body) return respond(400, { error: { message: "Invalid JSON body" } });

    const mode = pickMode(body);
    const model = String(body.model || process.env.OPENAI_IMAGE_MODEL || "gpt-image-1.5");
    const prompt = String(body.prompt || "");
    const size = String(body.size || "2048x2048");
    const n = clampInt(body.n, 1, 8, 1);

    // If you keep seeing timeouts, reduce size to 512x512 from the client
    const UPSTREAM_TIMEOUT_MS = clampInt(body.timeout_ms, 10_000, 120_000, 95_000);

    const baseUrl = "https://api.openai.com/v1/images";

    if (mode === "generations") {
      const payload = { model, prompt, size, n };

      const upstream = await fetchWithTimeout(
        `${baseUrl}/generations`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${apiKey}`,
          },
          body: JSON.stringify(payload),
        },
        UPSTREAM_TIMEOUT_MS
      ).catch((err) => {
        const isAbort = err?.name === "AbortError";
        throw new Error(isAbort ? `Upstream timeout after ${UPSTREAM_TIMEOUT_MS}ms` : (err?.message || String(err)));
      });

      const { text, json, isHtml } = await readBodySafe(upstream);

      if (!upstream.ok) {
        return respond(upstream.status, {
          error: {
            message:
              json?.error?.message ||
              json?.message ||
              (isHtml ? "Upstream returned HTML error page (likely proxy timeout)." : "") ||
              text ||
              `OpenAI images/generations failed with HTTP ${upstream.status} (empty body)`,
            upstream_status: upstream.status,
          },
        });
      }

      if (!json) {
        return respond(502, {
          error: { message: "OpenAI returned 200 but body was empty or not JSON." },
        });
      }

      return respond(200, json);
    }

    if (mode === "edits") {
      const input_image = body.input_image;
      if (!input_image) {
        return respond(400, { error: { message: 'kind/mode "edits" requires input_image (data URL)' } });
      }

     // Optional: Charm macro data URL to be appended as Image[1]
     const input_charm_image = body.input_charm_image;

      // Guard: huge payloads increase latency + timeouts
      const approxBytes = Math.floor((String(input_image).length * 3) / 4);
      if (approxBytes > 9_000_000) {
        return respond(413, {
          error: {
            message:
              "Input image payload is too large. Please downscale/compress (target < ~3MB file; your UI already converts to JPEG).",
          },
        });
      }

      const { mime, buffer } = dataUrlToBuffer(input_image);

      const form = new FormData();
      form.append("model", model);
      form.append("prompt", prompt);
      form.append("size", size);
      form.append("n", String(n));
      form.append("image", new Blob([buffer], { type: mime }), "input.png");

      // IMPORTANT: order matters — this becomes Image[1] in the model
      if (input_charm_image) {
        const c2 = dataUrlToBuffer(input_charm_image);
        form.append("image", new Blob([c2.buffer], { type: c2.mime }), "charm.png");
      }

      if (body.mask_image) {
        const m2 = dataUrlToBuffer(body.mask_image);
        form.append("mask", new Blob([m2.buffer], { type: m2.mime }), "mask.png");
      }

      const upstream = await fetchWithTimeout(
        `${baseUrl}/edits`,
        {
          method: "POST",
          headers: { Authorization: `Bearer ${apiKey}` },
          body: form,
        },
        UPSTREAM_TIMEOUT_MS
      ).catch((err) => {
        const isAbort = err?.name === "AbortError";
        throw new Error(isAbort ? `Upstream timeout after ${UPSTREAM_TIMEOUT_MS}ms` : (err?.message || String(err)));
      });

      const { text, json, isHtml } = await readBodySafe(upstream);

      if (!upstream.ok) {
        return respond(upstream.status, {
          error: {
            message:
              json?.error?.message ||
              json?.message ||
              (isHtml ? "Upstream returned HTML error page (likely proxy timeout)." : "") ||
              text ||
              `OpenAI images/edits failed with HTTP ${upstream.status} (empty body)`,
            upstream_status: upstream.status,
          },
        });
      }

      if (!json) {
        return respond(502, {
          error: { message: "OpenAI returned 200 but body was empty or not JSON." },
        });
      }

      return respond(200, json);
    }

    return respond(400, { error: { message: `Unknown kind/mode: ${mode}` } });
  } catch (err) {
    return respond(500, {
      error: {
        message: err?.message || String(err),
      },
    });
  }
};