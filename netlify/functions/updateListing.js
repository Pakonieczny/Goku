// netlify/functions/updateListing.js
const { etsyFetch } = require("./etsyRateLimiter");

exports.handler = async function (event) { 
const corsHeaders = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, access-token",
    "Access-Control-Allow-Methods": "GET,POST,PUT,PATCH,OPTIONS",
  };

  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: corsHeaders, body: "" };
  }

  try {
    const listingId =
      (event.queryStringParameters && event.queryStringParameters.listingId) || null;

    // Token can come via query (?token=...), or headers
    let token =
      (event.queryStringParameters && event.queryStringParameters.token) ||
      event.headers['access-token'] ||
      event.headers['Access-Token'] ||
      event.headers['authorization'] ||
      event.headers['Authorization'] ||
      null;
    if (typeof token === "string") token = token.replace(/^Bearer\s+/i, "").trim();

    const clientId =
      process.env.CLIENT_ID ||
      process.env.ETSY_CLIENT_ID ||
      process.env.ETSY_API_KEY ||
      process.env.API_KEY;
    const clientSecret =
      process.env.CLIENT_SECRET ||
      process.env.ETSY_CLIENT_SECRET ||
      process.env.ETSY_SHARED_SECRET;
    const shopId = process.env.SHOP_ID;

    if (!listingId) {
      return { statusCode: 400, headers: corsHeaders, body: JSON.stringify({ error: "Missing listingId parameter" }) };
    }
    if (!token) {
      return { statusCode: 400, headers: corsHeaders, body: JSON.stringify({ error: "Missing access token" }) };
    }
    if (!shopId) {
      return { statusCode: 500, headers: corsHeaders, body: JSON.stringify({ error: "SHOP_ID environment variable is not set." }) };
    }
    if (!clientId) {
      console.error("Missing Etsy app key env var for x-api-key header.");
      console.log("Env presence:", {
        CLIENT_ID: !!process.env.CLIENT_ID,
        ETSY_CLIENT_ID: !!process.env.ETSY_CLIENT_ID,
        ETSY_API_KEY: !!process.env.ETSY_API_KEY,
        API_KEY: !!process.env.API_KEY,
      });
      return {
        statusCode: 500,
        headers: corsHeaders,
        body: JSON.stringify({
          error: "Missing Etsy app key env var for x-api-key header.",
          checked: ["CLIENT_ID", "ETSY_CLIENT_ID", "ETSY_API_KEY", "API_KEY"],
        }),
      };
    }

   if (!clientSecret) {
      console.error("Missing Etsy shared secret env var for x-api-key header.");
      console.log("Env presence:", {
        CLIENT_SECRET: !!process.env.CLIENT_SECRET,
        ETSY_CLIENT_SECRET: !!process.env.ETSY_CLIENT_SECRET,
        ETSY_SHARED_SECRET: !!process.env.ETSY_SHARED_SECRET,
      });
      return {
        statusCode: 500,
        headers: corsHeaders,
        body: JSON.stringify({
          error: "Missing Etsy shared secret env var for x-api-key header.",
          checked: ["CLIENT_SECRET", "ETSY_CLIENT_SECRET", "ETSY_SHARED_SECRET"],
        }),
      };
    }

    const xApiKey = `${String(clientId).trim()}:${String(clientSecret).trim()}`;

    // Parse JSON payload (title/description/tags/etc.)
    let payload = {};
    try {
      payload = event.body ? JSON.parse(event.body) : {};
    } catch (e) {
      console.warn("Invalid JSON body; defaulting to empty object. Error:", e.message);
      payload = {};
    }

    // ── Etsy v3 constraint: updating `tags` (or `materials`) REQUIRES
    // `title` and `description` in the SAME request — even if the listing
    // already has both. A payload that carries tags but an empty/missing
    // title or description is guaranteed to 400 with:
    //   "'title' and 'description' are both required to update 'tags'"
    // Rescue: GET the live listing and backfill its current title/
    // description into the PATCH. Costs one extra GET only on requests
    // that would otherwise fail. If the GET itself fails we proceed
    // unchanged and let Etsy's own error surface (no worse than before).
    const isBlank = (v) => v == null || String(v).trim() === "";
    const touchesTagLike = ("tags" in payload) || ("materials" in payload);
    if (touchesTagLike && (isBlank(payload.title) || isBlank(payload.description))) {
      try {
        const getResp = await etsyFetch(
          `https://api.etsy.com/v3/application/listings/${encodeURIComponent(listingId)}`,
          {
            headers: {
              "Accept"       : "application/json",
              "Authorization": `Bearer ${token}`,   // drafts are owner-visible only
              "x-api-key"    : xApiKey
            }
          }
        );
        const current = await getResp.json().catch(() => ({}));
        if (getResp.ok) {
          if (isBlank(payload.title) && !isBlank(current.title)) {
            payload.title = current.title;
            console.warn(`updateListing: backfilled title from live listing ${listingId} (caller sent none).`);
          }
          if (isBlank(payload.description) && !isBlank(current.description)) {
            payload.description = current.description;
            console.warn(`updateListing: backfilled description from live listing ${listingId} (caller sent none).`);
          }
        } else {
          console.warn(`updateListing: backfill GET failed ${getResp.status}; forwarding payload as-is.`);
        }
      } catch (e) {
        console.warn("updateListing: backfill GET errored; forwarding payload as-is:", e.message);
      }
    }

    // Never forward an EMPTY title/description — Etsy treats an empty
    // string as "not provided" for the tags constraint, and an empty
    // value could only ever blank a required field.
    if (isBlank(payload.title)) delete payload.title;
    if (isBlank(payload.description)) delete payload.description;

    // Build x-www-form-urlencoded body.
    // IMPORTANT: Etsy v3 expects arrays (e.g., tags) as a SINGLE comma-separated string.
    const form = new URLSearchParams();
    for (const [key, value] of Object.entries(payload)) {
      if (value == null) continue;

      if (Array.isArray(value)) {
        // Filter null/undefined, stringify, join with commas
        const csv = value
          .filter(v => v != null && String(v).trim() !== "")
          .map(v => String(v))
          .join(',');
        form.append(key, csv);
      } else if (typeof value === 'object') {
        // If you ever send structured fields, serialize safely
        form.append(key, JSON.stringify(value));
      } else {
        form.append(key, String(value));
      }
    }

    const updateUrl = `https://api.etsy.com/v3/application/shops/${shopId}/listings/${encodeURIComponent(listingId)}`;

    const response = await etsyFetch(updateUrl, {
      method: "PATCH",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
        "Accept": "application/json",
        "Authorization": `Bearer ${token}`,
        "x-api-key": xApiKey
      },
      body: form.toString()
    });

    const text = await response.text();
    let data;
    try {
      data = JSON.parse(text);
    } catch {
      data = { raw: text };
    }

    if (!response.ok) {
      const details = JSON.stringify(data);
      const hint = /required to update 'tags'|required to update 'materials'/i.test(details)
        ? "The listing on Etsy is missing a usable title or description, so the automatic backfill could not satisfy Etsy's tags constraint. Set a title and description on the listing (or include them in this request) and retry."
        : undefined;
      return {
        statusCode: response.status,
        headers: corsHeaders,
        body: JSON.stringify(hint
          ? { error: "Error updating listing", details: data, hint }
          : { error: "Error updating listing", details: data })
      };
    }

    return {
      statusCode: 200,
      headers: corsHeaders,
      body: JSON.stringify(data)
    };
  } catch (error) {
    return { statusCode: 500, headers: corsHeaders, body: JSON.stringify({ error: error.message }) };
  }
};
