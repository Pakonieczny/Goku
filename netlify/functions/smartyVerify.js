/* netlify/functions/smartyVerify.js */
const fetch = require("node-fetch");

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
};

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  try {
    if (event.httpMethod !== "POST") {
      return { statusCode: 405, headers: CORS, body: "Method Not Allowed" };
    }

    /** Expected input:
     * { to: {
     *    to_name, to_address_1, to_address_2, to_city,
     *    to_province_code, to_postal_code, to_country_code
     * } }
     */
    const { to = {} } = JSON.parse(event.body || "{}") || {};
    const cc = String(to.to_country_code || "").trim().toUpperCase();
    if (!cc) {
      return {
        statusCode: 200,
        headers: CORS,
        body: JSON.stringify({ suggested: null, note: "Empty country" }),
      };
    }

    // Auth: prefer secret key pair, fallback to embedded website key
    const AUTH_ID  = process.env.SMARTY_AUTH_ID   || "";
    const AUTH_TOK = process.env.SMARTY_AUTH_TOKEN|| "";
    const EMB_KEY  = process.env.SMARTY_EMBEDDED_KEY || "";
    if (!(AUTH_ID && AUTH_TOK) && !EMB_KEY) {
      return {
        statusCode: 500,
        headers: CORS,
        body: JSON.stringify({ error: "Smarty keys not configured" }),
      };
    }

    // Small helper for auth params
    const addAuth = (qs) => {
      if (AUTH_ID && AUTH_TOK) {
        qs.set("auth-id", AUTH_ID);
        qs.set("auth-token", AUTH_TOK);
      } else {
        qs.set("key", EMB_KEY);
      }
      return qs;
    };

    let suggested = null;
    let raw = null;

    if (cc === "US") {
      // ===== US STREET =======================================================
      const qs = addAuth(new URLSearchParams());
      const street = [to.to_address_1 || "", to.to_address_2 || ""]
        .filter(Boolean).join(" ");
      if (street) qs.set("street", street);
      if (to.to_city)          qs.set("city", to.to_city);
      if (to.to_province_code) qs.set("state", to.to_province_code);
      if (to.to_postal_code)   qs.set("zipcode", to.to_postal_code);
      qs.set("candidates", "5");
      qs.set("match", "enhanced");

      const url  = `https://us-street.api.smarty.com/street-address?${qs}`;
      const resp = await fetch(url);
      const data = await resp.json();

      if (Array.isArray(data) && data.length) {
        const pick =
          data.find(d => d.analysis?.dpv_match_code === "Y") || data[0];
        raw = pick;

        const c  = pick.components || {};
        const zip = [c.zipcode || "", c.plus4_code ? `-${c.plus4_code}` : ""].join("");

        suggested = {
          to_name          : to.to_name || "",
          to_address_1     : pick.delivery_line_1 ||
                              [c.primary_number, c.street_predirection, c.street_name, c.street_suffix, c.street_postdirection]
                                .filter(Boolean).join(" "),
          to_address_2     : [c.secondary_designator, c.secondary_number].filter(Boolean).join(" ") || "",
          to_city          : c.city_name || "",
          to_province_code : c.state_abbreviation || "",
          to_postal_code   : zip.trim(),
          to_country_code  : "US",
        };
      }
    } else {
      // ===== INTERNATIONAL STREET ===========================================
      // Prefer structured params; fall back to freeform if needed.
      // Smarty can infer province from locality + postal code.
      const qs = addAuth(new URLSearchParams());
      qs.set("country", cc);
      const hasStructured =
        (to.to_address_1 || to.to_address_2) || to.to_city || to.to_postal_code;

      if (hasStructured) {
        if (to.to_address_1) qs.set("address1", to.to_address_1);
        if (to.to_address_2) qs.set("address2", to.to_address_2);
        if (to.to_city) qs.set("locality", to.to_city);
        if (to.to_province_code) qs.set("administrative_area", to.to_province_code);
        if (to.to_postal_code) qs.set("postal_code", to.to_postal_code);
      } else {
        // If caller only sends a big blob, allow a freeform one-line.
        const freeform = [
          to.to_address_1, to.to_address_2, to.to_city, to.to_province_code, to.to_postal_code
        ].filter(Boolean).join(", ");
        if (freeform) qs.set("freeform", freeform);
      }

      // Options
      qs.set("geocode", "false");          // we only need normalization
      qs.set("max_results", "5");

      const url  = `https://international-street.api.smarty.com/verify?${qs}`;
      const resp = await fetch(url);
      const data = await resp.json();

      if (Array.isArray(data) && data.length) {
        const pick = data[0];
        raw = pick;

        const c = pick.components || {};
        const addr1 =
          pick.address1 ||
          [c.primary_number, c.street_predirection, c.thoroughfare_name, c.thoroughfare_trailing_type, c.thoroughfare_postdirection]
            .filter(Boolean).join(" ");
        const addr2 =
          pick.address2 ||
          [c.premise_type, c.premise, c.sub_premise_type, c.sub_premise]
            .filter(Boolean).join(" ").trim();

        suggested = {
          to_name          : to.to_name || "",
          to_address_1     : addr1 || "",
          to_address_2     : addr2 || "",
          to_city          : c.locality || pick.components?.locality || "",
          to_province_code : c.administrative_area || "",  // may be full name or code
          to_postal_code   : c.postal_code || "",
          to_country_code  : (c.country_iso_2 || cc || "").toUpperCase(),
        };
      }
    }

    return {
      statusCode: 200,
      headers: CORS,
      body: JSON.stringify({ suggested, raw }),
    };
  } catch (err) {
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: String(err && err.message || err) }) };
  }
};