/* netlify/functions/_etsyMailTrackingRender.js
 *
 * Shared SVG → PNG timeline renderer for carrier tracking results.
 *
 * Input: the normalized tracking result from _etsyMailCarriers (usps.js or
 * chitchats.js). Output: both an SVG string and a PNG buffer.
 *
 * Visual design — intentionally close to the USPS tracking page that users
 * already recognize, but rendered with our OWN code (no copyrighted assets,
 * no carrier branding reproduced). Shown as a vertical timeline:
 *
 *   ┌──────────────────────────────────────────────────────────┐
 *   │ Tracking: 4206 5248 8986 ...                             │
 *   │ ◉  Expected Delivery                                     │
 *   │ │   MONDAY, April 27 by 9:00 PM                          │
 *   │                                                          │
 *   │ ●  IN TRANSIT                                            │
 *   │ │    ● Arrived at USPS Regional Origin Facility          │
 *   │ │       Northwest Rochester NY Distribution Center       │
 *   │ │       April 23, 2026 · 8:02 pm                         │
 *   │                                                          │
 *   │      ● Accepted at USPS Origin Facility                  │
 *   │      │  Niagara Falls, NY 14304                          │
 *   │      │  April 23, 2026 · 6:47 pm                         │
 *   │ ...                                                      │
 *   │                                                          │
 *   │                     via USPS tracking                    │
 *   └──────────────────────────────────────────────────────────┘
 *
 * Width: 800px. Height: computed from number of events (min 400, max 2400).
 * Exports PNG at 2× scale for retina sharpness.
 *
 * Sharp (already in package.json as "sharp": "^0.33.5") handles the SVG→PNG
 * rasterization.
 */

const fs   = require("fs");
const path = require("path");

// @resvg/resvg-js is lazy-loaded in the render() function to avoid a
// module-load failure if the package isn't installed yet. We cache the
// module reference + font buffers here after first use.
let _resvgModule     = null;
let _fontBuffer      = null;
let _fontBufferBold  = null;

// ─── Design tokens ──────────────────────────────────────────────────────
// Compact layout: 800px wide, ~60px per event row, minimal padding.
// Target is to fit 10-15 events on screen without scrolling, scannable
// at a glance.
const WIDTH          = 800;
const PADDING_X      = 32;
const ROW_HEIGHT     = 46;     // compact row height — title + meta on two lines
const ROW_HEIGHT_FIRST = 54;   // newest event gets slightly more room (bolder)
const HEADER_HEIGHT  = 78;     // tightened from 140
const FOOTER_HEIGHT  = 34;     // tightened from 50
const MAX_EVENTS     = 15;     // cap — 15 events at ~46px = ~690px

// Palette — muted navy + soft accents, works for both USPS and Chit Chats
const COLOR_BG         = "#ffffff";
const COLOR_BG_ACCENT  = "#f8fafc";
const COLOR_TEXT_DARK  = "#0f172a";
const COLOR_TEXT       = "#1e293b";
const COLOR_TEXT_MUTED = "#64748b";
const COLOR_PRIMARY    = "#1e40af";
const COLOR_TIMELINE   = "#cbd5e1";
const COLOR_BORDER     = "#e2e8f0";
const COLOR_SUCCESS    = "#16a34a";
const COLOR_WARNING    = "#f59e0b";
const COLOR_ERROR      = "#dc2626";

const statusColor = {
  delivered       : COLOR_SUCCESS,
  out_for_delivery: COLOR_PRIMARY,
  in_transit      : COLOR_PRIMARY,
  pre_shipment    : COLOR_TEXT_MUTED,
  exception       : COLOR_ERROR,
  returned        : COLOR_ERROR,
  rerouted        : COLOR_WARNING
};

// ─── XML escaping ───────────────────────────────────────────────────────
function esc(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

// ─── Date formatting ────────────────────────────────────────────────────
//
// Every formatter below pins toLocaleString() to DISPLAY_TZ. Without an
// explicit timeZone option, toLocaleString falls back to the Node runtime's
// environment timezone — which on Netlify/AWS Lambda is unstable across
// deploys and produced rendered times that drifted hours from the actual
// carrier wallclock time (e.g. ChitChats reporting an event at 7:51 AM
// Eastern but the rendered PNG showing 2:51 PM). Hard-pinning the display
// zone makes the output deterministic regardless of where the function
// runs.
//
// Default: America/Toronto (matches the shop's operating zone and the
// ChitChats/USPS source-of-truth scan times). Override per-deploy with
// the ETSYMAIL_DISPLAY_TZ env var if a different zone is ever needed.
const DISPLAY_TZ = process.env.ETSYMAIL_DISPLAY_TZ || "America/Toronto";

function fmtDate(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return String(iso);
  return d.toLocaleDateString("en-US", {
    weekday : "long",
    month   : "long",
    day     : "numeric",
    year    : "numeric",
    timeZone: DISPLAY_TZ
  });
}

function fmtDateShort(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return String(iso);
  return d.toLocaleDateString("en-US", {
    month   : "long",
    day     : "numeric",
    year    : "numeric",
    timeZone: DISPLAY_TZ
  });
}

function fmtTime(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return "";
  return d.toLocaleTimeString("en-US", {
    hour    : "numeric",
    minute  : "2-digit",
    hour12  : true,
    timeZone: DISPLAY_TZ
  }).toLowerCase();
}

function fmtDateTime(iso) {
  const d = fmtDateShort(iso);
  const t = fmtTime(iso);
  if (d && t) return `${d} · ${t}`;
  return d || t || "";
}

/** Datetime for event rows: "April 24 at 7:02 AM" */
function fmtDateTimeCompact(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return String(iso);
  const date = d.toLocaleDateString("en-US", {
    month   : "long",
    day     : "numeric",
    timeZone: DISPLAY_TZ
  });
  const time = d.toLocaleTimeString("en-US", {
    hour    : "numeric",
    minute  : "2-digit",
    hour12  : true,
    timeZone: DISPLAY_TZ
  });
  return `${date} at ${time}`;
}

// ─── Tracking code formatting ────────────────────────────────────────────
function formatTrackingCode(code) {
  // USPS IMpb numbers are long (~22-34 digits). Group them for readability,
  // but use a single space between groups of 4 (not the wider kerning we
  // had before). Keep short carrier codes unmodified.
  const s = String(code || "").replace(/\s+/g, "");
  if (s.length <= 10) return s;
  return s.replace(/(.{4})(?=.)/g, "$1 ").trim();
}

// ─── Expected-delivery display ───────────────────────────────────────────
function formatExpectedDelivery(iso) {
  if (!iso) return null;
  const d = new Date(iso);
  if (isNaN(d.getTime())) return null;

  const weekday = d.toLocaleDateString("en-US", {
    weekday : "long",
    timeZone: DISPLAY_TZ
  }).toUpperCase();
  const monthDay = d.toLocaleDateString("en-US", {
    month   : "long",
    day     : "numeric",
    timeZone: DISPLAY_TZ
  });
  const time = d.toLocaleTimeString("en-US", {
    hour    : "numeric",
    hour12  : true,
    timeZone: DISPLAY_TZ
  });

  return {
    weekday,
    monthDay,
    time: time.includes("12 AM") ? null : time
  };
}

// ─── Text measurement (rough, for wrapping) ──────────────────────────────
function estimateWidth(text, fontSize) {
  // Heuristic: avg character width ≈ 0.55 × fontSize for sans-serif
  return Math.ceil(String(text).length * fontSize * 0.55);
}

function wrapText(text, maxWidth, fontSize) {
  const words = String(text || "").split(/\s+/);
  const lines = [];
  let current = "";
  for (const word of words) {
    const candidate = current ? `${current} ${word}` : word;
    if (estimateWidth(candidate, fontSize) > maxWidth && current) {
      lines.push(current);
      current = word;
    } else {
      current = candidate;
    }
  }
  if (current) lines.push(current);
  return lines;
}

// ─── SVG building blocks ────────────────────────────────────────────────
function buildHeader(tracking) {
  const code = formatTrackingCode(tracking.trackingCode);
  const statusCol = statusColor[tracking.statusKey] || COLOR_PRIMARY;
  const ed = formatExpectedDelivery(tracking.estimatedDelivery);

  const elements = [];

  // LINE 1: tracking code (left) + status pill (right)
  elements.push(`
    <text x="${PADDING_X}" y="32" font-family="Open Sans,Helvetica,Arial,sans-serif"
          font-size="17" font-weight="700" fill="${COLOR_TEXT_DARK}">
      ${esc(code)}
    </text>
  `);

  // Status pill — right-aligned
  const statusLabel = tracking.status.toUpperCase();
  const pillWidth = Math.min(200, estimateWidth(statusLabel, 11) + 24);
  const pillX = WIDTH - PADDING_X - pillWidth;
  elements.push(`
    <rect x="${pillX}" y="14" width="${pillWidth}" height="26" rx="13" fill="${statusCol}"/>
    <text x="${pillX + pillWidth / 2}" y="31" font-family="Open Sans,Helvetica,Arial,sans-serif"
          font-size="11" font-weight="700" fill="#ffffff" text-anchor="middle"
          letter-spacing="0.6">
      ${esc(statusLabel)}
    </text>
  `);

  // LINE 2: carrier + ETA/destination subtitle
  const carrierBit = tracking.carrierDisplay || "";
  let subtitleParts = [carrierBit];
  if (ed) {
    const timeSuffix = ed.time ? ` by ${ed.time}` : "";
    subtitleParts.push(`Expected ${ed.monthDay}${timeSuffix}`);
  } else if (tracking.destination) {
    subtitleParts.push(`to ${tracking.destination}`);
  }
  const subtitle = subtitleParts.filter(Boolean).join(" · ");

  elements.push(`
    <text x="${PADDING_X}" y="56" font-family="Open Sans,Helvetica,Arial,sans-serif"
          font-size="13" fill="${COLOR_TEXT_MUTED}">
      ${esc(subtitle)}
    </text>
  `);

  // Thin separator below header
  elements.push(`
    <line x1="${PADDING_X}" y1="${HEADER_HEIGHT - 8}" x2="${WIDTH - PADDING_X}" y2="${HEADER_HEIGHT - 8}"
          stroke="${COLOR_BORDER}" stroke-width="1"/>
  `);

  return {
    svg: elements.join("\n"),
    height: HEADER_HEIGHT
  };
}

/**
 * Compact event row layout:
 *
 *   ○ Arrived at USPS Regional Origin Facility
 *   │  NIAGARA FALLS NY · Apr 23, 8:02 pm
 *
 * Two lines per event, ~46px total. The newest event (isFirst) gets bold
 * title + larger ring. All others use a muted dot.
 */
function buildEvent(event, x, y, isFirst, isLast, availableWidth) {
  const dotX = x + 12;
  const textX = x + 36;
  const rowH = isFirst ? ROW_HEIGHT_FIRST : ROW_HEIGHT;
  const elements = [];

  // Timeline connector (runs full row height for all but the last)
  if (!isLast) {
    elements.push(`
      <line x1="${dotX}" y1="${y}" x2="${dotX}" y2="${y + rowH}"
            stroke="${COLOR_TIMELINE}" stroke-width="2"/>
    `);
  }

  // Timeline dot
  const dotCY = y + 16;
  if (isFirst) {
    // Bold ring for newest event
    elements.push(`
      <circle cx="${dotX}" cy="${dotCY}" r="7"
              fill="#ffffff" stroke="${COLOR_PRIMARY}" stroke-width="3"/>
    `);
  } else {
    elements.push(`
      <circle cx="${dotX}" cy="${dotCY}" r="4" fill="${COLOR_TIMELINE}"/>
    `);
  }

  // LINE 1: event title (truncated if too long to fit on one line)
  const titleMaxChars = Math.floor((availableWidth - 36) / estimateWidth("a", 14) * 1.05);
  let title = event.title || "Scan";
  if (title.length > titleMaxChars) {
    title = title.slice(0, titleMaxChars - 1).trim() + "…";
  }

  elements.push(`
    <text x="${textX}" y="${y + 20}" font-family="Open Sans,Helvetica,Arial,sans-serif"
          font-size="${isFirst ? "15" : "14"}"
          font-weight="${isFirst ? "700" : "600"}"
          fill="${COLOR_TEXT_DARK}">
      ${esc(title)}
    </text>
  `);

  // LINE 2: location · datetime (muted, smaller)
  const dateTime = fmtDateTimeCompact(event.at);
  const metaParts = [];
  if (event.location) metaParts.push(event.location);
  if (dateTime)       metaParts.push(dateTime);
  const metaText = metaParts.join(" · ");

  if (metaText) {
    elements.push(`
      <text x="${textX}" y="${y + 38}" font-family="Open Sans,Helvetica,Arial,sans-serif"
            font-size="12" fill="${COLOR_TEXT_MUTED}">
        ${esc(metaText)}
      </text>
    `);
  }

  return {
    svg: elements.join("\n"),
    height: rowH
  };
}

function buildFooter(tracking, y) {
  const n = (tracking.events || []).length;
  const capped = n > MAX_EVENTS;
  const shown = Math.min(n, MAX_EVENTS);
  let footerText;
  if (capped) {
    footerText = `Showing ${shown} of ${n} events · via ${tracking.carrierDisplay}`;
  } else {
    footerText = `${shown} ${shown === 1 ? "event" : "events"} · via ${tracking.carrierDisplay}`;
  }

  const elements = [];

  // Footer text (centered, muted)
  elements.push(`
    <text x="${WIDTH / 2}" y="${y + 18}" font-family="Open Sans,Helvetica,Arial,sans-serif"
          font-size="11" font-weight="500" fill="${COLOR_TEXT_MUTED}"
          text-anchor="middle" letter-spacing="0.3">
      ${esc(footerText)}
    </text>
  `);

  return elements.join("\n");
}

// ─── Main SVG assembly ───────────────────────────────────────────────────
function buildSvg(tracking) {
  const events = (tracking.events || []).slice(0, MAX_EVENTS);

  const header = buildHeader(tracking);
  let currentY = header.height + 6;

  // Build each event (newest first) — tightly packed
  const eventSvgs = [];
  const availableWidth = WIDTH - PADDING_X * 2 - 12;
  for (let i = 0; i < events.length; i++) {
    const e = buildEvent(
      events[i],
      PADDING_X,
      currentY,
      i === 0,
      i === events.length - 1,
      availableWidth
    );
    eventSvgs.push(e.svg);
    currentY += e.height;
  }

  // Empty state if no events
  if (events.length === 0) {
    eventSvgs.push(`
      <text x="${WIDTH / 2}" y="${currentY + 32}"
            font-family="Open Sans,Helvetica,Arial,sans-serif" font-size="14" fill="${COLOR_TEXT_MUTED}"
            text-anchor="middle">
        No tracking events yet
      </text>
    `);
    currentY += 70;
  }

  currentY += 4;
  const footer = buildFooter(tracking, currentY);
  currentY += FOOTER_HEIGHT;

  const totalHeight = Math.max(200, Math.min(currentY + 8, 1400));

  const svg = `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${WIDTH} ${totalHeight}"
     width="${WIDTH}" height="${totalHeight}">
  <defs>
    <linearGradient id="headerGrad" x1="0%" y1="0%" x2="0%" y2="100%">
      <stop offset="0%" stop-color="${COLOR_BG_ACCENT}" stop-opacity="1"/>
      <stop offset="100%" stop-color="${COLOR_BG}" stop-opacity="1"/>
    </linearGradient>
  </defs>

  <!-- Background -->
  <rect x="0" y="0" width="${WIDTH}" height="${totalHeight}" fill="${COLOR_BG}"/>

  <!-- Header region background -->
  <rect x="0" y="0" width="${WIDTH}" height="${HEADER_HEIGHT}" fill="url(#headerGrad)"/>

  <!-- Header content -->
  ${header.svg}

  <!-- Events -->
  ${eventSvgs.join("\n")}

  <!-- Footer -->
  ${footer}
</svg>`;

  return { svg, width: WIDTH, height: totalHeight };
}

/**
 * Render a tracking result to both SVG (string) and PNG (Buffer).
 *
 * Rendering uses @resvg/resvg-js instead of sharp because:
 *   - sharp's SVG engine (librsvg) relies on system fonts, which aren't
 *     available on Netlify's Amazon Linux function runtime → text renders
 *     as missing-glyph boxes (□□□).
 *   - resvg-js is a Rust SVG renderer (via napi-rs) with per-architecture
 *     native binaries pre-compiled. It supports loading custom font files
 *     from a Buffer, so we bundle an Open Sans TTF with the function and
 *     pass it at render time. No system fonts needed.
 *
 *   Performance: ~50-100ms to render a typical tracking SVG → PNG at 2×.
 *
 * @param {object} tracking  Normalized tracking result
 * @returns {Promise<{svg: string, png: Buffer, width: number, height: number}>}
 */
async function render(tracking) {
  const { svg, width, height } = buildSvg(tracking);

  // Step 1: load the resvg module
  if (!_resvgModule) {
    try {
      _resvgModule = require("@resvg/resvg-js");
      console.log("[tracking-render] @resvg/resvg-js loaded");
    } catch (e) {
      throw new Error(
        `Failed to load @resvg/resvg-js — is it installed? ` +
        `Run 'npm install' and verify package.json + netlify.toml externals. ` +
        `Original: ${e.message}`
      );
    }
  }

  // Step 2: verify font files exist
  const fontPath = path.join(__dirname, "fonts", "OpenSans-Regular.ttf");
  const fontBoldPath = path.join(__dirname, "fonts", "OpenSans-Bold.ttf");
  if (!_fontBuffer) {
    try {
      _fontBuffer = fs.readFileSync(fontPath);
      console.log(`[tracking-render] Loaded OpenSans-Regular.ttf (${_fontBuffer.length} bytes)`);
    } catch (e) {
      throw new Error(
        `Could not load Open Sans font from ${fontPath}. ` +
        `Download it from https://fonts.google.com/specimen/Open+Sans and place at ` +
        `netlify/functions/fonts/OpenSans-Regular.ttf. ` +
        `Make sure netlify.toml includes: included_files = ["netlify/functions/fonts/**"]. ` +
        `Original: ${e.message}`
      );
    }
    try {
      _fontBufferBold = fs.readFileSync(fontBoldPath);
      console.log(`[tracking-render] Loaded OpenSans-Bold.ttf (${_fontBufferBold.length} bytes)`);
    } catch {
      console.warn("[tracking-render] OpenSans-Bold.ttf not found — bold text will use Regular");
    }
  }

  // Step 3: render SVG → PNG
  // Per resvg-js GitHub README and official examples, fontFiles (paths) is
  // the primary/most-reliable approach. fontBuffers has known issues with
  // font-name matching. Using file paths delegates loading to resvg's
  // internal fontdb which handles name table parsing correctly.
  const fontFiles = [fontPath];
  if (_fontBufferBold) fontFiles.push(fontBoldPath);

  // IMPORTANT — per resvg-js docs:
  //   Since resvg 0.28.0, `defaultFontFamily` option is NOT honored when
  //   fonts are loaded via fontBuffers. Instead, resvg reads the font's
  //   INTERNAL name (from the TTF's name table) and uses the first one as
  //   the default automatically.
  //
  //   This means:
  //     1. We must NOT set defaultFontFamily — resvg sets it from the buffer
  //     2. We must NOT have font-family attributes in the SVG that don't
  //        match the internal name. If we specify font-family="Open Sans"
  //        but the TTF's internal name is actually "OpenSans" or "Open Sans
  //        Regular", resvg fails to match and renders nothing.
  //
  //   Safest: strip all font-family attributes from the SVG entirely.
  //   resvg will use the auto-detected default (from the loaded TTF) for
  //   every text element. This works regardless of the font's internal name.
  //
  //   We also strip font-weight if we only loaded one weight, since
  //   requesting weight="700" against a Regular-only font will also fail
  //   to match and render nothing.
  // Embed both fonts as base64 data URIs inside the SVG via @font-face.
  // This bypasses resvg's font-matching entirely — the font is literally
  // IN the SVG as bytes, so there's nothing to match against name tables
  // or fontdb. resvg just uses the embedded font directly.
  const regularB64 = _fontBuffer.toString("base64");
  const boldB64    = _fontBufferBold ? _fontBufferBold.toString("base64") : null;

  const fontFaceBlock = [
    `@font-face {
       font-family: "Embedded";
       font-weight: 400;
       src: url("data:font/ttf;base64,${regularB64}") format("truetype");
     }`,
    boldB64 ? `@font-face {
       font-family: "Embedded";
       font-weight: 700;
       src: url("data:font/ttf;base64,${boldB64}") format("truetype");
     }` : ""
  ].filter(Boolean).join("\n");

  // Inject the @font-face block into the SVG's <defs>, and rewrite every
  // font-family attribute to use our embedded family name.
  let injectedSvg = svg.replace(
    /<defs>/,
    `<defs><style type="text/css"><![CDATA[\n${fontFaceBlock}\n]]></style>`
  );
  injectedSvg = injectedSvg.replace(
    /font-family\s*=\s*"[^"]*"/g,
    'font-family="Embedded"'
  );

  console.log(`[tracking-render] SVG length: ${injectedSvg.length} chars (with embedded fonts)`);

  let png, pngWidth, pngHeight;
  try {
    const resvg = new _resvgModule.Resvg(injectedSvg, {
      fitTo: { mode: "width", value: width * 2 },
      font: {
        // Still pass the fontFiles as a fallback — if resvg chokes on the
        // @font-face CSS for any reason, it'll have the TTFs on disk too
        fontFiles,
        loadSystemFonts: false,
        defaultFontFamily: "Embedded"
      },
      background: "rgba(255, 255, 255, 1)"
    });

    const pngData = resvg.render();
    png = Buffer.from(pngData.asPng());
    pngWidth = pngData.width;
    pngHeight = pngData.height;
    console.log(`[tracking-render] Rendered PNG ${pngWidth}×${pngHeight} (${png.length} bytes)`);
  } catch (e) {
    throw new Error(`resvg-js render failed: ${e.message}`);
  }

  return { svg, png, width: pngWidth, height: pngHeight };
}

module.exports = { render, buildSvg };
