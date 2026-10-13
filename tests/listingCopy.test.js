// Offline tests for the listing-copy library embedded in Index.html (script#listingCopyLib).
// No network, no OpenAI/Anthropic/Gemini: every model response here is a stub.
//   node tests/listingCopy.test.js
const fs = require("fs");
const path = require("path");
const vm = require("vm");
const assert = require("assert");

const html = fs.readFileSync(path.join(__dirname, "..", "Index.html"), "utf8");
const m = html.match(/<!-- LISTING_COPY_LIB_START -->\s*<script id="listingCopyLib">([\s\S]*?)<\/script>\s*<!-- LISTING_COPY_LIB_END -->/);
assert(m, "library block not found in Index.html");
const sandbox = { module: { exports: {} }, window: {} };
vm.runInNewContext(m[1], sandbox);
const L = sandbox.module.exports;

let passed = 0;
const t = async (name, fn) => { try { await fn(); passed++; console.log("ok   " + name); } catch (e) { console.log("FAIL " + name + "\n     " + e.message); process.exitCode = 1; } };
const stub = (replies) => { let i = 0; const calls = []; const fn = async (req) => { calls.push(req); const r = replies[Math.min(i++, replies.length - 1)]; return typeof r === "function" ? r(req) : r; }; fn.calls = calls; return fn; };

(async () => {
  await t("model is gpt-6.1-sol with reasoning effort high", () => {
    assert.strictEqual(L.MODEL.model, "gpt-6.1-sol");
    assert.strictEqual(L.MODEL.effort, "high");
  });

  await t("Index.html: copy calls use the copy model, not window.modelName, and send no unverified params", () => {
    const call = html.match(/async function copyCallText\(req\) \{[\s\S]*?\n    \}/)[0];
    assert(/model: ListingCopy\.MODEL\.model/.test(call));
    assert(/reasoning: \{ effort: ListingCopy\.MODEL\.effort \}/.test(call));
    assert(!/verbosity/.test(call));
    for (const fn of ["generateTitleOnly", "generateDescriptionOnly", "getInitialPhrases", "regenerateSinglePhrase", "generateShopifyFields"]) {
      const i = html.indexOf("async function " + fn);
      const body = html.slice(i, i + 3500);
      assert(!/window\.modelName/.test(body.split(/\n    (?:async )?function /)[0] || ""), fn + " still uses window.modelName");
    }
  });

  await t("every inline <script> in Index.html still parses", () => {
    const re = /<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/g;
    let x, n = 0;
    while ((x = re.exec(html))) { if (x[1].trim()) { new vm.Script(x[1]); n++; } }
    assert(n > 5);
  });

  await t("'Beady' is read as a CHAIN fact, never as part of the design", () => {
    const f = L.buildFacts({ listingDesc: "Christmas Ornament Charm Beady Necklace" });
    assert.strictEqual(f.motif, "Christmas Ornament");
    assert.strictEqual(f.category, "Beady_Necklace");
    assert.strictEqual(f.chain.beaded, true);
    const r = L.buildFacts({ listingDesc: "Westie Terrier Charm Regular Necklace" });
    assert.strictEqual(r.chain.beaded, false);
    assert.strictEqual(L.buildFacts({ listingDesc: "Sunflower charm" }).kind, "charm");
    assert(!L.buildFacts({ listingDesc: "Pine Tree Charm Beady Necklace" }).metals.some(x => /Rose/.test(x)));
  });

  await t("prompts never carry the raw 'Beady Necklace' string, and state the flat-charm fact", () => {
    const f = L.buildFacts({ listingDesc: "Christmas Ornament Charm Beady Necklace" });
    for (const req of [L.titleRequest({ facts: f, rules: "" }), L.descriptionRequest({ facts: f, rules: "" }), L.tagsRequest({ facts: f, rules: "" }), L.shopifyRequest({ facts: f })]) {
      assert(!/Charm Beady Necklace|Beady Necklace/i.test(req.user), "raw category string leaked into prompt");
      assert(/FLAT metal charm/.test(req.user));
    }
    const reg = L.titleRequest({ facts: L.buildFacts({ listingDesc: "X Charm Regular Necklace" }), rules: "" });
    assert(/Do NOT say beaded/.test(reg.user));
  });

  await t("bead rule: charm never beaded; chain may be, only on beady category", () => {
    const beady = L.buildFacts({ category: "Beady_Necklace", listingDesc: "Pine Tree" });
    const reg = L.buildFacts({ category: "Regular_Necklace", listingDesc: "Pine Tree" });
    for (const bad of ["pine bead charm", "beaded pine charm", "festive bead", "bead accents", "pearl accent", "beaded necklace", "beady necklace"]) {
      assert(L.findBeadMisuse(bad, beady), "should flag: " + bad);
      assert(L.findBeadMisuse(bad, reg), "should flag: " + bad);
    }
    assert.strictEqual(L.findBeadMisuse("flat pine tree charm on a beaded chain", beady), null);
    assert.strictEqual(L.findBeadMisuse("flat pine tree charm on a ball chain", reg), null);
    assert(L.findBeadMisuse("flat pine tree charm on a beaded chain", reg), "beaded chain is false on a regular necklace");
  });

  await t("beady chain wording is allowed for Beady necklaces (title, tags, Shopify tags), not for others", () => {
    const b = L.buildFacts({ listingDesc: "Pine Tree Charm Beady Necklace" });
    const r = L.buildFacts({ listingDesc: "Pine Tree Charm Regular Necklace" });
    assert.strictEqual(L.validateEtsyTitle("Pine Tree Charm Necklace on Beady Chain, Sterling Silver, Gold Filled, Flat Pendant", b).length, 0);
    assert.strictEqual(L.validateEtsyTitle("Pine Tree Beaded Chain Necklace, Sterling Silver, Flat Charm", b).length, 0);
    assert(L.validateEtsyTitle("Pine Tree Charm Necklace on Beady Chain, Sterling Silver, Flat Pendant", r).length > 0);
    const tg = "beady chain necklace\nbeaded chain\nball chain necklace\npine bead charm\nbeady pine pendant";
    assert.strictEqual(JSON.stringify(L.cleanTags(tg, b).tags), JSON.stringify(["beady chain necklace", "beaded chain", "ball chain necklace"]));
    assert.strictEqual(L.cleanTags(tg, r).tags.length, 1);
    assert(/MAY and should use "beady chain"/.test(L.factsBlock(b)));
    assert(/encouraged where natural: beady chain,/.test(L.tagsRequest({ facts: b, rules: "" }).user));
  });

  await t("Paul's junk keywords are rejected, good ones kept", () => {
    const f = L.buildFacts({ listingDesc: "Christmas Ornament Charm Beady Necklace" });
    const r = L.cleanTags("beaded necklace\npine bead\nfestive bead\nsnow brooch\nseasonal strand\nsanta locket\nchristmas ornament\nornament charm\nholiday charm\nxmas tree ornament charm pendant\nsterling silver\nchristmas ornament\n14k gold", f);
    assert.strictEqual(JSON.stringify(r.tags), JSON.stringify(["christmas ornament", "ornament charm", "holiday charm"]));
    const reasons = Object.fromEntries(r.rejected.map(x => [x.tag, x.reason]));
    assert.strictEqual(reasons["pine bead"], "bead/pearl word");
    assert.strictEqual(reasons["snow brooch"], "product we do not sell");
    assert.strictEqual(reasons["seasonal strand"], "product we do not sell");
    assert.strictEqual(reasons["santa locket"], "product we do not sell");
    assert.strictEqual(reasons["xmas tree ornament charm pendant"], "over 20 characters");
    assert.strictEqual(reasons["sterling silver"], "repeats a material attribute");
  });

  const f = L.buildFacts({ listingDesc: "Christmas Ornament Charm Beady Necklace" });
  const OLD = "Christmas Ornament Necklace: Solid Gold 10K, 14K, Handmade Holiday Charm Gift";

  await t("title validator catches each rule", () => {
    const bad = {
      "Beaded Christmas Ornament Charm Necklace, 14K Solid Gold": /beaded/,
      "Christmas Ornament Charm Necklace, Beautiful 14K Solid Gold Gift": /subjective/,
      "Christmas Ornament Charm Necklace, 14K Solid Gold Filled, Flat Pendant": /material/,
      "Christmas Ornament Charm Necklace, 14K Solid Gold, FLAT PENDANT, Ball Chain": /ALL-CAPS/,
      "Christmas Ornament Charm Necklace, Flat Pendant, Flat Ball Chain, Sterling Silver": /repeats the word "flat"/,
      "Christmas Ornament Charm Necklace, Sterling Silver, Free Shipping, Flat Pendant": /price\/sale/,
      "Christmas Ornament Charm Necklace 🎄 Sterling Silver, Flat Pendant, Ball Chain": /symbols/,
      "Sterling Silver, 14K Solid Gold, Flat Pendant, Dainty Ball Chain, Christmas Ornament Charm Necklace": /first 70/,
      "Snowman Charm Necklace, Sterling Silver, Gold Filled, 10K or 14K Solid Gold, Flat Pendant, Ball Chain": /design/,
      "Christmas Ornament Charm Necklace, 18K Gold, Flat Pendant, Ball Chain, Sterling Silver Handmade": /material/,
      "Christmas Ornament Charm Necklace, Hypoallergenic Sterling Silver, Flat Pendant, Ball Chain": /unprovable/,
      ["Christmas Ornament Charm Necklace, " + "Sterling Silver, ".repeat(10) + "Flat"]: /140/,
      "Christmas Ornament Locket Necklace, Sterling Silver, Flat Pendant, Ball Chain": /do not sell/
    };
    for (const [title, re] of Object.entries(bad)) {
      const probs = L.validateEtsyTitle(title, f).join(" | ");
      assert(re.test(probs), `expected ${re} for "${title.slice(0, 60)}" got: ${probs}`);
    }
    assert.strictEqual(L.validateEtsyTitle("Christmas Ornament Charm Necklace, 14K Solid Gold, Flat Dainty Holiday Pendant, Ball Chain", f).length, 0);
    assert.strictEqual(L.validateEtsyTitle("Flat Christmas Ornament Charm Necklace on Beaded Chain, Sterling Silver, Gold Filled", f).length, 0);
  });

  await t("repairTitle: cosmetic fixes only", () => {
    assert.strictEqual(L.repairTitle('  "Pine Tree Charm Necklace , SILVER ,"\n'), "Pine Tree Charm Necklace, Silver");
    assert(L.repairTitle("Pine Tree Charm Necklace, " + "Sterling Silver, ".repeat(20)).length <= 140);
  });

  await t("runTitle: bad first answer is rejected with feedback, good second is used", async () => {
    const call = stub(["Beaded Christmas Ornament Necklace, Pine Bead Charm, Perfect Gift", "Christmas Ornament Charm Necklace, Sterling Silver or 14K Gold Filled, Flat Holiday Pendant, Ball Chain"]);
    const r = await L.runTitle({ callText: call, facts: f, rules: "", recent: [] });
    assert.strictEqual(r.fallback, false);
    assert.strictEqual(r.attempts, 2);
    assert(/REJECTED/.test(call.calls[1].user) && /beaded/i.test(call.calls[1].user));
    assert(!/bead/i.test(r.title));
  });

  await t("runTitle: model that keeps failing gets the plain deterministic title, never banned words", async () => {
    const r = await L.runTitle({ callText: stub(["Beaded Ornament Necklace Bead Charm"]), facts: f, rules: "", recent: [] });
    assert.strictEqual(r.fallback, true);
    assert.strictEqual(L.validateEtsyTitle(r.title, f).length, 0, L.validateEtsyTitle(r.title, f).join("|"));
    assert(r.title.startsWith("Christmas Ornament Charm Necklace"));
  });

  await t("diversity guard: replica of a recent title is retried with the recent titles shown", async () => {
    const recentTitle = "Pine Tree Charm Necklace, Sterling Silver, Gold Filled, 10K or 14K Solid Gold, Flat Pendant, Ball Chain";
    const call = stub([
      "Christmas Ornament Charm Necklace, Sterling Silver, Gold Filled, 10K or 14K Solid Gold, Flat Pendant, Ball Chain",
      "14K Solid Gold Christmas Ornament Necklace Charm, Flat Round Bauble, Also in Sterling Silver"
    ]);
    const r = await L.runTitle({ callText: call, facts: f, rules: "", recent: [{ title: recentTitle, motif: "Pine Tree" }] });
    assert.strictEqual(r.attempts, 2);
    assert(/near-duplicate/.test(call.calls[1].user));
    assert(call.calls[0].user.includes(recentTitle), "recent title should be shown to the model up front");
    assert.notStrictEqual(r.title, "Christmas Ornament Charm Necklace, Sterling Silver, Gold Filled, 10K or 14K Solid Gold, Flat Pendant, Ball Chain");
  });

  await t("diversity guard: a template-y stub over 6 different pieces yields 6 non-duplicate titles", async () => {
    const motifs = ["Westie Terrier", "Sunflower", "Hummingbird", "Pine Tree", "Sea Turtle", "Crescent Moon"];
    const recent = [];
    const out = [];
    for (const motif of motifs) {
      const facts = L.buildFacts({ listingDesc: `${motif} Charm Beady Necklace` });
      // A lazy "model": first answer always the same template, second answer a different structure.
      const lazy = `${motif} Charm Necklace, Sterling Silver, Gold Filled, 10K or 14K Solid Gold, Flat Pendant, Ball Chain`;
      const varied = [`${motif} Necklace Charm in 14K Solid Gold, Flat Cutout`, `Flat ${motif} Charm Necklace, Gold Filled or Sterling Silver`, `10K Solid Gold ${motif} Pendant Necklace, Flat Charm`, `${motif} Charm Necklace on Ball Chain, Dainty Flat Shape, Solid Gold`, `Sterling Silver ${motif} Pendant, Flat Charm for Everyday Wear, Ball Chain`, `${motif} Necklace for Layering, Gold Filled or 14K Solid Gold, Flat Charm`][out.length];
      const r = await L.runTitle({ callText: stub([lazy, varied, varied]), facts, rules: "", recent });
      recent.unshift({ title: r.title, motif });
      out.push(r.title);
    }
    for (let i = 0; i < out.length; i++) for (let j = i + 1; j < out.length; j++) {
      const s = L.mostSimilar(out[i], motifs[i], [{ title: out[j], motif: motifs[j] }]).score;
      assert(s < L.SIMILARITY_LIMIT, `titles ${i}/${j} too similar (${s}): ${out[i]} // ${out[j]}`);
    }
  });

  await t("runDescription: beaded wording retried; last-attempt sentence repair; fallback is bead-free", async () => {
    const good = "A flat Christmas ornament charm cut from sheet metal, with a rounded cap and a clean, polished face. It hangs from a beaded ball chain that sits close at the collarbone, so it layers easily with other necklaces. A thoughtful choice for anyone who keeps a little holiday spirit all year round, and back engraving is available.";
    const bad = "This Christmas ornament charm has festive bead accents and a pine bead. " + good;
    const call = stub([bad, good]);
    const r = await L.runDescription({ callText: call, facts: f, rules: "", title: "x" });
    assert.strictEqual(r.attempts, 2); assert(!r.fallback);
    assert(!/pine bead|festive bead|bead accents/i.test(r.text));
    // repair path
    const r2 = await L.runDescription({ callText: stub([bad]), facts: f, rules: "", title: "x" });
    assert(r2.repaired, "should sentence-repair on last attempt");
    assert(!/festive bead|pine bead/i.test(r2.text));
    // fallback path
    const r3 = await L.runDescription({ callText: stub(["Beaded beads beads. Pearl bead."]), facts: f, rules: "", title: "x" });
    assert(r3.fallback);
    assert(L.validateDescription(r3.text, f, "").filter(p => !/only \d+ chars/.test(p)).length === 0, L.validateDescription(r3.text, f, "").join("|"));
    assert(!L.findBeadMisuse(r3.text, f));
  });

  await t("runTags: rejected tags are topped up in extra rounds; result has 13 valid tags", async () => {
    const r1 = "christmas ornament\nornament charm\nbeaded necklace\npine bead\nsnow brooch\nholiday decor\nxmas decoration charm necklace\nflat bauble pendant\ntree trimming charm\nsanta locket\nwinter holiday\nround ornament\nseasonal jewelry";
    const r2 = "xmas jewelry\nchristmas gift idea\nbauble style\ntree hanger look\nfestive pendant";
    const call = stub([r1, r2]);
    const r = await L.runTags({ callText: call, facts: f, rules: "" });
    assert.strictEqual(r.tags.length, 13);
    assert(r.tags.every(x => x.length <= 20 && !/bead|brooch|locket/.test(x)));
    assert.strictEqual(new Set(r.tags.map(x => x.toLowerCase())).size, 13);
    assert(/Rejected, avoid/.test(call.calls[1].user));
  });

  await t("runShopify: valid JSON passes; lengths trimmed; bead copy replaced; bad JSON falls back", async () => {
    const good = { title: "Christmas Ornament Charm Necklace", seo_title: "Christmas Ornament Charm Necklace, Flat Charm in Gold or Silver Metals", meta_description: "Flat Christmas ornament charm necklace in sterling silver, gold filled or solid gold. Handmade in small batches, with optional back engraving, on a ball chain. " + "x".repeat(40), google_title: "Christmas Ornament Charm Necklace, Sterling Silver, Flat Charm, Ball Chain", description_html: "<p>" + "A flat Christmas ornament charm cut from sheet metal and finished by hand. ".repeat(3) + "</p><ul><li>Sterling silver, gold filled or solid gold</li><li>Handmade</li><li>Optional back engraving</li><li>Gift ready</li></ul>", tags: ["christmas", "ornament"], theme_keywords: ["christmas"] };
    const ok = await L.runShopify({ callText: stub([JSON.stringify(good)]), facts: f, etsyTitle: "" });
    assert(ok.seo_title.length <= 60 && ok.meta_description.length <= 155 && ok.google_title.length <= 150, JSON.stringify([ok.seo_title.length, ok.meta_description.length]));
    const beadyOut = Object.assign({}, good, { description_html: "<p>A pine bead charm with festive bead accents. " + "filler text here ".repeat(25) + "</p>", google_title: "Beaded Christmas Ornament Necklace, Sterling Silver" });
    const r = await L.runShopify({ callText: stub([JSON.stringify(beadyOut)]), facts: f, etsyTitle: "" });
    assert(r.fallback);
    assert(!/bead/i.test(r.description_html.replace(/beaded ball chain/gi, "")) && !/bead/i.test(r.google_title));
    const r2 = await L.runShopify({ callText: stub(["not json at all"]), facts: f, etsyTitle: "" });
    assert(r2.fallback && r2.title === "Christmas Ornament Charm Necklace");
  });

  await t("Charms category never claims 'necklace' as the product; hoops have no rose gold or engraving", () => {
    const c = L.buildFacts({ listingDesc: "Sea Turtle charm" });
    assert.strictEqual(c.typeNoun, "Necklace Charm");
    const h = L.buildFacts({ listingDesc: "Sea Turtle Charm Hoop Earrings" });
    assert(!h.metals.some(x => /Rose/.test(x)) && h.engraving === false);
    assert(L.validateEtsyTitle("Sea Turtle Hoop Earrings, Sterling Silver, Rose Gold, Flat Charm", h).some(p => /rose gold/.test(p)));
  });

  await t("facts carry a style cue matched to the jewelry type", () => {
    const cue = (d) => L.factsBlock(L.buildFacts({ listingDesc: d })).match(/Style cues[^\n]*/)[0];
    assert(/charm necklace/.test(cue("Owl Charm Regular Necklace")));
    assert(/stud earrings/.test(cue("Owl Charm Stud Earrings")));
    assert(/huggie hoop/.test(cue("Owl Charm Hoop Earrings")));
    assert(/no chain included/.test(cue("Owl charm")));
    assert(/charm bracelet/.test(cue("Owl Charm Bracelets")));
  });

  await t("fallback title/description/shopify fields are valid and bead-free for every category and several designs", () => {
    for (const cat of Object.keys(L.CATS)) for (const motif of ["Christmas Ornament", "Westie Terrier", "Sea Turtle", "Sunflower", "Crescent Moon"]) {
      const fx = L.buildFacts({ category: cat, listingDesc: motif });
      const ti = L.fallbackTitle(fx);
      assert.strictEqual(L.validateEtsyTitle(ti, fx).length, 0, cat + "/" + motif + ": " + ti + " -> " + L.validateEtsyTitle(ti, fx).join("|"));
      assert(ti.length <= 110);
      assert.strictEqual(L.commonProblems(L.fallbackDescription(fx), fx).length, 0);
      assert.strictEqual(L.commonProblems(L.shopifyFallback(fx).description_html.replace(/<[^>]*>/g, " "), fx).length, 0);
    }
  });

  if (process.argv.includes("--demo")) {
    const fx = L.buildFacts({ listingDesc: "Christmas Ornament Charm Beady Necklace" });
    console.log("\n--- BEFORE (old pipeline output for this piece) ---");
    console.log('Title: Christmas Ornament Necklace: Solid Gold 10K, 14K, Handmade Holiday Charm Gift');
    console.log('Keywords: beaded necklace, pine bead, festive bead, snow brooch, seasonal strand, santa locket');
    console.log('Description: "...gives this beady necklace..." / "festive bead accents"');
    console.log("\n--- AFTER: facts the model now receives (instead of 'Charm Beady Necklace') ---");
    console.log(L.factsBlock(fx));
    console.log("\n--- AFTER: structure hint chosen for this design ---\n" + L.patternFor(fx, 1));
    console.log("\n--- AFTER: deterministic safe title (used only if the model keeps failing validation) ---\n" + L.fallbackTitle(fx));
    console.log("--- AFTER: deterministic safe description ---\n" + L.fallbackDescription(fx));
    console.log("\n--- OLD title run through the new validator ---\n" + JSON.stringify(L.validateEtsyTitle("Beaded Christmas Ornament Necklace: Solid Gold 10K, 14K, Handmade Holiday Charm Gift", fx)));
  }

  console.log(`\n${passed} passed`);
})();
