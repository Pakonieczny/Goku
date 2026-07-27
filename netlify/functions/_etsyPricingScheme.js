/*  netlify/functions/_etsyPricingScheme.js
 *
 *  THE canonical Brites Etsy pricing scheme — price sheets, priceFor() and
 *  planStandardRebuild(), extracted verbatim from etsyPricingBatch-background.js.
 *
 *  ═══ WHY THIS FILE EXISTS ══════════════════════════════════════════════
 *
 *  The sheets used to live in two places that had to be kept byte-identical
 *  by hand: the batch runner and etsy-pricing.html. Any drift between them
 *  meant a scheduled run and a manual run would price the same listing
 *  differently. Adding the Listing Generator as a third consumer would have
 *  made that worse, so the server-side copies now come from here:
 *
 *      _etsyPricingScheme.js   <-- this file, the single source of truth
 *        ├── etsyPricingBatch-background.js   (scheduled/batch runs)
 *        └── etsyPricingApplyOne.js           (one listing, called by Index.html)
 *
 *  etsy-pricing.html still carries its own copy because it prices in the
 *  browser for the live preview. It remains the one place that must be
 *  updated in step with this file.
 *
 *  ═══ WHAT IT DOES ══════════════════════════════════════════════════════
 *
 *  planStandardRebuild(products, chainType, engraving) reads a listing's
 *  existing inventory, locates its metal and chain-length dropdowns, and
 *  returns the COMPLETE variation matrix: every canonical metal option
 *  crossed with every real chain length, plus the chainless "Charm Only"
 *  row. Etsy rejects partial matrices, so the full Cartesian product is
 *  mandatory — this is why a listing cannot simply have one price patched.
 *
 *  Each metal option picks one of three near-identical price points at
 *  random (`version` 0/1/2) per listing.
 *
 *  Returns { rows } on success or { error } with a human-readable reason.
 *  It NEVER throws — callers branch on plan.error.
 *
 *  Known limitation: the scheme is necklace-shaped. It requires BOTH a
 *  metal and a chain-length dropdown, so Stud/Hoop earrings and standalone
 *  Charm listings return { error } rather than being priced. CHARM_LISTING_PRICES
 *  below is the pool for standalone charm listings, but the metal-only plan
 *  path that would use it does not exist yet.
 */

"use strict";

const CANON_ORDER=['Silver','Gold','Rose','Silver + Engrave','Gold + Engrave','Rose + Engrave','10k Solid Gold','10k Gold + Engrave','14k Solid Gold','14k Gold + Engrave','Gold-Charm Only','Silver-Charm Only','Rose-Charm Only','10k Solid-Charm Only','14k Solid-Charm Only'];
const CHARM_ONLY_METALS=['Gold-Charm Only','Silver-Charm Only','Rose-Charm Only','10k Solid-Charm Only','14k Solid-Charm Only'];
const NO_CHAIN_VALUE='Charm Only-No Chain';
const REGULAR_PRICES={'Silver':[39.69,40.00,40.31],'Gold':[46.56,46.88,47.19],'Rose':[49.69,50.00,50.31],'Silver + Engrave':[46.56,46.88,47.19],'Gold + Engrave':[51.56,51.88,52.19],'Rose + Engrave':[54.69,55.00,55.31],'10k Solid Gold':[252.81,253.13,253.44],'10k Gold + Engrave':[266.56,266.88,267.19],'14k Solid Gold':[315.94,316.56,316.88],'14k Gold + Engrave':[333.13,333.75,334.06]};
const BEADY_FLAT_PRICES={'Silver':[56.56,56.88,57.19],'Gold':[65.94,66.25,66.56],'Silver + Engrave':[61.56,61.88,62.19],'Gold + Engrave':[70.94,71.25,71.56]};
const BEADY_SOLID_BY_LENGTH={'10k Solid Gold':{14:[395.72,405.61,415.51],16:[423.57,434.16,444.75],18:[451.4,462.68,473.97]},'10k Gold + Engrave':{14:[405.72,415.61,425.51],16:[433.57,444.16,454.75],18:[461.4,472.68,483.97]},'14k Solid Gold':{14:[494.72,507.09,519.46],16:[529.6,542.84,556.08],18:[564.47,578.58,592.69]},'14k Gold + Engrave':{14:[504.72,517.09,529.46],16:[539.6,552.84,566.08],18:[574.47,588.58,602.69]}};
const CHARM_ONLY_PRICE_POOLS={'Gold-Charm Only':[28.13,28.75,29.38],'Silver-Charm Only':[27.81,28.44,29.06],'Rose-Charm Only':[29.06,29.38,30.00],'10k Solid-Charm Only':[110.63,111.56,112.50],'14k Solid-Charm Only':[138.44,139.38,140.63]};

// Standalone CHARM listings (Etsy). Separate from CHARM_ONLY_PRICE_POOLS above:
// that pool is the chainless option INSIDE a necklace listing, which ships free.
// A standalone charm listing charges shipping on top, so its base prices are lower.
// Gold set by the operator (20/21/22); Silver and Rose derived at the same ratio to
// Gold they held before; the two solid golds take the same DOLLAR cut Gold took
// (shipping is a flat cost, not a percentage); engrave premiums are the per-metal
// premiums already used in REGULAR_PRICES.
const CHARM_LISTING_PRICES={'Silver':[21.75,22.85,23.94],'Gold':[22.00,23.10,24.20],'Rose':[22.73,23.61,24.71],'Silver + Engrave':[26.75,28.85,29.94],'Gold + Engrave':[27.00,29.10,30.20],'Rose + Engrave':[27.73,29.61,30.71],'10k Solid Gold':[102.50,103.81,105.12],'10k Gold + Engrave':[112.50,114.81,116.12],'14k Solid Gold':[130.31,131.63,133.25],'14k Gold + Engrave':[140.31,142.63,144.25]};
const ENGRAVE_INSTRUCTIONS='To include back engraving on your piece, choose the "+ engrave" option and leave us your instructions here.';
function normOpt(v){return String(v).toLowerCase().replace(/[\u2013\u2014]/g,'-').replace(/\s*-\s*/g,'-').replace(/\s*\+\s*/g,' + ').replace(/\s+/g,' ').trim()}
const CANON_ALIASES=(()=>{const m=new Map();const add=(c,...alts)=>{m.set(normOpt(c),c);for(const a of alts)m.set(normOpt(a),c)};
  add('Silver');add('Gold');add('Rose','rose gold');
  add('Silver + Engrave');add('Gold + Engrave');add('Rose + Engrave','rose gold + engrave');
  add('10k Solid Gold');add('10k Gold + Engrave');add('14k Solid Gold');add('14k Gold + Engrave');
  add('Gold-Charm Only','charm only gold','gold charm only');
  add('Silver-Charm Only','charm only silver','silver charm only');
  add('Rose-Charm Only','charm only rose','rose charm only');
  add('10k Solid-Charm Only','charm only 10k solid','10k solid charm only');
  add('14k Solid-Charm Only','charm only 14k solid','14k solid charm only');
  // Engraved Charm Only. These were in CANON_ORDER but NOT here, so canonFor()
  // returned null for them: the console reported all five as "removed options"
  // on every re-run (options it was about to re-create), and lengthSortKey read
  // '10k Solid-Charm Only + Engrave' as a 10-inch chain.
  add('Gold-Charm Only + Engrave','charm only gold + engrave','gold charm only + engrave');
  add('Silver-Charm Only + Engrave','charm only silver + engrave','silver charm only + engrave');
  add('Rose-Charm Only + Engrave','charm only rose + engrave','rose charm only + engrave');
  add('10k Solid-Charm Only + Engrave','charm only 10k solid + engrave','10k solid charm only + engrave');
  add('14k Solid-Charm Only + Engrave','charm only 14k solid + engrave','14k solid charm only + engrave');
  return m})();
function canonFor(v){return CANON_ALIASES.get(normOpt(v))||null}
function isNoChainVal(v){const c=normOpt(v);
  // normOpt collapses ' - ' -> '-' and en/em dashes -> '-', so every spacing
  // variant of the chainless value is recognised. The old regex used '.?'
  // (exactly one char) and MISSED 'Charm Only - No Chain', which then survived
  // as a real chain length and shipped enabled full-price necklace rows.
  return /^no-?\s*chain/.test(c)||/charm-?\s*only[-\s]*no-?\s*chain/.test(c)||c==='charm only'}
function parseLen(v){const s=String(v);
  // A range ('20-22 inch') or a metric value ('20 cm') is NOT an inch length.
  // The old /(\d+)/ took the first integer anywhere, so both read as 20 and were
  // silently deleted from the live listing by the 20-inch filter.
  if(/\d+\s*[-\u2013\u2014]\s*\d+/.test(s))return null;
  if(/\d\s*(?:cm|mm)\b/i.test(s))return null;
  const m=s.match(/(\d+)\s*(?:"|\u201d|''|in\b|inch)/i)||s.match(/^\s*(\d+)\s*$/);
  return m?parseInt(m[1],10):null}
function titleCaseOpt(v){return String(v).split(/\s+/).map(w=>/^[a-z]/i.test(w)?w[0].toUpperCase()+w.slice(1).toLowerCase():w.toLowerCase()).join(' ')}
function firstOffering(p){return (p&&p.offerings||[])[0]||{price:null,quantity:0,is_enabled:true}}
function propValue2(p,id){const v=(p.property_values||[]).find(x=>Number(x.property_id)===Number(id));return v?(v.values||[]).join('/'):''}
function deep(v){return JSON.parse(JSON.stringify(v))}
function priceFor(opt,lengthValue,version,chainType){
  if(CHARM_ONLY_PRICE_POOLS[opt])return CHARM_ONLY_PRICE_POOLS[opt][version];
  if(chainType==='beady'){
    if(BEADY_SOLID_BY_LENGTH[opt]){const len=parseLen(lengthValue);const col=BEADY_SOLID_BY_LENGTH[opt][len];
      if(!col)throw new Error('No Beady '+opt+' price for chain length "'+lengthValue+'" (sheet covers 14/16/18 only).');
      return col[version]}
    const flat=BEADY_FLAT_PRICES[opt];
    if(!flat)throw new Error('No Beady price for "'+opt+'".');
    return flat[version];
  }
  const reg=REGULAR_PRICES[opt];
  if(!reg)throw new Error('No Regular price for "'+opt+'".');
  return reg[version];
}
function planStandardRebuild(products,chainType,engraving){
  const propsMap=new Map();
  for(const p of (products||[]))for(const v of (p.property_values||[])){const id=Number(v.property_id);
    if(!propsMap.has(id))propsMap.set(id,{property_id:id,property_name:v.property_name||'Variation',values:[]});
    for(const val of (v.values||[]))if(!propsMap.get(id).values.includes(val))propsMap.get(id).values.push(val)}
  const props=[...propsMap.values()];
  const find=res=>{for(const re of res){const h=props.find(p=>re.test(String(p.property_name||'')));if(h)return h}return null};
  const metalProp=find([/metal/i,/material/i,/colou?r/i]);
  if(!metalProp)return {error:'No metal dropdown found. Dropdowns: '+props.map(p=>p.property_name).join(', ')};
  // '/size/i' used to be an unconditional fallback, which made a Hoop Earring's
  // "Hoop Size" menu look like a chain-length menu and let the console rebuild an
  // earring as a necklace. A bare Size menu now only qualifies if its values
  // actually read as lengths.
  let lengthProp=find([/chain\s*length/i,/length/i,/chain/i]);
  if(!lengthProp)lengthProp=props.find(p=>/size/i.test(String(p.property_name||''))
    &&(p.values||[]).some(v=>/inch|\"|\u201d|''/i.test(String(v))||isNoChainVal(v)))||null;
  if(!lengthProp)return {error:'No chain-length dropdown found. Dropdowns: '+props.map(p=>p.property_name).join(', ')};
  if(metalProp.property_id===lengthProp.property_id)return {error:'Metal and chain-length detection matched the same dropdown.'};
  const skipRose=chainType==='beady';
  const targetMetals=CANON_ORDER.filter(o=>!(skipRose&&/rose/i.test(o))&&!(!engraving&&/engrave/i.test(o)));
  const realLengths=[...new Set(lengthProp.values.filter(v=>!isNoChainVal(v)&&parseLen(v)!==20).map(titleCaseOpt))];
  if(!realLengths.length)return {error:'No usable chain lengths (only 20-inch or no-chain values).'};
  if(chainType==='beady'){const bad=realLengths.filter(l=>![14,16,18].includes(parseLen(l)));
    if(bad.length)return {error:'Beady pricing covers only 14/16/18-inch chains; listing also has: '+bad.join(', ')}}
  const allLengths=[...realLengths,NO_CHAIN_VALUE];
  // The property map is the UNION across all products, but the row template used
  // to be products[0] unconditionally. If products[0] lacked one of the two
  // dropdowns, the row loop below threw a TypeError -- breaking this function's
  // documented "never throws" contract. Pick a product that carries both.
  const hasProp=(p,pr)=>(p.property_values||[]).some(v=>Number(v.property_id)===Number(pr.property_id));
  const tmpl=(products||[]).find(p=>hasProp(p,metalProp)&&hasProp(p,lengthProp));
  if(!tmpl)return {error:(products||[]).length
    ? 'No inventory product carries both the metal and chain-length menus; this listing\'s inventory is inconsistent.'
    : 'Listing has no inventory products to rebuild from.'};
  const enabledRow=products.find(p=>firstOffering(p).is_enabled!==false)||tmpl;
  const baseSku=String(enabledRow.sku||'').trim();
  const baseQty=Math.max(1,Number(firstOffering(enabledRow).quantity)||1);
  const plan=[];
  // ONE draw per listing. This used to sit inside the loop, so each metal picked
  // its own tier -- and because the Charm Only pools overlap, that let Gold price
  // ABOVE Rose inside the same dropdown (5 overlapping pairs). Shopify already
  // applies a single tier to every variant; this matches it.
  const version=Math.floor(Math.random()*3);
  try{
    for(const opt of targetMetals){
      const isCharm=CHARM_ONLY_METALS.includes(opt);
      const priceBy={};
      for(const len of allLengths){const isNC=isNoChainVal(len);
        priceBy[len]=priceFor(opt,isCharm?null:(isNC?realLengths[0]:len),version,chainType)}
      plan.push({opt,priceBy,isCharm});
    }
  }catch(e){return {error:e.message}}
  const rows=[];
  for(const {opt,priceBy,isCharm} of plan){
    const sku=isCharm?((baseSku?baseSku+'-CO':'CO-'+opt.replace(/[^A-Za-z0-9]+/g,'').slice(0,10)).slice(0,32)):baseSku;
    for(const len of allLengths){const isNC=isNoChainVal(len);const enabled=isCharm?isNC:!isNC;
      const c=deep(tmpl);c.product_id=null;
      if(c.offerings&&c.offerings[0]){c.offerings[0].offering_id=null;c.offerings[0].price=priceBy[len];c.offerings[0].is_enabled=enabled;c.offerings[0].quantity=enabled?baseQty:0}
      c.sku=sku;
      const mv=c.property_values.find(v=>Number(v.property_id)===Number(metalProp.property_id));mv.values=[opt];mv.value_ids=[];
      const lv=c.property_values.find(v=>Number(v.property_id)===Number(lengthProp.property_id));lv.values=[len];lv.value_ids=[];
      rows.push(c);
    }
  }
  return {rows};
}


/* ═══════════════════════════════════════════════════════════════════════
 *  METAL-ONLY LISTINGS — standalone Charms and Stud Earrings
 * ═══════════════════════════════════════════════════════════════════════
 *
 *  planStandardRebuild() above is necklace-shaped: it demands a metal AND a
 *  chain-length dropdown, so Charm and Stud listings could never be priced by
 *  the generator, the console or the batch. They went live at whatever their
 *  template ID carried. These two planners close that gap.
 *
 *  DISPLAY STRINGS ARE PRESERVED, NOT INVENTED. Charm and Stud listings use
 *  Shopify-style metal names ("Sterling Silver", "Gold Filled", "14K SOLID
 *  GOLD") rather than the necklaces' "Silver"/"Gold"/"Rose", and the casing
 *  varies. Rather than hardcode them, both planners read the listing's own
 *  values, canonicalise them to a price key, and write the ORIGINAL string
 *  back. A metal being newly added (10k) borrows its styling from the
 *  existing 14k value, so "14K SOLID GOLD" yields "10K SOLID GOLD" and
 *  "14k Solid Gold" yields "10k Solid Gold".
 */

// Canonical metal keys for metal-only listings, in display order.
const ML_ORDER = ['Sterling Silver', 'Gold Filled', 'Rose Gold Filled', '10k Solid Gold', '14k Solid Gold'];

// Maps a listing's real metal value onto a canonical key. Covers both the
// Shopify-style names these listings use and the necklace scheme's names.
const ML_ALIASES = (() => {
  const m = new Map();
  const add = (canon, ...alts) => { m.set(normOpt(canon), canon); for (const a of alts) m.set(normOpt(a), canon); };
  add('Sterling Silver',  'silver', 'sterling', 'ss');
  add('Gold Filled',      'gold', '14k gold filled', 'gold-filled', 'gf', '14k gf');
  add('Rose Gold Filled', 'rose', 'rose gold', '14k rose gold filled', 'rose-gold filled', 'rg', 'rgf');
  add('10k Solid Gold',   '10k solid', '10k gold', '10k', '10kt solid gold');
  add('14k Solid Gold',   '14k solid', '14k gold', '14k', '14kt solid gold');
  return m;
})();
function mlMetalFor(v) { return ML_ALIASES.get(normOpt(v)) || null; }

/*  Charm Type — the SECOND dropdown on a standalone Charm listing.
 *
 *  'huggie' is deliberately absent from the price sheet. Huggie CHARM SET was
 *  never in scope for the Etsy repricing (the settled Etsy charm table has two
 *  columns, Plain and + Engrave; only the Shopify table has a Huggie column),
 *  so its price is PRESERVED from the listing rather than rewritten.
 */
const CT_ORDER = ['plain', 'engrave', 'huggie'];
const CT_ALIASES = (() => {
  const m = new Map();
  const add = (k, ...alts) => { for (const a of alts) m.set(normOpt(a), k); };
  add('plain',   'necklace charm', 'charm', 'plain', 'necklace');
  add('engrave', 'charm + engraving', 'charm + engrave', '+ engrave', 'engrave', 'engraving', 'charm engraving');
  add('huggie',  'huggie charm set', 'huggie charm', 'huggie', 'huggie set');
  return m;
})();
function charmTypeFor(v) { return CT_ALIASES.get(normOpt(v)) || null; }

// Price keys in CHARM_LISTING_PRICES, by canonical metal.
const ML_TO_CHARM_KEY = {
  'Sterling Silver':  ['Silver', 'Silver + Engrave'],
  'Gold Filled':      ['Gold', 'Gold + Engrave'],
  'Rose Gold Filled': ['Rose', 'Rose + Engrave'],
  '10k Solid Gold':   ['10k Solid Gold', '10k Gold + Engrave'],
  '14k Solid Gold':   ['14k Solid Gold', '14k Gold + Engrave'],
};

/*  Etsy STUD EARRINGS.
 *
 *  Derived from the Shopify stud table: filled metals at -15%, and solid gold
 *  re-based so 10k takes over the previous 14k figures while 14k rises 20%.
 *  The same solid-gold move is applied to the Shopify table, so the two
 *  channels stay aligned on studs.
 */
const STUD_PRICES = {
  'Sterling Silver':  [38.25, 40.80, 43.35],
  'Gold Filled':      [41.65, 44.20, 46.75],
  'Rose Gold Filled': [44.20, 46.75, 49.30],
  '10k Solid Gold':   [184.80, 198.40, 212.80],
  '14k Solid Gold':   [221.76, 238.08, 255.36],
};

// 10k Huggie has no source: Huggie is out of scope for repricing, but 10k is a
// NEW metal so there is no existing 10k Huggie row to preserve. Derived from
// the listing's own preserved 14k Huggie using Etsy's 10k rule.
const HUGGIE_10K_FROM_14K = 0.80;

function mlMoney(n) { return Math.round(Number(n) * 100) / 100; }

// Shared: collect the listing's dropdowns.
function mlProps(products) {
  const map = new Map();
  for (const p of (products || [])) for (const v of (p.property_values || [])) {
    const id = Number(v.property_id);
    if (!map.has(id)) map.set(id, { property_id: id, property_name: v.property_name || 'Variation', values: [] });
    for (const val of (v.values || [])) if (!map.get(id).values.includes(val)) map.get(id).values.push(val);
  }
  return [...map.values()];
}
function mlFindMetal(props) {
  return props.find(p => /metal|material|colou?r/i.test(String(p.property_name || ''))) || null;
}
// Build canonical -> original display string, preserving the listing's own text.
function mlDisplayMap(values) {
  const out = {};
  for (const v of values) { const c = mlMetalFor(v); if (c && !out[c]) out[c] = String(v); }
  return out;
}
// Style a newly-added 10k value on the existing 14k one.
function tenKDisplayFrom(fourteen) {
  if (!fourteen) return '10K SOLID GOLD';
  return String(fourteen).replace(/14\s*k/i, m => (m[0] === '1' && /K/.test(m) ? '10K' : '10k'))
                          .replace(/14/, '10');
}


/*  planCharmListingRebuild(products) — standalone Etsy Charm listings.
 *
 *  Structure: Metal Choice x Charm Type. Rebuilds the FULL matrix (Etsy
 *  rejects partial ones), adding 10k Solid Gold as a new metal.
 *
 *  Plain and + Engrave come from CHARM_LISTING_PRICES. Huggie CHARM SET is
 *  PRESERVED at whatever the listing already charges — it was never in scope
 *  for the repricing. The single exception is the newly-added 10k row, which
 *  has nothing to preserve; it is derived from the preserved 14k Huggie.
 *
 *  Returns { rows } or { error }. Never throws.
 */
function planCharmListingRebuild(products, opts) {
  const o = opts || {};
  const props = mlProps(products);
  const metalProp = mlFindMetal(props);
  if (!metalProp) return { error: 'No metal dropdown found. Dropdowns: ' + props.map(p => p.property_name).join(', ') };
  const typeProp = props.find(p => p.property_id !== metalProp.property_id && /charm\s*type|type|style|option/i.test(String(p.property_name || '')))
    || props.find(p => p.property_id !== metalProp.property_id && (p.values || []).some(v => charmTypeFor(v)));
  if (!typeProp) return { error: 'No Charm Type dropdown found. A standalone charm listing needs Metal Choice x Charm Type. Dropdowns: ' + props.map(p => p.property_name).join(', ') };

  // Canonicalise both axes, refusing anything unrecognised rather than guessing.
  const display = mlDisplayMap(metalProp.values);
  const unknownMetals = metalProp.values.filter(v => !mlMetalFor(v));
  if (unknownMetals.length) return { error: 'Unrecognised metal option(s): ' + unknownMetals.join(', ') + '. Refusing to reprice a listing whose metals do not map to the price sheet.' };
  const typeDisplay = {};
  for (const v of typeProp.values) { const c = charmTypeFor(v); if (c && !typeDisplay[c]) typeDisplay[c] = String(v); }
  const unknownTypes = typeProp.values.filter(v => !charmTypeFor(v));
  if (unknownTypes.length) return { error: 'Unrecognised Charm Type option(s): ' + unknownTypes.join(', ') + '.' };
  if (!typeDisplay.plain) return { error: 'This listing has no plain "Necklace CHARM" option, so there is nothing to price from.' };

  // Existing Huggie prices per canonical metal — preserved, never rewritten.
  const huggiePrev = {};
  if (typeDisplay.huggie) {
    for (const pr of (products || [])) {
      const mv = (pr.property_values || []).find(v => Number(v.property_id) === Number(metalProp.property_id));
      const tv = (pr.property_values || []).find(v => Number(v.property_id) === Number(typeProp.property_id));
      if (!mv || !tv) continue;
      const mk = mlMetalFor((mv.values || [])[0]);
      const tk = charmTypeFor((tv.values || [])[0]);
      if (mk && tk === 'huggie') {
        const price = firstOffering(pr).price;
        const amt = (price && typeof price === 'object') ? (Number(price.amount) / Number(price.divisor || 100)) : Number(price);
        if (isFinite(amt) && amt > 0) huggiePrev[mk] = mlMoney(amt);
      }
    }
  }

  // Target metals: everything already present, plus 10k.
  const present = ML_ORDER.filter(m => display[m]);
  if (!present.length) return { error: 'No usable metal options on this listing.' };
  const targetMetals = ML_ORDER.filter(m => display[m] || (m === '10k Solid Gold' && o.add10k !== false && display['14k Solid Gold']));
  if (!display['10k Solid Gold'] && targetMetals.includes('10k Solid Gold')) {
    display['10k Solid Gold'] = tenKDisplayFrom(display['14k Solid Gold']);
  }
  const targetTypes = CT_ORDER.filter(t => typeDisplay[t]);

  // 10k Huggie: nothing to preserve (new metal). Derive from preserved 14k.
  if (targetTypes.includes('huggie') && huggiePrev['10k Solid Gold'] == null) {
    if (huggiePrev['14k Solid Gold'] != null) huggiePrev['10k Solid Gold'] = mlMoney(huggiePrev['14k Solid Gold'] * HUGGIE_10K_FROM_14K);
    else if (targetMetals.includes('10k Solid Gold')) return { error: 'Cannot price the new 10k Huggie CHARM SET: this listing has no existing 14k Huggie price to derive it from.' };
  }
  const missingHuggie = targetTypes.includes('huggie') ? targetMetals.filter(m => huggiePrev[m] == null) : [];
  if (missingHuggie.length) return { error: 'No existing Huggie CHARM SET price to preserve for: ' + missingHuggie.join(', ') + '. Huggie is not repriced by the scheme, so an existing value is required.' };

  const tmpl = (products || []).find(pr => [metalProp, typeProp].every(pp => (pr.property_values || []).some(v => Number(v.property_id) === Number(pp.property_id))));
  if (!tmpl) return { error: 'No inventory product carries both the metal and Charm Type menus; this listing\'s inventory is inconsistent.' };
  const enabledRow = products.find(pr => firstOffering(pr).is_enabled !== false) || tmpl;
  const baseSku = String(enabledRow.sku || '').trim();
  const baseQty = Math.max(1, Number(firstOffering(enabledRow).quantity) || 1);
  const version = Math.floor(Math.random() * 3);   // one draw per listing

  const rows = [];
  for (const m of targetMetals) {
    const [plainKey, engKey] = ML_TO_CHARM_KEY[m];
    for (const t of targetTypes) {
      let price;
      if (t === 'huggie') price = huggiePrev[m];
      else {
        const pool = CHARM_LISTING_PRICES[t === 'engrave' ? engKey : plainKey];
        if (!pool) return { error: 'No standalone-charm price for "' + m + '" (' + t + ').' };
        price = mlMoney(pool[version]);
      }
      const c = deep(tmpl); c.product_id = null;
      if (c.offerings && c.offerings[0]) {
        c.offerings[0].offering_id = null; c.offerings[0].price = price;
        c.offerings[0].is_enabled = true; c.offerings[0].quantity = baseQty;
      }
      c.sku = baseSku;
      const mv = c.property_values.find(v => Number(v.property_id) === Number(metalProp.property_id));
      mv.values = [display[m]]; mv.value_ids = [];
      const tv = c.property_values.find(v => Number(v.property_id) === Number(typeProp.property_id));
      tv.values = [typeDisplay[t]]; tv.value_ids = [];
      rows.push(c);
    }
  }
  return { rows, metals: targetMetals, types: targetTypes, preservedHuggie: huggiePrev };
}

/*  planStudRebuild(products) — Etsy Stud Earring listings.
 *
 *  Structure: a single Metal dropdown. Rebuilds the full set from STUD_PRICES,
 *  adding 10k and 14k Solid Gold (live stud listings carry neither).
 *
 *  Returns { rows } or { error }. Never throws.
 */
function planStudRebuild(products, opts) {
  const o = opts || {};
  const props = mlProps(products);
  const metalProp = mlFindMetal(props);
  if (!metalProp) return { error: 'No metal dropdown found. Dropdowns: ' + props.map(p => p.property_name).join(', ') };
  if (props.length > 1) {
    const extra = props.filter(p => p.property_id !== metalProp.property_id).map(p => p.property_name);
    return { error: 'Stud pricing expects a single Metal dropdown; this listing also has: ' + extra.join(', ') + '.' };
  }
  const unknown = metalProp.values.filter(v => !mlMetalFor(v));
  if (unknown.length) return { error: 'Unrecognised metal option(s): ' + unknown.join(', ') + '.' };
  const display = mlDisplayMap(metalProp.values);
  if (!Object.keys(display).length) return { error: 'No usable metal options on this listing.' };

  // Solid gold is added even when absent, which is the normal case.
  const addSolid = o.addSolidGold !== false;
  const targetMetals = ML_ORDER.filter(m => display[m] || (addSolid && /Solid Gold$/.test(m)));
  if (!display['14k Solid Gold'] && targetMetals.includes('14k Solid Gold')) display['14k Solid Gold'] = o.solidGold14Label || '14K SOLID GOLD';
  if (!display['10k Solid Gold'] && targetMetals.includes('10k Solid Gold')) display['10k Solid Gold'] = tenKDisplayFrom(display['14k Solid Gold']);

  const tmpl = (products || []).find(pr => (pr.property_values || []).some(v => Number(v.property_id) === Number(metalProp.property_id)));
  if (!tmpl) return { error: 'Listing has no inventory products to rebuild from.' };
  const enabledRow = products.find(pr => firstOffering(pr).is_enabled !== false) || tmpl;
  const baseSku = String(enabledRow.sku || '').trim();
  const baseQty = Math.max(1, Number(firstOffering(enabledRow).quantity) || 1);
  const version = Math.floor(Math.random() * 3);

  const rows = [];
  for (const m of targetMetals) {
    const pool = STUD_PRICES[m];
    if (!pool) return { error: 'No stud price for "' + m + '".' };
    const c = deep(tmpl); c.product_id = null;
    if (c.offerings && c.offerings[0]) {
      c.offerings[0].offering_id = null; c.offerings[0].price = mlMoney(pool[version]);
      c.offerings[0].is_enabled = true; c.offerings[0].quantity = baseQty;
    }
    c.sku = baseSku;
    const mv = c.property_values.find(v => Number(v.property_id) === Number(metalProp.property_id));
    mv.values = [display[m]]; mv.value_ids = [];
    rows.push(c);
  }
  return { rows, metals: targetMetals };
}

/*  listingKindFor() — which planner a listing needs.
 *  'regular' | 'beady' -> planStandardRebuild
 *  'charm'             -> planCharmListingRebuild
 *  'stud'              -> planStudRebuild
 *  null                -> skip. Hoop earrings land here: Shopify has a huggie
 *                         table but no Etsy hoop prices exist yet.
 */
function listingKindFor(queueId, category) {
  const q = String(queueId || '').toLowerCase().trim();
  if (q === 'queuebeadynecklace') return 'beady';
  if (q === 'queueregnecklace')   return 'regular';   // also serves Bracelets
  if (q === 'queuecharms')        return 'charm';
  if (q === 'queuestudearrings')  return 'stud';
  if (q === 'queuehoopearrings')  return null;
  const c = String(category || '').toLowerCase().trim();
  if (c.startsWith('beady')) return 'beady';
  if (c.startsWith('regular') || c.startsWith('bracelet')) return 'regular';
  if (c.startsWith('charm')) return 'charm';
  if (c.startsWith('stud'))  return 'stud';
  if (c.startsWith('hoop') || c.startsWith('huggie')) return null;
  return null;
}

/*  Chain-type resolution — THE single place this decision is made.
 *
 *  It previously existed only inside etsyPricingApplyOne.js, while
 *  etsyPricingBatch-background.js did `d.chain_type === "beady" ? "beady" : "regular"`.
 *  A listing whose Firestore doc had no chain_type was therefore SKIPPED by the
 *  generator and priced off the REGULAR sheet by the batch -- the same listing,
 *  two prices ($39.69 vs $56.56 on Silver). Both now call this.
 *
 *  Returns 'beady' | 'regular' | null. null means SKIP. It never guesses:
 *  defaulting an unknown source to 'regular' is a silent money error, not a
 *  visible failure.
 */
function normalizeChainType(v){
  const t=String(v||'').toLowerCase().trim();
  if(t==='beady')return 'beady';
  if(t==='regular')return 'regular';
  return null;
}
function chainTypeFor(queueId,category){
  const q=String(queueId||'').toLowerCase().trim();
  if(q==='queuebeadynecklace')return 'beady';
  if(q==='queueregnecklace')return 'regular';   // also serves Bracelets (same sheet)
  if(q==='queuestudearrings'||q==='queuehoopearrings'||q==='queuecharms')return null;
  const c=String(category||'').toLowerCase().trim();
  if(c.startsWith('beady'))return 'beady';
  if(c.startsWith('regular')||c.startsWith('bracelet'))return 'regular';
  return null;                                   // unknown -- skip rather than guess
}

module.exports = {
  chainTypeFor,
  listingKindFor,
  planCharmListingRebuild,
  planStudRebuild,
  STUD_PRICES,
  ML_ORDER,
  mlMetalFor,
  charmTypeFor,
  HUGGIE_10K_FROM_14K,
  normalizeChainType,
  canonFor,
  isNoChainVal,
  CANON_ORDER,
  CHARM_ONLY_METALS,
  NO_CHAIN_VALUE,
  ENGRAVE_INSTRUCTIONS,
  REGULAR_PRICES,
  BEADY_FLAT_PRICES,
  BEADY_SOLID_BY_LENGTH,
  CHARM_ONLY_PRICE_POOLS,
  CHARM_LISTING_PRICES,
  parseLen,
  titleCaseOpt,
  firstOffering,
  deep,
  priceFor,
  planStandardRebuild,
};
