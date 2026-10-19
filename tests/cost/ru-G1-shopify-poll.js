// MEASURES the Shopify/Etsy status polling every open tab does (each call is answered from Firestore by a Netlify function):
//   - shopifyListingUpload?op=status[&recent=N]   server reads = 5 (four counts + the budget doc) + N jobs
//   - etsyApiUsage                                server reads = 2
//   - shopifyDrain-background (kick)              server reads ~ the QUEUED docs (<=150) + a few, when the budget is spent
// on a virtual clock (hours of polling in seconds). Run it against the page before a fix with RU_INDEX=<old Index.html>.
// Usage: node ru-G1-shopify-poll.js [minutes=30]       Exit code 1 when a budget below is exceeded (only when run on the current page).
const { world, advance, setHidden, sleep } = require('./ru-G1-lib.js');
const MIN = Number(process.argv[2]) || 30;
const JOB_BYTES = 15000, BUDGET_BYTES = 2000, USAGE_BYTES = 1500;   // ASSUMED sizes of a queue job document (payload + variant matrix + results), the budget doc, a usage doc: a server-side read moves the whole document
const QUEUE = 40;                                              // QUEUED jobs the server holds while the budget is spent (drain reads them all, max 150)
const jobs = (n, queued) => Array.from({ length: n }, (_, i) => ({ id: 'j' + (i + 1), status: i < queued ? 'QUEUED' : 'DONE', title: 'T' + i, createdAtMs: Date.now() - i * 60000 }));
function stub(state) {
  return (u) => {
    if (/etsyApiUsage/.test(u)) return { ok: true, verified: true, count: 10, budget: 2500, server_time: Date.now(), etsy_reported_at: Date.now() };
    if (/etsyApiProbe/.test(u)) return { verified: true };
    if (/shopifyListingUpload/.test(u)) {
      const q = new URL('http://x' + u.slice(u.indexOf('?') >= 0 ? u.indexOf('?') : u.length)).searchParams;
      const res = { ok: true, queued: state.queued, uploading: 0, failed: 0, done: 500, budget: { used: 940, ceiling: 950, remaining: 10, paused: !!state.paused, resetsInMs: 3600000 } };
      const all = jobs(200, state.queued).filter(j => !(state.missing && j.id === 'j1'));          // 'missing': the server has no such job, not in a list and not by id
      if (q.get('recent')) res.recent = all.slice(0, Math.min(Number(q.get('recent')), 200));
      if (q.get('jobId')) res.job = all.find(j => j.id === q.get('jobId')) || null;
      return res;
    }
    return {};
  };
}
const serverReads = (calls) => {
  let r = 0, b = 0, by = { status: 0, recent: 0, usage: 0, kick: 0 };
  calls.forEach(c => {
    if (/etsyApiUsage/.test(c.url)) { r += 2; by.usage += 2; b += 2 * USAGE_BYTES; }
    else if (/shopifyDrain-background/.test(c.url)) { const k = Math.min(QUEUE, 150) + 4; r += k; by.kick += k; b += Math.min(QUEUE, 150) * JOB_BYTES + BUDGET_BYTES; }
    else if (/shopifyListingUpload/.test(c.url) && /op=status/.test(c.url)) {
      const m = /recent=(\d+)/.exec(c.url); const n = m ? Number(m[1]) : 0; r += 5 + n; by.status += 5; by.recent += n; b += n * JOB_BYTES + BUDGET_BYTES;
    }
  });
  return { r, b, by };
};
async function scenario(w, name, setup, mins, state) {
  const c = await w.computer(name, stub(state));
  const pg = await w.openFast(c);
  await setup(pg);
  await advance(pg, 20000);                                      // settle (startup calls)
  const i0 = w.calls.length, t0 = await pg.evaluate(() => Date.now());
  await advance(pg, mins * 60000, 2000);
  const t1 = await pg.evaluate(() => Date.now());
  const calls = w.calls.slice(i0).filter(x => x.tab === name);
  const virtMin = (t1 - t0) / 60000, sr = serverReads(calls);
  const kinds = {}; calls.forEach(x => { const k = x.url.replace(/^.*functions\//, '').replace(/t=\d+/, '').replace(/jobId=[^&]*/, 'jobId=*'); kinds[k] = (kinds[k] || 0) + 1; });
  const perDay = 1440 / virtMin;
  console.log(`\n--- ${name}: ${virtMin.toFixed(1)} virtual min ---`);
  console.log('calls:', JSON.stringify(kinds));
  console.log(`server bytes: ${(sr.b * perDay / 1e9).toFixed(2)} GB/day/tab (at ${JOB_BYTES} B per job doc)`);
  console.log(`server reads: ${sr.r} = ${Math.round(sr.r * perDay).toLocaleString()}/day/tab   (status overhead ${Math.round(sr.by.status * perDay)}, job lists ${Math.round(sr.by.recent * perDay)}, usage ${Math.round(sr.by.usage * perDay)}, drain kicks ${Math.round(sr.by.kick * perDay)})`);
  return { name, reads: sr.r * perDay, gb: sr.b * perDay / 1e9, calls: kinds };
}
(async () => {
  const w = await world(); const out = {};
  const stats = (status) => (pg) => pg.evaluate((status) => {            // one Statistics row whose Shopify job is not finished: every tab gets a watcher for it
    window.__listingStatsRows = [{ id: 'row1', shopify: { status, jobId: 'j1', url: '', title: 'T0' }, tsMs: Date.now() }];
    window.resumeShopifyWatchers(window.__listingStatsRows);
  }, status);
  out.idle = await scenario(w, 'idle-visible', async () => {}, MIN, { queued: 0 });
  out.idleHidden = await scenario(w, 'idle-hidden', async (pg) => { await setHidden(pg, true); }, MIN, { queued: 0 });
  out.watcher = await scenario(w, 'queued-watcher-visible', stats('QUEUED'), MIN, { queued: 3 });
  out.watcherHidden = await scenario(w, 'queued-watcher-hidden', async (pg) => { await stats('QUEUED')(pg); await setHidden(pg, true); }, MIN, { queued: 3 });
  out.paused = await scenario(w, 'queued-watcher-budget-paused', stats('QUEUED'), MIN, { queued: 3, paused: true });
  out.legacy = await scenario(w, 'pending-row-without-job', (pg) => pg.evaluate(() => {          // a Statistics row saved as PENDING whose job was never queued (a tab closed in between)
    window.__listingStatsRows = [{ id: 'row2', shopify: { status: 'PENDING', jobId: null, url: '', title: 'Never queued' }, tsMs: Date.now() - 86400000 }];
    window.resumeShopifyWatchers(window.__listingStatsRows);
  }), MIN, { queued: 0 });
  out.missing = await scenario(w, 'watcher-job-unknown-to-server', stats('QUEUED'), MIN, { queued: 3, missing: true });
  console.log('\nSUMMARY (modelled Firestore reads/day per open tab):'); Object.entries(out).forEach(([k, v]) => console.log('  ' + k.padEnd(14) + Math.round(v.reads).toLocaleString().padStart(12) + ' reads   ' + v.gb.toFixed(2).padStart(7) + ' GB'));
  await w.close();
  if (process.env.RU_INDEX) return;                                          // comparing an old page: numbers only
  // BUDGETS for the current page (modelled server reads/day per open tab; before the fix in brackets)
  const lim = { idle: [25000, 151459], idleHidden: [3000, 151387], watcher: [200000, 2464867], watcherHidden: [120000, 2464754], paused: [120000, 2464953], missing: [90000, 2464605] };
  let bad = 0; Object.entries(lim).forEach(([k, [max, was]]) => { if (!(out[k].reads <= max)) { bad++; console.log(`FAIL ${k}: ${Math.round(out[k].reads)} > ${max} (was ${was})`); } });
  const listPolls = (out.missing.calls['shopifyListingUpload?op=status&recen'] || 0);
  if (listPolls > 15) { bad++; console.log(`FAIL missing: ${listPolls} job-list polls in ${MIN} min for a job the server does not know (it must be given up after 3 lookups)`); }
  const gb = { idle: [0.05, 1.95], idleHidden: [0.02, 1.96], watcher: [2.3, 35.5], watcherHidden: [1.6, 35.5], paused: [1.5, 35.5], missing: [1.0, 35.5] };      // BYTES the server pulls from Firestore per open tab and day [budget, before]
  Object.entries(gb).forEach(([k, [max, was]]) => { if (!(out[k].gb <= max)) { bad++; console.log(`FAIL ${k}: ${out[k].gb.toFixed(2)} GB/day > ${max} GB (was ${was})`); } });
  const legacyPolls = (out.legacy.calls['shopifyListingUpload?op=status&recen'] || 0);          // one-off per page load, not per day
  if (legacyPolls > 3) { bad++; console.log(`FAIL legacy: ${legacyPolls} job-list reads for one PENDING row without a job in ${MIN} min (it must be given up after 3 tries)`); }
  const kicksPaused = (out.paused.calls['shopifyDrain-background'] || 0);
  if (kicksPaused > 0) { bad++; console.log(`FAIL paused: ${kicksPaused} drain kicks while the budget has nothing that fits`); }
  console.log(bad ? 'FAILED' : 'PASS: shopify/etsy status polling within budget');
  process.exit(bad ? 1 : 0);
})();
