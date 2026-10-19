// MEASURES what the multi-computer live sync costs: 1 controller + (N-1) followers, all visible, for T seconds, in two states:
//   idle    controller idle, viewers present
//   running Auto-Pilot running on the controller (mode "auto"; the 10 s lease renewal and the ledger heartbeat of a held set)
// Prints reads per TAB per hour/day by document class, and store writes per hour. Usage: node ru-G1-live.js [seconds=90] [tabs=3]
const { world, reads, bytesOf, sum, diff, until, sleep } = require('./ru-G1-lib.js');
const T = Number(process.argv[2]) || 90, N = Number(process.argv[3]) || 3;
(async () => {
  for (const state of ['idle', 'running']) {
    const w = await world(); const pages = [];
    for (let i = 0; i < N; i++) { const c = await w.computer('tab' + i); pages.push(await w.open(c)); await sleep(i === 0 ? 2500 : 500); if (i === 0) { await pages[0].mouse.click(600, 400); await until(pages[0], () => window.LiveMirror.state() === 'controller', 10000); } }
    for (let i = 1; i < N; i++) await until(pages[i], () => window.LiveMirror.state() === 'follower', 15000);
    const A = pages[0];
    if (state === 'running') {
      await A.evaluate(async () => {
        window.autoProcessingEnabled = true; window.isProcessingSet = true;
        await window.LiveMirror.setMode('auto');
        window.__held = await window.ListingLedger.enqueue('Set_X', { category: 'Regular_Necklace', description: 'd' });
        const c = await window.ListingLedger.claim('Set_X', { stage: 'start' }); window.__tok = c && c.token;
      });
    }
    await sleep(8000);                                     // settle: initial snapshots, presence, arrival bursts
    const r0 = await Promise.all(pages.map(reads)), b0 = await Promise.all(pages.map(bytesOf)); const w0 = Object.assign({}, w.store.w);
    await sleep(T * 1000);
    const r1 = await Promise.all(pages.map(reads)), b1 = await Promise.all(pages.map(bytesOf)); const w1 = Object.assign({}, w.store.w);
    const roles = await Promise.all(pages.map(p => p.evaluate(() => window.LiveMirror.state())));
    const per = 86400 / T;
    console.log(`\n=== ${state}: ${N} visible tabs, ${T} s window (roles ${roles.join(',')}) ===`);
    pages.forEach((p, i) => { const d = diff(r0[i], r1[i]); const db = diff(b0[i], b1[i]); console.log(`tab${i} (${roles[i]}): ${sum(d)} reads in window = ${Math.round(sum(d) * per)}/day, ${(sum(db) * per / 1e6).toFixed(2)} MB/day delivered  ` + JSON.stringify(Object.fromEntries(Object.entries(d).map(([k, v]) => [k, Math.round(v * per) + ' / ' + Math.round((db[k] || 0) * per / 1000) + 'KB'])))); });
    const wd = diff(w0, w1); console.log(`writes (all tabs): ${sum(wd)} in window = ${Math.round(sum(wd) * per)}/day  ` + JSON.stringify(Object.fromEntries(Object.entries(wd).map(([k, v]) => [k, Math.round(v * per)]))));
    const sizes = {}; w.store.docs.forEach((v, k) => { if (/live~/.test(k)) sizes[k.replace('ListingGen_Config/', '')] = JSON.stringify(v.d).length; });
    console.log('live doc sizes (bytes, empty-DOM page):', JSON.stringify(sizes));
    await w.close();
  }
})();
