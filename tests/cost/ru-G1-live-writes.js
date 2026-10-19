// Auto-Pilot running on one computer, a second computer watching: what is written (and so read by the watcher) over a few minutes (real time)
// when NOTHING changes. Before the fix the 30 s intent refresh rewrote ctl.auto (live~state~ctl) every time, a write every follower read.
// Usage: node ru-G1-live-writes.js [seconds=150]      Exit 1 when over budget (current page only: RU_INDEX unset).
const { world, reads, bytesOf, sum, diff, until, sleep } = require('./ru-G1-lib.js');
const SEC = Number(process.argv[2]) || 150;
(async () => {
  const w = await world();
  const cA = await w.computer('A'), cB = await w.computer('B');
  const A = await w.open(cA); await sleep(2500); await A.mouse.click(600, 400);
  await until(A, () => window.LiveMirror && window.LiveMirror.state() === 'controller', 8000);
  const B = await w.open(cB);
  await until(B, () => window.LiveMirror.state() === 'follower', 15000);
  await A.evaluate(async () => {
    window.autoProcessingEnabled = true; window.isProcessingSet = true; await window.LiveMirror.setMode('auto');
    await window.ListingLedger.enqueue('Set_X', { category: 'Regular_Necklace', description: 'd' });
    await window.ListingLedger.claim('Set_X', { stage: 'start' });
  });
  await sleep(8000);                                              // settle
  const w0 = Object.assign({}, w.store.w), r0 = await reads(B), b0 = await bytesOf(B), t0 = Date.now();
  await sleep(SEC * 1000);
  const vmin = (Date.now() - t0) / 60000;
  const wd = diff(w0, w.store.w), rd = diff(r0, await reads(B)), bd = diff(b0, await bytesOf(B));
  console.log(`window ${(vmin * 60).toFixed(0)} s | roles: ${await A.evaluate(() => window.LiveMirror.state())}/${await B.evaluate(() => window.LiveMirror.state())}`);
  console.log('store writes in window:', JSON.stringify(wd));
  console.log('follower reads in window:', JSON.stringify(rd), `| ${sum(rd)} reads = ${Math.round(sum(rd) * 1440 / vmin)}/day, ${(sum(bd) * 1440 / vmin / 1e6).toFixed(1)} MB/day`);
  await w.close();
  if (process.env.RU_INDEX) return;
  const ctl = wd['cfg/live~state~ctl'] || 0, limit = Math.ceil(vmin / 5);
  const mb = sum(bd) * 1440 / vmin / 1e6;
  const ok = ctl <= limit && mb <= 10;             // and the follower is delivered at most ~10 MB a day by the live documents (empty-DOM page; real content grows the state docs)
  console.log(ok ? `PASS: ${ctl} writes of live~state~ctl in ${(vmin * 60).toFixed(0)} s while nothing changes (the 30 s refresh used to write ${Math.round(vmin * 2)})` : `FAIL: ${ctl} writes of live~state~ctl in ${(vmin * 60).toFixed(0)} s while nothing changes (limit ${limit}) or ${mb.toFixed(1)} MB/day delivered > 10`);
  process.exit(ok ? 0 : 1);
})();
