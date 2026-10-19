// The build-version check (assets/build.json every 2 min) is a static file: it must read NOTHING from Firestore. A reload re-attaches every
// listener and re-reads every result set, so a reload that does not cure the mismatch (an old copy still served) must not repeat:
// before, a tab did it 3 times every 10 minutes for as long as it stayed open.
const { world, reads, sum, diff, sleep } = require('./ru-G1-lib.js');
(async () => {
  const w = await world();
  const c = await w.computer('A'); const pg = await w.open(c); await sleep(3000);
  const out = await pg.evaluate(async () => {
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));
    window.__reloads = 0; window.__reloadPage = () => { window.__reloads++; };
    const r = {};
    window.APP_BUILD_URL = 'data:application/json,' + encodeURIComponent(JSON.stringify({ build: 'NEWER' }));
    await window.__buildCheckNow(); await sleep(6000);
    r.afterFirstDetect = window.__reloads;                                   // idle: reloads once
    await window.__buildCheckNow(); await sleep(11000);                      // "reloaded" page is still the old build and sees the same target
    r.afterSameTargetAgain = window.__reloads;                               // must NOT reload again
    window.APP_BUILD_URL = 'data:application/json,' + encodeURIComponent(JSON.stringify({ build: 'NEWER2' }));
    await window.__buildCheckNow(); await sleep(6000);
    r.afterNewerDeploy = window.__reloads;                                   // a different, newer target is still honoured
    return r;
  });
  console.log(JSON.stringify(out));
  await w.close();
  const ok = out.afterFirstDetect === 1 && out.afterSameTargetAgain === 1 && out.afterNewerDeploy === 2;
  console.log(ok ? 'PASS: reload not repeated for the same target; new target still reloads' : 'FAIL');
  process.exit(ok ? 0 : 1);
})();
