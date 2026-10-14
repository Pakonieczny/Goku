const P = require('child_process').execSync('readlink -f $(npm root -g)/playwright').toString().trim();   // needs Playwright installed globally
const { chromium } = require(P);
const fs=require('fs');
const { Store } = require('./store.js');
const FAKE = fs.readFileSync(__dirname + '/fakefb.js', 'utf8');
const URL = 'file://' + require('path').resolve(__dirname, '../../Index.html');
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const which = process.argv[2] || 'all';
(async () => {
  const browser = await chromium.launch({ args: ['--no-sandbox'] });
  async function world() {
    const store = new Store(), dead = new Set();
    async function computer() {
      const c = await browser.newContext();
      await c.exposeBinding('__fs', async (src, op, json) => { if (dead.has(src.page)) return new Promise(() => {}); return JSON.stringify(store.op(op, JSON.parse(json))); });
      await c.addInitScript(FAKE);
      await c.route(/^(?!file:)/, r => r.abort());
      return c;
    }
    return { store, dead, computer };
  }
  async function open(c, name) {
    const pg = await c.newPage({ viewport: { width: 1300, height: 900 } });
    pg.on('pageerror', e => { if (!/firebase is not defined|before initialization|M is not defined/.test(e.message)) console.log(`[${name}] pageerror:`, e.message.slice(0, 140)); });
    pg.on('console', m => { const t = m.text(); if (/auto takeover|\[control\].*(take|resum)/i.test(t)) console.log(`   [${name}]`, t.slice(0, 150)); });
    await pg.goto(URL);
    await pg.evaluate(() => { document.body.classList.remove('lg-locked'); });
    return pg;
  }
  const role = (pg) => pg.evaluate(() => window.LiveMirror ? window.LiveMirror.state() : 'none');
  async function until(pg, pred, ms) { const t0 = Date.now(); while (Date.now() - t0 < ms) { if (await pg.evaluate(pred).catch(() => false)) return Date.now() - t0; await sleep(150); } return -1; }
  const rec = (pg) => pg.evaluate(() => { window.__resumeCalls = 0; if (window.PipeGuard) { const o = window.PipeGuard.resume; window.PipeGuard.resume = function () { window.__resumeCalls++; try { return o.apply(this, arguments); } catch (_) {} }; window.PipeGuard.hasWorkWaiting = () => true; window.PipeGuard.restoreAuto = async () => { window.__restoreAuto = (window.__restoreAuto || 0) + 1; return true; }; } });

  if (which === 's1' || which === 'all') {
    console.log('\n=== S1: a work button pressed on a computer that is only mirroring ===');
    const w = await world(); const cA = await w.computer(), cB = await w.computer();
    const pa = await open(cA, 'A'); await sleep(2000); await pa.mouse.click(600, 400);
    await until(pa, () => window.LiveMirror.state() === 'controller', 8000);
    const pb = await open(cB, 'B'); await until(pb, () => window.LiveMirror.state() === 'follower', 10000);
    await pb.evaluate(() => { window.__clicked = 0; document.getElementById('generateBtn').addEventListener('click', () => { window.__clicked++; }); });
    const t0 = Date.now();
    await pb.click('#generateBtn', { force: true }).catch(() => {});
    const t1 = await until(pb, () => window.LiveMirror.state() === 'controller', 15000);
    await sleep(800);
    console.log(`B became controller after ${t1} ms | A now: ${await role(pa)} | B now: ${await role(pb)} | the click was carried out on B: ${await pb.evaluate(() => window.__clicked)}x`);
    await cA.close(); await cB.close();
  }

  if (which === 'd2') {
    const w = await world(); const cA = await w.computer(), cB = await w.computer();
    const pa = await open(cA, 'A'); await sleep(2000); await pa.mouse.click(600, 400);
    await until(pa, () => window.LiveMirror.state() === 'controller', 8000);
    await pa.evaluate(() => { window.autoProcessingEnabled = true; window.isProcessingSet = true; try { localStorage.setItem('lgAutoIntent.v1', JSON.stringify({ on: true, at: Date.now() })); } catch (_) {} });
    await sleep(3000);
    const pb = await open(cB, 'B'); await until(pb, () => window.LiveMirror.state() === 'follower', 10000); await rec(pb);
    w.dead.add(pa);
    const t0 = Date.now();
    for (let i = 0; i < 9; i++) {
      await sleep(10000);
      const d = await pb.evaluate(() => { const l = window.LiveMirror, c = l.controller(), inf = l.info(); let auto = null; try { auto = l.get('ctl.auto'); } catch (_) {} return { role: l.state(), ctrl: c && { label: c.label, mode: c.mode }, orphaned: inf.orphaned, unresp: inf.unresponsive, cloud: inf.cloud, auto: auto && { on: auto.on, at: auto.at }, localIntent: localStorage.getItem('lgAutoIntent.v1'), resume: window.__resumeCalls }; });
      console.log(Math.round((Date.now() - t0) / 1000) + 's', JSON.stringify(d));
    }
    const a = await pb.evaluate(async () => { try { return await window.PipeControl.acquire('controller-lost'); } catch (e) { return 'ERR ' + e.message; } });
    console.log('manual acquire:', JSON.stringify(a), '| role now:', await role(pb));
    await cA.close(); await cB.close();
  }
  if (which === 's2' || which === 'all') {
    console.log('\n=== S2: another computer is working, then goes dead (no goodbye); this one is open ===');
    const w = await world(); const cA = await w.computer(), cB = await w.computer();
    const pa = await open(cA, 'A'); await sleep(2000); await pa.mouse.click(600, 400);
    await until(pa, () => window.LiveMirror.state() === 'controller', 8000);
    await pa.evaluate(() => { window.autoProcessingEnabled = true; window.isProcessingSet = true; try { localStorage.setItem('lgAutoIntent.v1', JSON.stringify({ on: true, at: Date.now() })); } catch (_) {} });
    await sleep(3000);
    const pb = await open(cB, 'B'); await until(pb, () => window.LiveMirror.state() === 'follower', 10000); await rec(pb);
    console.log('B before:', await role(pb), '| controller mode:', await pb.evaluate(() => { const c = window.LiveMirror.controller(); return c && c.mode; }));
    w.dead.add(pa);                                   // A stops answering: no release, no goodbye
    const tDead = Date.now();
    const t = await until(pb, () => window.LiveMirror.state() === 'controller', 120000);
    console.log(`B took over automatically ${t < 0 ? 'NEVER (>120 s)' : 'after ' + Math.round(t / 1000) + ' s'} | resume calls on B: ${await pb.evaluate(() => window.__resumeCalls)} | auto-pilot adopted: ${await pb.evaluate(() => window.__restoreAuto || 0)}`);
    await cA.close(); await cB.close();
  }

  if (which === 's3' || which === 'all') {
    console.log('\n=== S3: same browser, the working tab is closed by accident, a new tab is opened ===');
    const w = await world(); const c = await w.computer();
    const pa = await open(c, 'A'); await sleep(2000); await pa.mouse.click(600, 400);
    await until(pa, () => window.LiveMirror.state() === 'controller', 8000);
    await pa.evaluate(() => { window.autoProcessingEnabled = true; window.isProcessingSet = true; try { localStorage.setItem('lgAutoIntent.v1', JSON.stringify({ on: true, at: Date.now() })); } catch (_) {} });
    await sleep(3000);
    w.dead.add(pa); await pa.close();                 // closed; its goodbye writes never reach the database
    const t0 = Date.now();
    const pb = await open(c, 'B'); await rec(pb);
    const t = await until(pb, () => window.LiveMirror.state() === 'controller', 120000);
    console.log(`new tab became controller ${t < 0 ? 'NEVER (>120 s)' : 'after ' + (Math.round((Date.now() - t0) / 100) / 10) + ' s from opening'} | resume calls: ${await pb.evaluate(() => window.__resumeCalls)} | auto-pilot adopted: ${await pb.evaluate(() => window.__restoreAuto || 0)}`);
    await c.close();
  }
  await browser.close();
})();
