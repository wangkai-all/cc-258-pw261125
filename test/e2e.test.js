'use strict';

/**
 * End-to-end test of the real page in a real browser.
 *
 * It boots the three real processes in one Node test, drives the UI with
 * Playwright, and asserts both the visible text and the resulting API state.
 * No skills or subagents are involved in producing or running this test.
 *
 * Browser prerequisites in a bare container:
 *   npx playwright install chromium
 *   LD_LIBRARY_PATH pointing at glib/nss/X11 libs, FONTCONFIG_FILE for CJK text.
 * If the browser cannot start, the test is skipped rather than reported green.
 */

const test = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { startSourceA, startSourceB } = require('../src/sources/start-lib');
const { createQueryServer } = require('../src/coordinator/server');

let chromium = null;
try {
  ({ chromium } = require('playwright'));
} catch {
  chromium = null;
}

async function bootServices() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'e2e-'));
  const a = await startSourceA({ port: 0, dataDir: dir });
  const b = await startSourceB({ port: 0, dataDir: dir, retention: 10 });
  const query = await createQueryServer({
    dbPath: path.join(dir, 'coordinator.sqlite'),
    sourceAUrl: `http://127.0.0.1:${a.port}`,
    sourceBUrl: `http://127.0.0.1:${b.port}`,
  });
  const port = await query.listen(0);
  return {
    dir,
    a,
    b,
    query,
    base: `http://127.0.0.1:${port}`,
    async close() {
      await new Promise((resolve) => query.server.close(resolve));
      query.store.close();
      a.close();
      b.close();
    },
  };
}

async function launchBrowser() {
  if (!chromium) return null;
  try {
    return await chromium.launch();
  } catch (err) {
    console.warn(`  跳过浏览器断言：Chromium 无法启动（${err.message.split('\n')[0]}）`);
    return null;
  }
}

async function waitForRun(base, runId, predicate, timeoutMs = 10000) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const response = await fetch(`${base}/api/runs/${runId}`);
    const payload = await response.json();
    if (predicate(payload.run)) return payload.run;
    await new Promise((resolve) => setTimeout(resolve, 60));
  }
  throw new Error(`run ${runId} did not reach the expected state within ${timeoutMs}ms`);
}

/** Open the fault controls, set "fail the next N reads" and wait for the admin call to land. */
async function injectFailNext(page, source, count = 1) {
  const details = page.locator('.faultbox');
  if (!(await details.evaluate((el) => el.open))) await page.click('.faultbox summary');
  await page.fill(`#${source}-failat`, '0');
  await page.fill(`#${source}-failn`, String(count));
  const posted = page.waitForResponse((response) => response.url().includes(`/api/sources/${source}/admin/faults`)
    && response.request().method() === 'POST');
  await page.click(`#${source}-apply-fault`);
  await posted;
  await page.waitForFunction((prefix) => document.querySelector(`#${prefix}-meta`) !== null, source);
}

async function createViaUI(page, { policy = 'pin', limit = '40' } = {}) {
  await page.check(`#new-run input[name="policy"][value="${policy}"]`);
  await page.fill('#new-run input[name="limit"]', String(limit));
  await page.click('#new-run button[type="submit"]');
  await page.waitForSelector('#run-view:not([hidden])');
  const runId = (await page.textContent('#run-id')).trim();
  assert.match(runId, /^run-/, 'the page must show the created run id');
  return runId;
}

test('e2e: the page shows pinned versions, delivered range and a failure position', async (t) => {
  const env = await bootServices();
  const browser = await launchBrowser();
  t.after(async () => {
    if (browser) await browser.close();
    await env.close();
  });
  if (!browser) {
    t.skip('Chromium unavailable');
    return;
  }

  const page = await browser.newPage({ viewport: { width: 1440, height: 1200 } });
  const consoleErrors = [];
  page.on('console', (msg) => { if (msg.type() === 'error') consoleErrors.push(msg.text()); });
  page.on('pageerror', (err) => consoleErrors.push(err.message));

  await page.goto(env.base, { waitUntil: 'networkidle' });
  await page.waitForSelector('#live-a:not(:has-text("…"))');
  assert.match(await page.textContent('#a-version'), /^snap-/, 'source A version is visible');
  assert.match(await page.textContent('#b-version'), /^revision \d+$/, 'source B revision is visible');

  const runId = await createViaUI(page);
  const bound = await waitForRun(env.base, runId, (run) => run.state === 'bound' || run.state === 'running');
  assert.equal(bound.binding.a.token, (await page.textContent('#stat-a')).trim());
  assert.equal(`revision ${bound.binding.b.revision}`, (await page.textContent('#stat-b')).trim());
  assert.equal(await page.textContent('#stat-emitted'), '尚未输出');

  await page.click('#step3');
  await waitForRun(env.base, runId, (run) => run.emitted.pages === 3);
  await page.waitForFunction(() => /3 页 \/ 120 行/.test(document.querySelector('#stat-emitted').textContent));
  assert.match(await page.textContent('#stat-emitted-detail'), /页 1–3/);
  assert.equal(await page.locator('#pages-body tr').count(), 3);

  // Every row of the pages table names the same A snapshot and B revision.
  const pageRows = await page.locator('#pages-body tr').evaluateAll((rows) => rows.map((row) => row.textContent));
  for (const row of pageRows) {
    assert.ok(row.includes(bound.binding.a.token.slice(0, 20)), 'each page row names the pinned A snapshot');
    assert.ok(row.includes(`r${bound.binding.b.revision}`), 'each page row names the pinned B revision');
  }
  assert.ok(pageRows.every((row) => row.includes('来源读取')), 'first three pages are source reads, not cache hits');

  // Inject a B read failure on the next read, then read one page from the UI.
  await injectFailNext(page, 'b', 1);

  await page.click('#step1');
  await page.waitForSelector('#failure-card:not([hidden])');
  await waitForRun(env.base, runId, (run) => run.state === 'failed');

  const failure = (await (await fetch(`${env.base}/api/runs/${runId}`)).json()).run;
  assert.equal(failure.failure.pageNo, 4);
  assert.equal(failure.failure.source, 'B');
  assert.equal(failure.emitted.pages, 3, 'the failed page never advanced the delivered range');

  const failureText = await page.textContent('#failure-body');
  assert.match(failureText, /#4/);
  assert.match(failureText, /B/);
  assert.match(failureText, /3 页/);
  assert.match(await page.textContent('#stat-emitted'), /3 页 \/ 120 行/);

  // The retry card offers all three contracts and marks pin as current.
  await page.waitForSelector('#retry-card:not([hidden])');
  const options = await page.locator('#retry-options .retry-opt').evaluateAll((nodes) => nodes.map((n) => ({
    policy: n.dataset.policy,
    text: n.textContent,
    pressed: n.getAttribute('aria-pressed'),
  })));
  assert.deepEqual(options.map((o) => o.policy).sort(), ['mixed', 'pin', 'restart']);
  assert.equal(options.find((o) => o.policy === 'pin').pressed, 'true');
  assert.match(options.find((o) => o.policy === 'pin').text, /当前/);

  assert.deepEqual(consoleErrors, [], 'the page must not log console errors');
});

test('e2e: restart is visible as an invalidation; mixed declares a version boundary', async (t) => {
  const env = await bootServices();
  const browser = await launchBrowser();
  t.after(async () => {
    if (browser) await browser.close();
    await env.close();
  });
  if (!browser) {
    t.skip('Chromium unavailable');
    return;
  }

  const page = await browser.newPage({ viewport: { width: 1440, height: 1300 } });
  page.on('dialog', (dialog) => dialog.accept());
  await page.goto(env.base, { waitUntil: 'networkidle' });

  const runId = await createViaUI(page);
  await page.click('#step3');
  await waitForRun(env.base, runId, (run) => run.emitted.pages === 3);

  // Fail B, then advance its revision so pin's revision expires (retention 10,
  // 11 bumps) while the run is stopped.
  await injectFailNext(page, 'b', 1);
  await page.click('#step1');
  await page.waitForSelector('#failure-card:not([hidden])');
  await waitForRun(env.base, runId, (run) => run.state === 'failed');

  for (let i = 0; i < 12; i += 1) {
    await fetch(`${env.base}/api/sources/b/admin/bump`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ count: 9 }),
    });
  }
  await page.click('#refresh-all');
  await page.waitForFunction(() => /floor/.test(document.querySelector('#live-b').textContent));

  // Pin is now impossible and the page must say so before any click.
  await page.waitForFunction(() => {
    const button = document.querySelector('#retry-options .retry-opt[data-policy="pin"]');
    return button && button.disabled;
  });

  // Choose restart, acknowledge, retry, then finish the run from the UI.
  await page.click('#retry-options .retry-opt[data-policy="restart"]');
  await page.check('#ack');
  await page.click('#apply-retry');
  const restarted = await waitForRun(env.base, runId, (run) => run.epoch === 2 && run.state !== 'failed');
  assert.equal(restarted.emitted.pages, 0, 'restart invalidates the delivered prefix');
  assert.deepEqual(restarted.warnings.map((w) => w.code), ['restart_invalidated_prefix']);
  await page.waitForFunction(() => /epoch 2/.test(document.querySelector('#run-updated').textContent));

  await page.click('#stepall');
  const finished = await waitForRun(env.base, runId, (run) => run.state === 'completed', 20000);
  assert.equal(finished.emitted.pages, 6);
  assert.equal(finished.emitted.rows, 240);
  assert.equal(finished.epoch, 2);
  const superseded = (await (await fetch(`${env.base}/api/runs/${runId}`)).json()).run.superseded_pages;
  assert.deepEqual(superseded.map((p) => p.pageNo), [1, 2, 3]);
  await page.waitForFunction(() => document.querySelectorAll('#segments .segment').length >= 1);

  // Verify from the UI: a single new version segment, fully verified.
  await page.click('#verify-run');
  await page.waitForSelector('#verify-card:not([hidden])');
  const verdict = await page.textContent('#verify-summary');
  assert.match(verdict, /校验通过/);
  assert.match(verdict, /单一版本段/);

  // Now repeat the failure scenario on a fresh run and choose mixed instead.
  await page.reload({ waitUntil: 'networkidle' });
  const mixedRunId = await createViaUI(page);
  await page.click('#step3');
  await waitForRun(env.base, mixedRunId, (run) => run.emitted.pages === 3);
  await injectFailNext(page, 'b', 1);
  await page.click('#step1');
  await page.waitForSelector('#failure-card:not([hidden])');
  await waitForRun(env.base, mixedRunId, (run) => run.state === 'failed');

  await fetch(`${env.base}/api/sources/b/admin/bump`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ count: 9 }),
  });
  await page.click('#retry-options .retry-opt[data-policy="mixed"]');
  await page.check('#ack');
  await page.click('#apply-retry');
  const mixed = await waitForRun(env.base, mixedRunId, (run) => run.state !== 'failed');
  assert.equal(mixed.emitted.pages, 3, 'mixed keeps the delivered prefix');
  assert.equal(mixed.policy, 'mixed');
  assert.ok(mixed.warnings.some((w) => w.code === 'version_boundary'));

  await page.click('#stepall');
  const mixedDone = await waitForRun(env.base, mixedRunId, (run) => run.state === 'completed', 20000);
  assert.equal(mixedDone.emitted.pages, 6);
  await page.click('#verify-run');
  await page.waitForSelector('#verify-card:not([hidden])');
  const mixedVerdict = await page.textContent('#verify-summary');
  assert.match(mixedVerdict, /跨多个已声明的版本段/);
  assert.ok((await page.locator('#pages-body tr').count()) >= 6);
  const segments = await page.locator('#segments .segment').count();
  assert.ok(segments >= 2, 'the page renders two version segments');
});

test('e2e: cancel stops at a page boundary and resume keeps the pinned pair', async (t) => {
  const env = await bootServices();
  const browser = await launchBrowser();
  t.after(async () => {
    if (browser) await browser.close();
    await env.close();
  });
  if (!browser) {
    t.skip('Chromium unavailable');
    return;
  }

  const page = await browser.newPage({ viewport: { width: 1440, height: 1200 } });
  await page.goto(env.base, { waitUntil: 'networkidle' });
  const runId = await createViaUI(page);
  const bound = await waitForRun(env.base, runId, (run) => run.state === 'bound' && Boolean(run.binding));
  await page.click('#step3');
  await waitForRun(env.base, runId, (run) => run.emitted.pages === 3);

  await page.click('#cancel-run');
  const cancelled = await waitForRun(env.base, runId, (run) => run.state === 'cancelled');
  assert.equal(cancelled.emitted.pages, 3);
  assert.equal(await page.textContent('#run-state'), '已取消');
  await page.waitForSelector('#resume-run:not([hidden])');
  assert.equal(await page.isDisabled('#step1'), true, 'a cancelled run cannot advance');

  // Sources move on while the run is cancelled.
  await fetch(`${env.base}/api/sources/a/admin/update`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ count: 9 }),
  });
  for (let i = 0; i < 3; i += 1) {
    await fetch(`${env.base}/api/sources/b/admin/bump`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ count: 9 }),
    });
  }

  await page.click('#resume-run');
  const resumed = await waitForRun(env.base, runId, (run) => run.state === 'running');
  assert.equal(resumed.binding.a.token, bound.binding.a.token);
  assert.equal(resumed.binding.b.revision, bound.binding.b.revision);
  assert.equal(resumed.cursor.pageNo, 4);

  await page.click('#stepall');
  const finished = await waitForRun(env.base, runId, (run) => run.state === 'completed', 20000);
  assert.equal(finished.emitted.pages, 6);
  for (const pageRecord of (await (await fetch(`${env.base}/api/runs/${runId}`)).json()).run.pages) {
    assert.equal(pageRecord.versions.a.token, bound.binding.a.token);
    assert.equal(pageRecord.versions.b.revision, bound.binding.b.revision);
  }
  await page.click('#verify-run');
  await page.waitForSelector('#verify-card:not([hidden])');
  assert.match(await page.textContent('#verify-summary'), /校验通过/);
});
