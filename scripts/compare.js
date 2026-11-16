'use strict';

/**
 * Compare the three retry contracts against the same injected failure.
 *
 * The experiment is deliberately run against fresh, real sources (not mocks):
 *
 *   1. pin      — a B read fails after 2 of 6 pages were delivered, and B still
 *                 retains the pinned revision.
 *   2. pin ×    — the same failure, but B's retention floor moves past the
 *                 pinned revision while the run is stopped.
 *   3. restart  — from state 2, discard the delivered prefix and re-read from
 *                 a new (A snapshot, B revision) pair.
 *   4. mixed    — from state 2, keep the prefix and rebind only B, recording a
 *                 version boundary.
 *
 * For each: what the query promised, what it delivered, what it cost, and
 * whether the delivered pages pass verification.
 *
 *   node scripts/compare.js             human-readable report
 *   node scripts/compare.js --json out  also write the raw measurements
 */

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { startSourceA, startSourceB } = require('../src/sources/start-lib');
const { Store } = require('../src/coordinator/store');
const { SourceClient } = require('../src/coordinator/source-client');
const { QueryCoordinator, CONTRACT_TEXT } = require('../src/coordinator/coordinator');

const PAGE_LIMIT = 40; // 240 accounts in the fixture → 6 pages
const TOTAL_PAGES = 6;
const PAGES_BEFORE_FAILURE = 2;

async function boot({ retention = 10 } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'compare-'));
  const a = await startSourceA({ port: 0, dataDir: dir });
  const b = await startSourceB({ port: 0, dataDir: dir, retention });
  const store = new Store(path.join(dir, 'coordinator.sqlite'));
  const sourceA = new SourceClient({ baseUrl: `http://127.0.0.1:${a.port}`, name: 'A' });
  const sourceB = new SourceClient({ baseUrl: `http://127.0.0.1:${b.port}`, name: 'B' });
  const coordinator = new QueryCoordinator({ store, sourceA, sourceB });
  return {
    dir,
    a,
    b,
    store,
    sourceA,
    sourceB,
    coordinator,
    async close() {
      store.close();
      await new Promise((resolve) => a.server.close(resolve));
      await new Promise((resolve) => b.server.close(resolve));
    },
  };
}

/** Drive the run to the same failure position every scenario starts from. */
async function failAfterTwoPages(env, { expirePinnedRevision = false } = {}) {
  const run = await env.coordinator.createRun({ policy: 'pin', limit: PAGE_LIMIT });
  const first = await env.coordinator.advance(run.id, PAGES_BEFORE_FAILURE);
  if (first.run.emitted.pages !== PAGES_BEFORE_FAILURE) {
    throw new Error(`expected ${PAGES_BEFORE_FAILURE} delivered pages, got ${first.run.emitted.pages}`);
  }
  await env.sourceB.post('/admin/faults', { fail_next: 1 });
  const stopped = await env.coordinator.advance(run.id, 1);
  if (stopped.stopped !== 'failed') throw new Error('the injected B failure did not stop the run');

  // Both sources move on while the run is stopped.
  await env.sourceA.post('/admin/update', { count: 9 });
  const bumps = expirePinnedRevision ? 12 : 1;
  for (let i = 0; i < bumps; i += 1) await env.sourceB.post('/admin/bump', { count: 9 });

  return { run: env.store.getRun(run.id), stopped };
}

function pageVersions(descriptor) {
  return descriptor.pages.map((page) => ({
    page: page.pageNo,
    a: page.versions.a.token,
    b: page.versions.b.revision,
  }));
}

function distinctVersionSets(descriptor) {
  return new Set(descriptor.pages.map((page) => `${page.versions.a.token}/${page.versions.b.revision}`)).size;
}

async function measure(env, label, retry) {
  const started = Date.now();
  let retryError = null;
  let retried = null;
  try {
    retried = await retry();
  } catch (err) {
    retryError = { code: err.code, status: err.status, message: err.message, details: err.details || {} };
  }
  const finished = retryError ? null : await env.coordinator.advance(retried.run.id, 50);
  const run = env.store.getRun(retried ? retried.run.id : env.store.recentRuns()[0].id);
  const descriptor = env.coordinator.describeRun(run);
  const verification = retryError ? null : await env.coordinator.verify(run.id);
  return {
    label,
    elapsed_ms: Date.now() - started,
    retry_error: retryError,
    state: run.state,
    policy: descriptor.policy,
    epoch: descriptor.epoch,
    emitted: run.emitted,
    pages: pageVersions(descriptor),
    version_segments: descriptor.version_segments.length,
    distinct_version_sets: distinctVersionSets(descriptor),
    availability: verification
      ? {
        re_readable: verification.re_readable,
        verdict: verification.verdict,
        failed_checks: verification.availability.filter((c) => !c.ok).map((c) => c.name),
      }
      : null,
    verification: verification ? { ok: verification.ok, verdict: verification.verdict } : null,
    warnings: run.warnings.map((w) => w.code),
    version_loss: run.versionLoss,
    superseded_pages: descriptor.superseded_pages.map((p) => p.pageNo),
  };
}

async function scenarioPinRetained() {
  const env = await boot({ retention: 10 });
  try {
    const { run, stopped } = await failAfterTwoPages(env);
    const outcome = await measure(env, 'pin（版本仍可读）', () => env.coordinator.retry(run.id, { policy: 'pin' }));
    return {
      scenario: 'pin-retained',
      failure: stopped.error ? stopped.error.toJSON().error || stopped.error : null,
      before: { emitted: run.emitted, failure: run.failure },
      outcome,
    };
  } finally {
    await env.close();
  }
}

async function scenarioPinExpired() {
  const env = await boot({ retention: 10 });
  try {
    const { run, stopped } = await failAfterTwoPages(env, { expirePinnedRevision: true });
    const outcome = await measure(env, 'pin（修订已过期）', () => env.coordinator.retry(run.id, { policy: 'pin' }));
    const after = env.store.getRun(run.id);
    return {
      scenario: 'pin-expired',
      failure: stopped.error ? stopped.error.toJSON().error || stopped.error : null,
      before: { emitted: run.emitted, failure: run.failure },
      outcome,
      persisted: {
        state: after.state,
        emitted: after.emitted,
        version_loss: after.versionLoss,
        recommended: after.nextRetry.recommended,
        pin_possible: after.nextRetry.options.find((o) => o.policy === 'pin').possible,
      },
    };
  } finally {
    await env.close();
  }
}

async function scenarioRestart() {
  const env = await boot({ retention: 10 });
  try {
    const { run, stopped } = await failAfterTwoPages(env, { expirePinnedRevision: true });
    const outcome = await measure(env, 'restart', () => env.coordinator.retry(run.id, {
      policy: 'restart', acknowledge_invalidation: true,
    }));
    return {
      scenario: 'restart',
      failure: stopped.error ? stopped.error.toJSON().error || stopped.error : null,
      before: { emitted: run.emitted, failure: run.failure },
      outcome,
    };
  } finally {
    await env.close();
  }
}

async function scenarioMixed() {
  const env = await boot({ retention: 10 });
  try {
    const { run, stopped } = await failAfterTwoPages(env, { expirePinnedRevision: true });
    const retried = await env.coordinator.retry(run.id, { policy: 'mixed', acknowledge_mixed: true });
    const boundary = retried.run.warnings.find((w) => w.code === 'version_boundary') || null;
    const outcome = await measure(env, 'mixed', () => Promise.resolve(retried));
    return {
      scenario: 'mixed',
      failure: stopped.error ? stopped.error.toJSON().error || stopped.error : null,
      before: { emitted: run.emitted, failure: run.failure },
      boundary,
      outcome,
    };
  } finally {
    await env.close();
  }
}

function compareTable(results) {
  const rows = [];
  for (const result of results) {
    const o = result.outcome;
    const refused = Boolean(o.retry_error);
    rows.push({
      id: result.scenario,
      '策略/状态': o.label,
      '查询能否继续': refused ? `否（${o.retry_error.code}）` : '是',
      '最终状态': o.state,
      '交付页数': `${o.emitted.pages}/${TOTAL_PAGES}`,
      '交付行数': o.emitted.rows,
      '作废页数': o.superseded_pages.length,
      '版本段': o.version_segments,
      '不同版本组': o.distinct_version_sets,
      '校验结论': o.verification ? o.verification.verdict : '—（未交付新结果）',
      '承诺': CONTRACT_TEXT[o.policy].guarantee,
    });
  }
  return rows;
}

function printReport(results, table) {
  const line = (char = '─') => console.log(char.repeat(96));
  console.log('');
  console.log('  两个来源、三种重试口径：同一故障下的承诺与代价');
  line('═');
  console.log(`  实验条件：源 A 支持快照读取，源 B 只有修订号；共 ${TOTAL_PAGES} 页，每页 ${PAGE_LIMIT} 行。`);
  console.log(`  在交付 ${PAGES_BEFORE_FAILURE} 页后注入一次 B 读取失败，随后两个来源都继续更新/推进修订。`);
  console.log('  来源之间没有共享事务边界，因此这里的“版本”始终是一个读对 (A 快照, B 修订号)。');
  line();

  for (const result of results) {
    const o = result.outcome;
    console.log('');
    console.log(`  ▶ ${o.label}   [${result.scenario}]`);
    if (o.retry_error) {
      console.log(`    重试结果：拒绝 — ${o.retry_error.code}`);
      console.log(`    原因：    ${o.retry_error.message}`);
      const checks = (o.retry_error.details.checks || []).filter((c) => !c.ok);
      for (const check of checks) console.log(`              · ${check.source} ${check.name}: ${check.detail || 'failed'}`);
      console.log(`    已交付：  ${o.emitted.pages} 页 / ${o.emitted.rows} 行（保留在原位，未前进）`);
      console.log(`    结论：    固定版本这一口径在此状态下不可继续；必须显式选择 restart 或 mixed。`);
    } else {
      console.log(`    重试结果：接受 — 最终 ${o.state}，共 ${o.emitted.pages} 页 / ${o.emitted.rows} 行`);
      console.log(`    版本段：  ${o.version_segments} 段；每页声明的版本组数量：${o.distinct_version_sets}`);
      console.log(`    作废页：  ${o.superseded_pages.length ? o.superseded_pages.join(', ') : '无'}`);
      if (o.verification) {
        console.log(`    校验：    ${o.verification.verdict}（integrity ${o.verification.ok ? 'ok' : 'FAILED'}）`);
      }
      if (o.availability && !o.availability.re_readable) {
        console.log(`    可重读：  否（${o.availability.failed_checks.join(', ')}）；历史结果本身仍按完整性判定`);
      } else {
        console.log('    可重读：  是');
      }
      if (o.warnings.length) console.log(`    警告：    ${[...new Set(o.warnings)].join(', ')}`);
    }
    console.log(`    承诺：    ${CONTRACT_TEXT[o.policy].promise}`);
    console.log(`    代价：    ${CONTRACT_TEXT[o.policy].cost}`);
  }

  console.log('');
  line();
  console.log('  汇总');
  line();
  const columns = ['策略/状态', '查询能否继续', '最终状态', '交付页数', '交付行数', '作废页数', '版本段', '不同版本组', '校验结论'];
  const widths = columns.map((c) => Math.max(displayWidth(c), ...table.map((row) => displayWidth(String(row[c])))));
  const render = (cells) => cells.map((cell, i) => pad(cell, widths[i])).join('  ');
  console.log(`  ${render(columns)}`);
  console.log(`  ${widths.map((w) => '─'.repeat(w)).join('  ')}`);
  for (const row of table) console.log(`  ${render(columns.map((c) => String(row[c])))}`);
  line('═');
  console.log('  读法：');
  console.log('  · pin 的承诺是“已输出前缀精确来自一组版本”；代价是必须保持 B 修订可读，否则查询只能停下。');
  console.log('  · restart 的承诺是“最终只来自一组版本”；代价是已交付前缀全部作废，重新读取的行数 = 总量。');
  console.log('  · mixed 的承诺是“每个分页自报版本”；代价是跨分页（跨版本）比较会出现漂移，必须由消费方接受。');
  console.log('  · 三者都不能把两个来源放进同一个全局事务；“版本一致”只能是读对意义上的、可审计的一致。');
  line('═');
  console.log('');
}

function displayWidth(text) {
  let width = 0;
  for (const char of text) width += /[　-鿿＀-￯]/.test(char) ? 2 : 1;
  return width;
}

function pad(text, width) {
  const missing = width - displayWidth(text);
  return missing > 0 ? text + ' '.repeat(missing) : text;
}

function assertInvariants(results) {
  const failures = [];
  const byId = new Map(results.map((result) => [result.scenario, result]));

  const retained = byId.get('pin-retained');
  if (!retained.outcome.verification || retained.outcome.verification.verdict !== 'verified') {
    failures.push('pin-retained: expected a fully verified single-version result');
  }
  if (retained.outcome.distinct_version_sets !== 1) {
    failures.push('pin-retained: pages must share exactly one version set');
  }

  const expired = byId.get('pin-expired');
  if (!expired.outcome.retry_error || expired.outcome.retry_error.code !== 'pinned_version_unavailable') {
    failures.push('pin-expired: pin must refuse rather than silently rebind');
  }
  if (expired.outcome.emitted.pages !== PAGES_BEFORE_FAILURE) {
    failures.push('pin-expired: refusal must not advance the delivered prefix');
  }
  if (!expired.persisted.version_loss || expired.persisted.version_loss.source !== 'B') {
    failures.push('pin-expired: version loss must be recorded with the affected source');
  }

  const restart = byId.get('restart');
  if (restart.outcome.state !== 'completed' || restart.outcome.distinct_version_sets !== 1) {
    failures.push('restart: final result must be complete and come from one version set');
  }
  if (restart.outcome.superseded_pages.length !== PAGES_BEFORE_FAILURE) {
    failures.push('restart: the delivered prefix must be explicitly superseded');
  }

  const mixed = byId.get('mixed');
  if (!mixed.outcome.verification || mixed.outcome.verification.verdict !== 'verified_mixed') {
    failures.push('mixed: expected integrity-verified output with a declared version boundary');
  }
  if (mixed.outcome.version_segments !== 2) failures.push('mixed: expected exactly two recorded version segments');
  if (mixed.outcome.distinct_version_sets < 2) failures.push('mixed: expected pages to carry different version sets');
  if (!mixed.boundary || mixed.boundary.boundary_page !== PAGES_BEFORE_FAILURE + 1) {
    failures.push('mixed: boundary must start at the first page after the prefix');
  }
  return failures;
}

async function main() {
  const jsonIndex = process.argv.indexOf('--json');
  const jsonPath = jsonIndex >= 0 ? process.argv[jsonIndex + 1] : null;

  const results = [];
  results.push(await scenarioPinRetained());
  results.push(await scenarioPinExpired());
  results.push(await scenarioRestart());
  results.push(await scenarioMixed());

  const table = compareTable(results);
  printReport(results, table);

  const failures = assertInvariants(results);
  if (failures.length) {
    console.error('  不变量检查失败：');
    for (const failure of failures) console.error(`   ✖ ${failure}`);
    process.exitCode = 1;
  } else {
    console.log('  ✓ 不变量检查全部通过：拒绝不前进，重启全量作废，混合有显式边界。');
    console.log('');
  }

  if (jsonPath) {
    const payload = { generated_at: new Date().toISOString(), total_pages: TOTAL_PAGES, page_limit: PAGE_LIMIT, results, table };
    fs.writeFileSync(path.resolve(jsonPath), `${JSON.stringify(payload, null, 2)}\n`);
    console.log(`  原始测量已写入 ${path.resolve(jsonPath)}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
