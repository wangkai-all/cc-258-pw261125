'use strict';

const test = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { startSourceA, startSourceB } = require('../src/sources/start-lib');
const { Store } = require('../src/coordinator/store');
const { SourceClient } = require('../src/coordinator/source-client');
const { QueryCoordinator, POLICIES, pageDigest } = require('../src/coordinator/coordinator');

async function boot({ retention = 10 } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'coord-'));
  const a = await startSourceA({ port: 0, dataDir: dir });
  const b = await startSourceB({ port: 0, dataDir: dir, retention });
  const store = new Store(path.join(dir, 'coordinator.sqlite'));
  const sourceA = new SourceClient({ baseUrl: `http://127.0.0.1:${a.port}`, name: 'A' });
  const sourceB = new SourceClient({ baseUrl: `http://127.0.0.1:${b.port}`, name: 'B' });
  const coordinator = new QueryCoordinator({ store, sourceA, sourceB });
  const close = () => { store.close(); a.close(); b.close(); };
  return { dir, a, b, store, sourceA, sourceB, coordinator, close };
}

async function adminA(env, path, body = {}) {
  return env.sourceA.post(`/admin/${path}`, body);
}
async function adminB(env, path, body = {}) {
  return env.sourceB.post(`/admin/${path}`, body);
}

test('pin: a full run reads every page from one (A snapshot, B revision) pair', async (t) => {
  const env = await boot();
  t.after(env.close);

  const run = await env.coordinator.createRun({ policy: 'pin', limit: 50 });
  assert.equal(run.state, 'bound');
  assert.equal(run.binding.a.token, (await env.sourceA.get('/v1/snapshot')).snapshot.token);

  const result = await env.coordinator.advance(run.id, 50);
  assert.equal(result.run.state, 'completed');
  assert.equal(result.run.emitted.rows, 240);
  assert.equal(result.run.emitted.pages, 5);

  // Mutate both sources mid-way is impossible here (run already done), so prove
  // the pages really carry the bound versions.
  const described = env.coordinator.describeRun(result.run);
  for (const page of described.pages) {
    assert.equal(page.versions.a.token, run.binding.a.token);
    assert.equal(page.versions.b.revision, run.binding.b.revision);
  }

  const verification = await env.coordinator.verify(run.id);
  assert.equal(verification.ok, true);
  assert.equal(verification.verdict, 'verified');
  assert.equal(verification.mixed, false);
  assert.ok(verification.integrity.every((c) => c.ok), 'no integrity failures');
});

test('pin: a source failure stops at the failure position and never emits a partial page', async (t) => {
  const env = await boot();
  t.after(env.close);

  const run = await env.coordinator.createRun({ policy: 'pin', limit: 40 });
  await env.coordinator.advance(run.id, 2);
  const before = env.store.getRun(run.id);
  assert.equal(before.emitted.pages, 2);

  await adminB(env, 'faults', { fail_next: 1 });
  const result = await env.coordinator.advance(run.id, 1);
  assert.equal(result.stopped, 'failed');

  const failed = env.store.getRun(run.id);
  assert.equal(failed.state, 'failed');
  assert.equal(failed.emitted.pages, 2, 'failure must not advance the emitted cursor');
  assert.equal(failed.failure.pageNo, 3);
  assert.equal(failed.failure.source, 'B');
  assert.equal(failed.failure.cursor.after, before.cursor.after);
  assert.equal(failed.failure.emitted_pages, 2);

  // No page 3 material may exist: a page is committed whole or not at all.
  assert.equal(env.store.getPages(run.id, 1).some((p) => p.pageNo === 3), false);

  const verification = await env.coordinator.verify(run.id);
  assert.equal(verification.ok, true, 'the delivered prefix is still self-consistent');
  assert.equal(verification.complete, false);
  assert.equal(verification.verdict, 'prefix_verified');
});

test('pin retry: continues only after both versions verify as unchanged', async (t) => {
  const env = await boot();
  t.after(env.close);

  const run = await env.coordinator.createRun({ policy: 'pin', limit: 40 });
  await env.coordinator.advance(run.id, 1);
  await adminA(env, 'faults', { fail_next: 1 });
  const stopped = await env.coordinator.advance(run.id, 1);
  assert.equal(stopped.stopped, 'failed');

  // Someone updates both sources while the run is stopped.
  await adminA(env, 'update', { count: 9 });
  await adminB(env, 'bump', { count: 7 });

  const retried = await env.coordinator.retry(run.id, { policy: 'pin' });
  assert.equal(retried.verification.ok, true, 'pinned versions are still retained');
  assert.equal(retried.run.binding.a.token, run.binding.a.token);
  assert.equal(retried.run.binding.b.revision, run.binding.b.revision);

  const finished = await env.coordinator.advance(run.id, 20);
  assert.equal(finished.run.state, 'completed');
  const described = env.coordinator.describeRun(finished.run);
  assert.equal(described.version_segments.length, 1);
  for (const page of described.pages) {
    assert.equal(page.versions.a.token, run.binding.a.token);
    assert.equal(page.versions.b.revision, run.binding.b.revision);
  }
});

test('pin retry: refuses once a pinned revision leaves the retention window', async (t) => {
  const env = await boot({ retention: 3 });
  t.after(env.close);

  const run = await env.coordinator.createRun({ policy: 'pin', limit: 30 });
  await env.coordinator.advance(run.id, 1);
  await adminB(env, 'faults', { fail_next: 1 });
  assert.equal((await env.coordinator.advance(run.id, 1)).stopped, 'failed');

  for (let i = 0; i < 4; i += 1) await adminB(env, 'bump', { count: 4 });

  await assert.rejects(
    () => env.coordinator.retry(run.id, { policy: 'pin' }),
    (err) => {
      assert.equal(err.code, 'pinned_version_unavailable');
      assert.equal(err.details.checks.some((c) => c.name === 'revision_retained' && !c.ok), true);
      return true;
    },
  );
  const after = env.store.getRun(run.id);
  assert.equal(after.state, 'failed');
  assert.equal(after.versionLoss.source, 'B');
  assert.equal(after.nextRetry.options.find((o) => o.policy === 'pin').possible, false);
  assert.equal(after.nextRetry.recommended, 'mixed');
});

test('restart: invalidates the delivered prefix, bumps epoch and starts from page 1', async (t) => {
  const env = await boot();
  t.after(env.close);

  const run = await env.coordinator.createRun({ policy: 'pin', limit: 40 });
  await env.coordinator.advance(run.id, 2);
  await adminA(env, 'faults', { fail_next: 1 });
  assert.equal((await env.coordinator.advance(run.id, 1)).stopped, 'failed');

  await adminA(env, 'update', { count: 5 });
  await adminB(env, 'bump', { count: 5 });

  await assert.rejects(
    () => env.coordinator.retry(run.id, { policy: 'restart' }),
    (err) => err.code === 'acknowledgement_required' && err.details.would_invalidate.from_page === 1,
  );

  const retried = await env.coordinator.retry(run.id, { policy: 'restart', acknowledge_invalidation: true });
  assert.equal(retried.run.epoch, 2);
  assert.equal(retried.run.emitted.pages, 0);
  assert.equal(retried.run.cursor.pageNo, 1);
  assert.deepEqual(retried.invalidated.pages, { from_page: 1, to_page: 2 });
  assert.notEqual(retried.run.binding.a.token, run.binding.a.token, 'restart rebinds A');
  assert.notEqual(retried.run.binding.b.revision, run.binding.b.revision, 'restart rebinds B');

  const superseded = env.coordinator.describeRun(retried.run).superseded_pages;
  assert.deepEqual(superseded.map((p) => p.pageNo), [1, 2]);
  assert.equal(superseded.every((p) => p.epoch === 1), true);

  const finished = await env.coordinator.advance(run.id, 20);
  assert.equal(finished.run.state, 'completed');
  const verification = await env.coordinator.verify(run.id);
  assert.equal(verification.ok, true);
  assert.equal(verification.verdict, 'verified', 'only the new epoch is verifiable');
});

test('mixed: keeps the delivered prefix, rebinds only the failed source and records a boundary', async (t) => {
  const env = await boot();
  t.after(env.close);

  const run = await env.coordinator.createRun({ policy: 'pin', limit: 40 });
  await env.coordinator.advance(run.id, 2);
  await adminB(env, 'faults', { fail_next: 1 });
  assert.equal((await env.coordinator.advance(run.id, 1)).stopped, 'failed');

  const snapshotBefore = run.binding.a.token;
  await adminB(env, 'bump', { count: 11 });

  await assert.rejects(
    () => env.coordinator.retry(run.id, { policy: 'mixed' }),
    (err) => err.code === 'acknowledgement_required',
  );

  const retried = await env.coordinator.retry(run.id, { policy: 'mixed', acknowledge_mixed: true });
  assert.equal(retried.run.binding.a.token, snapshotBefore, 'A is untouched: only the failed source rebinds');
  assert.notEqual(retried.run.binding.b.revision, run.binding.b.revision);
  assert.equal(retried.run.emitted.pages, 2, 'prefix is kept');
  assert.equal(retried.run.request.policy, 'mixed');
  assert.ok(retried.run.warnings.some((w) => w.code === 'version_boundary'), 'boundary must be recorded');
  assert.equal(retried.contract_change.from, 'pin');
  assert.equal(retried.contract_change.to, 'mixed');
  assert.equal(retried.contract_change.visible_to_consumer, true);

  const finished = await env.coordinator.advance(run.id, 20);
  assert.equal(finished.run.state, 'completed');
  const described = env.coordinator.describeRun(finished.run);
  assert.equal(described.version_segments.length, 2);

  // Pages before the boundary keep the old revision; pages after use the new one.
  const firstTwo = described.pages.filter((p) => p.pageNo <= 2);
  const rest = described.pages.filter((p) => p.pageNo > 2);
  assert.equal(firstTwo.every((p) => p.versions.b.revision === run.binding.b.revision), true);
  assert.equal(rest.every((p) => p.versions.b.revision === retried.run.binding.b.revision), true);

  const verification = await env.coordinator.verify(run.id);
  assert.equal(verification.ok, true);
  assert.equal(verification.verdict, 'verified_mixed');
  assert.equal(verification.mixed, true);
  assert.ok(verification.integrity.find((c) => c.name === 'page_versions_match_segments').ok);
});

test('mixed: cannot be used when no failure position names a source', async (t) => {
  const env = await boot();
  t.after(env.close);
  const run = await env.coordinator.createRun({ policy: 'mixed', limit: 40 });
  await env.coordinator.advance(run.id, 1);
  await env.coordinator.cancel(run.id);
  await assert.rejects(
    () => env.coordinator.retry(run.id, { policy: 'mixed', acknowledge_mixed: true }),
    (err) => err.code === 'mixed_requires_failure',
  );
});

test('cancel and resume: resume keeps the exact binding and cursor', async (t) => {
  const env = await boot();
  t.after(env.close);

  const run = await env.coordinator.createRun({ policy: 'pin', limit: 30 });
  await env.coordinator.advance(run.id, 2);
  const cancelled = await env.coordinator.cancel(run.id, 'test');
  assert.equal(cancelled.state, 'cancelled');
  assert.equal(cancelled.cursor.pageNo, 3);

  // Sources move on while cancelled; resume must still use the old versions.
  await adminA(env, 'update', { count: 6 });
  await adminB(env, 'bump', { count: 6 });

  const resumed = await env.coordinator.resume(run.id);
  assert.equal(resumed.run.state, 'running');
  assert.equal(resumed.run.binding.a.token, run.binding.a.token);
  assert.equal(resumed.run.binding.b.revision, run.binding.b.revision);
  assert.equal(resumed.run.cursor.pageNo, 3);
  assert.equal(resumed.run.emitted.pages, 2);

  const finished = await env.coordinator.advance(run.id, 20);
  assert.equal(finished.run.state, 'completed');
  const described = env.coordinator.describeRun(finished.run);
  assert.equal(described.version_segments.length, 1);
  assert.equal(verifiedVersions(described.pages, run.binding), true);
});

function verifiedVersions(pages, binding) {
  return pages.every((p) => p.versions.a.token === binding.a.token && p.versions.b.revision === binding.b.revision);
}

test('resume: refuses once the pinned revision expired, instead of silently rebinding', async (t) => {
  const env = await boot({ retention: 2 });
  t.after(env.close);

  const run = await env.coordinator.createRun({ policy: 'pin', limit: 30 });
  await env.coordinator.advance(run.id, 1);
  await env.coordinator.cancel(run.id);
  for (let i = 0; i < 3; i += 1) await adminB(env, 'bump', { count: 3 });

  await assert.rejects(
    () => env.coordinator.resume(run.id),
    (err) => err.code === 'pinned_version_unavailable',
  );
  const after = env.store.getRun(run.id);
  assert.equal(after.state, 'cancelled');
  assert.equal(after.versionLoss.source, 'B');
});

test('a page digest is recomputable from the page body alone', async (t) => {
  const env = await boot();
  t.after(env.close);
  const run = await env.coordinator.createRun({ policy: 'pin', limit: 25 });
  await env.coordinator.advance(run.id, 2);
  for (const record of env.store.getPages(run.id, 1)) {
    assert.equal(pageDigest(record.body), record.digest);
    assert.equal(record.digest, record.body.page_digest);
  }
});

test('a tampered page body fails verification', async (t) => {
  const env = await boot();
  t.after(env.close);
  const run = await env.coordinator.createRun({ policy: 'pin', limit: 25 });
  await env.coordinator.advance(run.id, 2);
  const pages = env.store.getPages(run.id, 1);
  const tampered = JSON.parse(JSON.stringify(pages[0].body));
  tampered.rows[0].balance_cents += 1;
  env.store.savePage(run.id, 1, tampered.pageNo, tampered, pages[0].digest);
  const verification = await env.coordinator.verify(run.id);
  assert.equal(verification.ok, false);
  assert.equal(verification.verdict, 'integrity_failed');
  const failed = verification.integrity.find((c) => !c.ok);
  assert.match(failed.name, /page_digest_1/);
});

test('run state survives a coordinator restart', async (t) => {
  const env = await boot();
  const run = await env.coordinator.createRun({ policy: 'pin', limit: 40 });
  await env.coordinator.advance(run.id, 2);
  await adminB(env, 'faults', { fail_next: 1 });
  await env.coordinator.advance(run.id, 1);

  // Simulate a process restart: close the coordinator store and source A,
  // then reopen both from the same data directory (B stays up).
  const dir = env.dir;
  const bPort = env.b.port;
  env.store.close();
  await new Promise((resolve) => env.a.server.close(resolve));

  const { startSourceA } = require('../src/sources/start-lib');
  let a2 = null;
  let store2 = null;
  t.after(() => { if (store2) store2.close(); if (a2) a2.close(); env.b.close(); });
  a2 = await startSourceA({ port: 0, dataDir: dir });
  store2 = new Store(path.join(dir, 'coordinator.sqlite'));
  const coordinator2 = new QueryCoordinator({
    store: store2,
    sourceA: new SourceClient({ baseUrl: `http://127.0.0.1:${a2.port}`, name: 'A' }),
    sourceB: new SourceClient({ baseUrl: `http://127.0.0.1:${bPort}`, name: 'B' }),
  });

  const restored = store2.getRun(run.id);
  assert.equal(restored.state, 'failed');
  assert.equal(restored.failure.pageNo, 3);
  assert.equal(restored.emitted.pages, 2);
  assert.equal(restored.binding.a.token, run.binding.a.token);

  const retried = await coordinator2.retry(run.id, { policy: 'pin' });
  assert.equal(retried.verification.ok, true);
  const finished = await coordinator2.advance(run.id, 20);
  assert.equal(finished.run.state, 'completed');
  // The old store handle is closed; the reopened one carries the whole run.
  assert.equal(store2.getRun(run.id).state, 'completed');
  const verification = await coordinator2.verify(run.id);
  assert.equal(verification.ok, true);
});

test('two concurrent steps never emit the same page twice', async (t) => {
  const env = await boot();
  t.after(env.close);
  const run = await env.coordinator.createRun({ policy: 'pin', limit: 30 });
  const results = await Promise.all([
    env.coordinator.step(run.id),
    env.coordinator.step(run.id),
    env.coordinator.step(run.id),
  ]);
  const pageNos = results.map((r) => r.page && r.page.pageNo).filter(Boolean);
  assert.deepEqual(pageNos, [1, 2, 3]);
  const record = env.store.getRun(run.id);
  assert.equal(record.emitted.pages, 3);
  assert.equal(record.emitted.rows, 90);
});

test('cross-run page cache reuses material only under an identical binding', async (t) => {
  const env = await boot();
  t.after(env.close);

  const first = await env.coordinator.createRun({ policy: 'pin', limit: 40 });
  await env.coordinator.advance(first.id, 2);
  const firstPages = env.store.getPages(first.id, 1);

  // Same sources, same filter → identical binding, so the cache may reuse pages.
  const second = await env.coordinator.createRun({ policy: 'pin', limit: 40 });
  assert.equal(second.binding.digest, first.binding.digest);
  const stepped = await env.coordinator.step(second.id);
  assert.equal(stepped.page.cache.hit, true);
  assert.equal(stepped.page.cache.kind, 'cross-run');
  assert.equal(stepped.page.page_digest, firstPages[0].digest);

  // A different filter must not hit.
  const third = await env.coordinator.createRun({ policy: 'pin', limit: 40, filters: { region: 'EU' } });
  const thirdStep = await env.coordinator.step(third.id);
  assert.equal(thirdStep.page.cache.hit, false);
});

test('cache entries with a mismatched version envelope are rejected, not reused', async (t) => {
  const env = await boot();
  t.after(env.close);

  const run = await env.coordinator.createRun({ policy: 'pin', limit: 40 });
  await env.coordinator.advance(run.id, 1);

  // Poison every cache row: keep the key, replace the versions envelope.
  const rows = env.store.db.prepare('SELECT cache_key FROM page_cache').all();
  for (const row of rows) {
    env.store.db.prepare('UPDATE page_cache SET versions_json = ? WHERE cache_key = ?')
      .run(JSON.stringify({ a: { token: 'snap-forged' }, b: { revision: 99 }, binding_digest: 'forged' }), row.cache_key);
  }

  const second = await env.coordinator.createRun({ policy: 'pin', limit: 40 });
  const stepped = await env.coordinator.step(second.id);
  assert.equal(stepped.page.cache.hit, false, 'poisoned entry must not be served');
  const afterStep = env.store.getRun(second.id);
  assert.equal(afterStep.cache.mismatch_rejections, 1);
  assert.ok(afterStep.warnings.some((w) => w.code === 'cache_entry_rejected'));
  // The poisoned row is gone; any row left must have been rewritten from a real read.
  const leftovers = env.store.db.prepare('SELECT versions_json FROM page_cache').all();
  assert.equal(leftovers.some((r) => r.versions_json.includes('snap-forged')), false);
});

test('filters create distinct filtered-set digests and filtered row counts', async (t) => {
  const env = await boot();
  t.after(env.close);

  const eu = await env.coordinator.createRun({ policy: 'pin', limit: 200, filters: { region: 'EU' } });
  const done = await env.coordinator.advance(eu.id, 5);
  assert.equal(done.run.state, 'completed');
  assert.ok(done.run.emitted.rows < 240);
  assert.equal(done.run.emitted.rows, done.run.sourceA.filter_total);
  const verification = await env.coordinator.verify(eu.id);
  assert.equal(verification.ok, true);
  const count = verification.integrity.find((c) => c.name === 'filtered_row_count');
  assert.equal(count.actual, count.expected);
});

test('left join keeps accounts without B activity and flags them', async (t) => {
  const env = await boot();
  t.after(env.close);
  const run = await env.coordinator.createRun({ policy: 'pin', limit: 200, join: 'left' });
  const done = await env.coordinator.advance(run.id, 5);
  const page = env.store.getPages(run.id, 1)[0].body;
  assert.equal(page.rows.length, page.a.row_count);
  assert.ok(page.rows.every((row) => typeof row.activity_present === 'boolean'));
  assert.equal(done.run.state, 'completed');
});

test('policy is immutable without acknowledgement and recorded on the run', async (t) => {
  const env = await boot();
  t.after(env.close);
  const run = await env.coordinator.createRun({ policy: 'restart', limit: 40 });
  assert.equal(run.request.policy, 'restart');
  assert.match(run.contract.guarantee, /single-version-set/);
  await assert.rejects(
    () => env.coordinator.createRun({ policy: 'nonsense', limit: 40 }),
    (err) => err.code === 'invalid_policy',
  );
});

test('failed first bind is retryable and records the failure position', async (t) => {
  const env = await boot();
  t.after(env.close);
  await adminA(env, 'faults', { fail_next: 1 });
  await assert.rejects(
    () => env.coordinator.createRun({ policy: 'pin', limit: 40 }),
    (err) => err.code === 'bind_failed' && Boolean(err.details.run_id),
  );
  const runId = env.store.recentRuns()[0].id;
  const state = env.store.getRun(runId);
  assert.equal(state.state, 'failed');
  assert.equal(state.failure.phase, 'bind');
  assert.equal(state.emitted.pages, 0);
  const retried = await env.coordinator.retry(runId, { policy: 'pin' });
  assert.equal(retried.run.state, 'bound');
});
