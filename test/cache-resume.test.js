'use strict';

/**
 * HTTP-level contract tests. These drive the same REST API the page uses, so
 * they prove the promise survives the wire, not just the in-process object:
 * cancel, resume, cache hits, poisoned cache entries and retry after failure.
 */

const test = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { startSourceA, startSourceB } = require('../src/sources/start-lib');
const { createQueryServer } = require('../src/coordinator/server');

async function boot({ retention = 10 } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'http-'));
  const a = await startSourceA({ port: 0, dataDir: dir });
  const b = await startSourceB({ port: 0, dataDir: dir, retention });
  const query = await createQueryServer({
    dbPath: path.join(dir, 'coordinator.sqlite'),
    sourceAUrl: `http://127.0.0.1:${a.port}`,
    sourceBUrl: `http://127.0.0.1:${b.port}`,
  });
  const port = await query.listen(0);
  const base = `http://127.0.0.1:${port}`;
  const close = async () => {
    await new Promise((resolve) => query.server.close(resolve));
    query.store.close();
    a.close();
    b.close();
  };
  return { dir, base, a, b, query, close };
}

async function api(env, method, route, body) {
  const response = await fetch(`${env.base}${route}`, {
    method,
    headers: body === undefined ? undefined : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const json = await response.json();
  return { status: response.status, body: json };
}

async function sourceAdmin(env, source, action, body = {}) {
  return api(env, 'POST', `/api/sources/${source}/admin/${action}`, body);
}

test('http: create, step and describe expose per-source versions and emitted range', async (t) => {
  const env = await boot();
  t.after(env.close);

  const created = await api(env, 'POST', '/api/runs', { policy: 'pin', limit: 40 });
  assert.equal(created.status, 201);
  const run = created.body.run;
  assert.equal(run.policy, 'pin');
  assert.match(run.contract.promise, /同一组/);
  assert.equal(run.source_a.filter_digest, null);
  assert.equal(run.emitted.pages, 0);
  const pinnedA = run.binding.a.token;
  const pinnedB = run.binding.b.revision;

  const stepped = await api(env, 'POST', `/api/runs/${run.id}/step`, { pages: 2 });
  assert.equal(stepped.status, 200);
  assert.equal(stepped.body.pages.length, 2);
  const after = stepped.body.run;
  assert.equal(after.emitted.pages, 2);
  assert.equal(after.emitted.first_page, 1);
  assert.equal(after.emitted.last_page, 2);
  assert.equal(after.cursor.pageNo, 3);
  assert.equal(after.binding.a.token, pinnedA);
  assert.equal(after.binding.b.revision, pinnedB);
  for (const page of after.pages) {
    assert.equal(page.versions.a.token, pinnedA);
    assert.equal(page.versions.b.revision, pinnedB);
  }
  assert.equal(after.pages[0].account_range.from, 'ACC-0001');
  assert.equal(after.pages[1].account_range.to, 'ACC-0080');
});

test('http: cancel takes effect at a page boundary and resume keeps the same versions', async (t) => {
  const env = await boot();
  t.after(env.close);

  const run = (await api(env, 'POST', '/api/runs', { policy: 'pin', limit: 30 })).body.run;
  await api(env, 'POST', `/api/runs/${run.id}/step`, { pages: 2 });

  const cancelled = await api(env, 'POST', `/api/runs/${run.id}/cancel`, { reason: 'test cancel' });
  assert.equal(cancelled.status, 200);
  assert.equal(cancelled.body.run.state, 'cancelled');
  assert.equal(cancelled.body.run.emitted.pages, 2);
  assert.equal(cancelled.body.run.cursor.pageNo, 3);

  // A cancelled run cannot advance: advance() reports a soft stop, pages stay put.
  const steppedWhileCancelled = await api(env, 'POST', `/api/runs/${run.id}/step`, { pages: 1 });
  assert.equal(steppedWhileCancelled.status, 200);
  assert.equal(steppedWhileCancelled.body.stopped, 'cancelled');
  assert.equal(steppedWhileCancelled.body.pages.length, 0);
  assert.equal(steppedWhileCancelled.body.run.emitted.pages, 2);

  // Sources move on while the run is cancelled. Resume must not adopt them.
  await sourceAdmin(env, 'a', 'update', { count: 9 });
  for (let i = 0; i < 6; i += 1) await sourceAdmin(env, 'b', 'bump', { count: 9 });

  const resumed = await api(env, 'POST', `/api/runs/${run.id}/resume`);
  assert.equal(resumed.status, 200);
  assert.equal(resumed.body.run.state, 'running');
  assert.equal(resumed.body.run.binding.a.token, run.binding.a.token);
  assert.equal(resumed.body.run.binding.b.revision, run.binding.b.revision);

  const finished = await api(env, 'POST', `/api/runs/${run.id}/step`, { pages: 20 });
  assert.equal(finished.body.run.state, 'completed');
  for (const page of finished.body.run.pages) {
    assert.equal(page.versions.a.token, run.binding.a.token);
    assert.equal(page.versions.b.revision, run.binding.b.revision);
  }
  const verification = await api(env, 'GET', `/api/runs/${run.id}/verify`);
  assert.equal(verification.body.verdict, 'verified');
});

test('http: resume refuses when a pinned version expired, and retry says why', async (t) => {
  const env = await boot({ retention: 2 });
  t.after(env.close);

  const run = (await api(env, 'POST', '/api/runs', { policy: 'pin', limit: 30 })).body.run;
  await api(env, 'POST', `/api/runs/${run.id}/step`, { pages: 1 });
  await api(env, 'POST', `/api/runs/${run.id}/cancel`, {});
  for (let i = 0; i < 4; i += 1) await sourceAdmin(env, 'b', 'bump', { count: 9 });

  const refused = await api(env, 'POST', `/api/runs/${run.id}/resume`);
  assert.equal(refused.status, 409);
  assert.equal(refused.body.error.code, 'pinned_version_unavailable');
  assert.equal(refused.body.error.details.checks.some((c) => c.source === 'B' && !c.ok), true);

  // The refusal is persisted with an explicit version loss, not hidden in a log.
  const described = (await api(env, 'GET', `/api/runs/${run.id}`)).body.run;
  assert.equal(described.state, 'cancelled');
  assert.equal(described.version_loss.source, 'B');
  assert.equal(described.next_retry.options.find((o) => o.policy === 'pin').possible, false);
  assert.equal(described.next_retry.recommended, 'mixed');

  // Mixed keeps the prefix but must be acknowledged; then it declares the boundary.
  const unacknowledged = await api(env, 'POST', `/api/runs/${run.id}/retry`, { policy: 'mixed' });
  assert.equal(unacknowledged.status, 409);
  assert.equal(unacknowledged.body.error.code, 'acknowledgement_required');
  const mixed = await api(env, 'POST', `/api/runs/${run.id}/retry`, { policy: 'mixed', acknowledge_mixed: true });
  assert.equal(mixed.status, 200);
  assert.equal(mixed.body.contract_change.from, 'pin');
  assert.equal(mixed.body.contract_change.to, 'mixed');
  assert.equal(mixed.body.run.emitted.pages, 1, 'prefix survives the version boundary');
  assert.equal(mixed.body.run.binding.a.token, run.binding.a.token);
  assert.notEqual(mixed.body.run.binding.b.revision, run.binding.b.revision);
});

test('http: retry guidance is re-probed after the sources move, without changing the run', async (t) => {
  const env = await boot({ retention: 4 });
  t.after(env.close);

  const run = (await api(env, 'POST', '/api/runs', { policy: 'pin', limit: 30 })).body.run;
  await api(env, 'POST', `/api/runs/${run.id}/step`, { pages: 2 });
  await sourceAdmin(env, 'b', 'faults', { fail_next: 1 });
  await api(env, 'POST', `/api/runs/${run.id}/step`, { pages: 1 });

  const before = (await api(env, 'GET', `/api/runs/${run.id}`)).body.run;
  assert.equal(before.version_loss, null);
  assert.equal(before.next_retry.options.find((o) => o.policy === 'pin').possible, true);

  // The page refreshes guidance right after the operator advances B; the run
  // itself must stay stopped at the same page with the same delivered prefix.
  for (let i = 0; i < 5; i += 1) await sourceAdmin(env, 'b', 'bump', { count: 9 });
  const refreshed = await api(env, 'POST', `/api/runs/${run.id}/retry-guidance`);
  assert.equal(refreshed.status, 200);
  assert.equal(refreshed.body.re_readable, false);
  assert.equal(refreshed.body.verification.checks.some((c) => c.code === 'revision_expired'), true);
  assert.equal(refreshed.body.run.state, 'failed');
  assert.equal(refreshed.body.run.emitted.pages, 2);
  assert.equal(refreshed.body.run.failure.pageNo, 3);
  assert.equal(refreshed.body.run.version_loss.source, 'B');
  assert.equal(refreshed.body.run.next_retry.options.find((o) => o.policy === 'pin').possible, false);
  assert.equal(refreshed.body.run.next_retry.recommended, 'mixed');

  const again = await api(env, 'POST', `/api/runs/${run.id}/retry-guidance`);
  assert.equal(again.body.run.version_loss.at, refreshed.body.run.version_loss.at, 'loss is recorded once');

  const completed = (await api(env, 'POST', '/api/runs', { policy: 'pin', limit: 40 })).body.run;
  await api(env, 'POST', `/api/runs/${completed.id}/step`, { pages: 20 });
  const notRetryable = await api(env, 'POST', `/api/runs/${completed.id}/retry-guidance`);
  assert.equal(notRetryable.status, 409);
  assert.equal(notRetryable.body.error.code, 'run_not_retryable');
});

test('http: cache hits carry the same page digest and never cross bindings', async (t) => {
  const env = await boot();
  t.after(env.close);

  const first = (await api(env, 'POST', '/api/runs', { policy: 'pin', limit: 40 })).body.run;
  const stepped = await api(env, 'POST', `/api/runs/${first.id}/step`, { pages: 2 });
  assert.equal(stepped.body.pages.every((p) => p.cache.hit === false), true);
  const original = stepped.body.run.pages.map((p) => p.digest);

  // Same read pair, same filter -> identical binding; the cache is legal here.
  const second = (await api(env, 'POST', '/api/runs', { policy: 'pin', limit: 40 })).body.run;
  assert.equal(second.binding.digest, first.binding.digest);
  const replay = await api(env, 'POST', `/api/runs/${second.id}/step`, { pages: 2 });
  assert.equal(replay.body.pages.every((p) => p.cache.hit === true), true);
  assert.equal(replay.body.pages.every((p) => p.cache.kind === 'cross-run'), true);
  assert.deepEqual(replay.body.run.pages.map((p) => p.digest), original);
  const stats = (await api(env, 'GET', '/api/runs')).body.cache;
  assert.ok(stats.hits >= 2);

  // Different filter => different binding => no hit, and a distinct page digest.
  const third = (await api(env, 'POST', '/api/runs', { policy: 'pin', limit: 40, filters: { plan: 'team' } })).body.run;
  const different = await api(env, 'POST', `/api/runs/${third.id}/step`, { pages: 1 });
  assert.equal(different.body.pages[0].cache.hit, false);
  assert.notEqual(different.body.pages[0].page_digest, original[0]);
});

test('http: a poisoned cache row is deleted, counted, and never served', async (t) => {
  const env = await boot();
  t.after(env.close);

  const first = (await api(env, 'POST', '/api/runs', { policy: 'pin', limit: 40 })).body.run;
  await api(env, 'POST', `/api/runs/${first.id}/step`, { pages: 1 });

  const rows = env.query.store.db.prepare('SELECT cache_key FROM page_cache').all();
  assert.ok(rows.length > 0);
  for (const row of rows) {
    env.query.store.db.prepare('UPDATE page_cache SET versions_json = ? WHERE cache_key = ?')
      .run(JSON.stringify({ a: { token: 'snap-forged' }, b: { revision: 99 }, binding_digest: 'forged' }), row.cache_key);
  }

  const second = (await api(env, 'POST', '/api/runs', { policy: 'pin', limit: 40 })).body.run;
  const stepped = await api(env, 'POST', `/api/runs/${second.id}/step`, { pages: 1 });
  assert.equal(stepped.body.pages[0].cache.hit, false);
  assert.equal(stepped.body.run.cache.mismatch_rejections, 1);
  assert.ok(stepped.body.run.warnings.some((w) => w.code === 'cache_entry_rejected'));
  const leftovers = env.query.store.db.prepare('SELECT versions_json FROM page_cache').all();
  assert.equal(leftovers.some((r) => r.versions_json.includes('snap-forged')), false);
});

test('http: restart requires acknowledgement and moves the run to a fresh epoch', async (t) => {
  const env = await boot();
  t.after(env.close);

  const run = (await api(env, 'POST', '/api/runs', { policy: 'pin', limit: 40 })).body.run;
  await api(env, 'POST', `/api/runs/${run.id}/step`, { pages: 2 });
  await sourceAdmin(env, 'a', 'faults', { fail_next: 1 });
  const failed = await api(env, 'POST', `/api/runs/${run.id}/step`, { pages: 1 });
  assert.equal(failed.status, 200);
  assert.equal(failed.body.stopped, 'failed');
  assert.equal(failed.body.run.failure.pageNo, 3);
  assert.equal(failed.body.run.failure.emitted_pages, 2);

  const unacknowledged = await api(env, 'POST', `/api/runs/${run.id}/retry`, { policy: 'restart' });
  assert.equal(unacknowledged.status, 409);
  assert.equal(unacknowledged.body.error.code, 'acknowledgement_required');
  assert.equal(unacknowledged.body.error.details.would_invalidate.from_page, 1);

  const restarted = await api(env, 'POST', `/api/runs/${run.id}/retry`, {
    policy: 'restart', acknowledge_invalidation: true,
  });
  assert.equal(restarted.status, 200);
  assert.equal(restarted.body.run.epoch, 2);
  assert.equal(restarted.body.run.emitted.pages, 0);
  assert.equal(restarted.body.run.cursor.pageNo, 1);
  assert.deepEqual(restarted.body.invalidated.pages, { from_page: 1, to_page: 2 });

  // The page material of the old epoch is still auditable, but marked superseded.
  assert.deepEqual(restarted.body.run.superseded_pages.map((p) => p.pageNo), [1, 2]);
});

test('http: getPage only serves the active epoch, and page digests match the listing', async (t) => {
  const env = await boot();
  t.after(env.close);

  const run = (await api(env, 'POST', '/api/runs', { policy: 'pin', limit: 40 })).body.run;
  const stepped = await api(env, 'POST', `/api/runs/${run.id}/step`, { pages: 2 });
  const listed = stepped.body.run.pages[0];

  const detail = await api(env, 'GET', `/api/runs/${run.id}/pages/1`);
  assert.equal(detail.status, 200);
  assert.equal(detail.body.digest, listed.digest);
  assert.equal(detail.body.page.page_digest, listed.digest);
  assert.equal(detail.body.page.rows.length, 40);
  assert.equal(detail.body.page.versions.b.revision, run.binding.b.revision);

  const missing = await api(env, 'GET', `/api/runs/${run.id}/pages/99`);
  assert.equal(missing.status, 404);
  assert.equal(missing.body.error.code, 'page_not_found');

  const verification = await api(env, 'GET', `/api/runs/${run.id}/verify`);
  assert.equal(verification.body.verdict, 'prefix_verified');
  assert.equal(verification.body.integrity.find((c) => c.name === 'page_digest_1').ok, true);
  assert.ok(verification.body.availability.some((c) => c.source === 'A' && c.name === 'now_snapshot_retained' && c.ok));
});
