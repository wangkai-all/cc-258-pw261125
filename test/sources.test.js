'use strict';

const test = require('node:test');
const assert = require('node:assert');

const { startSourceA, startSourceB } = require('../src/sources/start-lib');

function tmpDir(label) {
  const dir = require('node:fs').mkdtempSync(require('node:path').join(require('node:os').tmpdir(), `sources-${label}-`));
  return dir;
}

async function boot() {
  const dir = tmpDir('s');
  const a = await startSourceA({ port: 0, dataDir: dir });
  const b = await startSourceB({ port: 0, dataDir: dir, retention: 4 });
  const close = () => { a.close(); b.close(); };
  return { a, b, dir, close };
}

function url(svc, path) {
  return `http://127.0.0.1:${svc.port}${path}`;
}

async function getJson(svc, path) {
  const response = await fetch(url(svc, path));
  const body = await response.json();
  return { status: response.status, body };
}

async function postJson(svc, path, body = {}) {
  const response = await fetch(url(svc, path), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  return { status: response.status, body: await response.json() };
}

test('source A: snapshot reads are stable while live data changes', async (t) => {
  const env = await boot();
  t.after(env.close);

  const latest = await getJson(env.a, '/v1/snapshot');
  assert.equal(latest.status, 200);
  const token = latest.body.snapshot.token;

  const first = await getJson(env.a, `/v1/accounts?snapshot=${token}&limit=10`);
  assert.equal(first.status, 200);
  assert.equal(first.body.page.rows.length, 10);

  await postJson(env.a, '/admin/update', { count: 12 });
  const afterUpdate = await getJson(env.a, `/v1/accounts?snapshot=${token}&limit=10`);
  assert.deepEqual(afterUpdate.body.page.rows, first.body.page.rows, 'old snapshot must not see new writes');
  assert.equal(afterUpdate.body.filtered.digest, first.body.filtered.digest);

  const newest = await getJson(env.a, '/v1/snapshot');
  assert.notEqual(newest.body.snapshot.token, token);
});

test('source A: unknown snapshot is a non-retryable 404', async (t) => {
  const env = await boot();
  t.after(env.close);
  const missing = await getJson(env.a, '/v1/accounts?snapshot=snap-9999-deadbeef&limit=5');
  assert.equal(missing.status, 404);
  assert.equal(missing.body.error.code, 'snapshot_not_found');
  assert.equal(missing.body.error.retryable, false);
});

test('source B: revision reads reject missing and expired revisions', async (t) => {
  const env = await boot();
  t.after(env.close);

  const noRevision = await getJson(env.b, '/v1/activity?limit=5');
  assert.equal(noRevision.status, 400);
  assert.equal(noRevision.body.error.code, 'revision_required');

  const rows = await getJson(env.b, '/v1/activity?revision=1&limit=5');
  assert.equal(rows.status, 200);
  assert.equal(rows.body.page.rows.length, 5);

  // retention is 4 in this fixture; 5 bumps pushes revision 1 below the floor.
  for (let i = 0; i < 5; i += 1) await postJson(env.b, '/admin/bump', { count: 3 });
  const expired = await getJson(env.b, '/v1/activity?revision=1&limit=5');
  assert.equal(expired.status, 410);
  assert.equal(expired.body.error.code, 'revision_expired');
  assert.ok(expired.body.error.details.floor > 1);

  const future = await getJson(env.b, '/v1/activity?revision=999&limit=5');
  assert.equal(future.status, 404);
  assert.equal(future.body.error.code, 'revision_unknown');
});

test('source B: id lookup marks missing ids explicitly and digests them', async (t) => {
  const env = await boot();
  t.after(env.close);

  const found = await getJson(env.b, '/v1/activity?revision=1&ids=ACC-0001,ACC-0002');
  assert.equal(found.status, 200);
  assert.equal(found.body.missing.length, 0);

  const withMissing = await getJson(env.b, '/v1/activity?revision=1&ids=ACC-0001,ACC-9999');
  assert.deepEqual(withMissing.body.missing, ['ACC-9999']);
  const last = withMissing.body.rows[withMissing.body.rows.length - 1];
  assert.equal(last.present, false);
  assert.notEqual(withMissing.body.digest, found.body.digest, 'missing ids must change the digest');
});

test('source A: faults are injected deterministically and counted', async (t) => {
  const env = await boot();
  t.after(env.close);

  const configured = await postJson(env.a, '/admin/faults', { fail_next: 1 });
  assert.equal(configured.status, 200);

  const first = await getJson(env.a, '/v1/snapshot');
  assert.equal(first.status, 503);
  assert.equal(first.body.error.code, 'simulated_source_unavailable');

  const second = await getJson(env.a, '/v1/snapshot');
  assert.equal(second.status, 200);
});

test('both sources: admin state exposes versions and read history', async (t) => {
  const env = await boot();
  t.after(env.close);

  await getJson(env.a, '/v1/accounts?limit=2');
  await getJson(env.b, '/v1/activity?revision=1&limit=2');

  const aState = await getJson(env.a, '/admin/state');
  assert.ok(aState.body.snapshots.length >= 1);
  assert.ok(aState.body.reads.length >= 1);

  const bState = await getJson(env.b, '/admin/state');
  assert.equal(bState.body.current_revision, 1);
  assert.ok(bState.body.reads.some((r) => r.revision === 1));
});

test('sources persist across reopen', async (t) => {
  const dir = tmpDir('persist');
  const a1 = await startSourceA({ port: 0, dataDir: dir });
  const token = (await getJson(a1, '/v1/snapshot')).body.snapshot.token;
  await postJson(a1, '/admin/update', { count: 8 });
  a1.close();

  const a2 = await startSourceA({ port: 0, dataDir: dir });
  t.after(() => a2.close());
  const state = await getJson(a2, '/admin/state');
  assert.ok(state.body.snapshots.some((s) => s.token === token), 'snapshot survives restart');
  assert.ok(state.body.data_seq >= 2);
});
