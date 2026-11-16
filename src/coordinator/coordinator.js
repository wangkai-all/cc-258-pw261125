'use strict';

const crypto = require('node:crypto');
const { sha256, canonical, nowIso } = require('../lib/canonical');
const { CoordinatorError } = require('../lib/coordinator-error');
const { UpstreamError } = require('./source-client');
const { accountRowsDigest, activityIdsDigest } = require('../lib/source-digests');

const POLICIES = Object.freeze({
  PIN: 'pin',
  RESTART: 'restart',
  MIXED: 'mixed',
});
const POLICY_LIST = Object.values(POLICIES);

const MAX_LIMIT = 200;
const MAX_ATTEMPTS = 800;

const CONTRACT_TEXT = Object.freeze({
  pin: {
    name: '固定来源版本（pin）',
    promise: '已输出的每一个分页都来自同一组 (A 快照, B 修订号)。不跳号、不重复、不换版本；任一来源失败时停在失败位置，绝不静默改口径。',
    guarantee: 'exact-versions-over-emitted-prefix',
    cost: '必须保持 B 修订号在保留窗口内（默认 10 个修订）；超出窗口时只能选择重启或显式混合，不能继续 pin。失败期间查询无法前进。',
    failure_behavior: '停止并持久化失败位置；重试前重新校验两个版本的 content_digest 未变。',
  },
  restart: {
    name: '整次重试（restart）',
    promise: '最终交付的结果只来自一组版本；但重试会作废此前已经输出的所有分页，并从第 1 页重新开始。',
    guarantee: 'single-version-set-after-last-restart',
    cost: '已交付进度归零，重试成本随查询规模线性增长；调用方必须能够丢弃此前收到的分页。',
    failure_behavior: '提升 epoch，明确列出被作废的分页范围，重新绑定两个来源。',
  },
  mixed: {
    name: '显式不一致风险（mixed）',
    promise: '不承诺单一版本；每个分页标注自己来自哪一组版本，跨版本的漂移被逐页记录并在结果末尾汇总。绝不静默混合。',
    guarantee: 'declared-version-per-page',
    cost: '前后分页可能来自不同时刻；跨行比较会出现漂移，消费方必须接受并处理已声明的版本差异。',
    failure_behavior: '只重新绑定失败的那个来源，保留已输出前缀，写入警告与版本分段。',
  },
});

function clampLimit(value) {
  const n = Number(value);
  if (!Number.isInteger(n) || n < 1 || n > MAX_LIMIT) {
    throw new CoordinatorError('invalid_limit', `limit must be an integer in [1, ${MAX_LIMIT}]`, { status: 400 });
  }
  return n;
}

function normalizeFilters(input) {
  const raw = input || {};
  const out = {
    region: raw.region ? String(raw.region).slice(0, 32) : null,
    plan: raw.plan ? String(raw.plan).slice(0, 32) : null,
    min_balance: raw.min_balance === undefined || raw.min_balance === null || raw.min_balance === ''
      ? null
      : Number(raw.min_balance),
  };
  if (out.min_balance !== null && (!Number.isFinite(out.min_balance) || out.min_balance < 0)) {
    throw new CoordinatorError('invalid_filter', 'min_balance must be a non-negative number', { status: 400 });
  }
  return out;
}

function normalizePolicy(policy, fallback = POLICIES.PIN) {
  const value = policy === undefined || policy === null || policy === '' ? fallback : String(policy);
  if (!POLICY_LIST.includes(value)) {
    throw new CoordinatorError('invalid_policy', `policy must be one of ${POLICY_LIST.join(', ')}`, { status: 400 });
  }
  return value;
}

function publicVersionA(snapshot) {
  return {
    token: snapshot.token,
    data_seq: snapshot.data_seq,
    content_digest: snapshot.content_digest,
    row_count: snapshot.row_count,
    created_at: snapshot.created_at,
  };
}

function publicVersionB(revision) {
  return {
    revision: revision.revision,
    content_digest: revision.content_digest,
    row_count: revision.row_count,
    created_at: revision.created_at,
  };
}

function bindingDigest({ a, b, filters, limit, join }) {
  return sha256(canonical({
    a: { token: a.token, content_digest: a.content_digest },
    b: { revision: b.revision, content_digest: b.content_digest },
    filters,
    limit,
    join,
  }));
}

function cacheKey({ binding, policy, pageNo, after }) {
  return sha256(canonical({
    binding: binding.digest,
    policy,
    pageNo,
    after: after || null,
    schema: 'page-v1',
  }));
}

/**
 * Content identity of a page. Deliberately excludes run_id/epoch so a page
 * materialized by one run can be reused by another only when the binding,
 * policy, cursor and rows are byte-identical. The stored digest is
 * recomputable from the page body alone.
 */
function pageDigest(page) {
  return sha256(canonical({
    policy: page.policy,
    binding_digest: page.binding_digest,
    pageNo: page.pageNo,
    versions: page.versions,
    rows: page.rows,
    cursor_before: page.cursor_before,
    cursor_after: page.cursor_after,
    a: page.a,
    b: page.b,
  }));
}

function newRunId() {
  return `run-${Date.now().toString(36)}-${crypto.randomBytes(3).toString('hex')}`;
}

class QueryCoordinator {
  constructor({ store, sourceA, sourceB, maxPages = 10000, logger = null }) {
    if (!store) throw new Error('store is required');
    if (!sourceA || !sourceB) throw new Error('both source clients are required');
    this.store = store;
    this.sourceA = sourceA;
    this.sourceB = sourceB;
    this.maxPages = maxPages;
    this.logger = logger;
    this.locks = new Map();
  }

  log(runId, kind, body) {
    this.store.appendEvent(runId, kind, body);
    if (this.logger) this.logger(kind, { runId, ...body });
  }

  #withLock(runId, fn) {
    const previous = this.locks.get(runId) || Promise.resolve();
    const next = previous.then(fn, fn);
    const tracked = next.catch(() => {});
    this.locks.set(runId, tracked);
    tracked.finally(() => {
      if (this.locks.get(runId) === tracked) this.locks.delete(runId);
    });
    return next;
  }

  #requireRun(runId) {
    const run = this.store.getRun(runId);
    if (!run) throw new CoordinatorError('run_not_found', `run ${runId} does not exist`, { status: 404 });
    return run;
  }

  #touch(run) {
    run.updatedAt = nowIso();
    return run;
  }

  #recordAttempt(run, attempt) {
    run.attempts.push({ at: nowIso(), ...attempt });
    if (run.attempts.length > MAX_ATTEMPTS) run.attempts.splice(0, run.attempts.length - MAX_ATTEMPTS);
  }

  async createRun(input = {}) {
    const policy = normalizePolicy(input.policy);
    const filters = normalizeFilters(input.filters);
    const limit = clampLimit(input.limit === undefined ? 40 : input.limit);
    const join = input.join === 'inner' ? 'inner' : 'left';
    const id = newRunId();
    const createdAt = nowIso();
    const run = {
      id,
      createdAt,
      updatedAt: createdAt,
      state: 'binding',
      epoch: 1,
      request: { filters, limit, join, policy, limit_ceiling: MAX_LIMIT },
      binding: null,
      pin: { history: [] },
      cursor: { after: null, pageNo: 1 },
      emitted: { pages: 0, rows: 0, first_page: null, last_page: null, first_account: null, last_account: null },
      failure: null,
      sourceA: {
        filter_total: null, filter_digest: null, pages: 0, rows: 0,
        last_digest: null, last_read_at: null, chain: null,
      },
      sourceB: {
        pages: 0, rows: 0, missing: 0, last_digest: null, last_read_at: null,
        ids_seen: 0, chain: null,
      },
      nextRetry: { allowed: false },
      warnings: [],
      cache: { hits: 0, misses: 0, run_page_hits: 0, mismatch_rejections: 0 },
      attempts: [],
      versionLoss: null,
    };
    this.store.createRun(run);
    this.log(id, 'run_created', { policy, filters, limit, join });
    try {
      await this.#bind(run, 'initial bind');
      run.state = 'bound';
      this.#touch(run);
      this.store.saveRun(run);
      this.log(id, 'bound', { binding: run.binding });
      const stored = this.store.getRun(id);
      return { ...stored, policy: stored.request.policy, contract: CONTRACT_TEXT[stored.request.policy] };
    } catch (err) {
      run.state = 'failed';
      run.failure = this.#failureFromError(err, { phase: 'bind', pageNo: null, source: err.source || 'sources' });
      run.nextRetry = this.#retryGuidance(run);
      this.#recordAttempt(run, {
        phase: 'bind', source: err.source || 'sources', outcome: 'error',
        code: err.code || 'bind_failed', message: err.message,
      });
      this.#touch(run);
      this.store.saveRun(run);
      this.log(id, 'bind_failed', { error: run.failure });
      throw new CoordinatorError('bind_failed', `could not bind source versions: ${err.message}`, {
        status: 503,
        details: { run_id: id, failure: run.failure, retry: run.nextRetry },
      });
    }
  }

  async #probeA(token = null) {
    const started = Date.now();
    try {
      const payload = token
        ? await this.sourceA.get(`/v1/snapshot/${encodeURIComponent(token)}`)
        : await this.sourceA.get('/v1/snapshot');
      return { payload, ms: Date.now() - started };
    } catch (err) {
      throw this.#wrapUpstream(err, 'A', token ? 'probe_snapshot' : 'probe_latest');
    }
  }

  async #probeB(revision = null) {
    const started = Date.now();
    try {
      const payload = revision
        ? await this.sourceB.get(`/v1/revision/${revision}`)
        : await this.sourceB.get('/v1/revision');
      return { payload, ms: Date.now() - started };
    } catch (err) {
      throw this.#wrapUpstream(err, 'B', revision ? 'probe_revision' : 'probe_latest');
    }
  }

  #wrapUpstream(err, source, phase) {
    if (err instanceof UpstreamError) {
      if (err.source && err.source !== source) err.source = source;
      err.phase = phase;
      return err;
    }
    return new UpstreamError({
      source,
      code: 'source_protocol_error',
      message: err && err.message ? err.message : String(err),
      status: 502,
      retryable: true,
      phase,
    });
  }

  async #bind(run, reason) {
    const [aProbe, bProbe] = await Promise.all([this.#probeA(), this.#probeB()]);
    const a = publicVersionA(aProbe.payload.snapshot);
    const b = publicVersionB(bProbe.payload.revision);
    const binding = {
      a,
      b,
      filters: run.request.filters,
      limit: run.request.limit,
      join: run.request.join,
      bound_at: nowIso(),
      digest: bindingDigest({ a, b, filters: run.request.filters, limit: run.request.limit, join: run.request.join }),
      /** Read pairs are pinned, not simultaneous: A and B are probed at different instants. */
      disclaimer: 'Pinned read pair, not a global transaction. A and B have no shared transaction boundary; version skew is reported as A.created_at → B.created_at.',
      skew_ms: Date.now() - Math.min(Date.parse(a.created_at) || Date.now(), Date.parse(b.created_at) || Date.now()),
    };
    run.binding = binding;
    if (!run.pin) run.pin = { history: [] };
    for (const entry of run.pin.history) {
      if (entry.to_page === null) entry.to_page = Math.max(0, run.cursor.pageNo - 1);
    }
    run.pin.history.push({
      binding,
      from_page: run.cursor.pageNo,
      to_page: null,
      reason,
      at: binding.bound_at,
    });
    this.#recordAttempt(run, { phase: 'bind', source: 'A+B', outcome: 'ok', code: 'bound', digest: binding.digest, ms: aProbe.ms + bProbe.ms });
    return binding;
  }

  async #rebind(run, sources, reason) {
    if (sources === 'both') return this.#bind(run, reason);
    const active = run.binding;
    const next = { ...active, filters: run.request.filters, limit: run.request.limit, join: run.request.join };
    if (sources === 'A') {
      const probe = await this.#probeA();
      next.a = publicVersionA(probe.payload.snapshot);
    } else {
      const probe = await this.#probeB();
      next.b = publicVersionB(probe.payload.revision);
    }
    next.bound_at = nowIso();
    next.digest = bindingDigest(next);
    next.disclaimer = active.disclaimer;
    run.binding = next;
    for (const entry of run.pin.history) {
      if (entry.to_page === null) entry.to_page = Math.max(0, run.cursor.pageNo - 1);
    }
    run.pin.history.push({
      binding: next,
      from_page: run.cursor.pageNo,
      to_page: null,
      reason,
      at: next.bound_at,
    });
    this.#recordAttempt(run, { phase: 'rebind', source: sources, outcome: 'ok', code: 'rebound', digest: next.digest });
    return next;
  }

  async #verifyPinnedVersions(run) {
    const active = run.binding;
    const checks = [];
    let aOk = true;
    let bOk = true;
    let aExpired = false;
    let bExpired = false;
    try {
      const { payload } = await this.#probeA(active.a.token);
      const token = payload.snapshot;
      const sameContent = token.content_digest === active.a.content_digest;
      checks.push({ source: 'A', name: 'snapshot_retained', ok: true, detail: token.token });
      checks.push({
        source: 'A', name: 'snapshot_content_unchanged',
        ok: sameContent,
        expected: active.a.content_digest,
        actual: token.content_digest,
      });
      aOk = sameContent;
    } catch (err) {
      const gone = err.status === 404 || err.status === 410 || err.code === 'snapshot_not_found';
      aExpired = gone;
      aOk = false;
      checks.push({
        source: 'A', name: 'snapshot_retained', ok: false,
        detail: gone ? `snapshot ${active.a.token} is no longer retained` : err.message,
        code: err.code,
      });
    }
    try {
      const { payload } = await this.#probeB(active.b.revision);
      const rev = payload.revision;
      const sameContent = rev.content_digest === active.b.content_digest;
      checks.push({ source: 'B', name: 'revision_retained', ok: true, detail: String(rev.revision) });
      checks.push({
        source: 'B', name: 'revision_content_unchanged',
        ok: sameContent,
        expected: active.b.content_digest,
        actual: rev.content_digest,
      });
      bOk = sameContent;
    } catch (err) {
      const gone = err.status === 404 || err.status === 410 || err.code === 'revision_expired';
      bExpired = gone;
      bOk = false;
      checks.push({
        source: 'B', name: 'revision_retained', ok: false,
        detail: gone ? `revision ${active.b.revision} fell below floor` : err.message,
        code: err.code,
        floor: err.details && err.details.floor,
      });
    }
    return { ok: aOk && bOk, checks, aOk, bOk, aExpired, bExpired };
  }

  async step(runId) {
    const run = this.#requireRun(runId);
    if (run.state === 'completed') return { run, page: null, finished: true };
    if (run.state === 'cancelled') {
      throw new CoordinatorError('run_cancelled', 'run was cancelled; resume it before stepping', {
        status: 409, details: { run_id: runId, failure: run.failure },
      });
    }
    if (run.state === 'failed') {
      throw new CoordinatorError('run_failed', 'run stopped at a failure position; retry or resume before stepping', {
        status: 409, details: { run_id: runId, failure: run.failure, retry: run.nextRetry },
      });
    }
    if (run.state === 'binding') {
      throw new CoordinatorError('run_binding', 'run has not finished binding yet', { status: 409 });
    }
    return this.#withLock(runId, async () => {
      // Re-read under the lock: cancel/retry may have changed state while queued.
      const current = this.#requireRun(runId);
      if (current.state === 'completed') return { run: current, page: null, finished: true };
      if (current.state === 'cancelled') {
        throw new CoordinatorError('run_cancelled', 'run was cancelled before this step could start', {
          status: 409, details: { run_id: runId },
        });
      }
      if (current.state === 'failed') {
        throw new CoordinatorError('run_failed', 'run failed before this step could start', {
          status: 409, details: { run_id: runId, failure: current.failure, retry: current.nextRetry },
        });
      }
      // The in-flight read pair either commits whole or fails whole; cancel()
      // serializes behind it and takes effect at the next page boundary.
      return this.#emitNextPage(current);
    });
  }

  async advance(runId, pages = 1) {
    const emitted = [];
    const count = Math.max(1, Math.min(50, Number(pages) || 1));
    for (let i = 0; i < count; i += 1) {
      let result;
      try {
        result = await this.step(runId);
      } catch (err) {
        if (err instanceof CoordinatorError && (err.code === 'run_failed' || err.code === 'source_failed')) {
          return { run: this.store.getRun(runId), pages: emitted, stopped: 'failed', error: err };
        }
        if (err instanceof CoordinatorError && err.code === 'run_cancelled') {
          return { run: this.store.getRun(runId), pages: emitted, stopped: 'cancelled' };
        }
        throw err;
      }
      if (result.page) emitted.push(result.page);
      if (result.finished || result.cancelled) break;
    }
    return { run: this.store.getRun(runId), pages: emitted, stopped: null };
  }

  async #emitNextPage(run) {
    const pageNo = run.cursor.pageNo;
    const epoch = run.epoch;
    if (pageNo > this.maxPages) {
      throw new CoordinatorError('page_ceiling', `run exceeded the ${this.maxPages}-page safety ceiling`, { status: 409 });
    }

    // 1) Integrity guard: page material and cursor are committed atomically,
    //    so a page at the active cursor can only exist with the same cursor_before.
    //    Any divergence is corruption, not something to paper over by replaying.
    const storedPages = this.store.getPages(run.id, epoch);
    const existing = storedPages.find((page) => page.pageNo === pageNo);
    if (existing) {
      if (existing.body.cursor_before !== run.cursor.after) {
        throw new CoordinatorError(
          'state_divergence',
          `page ${pageNo} persisted with cursor_before ${JSON.stringify(existing.body.cursor_before)} but the run cursor is ${JSON.stringify(run.cursor.after)}`,
          { status: 500, details: { run_id: run.id, pageNo, epoch } },
        );
      }
      run.cache.run_page_hits += 1;
      this.#recordAttempt(run, {
        phase: 'emit', source: 'cache', outcome: 'cache_hit', code: 'run_page',
        pageNo, digest: existing.digest,
      });
      this.#touch(run);
      this.store.saveRun(run);
      return {
        run: this.store.getRun(run.id),
        page: { ...existing.body, cache: { hit: true, kind: 'run-page' } },
        replayed: true,
      };
    }

    const sourceARead = { at: null, ms: null, snapshot: run.binding.a.token };
    const sourceBRead = { at: null, ms: null, revision: run.binding.b.revision };

    // 2) Cross-run cache: key includes full binding + policy + cursor, so a hit
    //    can never splice a different version set. Mismatches are refused loudly.
    const after = run.cursor.after;
    const key = cacheKey({ binding: run.binding, policy: run.request.policy, pageNo, after });
    const cached = this.store.getCache(key);
    if (cached) {
      const versionsMatch = canonical(cached.versions)
        === canonical({ a: run.binding.a, b: run.binding.b, binding_digest: run.binding.digest });
      const digestOk = cached.sliceDigest === pageDigest(cached.slice);
      if (versionsMatch && digestOk) {
        const page = { ...cached.slice, cache: { hit: true, kind: 'cross-run', key, entry_created_at: cached.createdAt } };
        this.store.recordCacheHit(key);
        run.cache.hits += 1;
        this.#recordAttempt(run, {
          phase: 'emit', source: 'cache', outcome: 'cache_hit', code: 'cross_run',
          pageNo, digest: cached.sliceDigest, key,
        });
        return this.#commitPage(run, page, { cacheHit: true });
      }
      this.store.deleteCache(key);
      run.cache.mismatch_rejections += 1;
      run.warnings.push({
        code: 'cache_entry_rejected', pageNo, at: nowIso(),
        message: 'a cache entry had matching key but mismatched versions or digest; it was deleted instead of reused',
      });
      this.log(run.id, 'cache_entry_rejected', { key, pageNo, versionsMatch, digestOk });
    }
    run.cache.misses += 1;

    // 3) Read source A at the pinned snapshot.
    const aStarted = Date.now();
    let aPayload;
    try {
      aPayload = await this.sourceA.get('/v1/accounts', {
        snapshot: run.binding.a.token,
        region: run.request.filters.region,
        plan: run.request.filters.plan,
        min_balance: run.request.filters.min_balance,
        after,
        limit: run.request.limit,
      });
    } catch (err) {
      const wrapped = this.#wrapUpstream(err, 'A', 'accounts');
      throw await this.#failRun(run, wrapped, {
        phase: 'accounts_read', pageNo, source: 'A',
        cursor: { after },
        note: 'A read failed before B was consulted; no rows from this page were emitted.',
      });
    }
    sourceARead.at = nowIso();
    sourceARead.ms = Date.now() - aStarted;

    const aCheck = this.#verifyAPage(run, aPayload);
    if (!aCheck.ok) {
      const err = new UpstreamError({
        source: 'A',
        code: aCheck.code,
        message: aCheck.message,
        status: 502,
        retryable: true,
        phase: 'accounts',
      });
      throw await this.#failRun(run, err, {
        phase: 'accounts_verify', pageNo, source: 'A', cursor: { after },
        note: 'A returned data that did not match the pinned snapshot; the page was discarded.',
      });
    }
    this.#recordAttempt(run, {
      phase: 'accounts', source: 'A', outcome: 'ok', code: 'verified',
      pageNo, digest: aPayload.page.digest, rows: aPayload.page.row_count, ms: sourceARead.ms,
    });
    const aRows = aPayload.page.rows;
    const ids = aRows.map((row) => row.account_id);

    // 4) Read source B at the pinned revision, by the ids A just returned.
    let bPayload = { rows: [], missing: [], digest: activityIdsDigest(ids, new Map()) };
    if (ids.length > 0) {
      const bStarted = Date.now();
      try {
        bPayload = await this.sourceB.get('/v1/activity', {
          revision: run.binding.b.revision,
          ids: ids.join(','),
        });
      } catch (err) {
        const wrapped = this.#wrapUpstream(err, 'B', 'activity');
        throw await this.#failRun(run, wrapped, {
          phase: 'activity_read', pageNo, source: 'B',
          cursor: { after },
          note: `A page ${pageNo} was read from snapshot ${run.binding.a.token} but discarded because B failed; retry re-reads A at the same snapshot.`,
        });
      }
      sourceBRead.at = nowIso();
      sourceBRead.ms = Date.now() - bStarted;
      const bCheck = this.#verifyBPage(run, bPayload, ids);
      if (!bCheck.ok) {
        const err = new UpstreamError({
          source: 'B', code: bCheck.code, message: bCheck.message,
          status: 502, retryable: true, phase: 'activity',
        });
        throw await this.#failRun(run, err, {
          phase: 'activity_verify', pageNo, source: 'B', cursor: { after },
          note: 'B returned data that did not match the pinned revision; the page was discarded.',
        });
      }
      this.#recordAttempt(run, {
        phase: 'activity', source: 'B', outcome: 'ok', code: 'verified',
        pageNo, digest: bPayload.digest, rows: bPayload.rows.length, missing: bPayload.missing.length, ms: sourceBRead.ms,
      });
    }

    // 5) Join. Left join keeps accounts without B activity, flagged rather than dropped.
    const byId = new Map(bPayload.rows.map((row) => [row.account_id, row]));
    const joined = [];
    let missing = 0;
    for (const aRow of aRows) {
      const bRow = byId.get(aRow.account_id);
      const present = Boolean(bRow && bRow.present !== false && bRow.segment !== null);
      if (!present) missing += 1;
      if (!present && run.request.join === 'inner') continue;
      joined.push({
        account_id: aRow.account_id,
        name: aRow.name,
        region: aRow.region,
        plan: aRow.plan,
        balance_cents: aRow.balance_cents,
        segment: present ? bRow.segment : null,
        risk_score: present ? bRow.risk_score : null,
        activity_present: present,
      });
    }

    const nextAfter = aPayload.page.next_after;
    const page = {
      run_id: run.id,
      epoch,
      pageNo,
      policy: run.request.policy,
      binding_digest: run.binding.digest,
      versions: { a: run.binding.a, b: run.binding.b },
      cursor_before: after,
      cursor_after: nextAfter,
      rows: joined,
      row_count: joined.length,
      a: {
        after: aPayload.page.after,
        row_count: aPayload.page.row_count,
        digest: aPayload.page.digest,
        complete: aPayload.page.complete,
        filtered_total: aPayload.filtered.total,
        filtered_digest: aPayload.filtered.digest,
        filters_digest: aPayload.filters_digest,
      },
      b: {
        ids,
        digest: bPayload.digest,
        missing: bPayload.missing,
        revision: bPayload.revision ? bPayload.revision.revision : run.binding.b.revision,
      },
      warnings: [],
      read_at: { a: sourceARead.at, b: sourceBRead.at },
      duration_ms: { a: sourceARead.ms, b: sourceBRead.ms },
      cache: { hit: false, kind: null },
    };
    page.page_digest = pageDigest(page);

    return this.#commitPage(run, page, { cacheHit: false, cacheKey: key, aPayload });
  }

  #verifyAPage(run, payload) {
    if (!payload || !payload.snapshot || !payload.page) {
      return { ok: false, code: 'source_protocol_error', message: 'A response is missing snapshot/page fields' };
    }
    if (payload.snapshot.token !== run.binding.a.token) {
      return {
        ok: false, code: 'snapshot_mismatch',
        message: `A answered from snapshot ${payload.snapshot.token} but ${run.binding.a.token} is pinned`,
      };
    }
    if (payload.snapshot.content_digest !== run.binding.a.content_digest) {
      return {
        ok: false, code: 'snapshot_content_changed',
        message: `snapshot ${payload.snapshot.token} content digest changed after binding`,
      };
    }
    const recomputed = accountRowsDigest(payload.page.rows);
    if (recomputed !== payload.page.digest) {
      return {
        ok: false, code: 'page_digest_mismatch',
        message: `A page digest ${payload.page.digest} does not match the recomputed ${recomputed}`,
      };
    }
    if (run.sourceA.filter_digest && payload.filtered.digest !== run.sourceA.filter_digest) {
      return {
        ok: false, code: 'filtered_set_changed',
        message: 'A reported a different filtered-set digest than earlier pages of this run',
      };
    }
    return { ok: true };
  }

  #verifyBPage(run, payload, ids) {
    if (!payload || !payload.revision) {
      return { ok: false, code: 'source_protocol_error', message: 'B response is missing revision metadata' };
    }
    if (payload.revision.revision !== run.binding.b.revision) {
      return {
        ok: false, code: 'revision_mismatch',
        message: `B answered from revision ${payload.revision.revision} but ${run.binding.b.revision} is pinned`,
      };
    }
    if (payload.revision.content_digest !== run.binding.b.content_digest) {
      return {
        ok: false, code: 'revision_content_changed',
        message: `revision ${payload.revision.revision} content digest changed after binding`,
      };
    }
    const byId = new Map(payload.rows.map((row) => [row.account_id, row]));
    const recomputed = activityIdsDigest(ids, byId);
    if (recomputed !== payload.digest) {
      return {
        ok: false, code: 'lookup_digest_mismatch',
        message: `B lookup digest ${payload.digest} does not match the recomputed ${recomputed}`,
      };
    }
    return { ok: true };
  }

  #commitPage(run, page, { cacheHit, cacheKey: key }) {
    const started = Date.now();
    if (page.a.complete && run.sourceA.filter_digest && page.a.filtered_digest !== run.sourceA.filter_digest) {
      throw new CoordinatorError('filtered_set_changed', 'A changed its filtered-set digest mid-run', { status: 409 });
    }
    if (!run.sourceA.filter_digest) {
      run.sourceA.filter_digest = page.a.filtered_digest;
      run.sourceA.filter_total = page.a.filtered_total;
    }
    run.sourceA.pages += 1;
    run.sourceA.rows += page.a.row_count;
    run.sourceA.last_digest = page.a.digest;
    run.sourceA.last_read_at = page.read_at.a;
    run.sourceA.chain = sha256(`${run.sourceA.chain || 'genesis'}\n${page.a.digest}`);
    if (page.b.ids.length > 0) {
      run.sourceB.pages += 1;
      run.sourceB.rows += page.rows.filter((row) => row.activity_present).length;
      run.sourceB.missing += page.b.missing.length;
      run.sourceB.last_digest = page.b.digest;
      run.sourceB.last_read_at = page.read_at.b;
      run.sourceB.ids_seen += page.b.ids.length;
      run.sourceB.chain = sha256(`${run.sourceB.chain || 'genesis'}\n${page.b.digest}`);
    }

    run.emitted.pages += 1;
    run.emitted.rows += page.row_count;
    run.emitted.first_page = run.emitted.first_page || page.pageNo;
    run.emitted.last_page = page.pageNo;
    run.emitted.first_account = run.emitted.first_account || (page.rows[0] && page.rows[0].account_id) || null;
    run.emitted.last_account = (page.rows[page.rows.length - 1] && page.rows[page.rows.length - 1].account_id)
      || run.emitted.last_account;

    run.cursor = {
      after: page.cursor_after,
      pageNo: page.pageNo + 1,
      last_page_rows: page.row_count,
    };
    run.updatedAt = nowIso();
    run.failure = null;
    run.nextRetry = { allowed: false };

    if (page.a.complete) {
      run.state = 'completed';
      run.completedAt = nowIso();
      this.#logCompletion(run, page);
    } else {
      run.state = 'running';
    }

    // Page material, cache entry and the advanced cursor commit together. A
    // crash can then never leave a page on disk that the cursor has skipped,
    // or a cursor that points past a page that was never written.
    this.store.transaction(() => {
      this.store.savePage(run.id, page.epoch, page.pageNo, page, page.page_digest);
      if (!cacheHit && key) {
        this.store.putCache(key, page, page.page_digest, run.request.policy, {
          a: run.binding.a,
          b: run.binding.b,
          binding_digest: run.binding.digest,
        });
      }
      this.store.saveRun(run);
    });
    this.log(run.id, 'page_emitted', {
      pageNo: page.pageNo,
      rows: page.row_count,
      cursor_after: page.cursor_after,
      cache_hit: cacheHit,
      page_digest: page.page_digest,
      versions: { a: page.versions.a.token, b: page.versions.b.revision },
      duration_ms: Date.now() - started,
    });
    if (run.state === 'completed') this.log(run.id, 'run_completed', { emitted: run.emitted });
    return { run: this.store.getRun(run.id), page, finished: run.state === 'completed' };
  }

  #logCompletion(run, page) {
    // The active segment now has a known end: the last emitted page.
    const active = run.pin.history[run.pin.history.length - 1];
    if (active && active.to_page === null) active.to_page = page.pageNo;
    const expected = run.sourceA.filter_total;
    if (expected !== null && run.sourceA.rows !== expected) {
      run.warnings.push({
        code: 'row_count_drift',
        at: nowIso(),
        message: `A reported ${expected} filtered rows across the run but ${run.sourceA.rows} were emitted`,
      });
    }
    if (run.pin.history.length > 1) {
      run.warnings.push({
        code: 'mixed_versions',
        at: nowIso(),
        message: `run completed across ${run.pin.history.length} distinct version segments; see versions by page, not a single cut`,
      });
    }
  }

  async #failRun(run, err, context) {
    const wrapped = err instanceof UpstreamError
      ? err
      : new UpstreamError({
        source: context.source, code: err.code || 'source_error',
        message: err.message, status: err.status || 503,
        retryable: err.retryable !== false,
        details: err.details, phase: context.phase,
      });
    // The failure position records how much was delivered before the stop, so a
    // later retry can state exactly which range a policy change would affect.
    context.emittedPages = run.emitted.pages;

    // A 410 from either source is a durable fact: that recorded version can
    // never be re-read, so pin is dead for this run even after the outage ends.
    const details = wrapped.details || {};
    if (wrapped.code === 'revision_expired') {
      run.versionLoss = {
        source: 'B',
        at: nowIso(),
        reason: 'retention_floor_advanced',
        lost: run.binding ? run.binding.b : null,
        floor: details.floor === undefined ? null : details.floor,
        current: details.current === undefined ? null : details.current,
      };
    } else if (wrapped.code === 'snapshot_not_found' || wrapped.code === 'snapshot_expired') {
      run.versionLoss = {
        source: 'A',
        at: nowIso(),
        reason: 'snapshot_pruned',
        lost: run.binding ? run.binding.a : null,
      };
    }
    run.state = 'failed';
    run.failure = this.#failureFromError(wrapped, context);
    run.nextRetry = this.#retryGuidance(run);
    this.#recordAttempt(run, {
      phase: context.phase, source: context.source, outcome: 'error',
      code: wrapped.code, message: wrapped.message, pageNo: context.pageNo,
    });
    this.#touch(run);
    this.store.saveRun(run);
    this.log(run.id, 'page_failed', { failure: run.failure, retry: run.nextRetry });
    throw new CoordinatorError('source_failed', `${context.source} failed at page ${context.pageNo}: ${wrapped.message}`, {
      status: 503,
      details: { run_id: run.id, failure: run.failure, retry: run.nextRetry },
    });
  }

  #failureFromError(err, context) {
    return {
      at: nowIso(),
      phase: context.phase || err.phase || 'unknown',
      pageNo: context.pageNo === undefined ? null : context.pageNo,
      source: context.source || err.source || 'unknown',
      code: err.code || 'source_error',
      message: err.message || String(err),
      retryable: err.retryable !== false,
      status: err.status || null,
      details: err.details || {},
      cursor: context.cursor || null,
      emitted_pages: context.emittedPages === undefined ? null : context.emittedPages,
      note: context.note || null,
      retention_floor: err.details && err.details.floor !== undefined ? err.details.floor : null,
    };
  }

  #retryGuidance(run) {
    return this.#retryGuidanceFrom(run, null);
  }

  #retryGuidanceFrom(run, verification) {
    const hasEmitted = run.emitted.pages > 0;
    const loss = run.versionLoss || null;
    const readable = verification ? verification.ok : !loss;
    const expired = Boolean(loss) || Boolean(run.failure && (run.failure.code === 'revision_expired' || run.failure.code === 'snapshot_not_found'));
    if (!hasEmitted) {
      return {
        allowed: true,
        recommended: POLICIES.PIN,
        options: [
          { policy: POLICIES.PIN, effect: '重新探测两个来源并从第 1 页继续；尚未交付任何分页，消费方看不到口径变化。' },
          { policy: POLICIES.RESTART, effect: '重新绑定两个来源并从头开始；因为没有已输出分页，这里与 pin 等价。' },
          { policy: POLICIES.MIXED, effect: '重新绑定并继续；会产生混合版本运行，但尚不存在已交付前缀。' },
        ],
        note: '尚未输出任何分页，此刻改变口径不会追溯影响已交付结果。',
      };
    }
    return {
      allowed: true,
      recommended: !readable ? POLICIES.MIXED : run.request.policy,
      version_loss: loss,
      re_readable: readable,
      options: [
        {
          policy: POLICIES.PIN,
          effect: '仅当两个固定版本仍可重读时，从失败位置继续。',
          possible: readable,
          blocked_reason: readable ? null : '某个固定版本已离开保留窗口，无法再次读取',
        },
        {
          policy: POLICIES.RESTART,
          effect: `作废第 ${run.emitted.first_page}–${run.emitted.last_page} 页（${run.emitted.pages} 页、${run.emitted.rows} 行），用新版本从第 1 页重新开始。`,
          requires_acknowledgement: 'acknowledge_invalidation',
        },
        {
          policy: POLICIES.MIXED,
          effect: `保留第 ${run.emitted.first_page}–${run.emitted.last_page} 页，只重新绑定失败的来源；之后的分页携带显式版本警告。`,
          requires_acknowledgement: 'acknowledge_mixed',
        },
      ],
      note: '已存在交付：任何版本组或口径的改变都是消费方可见的承诺变化，必须显式确认。',
      failure_position: {
        page: run.failure ? run.failure.pageNo : null,
        cursor: run.failure ? run.failure.cursor : run.cursor,
        phase: run.failure ? run.failure.phase : null,
        source: run.failure ? run.failure.source : null,
        emitted_pages: run.emitted.pages,
      },
    };
  }

  async retry(runId, options = {}) {
    return this.#withLock(runId, async () => {
      const run = this.#requireRun(runId);
      if (run.state === 'completed') {
        throw new CoordinatorError('run_completed', 'run already completed', { status: 409, details: { run_id: runId } });
      }
      if (run.state !== 'failed' && run.state !== 'cancelled') {
        throw new CoordinatorError('run_not_retryable', `run state ${run.state} cannot be retried`, {
          status: 409, details: { run_id: runId, state: run.state },
        });
      }
      const requestedPolicy = normalizePolicy(options.policy, run.request.policy);
      const hasEmitted = run.emitted.pages > 0;
      const policyChanged = requestedPolicy !== run.request.policy;
      const contractChange = hasEmitted && (policyChanged || requestedPolicy !== POLICIES.PIN);

      if (hasEmitted && requestedPolicy === POLICIES.RESTART && options.acknowledge_invalidation !== true) {
        throw new CoordinatorError(
          'acknowledgement_required',
          'restarting after pages were delivered invalidates them; pass acknowledge_invalidation: true',
          {
            status: 409,
            details: {
              run_id: runId,
              would_invalidate: { from_page: run.emitted.first_page, to_page: run.emitted.last_page, rows: run.emitted.rows },
            },
          },
        );
      }
      if (hasEmitted && requestedPolicy === POLICIES.MIXED && options.acknowledge_mixed !== true) {
        throw new CoordinatorError(
          'acknowledgement_required',
          'continuing with mixed versions creates a declared version boundary after delivered pages; pass acknowledge_mixed: true',
          {
            status: 409,
            details: {
              run_id: runId,
              kept_prefix: { from_page: run.emitted.first_page, to_page: run.emitted.last_page, rows: run.emitted.rows },
            },
          },
        );
      }

      // A run that failed its very first bind has no pinned pair to verify; the
      // retry rebinds from scratch and nothing has been delivered yet.
      const verification = run.binding
        ? await this.#verifyPinnedVersions(run)
        : { ok: true, checks: [], aOk: true, bOk: true, aExpired: false, bExpired: false, unbounded: true };
      for (const check of verification.checks) {
        this.#recordAttempt(run, {
          phase: 'retry_verify', source: check.source, outcome: check.ok ? 'ok' : 'error',
          code: check.name, message: check.detail || null,
        });
      }

      let invalidated = null;
      const fromPolicy = run.request.policy;
      if (!hasEmitted) {
        // Nothing delivered yet: any rebinding is invisible to a consumer.
        if (!verification.ok || requestedPolicy !== run.request.policy) {
          await this.#rebind(run, 'both', `retry before first delivery (${requestedPolicy})`);
        }
        run.request.policy = requestedPolicy;
      } else if (requestedPolicy === POLICIES.PIN) {
        if (!verification.ok) {
          // Retry-time probing can also discover expiry, not just a page read.
          if (!run.versionLoss) {
            if (verification.bExpired) {
              run.versionLoss = {
                source: 'B',
                at: nowIso(),
                reason: 'retention_floor_advanced',
                lost: run.binding.b,
                floor: (verification.checks.find((c) => c.code === 'revision_expired') || {}).floor ?? null,
              };
            } else if (verification.aExpired) {
              run.versionLoss = {
                source: 'A',
                at: nowIso(),
                reason: 'snapshot_pruned',
                lost: run.binding.a,
              };
            }
          }
          run.nextRetry = this.#retryGuidance(run);
          this.#touch(run);
          this.store.saveRun(run);
          throw new CoordinatorError(
            'pinned_version_unavailable',
            'pinned versions can no longer be re-read; pin cannot continue. Choose restart (invalidate delivered pages) or mixed (declare a version boundary).',
            {
              status: 409,
              details: {
                run_id: runId,
                checks: verification.checks,
                failure: run.failure,
                emitted: run.emitted,
                options: run.nextRetry.options,
              },
            },
          );
        }
        // Verification succeeded; binding is unchanged and no contract change occurs.
        run.request.policy = requestedPolicy;
      } else if (requestedPolicy === POLICIES.RESTART) {
        invalidated = {
          epoch: run.epoch,
          pages: { from_page: run.emitted.first_page, to_page: run.emitted.last_page },
          rows: run.emitted.rows,
        };
        this.store.supersede(run.id, run.epoch);
        run.epoch += 1;
        run.cursor = { after: null, pageNo: 1, last_page_rows: null };
        run.emitted = { pages: 0, rows: 0, first_page: null, last_page: null, first_account: null, last_account: null };
        run.sourceA = {
          filter_total: null, filter_digest: null, pages: 0, rows: 0,
          last_digest: null, last_read_at: null, chain: null,
        };
        run.sourceB = {
          pages: 0, rows: 0, missing: 0, last_digest: null, last_read_at: null,
          ids_seen: 0, chain: null,
        };
        run.pin = { history: [] };
        run.failure = null;
        run.versionLoss = null;
        run.request.policy = requestedPolicy;
        await this.#bind(run, `restart after ${fromPolicy}`);
        run.warnings.push({
          code: 'restart_invalidated_prefix', at: nowIso(),
          message: `restart invalidated epoch ${invalidated.epoch} pages ${invalidated.pages.from_page}..${invalidated.pages.to_page}; consumers must discard them.`,
          invalidated,
        });
      } else {
        // MIXED: keep the delivered prefix; rebind only the source that can no
        // longer be read. The source is named either by a recorded failure or
        // by a version loss discovered during resume/retry verification.
        const lostSource = run.versionLoss && ['A', 'B'].includes(run.versionLoss.source)
          ? run.versionLoss.source
          : null;
        const failedSource = run.failure && ['A', 'B'].includes(run.failure.source)
          ? run.failure.source
          : null;
        const source = failedSource || lostSource;
        if (!source) {
          throw new CoordinatorError(
            'mixed_requires_failure',
            'mixed retry needs a recorded failure position or version loss naming the affected source; without one there is no version boundary to declare',
            { status: 409, details: { run_id: runId, failure: run.failure, version_loss: run.versionLoss } },
          );
        }
        const failedAtPage = run.failure ? run.failure.pageNo : null;
        const reason = failedSource
          ? `mixed retry after ${fromPolicy} failure at page ${failedAtPage}`
          : `mixed retry after ${fromPolicy} version loss on ${source} before page ${run.cursor.pageNo}`;
        run.request.policy = requestedPolicy;
        run.failure = null;
        run.versionLoss = null;
        await this.#rebind(run, source, reason);
        this.store.saveRun(run);
        run.warnings.push({
          code: 'version_boundary', at: nowIso(),
          message: `source ${source} rebinds at page ${run.cursor.pageNo}; pages before this boundary stay on the original versions.`,
          boundary_page: run.cursor.pageNo,
          source,
          trigger: failedSource ? 'failure' : 'version_loss',
          previous: run.pin.history.length > 1 ? run.pin.history[run.pin.history.length - 2].binding : null,
          current: run.binding,
        });
      }

      run.state = 'bound';
      run.nextRetry = { allowed: false };
      run.updatedAt = nowIso();
      if (invalidated) run.lastInvalidated = invalidated;
      this.store.saveRun(run);
      this.log(runId, 'retry', {
        policy: requestedPolicy,
        contract_change: contractChange,
        invalidated,
        verification: verification.ok,
      });
      const fresh = this.store.getRun(runId);
      return {
        run: fresh,
        verification,
        invalidated,
        contract_change: contractChange ? { from: fromPolicy, to: requestedPolicy, visible_to_consumer: hasEmitted } : null,
        acknowledgement_required: false,
      };
    });
  }

  async resume(runId) {
    return this.#withLock(runId, async () => {
      const run = this.#requireRun(runId);
      if (run.state === 'failed') {
        throw new CoordinatorError(
          'use_retry',
          'this run stopped at a failure position; choose a retry policy so the version decision is explicit',
          { status: 409, details: { run_id: runId, failure: run.failure, retry: run.nextRetry } },
        );
      }
      if (run.state !== 'cancelled') {
        throw new CoordinatorError('run_not_resumable', `run state ${run.state} cannot be resumed`, {
          status: 409, details: { run_id: runId, state: run.state },
        });
      }
      // Resume keeps the exact binding: same A snapshot, same B revision, same
      // policy, same cursor. It is only legal while both versions are still
      // re-readable with unchanged content — otherwise the run would silently
      // continue on different data, which is precisely what pin forbids.
      // A run that failed its very first bind has no pinned pair to verify; the
      // retry rebinds from scratch and nothing has been delivered yet.
      const verification = run.binding
        ? await this.#verifyPinnedVersions(run)
        : { ok: true, checks: [], aOk: true, bOk: true, aExpired: false, bExpired: false, unbounded: true };
      for (const check of verification.checks) {
        this.#recordAttempt(run, {
          phase: 'resume_verify', source: check.source, outcome: check.ok ? 'ok' : 'error',
          code: check.name, message: check.detail || null,
        });
      }
      if (!verification.ok) {
        if (!run.versionLoss) {
          run.versionLoss = verification.bExpired
            ? { source: 'B', at: nowIso(), reason: 'retention_floor_advanced', lost: run.binding.b, floor: null }
            : verification.aExpired
              ? { source: 'A', at: nowIso(), reason: 'snapshot_pruned', lost: run.binding.a }
              : { source: 'A+B', at: nowIso(), reason: 'version_unreadable', lost: run.binding, floor: null };
        }
        run.nextRetry = this.#retryGuidance(run);
        this.#touch(run);
        this.store.saveRun(run);
        throw new CoordinatorError(
          'pinned_version_unavailable',
          'cannot resume: at least one pinned version is no longer re-readable. Choose restart or mixed explicitly.',
          {
            status: 409,
            details: {
              run_id: runId, checks: verification.checks, emitted: run.emitted, options: run.nextRetry.options,
            },
          },
        );
      }
      run.state = 'running';
      run.failure = null;
      run.nextRetry = { allowed: false };
      run.versionLoss = null;
      run.updatedAt = nowIso();
      this.store.saveRun(run);
      this.log(runId, 'resumed', { cursor: run.cursor, versions: { a: run.binding.a.token, b: run.binding.b.revision } });
      return { run: this.store.getRun(runId), verification };
    });
  }

  /**
   * Re-check the pinned versions without changing anything. The page calls this
   * after the operator advances a source, so the retry options it renders are
   * based on the sources' current state rather than on stale ones.
   */
  async refreshRetryGuidance(runId) {
    return this.#withLock(runId, async () => {
      const run = this.#requireRun(runId);
      if (run.state !== 'failed' && run.state !== 'cancelled') {
        throw new CoordinatorError('run_not_retryable', `run state ${run.state} has no retry guidance to refresh`, {
          status: 409, details: { run_id: runId, state: run.state },
        });
      }
      const verification = run.binding
        ? await this.#verifyPinnedVersions(run)
        : { ok: true, checks: [], aOk: true, bOk: true, aExpired: false, bExpired: false, unbounded: true };
      if (!verification.ok && !run.versionLoss) {
        run.versionLoss = verification.bExpired
          ? {
            source: 'B', at: nowIso(), reason: 'retention_floor_advanced', lost: run.binding.b,
            floor: (verification.checks.find((c) => c.code === 'revision_expired') || {}).floor ?? null,
          }
          : verification.aExpired
            ? { source: 'A', at: nowIso(), reason: 'snapshot_pruned', lost: run.binding.a }
            : { source: 'A+B', at: nowIso(), reason: 'version_unreadable', lost: run.binding, floor: null };
      }
      // A version that becomes readable again does not erase the fact that it
      // was once unavailable; guidance reflects the current probe, loss stays.
      run.nextRetry = this.#retryGuidanceFrom(run, verification);
      this.#recordAttempt(run, {
        phase: 'guidance_probe', source: 'A+B', outcome: verification.ok ? 'ok' : 'error',
        code: verification.ok ? 'versions_readable' : 'versions_unavailable',
      });
      this.#touch(run);
      this.store.saveRun(run);
      this.log(runId, 'guidance_refreshed', {
        re_readable: verification.ok,
        version_loss: run.versionLoss,
        checks: verification.checks,
      });
      return { run: this.store.getRun(runId), verification, re_readable: verification.ok };
    });
  }

  async cancel(runId, reason = 'cancelled by client') {
    return this.#withLock(runId, async () => {
      const run = this.#requireRun(runId);
      if (run.state === 'completed') {
        throw new CoordinatorError('run_completed', 'completed runs cannot be cancelled', { status: 409 });
      }
      if (run.state === 'cancelled') return run;
      // Serialized behind any in-flight step: a page is either fully committed
      // or fully discarded, and cancellation takes effect at a page boundary.
      run.state = 'cancelled';
      run.nextRetry = this.#retryGuidance(run);
      run.updatedAt = nowIso();
      this.store.saveRun(run);
      this.log(runId, 'cancelled', { reason, emitted: run.emitted, cursor: run.cursor });
      return this.store.getRun(runId);
    });
  }

  async verify(runId) {
    const run = this.#requireRun(runId);
    const integrity = [];
    const availability = [];

    // --- Integrity: recomputed from delivered pages and recorded digests alone.
    // This is what the run promised about its output, independent of whether the
    // sources still retain those versions today.
    const pages = this.store.getPages(run.id, run.epoch);
    let accumulatedA = [];
    let pageOk = true;
    let chainOk = true;
    let chainBreak = null;
    const duplicateIds = new Set();
    let bIdDup = false;
    let previousAfter = null;
    const segments = run.pin.history;
    let segmentOk = true;
    for (let i = 0; i < pages.length; i += 1) {
      const record = pages[i];
      const body = record.body;
      const recomputed = pageDigest(body);
      const digestOk = recomputed === record.digest && recomputed === body.page_digest;
      if (!digestOk) pageOk = false;
      integrity.push({
        source: 'page', name: `page_digest_${record.pageNo}`, ok: digestOk,
        expected: record.digest, actual: recomputed,
      });
      const expectedNo = i + 1;
      const cursorOk = body.pageNo === expectedNo && body.cursor_before === previousAfter;
      if (!cursorOk && chainOk) {
        chainOk = false;
        chainBreak = { pageNo: body.pageNo, expected_pageNo: expectedNo, cursor_before: body.cursor_before, expected_cursor: previousAfter };
      }
      previousAfter = body.cursor_after;
      for (const segment of segments) {
        const covers = body.pageNo >= segment.from_page && (segment.to_page === null || body.pageNo <= segment.to_page);
        if (!covers) continue;
        const matches = body.versions
          && body.versions.a.token === segment.binding.a.token
          && body.versions.b.revision === segment.binding.b.revision
          && body.binding_digest === segment.binding.digest;
        if (!matches) segmentOk = false;
        break;
      }
      accumulatedA = accumulatedA.concat(body.rows);
      for (const id of (body.b && body.b.ids) || []) {
        if (duplicateIds.has(id)) bIdDup = true;
        duplicateIds.add(id);
      }
    }
    integrity.push({
      source: 'page', name: 'all_page_digests', ok: pageOk,
      detail: `epoch ${run.epoch}，${pages.length} 页全部可重算`,
    });
    integrity.push({
      source: 'page', name: 'cursor_chain_contiguous', ok: chainOk,
      detail: chainOk
        ? `${pages.length} 页游标首尾相接，无跳号、无重复`
        : `第 ${chainBreak.pageNo} 页前游标为 ${JSON.stringify(chainBreak.cursor_before)}，期望 ${JSON.stringify(chainBreak.expected_cursor)}`,
    });
    integrity.push({
      source: 'page', name: 'page_versions_match_segments', ok: segmentOk,
      detail: '每页记录的 A/B 版本与版本分段完全一致',
    });
    integrity.push({
      source: 'B', name: 'no_duplicate_ids_across_pages', ok: !bIdDup,
      detail: `${duplicateIds.size} 个账户 id 跨页无重复查询`,
    });
    if (run.sourceA.filter_digest) {
      if (run.state === 'completed') {
        const recomputed = accountRowsDigest(accumulatedA.map((row) => ({
          account_id: row.account_id, name: row.name, region: row.region, plan: row.plan, balance_cents: row.balance_cents,
        })).filter((row) => row.account_id));
        integrity.push({
          source: 'A', name: 'filtered_set_reconstructed', ok: recomputed === run.sourceA.filter_digest,
          expected: run.sourceA.filter_digest, actual: recomputed,
          detail: `${accumulatedA.length} 行由已输出分页重建，与绑定时的过滤集摘要比对`,
        });
        integrity.push({
          source: 'A', name: 'filtered_row_count', ok: accumulatedA.length === run.sourceA.filter_total,
          expected: run.sourceA.filter_total, actual: accumulatedA.length,
        });
      } else {
        // For a prefix, the only honest claim is that it is a strict prefix:
        // fewer rows than the filtered set, ending exactly at the saved cursor.
        integrity.push({
          source: 'A', name: 'prefix_is_strict_subset', ok: accumulatedA.length < run.sourceA.filter_total,
          expected: `< ${run.sourceA.filter_total}`, actual: accumulatedA.length,
          detail: '未完成的运行只承诺已输出前缀，不承诺覆盖整个过滤集',
        });
        integrity.push({
          source: 'A', name: 'prefix_end_matches_cursor', ok: previousAfter === run.cursor.after,
          expected: run.cursor.after, actual: previousAfter,
          detail: '前缀的最后一页游标与持久化游标一致，恢复后不会跳号或重复',
        });
      }
    }

    // --- Availability: a separate question. A run can be correctly delivered
    // and still have its versions pruned afterwards; that does not falsify the
    // output, it only means the result can no longer be re-derived from source.
    if (run.binding) {
      const live = await this.#verifyPinnedVersions(run);
      for (const check of live.checks) {
        availability.push({
          ...check,
          name: `now_${check.name}`,
          detail: check.detail || (check.ok ? '当前仍可重读' : '当前不可重读'),
        });
      }
      availability.push({
        source: 'A+B', name: 're_readable_from_source', ok: live.ok,
        detail: live.ok
          ? '两个固定版本当前仍可重读且内容未变'
          : '至少一个固定版本已不可重读；已交付结果本身仍按完整性检查独立判定',
      });
    } else {
      availability.push({ source: 'A+B', name: 'binding_exists', ok: false, detail: '运行没有绑定记录' });
    }

    const integrityOk = integrity.every((check) => check.ok);
    const complete = run.state === 'completed';
    const reReadable = availability.every((check) => check.ok);
    const versions = run.pin.history.map((entry) => ({
      from_page: entry.from_page,
      to_page: entry.to_page,
      reason: entry.reason,
      at: entry.at,
      a: entry.binding.a.token,
      b: entry.binding.b.revision,
      a_digest: entry.binding.a.content_digest,
      b_digest: entry.binding.b.content_digest,
      binding_digest: entry.binding.digest,
    }));
    const verdict = !integrityOk
      ? 'integrity_failed'
      : !complete
        ? 'prefix_verified'
        : versions.length > 1 ? 'verified_mixed' : 'verified';
    this.log(runId, 'verified', { ok: integrityOk, pages: pages.length, verdict });
    return {
      run_id: run.id,
      ok: integrityOk,
      complete,
      state: run.state,
      pages_checked: pages.length,
      verdict,
      verdict_text: {
        verified: '校验通过：全部已输出分页来自同一组固定版本',
        verified_mixed: '校验通过：完整性成立，但结果跨多个已声明的版本段',
        prefix_verified: '前缀校验通过：仅覆盖已输出部分，运行尚未完成',
        integrity_failed: '完整性校验失败：已输出分页无法自洽',
      }[verdict],
      integrity,
      availability,
      re_readable: reReadable,
      versions,
      promise: CONTRACT_TEXT[run.request.policy],
      mixed: versions.length > 1,
      warnings: run.warnings,
    };
  }

  async deleteRun(runId) {
    this.store.purgeRun(runId);
    this.log(runId, 'deleted', {});
    return { deleted: true, run_id: runId };
  }

  describeRun(run) {
    const pages = this.store.getPages(run.id, run.epoch);
    const allPages = this.store.getAllPages(run.id);
    return {
      id: run.id,
      created_at: run.createdAt,
      updated_at: run.updatedAt,
      state: run.state,
      epoch: run.epoch,
      policy: run.request.policy,
      contract: CONTRACT_TEXT[run.request.policy],
      request: run.request,
      binding: run.binding,
      version_segments: run.pin.history.map((entry) => ({
        from_page: entry.from_page,
        to_page: entry.to_page,
        reason: entry.reason,
        at: entry.at,
        a: entry.binding.a,
        b: entry.binding.b,
        binding_digest: entry.binding.digest,
      })),
      cursor: run.cursor,
      emitted: run.emitted,
      failure: run.failure,
      next_retry: run.nextRetry,
      version_loss: run.versionLoss || null,
      source_a: run.sourceA,
      source_b: run.sourceB,
      warnings: run.warnings,
      cache: run.cache,
      attempts: run.attempts.slice(-60),
      pages: pages.map((record) => ({
        pageNo: record.pageNo,
        digest: record.digest,
        created_at: record.createdAt,
        cursor_before: record.body.cursor_before,
        cursor_after: record.body.cursor_after,
        row_count: record.body.row_count,
        versions: record.body.versions,
        warnings: record.body.warnings,
        cache: record.body.cache,
        account_range: {
          from: record.body.rows[0] ? record.body.rows[0].account_id : null,
          to: record.body.rows.length ? record.body.rows[record.body.rows.length - 1].account_id : null,
        },
        a: record.body.a,
        b: record.body.b,
      })),
      superseded_pages: allPages.filter((record) => record.superseded).map((record) => ({
        epoch: record.epoch, pageNo: record.pageNo, digest: record.digest, rows: record.body.row_count,
      })),
      events: this.store.getRecentEvents(run.id, 40),
    };
  }

  getPage(runId, pageNo) {
    const run = this.#requireRun(runId);
    const page = this.store.getPages(run.id, run.epoch).find((record) => record.pageNo === Number(pageNo));
    if (!page) {
      throw new CoordinatorError('page_not_found', `page ${pageNo} is not part of the active epoch of run ${runId}`, { status: 404 });
    }
    return { run_id: runId, epoch: run.epoch, page: page.body, digest: page.digest };
  }
}

module.exports = {
  QueryCoordinator,
  POLICIES,
  POLICY_LIST,
  CONTRACT_TEXT,
  MAX_LIMIT,
  normalizePolicy,
  normalizeFilters,
  publicVersionA,
  publicVersionB,
  bindingDigest,
  pageDigest,
  cacheKey,
};
