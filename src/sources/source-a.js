'use strict';

const path = require('node:path');
const fs = require('node:fs');
const { DatabaseSync } = require('node:sqlite');

const { buildAccounts, PLANS } = require('./fixtures');
const {
  MAX_LIMIT,
  parseCsv,
  parseIntParam,
  buildIn,
  filtersDigest,
  FaultController,
  ReadLog,
  AdminLog,
  nowIso,
} = require('./common');
const { createServer, route, sendJson, readJsonBody, listen } = require('../lib/http');
const { SourceError, errorBody } = require('../lib/errors');
const { accountRowsDigest, accountRow } = require('../lib/source-digests');
const { sha256, canonical } = require('../lib/canonical');

const SERVICE = 'source-a-snapshot-accounts';

class SourceA {
  constructor({ dbPath }) {
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    this.db = new DatabaseSync(dbPath);
    this.db.exec('PRAGMA journal_mode = WAL');
    this.db.exec('PRAGMA synchronous = FULL');
    this.db.exec('PRAGMA busy_timeout = 5000');
    this.faults = new FaultController();
    this.reads = new ReadLog();
    this.admin = new AdminLog();
    this.#migrate();
    this.#seedIfEmpty();
  }

  #migrate() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS meta (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        data_seq INTEGER NOT NULL,
        seeded_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS accounts (
        account_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        region TEXT NOT NULL,
        plan TEXT NOT NULL,
        balance_cents INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS snapshots (
        token TEXT PRIMARY KEY,
        created_at TEXT NOT NULL,
        data_seq INTEGER NOT NULL,
        row_count INTEGER NOT NULL,
        content_digest TEXT NOT NULL,
        reason TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS snapshot_rows (
        token TEXT NOT NULL,
        account_id TEXT NOT NULL,
        name TEXT NOT NULL,
        region TEXT NOT NULL,
        plan TEXT NOT NULL,
        balance_cents INTEGER NOT NULL,
        PRIMARY KEY (token, account_id)
      );
      CREATE INDEX IF NOT EXISTS snapshot_rows_token ON snapshot_rows(token, account_id);
    `);
  }

  #seedIfEmpty() {
    const meta = this.db.prepare('SELECT * FROM meta WHERE id = 1').get();
    if (meta) return;
    const accounts = buildAccounts(240);
    this.db.exec('BEGIN');
    try {
      this.db.prepare('INSERT INTO meta (id, data_seq, seeded_at) VALUES (1, ?, ?)').run(1, nowIso());
      const insert = this.db.prepare('INSERT INTO accounts (account_id, name, region, plan, balance_cents) VALUES (?, ?, ?, ?, ?)');
      for (const acct of accounts) insert.run(acct.account_id, acct.name, acct.region, acct.plan, acct.balance_cents);
      this.db.exec('COMMIT');
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw err;
    }
    this.createSnapshot('seed');
    this.admin.push('seed', { accounts: accounts.length });
  }

  meta() {
    return this.db.prepare('SELECT * FROM meta WHERE id = 1').get();
  }

  latestSnapshotRow() {
    return this.db.prepare('SELECT * FROM snapshots ORDER BY data_seq DESC, created_at DESC LIMIT 1').get();
  }

  snapshotMeta(row) {
    return {
      token: row.token,
      created_at: row.created_at,
      data_seq: row.data_seq,
      row_count: row.row_count,
      content_digest: row.content_digest,
      reason: row.reason,
    };
  }

  createSnapshot(reason = 'manual') {
    const data = this.meta();
    const rows = this.db.prepare('SELECT * FROM accounts ORDER BY account_id').all();
    const digest = accountRowsDigest(rows);
    const token = `snap-${String(data.data_seq).padStart(4, '0')}-${digest.slice(0, 10)}`;
    const existing = this.db.prepare('SELECT * FROM snapshots WHERE token = ?').get(token);
    if (existing) return this.snapshotMeta(existing);
    const createdAt = nowIso();
    this.db.exec('BEGIN');
    try {
      this.db.prepare('INSERT INTO snapshots (token, created_at, data_seq, row_count, content_digest, reason) VALUES (?, ?, ?, ?, ?, ?)')
        .run(token, createdAt, data.data_seq, rows.length, digest, reason);
      const insert = this.db.prepare('INSERT INTO snapshot_rows (token, account_id, name, region, plan, balance_cents) VALUES (?, ?, ?, ?, ?, ?)');
      for (const row of rows) insert.run(token, row.account_id, row.name, row.region, row.plan, row.balance_cents);
      this.db.exec('COMMIT');
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw err;
    }
    this.admin.push('snapshot', { token, rows: rows.length, data_seq: data.data_seq });
    return this.snapshotMeta({ token, created_at: createdAt, data_seq: data.data_seq, row_count: rows.length, content_digest: digest, reason });
  }

  requireSnapshot(token) {
    const row = this.db.prepare('SELECT * FROM snapshots WHERE token = ?').get(token);
    if (!row) {
      throw new SourceError('snapshot_not_found', `snapshot ${token} is not retained by source A`, {
        status: 404,
        retryable: false,
        details: { snapshot: token },
      });
    }
    return this.snapshotMeta(row);
  }

  applyFilters(token, filters) {
    const where = ['token = ?'];
    const params = [token];
    if (filters.region) { where.push('region = ?'); params.push(filters.region); }
    if (filters.plan) { where.push('plan = ?'); params.push(filters.plan); }
    if (filters.min_balance != null) { where.push('balance_cents >= ?'); params.push(filters.min_balance); }
    return { sql: where.join(' AND '), params };
  }

  readAccounts({ snapshotToken, filters, after, limit }) {
    const meta = this.requireSnapshot(snapshotToken);
    const { sql, params } = this.applyFilters(meta.token, filters);
    const filteredTotal = this.db.prepare(`SELECT COUNT(*) AS n FROM snapshot_rows WHERE ${sql}`).get(...params).n;
    const allRows = this.db.prepare(`SELECT * FROM snapshot_rows WHERE ${sql} ORDER BY account_id`).all(...params);
    const filteredDigest = accountRowsDigest(allRows);
    const afterClause = after ? ' AND account_id > ?' : '';
    const pageRows = this.db
      .prepare(`SELECT * FROM snapshot_rows WHERE ${sql}${afterClause} ORDER BY account_id LIMIT ?`)
      .all(...params, ...(after ? [after] : []), limit);
    const complete = pageRows.length < limit
      || (pageRows.length > 0 && pageRows[pageRows.length - 1].account_id === allRows[allRows.length - 1].account_id);
    const nextAfter = complete ? null : pageRows[pageRows.length - 1].account_id;
    return {
      source: 'A',
      service: SERVICE,
      snapshot: meta,
      filters,
      filters_digest: filtersDigest(filters),
      filtered: { total: filteredTotal, digest: filteredDigest },
      page: {
        after: after || null,
        limit,
        rows: pageRows.map(accountRow),
        row_count: pageRows.length,
        digest: accountRowsDigest(pageRows),
        next_after: nextAfter,
        complete,
      },
      read_at: nowIso(),
    };
  }

  readAccountById({ snapshotToken, accountId }) {
    const meta = this.requireSnapshot(snapshotToken);
    const row = this.db.prepare('SELECT * FROM snapshot_rows WHERE token = ? AND account_id = ?').get(meta.token, accountId);
    if (!row) {
      throw new SourceError('account_not_found', `account ${accountId} is absent from snapshot ${meta.token}`, {
        status: 404,
        retryable: false,
        details: { snapshot: meta.token, account_id: accountId },
      });
    }
    return {
      source: 'A',
      service: SERVICE,
      snapshot: meta,
      row: accountRow(row),
      row_digest: sha256(canonical(accountRow(row))),
      read_at: nowIso(),
    };
  }

  updateAccounts({ count = 17, ids = null, snapshot = true, reason = 'admin update' }) {
    const seq = this.meta().data_seq + 1;
    let targets;
    if (ids && ids.length) {
      const placeholders = buildIn(ids);
      targets = this.db.prepare(`SELECT * FROM accounts WHERE account_id IN (${placeholders}) ORDER BY account_id`).all(...ids);
    } else {
      const all = this.db.prepare('SELECT * FROM accounts ORDER BY account_id').all();
      const stride = Math.max(3, Math.floor(all.length / Math.max(1, Math.min(count, all.length))));
      targets = all.filter((_, index) => index % stride === 0).slice(0, Math.max(1, count));
    }
    const changes = [];
    this.db.exec('BEGIN');
    try {
      const update = this.db.prepare('UPDATE accounts SET balance_cents = ?, plan = ? WHERE account_id = ?');
      for (const row of targets) {
        const nextBalance = Math.max(1_000, row.balance_cents + ((seq * 7919) % 250_000) - 100_000);
        const nextPlan = PLANS[(PLANS.indexOf(row.plan) + 1 + (seq % 2)) % PLANS.length];
        update.run(nextBalance, nextPlan, row.account_id);
        if (changes.length < 25) {
          changes.push({ account_id: row.account_id, balance_cents: [row.balance_cents, nextBalance], plan: [row.plan, nextPlan] });
        }
      }
      this.db.prepare('UPDATE meta SET data_seq = ? WHERE id = 1').run(seq);
      this.db.exec('COMMIT');
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw err;
    }
    const changedCount = targets.length;
    const snap = snapshot ? this.createSnapshot(reason) : null;
    this.admin.push('update', { data_seq: seq, changed: changedCount, snapshot: snap && snap.token });
    return { source: 'A', data_seq: seq, changed_count: changedCount, changes, snapshot: snap };
  }

  pruneSnapshots(keep) {
    const all = this.db.prepare('SELECT * FROM snapshots ORDER BY data_seq DESC, created_at DESC').all();
    const removed = all.slice(Math.max(0, keep)).map((row) => row.token);
    if (removed.length) {
      const placeholders = buildIn(removed);
      this.db.exec('BEGIN');
      try {
        this.db.prepare(`DELETE FROM snapshot_rows WHERE token IN (${placeholders})`).run(...removed);
        this.db.prepare(`DELETE FROM snapshots WHERE token IN (${placeholders})`).run(...removed);
        this.db.exec('COMMIT');
      } catch (err) {
        this.db.exec('ROLLBACK');
        throw err;
      }
      this.admin.push('prune_snapshots', { removed });
    }
    return { source: 'A', retained: this.db.prepare('SELECT token FROM snapshots ORDER BY data_seq DESC').all().map((r) => r.token), removed };
  }

  reseed() {
    const accounts = buildAccounts(240);
    this.db.exec('BEGIN');
    try {
      this.db.exec('DELETE FROM snapshot_rows; DELETE FROM snapshots; DELETE FROM accounts;');
      const insert = this.db.prepare('INSERT INTO accounts (account_id, name, region, plan, balance_cents) VALUES (?, ?, ?, ?, ?)');
      for (const acct of accounts) insert.run(acct.account_id, acct.name, acct.region, acct.plan, acct.balance_cents);
      this.db.prepare('UPDATE meta SET data_seq = 1, seeded_at = ? WHERE id = 1').run(nowIso());
      this.db.exec('COMMIT');
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw err;
    }
    this.faults.reset();
    this.reads.clear();
    this.admin.clear();
    this.admin.push('reseed', { accounts: accounts.length });
    return { source: 'A', data_seq: 1, accounts: accounts.length, snapshot: this.createSnapshot('reseed') };
  }

  state() {
    const meta = this.meta();
    const snapshots = this.db.prepare('SELECT * FROM snapshots ORDER BY data_seq DESC, created_at DESC').all().map((row) => this.snapshotMeta(row));
    return {
      source: 'A',
      service: SERVICE,
      data_seq: meta.data_seq,
      seeded_at: meta.seeded_at,
      account_count: this.db.prepare('SELECT COUNT(*) AS n FROM accounts').get().n,
      snapshots,
      faults: this.faults.describe(),
      reads: this.reads.recent(20),
      admin: this.admin.recent(10),
    };
  }

  async start(port) {
    const routes = [
      route('GET', '/v1/health', async (req, res) => {
        sendJson(res, 200, { ok: true, source: 'A', service: SERVICE, data_seq: this.meta().data_seq });
      }),
      route('GET', '/v1/snapshot', async (req, res) => {
        const fault = await this.faults.beforeRead('probe');
        if (fault.fail) {
          this.reads.push({ kind: 'probe', status: fault.fail.status, code: fault.fail.code });
          throw new SourceError(fault.fail.code, fault.fail.message, { status: fault.fail.status, details: { at: fault.fail.at } });
        }
        let row = this.latestSnapshotRow();
        if (!row) {
          const created = this.createSnapshot('auto-latest');
          row = this.db.prepare('SELECT * FROM snapshots WHERE token = ?').get(created.token);
        }
        this.reads.push({ kind: 'probe', status: 200, snapshot: row.token });
        sendJson(res, 200, { source: 'A', service: SERVICE, snapshot: this.snapshotMeta(row), read_at: nowIso() });
      }),
      route('GET', '/v1/snapshot/:token', async (req, res, { params }) => {
        const fault = await this.faults.beforeRead('probe');
        if (fault.fail) {
          this.reads.push({ kind: 'probe', status: fault.fail.status, code: fault.fail.code, snapshot: params.token });
          throw new SourceError(fault.fail.code, fault.fail.message, { status: fault.fail.status, details: { at: fault.fail.at } });
        }
        const meta = this.requireSnapshot(params.token);
        this.reads.push({ kind: 'probe', status: 200, snapshot: meta.token });
        sendJson(res, 200, { source: 'A', service: SERVICE, snapshot: meta, retained: true, read_at: nowIso() });
      }),
      route('GET', '/v1/accounts', async (req, res, { url }) => {
        const fault = await this.faults.beforeRead('read');
        if (fault.fail) {
          this.reads.push({ kind: 'read', status: fault.fail.status, code: fault.fail.code });
          throw new SourceError(fault.fail.code, fault.fail.message, { status: fault.fail.status, details: { at: fault.fail.at } });
        }
        const snapshotToken = url.searchParams.get('snapshot');
        const token = snapshotToken || (this.latestSnapshotRow() || {}).token;
        if (!token) {
          const created = this.createSnapshot('auto-read');
          const result = this.readAccounts({ snapshotToken: created.token, filters: parseFilters(url), after: parseAfter(url), limit: parseLimit(url) });
          this.reads.push({ kind: 'read', status: 200, snapshot: created.token, rows: result.page.row_count, after: result.page.after });
          return sendJson(res, 200, result);
        }
        const result = this.readAccounts({
          snapshotToken: token,
          filters: parseFilters(url),
          after: parseAfter(url),
          limit: parseLimit(url),
        });
        this.reads.push({ kind: 'read', status: 200, snapshot: result.snapshot.token, rows: result.page.row_count, after: result.page.after });
        sendJson(res, 200, result);
      }),
      route('GET', '/v1/accounts/:id', async (req, res, { url, params }) => {
        const fault = await this.faults.beforeRead('read');
        if (fault.fail) {
          this.reads.push({ kind: 'read', status: fault.fail.status, code: fault.fail.code });
          throw new SourceError(fault.fail.code, fault.fail.message, { status: fault.fail.status, details: { at: fault.fail.at } });
        }
        const token = url.searchParams.get('snapshot') || (this.latestSnapshotRow() || {}).token;
        if (!token) throw new SourceError('snapshot_not_found', 'no snapshot exists; create one first', { status: 404, retryable: false });
        const result = this.readAccountById({ snapshotToken: token, accountId: params.id });
        this.reads.push({ kind: 'read', status: 200, snapshot: token, rows: 1, account: params.id });
        sendJson(res, 200, result);
      }),
      route('GET', '/admin/state', async (req, res) => {
        sendJson(res, 200, this.state());
      }),
      route('POST', '/admin/snapshot', async (req, res) => {
        const body = await readJsonBody(req);
        const snap = this.createSnapshot(body.reason ? String(body.reason) : 'manual');
        sendJson(res, 200, { source: 'A', snapshot: snap });
      }),
      route('POST', '/admin/update', async (req, res) => {
        const body = await readJsonBody(req);
        const count = parseIntParam(body.count, 'count', { min: 1, max: 240, fallback: 17 });
        const ids = parseCsv(body.ids);
        const snapshot = body.snapshot === false ? false : true;
        const result = this.updateAccounts({ count, ids, snapshot, reason: body.reason || 'admin update' });
        sendJson(res, 200, result);
      }),
      route('POST', '/admin/prune_snapshots', async (req, res) => {
        const body = await readJsonBody(req);
        const keep = parseIntParam(body.keep, 'keep', { min: 0, max: 1000, fallback: 0 });
        sendJson(res, 200, this.pruneSnapshots(keep));
      }),
      route('POST', '/admin/faults', async (req, res) => {
        const body = await readJsonBody(req);
        const config = this.faults.configure(body);
        this.admin.push('faults', config);
        sendJson(res, 200, { source: 'A', faults: config });
      }),
      route('POST', '/admin/reset', async (req, res) => {
        const body = await readJsonBody(req);
        if (body.reseed) {
          sendJson(res, 200, this.reseed());
          return;
        }
        this.faults.reset();
        this.reads.clear();
        this.admin.push('reset', {});
        sendJson(res, 200, { source: 'A', reset: true, faults: this.faults.describe() });
      }),
    ];
    this.server = createServer(routes, {
      onError: (err, req, res) => {
        const body = errorBody(err);
        sendJson(res, body.status, { error: body });
      },
    });
    await listen(this.server, port);
    this.port = this.server.address().port;
    return this;
  }

  close() {
    if (this.server) this.server.close();
    this.db.close();
  }
}

function parseLimit(url) {
  return parseIntParam(url.searchParams.get('limit'), 'limit', { min: 1, max: MAX_LIMIT, fallback: 25 });
}

function parseAfter(url) {
  const after = url.searchParams.get('after');
  return after ? String(after).slice(0, 64) : null;
}

function parseFilters(url) {
  const region = url.searchParams.get('region');
  const plan = url.searchParams.get('plan');
  const minBalanceRaw = url.searchParams.get('min_balance');
  return {
    region: region ? String(region).slice(0, 32) : null,
    plan: plan ? String(plan).slice(0, 32) : null,
    min_balance: minBalanceRaw ? Number(minBalanceRaw) : null,
  };
}

function createSourceA(options) {
  return new SourceA(options);
}

module.exports = { SourceA, createSourceA, SERVICE };
