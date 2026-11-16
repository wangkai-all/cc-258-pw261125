'use strict';

const path = require('node:path');
const fs = require('node:fs');
const { DatabaseSync } = require('node:sqlite');

const { buildAccounts, buildActivity } = require('./fixtures');
const {
  MAX_LIMIT,
  parseCsv,
  parseIntParam,
  buildIn,
  FaultController,
  ReadLog,
  AdminLog,
  nowIso,
} = require('./common');
const { createServer, route, sendJson, readJsonBody, listen } = require('../lib/http');
const { SourceError, errorBody } = require('../lib/errors');
const { activityRowsDigest, activityIdsDigest, activityRow } = require('../lib/source-digests');
const { sha256, canonical } = require('../lib/canonical');

const SERVICE = 'source-b-revision-activity';
const DEFAULT_RETENTION = 10;

class SourceB {
  constructor({ dbPath, retention = DEFAULT_RETENTION }) {
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    this.db = new DatabaseSync(dbPath);
    this.db.exec('PRAGMA journal_mode = WAL');
    this.db.exec('PRAGMA synchronous = FULL');
    this.db.exec('PRAGMA busy_timeout = 5000');
    this.faults = new FaultController();
    this.reads = new ReadLog();
    this.admin = new AdminLog();
    this.#migrate();
    this.#seedIfEmpty(retention);
  }

  #migrate() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS meta (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        current_revision INTEGER NOT NULL,
        retention INTEGER NOT NULL,
        floor INTEGER NOT NULL,
        seeded_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS activity (
        account_id TEXT PRIMARY KEY,
        segment TEXT NOT NULL,
        risk_score INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS revisions (
        revision INTEGER PRIMARY KEY,
        created_at TEXT NOT NULL,
        row_count INTEGER NOT NULL,
        content_digest TEXT NOT NULL,
        reason TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS revision_rows (
        revision INTEGER NOT NULL,
        account_id TEXT NOT NULL,
        segment TEXT NOT NULL,
        risk_score INTEGER NOT NULL,
        PRIMARY KEY (revision, account_id)
      );
      CREATE INDEX IF NOT EXISTS revision_rows_lookup ON revision_rows(revision, account_id);
    `);
  }

  #seedIfEmpty(retention) {
    const meta = this.db.prepare('SELECT * FROM meta WHERE id = 1').get();
    if (meta) return;
    const accounts = buildAccounts(240);
    const rows = buildActivity(accounts);
    this.db.exec('BEGIN');
    try {
      this.db.prepare('INSERT INTO meta (id, current_revision, retention, floor, seeded_at) VALUES (1, 1, ?, 1, ?)')
        .run(retention, nowIso());
      const insertActivity = this.db.prepare('INSERT INTO activity (account_id, segment, risk_score) VALUES (?, ?, ?)');
      const insertHistory = this.db.prepare('INSERT INTO revision_rows (revision, account_id, segment, risk_score) VALUES (1, ?, ?, ?)');
      for (const row of rows) {
        insertActivity.run(row.account_id, row.segment, row.risk_score);
        insertHistory.run(row.account_id, row.segment, row.risk_score);
      }
      const digest = activityRowsDigest(rows);
      this.db.prepare('INSERT INTO revisions (revision, created_at, row_count, content_digest, reason) VALUES (1, ?, ?, ?, ?)')
        .run(nowIso(), rows.length, digest, 'seed');
      this.db.exec('COMMIT');
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw err;
    }
    this.admin.push('seed', { rows: rows.length, retention });
  }

  meta() {
    return this.db.prepare('SELECT * FROM meta WHERE id = 1').get();
  }

  revisionMeta(revision) {
    const row = this.db.prepare('SELECT * FROM revisions WHERE revision = ?').get(revision);
    if (!row) return null;
    return {
      revision: row.revision,
      created_at: row.created_at,
      row_count: row.row_count,
      content_digest: row.content_digest,
      reason: row.reason,
    };
  }

  latestRevisionMeta() {
    const meta = this.meta();
    return this.revisionMeta(meta.current_revision);
  }

  requireRevision(revision) {
    const meta = this.meta();
    if (!Number.isInteger(revision) || revision < 1) {
      throw new SourceError('invalid_revision', 'revision must be a positive integer', {
        status: 400,
        retryable: false,
      });
    }
    if (revision > meta.current_revision) {
      throw new SourceError('revision_unknown', `revision ${revision} has not been committed yet`, {
        status: 404,
        retryable: false,
        details: { requested: revision, current: meta.current_revision, floor: meta.floor },
      });
    }
    if (revision < meta.floor) {
      throw new SourceError('revision_expired', `revision ${revision} fell below the retention floor ${meta.floor}`, {
        status: 410,
        retryable: false,
        details: { requested: revision, current: meta.current_revision, floor: meta.floor },
      });
    }
    const found = this.revisionMeta(revision);
    if (!found) {
      throw new SourceError('revision_expired', `revision ${revision} is not retained`, {
        status: 410,
        retryable: false,
        details: { requested: revision, current: meta.current_revision, floor: meta.floor },
      });
    }
    return found;
  }

  readActivity({ revision, after, limit }) {
    const meta = this.requireRevision(revision);
    const allRows = this.db
      .prepare('SELECT * FROM revision_rows WHERE revision = ? ORDER BY account_id')
      .all(meta.revision);
    const where = ['revision = ?'];
    const params = [meta.revision];
    if (after) {
      where.push('account_id > ?');
      params.push(after);
    }
    const rows = this.db
      .prepare(`SELECT * FROM revision_rows WHERE ${where.join(' AND ')} ORDER BY account_id LIMIT ?`)
      .all(...params, limit);
    const complete = rows.length < limit;
    const nextAfter = complete ? null : rows[rows.length - 1].account_id;
    return {
      source: 'B',
      service: SERVICE,
      revision: meta,
      revision_floor: this.meta().floor,
      retention: this.meta().retention,
      page: {
        after: after || null,
        limit,
        rows: rows.map(activityRow),
        row_count: rows.length,
        digest: activityRowsDigest(rows),
        next_after: nextAfter,
        complete,
      },
      read_at: nowIso(),
    };
  }

  lookupActivity({ revision, ids }) {
    const meta = this.requireRevision(revision);
    if (!ids || ids.length === 0) {
      throw new SourceError('ids_required', 'ids must contain at least one account id', { status: 400, retryable: false });
    }
    if (ids.length > MAX_LIMIT) {
      throw new SourceError('too_many_ids', `at most ${MAX_LIMIT} ids per lookup`, { status: 400, retryable: false });
    }
    const unique = [...new Set(ids)];
    const placeholders = buildIn(unique);
    const rows = this.db
      .prepare(`SELECT * FROM revision_rows WHERE revision = ? AND account_id IN (${placeholders})`)
      .all(meta.revision, ...unique);
    const byId = new Map(rows.map((row) => [row.account_id, row]));
    const ordered = ids.map((id) => {
      const row = byId.get(id);
      return row ? activityRow(row) : { account_id: id, segment: null, risk_score: null, present: false };
    });
    return {
      source: 'B',
      service: SERVICE,
      revision: meta,
      revision_floor: this.meta().floor,
      ids,
      rows: ordered,
      missing: ids.filter((id) => !byId.has(id)),
      digest: activityIdsDigest(ids, byId),
      read_at: nowIso(),
    };
  }

  bump({ count = 23, ids = null, reason = 'admin bump' }) {
    const meta = this.meta();
    const revision = meta.current_revision + 1;
    let targets;
    if (ids && ids.length) {
      const placeholders = buildIn(ids);
      targets = this.db.prepare(`SELECT * FROM activity WHERE account_id IN (${placeholders}) ORDER BY account_id`).all(...ids);
    } else {
      const all = this.db.prepare('SELECT * FROM activity ORDER BY account_id').all();
      const stride = Math.max(3, Math.floor(all.length / Math.max(1, Math.min(count, all.length))));
      targets = all.filter((_, index) => index % stride === 0).slice(0, Math.max(1, count));
    }
    const changes = [];
    this.db.exec('BEGIN');
    try {
      const update = this.db.prepare('UPDATE activity SET segment = ?, risk_score = ? WHERE account_id = ?');
      for (const row of targets) {
        const nextRisk = ((row.risk_score + revision * 13) % 99) + 1;
        const nextSegment = row.segment;
        update.run(nextSegment, nextRisk, row.account_id);
        if (changes.length < 25) changes.push({ account_id: row.account_id, risk_score: [row.risk_score, nextRisk] });
      }
      const rows = this.db.prepare('SELECT * FROM activity ORDER BY account_id').all();
      const digest = activityRowsDigest(rows);
      const createdAt = nowIso();
      this.db.prepare('INSERT INTO revisions (revision, created_at, row_count, content_digest, reason) VALUES (?, ?, ?, ?, ?)')
        .run(revision, createdAt, rows.length, digest, reason);
      const insert = this.db.prepare('INSERT INTO revision_rows (revision, account_id, segment, risk_score) VALUES (?, ?, ?, ?)');
      for (const row of rows) insert.run(revision, row.account_id, row.segment, row.risk_score);
      const floor = Math.max(1, revision - meta.retention + 1);
      this.db.prepare('DELETE FROM revision_rows WHERE revision < ?').run(floor);
      this.db.prepare('DELETE FROM revisions WHERE revision < ?').run(floor);
      this.db.prepare('UPDATE meta SET current_revision = ?, floor = ? WHERE id = 1').run(revision, floor);
      this.db.exec('COMMIT');
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw err;
    }
    this.admin.push('bump', { revision, changed: targets.length });
    return {
      source: 'B',
      revision: this.revisionMeta(revision),
      changed_count: targets.length,
      changes,
      floor: this.meta().floor,
      retention: this.meta().retention,
    };
  }

  setRetention(retention) {
    const meta = this.meta();
    const floor = Math.max(1, meta.current_revision - retention + 1);
    this.db.exec('BEGIN');
    try {
      this.db.prepare('DELETE FROM revision_rows WHERE revision < ?').run(floor);
      this.db.prepare('DELETE FROM revisions WHERE revision < ?').run(floor);
      this.db.prepare('UPDATE meta SET retention = ?, floor = ? WHERE id = 1').run(retention, floor);
      this.db.exec('COMMIT');
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw err;
    }
    this.admin.push('retention', { retention, floor });
    return { source: 'B', retention, floor, current: meta.current_revision };
  }

  reseed() {
    const accounts = buildAccounts(240);
    const rows = buildActivity(accounts);
    this.db.exec('BEGIN');
    try {
      this.db.exec('DELETE FROM revision_rows; DELETE FROM revisions; DELETE FROM activity;');
      const insertActivity = this.db.prepare('INSERT INTO activity (account_id, segment, risk_score) VALUES (?, ?, ?)');
      const insertHistory = this.db.prepare('INSERT INTO revision_rows (revision, account_id, segment, risk_score) VALUES (1, ?, ?, ?)');
      for (const row of rows) {
        insertActivity.run(row.account_id, row.segment, row.risk_score);
        insertHistory.run(row.account_id, row.segment, row.risk_score);
      }
      const digest = activityRowsDigest(rows);
      this.db.prepare('INSERT INTO revisions (revision, created_at, row_count, content_digest, reason) VALUES (1, ?, ?, ?, ?)')
        .run(nowIso(), rows.length, digest, 'reseed');
      this.db.prepare('UPDATE meta SET current_revision = 1, floor = 1, seeded_at = ? WHERE id = 1').run(nowIso());
      this.db.exec('COMMIT');
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw err;
    }
    this.faults.reset();
    this.reads.clear();
    this.admin.clear();
    this.admin.push('reseed', { rows: rows.length });
    return { source: 'B', revision: this.revisionMeta(1), floor: 1, rows: rows.length };
  }

  state() {
    const meta = this.meta();
    const revisions = this.db.prepare('SELECT * FROM revisions ORDER BY revision DESC').all().map((row) => ({
      revision: row.revision,
      created_at: row.created_at,
      row_count: row.row_count,
      content_digest: row.content_digest,
      reason: row.reason,
    }));
    return {
      source: 'B',
      service: SERVICE,
      current_revision: meta.current_revision,
      floor: meta.floor,
      retention: meta.retention,
      seeded_at: meta.seeded_at,
      row_count: this.db.prepare('SELECT COUNT(*) AS n FROM activity').get().n,
      revisions,
      faults: this.faults.describe(),
      reads: this.reads.recent(20),
      admin: this.admin.recent(10),
    };
  }

  async start(port) {
    const routes = [
      route('GET', '/v1/health', async (req, res) => {
        sendJson(res, 200, { ok: true, source: 'B', service: SERVICE, current_revision: this.meta().current_revision });
      }),
      route('GET', '/v1/revision', async (req, res) => {
        const fault = await this.faults.beforeRead('probe');
        if (fault.fail) {
          this.reads.push({ kind: 'probe', status: fault.fail.status, code: fault.fail.code });
          throw new SourceError(fault.fail.code, fault.fail.message, { status: fault.fail.status, details: { at: fault.fail.at } });
        }
        const meta = this.latestRevisionMeta();
        this.reads.push({ kind: 'probe', status: 200, revision: meta.revision });
        sendJson(res, 200, {
          source: 'B',
          service: SERVICE,
          revision: meta,
          current_revision: meta.revision,
          floor: this.meta().floor,
          retention: this.meta().retention,
          read_at: nowIso(),
        });
      }),
      route('GET', '/v1/revision/:rev', async (req, res, { params }) => {
        const fault = await this.faults.beforeRead('probe');
        if (fault.fail) {
          this.reads.push({ kind: 'probe', status: fault.fail.status, code: fault.fail.code, revision: params.rev });
          throw new SourceError(fault.fail.code, fault.fail.message, { status: fault.fail.status, details: { at: fault.fail.at } });
        }
        const revision = Number(params.rev);
        const meta = this.requireRevision(revision);
        this.reads.push({ kind: 'probe', status: 200, revision: meta.revision });
        sendJson(res, 200, {
          source: 'B',
          service: SERVICE,
          revision: meta,
          current_revision: this.meta().current_revision,
          floor: this.meta().floor,
          retention: this.meta().retention,
          retained: true,
          read_at: nowIso(),
        });
      }),
      route('GET', '/v1/activity', async (req, res, { url }) => {
        const fault = await this.faults.beforeRead('read');
        if (fault.fail) {
          this.reads.push({ kind: 'read', status: fault.fail.status, code: fault.fail.code });
          throw new SourceError(fault.fail.code, fault.fail.message, { status: fault.fail.status, details: { at: fault.fail.at } });
        }
        const rawRevision = url.searchParams.get('revision');
        if (rawRevision === null || rawRevision === '') {
          throw new SourceError('revision_required', 'source B is revision-only: every read must name a revision', {
            status: 400,
            retryable: false,
          });
        }
        const revision = parseIntParam(rawRevision, 'revision', { min: 1, max: 1_000_000 });
        const idsRaw = url.searchParams.get('ids');
        if (idsRaw) {
          const ids = parseCsv(idsRaw, MAX_LIMIT);
          const result = this.lookupActivity({ revision, ids });
          this.reads.push({ kind: 'read', status: 200, revision, rows: result.rows.length, ids: ids.length });
          sendJson(res, 200, result);
          return;
        }
        const after = url.searchParams.get('after') ? String(url.searchParams.get('after')).slice(0, 64) : null;
        const limit = parseIntParam(url.searchParams.get('limit'), 'limit', { min: 1, max: MAX_LIMIT, fallback: 25 });
        const result = this.readActivity({ revision, after, limit });
        this.reads.push({ kind: 'read', status: 200, revision, rows: result.page.row_count, after });
        sendJson(res, 200, result);
      }),
      route('GET', '/v1/activity/:id', async (req, res, { url, params }) => {
        const fault = await this.faults.beforeRead('read');
        if (fault.fail) {
          this.reads.push({ kind: 'read', status: fault.fail.status, code: fault.fail.code });
          throw new SourceError(fault.fail.code, fault.fail.message, { status: fault.fail.status, details: { at: fault.fail.at } });
        }
        const rawRevision = url.searchParams.get('revision');
        if (!rawRevision) {
          throw new SourceError('revision_required', 'source B is revision-only: every read must name a revision', {
            status: 400, retryable: false,
          });
        }
        const revision = parseIntParam(rawRevision, 'revision', { min: 1, max: 1_000_000 });
        const result = this.lookupActivity({ revision, ids: [params.id] });
        this.reads.push({ kind: 'read', status: 200, revision, rows: result.rows.length, account: params.id });
        sendJson(res, 200, result);
      }),
      route('GET', '/admin/state', async (req, res) => sendJson(res, 200, this.state())),
      route('POST', '/admin/bump', async (req, res) => {
        const body = await readJsonBody(req);
        const count = parseIntParam(body.count, 'count', { min: 1, max: 240, fallback: 23 });
        const ids = parseCsv(body.ids);
        sendJson(res, 200, this.bump({ count, ids, reason: body.reason || 'admin bump' }));
      }),
      route('POST', '/admin/settings', async (req, res) => {
        const body = await readJsonBody(req);
        const retention = parseIntParam(body.retention, 'retention', { min: 1, max: 1000 });
        sendJson(res, 200, this.setRetention(retention));
      }),
      route('POST', '/admin/faults', async (req, res) => {
        const body = await readJsonBody(req);
        const config = this.faults.configure(body);
        this.admin.push('faults', config);
        sendJson(res, 200, { source: 'B', faults: config });
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
        sendJson(res, 200, { source: 'B', reset: true, faults: this.faults.describe() });
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

function createSourceB(options) {
  return new SourceB(options);
}

module.exports = { SourceB, createSourceB, SERVICE, DEFAULT_RETENTION };
