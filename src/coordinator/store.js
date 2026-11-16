'use strict';

const { DatabaseSync } = require('node:sqlite');

/**
 * Persistent coordinator state. SQLite in WAL mode; the coordinator process
 * can restart and resume a run from its pinned versions, emitted cursor and
 * cached page material.
 */
class Store {
  constructor(file) {
    this.db = new DatabaseSync(file);
    this.db.exec('PRAGMA journal_mode = WAL');
    this.db.exec('PRAGMA synchronous = FULL');
    this.db.exec('PRAGMA busy_timeout = 5000');
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS runs (
        id TEXT PRIMARY KEY,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        state TEXT NOT NULL,
        epoch INTEGER NOT NULL,
        request_json TEXT NOT NULL,
        binding_json TEXT,
        pin_json TEXT,
        cursor_json TEXT NOT NULL,
        emitted_json TEXT NOT NULL,
        failure_json TEXT,
        source_a_json TEXT NOT NULL,
        source_b_json TEXT NOT NULL,
        next_retry_json TEXT NOT NULL,
        warnings_json TEXT NOT NULL,
        cache_json TEXT NOT NULL,
        attempts_json TEXT NOT NULL,
        version_loss_json TEXT
      );
      CREATE TABLE IF NOT EXISTS pages (
        run_id TEXT NOT NULL,
        epoch INTEGER NOT NULL,
        page_no INTEGER NOT NULL,
        body_json TEXT NOT NULL,
        digest TEXT NOT NULL,
        created_at TEXT NOT NULL,
        superseded INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (run_id, epoch, page_no)
      );
      CREATE TABLE IF NOT EXISTS page_cache (
        cache_key TEXT PRIMARY KEY,
        slice_json TEXT NOT NULL,
        slice_digest TEXT NOT NULL,
        policy TEXT NOT NULL,
        versions_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        hits INTEGER NOT NULL DEFAULT 0,
        last_hit_at TEXT
      );
      CREATE TABLE IF NOT EXISTS events (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL,
        at TEXT NOT NULL,
        kind TEXT NOT NULL,
        body_json TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS events_run ON events(run_id, seq);
    `);
    // Forward migration for databases created before version-loss tracking.
    const columns = this.db.prepare('PRAGMA table_info(runs)').all().map((row) => row.name);
    if (!columns.includes('version_loss_json')) {
      this.db.exec('ALTER TABLE runs ADD COLUMN version_loss_json TEXT');
    }
    // A run whose first bind fails has no binding yet; older databases declared
    // binding_json NOT NULL, so rebuild the table without that constraint.
    const bindingColumn = this.db.prepare('PRAGMA table_info(runs)').all().find((row) => row.name === 'binding_json');
    if (bindingColumn && bindingColumn.notnull) {
      this.db.exec('PRAGMA foreign_keys = OFF');
      this.db.exec('BEGIN IMMEDIATE');
      this.db.exec(`CREATE TABLE runs_new (
        id TEXT PRIMARY KEY, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        state TEXT NOT NULL, epoch INTEGER NOT NULL, request_json TEXT NOT NULL,
        binding_json TEXT, pin_json TEXT, cursor_json TEXT NOT NULL,
        emitted_json TEXT NOT NULL, failure_json TEXT, source_a_json TEXT NOT NULL,
        source_b_json TEXT NOT NULL, next_retry_json TEXT NOT NULL,
        warnings_json TEXT NOT NULL, cache_json TEXT NOT NULL,
        attempts_json TEXT NOT NULL, version_loss_json TEXT
      )`);
      this.db.exec(`INSERT INTO runs_new SELECT id, created_at, updated_at, state, epoch, request_json,
        binding_json, pin_json, cursor_json, emitted_json, failure_json, source_a_json,
        source_b_json, next_retry_json, warnings_json, cache_json, attempts_json,
        version_loss_json FROM runs`);
      this.db.exec('DROP TABLE runs');
      this.db.exec('ALTER TABLE runs_new RENAME TO runs');
      this.db.exec('COMMIT');
      this.db.exec('PRAGMA foreign_keys = ON');
    }
    this.insertRun = this.db.prepare(`
      INSERT INTO runs (id, created_at, updated_at, state, epoch, request_json, binding_json,
        pin_json, cursor_json, emitted_json, failure_json, source_a_json, source_b_json,
        next_retry_json, warnings_json, cache_json, attempts_json, version_loss_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    this.selectRun = this.db.prepare('SELECT * FROM runs WHERE id = ?');
    this.updateRun = this.db.prepare(`
      UPDATE runs SET updated_at = ?, state = ?, epoch = ?, request_json = ?, binding_json = ?, pin_json = ?,
        cursor_json = ?, emitted_json = ?, failure_json = ?, source_a_json = ?,
        source_b_json = ?, next_retry_json = ?, warnings_json = ?, cache_json = ?,
        attempts_json = ?, version_loss_json = ?
      WHERE id = ?
    `);
    this.upsertPage = this.db.prepare(`
      INSERT INTO pages (run_id, epoch, page_no, body_json, digest, created_at, superseded)
      VALUES (?, ?, ?, ?, ?, ?, 0)
      ON CONFLICT(run_id, epoch, page_no) DO UPDATE SET body_json = excluded.body_json,
        digest = excluded.digest, created_at = excluded.created_at, superseded = 0
    `);
    this.selectPage = this.db.prepare('SELECT * FROM pages WHERE run_id = ? AND epoch = ? AND page_no = ?');
    this.selectPages = this.db.prepare('SELECT * FROM pages WHERE run_id = ? AND epoch = ? ORDER BY page_no');
    this.selectAllPages = this.db.prepare('SELECT * FROM pages WHERE run_id = ? ORDER BY epoch, page_no');
    this.supersedeEpoch = this.db.prepare('UPDATE pages SET superseded = 1 WHERE run_id = ? AND epoch = ?');
    this.insertEvent = this.db.prepare('INSERT INTO events (run_id, at, kind, body_json) VALUES (?, ?, ?, ?)');
    this.selectEvents = this.db.prepare('SELECT * FROM events WHERE run_id = ? AND seq > ? ORDER BY seq');
    this.selectEventKinds = this.db.prepare('SELECT * FROM events WHERE run_id = ? ORDER BY seq DESC LIMIT ?');
    this.selectCache = this.db.prepare('SELECT * FROM page_cache WHERE cache_key = ?');
    this.insertCache = this.db.prepare(`
      INSERT INTO page_cache (cache_key, slice_json, slice_digest, policy, versions_json, created_at, hits)
      VALUES (?, ?, ?, ?, ?, ?, 0)
      ON CONFLICT(cache_key) DO NOTHING
    `);
    this.hitCache = this.db.prepare('UPDATE page_cache SET hits = hits + 1, last_hit_at = ? WHERE cache_key = ?');
    this.deleteCacheStmt = this.db.prepare('DELETE FROM page_cache WHERE cache_key = ?');
    this.cacheStatsStmt = this.db.prepare('SELECT COUNT(*) AS entries, COALESCE(SUM(hits), 0) AS hits FROM page_cache');
    this.deleteRun = this.db.prepare('DELETE FROM runs WHERE id = ?');
    this.deletePages = this.db.prepare('DELETE FROM pages WHERE run_id = ?');
    this.deleteEvents = this.db.prepare('DELETE FROM events WHERE run_id = ?');
    this.listRuns = this.db.prepare(`
      SELECT id, created_at, updated_at, state, epoch, pin_json, cursor_json, emitted_json
      FROM runs ORDER BY updated_at DESC LIMIT 40
    `);
    this.countRuns = this.db.prepare('SELECT COUNT(*) AS n FROM runs');
  }

  createRun(run) {
    this.insertRun.run(
      run.id, run.createdAt, run.updatedAt, run.state, run.epoch,
      JSON.stringify(run.request), JSON.stringify(run.binding),
      run.pin ? JSON.stringify(run.pin) : null,
      JSON.stringify(run.cursor), JSON.stringify(run.emitted),
      run.failure ? JSON.stringify(run.failure) : null,
      JSON.stringify(run.sourceA), JSON.stringify(run.sourceB),
      JSON.stringify(run.nextRetry), JSON.stringify(run.warnings),
      JSON.stringify(run.cache), JSON.stringify(run.attempts),
      run.versionLoss ? JSON.stringify(run.versionLoss) : null,
    );
    return this.getRun(run.id);
  }

  getRun(id) {
    const row = this.selectRun.get(id);
    if (!row) return null;
    return {
      id: row.id,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      state: row.state,
      epoch: row.epoch,
      request: JSON.parse(row.request_json),
      binding: row.binding_json ? JSON.parse(row.binding_json) : null,
      pin: row.pin_json ? JSON.parse(row.pin_json) : null,
      cursor: JSON.parse(row.cursor_json),
      emitted: JSON.parse(row.emitted_json),
      failure: row.failure_json ? JSON.parse(row.failure_json) : null,
      sourceA: JSON.parse(row.source_a_json),
      sourceB: JSON.parse(row.source_b_json),
      nextRetry: JSON.parse(row.next_retry_json),
      warnings: JSON.parse(row.warnings_json),
      cache: JSON.parse(row.cache_json),
      attempts: JSON.parse(row.attempts_json),
      versionLoss: row.version_loss_json ? JSON.parse(row.version_loss_json) : null,
    };
  }

  saveRun(run) {
    const result = this.updateRun.run(
      run.updatedAt, run.state, run.epoch, JSON.stringify(run.request),
      run.binding ? JSON.stringify(run.binding) : null,
      run.pin ? JSON.stringify(run.pin) : null,
      JSON.stringify(run.cursor), JSON.stringify(run.emitted),
      run.failure ? JSON.stringify(run.failure) : null,
      JSON.stringify(run.sourceA), JSON.stringify(run.sourceB),
      JSON.stringify(run.nextRetry), JSON.stringify(run.warnings),
      JSON.stringify(run.cache), JSON.stringify(run.attempts),
      run.versionLoss ? JSON.stringify(run.versionLoss) : null, run.id,
    );
    if (result.changes !== 1) throw new Error(`run ${run.id} disappeared during save`);
    return this.getRun(run.id);
  }

  savePage(runId, epoch, pageNo, body, digest) {
    this.upsertPage.run(runId, epoch, pageNo, JSON.stringify(body), digest, new Date().toISOString());
  }

  /**
   * Page material and the cursor that points past it must land together.
   * Without this, a crash between the two writes could re-emit or skip a page.
   */
  transaction(fn) {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const result = fn();
      this.db.exec('COMMIT');
      return result;
    } catch (err) {
      try { this.db.exec('ROLLBACK'); } catch { /* already rolled back */ }
      throw err;
    }
  }

  getPages(runId, epoch) {
    return this.selectPages.all(runId, epoch).map((row) => ({
      epoch: row.epoch,
      pageNo: row.page_no,
      body: JSON.parse(row.body_json),
      digest: row.digest,
      createdAt: row.created_at,
      superseded: Boolean(row.superseded),
    }));
  }

  getAllPages(runId) {
    return this.selectAllPages.all(runId).map((row) => ({
      epoch: row.epoch,
      pageNo: row.page_no,
      body: JSON.parse(row.body_json),
      digest: row.digest,
      createdAt: row.created_at,
      superseded: Boolean(row.superseded),
    }));
  }

  supersede(runId, epoch) {
    this.supersedeEpoch.run(runId, epoch);
  }

  getCache(key) {
    const row = this.selectCache.get(key);
    if (!row) return null;
    return {
      key,
      slice: JSON.parse(row.slice_json),
      sliceDigest: row.slice_digest,
      policy: row.policy,
      versions: JSON.parse(row.versions_json),
      createdAt: row.created_at,
      hits: row.hits,
      lastHitAt: row.last_hit_at,
    };
  }

  putCache(key, slice, sliceDigest, policy, versions) {
    this.insertCache.run(key, JSON.stringify(slice), sliceDigest, policy, JSON.stringify(versions), new Date().toISOString());
  }

  recordCacheHit(key) {
    this.hitCache.run(new Date().toISOString(), key);
  }

  deleteCache(key) {
    this.deleteCacheStmt.run(key);
  }

  cacheStats() {
    const row = this.cacheStatsStmt.get();
    return { entries: row.entries, hits: row.hits };
  }

  appendEvent(runId, kind, body) {
    this.insertEvent.run(runId, new Date().toISOString(), kind, JSON.stringify(body || {}));
  }

  getEvents(runId, afterSeq = 0) {
    return this.selectEvents.all(runId, afterSeq).map((row) => ({
      seq: row.seq, at: row.at, kind: row.kind, body: JSON.parse(row.body_json),
    }));
  }

  getRecentEvents(runId, limit = 30) {
    return this.selectEventKinds.all(runId, limit).reverse().map((row) => ({
      seq: row.seq, at: row.at, kind: row.kind, body: JSON.parse(row.body_json),
    }));
  }

  purgeRun(runId) {
    this.deletePages.run(runId);
    this.deleteEvents.run(runId);
    this.deleteRun.run(runId);
  }

  purgeAllRuns() {
    this.db.exec('DELETE FROM pages; DELETE FROM events; DELETE FROM runs;');
  }

  recentRuns() {
    return this.listRuns.all().map((row) => ({
      id: row.id,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      state: row.state,
      epoch: row.epoch,
      pin: row.pin_json ? JSON.parse(row.pin_json) : null,
      cursor: JSON.parse(row.cursor_json),
      emitted: JSON.parse(row.emitted_json),
    }));
  }

  close() {
    this.db.close();
  }
}

module.exports = { Store };
