'use strict';

const { sha256, canonical, nowIso, sleep } = require('../lib/canonical');

const MAX_LIMIT = 200;
const DEFAULT_RETENTION = 200;

function parseCsv(value, max = 24) {
  if (value === undefined || value === null || value === '') return null;
  if (Array.isArray(value)) {
    return value.map((v) => String(v).trim()).filter(Boolean).slice(0, max);
  }
  return String(value).split(',').map((v) => v.trim()).filter(Boolean).slice(0, max);
}

function parseIntParam(value, name, { min = -Infinity, max = Infinity, fallback = null } = {}) {
  if (value === undefined || value === null || value === '') return fallback;
  const n = Number(value);
  if (!Number.isInteger(n) || n < min || n > max) {
    const err = new Error(`${name} must be an integer in [${min}, ${max}]`);
    err.status = 400;
    err.code = 'invalid_parameter';
    throw err;
  }
  return n;
}

function parseString(value, name, max = 128) {
  if (value === undefined || value === null) return null;
  const s = String(value).trim();
  if (s.length > max) {
    const err = new Error(`${name} is too long`);
    err.status = 400;
    err.code = 'invalid_parameter';
    throw err;
  }
  return s === '' ? null : s;
}

function buildIn(list) {
  return list.map(() => '?').join(', ');
}

function normalizeAEnabled(filters) {
  const f = filters || {};
  return {
    region: f.region === null || f.region === undefined ? null : f.region,
    plan: f.plan === null || f.plan === undefined ? null : f.plan,
    min_balance: f.min_balance === undefined ? null : f.min_balance,
  };
}

function filtersDigest(filters) {
  return sha256(canonical(filters));
}

class FaultController {
  constructor() {
    this.reset();
  }

  reset() {
    this.delayMs = 0;
    this.failReads = [];
    this.failProbes = [];
    this.failNext = 0;
    this.readSeq = 0;
    this.probeSeq = 0;
    this.triggered = [];
  }

  configure(cfg = {}) {
    const delayMs = Number(cfg.delay_ms || 0);
    if (!Number.isFinite(delayMs) || delayMs < 0 || delayMs > 30_000) {
      throw Object.assign(new Error('delay_ms must be in [0, 30000]'), { status: 400, code: 'invalid_parameter' });
    }
    const normalizeRules = (rules, label) => {
      if (rules === undefined || rules === null) return [];
      if (!Array.isArray(rules)) throw Object.assign(new Error(`${label} must be an array`), { status: 400, code: 'invalid_parameter' });
      return rules.map((rule, i) => {
        const at = Number(rule.at);
        const times = rule.times === undefined ? 1 : Number(rule.times);
        if (!Number.isInteger(at) || at < 1 || at > 100_000) {
          throw Object.assign(new Error(`${label}[${i}].at must be an integer >= 1`), { status: 400, code: 'invalid_parameter' });
        }
        if (!Number.isInteger(times) || times < 1 || times > 1000) {
          throw Object.assign(new Error(`${label}[${i}].times must be an integer in [1, 1000]`), { status: 400, code: 'invalid_parameter' });
        }
        return {
          at,
          times,
          code: String(rule.code || 'simulated_read_failure').slice(0, 64),
          message: String(rule.message || `injected failure at ${label} #${at}`).slice(0, 300),
          status: Number(rule.status || 503),
        };
      });
    };
    const failNext = Number(cfg.fail_next || 0);
    if (!Number.isInteger(failNext) || failNext < 0 || failNext > 100) {
      throw Object.assign(new Error('fail_next must be an integer in [0, 100]'), { status: 400, code: 'invalid_parameter' });
    }
    this.delayMs = delayMs;
    this.failReads = normalizeRules(cfg.fail_reads, 'fail_reads');
    this.failProbes = normalizeRules(cfg.fail_probes, 'fail_probes');
    this.failNext = failNext;
    this.readSeq = 0;
    this.probeSeq = 0;
    this.triggered = [];
    return this.describe();
  }

  describe() {
    return {
      delay_ms: this.delayMs,
      fail_reads: this.failReads.map((r) => ({ ...r })),
      fail_probes: this.failProbes.map((r) => ({ ...r })),
      fail_next: this.failNext,
      read_seq: this.readSeq,
      probe_seq: this.probeSeq,
      triggered: this.triggered.slice(-20),
    };
  }

  async beforeRead(kind) {
    let seq;
    if (kind === 'read') {
      this.readSeq += 1;
      seq = this.readSeq;
    } else {
      this.probeSeq += 1;
      seq = this.probeSeq;
    }
    if (this.delayMs > 0) await sleep(this.delayMs);
    if (this.failNext > 0) {
      this.failNext -= 1;
      const failure = {
        at: seq,
        kind,
        code: 'simulated_source_unavailable',
        message: `injected failure on ${kind} #${seq} (fail_next)`,
        status: 503,
      };
      this.triggered.push({ ...failure, at: nowIso() });
      return { fail: failure };
    }
    const rules = kind === 'read' ? this.failReads : this.failProbes;
    for (const rule of rules) {
      if (rule.at === seq && rule.times > 0) {
        rule.times -= 1;
        const failure = {
          at: seq,
          kind,
          code: rule.code,
          message: rule.message,
          status: rule.status,
        };
        this.triggered.push({ ...failure, at: nowIso() });
        return { fail: failure };
      }
    }
    return { seq };
  }
}

class ReadLog {
  constructor(limit = 200) {
    this.limit = limit;
    this.entries = [];
  }

  push(entry) {
    this.entries.push({ ...entry, at: nowIso() });
    if (this.entries.length > this.limit) this.entries.shift();
  }

  recent(n = 30) {
    return this.entries.slice(-n).reverse();
  }

  clear() {
    this.entries = [];
  }
}

class AdminLog {
  constructor(limit = 100) {
    this.limit = limit;
    this.entries = [];
  }

  push(kind, detail) {
    this.entries.push({ at: nowIso(), kind, detail });
    if (this.entries.length > this.limit) this.entries.shift();
  }

  recent(n = 20) {
    return this.entries.slice(-n).reverse();
  }

  clear() {
    this.entries = [];
  }
}

module.exports = {
  MAX_LIMIT,
  DEFAULT_RETENTION,
  parseCsv,
  parseIntParam,
  parseString,
  buildIn,
  filtersDigest,
  FaultController,
  ReadLog,
  AdminLog,
  nowIso,
};
