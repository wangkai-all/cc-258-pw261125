'use strict';

const crypto = require('node:crypto');

function canonical(value) {
  if (value === null || value === undefined) return 'null';
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new TypeError('non-finite number in canonical JSON');
    return JSON.stringify(value);
  }
  if (typeof value === 'boolean' || typeof value === 'string') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (typeof value === 'object') {
    const keys = Object.keys(value).sort();
    const parts = [];
    for (const key of keys) {
      if (value[key] === undefined) continue;
      parts.push(`${JSON.stringify(key)}:${canonical(value[key])}`);
    }
    return `{${parts.join(',')}}`;
  }
  throw new TypeError(`cannot canonicalize ${typeof value}`);
}

function sha256(value) {
  const input = typeof value === 'string' ? value : canonical(value);
  return crypto.createHash('sha256').update(input).digest('hex');
}

function shortHash(value, length = 12) {
  return sha256(value).slice(0, length);
}

function nowIso() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

module.exports = { canonical, sha256, shortHash, nowIso, sleep };
