'use strict';

const path = require('node:path');
const { SourceA } = require('./source-a');
const { SourceB } = require('./source-b');

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data');

function resolveDataDir(base = DATA_DIR) {
  return path.resolve(base);
}

async function startSourceA({ port = 0, dataDir = DATA_DIR } = {}) {
  const source = new SourceA({ dbPath: path.join(resolveDataDir(dataDir), 'source-a.sqlite') });
  await source.start(port);
  return source;
}

async function startSourceB({ port = 0, dataDir = DATA_DIR, retention } = {}) {
  const source = new SourceB({ dbPath: path.join(resolveDataDir(dataDir), 'source-b.sqlite'), retention });
  await source.start(port);
  return source;
}

module.exports = { startSourceA, startSourceB, resolveDataDir, DATA_DIR };
