'use strict';

const http = require('node:http');

function sendJson(res, status, payload, headers = {}) {
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': body.length,
    'cache-control': 'no-store',
    ...headers,
  });
  res.end(body);
}

function readJsonBody(req, limit = 64 * 1024) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > limit) {
        reject(Object.assign(new Error('request body too large'), { status: 413 }));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      if (chunks.length === 0) return resolve({});
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch {
        reject(Object.assign(new Error('invalid JSON body'), { status: 400 }));
      }
    });
    req.on('error', reject);
  });
}

function createServer(routes, { onError, onMissing } = {}) {
  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    try {
      for (const route of routes) {
        if (route.method !== req.method) continue;
        const match = route.path.exec(url.pathname);
        if (!match) continue;
        const params = Object.fromEntries((route.keys || []).map((key, i) => [key, decodeURIComponent(match[i + 1])]));
        await route.handler(req, res, { url, params });
        return;
      }
      if (onMissing && (await onMissing(req, res, url))) return;
      sendJson(res, 404, { error: 'not_found', path: url.pathname });
    } catch (err) {
      if (onError) onError(err, req, res);
      else sendJson(res, err.status || 500, { error: err.code || 'internal_error', message: err.message });
    }
  });
  return server;
}

/** Compile "/v1/read/:id" into a regex with keys. */
function route(method, pattern, handler) {
  const keys = [];
  const source = pattern.replace(/:([A-Za-z_][A-Za-z0-9_]*)/g, (_, key) => {
    keys.push(key);
    return '([^/]+)';
  });
  return { method, path: new RegExp(`^${source}$`), keys, handler };
}

function listen(server, port) {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', () => resolve(server));
  });
}

module.exports = { createServer, route, sendJson, readJsonBody, listen };
