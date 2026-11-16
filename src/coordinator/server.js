'use strict';

const path = require('node:path');
const fs = require('node:fs');

const { QueryCoordinator, POLICIES, CONTRACT_TEXT, MAX_LIMIT } = require('./coordinator');
const { Store } = require('./store');
const { SourceClient } = require('./source-client');
const { createServer, route, sendJson, readJsonBody, listen } = require('../lib/http');
const { CoordinatorError } = require('../lib/coordinator-error');

const WEB_ROOT = path.join(__dirname, '..', 'web');

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.json': 'application/json; charset=utf-8',
  '.ico': 'image/x-icon',
};

function serveStatic(req, res, pathname) {
  const relative = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
  const resolved = path.resolve(WEB_ROOT, relative);
  if (!resolved.startsWith(WEB_ROOT + path.sep) && resolved !== path.join(WEB_ROOT, 'index.html')) {
    sendJson(res, 403, { error: 'forbidden' });
    return true;
  }
  let stat;
  try {
    stat = fs.statSync(resolved);
  } catch {
    return false;
  }
  if (!stat.isFile()) return false;
  const body = fs.readFileSync(resolved);
  res.writeHead(200, {
    'content-type': MIME[path.extname(resolved)] || 'application/octet-stream',
    'content-length': body.length,
    'cache-control': 'no-cache',
    'x-content-type-options': 'nosniff',
  });
  res.end(body);
  return true;
}

async function createQueryServer({ dbPath, sourceAUrl, sourceBUrl, timeouts }) {
  const store = new Store(dbPath);
  const sourceA = new SourceClient({ baseUrl: sourceAUrl, name: 'A', timeoutMs: (timeouts && timeouts.a) || 6000 });
  const sourceB = new SourceClient({ baseUrl: sourceBUrl, name: 'B', timeoutMs: (timeouts && timeouts.b) || 6000 });
  const coordinator = new QueryCoordinator({ store, sourceA, sourceB });

  const routes = [
    route('GET', '/api/health', async (req, res) => {
      sendJson(res, 200, {
        ok: true,
        service: 'query-coordinator',
        source_a: sourceAUrl,
        source_b: sourceBUrl,
        promises: CONTRACT_TEXT,
        policies: Object.values(POLICIES),
        max_limit: MAX_LIMIT,
      });
    }),
    route('GET', '/api/config', async (req, res) => {
      sendJson(res, 200, {
        policies: Object.values(POLICIES).map((key) => ({ key, ...CONTRACT_TEXT[key] })),
        max_limit: MAX_LIMIT,
      });
    }),
    route('GET', '/api/runs', async (req, res) => {
      sendJson(res, 200, { runs: store.recentRuns(), cache: store.cacheStats() });
    }),
    route('POST', '/api/runs', async (req, res) => {
      const body = await readJsonBody(req);
      const run = await coordinator.createRun({
        policy: body.policy,
        filters: body.filters,
        limit: body.limit,
        join: body.join,
      });
      sendJson(res, 201, { run: coordinator.describeRun(run) });
    }),
    route('GET', '/api/runs/:id', async (req, res, { params }) => {
      const run = store.getRun(params.id);
      if (!run) throw new CoordinatorError('run_not_found', `run ${params.id} does not exist`, { status: 404 });
      sendJson(res, 200, { run: coordinator.describeRun(run), cache: store.cacheStats() });
    }),
    route('POST', '/api/runs/:id/step', async (req, res, { params }) => {
      const body = await readJsonBody(req);
      const pages = body.pages === undefined ? 1 : body.pages;
      try {
        const result = await coordinator.advance(params.id, pages);
        sendJson(res, 200, {
          run: coordinator.describeRun(result.run),
          pages: result.pages,
          stopped: result.stopped,
          error: result.error ? result.error.toJSON() : null,
        });
      } catch (err) {
        if (err instanceof CoordinatorError && err.code === 'source_failed') {
          const run = store.getRun(params.id);
          sendJson(res, 200, {
            run: coordinator.describeRun(run),
            pages: [],
            stopped: 'failed',
            error: err.toJSON(),
          });
          return;
        }
        throw err;
      }
    }),
    route('POST', '/api/runs/:id/retry', async (req, res, { params }) => {
      const body = await readJsonBody(req);
      const result = await coordinator.retry(params.id, {
        policy: body.policy,
        acknowledge_invalidation: body.acknowledge_invalidation === true,
        acknowledge_mixed: body.acknowledge_mixed === true,
      });
      sendJson(res, 200, {
        run: coordinator.describeRun(result.run),
        verification: result.verification,
        invalidated: result.invalidated,
        contract_change: result.contract_change,
      });
    }),
    route('POST', '/api/runs/:id/retry-guidance', async (req, res, { params }) => {
      const result = await coordinator.refreshRetryGuidance(params.id);
      sendJson(res, 200, {
        run: coordinator.describeRun(result.run),
        verification: result.verification,
        re_readable: result.re_readable,
      });
    }),
    route('POST', '/api/runs/:id/resume', async (req, res, { params }) => {
      await coordinator.resume(params.id);
      sendJson(res, 200, { run: coordinator.describeRun(store.getRun(params.id)) });
    }),
    route('POST', '/api/runs/:id/cancel', async (req, res, { params }) => {
      const body = await readJsonBody(req);
      const run = await coordinator.cancel(params.id, body.reason || 'cancelled from page');
      sendJson(res, 200, { run: coordinator.describeRun(run) });
    }),
    route('GET', '/api/runs/:id/verify', async (req, res, { params }) => {
      const result = await coordinator.verify(params.id);
      sendJson(res, 200, result);
    }),
    route('GET', '/api/runs/:id/pages/:pageNo', async (req, res, { params }) => {
      const result = coordinator.getPage(params.id, params.pageNo);
      sendJson(res, 200, result);
    }),
    route('DELETE', '/api/runs/:id', async (req, res, { params }) => {
      sendJson(res, 200, await coordinator.deleteRun(params.id));
    }),
    route('POST', '/api/runs/purge', async (req, res) => {
      store.purgeAllRuns();
      sendJson(res, 200, { purged: true });
    }),
    // Same-origin proxy so the page can drive source controls without CORS.
    route('GET', '/api/sources/:source/admin/state', async (req, res, { params }) => {
      const client = params.source === 'a' ? sourceA : params.source === 'b' ? sourceB : null;
      if (!client) throw new CoordinatorError('unknown_source', `unknown source ${params.source}`, { status: 404 });
      sendJson(res, 200, await client.get('/admin/state'));
    }),
    route('POST', '/api/sources/:source/admin/:action', async (req, res, { params }) => {
      const client = params.source === 'a' ? sourceA : params.source === 'b' ? sourceB : null;
      if (!client) throw new CoordinatorError('unknown_source', `unknown source ${params.source}`, { status: 404 });
      const allowed = new Set(['update', 'snapshot', 'bump', 'settings', 'faults', 'reset', 'prune_snapshots']);
      if (!allowed.has(params.action)) {
        throw new CoordinatorError('unknown_action', `source action ${params.action} is not proxied`, { status: 404 });
      }
      const body = await readJsonBody(req);
      sendJson(res, 200, await client.post(`/admin/${params.action}`, body));
    }),
  ];

  const server = createServer(routes, {
    onError: (err, req, res) => {
      if (err instanceof CoordinatorError) {
        sendJson(res, err.status, { error: err.toJSON() });
        return;
      }
      sendJson(res, err.status || 500, {
        error: { code: err.code || 'internal_error', message: err.message, status: err.status || 500, details: err.details || {} },
      });
    },
    onMissing: (req, res, url) => {
      if (url.pathname.startsWith('/api/')) return false;
      if (serveStatic(req, res, url.pathname)) return true;
      if (url.pathname !== '/' && serveStatic(req, res, '/')) return true;
      sendJson(res, 404, { error: { code: 'not_found', message: `no such path ${url.pathname}`, status: 404 } });
      return true;
    },
  });

  return {
    server,
    store,
    coordinator,
    sourceA,
    sourceB,
    async listen(port) {
      await listen(server, port);
      return server.address().port;
    },
    close() {
      server.close();
      store.close();
    },
  };
}

module.exports = { createQueryServer, WEB_ROOT };
