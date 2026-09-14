const assert = require('node:assert/strict');
const test = require('node:test');
const Module = require('node:module');

test('maps the normalized tracking-id constraint to a clear conflict response', async () => {
  const originalLoad = Module._load;
  const routes = [];
  const duplicateTrackingError = Object.assign(new Error('duplicate tracking ID'), {
    code: '23505',
    constraint: 'ux_reklamationen_tracking_id_norm',
    detail: 'Key (lower(TRIM(BOTH FROM tracking_id)))=(00000) already exists.',
  });
  const client = {
    async query(sql) {
      const normalized = String(sql).replace(/\s+/g, ' ').trim();
      if (normalized.includes('INSERT INTO reklamationen')) throw duplicateTrackingError;
      return { rows: [], rowCount: 1 };
    },
    release() {},
  };
  const pool = {
    connect: async () => client,
    query: async () => ({ rows: [] }),
  };

  function makeRouter() {
    const router = {};
    for (const method of ['get', 'post', 'put', 'patch', 'delete']) {
      router[method] = (path, ...handlers) => routes.push({ method: method.toUpperCase(), path, handlers });
    }
    return router;
  }

  Module._load = function loadWithRouteDoubles(request, parent, isMain) {
    if (request === 'express') return { Router: makeRouter };
    if (request === '../db') return pool;
    if (request === '../middleware/verifyToken') return () => (_req, _res, next) => next();
    return originalLoad.call(this, request, parent, isMain);
  };

  let router;
  try {
    delete require.cache[require.resolve('./reklamationen')];
    router = require('./reklamationen');
  } finally {
    Module._load = originalLoad;
  }

  assert.ok(router);
  const route = routes.find((item) => item.method === 'POST' && item.path === '/');
  assert.ok(route, 'POST / route must be registered');

  const req = {
    user: { role: 'Filiale', filiale: 'Ahaus', name: 'sandbox-test' },
    body: {
      datum: '2099-01-02',
      rekla_nr: 'SANDBOX-TRACKING-CONFLICT',
      lieferant: 'SodaFixx',
      filiale: 'Ahaus',
      tracking_id: '00000',
      positionen: [{ rekla_menge: '1' }],
    },
  };
  const response = { statusCode: 200, body: null };
  const res = {
    status(statusCode) {
      response.statusCode = statusCode;
      return this;
    },
    json(body) {
      response.body = body;
      return this;
    },
  };

  await route.handlers.at(-1)(req, res);

  assert.equal(response.statusCode, 409);
  assert.deepEqual(response.body, {
    code: 'TRACKING_ID_EXISTS',
    message: 'Tracking-ID bereits vorhanden (muss global eindeutig sein).',
  });
});
