const assert = require('node:assert/strict');
const test = require('node:test');
const Module = require('node:module');

function loadBookingsPostHandler(client, routePath = '/bookings') {
  const originalLoad = Module._load;
  const routes = [];

  function makeRouter() {
    const router = {};
    for (const method of ['get', 'post', 'put', 'patch', 'delete']) {
      router[method] = (path, ...handlers) => {
        routes.push({ method: method.toUpperCase(), path, handlers });
      };
    }
    return router;
  }

  Module._load = function loadWithRouteDoubles(request, parent, isMain) {
    if (request === 'express') return { Router: makeRouter };
    if (request === '../db') {
      return {
        connect: async () => client,
        query: async () => ({ rows: [] }),
      };
    }
    if (request === '../middleware/verifyToken') {
      return () => (_req, _res, next) => next();
    }
    return originalLoad.call(this, request, parent, isMain);
  };

  try {
    delete require.cache[require.resolve('./budget')];
    require('./budget');
  } finally {
    Module._load = originalLoad;
  }

  const route = routes.find((item) => item.method === 'POST' && item.path === routePath);
  assert.ok(route, `POST ${routePath} route must be registered`);
  return route.handlers.at(-1);
}

function makeActionRequest(body = {}) {
  return {
    user: { role: 'Manager-1', filiale: 'Alle', name: 'Jörg Test' },
    query: {},
    headers: {},
    body: {
      aktion_nr: 'a02713',
      filialen: [
        { filiale: 'Ahaus', betrag: 50 },
        { filiale: 'Vreden', betrag: 75.25 },
      ],
      ...body,
    },
  };
}

function makeRequest(body = {}) {
  return {
    user: { role: 'Manager-1', filiale: 'Alle', name: 'Jörg Test' },
    query: {},
    headers: {},
    body: {
      jahr: 2026,
      kw: 28,
      filiale: 'Ahaus',
      datum: '2026-07-08',
      typ: 'aktionsvorab',
      betrag: 50,
      lieferant: 'Testlieferant',
      aktion_nr: 'a02713',
      ...body,
    },
  };
}

function makeResponse() {
  const response = { statusCode: 200, body: null };
  response.api = {
    status(statusCode) {
      response.statusCode = statusCode;
      return this;
    },
    json(body) {
      response.body = body;
      return this;
    },
  };
  return response;
}

test('rejects an existing action in the same branch with merge details', async () => {
  const queries = [];
  const client = {
    async query(sql, params) {
      const normalized = String(sql).replace(/\s+/g, ' ').trim();
      queries.push({ sql: normalized, params });

      if (normalized.includes('UPPER(BTRIM(b.aktion_nr))')) {
        return {
          rows: [
            {
              id: '11111111-1111-1111-1111-111111111111',
              betrag: '125.50',
              filiale: 'Ahaus',
              jahr: 2027,
              kw: 13,
              parent_booking_id: null,
              has_splits: false,
            },
          ],
        };
      }
      return { rows: [] };
    },
    release() {},
  };

  const handler = loadBookingsPostHandler(client);
  const req = makeRequest();
  const response = makeResponse();

  await handler(req, response.api);

  assert.equal(response.statusCode, 409);
  assert.equal(response.body.code, 'ACTION_ALREADY_EXISTS');
  assert.deepEqual(response.body.existing_action, {
    aktion_nr: 'A02713',
    filiale: 'Ahaus',
    jahr: 2027,
    kw: 13,
    betrag: 125.5,
    booking_count: 1,
  });
  assert.ok(
    queries.some(
      (query) =>
        query.sql.includes('wb.filiale = $1') &&
        query.params?.[0] === 'Ahaus' &&
        query.params?.[1] === 'A02713'
    ),
    'duplicate lookup must be scoped by branch and action number'
  );
  assert.ok(!queries.some((query) => query.sql.includes('INSERT INTO budget.bookings')));
  assert.ok(queries.some((query) => query.sql === 'ROLLBACK'));
});

test('merges all simple duplicate amounts into one booking after confirmation', async () => {
  const queries = [];
  const existing = [
    {
      id: '11111111-1111-1111-1111-111111111111',
      betrag: '100.25',
      parent_booking_id: null,
      has_splits: false,
    },
    {
      id: '22222222-2222-2222-2222-222222222222',
      betrag: '25.25',
      parent_booking_id: null,
      has_splits: false,
    },
  ];
  const client = {
    async query(sql, params) {
      const normalized = String(sql).replace(/\s+/g, ' ').trim();
      queries.push({ sql: normalized, params });

      if (normalized.includes('UPPER(BTRIM(b.aktion_nr))')) return { rows: existing };
      if (normalized.startsWith('UPDATE budget.bookings')) {
        return { rows: [{ id: existing[0].id, betrag: params[1], aktion_nr: params[2] }] };
      }
      if (normalized.includes('FROM budget.v_week_summary_global_ytd')) {
        return { rows: [{ filiale: 'Ahaus', jahr: 2027, kw: 13, verbraucht: '175.50' }] };
      }
      return { rows: [] };
    },
    release() {},
  };

  const handler = loadBookingsPostHandler(client);
  const req = makeRequest({ merge_existing_action: true });
  const response = makeResponse();

  await handler(req, response.api);

  assert.equal(response.statusCode, 200);
  assert.equal(response.body.merged, true);
  assert.equal(response.body.booking.betrag, 175.5);
  const update = queries.find((query) => query.sql.startsWith('UPDATE budget.bookings'));
  assert.deepEqual(update.params, [existing[0].id, 175.5, 'A02713']);
  const removal = queries.find((query) => query.sql.startsWith('DELETE FROM budget.bookings'));
  assert.deepEqual(removal.params, [[existing[1].id]]);
  assert.ok(queries.some((query) => query.sql === 'COMMIT'));
  assert.ok(!queries.some((query) => query.sql.includes('INSERT INTO budget.bookings')));
});

test('rejects fantasy action numbers before accessing the database', async () => {
  let connected = false;
  const client = {
    async query() {
      throw new Error('database must not be queried');
    },
    release() {},
  };
  const handler = loadBookingsPostHandler(client, '/bookings/actions');
  const req = makeActionRequest({ aktion_nr: 'X02713' });
  const response = makeResponse();

  await handler(req, response.api);

  assert.equal(connected, false);
  assert.equal(response.statusCode, 400);
  assert.match(response.body.message, /A oder S/);
});

test('creates one regular action booking for every selected active branch atomically', async () => {
  const queries = [];
  let weekBudgetIndex = 0;
  const client = {
    async query(sql, params) {
      const normalized = String(sql).replace(/\s+/g, ' ').trim();
      queries.push({ sql: normalized, params });

      if (normalized.startsWith('SELECT name FROM filialen')) {
        return { rows: [{ name: 'Ahaus' }, { name: 'Vreden' }] };
      }
      if (normalized.includes('UPPER(BTRIM(b.aktion_nr))')) return { rows: [] };
      if (normalized.startsWith('INSERT INTO budget.week_budgets')) {
        weekBudgetIndex += 1;
        return { rows: [{ id: `week-${weekBudgetIndex}` }] };
      }
      if (normalized.startsWith('INSERT INTO budget.bookings')) {
        return { rows: [{ id: `booking-${params[0]}`, betrag: params[1], aktion_nr: params[2] }] };
      }
      return { rows: [] };
    },
    release() {},
  };

  const handler = loadBookingsPostHandler(client, '/bookings/actions');
  const response = makeResponse();
  await handler(makeActionRequest(), response.api);

  assert.equal(response.statusCode, 201);
  assert.deepEqual(response.body.action, { aktion_nr: 'A02713', jahr: 2027, kw: 13 });
  assert.equal(response.body.bookings.length, 2);
  assert.deepEqual(
    response.body.bookings.map((booking) => [booking.filiale, booking.betrag]),
    [['Ahaus', 50], ['Vreden', 75.25]]
  );
  const weekBudgets = queries.filter((query) => query.sql.startsWith('INSERT INTO budget.week_budgets'));
  assert.deepEqual(weekBudgets.map((query) => query.params), [
    ['Ahaus', 2027, 13],
    ['Vreden', 2027, 13],
  ]);
  assert.equal(queries.filter((query) => query.sql.startsWith('INSERT INTO budget.bookings')).length, 2);
  assert.ok(queries.some((query) => query.sql === 'COMMIT'));
});

test('requires a positive amount for every selected branch', async () => {
  const client = { async query() { return { rows: [] }; }, release() {} };
  const handler = loadBookingsPostHandler(client, '/bookings/actions');
  const response = makeResponse();

  await handler(
    makeActionRequest({ filialen: [{ filiale: 'Ahaus', betrag: '' }] }),
    response.api
  );

  assert.equal(response.statusCode, 400);
  assert.match(response.body.message, /Ahaus/);
});

test('reports branch-specific duplicates before changing any booking', async () => {
  const queries = [];
  const client = {
    async query(sql, params) {
      const normalized = String(sql).replace(/\s+/g, ' ').trim();
      queries.push({ sql: normalized, params });
      if (normalized.startsWith('SELECT name FROM filialen')) {
        return { rows: [{ name: 'Ahaus' }, { name: 'Vreden' }] };
      }
      if (normalized.includes('UPPER(BTRIM(b.aktion_nr))') && params[0] === 'Ahaus') {
        return {
          rows: [{ id: 'existing', betrag: '100.00', parent_booking_id: null, has_splits: false }],
        };
      }
      return { rows: [] };
    },
    release() {},
  };
  const handler = loadBookingsPostHandler(client, '/bookings/actions');
  const response = makeResponse();

  await handler(makeActionRequest(), response.api);

  assert.equal(response.statusCode, 409);
  assert.equal(response.body.code, 'ACTION_ALREADY_EXISTS');
  assert.deepEqual(response.body.existing_actions, [
    { filiale: 'Ahaus', betrag: 100, booking_count: 1 },
  ]);
  assert.ok(!queries.some((query) => query.sql.startsWith('UPDATE budget.bookings')));
  assert.ok(!queries.some((query) => query.sql.startsWith('INSERT INTO budget.bookings')));
  assert.ok(queries.some((query) => query.sql === 'ROLLBACK'));
});
