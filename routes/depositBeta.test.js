const assert = require('node:assert/strict');
const test = require('node:test');
const express = require('express');
const jwt = require('jsonwebtoken');
const { createVerifyToken } = require('../middleware/verifyToken');
const { depositBetaConfig, depositCycle, depositStatuses, depositWeek } = require('../lib/depositBeta');
const { createDepositBetaHandler, mountDepositBeta } = require('./depositBeta');

const env = {
  DEPOSIT_BETA_ENABLED: '1',
  DEPOSIT_BETA_ADMIN_ID: '101',
  DEPOSIT_BETA_SUPERVISOR_ID: '102',
  DEPOSIT_BETA_VREDEN_ID: '103',
  DEPOSIT_BETA_HOLIDAYS: '2026-10-03',
};
const config = depositBetaConfig(env);
const monday = new Date('2026-10-05T09:59:59Z');

test('Berlin deadlines, weekday mappings and date-specific holidays', () => {
  assert.equal(depositBetaConfig({}), null);
  assert.throws(() => depositBetaConfig({ ...env, DEPOSIT_BETA_HOLIDAYS: '2026-02-30' }));
  assert.throws(() => depositBetaConfig({ ...env, DEPOSIT_BETA_ADMIN_ID: '103' }));
  const cycles = depositWeek(monday, config.holidays);
  assert.deepEqual(cycles.map(cycle => cycle.dates), [
    ['2026-10-02'], ['2026-10-05', '2026-10-06'], ['2026-10-07', '2026-10-08'],
  ]);
  assert.equal(cycles[2].upcoming, true);
  assert.equal(cycles[0].overdue, false);
  assert.equal(depositCycle(new Date('2026-10-05T10:00:00Z'), config.holidays).overdue, true);
  assert.equal(depositCycle(new Date('2026-11-23T10:59:59Z'), config.holidays).overdue, false);
  assert.equal(depositCycle(new Date('2026-11-23T11:00:00Z'), config.holidays).overdue, true);
  assert.equal(depositCycle(new Date('2026-10-06T07:00:00Z'), config.holidays).dueDate, '2026-10-05');
});

test('release switch is opt-in, requires complete configuration and mounts the existing verifier', () => {
  const routes = [];
  const errors = [];
  const authenticate = () => {};
  const app = { get(...args) { routes.push(args); } };
  const options = { verifyToken: () => authenticate, reportError: error => errors.push(error) };

  mountDepositBeta(app, { ...options, env: { ...env, NODE_ENV: 'production', DEPOSIT_BETA_ENABLED: '0' } });
  mountDepositBeta(app, { ...options, env: { ...env, DEPOSIT_BETA_HOLIDAYS: undefined } });
  assert.equal(routes.length, 0);
  assert.equal(errors.length, 1);
  mountDepositBeta(app, { ...options, env: { ...env, NODE_ENV: 'production' } });
  assert.equal(routes.length, 1);
  assert.equal(routes[0][0], '/api/deposit-beta');
  assert.equal(routes[0][1], authenticate);
  assert.equal(typeof routes[0][2], 'function');
});

test('all expected days and all funded closures must be verified and plausible', () => {
  const row = {
    branch: 'Vreden', date: '2026-10-02', final: true, verified: true, valid_links: true,
    bank_amount_cents: 500, linked_total_cents: '500', receipt_total_cents: 500, banknote_counts: { 500: 1 },
  };
  const cycle = depositCycle(monday, config.holidays);
  const status = (rows, period = cycle) => depositStatuses(rows, period)[0].status;
  assert.equal(status([row]), 'green');
  assert.equal(status([row, { ...row, verified: false }]), 'yellow');
  assert.equal(status([{ ...row, banknote_counts: {} }], { ...cycle, overdue: true }), 'red');
  assert.equal(status([{ ...row, valid_links: false }]), 'yellow');
  assert.equal(status([{ ...row, final: false }]), 'yellow');
  assert.equal(status([{ ...row, bank_amount_cents: 0, verified: false }]), 'green');
  assert.equal(status([{ branch: 'Vreden' }], { ...cycle, dates: [] }), 'green');
});

test('handler queries only the authorized scope and emits only the relevant cycles', async () => {
  const calls = [];
  const handler = createDepositBetaHandler({
    config, now: () => monday,
    pool: { async query(sql, params) {
      assert.match(sql, /LOWER\(f.name\) = LOWER\(\$1\)/);
      calls.push(params);
      return { rows: [{ branch: 'Vreden' }] };
    } },
  });
  const request = async user => {
    const res = {
      set(name, value) { assert.equal(name, 'Cache-Control'); assert.equal(value, 'private, no-store'); },
      sendStatus(status) { this.status = status; },
      json(body) { this.body = body; },
    };
    await handler({ user, query: { filiale: 'Andere Filiale' } }, res);
    return res;
  };
  for (const user of [
    undefined, { id: 999, role: 'Admin', name: 'Sandbox Namensgleich' },
    { id: 101, role: 'Supervisor' }, { id: 103, role: 'Filiale', filiale: 'Andere Filiale' },
  ]) assert.equal((await request(user)).status, 404);
  assert.equal(calls.length, 0);
  const branch = await request({ id: 103, role: 'Filiale', filiale: 'Vreden' });
  assert.deepEqual(calls[0], ['Vreden', ['2026-10-02']]);
  assert.equal(branch.body.cycles.length, 1);
  for (const [id, role] of [[101, 'Admin'], [102, 'Supervisor']]) {
    const result = await request({ id, role });
    assert.equal(result.body.cycles.length, 3);
    assert.equal(calls.at(-1)[0], null);
  }
});

test('HTTP route verifies real session tokens and enforces the allowlist before reading status data', async t => {
  const accounts = {
    101: { role: 'Admin' },
    102: { role: 'Supervisor' },
    103: { role: 'Filiale', filiale: 'Vreden' },
    104: { role: 'Admin', name: 'Sandbox Namensgleich' },
  };
  const reads = [];
  const pool = {
    async query(sql, params) {
      if (sql.includes('FROM public.users')) {
        const id = params[0];
        const account = accounts[id];
        return { rows: account ? [{ id, active: true, token_version: 0, session_epoch: 0, ...account }] : [] };
      }
      reads.push(params[0]);
      return { rows: (params[0] ? ['Vreden'] : ['Ahaus', 'Vreden']).map(branch => ({ branch })) };
    },
  };
  const jwtSecret = ['sandbox', 'deposit', 'beta', 'http'].join('-');
  const app = express();
  app.get('/existing', (_req, res) => res.sendStatus(204));
  mountDepositBeta(app, { env, pool, verifyToken: createVerifyToken({ jwtSecret, pool }), now: () => monday });
  const server = await new Promise(resolve => {
    const listener = app.listen(0, '127.0.0.1', () => resolve(listener));
  });
  t.after(() => new Promise(resolve => server.close(resolve)));
  const endpoint = `http://127.0.0.1:${server.address().port}`;
  function request(id) {
    const headers = {};
    if (id) headers.Authorization = 'Bearer ' + jwt.sign({ id, tokenVersion: 0, sessionEpoch: 0 }, jwtSecret, { expiresIn: '5m' });
    return fetch(endpoint + '/api/deposit-beta?filiale=Ahaus', { headers });
  }

  assert.equal((await fetch(endpoint + '/existing')).status, 204);
  assert.equal((await request()).status, 401);
  assert.equal((await request(104)).status, 404);
  assert.deepEqual(reads, []);
  for (const id of [101, 102, 103]) {
    const response = await request(id);
    assert.equal(response.status, 200);
    assert.match(response.headers.get('cache-control'), /no-store/);
    const data = await response.json();
    assert.deepEqual(data.cycles[0].branches.map(row => row.branch), id === 103 ? ['Vreden'] : ['Ahaus', 'Vreden']);
  }
  assert.deepEqual(reads, [null, null, 'Vreden']);
});
