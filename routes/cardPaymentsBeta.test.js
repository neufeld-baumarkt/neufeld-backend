const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const jwt = require('jsonwebtoken');
const { createVerifyToken } = require('../middleware/verifyToken');
const { sellingWeek } = require('../lib/sellingWeek');
const { cardStatus, mountCardPaymentsBeta } = require('./cardPaymentsBeta');

test('Berlin selling weeks cross the ISO week-year boundary and reject invalid dates', () => {
  const last = sellingWeek('2027-01-01');
  assert.equal(last.week, 53);
  assert.equal(last.year, 2026);
  assert.equal(last.days[5], '2027-01-02');
  const first = sellingWeek('2027-01-01', last.next);
  assert.equal(first.week, 1);
  assert.equal(first.year, 2027);
  assert.equal(first.previous, last.start);
  for (const invalid of ['2026-02-30', '2027-01-05', ['2027-01-04'], '']) {
    assert.equal(sellingWeek('2027-01-01', invalid), null);
  }
});

test('blue requires both proofs and a past unreviewed day has a neutral status', () => {
  const today = '2027-01-01';
  assert.equal(cardStatus(null, today, today), 'import_pending');
  assert.equal(cardStatus({ imported: true, matched: true }, today, today), 'review_pending');
  assert.equal(cardStatus({ imported: true, reviewed: true }, today, today), 'verified');
  assert.equal(cardStatus({ imported: true, reviewed: true, matched: true }, today, today), 'matched');
  assert.equal(cardStatus(null, '2027-01-02', today), 'future');
  assert.equal(cardStatus({ imported: true }, '2026-12-31', today), 'past_review_pending');
  assert.equal(cardStatus(null, '2026-12-31', today), 'past_import_pending');
});

test('direct HTTP access requires one of three stable session IDs and returns status only', async t => {
  const env = { CARD_PAYMENTS_BETA_ENABLED: '1', CARD_PAYMENTS_BETA_PETER_ID: '101',
    CARD_PAYMENTS_BETA_MIRKO_ID: '102', CARD_PAYMENTS_BETA_ALEX_ID: '103' };
  const roles = { 101: 'Admin', 102: 'Supervisor', 103: 'Geschäftsführer', 104: 'Admin' };
  const secret = ['sandbox', 'cards', 'http', 'key'].join('-');
  let statusReads = 0;
  const pool = {
    async query(sql, params) {
      if (sql.includes('FROM public.users')) {
        const id = params[0];
        return { rows: [{ id, role: roles[id], active: true, name: 'Sandbox', token_version: 0, session_epoch: 0 }] };
      }
      statusReads++;
      assert.match(sql, /canonical_daily_metric_values/);
      assert.match(sql, /payment_review_events/);
      assert.match(sql, /payment_transfer_match_items/);
      assert.equal(params[2], '102');
      return { rows: [{ branch: 'Ahaus', day: params[0][0], code: 'EC_CASH',
        imported: true, reviewed: true, matched: true, amount: 100 }] };
    },
  };
  const options = { pool, verifyToken: createVerifyToken({ pool, jwtSecret: secret }),
    now: () => new Date('2026-12-31T23:30:00Z') };
  mountCardPaymentsBeta({ get() { assert.fail('Off by default'); } }, { ...options, env: {} });
  const app = express();
  mountCardPaymentsBeta(app, { ...options, env });
  const server = await new Promise(resolve => {
    const listener = app.listen(0, '127.0.0.1', () => resolve(listener));
  });
  t.after(() => new Promise(resolve => server.close(resolve)));
  const url = `http://127.0.0.1:${server.address().port}/api/card-payments-beta`;
  const request = (id, query = '') => fetch(url + query, {
    headers: id ? { Authorization: 'Bearer ' + jwt.sign({ id, tokenVersion: 0, sessionEpoch: 0 }, secret, { expiresIn: '5m' }) } : {},
  });
  assert.equal((await request()).status, 401);
  assert.equal((await request(104)).status, 404);
  assert.equal((await request(101, '?week=2027-01-05')).status, 400);
  assert.equal(statusReads, 0);
  for (const id of [101, 102, 103]) {
    const res = await request(id);
    assert.equal(res.status, 200);
    assert.match(res.headers.get('cache-control'), /no-store/);
    const body = await res.json();
    assert.equal(body.week, 53);
    assert.deepEqual(body.branches.map(b => b.branch), ['Ahaus', 'Telgte', 'Vreden', 'Münster']);
    assert.equal(body.branches[0].rows[0].statuses[0], 'matched');
    assert.equal(body.branches[0].rows[0].statuses[4], 'import_pending');
    assert.equal(body.branches[0].rows[0].statuses[5], 'future');
    assert.doesNotMatch(JSON.stringify(body), /amount|actor_user_id/);
  }
  const next = await (await request(101, '?week=2027-01-04')).json();
  assert.equal(next.week, 1);
});
