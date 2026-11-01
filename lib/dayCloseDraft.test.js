const test = require('node:test');
const assert = require('node:assert/strict');
const { completeDayCloseDraft } = require('./dayCloseDraft');

test('schließt den aktiven Tagesabschluss-Entwurf atomar ab', async () => {
  const calls = [];
  const expected = { id: 17, status: 'completed', revision: 8 };
  const queryable = {
    query: async (sql, params) => {
      calls.push({ sql, params });
      return { rows: [expected] };
    },
  };

  const result = await completeDayCloseDraft(queryable, {
    branchId: 4,
    businessDate: '2026-11-01',
    userId: 9,
  });

  assert.equal(result, expected);
  assert.deepEqual(calls[0].params, [4, '2026-11-01', 9]);
  assert.match(calls[0].sql, /workflow_type='day_close'/);
  assert.match(calls[0].sql, /status='active'/);
  assert.match(calls[0].sql, /status='completed'/);
});

test('ist ohne aktiven Entwurf idempotent', async () => {
  const queryable = { query: async () => ({ rows: [] }) };
  assert.equal(await completeDayCloseDraft(queryable, {
    branchId: 4,
    businessDate: '2026-11-01',
    userId: 9,
  }), null);
});
