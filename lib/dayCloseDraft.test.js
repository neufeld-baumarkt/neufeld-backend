const test = require('node:test');
const assert = require('node:assert/strict');
const { deleteBusinessDayDrafts, lockDraftBusinessDay } = require('./dayCloseDraft');

test('löscht nach Tagesabschluss alle Entwürfe des Geschäftstags', async () => {
  const calls = [];
  const queryable = {
    query: async (sql, params) => {
      calls.push({ sql, params });
      return { rowCount: 8, rows: [] };
    },
  };

  const result = await deleteBusinessDayDrafts(queryable, {
    branchId: 4,
    businessDate: '2026-11-01',
  });

  assert.equal(result, 8);
  assert.deepEqual(calls[0].params, [4, '2026-11-01']);
  assert.match(calls[0].sql, /DELETE FROM controlling\.cash_workflow_drafts/);
  assert.match(calls[0].sql, /filiale_id=\$1 AND business_date=\$2/);
  assert.doesNotMatch(calls[0].sql, /workflow_type|status=/);
});

test('Löschen ist ohne vorhandene Entwürfe idempotent', async () => {
  const queryable = { query: async () => ({ rowCount: 0, rows: [] }) };
  assert.equal(await deleteBusinessDayDrafts(queryable, {
    branchId: 4,
    businessDate: '2026-11-01',
  }), 0);
});

test('sperrt den Geschäftstag gegen paralleles Finalisieren', async () => {
  const calls = [];
  const queryable = { query: async (sql, params) => {
    calls.push({ sql, params });
    return { rows: [{ status: 'finalized' }] };
  } };

  assert.equal(await lockDraftBusinessDay(queryable, {
    branchId: 4,
    businessDate: '2026-11-01',
  }), 'finalized');
  assert.deepEqual(calls[0].params, [4, '2026-11-01']);
  assert.match(calls[0].sql, /core\.cash_business_days/);
  assert.match(calls[0].sql, /FOR SHARE/);
});

test('erlaubt Entwürfe, solange noch kein Geschäftstag existiert', async () => {
  const queryable = { query: async () => ({ rows: [] }) };
  assert.equal(await lockDraftBusinessDay(queryable, {
    branchId: 4,
    businessDate: '2026-11-01',
  }), null);
});
