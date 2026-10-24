const test = require('node:test');
const assert = require('node:assert/strict');
const { runCashClosingMigration } = require('./runCashClosingMigration');

test('führt die idempotente und unveränderbare Abschlussmigration aus', async () => {
  const calls = [];
  await runCashClosingMigration({ query: async (sql) => calls.push(sql) });
  assert.equal(calls.length, 1);
  assert.match(calls[0], /ADD COLUMN IF NOT EXISTS business_date/);
  assert.match(calls[0], /ux_cash_drawer_closings_opening/);
  assert.match(calls[0], /cash_drawer_closings_immutable/);
});
