const test = require('node:test');
const assert = require('node:assert/strict');

const { runCashOpeningMigration } = require('./runCashOpeningMigration');

test('führt die idempotente Kassenschema-Migration vor dem Serverstart aus', async () => {
  const calls = [];
  const pool = {
    query: async (sql) => {
      calls.push(sql);
    },
  };

  await runCashOpeningMigration(pool);

  assert.equal(calls.length, 1);
  assert.match(calls[0], /CREATE TABLE IF NOT EXISTS core\.cash_drawer_closings/);
  assert.match(calls[0], /CREATE TABLE IF NOT EXISTS core\.cash_drawer_opening_counts/);
});
