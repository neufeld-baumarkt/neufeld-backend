const assert = require('node:assert/strict');
const test = require('node:test');
const { runEmployeePinMigration } = require('./runEmployeePinMigration');

test('führt die idempotente Mitarbeiter-PIN-Migration aus', async () => {
  let receivedSql = '';
  const pool = {
    async query(sql) {
      receivedSql = sql;
    },
  };

  await runEmployeePinMigration(pool);

  assert.match(receivedSql, /must_change_pin BOOLEAN NOT NULL DEFAULT FALSE/);
  assert.match(receivedSql, /ADD COLUMN IF NOT EXISTS first_name TEXT/);
});
