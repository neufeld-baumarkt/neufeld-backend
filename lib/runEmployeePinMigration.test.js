const assert = require('node:assert/strict');
const test = require('node:test');
const { runEmployeePinMigration } = require('./runEmployeePinMigration');

test('führt die idempotente Mitarbeiter-PIN-Migration aus', async () => {
  const receivedSql = [];
  const pool = {
    async query(sql) {
      receivedSql.push(sql);
    },
  };

  await runEmployeePinMigration(pool);

  assert.equal(receivedSql.length, 2);
  assert.match(receivedSql[0], /must_change_pin BOOLEAN NOT NULL DEFAULT FALSE/);
  assert.match(receivedSql[0], /ADD COLUMN IF NOT EXISTS first_name TEXT/);
  assert.match(receivedSql[1], /ALTER COLUMN filiale_id DROP NOT NULL/);
  assert.match(receivedSql[1], /DROP INDEX IF EXISTS core\.ux_filiale_pins_filiale_display/);
});
