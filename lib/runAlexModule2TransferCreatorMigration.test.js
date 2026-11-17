const assert = require('node:assert/strict');
const fs = require('node:fs');
const test = require('node:test');
const {
  migrationPath,
  runAlexModule2TransferCreatorMigration,
} = require('./runAlexModule2TransferCreatorMigration');

test('Alex 2 führt die idempotente Creator-Migration vor dem Serverstart aus', async () => {
  const sql = fs.readFileSync(migrationPath, 'utf8');
  assert.match(sql, /created_by_user_id integer REFERENCES public\.users\(id\)/);
  const calls = [];
  await runAlexModule2TransferCreatorMigration({ query: async (statement) => calls.push(statement) });
  assert.deepEqual(calls, [sql]);
});
