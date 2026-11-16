const assert = require('node:assert/strict');
const fs = require('node:fs');
const test = require('node:test');
const { migrationPath, runAlexModule2LiveSyncMigration } = require('./runAlexModule2LiveSyncMigration');

test('Alex 2 live-sync migration installs transactional statement notifications', async () => {
  const sql = fs.readFileSync(migrationPath, 'utf8');
  assert.match(sql, /pg_notify\(\s*'alex_module_2_changed'/);
  assert.match(sql, /FOR EACH STATEMENT/);
  const calls = [];
  await runAlexModule2LiveSyncMigration({ query: async (statement) => calls.push(statement) });
  assert.deepEqual(calls, [sql]);
});
