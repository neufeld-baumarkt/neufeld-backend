const test = require('node:test');
const assert = require('node:assert/strict');
const { migrationPath, runEuroControlImportMigration } = require('./runEuroControlImportMigration');

test('EuroControl-Schema wird vor dem Backendstart installiert', async () => {
  let sql = '';
  await runEuroControlImportMigration({ query: async (value) => { sql = value; } });
  assert.match(migrationPath, /056_eurocontrol_fallback_source\.sql$/);
  assert.match(sql, /CREATE TABLE IF NOT EXISTS controlling\.eurocontrol_imports/);
  assert.match(sql, /reject_eurocontrol_fact_mutation/);
});
