const test = require('node:test');
const assert = require('node:assert/strict');
const { migrationPaths, runEuroControlImportMigration } = require('./runEuroControlImportMigration');

test('EuroControl-Schema wird vor dem Backendstart installiert', async () => {
  const statements = [];
  await runEuroControlImportMigration({ query: async (value) => { statements.push(value); } });
  assert.match(migrationPaths[0], /056_eurocontrol_fallback_source\.sql$/);
  assert.match(migrationPaths[1], /057_eurocontrol_canonical_fallback\.sql$/);
  assert.match(migrationPaths[2], /058_alex2_canonical_payment_days\.sql$/);
  assert.match(statements[0], /CREATE TABLE IF NOT EXISTS controlling\.eurocontrol_imports/);
  assert.match(statements[0], /reject_eurocontrol_fact_mutation/);
  assert.match(statements[1], /canonical_daily_metric_values/);
  assert.match(statements[1], /eurocontrol_derived_daily_values/);
  assert.match(statements[1], /selected_fingerprint,\s+COALESCE\(qlik\.id, eurocontrol\.id\) AS id/);
  assert.match(statements[2], /canonical_source_system/);
  assert.match(statements[2], /canonical_daily_metric_values/);
  assert.match(statements[2], /selected_fingerprint,\s+COALESCE\(qlik\.id, eurocontrol\.id\) AS id/);
});
