const test = require('node:test');
const assert = require('node:assert/strict');
const {
  migrationPaths,
  reconcileImportedEuroControlPaymentDays,
  runEuroControlImportMigration,
} = require('./runEuroControlImportMigration');

test('EuroControl-Schema wird vor dem Backendstart installiert', async () => {
  const statements = [];
  const reconciliation = { months: [{ jahr: 2026, monat: 11, synchronized: 16 }], synchronized: 16 };
  const result = await runEuroControlImportMigration(
    { query: async (value) => { statements.push(value); } },
    { reconcile: async () => reconciliation }
  );
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
  assert.deepEqual(result, reconciliation);
});

test('bestehende EuroControl-Monate werden nach der Migration erneut in Alex 2 materialisiert', async () => {
  const calls = [];
  const periods = [{ jahr: 2026, monat: 10 }, { jahr: 2026, monat: 11 }];
  const client = {
    query: async (sql) => {
      calls.push(sql);
      if (String(sql).includes('FROM controlling.eurocontrol_derived_daily_values')) {
        return { rows: periods };
      }
      return { rows: [] };
    },
    release: () => { calls.push('RELEASE'); },
  };
  const synchronizedPeriods = [];
  const result = await reconcileImportedEuroControlPaymentDays(
    { connect: async () => client },
    async (_client, period) => {
      synchronizedPeriods.push(period);
      return { synchronized: period.monat === 10 ? 8 : 16 };
    }
  );

  assert.deepEqual(synchronizedPeriods, periods);
  assert.deepEqual(result, {
    months: [
      { jahr: 2026, monat: 10, synchronized: 8 },
      { jahr: 2026, monat: 11, synchronized: 16 },
    ],
    synchronized: 24,
  });
  assert.equal(calls[0], 'BEGIN');
  assert.equal(calls.at(-2), 'COMMIT');
  assert.equal(calls.at(-1), 'RELEASE');
});
