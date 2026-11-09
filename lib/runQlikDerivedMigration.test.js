const test = require('node:test');
const assert = require('node:assert/strict');
const { migrationPaths, runQlikDerivedMigration } = require('./runQlikDerivedMigration');

test('runs every derived-value migration and verifies the resulting schema', async () => {
  const calls = [];
  const pool = {
    async query(sql) {
      calls.push(sql);
      if (sql.includes("to_regclass('controlling.qlik_derived_daily_values')")) {
        return { rows: [{
          derived_values_ready: true, bindings_ready: true, adapter_ready: true,
          classic_transfers_ready: true, aggregates_ready: true, alex1_adapter_ready: true,
        }] };
      }
      return { rows: [] };
    },
  };

  await runQlikDerivedMigration(pool);

  assert.equal(migrationPaths.length, 8);
  assert.equal(calls.length, 9);
  assert.match(calls[0], /qlik_payment_type_mappings/);
  assert.match(calls[1], /qlik_metric_definitions/);
  assert.match(calls[2], /materialize_qlik_derived_daily_values/);
  assert.match(calls[3], /qlik_derived_value_id/);
  assert.match(calls[4], /ux_payment_transfers_classic_day/);
  assert.match(calls[5], /GESAMTUMSATZ_ALLE_FILIALEN_QLIK/);
  assert.match(calls[6], /qlik_aggregate_value_id/);
  assert.match(calls[7], /idx_payment_days_module2_month_read/);
});
