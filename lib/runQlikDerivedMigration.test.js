const test = require('node:test');
const assert = require('node:assert/strict');
const { migrationPaths, runQlikDerivedMigration } = require('./runQlikDerivedMigration');

test('runs migrations, historical Alex-2 backfill and the targeted transfer cleanup in order', async () => {
  const poolCalls = [];
  const clientCalls = [];
  const targets = [
    { transfer_id: '11111111-1111-4111-8111-111111111111', buchungsdatum: '2026-10-02', match_id: null, match_status: null },
    { transfer_id: '22222222-2222-4222-8222-222222222222', buchungsdatum: '2026-10-03', match_id: null, match_status: null },
  ];
  const client = {
    async query(sql, params = []) {
      clientCalls.push({ sql, params });
      if (sql.includes('FROM controlling.module2_maintenance_operations')) return { rowCount: 0, rows: [] };
      if (sql.includes('FROM controlling.qlik_derived_daily_values derived') && sql.includes('SELECT DISTINCT')) {
        return { rowCount: 0, rows: [] };
      }
      if (sql.includes('FROM controlling.payment_transfers transfer')) return { rowCount: 2, rows: targets };
      if (sql.includes("SET review_status='OFFEN'")) return { rowCount: 0, rows: [] };
      if (sql.includes('deleted_at=now()')) return { rowCount: 2, rows: targets };
      return { rowCount: 0, rows: [] };
    },
    release() {},
  };
  const pool = {
    async query(sql) {
      poolCalls.push(sql);
      if (sql.includes("to_regclass('controlling.qlik_derived_daily_values')")) {
        return { rows: [{
          derived_values_ready: true, bindings_ready: true, adapter_ready: true,
          classic_transfers_ready: true, aggregates_ready: true, alex1_adapter_ready: true,
          alex2_positive_sales_ready: true, alex2_cleanup_ready: true,
          alex2_difference_carries_ready: true,
        }] };
      }
      return { rows: [] };
    },
    async connect() { return client; },
  };

  await runQlikDerivedMigration(pool);

  assert.equal(migrationPaths.length, 12);
  assert.match(poolCalls[0], /qlik_payment_type_mappings/);
  assert.match(poolCalls[8], /module2_maintenance_operations/);
  assert.match(poolCalls[9], /idx_payment_days_module2_positive_sales/);
  assert.match(poolCalls[10], /deleted_at/);
  assert.match(poolCalls[11], /payment_transfer_difference_carries/);
  const historicalInsert = clientCalls.findIndex((call) => call.sql.includes('INSERT INTO controlling.module2_maintenance_operations')
    && call.params[0] === '2026-11-10_ALEX2_QLIK_HISTORY_20260601_20261107');
  const cleanupSelection = clientCalls.findIndex((call) => call.sql.includes('FROM controlling.payment_transfers transfer'));
  const cleanupInsert = clientCalls.findIndex((call) => call.sql.includes('INSERT INTO controlling.module2_maintenance_operations')
    && call.params[0] === '2026-11-10_DELETE_CC_TRANSFERS_20261002_20261003');
  assert.ok(historicalInsert >= 0 && cleanupSelection > historicalInsert && cleanupInsert > cleanupSelection);
});
