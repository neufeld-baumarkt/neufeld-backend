const test = require('node:test');
const assert = require('node:assert/strict');
const { runAlexModule2StartupMaintenance } = require('./alexModule2StartupMaintenance');

test('startup maintenance deletes, rebuilds and verifies before server start', async () => {
  const calls = [];
  const pool = { query: async (sql) => {
    calls.push(sql);
    if (sql.includes('payment_day_qlik_guard')) return { rows: [{
      payment_day_qlik_guard: true, transfer_provider_guard: true, provider_mismatches: 0,
    }] };
    return { rows: [] };
  } };
  const result = await runAlexModule2StartupMaintenance(pool, {
    sql: 'BEGIN; SELECT 1; COMMIT;',
    rebuild: async () => ({ status: 'APPLIED', synchronized: 400 }),
  });
  assert.equal(result.rebuilt.synchronized, 400);
  assert.equal(calls[0], 'BEGIN; SELECT 1; COMMIT;');
  assert.match(calls[1], /JOIN controlling\.payment_types payment_type/);
  assert.match(calls[1], /payment_type\.code = 'EC_CASH'/);
});

test('startup maintenance blocks server start when a source-purity guard is disabled', async () => {
  const pool = { query: async (sql) => sql.includes('payment_day_qlik_guard')
    ? { rows: [{ payment_day_qlik_guard: false, transfer_provider_guard: true, provider_mismatches: 0 }] }
    : { rows: [] } };
  await assert.rejects(runAlexModule2StartupMaintenance(pool, {
    sql: 'BEGIN; SELECT 1; COMMIT;', rebuild: async () => ({ status: 'APPLIED' }),
  }), /verification failed/);
});
