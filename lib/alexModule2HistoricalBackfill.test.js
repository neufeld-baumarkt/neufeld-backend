const assert = require('node:assert/strict');
const test = require('node:test');
const { HISTORICAL_BACKFILL_OPERATION, backfillHistoricalQlikPaymentDays } = require('./alexModule2');

function fakePool({ applied = false, failInMonth = null } = {}) {
  const calls = [];
  const client = {
    async query(sql, params = []) {
      calls.push({ sql, params });
      if (sql.includes('FROM controlling.module2_maintenance_operations')) {
        return { rowCount: applied ? 1 : 0, rows: applied ? [{
          operation_key: HISTORICAL_BACKFILL_OPERATION,
          details: { months: [{ jahr: 2026, monat: 9, synchronized: 176 }], synchronized: 176 },
        }] : [] };
      }
      if (sql.includes('SELECT DISTINCT') && sql.includes('qlik_derived_daily_values')) {
        return { rowCount: 6, rows: [
          { jahr: 2026, monat: 6 }, { jahr: 2026, monat: 7 }, { jahr: 2026, monat: 8 },
          { jahr: 2026, monat: 9 }, { jahr: 2026, monat: 10 }, { jahr: 2026, monat: 11 },
        ] };
      }
      return { rowCount: 0, rows: [] };
    },
    release() {},
  };
  const synchronize = async (_client, month) => {
    if (failInMonth === month.monat) throw new Error('synthetic backfill failure');
    return { synchronized: month.monat === 9 ? 176 : 160 };
  };
  return { calls, synchronize, connect: async () => client };
}

test('zieht alle veredelten Monate einschließlich September transaktional in Alex 2 nach', async () => {
  const pool = fakePool();
  const result = await backfillHistoricalQlikPaymentDays(pool, { synchronize: pool.synchronize });

  assert.deepEqual(result.months.map(({ jahr, monat }) => [jahr, monat]), [
    [2026, 6], [2026, 7], [2026, 8], [2026, 9], [2026, 10], [2026, 11],
  ]);
  assert.equal(result.months.find((month) => month.monat === 9).synchronized, 176);
  assert.equal(pool.calls.some((call) => call.sql.includes('derived.business_date BETWEEN')), true);
});

test('wiederholt einen bereits abgeschlossenen Historien-Backfill nicht', async () => {
  const pool = fakePool({ applied: true });
  assert.deepEqual(await backfillHistoricalQlikPaymentDays(pool, { synchronize: pool.synchronize }), {
    status: 'ALREADY_APPLIED', months: [{ jahr: 2026, monat: 9, synchronized: 176 }], synchronized: 176,
  });
});

test('rollt bei einem Monatsfehler den gesamten Historien-Backfill zurück', async () => {
  const pool = fakePool({ failInMonth: 9 });
  await assert.rejects(backfillHistoricalQlikPaymentDays(pool, { synchronize: pool.synchronize }), /synthetic/);
  assert.equal(pool.calls.some((call) => call.sql === 'ROLLBACK'), true);
});
