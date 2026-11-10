const test = require('node:test');
const assert = require('node:assert/strict');
const { deleteOctoberCreditCardTransfers } = require('./alexModule2');

const targets = [
  { transfer_id: '11111111-1111-4111-8111-111111111111', buchungsdatum: '2026-10-02', match_id: null },
  { transfer_id: '22222222-2222-4222-8222-222222222222', buchungsdatum: '2026-10-03', match_id: null },
];

function fakePool(rows = targets) {
  const calls = [];
  const client = {
    async query(sql, params = []) {
      calls.push({ sql, params });
      if (sql.includes('FROM controlling.module2_maintenance_operations')) return { rowCount: 0, rows: [] };
      if (sql.includes('FROM controlling.payment_transfers transfer')) return { rowCount: rows.length, rows };
      if (sql.includes("SET review_status='OFFEN'")) return { rowCount: 3, rows: [{ id: 'a' }, { id: 'b' }, { id: 'c' }] };
      if (sql.includes('deleted_at=now()')) return { rowCount: rows.length, rows };
      return { rowCount: 0, rows: [] };
    },
    release() {},
  };
  return { calls, connect: async () => client };
}

test('bereinigt exakt die Kreditkarten-Überweisungen vom 02.10. und 03.10.', async () => {
  const pool = fakePool();
  const result = await deleteOctoberCreditCardTransfers(pool);
  assert.deepEqual(result, { status: 'APPLIED', deletedTransfers: 2, resetPaymentDays: 3 });
  const selection = pool.calls.find((call) => call.sql.includes('FROM controlling.payment_transfers transfer'));
  assert.deepEqual(selection.params[0], ['2026-10-02', '2026-10-03']);
  assert.match(selection.sql, /payment_type\.code='KREDITKARTE'/);
});

test('bereinigt nichts, wenn eines der beiden Buchungsdaten fehlt', async () => {
  const pool = fakePool(targets.slice(0, 1));
  await assert.rejects(deleteOctoberCreditCardTransfers(pool), /nicht vollständig gefunden/);
  assert.equal(pool.calls.some((call) => call.sql === 'ROLLBACK'), true);
  assert.equal(pool.calls.some((call) => call.sql.includes('deleted_at=now()')), false);
});
