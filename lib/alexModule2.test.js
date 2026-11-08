const assert = require('node:assert/strict');
const test = require('node:test');
const {
  findBestMatch,
  berlinDateOnly,
  moneyToCents,
  normalizeReviewDecision,
  normalizeTransferDecision,
  synchronizeQlikPaymentDays,
} = require('./alexModule2');

test('Match-Engine findet eine centgenaue Kombination ohne automatische Freigabe', () => {
  const result = findBestMatch([
    { id: 'a', datum: '2026-11-07', amountCents: 429000 },
    { id: 'b', datum: '2026-11-08', amountCents: 433000 },
    { id: 'c', datum: '2026-11-06', amountCents: 425000 },
  ], 862000);
  assert.deepEqual(result, { exact: true, sumCents: 862000, differenceCents: 0, itemIds: ['a', 'b'] });
});

test('Match-Engine liefert bei fehlender Exaktheit nur den besten Vorschlag', () => {
  const result = findBestMatch([
    { id: 'a', datum: '2026-11-07', amountCents: 400 },
    { id: 'b', datum: '2026-11-08', amountCents: 550 },
  ], 1000);
  assert.equal(result.exact, false);
  assert.equal(result.sumCents, 950);
  assert.equal(result.differenceCents, 50);
});

test('Geldwerte werden deutsch und centgenau normalisiert', () => {
  assert.equal(moneyToCents('8.620,00 €'), 862000);
  assert.equal(moneyToCents('8620.00'), 862000);
  assert.throws(() => moneyToCents('-1,00'), /größer als null/);
});

test('PostgreSQL-Datumswerte bleiben am Berliner Geschäftstag', () => {
  assert.equal(berlinDateOnly(new Date('2026-11-03T23:00:00.000Z')), '2026-11-04');
  assert.equal(berlinDateOnly('2026-11-04'), '2026-11-04');
});

test('Vertagen verlangt Grund und Wiedervorlagedatum', () => {
  assert.deepEqual(
    normalizeReviewDecision({ action: 'defer', note: 'Bankbeleg fehlt', deferredUntil: '2026-11-12' }),
    { action: 'defer', status: 'VERTAGT', note: 'Bankbeleg fehlt', deferredUntil: '2026-11-12' }
  );
  assert.throws(() => normalizeReviewDecision({ action: 'defer' }), /Begründung/);
  assert.throws(() => normalizeTransferDecision({ action: 'reject' }), /Begründung/);
});

test('Alex 2 synchronisiert ausschließlich freigegebene veredelte Qlik-Werte', async () => {
  const calls = [];
  const ids = {
    derived: '10000000-0000-4000-8000-000000000001',
    metric: '10000000-0000-4000-8000-000000000002',
    binding: '10000000-0000-4000-8000-000000000003',
    consumer: '10000000-0000-4000-8000-000000000004',
    run: '10000000-0000-4000-8000-000000000005',
    paymentDay: '10000000-0000-4000-8000-000000000006',
  };
  const client = {
    async query(sql, params = []) {
      calls.push({ sql: String(sql), params });
      if (String(sql).includes('WITH candidates AS')) {
        return { rows: [{
          derived_value_id: ids.derived,
          metric_definition_id: ids.metric,
          business_date: '2026-11-07',
          branch_name: 'Vreden',
          numeric_value: '544687',
          metric_code: 'EC_ZAHLUNGEN',
          rule_version: 1,
          binding_id: ids.binding,
          consumer_id: ids.consumer,
          target_payment_type_code: 'EC_CASH',
          import_run_id: ids.run,
          source_count: 1,
        }] };
      }
      if (String(sql).includes('FROM controlling.payment_days pd')) return { rows: [] };
      if (String(sql).includes('INSERT INTO controlling.payment_days')) return { rows: [{ id: ids.paymentDay }] };
      return { rows: [] };
    },
  };

  const result = await synchronizeQlikPaymentDays(client, { jahr: 2026, monat: 11 });

  assert.deepEqual(result, { synchronized: 1 });
  assert.match(calls[0].sql, /qlik_derived_daily_values/);
  assert.match(calls[0].sql, /consumer_code='ALEX_MODULE_2'/);
  assert.doesNotMatch(calls[0].sql, /qlik_payment_type_mappings/);
  const upsert = calls.find((call) => call.sql.includes('INSERT INTO controlling.payment_days'));
  assert.deepEqual(upsert.params, ['EC_CASH', 'Vreden', '2026-11-07', 2026, 11, 544687, ids.run, ids.derived]);
  assert.equal(calls.some((call) => call.sql.includes('INSERT INTO controlling.qlik_consumer_value_events')), true);
});
