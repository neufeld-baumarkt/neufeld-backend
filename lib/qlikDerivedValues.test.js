const assert = require('node:assert/strict');
const test = require('node:test');
const {
  compareValue,
  loadDayCloseReconciliation,
  loadDerivedValues,
  validateConsumerCode,
} = require('./qlikDerivedValues');

test('lädt modulneutrale Werte ohne Rohdaten offenzulegen', async () => {
  const calls = [];
  const queryable = { query: async (sql, params) => {
    calls.push({ sql, params });
    return { rows: [{
      id: 'value-1', metric_code: 'EC_ZAHLUNGEN', display_name: 'EC-Zahlungen',
      value_kind: 'MONEY_CENTS', numeric_value: '544687.0000', availability_status: 'VORHANDEN',
      quality_status: 'NICHT_GEPRUEFT', rule_version: 1, calculated_at: '2026-11-08T10:00:00Z', source_count: '1',
    }] };
  } };
  const result = await loadDerivedValues(queryable, {
    branch: 'Vreden', businessDate: '2026-11-07', consumerCode: 'ALEX_MODULE_2',
  });
  assert.equal(result.values[0].valueCents, 544687);
  assert.equal(result.values[0].sourceCount, 1);
  assert.equal(Object.hasOwn(result.values[0], 'rawFields'), false);
  assert.deepEqual(calls[0].params, ['Vreden', '2026-11-07', 'ALEX_MODULE_2']);
  assert.match(calls[0].sql, /binding\.status='APPROVED'/);
});

test('Vergleich ist rein informativ und weist nur echte Abweichungen Mirko zu', () => {
  const qlikValue = { valueCents: 961610, availabilityStatus: 'VORHANDEN' };
  assert.deepEqual(compareValue({ appValue: 961610, qlikValue }), {
    status: 'MATCH', differenceCents: 0, requiresMirkoReview: false,
  });
  assert.deepEqual(compareValue({ appValue: 961600, qlikValue }), {
    status: 'DEVIATION', differenceCents: -10, requiresMirkoReview: true,
  });
  assert.equal(compareValue({ appValue: 0, qlikValue: { ...qlikValue, availabilityStatus: 'UNVOLLSTAENDIG' } }).status, 'QLIK_INCOMPLETE');
});

test('Tagesabschluss-Gegenprüfung verändert keine Mitarbeiterwerte', async () => {
  const queries = [];
  const queryable = { query: async (sql) => {
    queries.push(sql);
    if (sql.includes('WITH candidates')) return { rows: [
      { id: 'a', metric_code: 'GESAMTUMSATZ_QLIK', display_name: 'Umsatz', value_kind: 'MONEY_CENTS', numeric_value: '961610', availability_status: 'VORHANDEN', quality_status: 'NICHT_GEPRUEFT', rule_version: 1, source_count: 2 },
      { id: 'b', metric_code: 'BAR_AN_BANK_QLIK', display_name: 'Bar', value_kind: 'MONEY_CENTS', numeric_value: '279507', availability_status: 'VORHANDEN', quality_status: 'NICHT_GEPRUEFT', rule_version: 1, source_count: 1 },
      { id: 'c', metric_code: 'KASSENDIFFERENZ', display_name: 'Differenz', value_kind: 'MONEY_CENTS', numeric_value: '7', availability_status: 'VORHANDEN', quality_status: 'NICHT_GEPRUEFT', rule_version: 1, source_count: 1 },
    ] };
    return { rows: [{ daily_revenue_cents: 961610, eurocontrol_bank_cents: 279500, cash_difference_cents: 7, business_day_status: 'final' }] };
  } };
  const result = await loadDayCloseReconciliation(queryable, { branch: 'Vreden', businessDate: '2026-11-07' });
  assert.equal(result.blocking, false);
  assert.equal(result.reviewOwner, 'MIRKO');
  assert.deepEqual(result.comparisons.map((item) => item.status), ['MATCH', 'DEVIATION', 'MATCH']);
  assert.equal(queries.some((sql) => /\b(UPDATE|INSERT|DELETE)\b/i.test(sql)), false);
});

test('weist ungültige Verbraucher-Codes ab', () => {
  assert.equal(validateConsumerCode('alex_module_3'), 'ALEX_MODULE_3');
  assert.throws(() => validateConsumerCode('alex module 3'), /Verbraucher-Code/);
});
