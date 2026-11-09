const test = require('node:test');
const assert = require('node:assert/strict');
const { isoCalendarParts, synchronizeQlikDailyRevenue } = require('./alexModule1');

function queryable(responses) {
  const calls = [];
  return {
    calls,
    async query(sql, params = []) {
      calls.push({ sql, params });
      const response = responses.shift();
      if (response instanceof Error) throw response;
      return response || { rows: [] };
    },
  };
}

test('ordnet den Qlik-Geschaeftstag der Alex-1-Kalenderwoche zu', () => {
  assert.deepEqual(isoCalendarParts('2026-11-07'), {
    businessDate: '2026-11-07', jahr: 2026, kw: 45, tag: 'Sa',
  });
  assert.equal(isoCalendarParts(new Date('2026-11-07T00:00:00.000Z')).businessDate, '2026-11-07');
});

test('bucht nur ein vollstaendiges 4-von-4-Aggregat und bewahrt die Planung', async () => {
  const db = queryable([
    { rows: [{ id: 'aggregate-1', numeric_value: '4006194', availability_status: 'VORHANDEN', expected_member_count: '4', actual_member_count: '4', source_count: '4' }] },
    { rows: [] },
    { rows: [{ id: 'booking-1', betrag: '39000.00', quelle: 'fast_booking', erstellt_von: 'Alex' }] },
    { rows: [{ id: 'booking-1', planbetrag: '39000.00', betrag: '40061.94', abweichung_betrag: '1061.94', abweichung_prozent: '2.72', status: 'gebucht', plan_quelle: 'fast_booking', plan_erstellt_von: 'Alex', qlik_aggregate_value_id: 'aggregate-1' }] },
  ]);
  const result = await synchronizeQlikDailyRevenue(db, { businessDate: '2026-11-07' });
  assert.equal(result.status, 'BOOKED');
  assert.equal(result.synchronized, 1);
  assert.deepEqual(db.calls[2].params, ['2026-11-07', 2026, 45, 'Sa']);
  assert.deepEqual(db.calls[3].params, ['booking-1', '2026-11-07', 40061.94, 1061.94, 2.72, 'aggregate-1']);
  assert.match(db.calls[3].sql, /planbetrag=betrag/);
});

test('schreibt bei unvollstaendigen Filialwerten niemals in den Cashflow', async () => {
  const db = queryable([{ rows: [{ id: 'partial', numeric_value: '3000000', availability_status: 'UNVOLLSTAENDIG', expected_member_count: '4', actual_member_count: '3', source_count: '3' }] }]);
  const result = await synchronizeQlikDailyRevenue(db, { businessDate: '2026-11-07' });
  assert.equal(result.status, 'QLIK_INCOMPLETE');
  assert.equal(db.calls.length, 1);
});

test('schreibt ohne eindeutige offene Planung nicht', async () => {
  for (const plans of [[], [{ id: 'a' }, { id: 'b' }]]) {
    const db = queryable([
      { rows: [{ id: 'aggregate-1', numeric_value: '4006194', availability_status: 'VORHANDEN', expected_member_count: '4', actual_member_count: '4', source_count: '4' }] },
      { rows: [] },
      { rows: plans },
    ]);
    const result = await synchronizeQlikDailyRevenue(db, { businessDate: '2026-11-07' });
    assert.ok(['PLAN_NOT_FOUND', 'PLAN_AMBIGUOUS'].includes(result.status));
    assert.equal(db.calls.length, 3);
  }
});

test('ist bei bereits gebuchtem Aggregat idempotent', async () => {
  const db = queryable([
    { rows: [{ id: 'aggregate-1', numeric_value: '4006194', availability_status: 'VORHANDEN', expected_member_count: '4', actual_member_count: '4', source_count: '4' }] },
    { rows: [{ id: 'booking-1' }] },
  ]);
  const result = await synchronizeQlikDailyRevenue(db, { businessDate: '2026-11-07' });
  assert.deepEqual(result, { status: 'ALREADY_BOOKED', synchronized: 0, bookingId: 'booking-1' });
  assert.equal(db.calls.length, 2);
});
