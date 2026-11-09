const test = require('node:test');
const assert = require('node:assert/strict');
const {
  isoCalendarParts,
  listPendingQlikDailyRevenueDates,
  retryPendingQlikDailyRevenue,
  synchronizeQlikDailyRevenue,
} = require('./alexModule1');

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
    { rows: [{ id: 'booking-1', betrag: '39000.00', quelle: 'fast_booking', erstellt_von: 'Alex', status: 'angekuendigt' }] },
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
  for (const plans of [[], [{ id: 'a', status: 'angekuendigt' }, { id: 'b', status: 'angekuendigt' }]]) {
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

test('ueberschreibt eine fehlende oder leere Planzahl niemals mit dem Qlik-Wert', async () => {
  const aggregate = { id: 'aggregate-1', numeric_value: '4006194', availability_status: 'VORHANDEN', expected_member_count: '4', actual_member_count: '4', source_count: '4' };
  for (const [suffix, betrag] of [['null', null], ['empty', ''], ['zero', '0']]) {
    const invalidPlan = queryable([
      { rows: [aggregate] }, { rows: [] },
      { rows: [{ id: `booking-${suffix}`, betrag, quelle: 'fast_booking', erstellt_von: 'Alex', status: 'angekuendigt' }] },
    ]);
    assert.deepEqual(
      await synchronizeQlikDailyRevenue(invalidPlan, { businessDate: '2026-11-07' }),
      { status: 'PLAN_INVALID', synchronized: 0, bookingId: `booking-${suffix}` },
    );
    assert.equal(invalidPlan.calls.length, 3);
  }
});

test('ueberspringt ungueltige Qlik-Betraege ohne Serverfehler', async () => {
  const aggregate = { id: 'aggregate-1', numeric_value: '4006194', availability_status: 'VORHANDEN', expected_member_count: '4', actual_member_count: '4', source_count: '4' };
  const invalidQlik = queryable([
    { rows: [{ ...aggregate, numeric_value: '0' }] }, { rows: [] },
    { rows: [{ id: 'booking-1', betrag: '39000', quelle: 'fast_booking', erstellt_von: 'Alex', status: 'angekuendigt' }] },
  ]);
  assert.deepEqual(
    await synchronizeQlikDailyRevenue(invalidQlik, { businessDate: '2026-11-07' }),
    { status: 'QLIK_INVALID', synchronized: 0, aggregateId: 'aggregate-1' },
  );
});

test('behaelt vollstaendige ungebuchte Qlik-Tage dauerhaft in der Wiederholungspruefung', async () => {
  const db = queryable([{ rows: [
    { business_date: '2026-11-06' },
    { business_date: new Date(2026, 10, 7) },
  ] }]);
  assert.deepEqual(
    await listPendingQlikDailyRevenueDates(db),
    ['2026-11-06', '2026-11-07'],
  );
  assert.match(db.calls[0].sql, /existing\.status='gebucht'/);
  assert.match(db.calls[0].sql, /aggregate_rank=1/);
  assert.match(db.calls[0].sql, /count\(source\.derived_daily_value_id\)=aggregate\.expected_member_count/);
});

test('prueft offene Qlik-Tage transaktional erneut und bucht erst nach vorhandener Planzahl', async () => {
  const clients = [
    queryable([
      { rows: [] },
      { rows: [{ id: 'aggregate-1', numeric_value: '10000', availability_status: 'VORHANDEN', expected_member_count: 4, actual_member_count: 4, source_count: 4 }] },
      { rows: [] },
      { rows: [] },
      { rows: [] },
    ]),
    queryable([
      { rows: [] },
      { rows: [{ id: 'aggregate-2', numeric_value: '12000', availability_status: 'VORHANDEN', expected_member_count: 4, actual_member_count: 4, source_count: 4 }] },
      { rows: [] },
      { rows: [{ id: 'plan-2', betrag: '100', quelle: 'fast_booking', erstellt_von: 'Alex', status: 'angekuendigt' }] },
      { rows: [{ id: 'plan-2', status: 'gebucht' }] },
      { rows: [] },
    ]),
  ];
  for (const client of clients) client.release = () => { client.released = true; };
  const firstClient = clients[0];
  const secondClient = clients[1];
  const pool = {
    async query() {
      return { rows: [{ business_date: '2026-11-06' }, { business_date: '2026-11-07' }] };
    },
    async connect() { return clients.shift(); },
  };

  const result = await retryPendingQlikDailyRevenue(pool);

  assert.equal(result.checked, 2);
  assert.equal(result.synchronized, 1);
  assert.deepEqual(result.results.map((item) => item.status), ['PLAN_NOT_FOUND', 'BOOKED']);
  assert.equal(firstClient.calls[0].sql, 'BEGIN');
  assert.equal(firstClient.calls.at(-1).sql, 'COMMIT');
  assert.equal(secondClient.calls[0].sql, 'BEGIN');
  assert.equal(secondClient.calls.at(-1).sql, 'COMMIT');
  assert.equal(firstClient.released, true);
  assert.equal(secondClient.released, true);
});

test('laesst einen bereits gebuchten Tageswert gelten und ueberschreibt ihn niemals', async () => {
  const db = queryable([
    { rows: [{ id: 'aggregate-1', numeric_value: '12000', availability_status: 'VORHANDEN', expected_member_count: 4, actual_member_count: 4, source_count: 4 }] },
    { rows: [] },
    { rows: [{ id: 'manual-booking', betrag: '110', status: 'gebucht' }] },
  ]);
  assert.deepEqual(
    await synchronizeQlikDailyRevenue(db, { businessDate: '2026-11-07' }),
    { status: 'EXISTING_BOOKING', synchronized: 0, bookingId: 'manual-booking' },
  );
  assert.equal(db.calls.length, 3);
});
