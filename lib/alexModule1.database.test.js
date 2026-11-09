const test = require('node:test');
const assert = require('node:assert/strict');
const pool = require('../db');
const { isoCalendarParts, retryPendingQlikDailyRevenue } = require('./alexModule1');
const { importQlikCsv } = require('./qlikImport');

const enabled = process.env.ALEX1_DATABASE_TEST === '1';

test('behaelt einen echten 4-von-4-Qlik-Import offen und bucht ihn nach spaeterer Planung', { skip: !enabled }, async () => {
  const businessDate = '2099-01-02';
  const calendar = isoCalendarParts(businessDate);
  const payload = Buffer.from([
    '\uFEFFDatum;KundeNr;Zahlart;Zahlbetrag',
    '02.01.2099;041117;Umsatz 7%;100,00',
    '02.01.2099;041117;Umsatz 19%;900,00',
    '02.01.2099;041117;Gegenbuchung;-1.000,00',
    '02.01.2099;041129;Umsatz 7%;200,00',
    '02.01.2099;041129;Umsatz 19%;1.800,00',
    '02.01.2099;041129;Gegenbuchung;-2.000,00',
    '02.01.2099;041135;Umsatz 7%;300,00',
    '02.01.2099;041135;Umsatz 19%;2.700,00',
    '02.01.2099;041135;Gegenbuchung;-3.000,00',
    '02.01.2099;041140;Umsatz 7%;400,00',
    '02.01.2099;041140;Umsatz 19%;3.600,00',
    '02.01.2099;041140;Gegenbuchung;-4.000,00',
  ].join('\r\n'), 'utf8');

  await pool.query(
    `DELETE FROM cashflow.buchungen
     WHERE datum=$1::date AND kategorie_id=1 AND filiale='Unternehmen'`,
    [businessDate],
  );
  let plan;
  try {
    const result = await importQlikCsv({
      pool,
      payload,
      fileName: 'Filialbericht_AlleFilialen_02012099.csv',
    });
    assert.equal(result.alexModule1.status, 'PLAN_NOT_FOUND');
    assert.equal(result.alexModule1.synchronized, 0);
    const stillPending = await retryPendingQlikDailyRevenue(pool);
    assert.ok(stillPending.checked >= 1);
    assert.equal(
      stillPending.results.find((item) => item.businessDate === businessDate)?.status,
      'PLAN_NOT_FOUND',
    );

    plan = await pool.query(
      `INSERT INTO cashflow.buchungen
         (jahr,kw,datum,tag,kategorie_id,betrag,quelle,erstellt_von,filiale,status)
       VALUES ($1,$2,$3::date,$4,1,9000,'database_test','Alex','Unternehmen','angekuendigt')
       RETURNING id`,
      [calendar.jahr, calendar.kw, businessDate, calendar.tag],
    );
    const retried = await retryPendingQlikDailyRevenue(pool);
    assert.ok(retried.checked >= 1);
    assert.ok(retried.synchronized >= 1);
    assert.equal(
      retried.results.find((item) => item.businessDate === businessDate)?.status,
      'BOOKED',
    );
    const booking = await pool.query(
      `SELECT status,betrag,planbetrag,abweichung_betrag,abweichung_prozent,
              quelle,qlik_aggregate_value_id
       FROM cashflow.buchungen WHERE id=$1`,
      [plan.rows[0].id],
    );
    assert.equal(booking.rows[0].status, 'gebucht');
    assert.equal(Number(booking.rows[0].betrag), 10000);
    assert.equal(Number(booking.rows[0].planbetrag), 9000);
    assert.equal(Number(booking.rows[0].abweichung_betrag), 1000);
    assert.equal(Number(booking.rows[0].abweichung_prozent), 11.11);
    assert.equal(booking.rows[0].quelle, 'qlik_gesamtumsatz_alle_filialen');
    assert.ok(booking.rows[0].qlik_aggregate_value_id);

    await pool.query('DELETE FROM cashflow.buchungen WHERE id=$1', [plan.rows[0].id]);
    plan = await pool.query(
      `INSERT INTO cashflow.buchungen
         (jahr,kw,datum,tag,kategorie_id,betrag,quelle,erstellt_von,filiale,status)
       VALUES ($1,$2,$3::date,$4,1,777,'database_test','Alex','Unternehmen','gebucht')
       RETURNING id`,
      [calendar.jahr, calendar.kw, businessDate, calendar.tag],
    );
    const existingBookingCounts = await retryPendingQlikDailyRevenue(pool);
    assert.equal(
      existingBookingCounts.results.some((item) => item.businessDate === businessDate),
      false,
    );
    const unchanged = await pool.query('SELECT betrag,status FROM cashflow.buchungen WHERE id=$1', [plan.rows[0].id]);
    assert.equal(Number(unchanged.rows[0].betrag), 777);
    assert.equal(unchanged.rows[0].status, 'gebucht');
  } finally {
    if (plan) await pool.query('DELETE FROM cashflow.buchungen WHERE id=$1', [plan.rows[0].id]);
    await pool.end();
  }
});
