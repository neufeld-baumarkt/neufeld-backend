const test = require('node:test');
const assert = require('node:assert/strict');
const {
  buildAmountSuggestions,
  parseAmountToCents,
} = require('./cashflowAmountSearch');

test('parses German and plain decimal search amounts exactly to cents', () => {
  assert.equal(parseAmountToCents('1.234,56 €'), 123456);
  assert.equal(parseAmountToCents('1234,5'), 123450);
  assert.equal(parseAmountToCents('1234.56'), 123456);
  assert.equal(parseAmountToCents('-42,00'), 4200);
  assert.throws(() => parseAmountToCents('12,345'), /zum Beispiel/);
  assert.throws(() => parseAmountToCents('0,00'), /größer als 0/);
});

test('suggests an exact single invoice and prefers its actual payment amount', () => {
  const rows = [
    { id: 'one', rechnungsnummer: 'R-1', betrag: '98.00', zahlbetrag: '100.00', status: 'gebucht' },
    { id: 'two', rechnungsnummer: 'R-2', betrag: '99.00', zahlbetrag: null, status: 'angekuendigt' },
  ];
  const result = buildAmountSuggestions(rows, 10000);
  assert.deepEqual(result.rechnungen.map((row) => row.id), ['one']);
  assert.equal(result.rechnungen[0].suchbetrag, '100.00');
});

test('suggests all invoices of an Avis when their payment sum matches', () => {
  const rows = [
    { id: 'a', avis_nummer: 'AV-7', betrag: '40.00', status: 'angekuendigt' },
    { id: 'b', avis_nummer: 'AV-7', betrag: '60.00', status: 'gebucht' },
    { id: 'c', avis_nummer: 'AV-8', betrag: '20.00', status: 'angekuendigt' },
  ];
  const result = buildAmountSuggestions(rows, 10000);
  assert.equal(result.avis.length, 1);
  assert.equal(result.avis[0].avis_nummer, 'AV-7');
  assert.equal(result.avis[0].gesamtsumme, '100.00');
  assert.equal(result.avis[0].status, 'teilweise_gebucht');
  assert.deepEqual(result.avis[0].rechnungen.map((row) => row.id), ['a', 'b']);
});

test('does not combine unrelated invoices into invented payment groups', () => {
  const rows = [
    { id: 'a', betrag: '40.00', status: 'angekuendigt' },
    { id: 'b', betrag: '60.00', status: 'angekuendigt' },
  ];
  const result = buildAmountSuggestions(rows, 10000);
  assert.deepEqual(result, { avis: [], rechnungen: [] });
});
