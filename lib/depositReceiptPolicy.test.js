const assert = require('node:assert/strict');
const test = require('node:test');
const {
  matchingClosingSets,
  parseDepositReceiptText,
  validateDepositReceipt,
} = require('./depositReceiptPolicy');

const syntheticReceipt = `
Musterbank Teststadt
Quittungsbeleg für Bargeldeinzahlung
Datum Uhrzeit Tx-Nr Terminal
02.01.26 18:58 00999123 00007777
Verwendungszweck:
"TEST2099"
eingezahlte Noten:
5,00 EUR * 2 = 10,00 EUR
10,00 EUR * 4 = 40,00 EUR
20,00 EUR * 5 = 100,00 EUR
50,00 EUR * 7 = 350,00 EUR
100,00 EUR * 1 = 100,00 EUR
Summe = 600,00 EUR
`;

test('receipt parser extracts identity, optional purpose, notes and total', () => {
  const parsed = parseDepositReceiptText(syntheticReceipt);
  assert.deepEqual(parsed, {
    depositDate: '2026-01-02', depositTime: '18:58', transactionNumber: '00999123', terminalNumber: '00007777',
    purpose: 'TEST2099', banknoteCounts: { 500: 2, 1000: 4, 2000: 5, 5000: 7, 10000: 1, 20000: 0 }, receiptTotalCents: 60000,
  });
});

test('purpose is optional and never needed for validation', () => {
  const parsed = parseDepositReceiptText(syntheticReceipt.replace(/Verwendungszweck:[\s\S]*?eingezahlte Noten:/, 'eingezahlte Noten:'));
  assert.equal(parsed.purpose, null);
  assert.deepEqual(validateDepositReceipt({ ...parsed, onedriveConfirmed: true }).errors, []);
});

test('an empty printed purpose does not consume the following banknote heading', () => {
  const parsed = parseDepositReceiptText(syntheticReceipt.replace('"TEST2099"', ''));
  assert.equal(parsed.purpose, null);
});

test('validation rejects OCR arithmetic mismatches and missing manual archive confirmation', () => {
  const parsed = parseDepositReceiptText(syntheticReceipt);
  const result = validateDepositReceipt({ ...parsed, receiptTotalCents: 60100, onedriveConfirmed: false });
  assert.match(result.errors.join(' '), /Scheinstückelung/);
  assert.match(result.errors.join(' '), /OneDrive/);
});

test('matching uses exact amounts and reports ambiguity instead of guessing', () => {
  const closings = [
    { id: 'a', expectedDepositCents: 249000 },
    { id: 'b', expectedDepositCents: 281500 },
    { id: 'c', expectedDepositCents: 100000 },
    { id: 'd', expectedDepositCents: 149000 },
  ];
  assert.deepEqual(matchingClosingSets(closings, 281500), [['b']]);
  assert.deepEqual(matchingClosingSets(closings, 249000), [['a'], ['c', 'd']]);
  assert.deepEqual(matchingClosingSets(closings, 12300), []);
});
