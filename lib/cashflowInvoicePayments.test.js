const test = require('node:test');
const assert = require('node:assert/strict');

const {
  calculatePaymentAmount,
  calculatePlannedAmount,
  normalizeSkontoRate,
} = require('./cashflowInvoicePayments');

test('berechnet geplanten Zahlbetrag mit zwei oder drei Prozent Skonto', () => {
  assert.equal(calculatePlannedAmount(1000, 2), 980);
  assert.equal(calculatePlannedAmount(1000, 3), 970);
  assert.equal(calculatePlannedAmount(19.99, 3), 19.39);
});

test('lehnt nicht freigegebene Skontosätze ab', () => {
  assert.throws(() => normalizeSkontoRate(5), /0, 2 oder 3/);
});

test('bezahlt wahlweise mit oder ohne Skonto und erhält den Originalbetrag', () => {
  assert.equal(calculatePaymentAmount({
    grossAmount: 1000,
    plannedAmount: 970,
    skontoRate: 3,
    useSkonto: true,
  }), 970);
  assert.equal(calculatePaymentAmount({
    grossAmount: 1000,
    plannedAmount: 970,
    skontoRate: 3,
    useSkonto: false,
  }), 1000);
  assert.equal(calculatePaymentAmount({
    grossAmount: null,
    plannedAmount: 250,
    skontoRate: 0,
    useSkonto: false,
  }), 250);
});
