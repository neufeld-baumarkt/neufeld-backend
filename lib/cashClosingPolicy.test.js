const test = require('node:test');
const assert = require('node:assert/strict');
const { validateCashClosing } = require('./cashClosingPolicy');

function validPayload(overrides = {}) {
  return {
    drawerNumber: 1,
    openingCountId: 41,
    proceededWithoutOpening: false,
    fullCounts: { 100: 2, 200: 4, 500: 8, 1000: 10, 2000: 10, 5000: 2 },
    retainedCoinCents: 1000,
    looseChangeCents: 0,
    retainedNoteCounts: { 500: 2, 1000: 3, 2000: 10 },
    bankNoteCounts: { 500: 6, 1000: 7, 2000: 0, 5000: 2, 10000: 0, 20000: 0 },
    bankAmountCents: 20000,
    eurocontrolTotalCents: 20000,
    differenceCents: 0,
    notificationConfirmed: false,
    ...overrides,
  };
}

test('validiert die vollständige Abschlussgleichung serverseitig', () => {
  const result = validateCashClosing(validPayload());
  assert.equal(result.countedTotalCents, 45000);
  assert.equal(result.bankAmountCents, 20000);
});

test('erlaubt eine Abrechnung ohne Einzählung nur nach ausdrücklicher Bestätigung', () => {
  assert.throws(()=>validateCashClosing(validPayload({openingCountId:null})), /ausdrücklich bestätigt/);
  assert.doesNotThrow(()=>validateCashClosing(validPayload({openingCountId:null,proceededWithoutOpening:true})));
});

test('weist manipulierte Kleingeld-, Schein- und EuroControl-Werte ab', () => {
  assert.throws(()=>validateCashClosing(validPayload({looseChangeCents:50})), /Kleingeldberechnung/);
  assert.throws(()=>validateCashClosing(validPayload({bankNoteCounts:{500:5,1000:7,2000:0,5000:2,10000:0,20000:0}})), /Bar-an-Bank-Zählung/);
  assert.throws(()=>validateCashClosing(validPayload({eurocontrolTotalCents:19000})), /EuroControl-Gesamtsumme/);
});

test('erzwingt die Meldung ausschließlich oberhalb von fünf Euro', () => {
  assert.doesNotThrow(()=>validateCashClosing(validPayload({differenceCents:-500})));
  assert.throws(()=>validateCashClosing(validPayload({differenceCents:501})), /Alex und Peter/);
  assert.equal(validateCashClosing(validPayload({differenceCents:-501,notificationConfirmed:true})).notificationRequired,true);
});

test('begrenzt und normalisiert verworfene EuroControl-Neuzählungen', () => {
  const prior = {
    fullCounts: validPayload().fullCounts,
    retainedNoteCounts: validPayload().retainedNoteCounts,
    bankNoteCounts: validPayload().bankNoteCounts,
    bankAmountCents: 20000,
    eurocontrolTotalCents: 20000,
    reportedDifferenceCents: 900,
  };
  assert.equal(validateCashClosing(validPayload({ recountAttempts: [prior] })).recountAttempts.length, 1);
  assert.throws(()=>validateCashClosing(validPayload({ recountAttempts: Array(21).fill(prior) })), /Anzahl der Neuzählungen/);
});

