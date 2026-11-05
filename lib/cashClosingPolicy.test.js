const test = require('node:test');
const assert = require('node:assert/strict');
const { cumulativeBabTotalCents, validateCashClosing, validateCumulativeBabCheck } = require('./cashClosingPolicy');

function validPayload(overrides = {}) {
  return {
    drawerNumber: 1,
    openingCountId: 41,
    proceededWithoutOpening: false,
    fullCounts: { 100: 2, 200: 4, 500: 8, 1000: 10, 2000: 10, 5000: 2 },
    retainedCoinCents: 1000,
    looseChangeCents: 0,
    bankNoteCounts: { 500: 6, 1000: 7, 2000: 0, 5000: 2, 10000: 0, 20000: 0 },
    bankAmountCents: 20000,
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

test('validiert Peters reale Kasse ohne Erfassung der verbleibenden Scheine', () => {
  const result = validateCashClosing(validPayload({
    fullCounts: { 1:22, 2:10, 5:34, 10:50, 20:33, 50:5, 100:12, 200:21, 500:17, 1000:19, 2000:13, 5000:15, 10000:0, 20000:0 },
    retainedCoinCents: 7000,
    looseChangeCents: 22,
    bankNoteCounts: { 500:7, 1000:14, 2000:9, 5000:15, 10000:0, 20000:0 },
    bankAmountCents: 110500,
  }));
  assert.equal(result.countedTotalCents, 135522);
  assert.deepEqual(result.retainedNoteCounts, {});
});

test('erlaubt einen 50-Euro-Schein in der Lade, wenn die Abschlussgleichung exakt 250 Euro ergibt', () => {
  const result = validateCashClosing(validPayload({
    bankNoteCounts: { 500:6, 1000:10, 2000:1, 5000:1, 10000:0, 20000:0 },
    bankAmountCents: 20000,
  }));
  assert.equal(result.bankNoteCounts[5000], 1);
  assert.equal(result.countedTotalCents - result.bankAmountCents - result.looseChangeCents, 25000);
});

test('erlaubt eine Abrechnung ohne Einzählung nur nach ausdrücklicher Bestätigung', () => {
  assert.throws(()=>validateCashClosing(validPayload({openingCountId:null})), /ausdrücklich bestätigt/);
  assert.doesNotThrow(()=>validateCashClosing(validPayload({openingCountId:null,proceededWithoutOpening:true})));
});

test('weist manipulierte Kleingeld- und Scheinwerte ab', () => {
  assert.throws(()=>validateCashClosing(validPayload({looseChangeCents:50})), /Kleingeldberechnung/);
  assert.throws(()=>validateCashClosing(validPayload({bankNoteCounts:{500:9,1000:7,2000:0,5000:2,10000:0,20000:0}})), /mehr Scheine/);
  assert.throws(()=>validateCashClosing(validPayload({bankNoteCounts:{500:5,1000:7,2000:0,5000:2,10000:0,20000:0},bankAmountCents:19500})), /Abschlussgleichung/);
});

test('berechnet den Einzelbeitrag intern aus Scheinen plus Kleingeld', () => {
  const input = validPayload({
    fullCounts: { 1:22, 2:10, 5:34, 10:50, 20:33, 50:5, 100:12, 200:21, 500:17, 1000:19, 2000:13, 5000:15, 10000:0, 20000:0 },
    retainedCoinCents: 7000,
    looseChangeCents: 22,
    bankNoteCounts: { 500:7, 1000:14, 2000:9, 5000:15, 10000:0, 20000:0 },
    bankAmountCents: 110500,
    differenceCents: -1,
  });
  assert.equal(validateCashClosing(input).eurocontrolTotalCents,110522);
});

test('berechnet und bestätigt die aufgelaufene BaB-Gesamtsumme', () => {
  const expected = cumulativeBabTotalCents(163601,110500,22);
  assert.equal(expected,274123);
  assert.deepEqual(validateCumulativeBabCheck({cumulativeBabExpectedCents:274123,cumulativeBabMatches:true},expected),{
    expectedCents:274123,eurocontrolCents:274123,differenceCents:0,matches:true,
  });
});

test('speichert bei Nein den EuroControl-Wert und seine Abweichung', () => {
  assert.deepEqual(validateCumulativeBabCheck({
    cumulativeBabExpectedCents:274123,cumulativeBabMatches:false,cumulativeBabEurocontrolCents:274000,
  },274123),{expectedCents:274123,eurocontrolCents:274000,differenceCents:-123,matches:false});
  assert.throws(()=>validateCumulativeBabCheck({cumulativeBabExpectedCents:274123,cumulativeBabMatches:false,cumulativeBabEurocontrolCents:274123},274123),/Jo, passt/);
  assert.throws(()=>validateCumulativeBabCheck({cumulativeBabExpectedCents:274000,cumulativeBabMatches:true},274123),/geändert/);
});

test('erzwingt die Meldung ausschließlich oberhalb von fünf Euro', () => {
  assert.doesNotThrow(()=>validateCashClosing(validPayload({differenceCents:-500})));
  assert.throws(()=>validateCashClosing(validPayload({differenceCents:501})), /Alex und Peter/);
  assert.equal(validateCashClosing(validPayload({differenceCents:-501,notificationConfirmed:true})).notificationRequired,true);
});

test('begrenzt und normalisiert verworfene EuroControl-Neuzählungen', () => {
  const prior = {
    fullCounts: validPayload().fullCounts,
    bankNoteCounts: validPayload().bankNoteCounts,
    bankAmountCents: 20000,
    eurocontrolTotalCents: 20000,
    reportedDifferenceCents: 900,
  };
  assert.equal(validateCashClosing(validPayload({ recountAttempts: [prior] })).recountAttempts.length, 1);
  assert.throws(()=>validateCashClosing(validPayload({ recountAttempts: Array(21).fill(prior) })), /Anzahl der Neuzählungen/);
});

