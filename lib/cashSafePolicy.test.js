const test = require('node:test');
const assert = require('node:assert/strict');
const { calculateSafeContext, countSafeCashCents, validateSafeCountType } = require('./cashSafePolicy');

test('berechnet die Abschöpfung immer in vollen fünf Euro', () => {
  assert.deepEqual(calculateSafeContext({ standardBalanceCents:250000, previousCountedCents:250000, addedLooseChangeCents:1200 }), {
    standardBalanceCents:250000, baseBalanceCents:250000, addedLooseChangeCents:1200,
    beforeSkimCents:251200, recommendedSkimCents:1000, expectedBalanceCents:250200,
  });
  assert.equal(calculateSafeContext({ standardBalanceCents:250000, previousCountedCents:250200, addedLooseChangeCents:320 }).recommendedSkimCents,500);
  assert.equal(calculateSafeContext({ standardBalanceCents:250000, previousCountedCents:250200, addedLooseChangeCents:320 }).expectedBalanceCents,250020);
});

test('summiert nur gültige Stückzahlen', () => {
  assert.equal(countSafeCashCents({ 1:22, 20000:12 }),240022);
  assert.throws(()=>countSafeCashCents({ 50000:1 }),/unbekannten Wert/);
  assert.throws(()=>countSafeCashCents({ 100:'1,5' }),/ganze/);
  assert.equal(validateSafeCountType('closing'),'closing');
  assert.throws(()=>validateSafeCountType('other'),/Bitte wähle/);
});
