const test = require('node:test');
const assert = require('node:assert/strict');
const { validateCashCorrection } = require('./cashCorrectionPolicy');

test('validiert Begründung und korrigierte Differenz', () => {
  assert.deepEqual(validateCashCorrection({ reason:'Dezimalzeichen berichtigt', differenceCents:89 }), {
    reason:'Dezimalzeichen berichtigt', differenceCents:89, notificationRequired:false,
  });
  assert.throws(()=>validateCashCorrection({ reason:'zu kurz', differenceCents:89 }), /mindestens 10/);
});

test('erzwingt auch bei Korrekturen die Meldung oberhalb von fünf Euro', () => {
  assert.throws(()=>validateCashCorrection({ reason:'Betrag laut Protokoll', differenceCents:501 }), /Alex und Peter/);
  assert.equal(validateCashCorrection({ reason:'Betrag laut Protokoll', differenceCents:-501, notificationConfirmed:true }).notificationRequired,true);
});
