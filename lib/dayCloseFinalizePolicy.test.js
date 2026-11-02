const test = require('node:test');
const assert = require('node:assert/strict');
const { matchesCompletedFinalization, normalizeBabReconciliation } = require('./dayCloseFinalizePolicy');

test('normalisiert einen bestätigten BaB-Abgleich', () => {
  assert.deepEqual(normalizeBabReconciliation({ status:'confirmed', displayedTotalCents:12500 }, 12500), {
    status:'confirmed', displayedTotalCents:12500, eurocontrolTotalCents:12500, differenceCents:0, note:null,
  });
});

test('verlangt bei einer BaB-Abweichung Wert und Begründung', () => {
  assert.deepEqual(normalizeBabReconciliation({ status:'difference', displayedTotalCents:12500, eurocontrolTotalCents:12000, note:'500 Cent fehlen' }, 12500), {
    status:'difference', displayedTotalCents:12500, eurocontrolTotalCents:12000, differenceCents:-500, note:'500 Cent fehlen',
  });
  assert.throws(() => normalizeBabReconciliation({ status:'difference', displayedTotalCents:12500, eurocontrolTotalCents:12000, note:'' }, 12500));
});

test('erkennt eine identische wiederholte V2-Finalisierung', () => {
  assert.equal(matchesCompletedFinalization({ workflowVersion:'v2', finalSafeCount:{ id:91 } }, { safeCountId:91 }), true);
  assert.equal(matchesCompletedFinalization({ workflowVersion:'v2', finalSafeCount:{ id:91 } }, { safeCountId:92 }), false);
});
