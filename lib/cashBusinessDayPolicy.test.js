const test = require('node:test');
const assert = require('node:assert/strict');
const { calculateBusinessDayTotals, validateBusinessDayFinalization } = require('./cashBusinessDayPolicy');

test('berechnet Auszahlung und Tresorrest einschließlich Übertrag vom Vortag', () => {
  const totals = calculateBusinessDayTotals({
    closings:[{bankAmountCents:100_000,looseChangeCents:320}],
    standardBalanceCents:250_000,baseSafeCountCents:250_200,looseAfterBaseCents:320,
  });
  assert.equal(totals.cashRevenueCents,100_320);
  assert.equal(totals.skimmedFromSafeCents,500);
  assert.equal(totals.bankPayoutCents,100_500);
  assert.equal(totals.expectedSafeBalanceCents,250_020);
});

test('blockiert den Tagesabschluss ohne aktuelle Abendzählung oder bei offenen Kassen', () => {
  const base={closings:[{}],activeOpenings:[],finalSafeCount:{id:7},finalSafeCountIsCurrent:true,totals:{cashRevenueCents:1234}};
  assert.throws(()=>validateBusinessDayFinalization({safeCountId:7,eurocontrolCashTotalCents:1234,bankPayoutConfirmed:true,safeBagConfirmed:true},{...base,activeOpenings:[{id:1}]}),/aktive/);
  assert.throws(()=>validateBusinessDayFinalization({safeCountId:7,eurocontrolCashTotalCents:1234,bankPayoutConfirmed:true,safeBagConfirmed:true},{...base,finalSafeCountIsCurrent:false}),/Abendzählung/);
  assert.equal(validateBusinessDayFinalization({safeCountId:7,eurocontrolCashTotalCents:1234,bankPayoutConfirmed:true,safeBagConfirmed:true},base),base.totals);
});
