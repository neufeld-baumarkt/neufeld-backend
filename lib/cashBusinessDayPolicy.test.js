const test = require('node:test');
const assert = require('node:assert/strict');
const { calculateBusinessDayTotals, validateBusinessDayFinalization } = require('./cashBusinessDayPolicy');

test('berechnet Auszahlung und Tresorrest einschließlich Übertrag vom Vortag', () => {
  const totals = calculateBusinessDayTotals({
    closings:[{bankAmountCents:100_000,looseChangeCents:320}],
    standardBalanceCents:250_000,baseSafeCountCents:250_200,looseAfterBaseCents:320,
  });
  assert.equal(totals.cashRevenueCents,100_320);
  assert.equal(totals.looseChangePayoutCents,0);
  assert.equal(totals.looseChangeRemainderCents,320);
  assert.equal(totals.additionalSafeSkimCents,500);
  assert.equal(totals.bankPayoutCents,100_500);
  assert.equal(totals.expectedSafeBalanceCents,250_020);
});

test('nimmt volle Fuenfer aus dem Tageskleingeld und schöpft den Tresor danach erneut ab', () => {
  const totals = calculateBusinessDayTotals({
    closings:[{bankAmountCents:247_500,looseChangeCents:1_459}],
    standardBalanceCents:250_000,baseSafeCountCents:250_255,looseAfterBaseCents:0,
  });
  assert.equal(totals.cashRevenueCents,248_959);
  assert.equal(totals.looseChangePayoutCents,1_000);
  assert.equal(totals.looseChangeRemainderCents,459);
  assert.equal(totals.safeAfterLooseChangeCents,250_714);
  assert.equal(totals.additionalSafeSkimCents,500);
  assert.equal(totals.bankPayoutCents,249_000);
  assert.equal(totals.expectedSafeBalanceCents,250_214);
});

test('blockiert den Tagesabschluss ohne aktuelle Abendzählung oder bei offenen Kassen', () => {
  const base={closings:[{}],activeOpenings:[],finalSafeCount:{id:7},finalSafeCountIsCurrent:true,totals:{cashRevenueCents:1234,looseChangePayoutCents:0,additionalSafeSkimCents:0}};
  assert.throws(()=>validateBusinessDayFinalization({safeCountId:7,eurocontrolCashTotalCents:1234,bankPayoutConfirmed:true,safeBagConfirmed:true},{...base,activeOpenings:[{id:1}]}),/aktive/);
  assert.throws(()=>validateBusinessDayFinalization({safeCountId:7,eurocontrolCashTotalCents:1234,bankPayoutConfirmed:true,safeBagConfirmed:true},{...base,finalSafeCountIsCurrent:false}),/Abendzählung/);
  assert.equal(validateBusinessDayFinalization({safeCountId:7,eurocontrolCashTotalCents:1234,bankPayoutConfirmed:true,safeBagConfirmed:true},base),base.totals);
});

test('fordert beide notwendigen Tresorentnahmen ausdrücklich an', () => {
  const context={closings:[{}],activeOpenings:[],finalSafeCount:{id:7},finalSafeCountIsCurrent:true,totals:{cashRevenueCents:248_959,looseChangePayoutCents:1_000,additionalSafeSkimCents:500}};
  const payload={safeCountId:7,eurocontrolCashTotalCents:248_959,bankPayoutConfirmed:true,safeBagConfirmed:true};
  assert.throws(()=>validateBusinessDayFinalization(payload,context),/Kleingeldbetrags/);
  assert.throws(()=>validateBusinessDayFinalization({...payload,looseChangePayoutConfirmed:true},context),/zusätzliche Tresorabschöpfung/);
  assert.equal(validateBusinessDayFinalization({...payload,looseChangePayoutConfirmed:true,additionalSafeSkimConfirmed:true},context),context.totals);
});
