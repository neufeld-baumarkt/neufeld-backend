const test = require('node:test');
const assert = require('node:assert/strict');
const { moneyInputToCents, openingPreview, closingPreview, safePreview, dayClosePreview } = require('./cashWorkflowCalculationPolicy');

test('parses German and international money input on the backend', () => {
  assert.equal(moneyInputToCents('1.234,56 €'), 123456);
  assert.equal(moneyInputToCents('1234.56'), 123456);
  assert.equal(moneyInputToCents('1,2'), 120);
  assert.equal(moneyInputToCents('nope'), null);
});

test('opening preview is calculated from raw denomination counts', () => {
  const result = openingPreview({ counts:{5000:5}, attemptsCount:1, branchId:1, drawerNumber:1 });
  assert.equal(result.countedCents, 25000);
  assert.equal(result.differenceCents, 0);
  assert.equal(result.nextStep, 'recommend-other-person');
});

test('closing preview returns the authoritative drawer equation', () => {
  const result = closingPreview({
    fullCounts:{20000:1,5000:2,2000:1},
    bankNoteCounts:{20000:1,5000:1},
    differenceKind:'minus', differenceAmount:'5,01',
  });
  assert.equal(result.countedTotalCents, 32000);
  assert.equal(result.looseChangeCents, 0);
  assert.equal(result.bankAmountCents, 25000);
  assert.equal(result.remainderCents, 7000);
  assert.equal(result.differenceCents, -501);
  assert.equal(result.notificationRequired, true);
});

test('safe preview uses persisted branch settings supplied by the route', () => {
  const result = safePreview({ settings:{fixedDrawerCents:50000,littleBerthaCents:25000}, looseMoney:'12,50', coinRollCounts:{200:1}, banknoteCounts:{500:2}, eurocontrolTotal:'822,50' });
  assert.equal(result.countedTotalCents, 82250);
  assert.equal(result.differenceCents, 0);
});

test('day-close preview validates metrics and calculates bank booking on the backend', () => {
  const result = dayClosePreview({
    context:{ totals:{cashRevenueCents:50000,looseChangeTotalCents:500,standardBalanceCents:100000},baseSafeCount:{countedTotalCents:100000} },
    report:{dailyRevenue:'1.000,00',customerCount:'100',averageReceipt:'10,00',eurocontrolBank:'500,00'},
    banknoteCounts:{100:4,50:1,5:9},
  });
  assert.equal(result.report.valid, true);
  assert.equal(result.report.data.dailyRevenueCents, 100000);
  assert.equal(result.bank.bookingTotalCents, 50000);
  assert.equal(result.bank.valid, true);
});

test('day-close preview verrechnet Kassendifferenzen nicht doppelt mit EuroControl', () => {
  const result = dayClosePreview({
    context:{
      totals:{cashRevenueCents:274123,looseChangeTotalCents:1123,standardBalanceCents:250000},
      baseSafeCount:{countedTotalCents:250000},
      closings:[{differenceCents:0},{differenceCents:-40},{differenceCents:-1},{differenceCents:-1}],
    },
    report:{dailyRevenue:'1.000,00',customerCount:'100',averageReceipt:'10,00',eurocontrolBank:'2.741,23'},
    banknoteCounts:{100:6,50:35,20:8,10:14,5:16},
  });

  assert.equal(result.bank.adjustedEurocontrolBankCents, 274123);
  assert.equal(result.bank.expectedBanknoteTotalCents, 273000);
  assert.equal(result.bank.banknoteTotalCents, 273000);
  assert.equal(result.bank.valid, true);
});
