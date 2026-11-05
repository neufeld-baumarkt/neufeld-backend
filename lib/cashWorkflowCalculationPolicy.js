const { DENOMINATION_CENTS, TARGET_CENTS, countCashCents, matchesImmediatePreviousClosing } = require('./cashOpeningPolicy');
const { calculateSafeTotal } = require('./cashSafePolicy');

const BANK_NOTE_CENTS = Object.freeze([500, 1000, 2000, 5000, 10000, 20000]);
const BANK_ONLY_NOTE_CENTS = Object.freeze([10000, 20000]);
const DAY_CLOSE_BANKNOTES = Object.freeze([5, 10, 20, 50, 100, 200]);

function quantity(value) {
  const number = Number(value ?? 0);
  return Number.isSafeInteger(number) && number >= 0 ? number : 0;
}

function moneyInputToCents(value) {
  const input = String(value ?? '').trim().replace(/\s/g, '').replace(/€/g, '');
  if (!input) return null;
  const comma = input.lastIndexOf(',');
  const dot = input.lastIndexOf('.');
  let normalized = input;
  if (comma >= 0 && dot >= 0) {
    const decimal = Math.max(comma, dot);
    const decimals = input.slice(decimal + 1);
    const integer = input.slice(0, decimal).replace(/[.,]/g, '');
    if (!/^\d+$/.test(integer) || !/^\d{1,2}$/.test(decimals)) return null;
    normalized = `${integer}.${decimals}`;
  } else if (comma >= 0) {
    if ((input.match(/,/g) || []).length !== 1) return null;
    normalized = input.replace(',', '.');
  } else if (dot >= 0 && (input.match(/\./g) || []).length !== 1) return null;
  if (!/^\d+(?:\.\d{1,2})?$/.test(normalized)) return null;
  const cents = Math.round(Number(normalized) * 100);
  return Number.isSafeInteger(cents) ? cents : null;
}

function openingPreview({ counts = {}, attemptsCount = 0, branchId, drawerNumber, previousClosing }) {
  const countedCents = countCashCents(counts);
  const denominationAmounts=Object.fromEntries(DENOMINATION_CENTS.map((cents)=>[cents,cents*quantity(counts[cents])]));
  const differenceCents = countedCents - TARGET_CENTS;
  const carryoverMatched = matchesImmediatePreviousClosing({ branchId, drawerNumber, openingDifferenceCents:differenceCents, previousClosing });
  const completed = quantity(attemptsCount);
  const nextStep = completed <= 0 ? 'same-person-recount' : completed === 1 ? 'recommend-other-person' : 'adjust-to-target';
  return { targetCents:TARGET_CENTS, countedCents, differenceCents, carryoverMatched, nextStep,denominationAmounts,
    adjustmentAction:differenceCents>0?'skim':'refill',adjustmentAmountCents:Math.abs(differenceCents) };
}

function closingPreview({ fullCounts = {}, bankNoteCounts = {}, differenceKind = 'zero', differenceAmount = '' }) {
  const fullAmounts=Object.fromEntries(DENOMINATION_CENTS.map((cents)=>[cents,cents*quantity(fullCounts[cents])]));
  const bankAmounts=Object.fromEntries(BANK_NOTE_CENTS.map((cents)=>[cents,cents*quantity(bankNoteCounts[cents])]));
  const countedTotalCents = DENOMINATION_CENTS.reduce((sum, cents) => sum + cents * quantity(fullCounts[cents]), 0);
  const coinTotalCents = DENOMINATION_CENTS.filter((cents) => cents <= 200)
    .reduce((sum, cents) => sum + cents * quantity(fullCounts[cents]), 0);
  const retainedCoinCents = Math.min(Math.floor(coinTotalCents / 500) * 500, TARGET_CENTS);
  const looseChangeCents = coinTotalCents - retainedCoinCents;
  const notesNeededCents = TARGET_CENTS - retainedCoinCents;
  const bankAmountCents = BANK_NOTE_CENTS.reduce((sum, cents) => sum + cents * quantity(bankNoteCounts[cents]), 0);
  let bankProblem = null;
  for (const cents of BANK_NOTE_CENTS) {
    if (quantity(bankNoteCounts[cents]) > quantity(fullCounts[cents])) bankProblem = `Bei ${cents / 100} Euro wurden mehr Bar-an-Bank-Scheine eingetragen als zuvor gezählt.`;
  }
  for (const cents of BANK_ONLY_NOTE_CENTS) {
    if (quantity(bankNoteCounts[cents]) !== quantity(fullCounts[cents])) bankProblem = `Alle ${cents / 100}-Euro-Scheine müssen als Bar an Bank erfasst werden.`;
  }
  const remainderCents = countedTotalCents - bankAmountCents - looseChangeCents;
  const parsedDifference = differenceKind === 'zero' ? 0 : moneyInputToCents(differenceAmount);
  const differenceCents = parsedDifference == null ? null : differenceKind === 'minus' ? -parsedDifference : parsedDifference;
  const notificationRequired = differenceCents != null && Math.abs(differenceCents) > 500;
  const floatSuggestion = suggestFloatNotes({ notesNeededCents, fullCounts });
  return {
    targetCents:TARGET_CENTS, countedTotalCents, coinTotalCents, retainedCoinCents, looseChangeCents,
    notesNeededCents, bankAmountCents, remainderCents, bankProblem, differenceCents,
    notificationRequired, floatSuggestion,fullAmounts,bankAmounts,
  };
}

function suggestFloatNotes({ notesNeededCents, fullCounts }) {
  if (!Number.isInteger(notesNeededCents) || notesNeededCents < 0 || notesNeededCents % 500 !== 0) return null;
  const targetTwentyCount = Math.min(3, Math.floor(notesNeededCents / 2000));
  let best = null;
  for (let twenties = 0; twenties <= Math.min(quantity(fullCounts[2000]), Math.floor(notesNeededCents / 2000)); twenties += 1) {
    const afterTwenties = notesNeededCents - twenties * 2000;
    for (let tens = 0; tens <= Math.min(quantity(fullCounts[1000]), Math.floor(afterTwenties / 1000)); tens += 1) {
      const afterTens = afterTwenties - tens * 1000;
      if (afterTens % 500 !== 0) continue;
      const fives = afterTens / 500;
      if (fives > quantity(fullCounts[500])) continue;
      const score = [Math.abs(twenties-targetTwentyCount), Math.abs(fives-tens), twenties>targetTwentyCount?1:0, fives+tens+twenties];
      if (!best || score.some((value,index)=>value<best.score[index]&&score.slice(0,index).every((entry,earlier)=>entry===best.score[earlier]))) {
        best = { score, counts:{500:fives,1000:tens,2000:twenties} };
      }
    }
  }
  return best ? { counts:best.counts, totalCents:notesNeededCents } : null;
}

function safePreview({ settings, looseMoney = '', coinRollCounts = {}, banknoteCounts = {}, eurocontrolTotal = '', eurocontrolTotalCents: suppliedEurocontrolCents = null }) {
  const looseMoneyCents = moneyInputToCents(looseMoney);
  const result = calculateSafeTotal({
    fixedDrawerCents:settings.fixedDrawerCents,
    littleBerthaCents:settings.littleBerthaCents,
    looseMoneyCents:looseMoneyCents ?? 0,
    coinRollCounts,
    banknoteCounts,
  });
  const eurocontrolTotalCents = Number.isInteger(suppliedEurocontrolCents) ? suppliedEurocontrolCents : moneyInputToCents(eurocontrolTotal);
  const rollAmountCents=Object.fromEntries(Object.entries(result.coinRollCounts).map(([key,value])=>[key,value*({1:50,2:100,5:250,10:400,20:800,50:2000,100:2500,200:5000}[key])]));
  const banknoteAmountCents=Object.fromEntries(Object.entries(result.banknoteCounts).map(([key,value])=>[key,value*Number(key)]));
  return { ...result, looseMoneyCents, eurocontrolTotalCents,rollAmountCents,banknoteAmountCents,
    differenceCents:eurocontrolTotalCents == null ? null : result.countedTotalCents-eurocontrolTotalCents };
}

function dayClosePreview({ context, report = {}, reportData = null, banknoteCounts = {}, safeTargetInput = '' }) {
  const totals = context.totals || {};
  const dailyRevenueCents = reportData ? Number(reportData.dailyRevenueCents) : moneyInputToCents(report.dailyRevenue);
  const averageReceiptCents = reportData ? Number(reportData.averageReceiptCents) : moneyInputToCents(report.averageReceipt);
  const eurocontrolBankCents = reportData ? Number(reportData.eurocontrolBankCents) : moneyInputToCents(report.eurocontrolBank);
  const customerCount = reportData ? Number(reportData.customerCount) : Number(String(report.customerCount ?? '').trim());
  const errors = {};
  if (!Number.isInteger(dailyRevenueCents) || dailyRevenueCents < 0) errors.dailyRevenue = 'Bitte Tagesumsatz eingeben.';
  if (!Number.isInteger(customerCount) || customerCount < 1) errors.customerCount = 'Bitte Kundenanzahl eingeben.';
  if (!Number.isInteger(averageReceiptCents) || averageReceiptCents < 0) errors.averageReceipt = 'Bitte Durchschnittsbon eingeben.';
  if (!Number.isInteger(eurocontrolBankCents) || eurocontrolBankCents < 0) errors.eurocontrolBank = 'Bitte Bar an Bank laut Filialbericht eingeben.';
  const counts = Object.fromEntries(DAY_CLOSE_BANKNOTES.map((value)=>[value,quantity(banknoteCounts[value])]));
  const banknoteTotalCents = DAY_CLOSE_BANKNOTES.reduce((sum,value)=>sum+counts[value]*value*100,0);
  const individualCashBanknotes = context.individualCashBanknotes || Object.fromEntries(DAY_CLOSE_BANKNOTES.map((denomination)=>{
    const cents=denomination*100;
    const count=(context.closings||[]).reduce((sum,closing)=>sum+quantity(closing?.bankNoteCounts?.[cents]??closing?.bankNoteCounts?.[String(cents)]),0);
    return [denomination,count];
  }));
  const looseChangeCents = Number(totals.looseChangeTotalCents ?? context.looseChangeCents ?? 0);
  const latestSafeCountCents = Number(context.baseSafeCount?.countedTotalCents ?? context.latestSafeCount?.countedTotalCents);
  const safeStandardCents = Number(totals.standardBalanceCents ?? context.safeStandardCents);
  const adjustedEurocontrolBankCents = Number.isInteger(eurocontrolBankCents) ? eurocontrolBankCents : null;
  const roundedEurocontrolBankCents = Number.isInteger(adjustedEurocontrolBankCents) ? Math.floor(adjustedEurocontrolBankCents/500)*500 : null;
  const changeTransferCents = Math.floor(Math.max(0,looseChangeCents)/500)*500;
  const expectedBanknoteTotalCents = Number.isInteger(roundedEurocontrolBankCents) ? roundedEurocontrolBankCents-changeTransferCents : null;
  const projectedSafeCents = Number.isInteger(latestSafeCountCents) ? latestSafeCountCents+looseChangeCents-changeTransferCents : null;
  const skimCents = Number.isInteger(projectedSafeCents)&&Number.isInteger(safeStandardCents) ? Math.max(0,Math.floor((projectedSafeCents-safeStandardCents)/500)*500) : null;
  const safeTransferCents = Number.isInteger(skimCents) ? changeTransferCents+skimCents : null;
  const finalCounts = { ...counts };
  if (Number.isInteger(safeTransferCents)) finalCounts[5] += safeTransferCents/500;
  const bookingTotalCents = Number.isInteger(roundedEurocontrolBankCents)&&Number.isInteger(skimCents) ? roundedEurocontrolBankCents+skimCents : null;
  const finalDenominationTotalCents = DAY_CLOSE_BANKNOTES.reduce((sum,value)=>sum+finalCounts[value]*value*100,0);
  const countAmounts=Object.fromEntries(DAY_CLOSE_BANKNOTES.map((value)=>[value,counts[value]*value*100]));
  const finalCountAmounts=Object.fromEntries(DAY_CLOSE_BANKNOTES.map((value)=>[value,finalCounts[value]*value*100]));
  return {
    base:{
      bankPayoutCents:Number.isInteger(totals.cashRevenueCents)?totals.cashRevenueCents:(Number.isInteger(context.bankPayoutCents)?context.bankPayoutCents:null),
      bankPayoutStatus:'provisional',looseChangeCents,closingCount:Array.isArray(context.closings)?context.closings.length:Number(context.closingCount||0),
      latestSafeCount:context.baseSafeCount||context.latestSafeCount||null,
      safeStandardCents:Number.isInteger(totals.standardBalanceCents)?totals.standardBalanceCents:(Number.isInteger(context.safeStandardCents)?context.safeStandardCents:null),
      fixedDrawerCents:Number(context.baseSafeCount?.fixedDrawerCents||context.latestSafeCount?.fixedDrawerCents||context.fixedDrawerCents||0),
      littleBerthaCents:Number(context.baseSafeCount?.littleBerthaCents||context.latestSafeCount?.littleBerthaCents||context.littleBerthaCents||0),
      individualCashBanknotes,eurocontrolSafeTargetCents:null,processStates:{},
    },
    report:{ valid:Object.keys(errors).length===0, errors, data:{dailyRevenueCents,customerCount,averageReceiptCents,eurocontrolBankCents} },
    safeTargetCents:moneyInputToCents(safeTargetInput),
    bank:{ counts,finalCounts,countAmounts,finalCountAmounts,banknoteTotalCents,adjustedEurocontrolBankCents,roundedEurocontrolBankCents,changeTransferCents,expectedBanknoteTotalCents,
      projectedSafeCents,skimCents,safeTransferCents,expectedSafeAfterSkimCents:Number.isInteger(projectedSafeCents)&&Number.isInteger(skimCents)?projectedSafeCents-skimCents:null,
      bookingTotalCents,finalDenominationTotalCents,valid:Number.isInteger(expectedBanknoteTotalCents)&&expectedBanknoteTotalCents>=0&&banknoteTotalCents===expectedBanknoteTotalCents&&Number.isInteger(skimCents)&&finalDenominationTotalCents===bookingTotalCents },
  };
}

module.exports = { moneyInputToCents, openingPreview, closingPreview, safePreview, dayClosePreview };
