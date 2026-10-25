function cents(value, label) {
  const number = Number(value ?? 0);
  if (!Number.isInteger(number) || number < 0) throw new Error(`${label} muss ein nicht negativer Cent-Betrag sein.`);
  return number;
}

function calculateBusinessDayTotals({ closings = [], standardBalanceCents, baseSafeCountCents, looseAfterBaseCents }) {
  const bankNoteTotalCents = closings.reduce((sum, closing) => sum + cents(closing.bankAmountCents, 'Bar an Bank'), 0);
  const looseChangeTotalCents = closings.reduce((sum, closing) => sum + cents(closing.looseChangeCents, 'Kleingeld'), 0);
  const cashRevenueCents = bankNoteTotalCents + looseChangeTotalCents;
  const standard = cents(standardBalanceCents, 'Tresor-Standardbestand');
  const base = cents(baseSafeCountCents, 'Letzter bestätigter Tresorbestand');
  const newLoose = cents(looseAfterBaseCents, 'Kleingeld seit der letzten Tresorzählung');
  const safeBeforeSkimCents = base + newLoose;
  const skimmedFromSafeCents = Math.floor(Math.max(0, safeBeforeSkimCents - standard) / 500) * 500;
  return {
    bankNoteTotalCents,
    looseChangeTotalCents,
    cashRevenueCents,
    standardBalanceCents: standard,
    baseSafeCountCents: base,
    looseAfterBaseCents: newLoose,
    safeBeforeSkimCents,
    skimmedFromSafeCents,
    bankPayoutCents: bankNoteTotalCents + skimmedFromSafeCents,
    expectedSafeBalanceCents: safeBeforeSkimCents - skimmedFromSafeCents,
  };
}

function validateBusinessDayFinalization(payload, context) {
  if (!context.closings.length) throw new Error('Ohne Kassenabrechnung kann der Geschäftstag nicht abgeschlossen werden.');
  if (context.activeOpenings.length) throw new Error('Es sind noch aktive Kassenschichten vorhanden. Bitte zuerst alle Kassen abrechnen oder ablösen.');
  if (!context.finalSafeCount || !context.finalSafeCountIsCurrent) throw new Error('Nach der letzten Kassenabrechnung ist eine aktuelle Abendzählung erforderlich.');
  if (Number(payload.safeCountId) !== Number(context.finalSafeCount.id)) throw new Error('Die bestätigte Abendzählung ist nicht mehr aktuell.');
  if (cents(payload.eurocontrolCashTotalCents, 'EuroControl Bar an Bank') !== context.totals.cashRevenueCents) {
    throw new Error('Die Bar-an-Bank-Gesamtsumme aus EuroControl stimmt nicht mit den Einzelkassen überein.');
  }
  if (payload.bankPayoutConfirmed !== true || payload.safeBagConfirmed !== true) {
    throw new Error('Auszahlung und physischer SafeBag müssen ausdrücklich bestätigt werden.');
  }
  return context.totals;
}

module.exports = { calculateBusinessDayTotals, validateBusinessDayFinalization };
