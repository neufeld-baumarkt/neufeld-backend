const TARGET_CENTS = 25_000;
const CASH_CENTS = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000];
const BANK_NOTE_CENTS = [500, 1_000, 2_000, 5_000, 10_000, 20_000];
const BANK_ONLY_NOTE_CENTS = [10_000, 20_000];

function readCounts(value, allowed, fieldName) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`${fieldName} fehlt.`);
  const result = {};
  for (const cents of allowed) {
    const quantity = Number(value[cents] ?? 0);
    if (!Number.isInteger(quantity) || quantity < 0) throw new Error(`${fieldName} enthält eine ungültige Anzahl.`);
    result[cents] = quantity;
  }
  return result;
}

function total(counts, centsValues) {
  return centsValues.reduce((sum, cents)=>sum + cents * counts[cents], 0);
}

function recountAttempts(value) {
  if (value == null) return [];
  if (!Array.isArray(value) || value.length > 20) throw new Error('Die Anzahl der Neuzählungen ist ungültig.');
  return value.map((attempt) => {
    const fullCounts = readCounts(attempt?.fullCounts, CASH_CENTS, 'Eine verworfene Kassenzählung');
    const bankNoteCounts = readCounts(attempt?.bankNoteCounts, BANK_NOTE_CENTS, 'Eine verworfene Bar-an-Bank-Zählung');
    const bankAmountCents = Number(attempt?.bankAmountCents);
    const eurocontrolTotalCents = Number(attempt?.eurocontrolTotalCents);
    const reportedDifferenceCents = Number(attempt?.reportedDifferenceCents);
    if (![bankAmountCents, eurocontrolTotalCents, reportedDifferenceCents].every(Number.isInteger)) {
      throw new Error('Eine verworfene Neuzählung enthält ungültige Beträge.');
    }
    return { fullCounts, bankNoteCounts, bankAmountCents, eurocontrolTotalCents, reportedDifferenceCents };
  });
}

function validateCashClosing(payload = {}) {
  const drawerNumber = Number(payload.drawerNumber);
  if (![1, 2].includes(drawerNumber)) throw new Error('Die Lade muss 1 oder 2 sein.');
  if (!payload.openingCountId && payload.proceededWithoutOpening !== true) throw new Error('Eine fehlende Einzählung muss ausdrücklich bestätigt werden.');

  const fullCounts = readCounts(payload.fullCounts, CASH_CENTS, 'Die vollständige Kassenzählung');
  const bankNoteCounts = readCounts(payload.bankNoteCounts, BANK_NOTE_CENTS, 'Die Bar-an-Bank-Zählung');
  const countedTotalCents = total(fullCounts, CASH_CENTS);
  const coinTotalCents = total(fullCounts, CASH_CENTS.filter((cents)=>cents<=200));
  const retainedCoinCents = Math.min(Math.floor(coinTotalCents / 500) * 500, TARGET_CENTS);
  const looseChangeCents = coinTotalCents - retainedCoinCents;

  if (countedTotalCents < TARGET_CENTS) throw new Error('Die Lade enthält weniger als 250,00 Euro.');
  if (Number(payload.retainedCoinCents) !== retainedCoinCents || Number(payload.looseChangeCents) !== looseChangeCents) throw new Error('Die Kleingeldberechnung stimmt nicht.');
  for (const cents of BANK_NOTE_CENTS) {
    if (bankNoteCounts[cents] > fullCounts[cents]) throw new Error('Bar an Bank enthält mehr Scheine als zuvor gezählt wurden.');
  }
  for (const cents of BANK_ONLY_NOTE_CENTS) {
    if (bankNoteCounts[cents] !== fullCounts[cents]) throw new Error(`Alle ${cents / 100}-Euro-Scheine müssen als Bar an Bank erfasst werden.`);
  }
  const bankAmountCents = total(bankNoteCounts, BANK_NOTE_CENTS);
  if (Number(payload.bankAmountCents) !== bankAmountCents || Number(payload.eurocontrolTotalCents) !== bankAmountCents) throw new Error('Die EuroControl-Gesamtsumme stimmt nicht mit Bar an Bank überein.');
  if (countedTotalCents - bankAmountCents - looseChangeCents !== TARGET_CENTS) throw new Error('Die Abschlussgleichung ergibt nicht 250,00 Euro.');

  const differenceCents = Number(payload.differenceCents);
  if (!Number.isInteger(differenceCents)) throw new Error('Die EuroControl-Differenz ist ungültig.');
  const notificationRequired = Math.abs(differenceCents) > 500;
  if (notificationRequired && payload.notificationConfirmed !== true) throw new Error('Alex und Peter müssen bei einer Einzelabweichung über 5,00 Euro informiert werden.');

  return {drawerNumber,fullCounts,retainedNoteCounts:{},bankNoteCounts,countedTotalCents,coinTotalCents,retainedCoinCents,looseChangeCents,bankAmountCents,differenceCents,notificationRequired,recountAttempts:recountAttempts(payload.recountAttempts)};
}

module.exports = { TARGET_CENTS, validateCashClosing };

