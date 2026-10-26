const TARGET_CENTS = 25_000;
const DENOMINATION_CENTS = Object.freeze([50_000, 20_000, 10_000, 5_000, 2_000, 1_000, 500, 200, 100, 50, 20, 10, 5, 2, 1]);
const DENOMINATION_SET = new Set(DENOMINATION_CENTS.map(String));

function normalizeCounts(counts) {
  if (!counts || typeof counts !== 'object' || Array.isArray(counts)) {
    throw Object.assign(new Error('Die Stückelung fehlt.'), { statusCode: 400 });
  }

  const normalized = {};
  for (const [rawDenomination, rawQuantity] of Object.entries(counts)) {
    if (!DENOMINATION_SET.has(String(rawDenomination))) {
      throw Object.assign(new Error('Die Stückelung enthält einen unbekannten Geldwert.'), { statusCode: 400 });
    }
    const quantity = Number(rawQuantity);
    if (!Number.isSafeInteger(quantity) || quantity < 0 || quantity > 10_000) {
      throw Object.assign(new Error('Alle Stückzahlen müssen ganze, nicht negative Zahlen sein.'), { statusCode: 400 });
    }
    if (quantity > 0) normalized[String(rawDenomination)] = quantity;
  }
  return normalized;
}

function countCashCents(counts) {
  const normalized = normalizeCounts(counts);
  return Object.entries(normalized).reduce((sum, [denomination, quantity]) => sum + Number(denomination) * quantity, 0);
}

function matchesImmediatePreviousClosing({ branchId, drawerNumber, openingDifferenceCents, previousClosing }) {
  if (!previousClosing || previousClosing.status !== 'closed' || previousClosing.resolved === true) return false;
  if (Number(previousClosing.branch_id) !== Number(branchId) || Number(previousClosing.drawer_number) !== Number(drawerNumber)) return false;
  const closingDifference = Number(previousClosing.difference_cents);
  return closingDifference !== 0 && closingDifference === -Number(openingDifferenceCents);
}

function validateOpeningCount(input, previousClosing, branchId) {
  const drawerNumber = Number(input?.drawerNumber);
  if (![1, 2].includes(drawerNumber)) {
    throw Object.assign(new Error('Bitte Lade 1 oder Lade 2 auswählen.'), { statusCode: 400 });
  }
  if (!Array.isArray(input?.attempts) || input.attempts.length === 0 || input.attempts.length > 10) {
    throw Object.assign(new Error('Die Zählversuche fehlen oder sind unplausibel.'), { statusCode: 400 });
  }

  const attempts = input.attempts.map((attempt) => {
    const counts = normalizeCounts(attempt?.counts);
    const countedCents = countCashCents(counts);
    if (countedCents <= 0) throw Object.assign(new Error('Leere Zählversuche sind nicht zulässig.'), { statusCode: 400 });
    return { counts, countedCents, employeePinId: Number(attempt.employeePinId), employeeName: String(attempt.employeeName || '').trim() };
  });

  const initialCountedCents = attempts[0].countedCents;
  const finalCountedCents = attempts.at(-1).countedCents;
  const initialDifferenceCents = initialCountedCents - TARGET_CENTS;
  const finalDifferenceCents = finalCountedCents - TARGET_CENTS;
  const carryoverDecision = String(input?.carryoverDecision || 'none');
  const carryoverMatched = matchesImmediatePreviousClosing({ branchId, drawerNumber, openingDifferenceCents: finalDifferenceCents, previousClosing });

  if (!['none', 'keep-in-drawer', 'normalize-to-target'].includes(carryoverDecision)) {
    throw Object.assign(new Error('Die Entscheidung zur Vorgängerdifferenz ist ungültig.'), { statusCode: 400 });
  }
  if (carryoverDecision === 'keep-in-drawer') {
    if (!carryoverMatched) throw Object.assign(new Error('Die Differenz passt nicht mehr zur unmittelbar letzten Abrechnung dieser Lade.'), { statusCode: 409 });
  } else if (carryoverDecision === 'none' && finalCountedCents !== TARGET_CENTS) {
    throw Object.assign(new Error('Der endgültige Anfangsbestand muss exakt 250,00 € betragen.'), { statusCode: 400 });
  }
  const openingBalanceCents = carryoverDecision === 'normalize-to-target' ? TARGET_CENTS : finalCountedCents;

  return {
    drawerNumber,
    attempts,
    initialCountedCents,
    initialDifferenceCents,
    finalCountedCents,
    openingBalanceCents,
    openingDifferenceCents: openingBalanceCents - TARGET_CENTS,
    carryoverDecision,
    carryoverMatched,
    previousClosingId: carryoverMatched ? Number(previousClosing.id) : null,
  };
}

module.exports = { TARGET_CENTS, DENOMINATION_CENTS, normalizeCounts, countCashCents, matchesImmediatePreviousClosing, validateOpeningCount };
