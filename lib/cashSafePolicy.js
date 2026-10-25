const CASH_DENOMINATION_CENTS = Object.freeze([1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000]);
const SAFE_COUNT_TYPES = Object.freeze(['morning', 'handover', 'closing']);

function countSafeCashCents(counts = {}) {
  if (!counts || typeof counts !== 'object' || Array.isArray(counts)) throw new Error('Die Tresorstückelung ist ungültig.');
  const allowed = new Set(CASH_DENOMINATION_CENTS.map(String));
  for (const key of Object.keys(counts)) {
    if (!allowed.has(String(key))) throw new Error('Die Tresorstückelung enthält einen unbekannten Wert.');
  }
  return CASH_DENOMINATION_CENTS.reduce((total, cents) => {
    const raw = counts[cents] ?? 0;
    if (raw === '') return total;
    const quantity = Number(raw);
    if (!Number.isInteger(quantity) || quantity < 0 || quantity > 100000) {
      throw new Error('Alle Stückzahlen müssen ganze, nicht negative Zahlen sein.');
    }
    return total + cents * quantity;
  }, 0);
}

function calculateSafeContext({ standardBalanceCents, previousCountedCents, addedLooseChangeCents = 0 }) {
  const standard = Number(standardBalanceCents);
  const base = previousCountedCents == null ? standard : Number(previousCountedCents);
  const loose = Number(addedLooseChangeCents);
  if (![standard, base, loose].every(Number.isInteger) || standard < 0 || base < 0 || loose < 0) {
    throw new Error('Der theoretische Tresorbestand konnte nicht berechnet werden.');
  }
  const beforeSkimCents = base + loose;
  const excessCents = Math.max(0, beforeSkimCents - standard);
  const recommendedSkimCents = Math.floor(excessCents / 500) * 500;
  return {
    standardBalanceCents: standard,
    baseBalanceCents: base,
    addedLooseChangeCents: loose,
    beforeSkimCents,
    recommendedSkimCents,
    expectedBalanceCents: beforeSkimCents - recommendedSkimCents,
  };
}

function validateSafeCountType(value) {
  const type = String(value || '').trim();
  if (!SAFE_COUNT_TYPES.includes(type)) throw new Error('Bitte wähle Morgenzählung, Schichtübergabe oder Schlusszählung.');
  return type;
}

module.exports = { CASH_DENOMINATION_CENTS, SAFE_COUNT_TYPES, calculateSafeContext, countSafeCashCents, validateSafeCountType };
