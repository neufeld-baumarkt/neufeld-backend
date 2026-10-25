const COUNT_TYPES = new Set(['morning', 'handover', 'closing']);
const COUNT_MODES = new Set(['confirmation', 'full_count']);

const COIN_ROLL_VALUES_CENTS = Object.freeze({
  1: 50, 2: 100, 5: 250, 10: 400, 20: 800, 50: 2000, 100: 2500, 200: 5000,
});
const BANKNOTE_VALUES_CENTS = Object.freeze([500, 1000, 2000, 5000, 10000, 20000]);

function nonNegativeInteger(value, label) {
  const number = Number(value ?? 0);
  if (!Number.isInteger(number) || number < 0) {
    throw Object.assign(new Error(`${label} muss eine ganze, nicht negative Zahl sein.`), { statusCode: 400 });
  }
  return number;
}

function normalizeCounts(input, allowedKeys, label) {
  const source = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  return Object.fromEntries(allowedKeys.map((key) => [String(key), nonNegativeInteger(source[key], label)]));
}

function calculateSafeTotal({ fixedDrawerCents, littleBerthaCents, looseMoneyCents, coinRollCounts, banknoteCounts }) {
  const drawers = nonNegativeInteger(fixedDrawerCents, 'Der feste Kassenladenbestand');
  const littleBertha = nonNegativeInteger(littleBerthaCents, 'Der Bestand von Little Bertha');
  const loose = nonNegativeInteger(looseMoneyCents, 'Loses Geld');
  const rolls = normalizeCounts(coinRollCounts, Object.keys(COIN_ROLL_VALUES_CENTS), 'Die Rollenanzahl');
  const notes = normalizeCounts(banknoteCounts, BANKNOTE_VALUES_CENTS, 'Die Scheinanzahl');
  const rollTotalCents = Object.entries(COIN_ROLL_VALUES_CENTS)
    .reduce((sum, [denomination, rollValue]) => sum + rolls[denomination] * rollValue, 0);
  const banknoteTotalCents = BANKNOTE_VALUES_CENTS
    .reduce((sum, denomination) => sum + notes[String(denomination)] * denomination, 0);
  return {
    fixedDrawerCents: drawers, littleBerthaCents: littleBertha, looseMoneyCents: loose,
    coinRollCounts: rolls, banknoteCounts: notes, rollTotalCents, banknoteTotalCents,
    countedTotalCents: drawers + littleBertha + loose + rollTotalCents + banknoteTotalCents,
  };
}

function validateSafeCountPayload(payload = {}, settings = {}) {
  const countType = String(payload.countType || '');
  const mode = String(payload.mode || '');
  if (!COUNT_TYPES.has(countType)) throw Object.assign(new Error('Der Zählzeitpunkt ist ungültig.'), { statusCode: 400 });
  if (!COUNT_MODES.has(mode)) throw Object.assign(new Error('Der Zählmodus ist ungültig.'), { statusCode: 400 });
  if (mode === 'confirmation' && countType !== 'morning') {
    throw Object.assign(new Error('Nur die morgendliche Kontrolle darf ohne vollständige Neuzählung bestätigt werden.'), { statusCode: 400 });
  }
  if (mode === 'confirmation' && !payload.previousEveningCountId) {
    throw Object.assign(new Error('Für die morgendliche Bestätigung fehlt die Abendzählung des Vortags.'), { statusCode: 400 });
  }
  const result = mode === 'full_count' ? calculateSafeTotal({
    fixedDrawerCents: settings.fixedDrawerCents,
    littleBerthaCents: settings.littleBerthaCents,
    looseMoneyCents: payload.looseMoneyCents,
    coinRollCounts: payload.coinRollCounts,
    banknoteCounts: payload.banknoteCounts,
  }) : null;
  let eurocontrolTotalCents = null;
  let differenceCents = null;
  if (countType === 'closing') {
    eurocontrolTotalCents = nonNegativeInteger(payload.eurocontrolTotalCents, 'Der EuroControl-Tresorbestand');
    if (!result) throw Object.assign(new Error('Die Abendzählung muss vollständig erfasst werden.'), { statusCode: 400 });
    differenceCents = result.countedTotalCents - eurocontrolTotalCents;
  }
  return { countType, mode, result, eurocontrolTotalCents, differenceCents };
}

function participantLabels(countType) {
  if (countType === 'morning') return ['Frühschicht 1', 'Frühschicht 2'];
  if (countType === 'handover') return ['Übernahme durch', 'Übergabe durch'];
  return ['Spätschicht 1', 'Spätschicht 2'];
}

module.exports = { BANKNOTE_VALUES_CENTS, COIN_ROLL_VALUES_CENTS, calculateSafeTotal, participantLabels, validateSafeCountPayload };
