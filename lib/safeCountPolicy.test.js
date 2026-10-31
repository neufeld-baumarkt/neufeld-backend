const test = require('node:test');
const assert = require('node:assert/strict');
const { calculateSafeTotal, participantLabels, validateSafeCountPayload } = require('./safeCountPolicy');

const settings = { fixedDrawerCents: 50_000, littleBerthaCents: 100_000 };
const screenshotCount = {
  looseMoneyCents: 472,
  coinRollCounts: { 1: 17, 2: 19, 5: 24, 10: 26, 20: 16, 50: 7, 100: 4, 200: 4 },
  banknoteCounts: { 500: 4, 1000: 10, 2000: 6, 5000: 0, 10000: 0, 20000: 0 },
};

test('berechnet den Excel-Beispielbestand centgenau mit festen Grundbeständen', () => {
  const result = calculateSafeTotal({ ...settings, ...screenshotCount });
  assert.equal(result.rollTotalCents, 75_950);
  assert.equal(result.banknoteTotalCents, 24_000);
  assert.equal(result.countedTotalCents, 250_422);
});

test('Abendzählung vergleicht ausschließlich den physischen Gesamtbestand mit EuroControl', () => {
  const result = validateSafeCountPayload({ countType: 'evening', mode: 'full_count', firstPin: '4711', secondPin: '2580', eurocontrolTotalCents: 250_500, ...screenshotCount }, settings);
  assert.equal(result.differenceCents, -78);
});

test('morgendliche Stückelungsbestätigung referenziert zwingend die letzte Abendzählung', () => {
  assert.throws(() => validateSafeCountPayload({ countType: 'morning', mode: 'confirmation', firstPin: '4711', secondPin: '2580' }, settings), /Abendzählung/);
  assert.equal(validateSafeCountPayload({ countType: 'morning', mode: 'confirmation', previousEveningCountId: 'previous', firstPin: '4711', secondPin: '2580' }, settings).result, null);
});

test('Vier-Augen-Rollen sind fest und derselbe PIN wird abgewiesen', () => {
  assert.deepEqual(participantLabels('handover'), ['Übergabe durch', 'Übernahme durch']);
  assert.throws(() => validateSafeCountPayload({ countType: 'evening', mode: 'full_count', firstPin: '4711', secondPin: '4711', eurocontrolTotalCents: 250_422, ...screenshotCount }, settings), /unterschiedliche Personen/);
});

test('negative oder gebrochene Stückzahlen werden abgewiesen', () => {
  assert.throws(() => calculateSafeTotal({ ...settings, ...screenshotCount, coinRollCounts: { ...screenshotCount.coinRollCounts, 1: 1.5 } }), /ganze/);
});
