const test = require('node:test');
const assert = require('node:assert/strict');
const { countCash, resolveBranch, validatePayload } = require('./tageskassePolicy');

test('Filialrolle kann Filiale nicht überschreiben', () => {
  assert.equal(resolveBranch({ role: 'Filiale', filiale: 'Ahaus' }, 'Vreden'), 'Ahaus');
});

test('Zählung wird centgenau summiert', () => {
  assert.equal(countCash({ 50: 2, 0.5: 3 }), 10150);
});

test('Abschluss mit Differenz braucht Begründung und Bestätigung', () => {
  const result = validatePayload({ businessDate: '2026-10-23', branch: 'Telgte', counts: { 100: 1 },
    expectedCash: 99, floatAmount: 50, ecRegister: 0, ecTerminal: 0,
    cardRegister: 0, cardTerminal: 0 }, { submitting: true });
  assert.equal(result.totalDifference, 1);
  assert.equal(result.errors.length, 2);
});
