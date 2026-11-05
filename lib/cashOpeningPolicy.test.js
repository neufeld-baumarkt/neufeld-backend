const test = require('node:test');
const assert = require('node:assert/strict');
const { countCashCents, previousClosingHasDifference, validateOpeningCount } = require('./cashOpeningPolicy');

test('summiert Stückelungen centgenau', () => {
  assert.equal(countCashCents({ 5000: 4, 2000: 2, 500: 2 }), 25_000);
});

test('erkennt jede offene Vorgängerdifferenz derselben Lade unabhängig vom Betrag', () => {
  const previousClosing = { id: 9, branch_id: 4, drawer_number: 1, status: 'closed', difference_cents: -500, resolved: false };
  assert.equal(previousClosingHasDifference({ branchId: 4, drawerNumber: 1, previousClosing }), true);
  assert.equal(previousClosingHasDifference({ branchId: 4, drawerNumber: 2, previousClosing }), false);
  assert.equal(previousClosingHasDifference({ branchId: 4, drawerNumber: 1, previousClosing: { ...previousClosing, difference_cents: -400 } }), true);
});

test('erlaubt jede ausdrücklich bestätigte Differenz bewusst in der Lade', () => {
  const previousClosing = { id: 9, branch_id: 4, drawer_number: 1, status: 'closed', difference_cents: -500, resolved: false };
  const result = validateOpeningCount({ drawerNumber: 1, carryoverDecision: 'keep-in-drawer', attempts: [{ counts: { 5000: 5, 1: 1 } }] }, previousClosing, 4);
  assert.equal(result.openingBalanceCents, 25_001);
  assert.equal(result.previousClosingId, 9);
});

test('erlaubt eine bewusst übernommene Differenz auch ohne Vorgängerdifferenz', () => {
  const result = validateOpeningCount({ drawerNumber: 1, carryoverDecision: 'keep-in-drawer', attempts: [{ counts: { 5000: 5, 500: 1 } }] }, null, 4);
  assert.equal(result.openingBalanceCents, 25_500);
  assert.equal(result.previousClosingId, null);
});

test('erlaubt den ausdrücklich bestätigten Ausgleich bereits nach der ersten Abweichung', () => {
  const result = validateOpeningCount({ drawerNumber: 1, carryoverDecision: 'normalize-to-target', attempts: [{ counts: { 5000: 5, 500: 1 } }] }, null, 4);
  assert.equal(result.openingBalanceCents, 25_000);
  assert.equal(result.finalCountedCents, 25_500);
});

test('protokolliert Neuzählungen, ohne sie für eine Entscheidung zu erzwingen', () => {
  const previousClosing = { id: 9, branch_id: 4, drawer_number: 1, status: 'closed', difference_cents: -500, resolved: false };
  const result = validateOpeningCount({
    drawerNumber: 1,
    carryoverDecision: 'keep-in-drawer',
    attempts: [{ counts: { 5000: 5, 200: 1 } }, { counts: { 5000: 5, 500: 1 } }],
  }, previousClosing, 4);
  assert.equal(result.openingBalanceCents, 25_500);
  assert.equal(result.previousClosingId, 9);
});
