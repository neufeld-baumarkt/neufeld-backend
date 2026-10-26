const test = require('node:test');
const assert = require('node:assert/strict');
const { countCashCents, matchesImmediatePreviousClosing, validateOpeningCount } = require('./cashOpeningPolicy');

test('summiert Stückelungen centgenau', () => {
  assert.equal(countCashCents({ 5000: 4, 2000: 2, 500: 2 }), 25_000);
});

test('prüft ausschließlich die unmittelbar übergebene letzte Abrechnung derselben Lade', () => {
  const previousClosing = { id: 9, branch_id: 4, drawer_number: 1, status: 'closed', difference_cents: -500, resolved: false };
  assert.equal(matchesImmediatePreviousClosing({ branchId: 4, drawerNumber: 1, openingDifferenceCents: 500, previousClosing }), true);
  assert.equal(matchesImmediatePreviousClosing({ branchId: 4, drawerNumber: 2, openingDifferenceCents: 500, previousClosing }), false);
  assert.equal(matchesImmediatePreviousClosing({ branchId: 4, drawerNumber: 1, openingDifferenceCents: 400, previousClosing }), false);
});

test('erlaubt eine exakt passende Vorgängerdifferenz bewusst in der Lade', () => {
  const previousClosing = { id: 9, branch_id: 4, drawer_number: 1, status: 'closed', difference_cents: -500, resolved: false };
  const result = validateOpeningCount({ drawerNumber: 1, carryoverDecision: 'keep-in-drawer', attempts: [{ counts: { 5000: 5, 500: 1 } }] }, previousClosing, 4);
  assert.equal(result.openingBalanceCents, 25_500);
  assert.equal(result.previousClosingId, 9);
});

test('erlaubt den ausdrücklich bestätigten Ausgleich bereits nach der ersten Abweichung', () => {
  const result = validateOpeningCount({ drawerNumber: 1, carryoverDecision: 'normalize-to-target', attempts: [{ counts: { 5000: 5, 500: 1 } }] }, null, 4);
  assert.equal(result.openingBalanceCents, 25_000);
  assert.equal(result.finalCountedCents, 25_500);
});

test('prüft eine übernommene Vorgängerdifferenz gegen die letzte durchgeführte Zählung', () => {
  const previousClosing = { id: 9, branch_id: 4, drawer_number: 1, status: 'closed', difference_cents: -500, resolved: false };
  const result = validateOpeningCount({
    drawerNumber: 1,
    carryoverDecision: 'keep-in-drawer',
    attempts: [{ counts: { 5000: 5, 200: 1 } }, { counts: { 5000: 5, 500: 1 } }],
  }, previousClosing, 4);
  assert.equal(result.openingBalanceCents, 25_500);
  assert.equal(result.previousClosingId, 9);
});
