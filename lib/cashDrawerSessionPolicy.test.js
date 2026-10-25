const test = require('node:test');
const assert = require('node:assert/strict');
const { requireOpeningReplacement } = require('./cashDrawerSessionPolicy');

const active = { id: 42, drawer_number: 1, employee_name: 'Daniela Behr' };

test('erlaubt eine freie Lade ohne Ablösebestätigung', () => {
  assert.equal(requireOpeningReplacement(null, null), null);
});

test('verlangt für eine belegte Lade die exakt aktuelle Einzählung', () => {
  assert.equal(requireOpeningReplacement(active, 42), 42);
  assert.throws(() => requireOpeningReplacement(active, null), (error) => error.code === 'DRAWER_OCCUPIED');
  assert.throws(() => requireOpeningReplacement(active, 41), /ausdrücklich bestätigt/);
});
