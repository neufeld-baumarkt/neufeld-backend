const test = require('node:test');
const assert = require('node:assert/strict');

const { findActivePinMatches } = require('./employeePinIdentity');

const compare = async (pin, hash) => pin === hash;

test('identifiziert einen Mitarbeiter unabhängig von seiner Stammfiliale', async () => {
  const candidates = [
    { id: 1, filiale_id: 4, pin_hash: '4711', must_change_pin: false },
    { id: 2, filiale_id: null, pin_hash: '2580', must_change_pin: false },
  ];
  assert.deepEqual((await findActivePinMatches(candidates, '2580', compare)).map(({ id }) => id), [2]);
});

test('liefert alle Treffer, damit doppelte globale PINs sicher abgewiesen werden', async () => {
  const candidates = [
    { id: 1, pin_hash: '4711', must_change_pin: false },
    { id: 2, pin_hash: '4711', must_change_pin: false },
  ];
  assert.deepEqual((await findActivePinMatches(candidates, '4711', compare)).map(({ id }) => id), [1, 2]);
});

test('ignoriert Erst-PINs und vorübergehend gesperrte Mitarbeiter', async () => {
  const candidates = [
    { id: 1, pin_hash: '4711', must_change_pin: true },
    { id: 2, pin_hash: '4711', must_change_pin: false, locked_until: '2999-01-01T00:00:00Z' },
  ];
  assert.deepEqual(await findActivePinMatches(candidates, '4711', compare), []);
});
