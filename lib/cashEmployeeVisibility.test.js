const test = require('node:test');
const assert = require('node:assert/strict');

const { visibleCashEmployees } = require('./cashEmployeeVisibility');

test('zeigt nur Mitarbeiter der gewählten Filiale und unternehmensweite Mitarbeiter', () => {
  const employees = [
    { id: 1, filiale_id: 7 },
    { id: 2, filiale_id: 9 },
    { id: 3, filiale_id: null },
  ];
  assert.deepEqual(visibleCashEmployees(employees, 7).map(({ id }) => id), [1, 3]);
});

test('vergleicht numerische Filial-IDs unabhängig vom Datenbanktyp', () => {
  assert.equal(visibleCashEmployees([{ id: 1, filiale_id: '7' }], 7).length, 1);
});
