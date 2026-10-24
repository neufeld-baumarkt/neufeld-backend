const test = require('node:test');
const assert = require('node:assert/strict');

const {
  DEFAULT_EMPLOYEE_PIN,
  branchId,
  employeeName,
  personalPin,
} = require('./employeePinPolicy');

test('verwendet ausschließlich 0000 als Standard-PIN', () => {
  assert.equal(DEFAULT_EMPLOYEE_PIN, '0000');
  assert.throws(() => personalPin('0000'), /ausschließlich der einmalige Standard-PIN/);
  assert.equal(personalPin('4711'), '4711');
});

test('normalisiert Vor- und Nachnamen getrennt', () => {
  assert.deepEqual(employeeName({ first_name: ' Peter ', last_name: ' Neufeld ' }), {
    firstName: 'Peter',
    lastName: 'Neufeld',
    displayName: 'Peter Neufeld',
  });
  assert.throws(() => employeeName({ first_name: 'P', last_name: 'Neufeld' }), /Vorname/);
});

test('akzeptiert nur positive Filial-IDs', () => {
  assert.equal(branchId('4'), 4);
  assert.throws(() => branchId(0), /gültige Filiale/);
});
