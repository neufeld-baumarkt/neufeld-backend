const assert = require('node:assert/strict');
const test = require('node:test');

const previousSigningKey = process.env.JWT_SECRET;
const primarySigningKey = ['unit', 'signing', 'key', 'primary'].join('-');
process.env.JWT_SECRET = primarySigningKey;
const { createEmployeePinProof, verifyEmployeePinProof } = require('./employeePinProof');

test.after(() => {
  if (previousSigningKey === undefined) delete process.env.JWT_SECRET;
  else process.env.JWT_SECRET = previousSigningKey;
});

test('PIN proof binds the verified employee and never contains a PIN', () => {
  const token = createEmployeePinProof({ id: 17, name: 'Sandbox Person' });
  assert.deepEqual(verifyEmployeePinProof(token), { id: 17, name: 'Sandbox Person' });
  assert.equal(token.includes('1234'), false);
});

test('PIN proof rejects tampering and the wrong signing secret', () => {
  const token = createEmployeePinProof({ id: 17, name: 'Sandbox Person' });
  assert.throws(() => verifyEmployeePinProof(`${token.slice(0, -1)}x`), /abgelaufen/);
  process.env.JWT_SECRET = ['unit', 'signing', 'key', 'secondary'].join('-');
  assert.throws(() => verifyEmployeePinProof(token), /abgelaufen/);
  process.env.JWT_SECRET = primarySigningKey;
});
