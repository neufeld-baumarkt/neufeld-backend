const test = require('node:test');
const assert = require('node:assert/strict');
const { CLOSING_CONTEXT_TTL_SECONDS, signClosingContext, verifyClosingContext } = require('./cashClosingContextProof');

const secret = 'local-test-secret';

test('closing context carries the initial employee confirmation through the workflow', () => {
  const token = signClosingContext(17, 23, 4, 2, secret);
  const proof = verifyClosingContext(token, { branchId: 4, drawerNumber: 2, responsibleEmployeePinId: 23 }, secret);
  assert.equal(proof.closedByPinId, 17);
  assert.equal(CLOSING_CONTEXT_TTL_SECONDS, 12 * 60 * 60);
});

test('closing context cannot be used for another drawer or responsible employee', () => {
  const token = signClosingContext(17, 23, 4, 2, secret);
  assert.throws(
    () => verifyClosingContext(token, { branchId: 4, drawerNumber: 1, responsibleEmployeePinId: 23 }, secret),
    /Abrechnungskontext/,
  );
  assert.throws(
    () => verifyClosingContext(token, { branchId: 4, drawerNumber: 2, responsibleEmployeePinId: 99 }, secret),
    /Abrechnungskontext/,
  );
});
