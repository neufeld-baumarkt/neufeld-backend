const jwt = require('jsonwebtoken');

// A closing may span counting at the safe and work at EuroControl. The
// workflow proof is therefore valid for one working day, while the general
// PIN proof remains short-lived.
const CLOSING_CONTEXT_TTL_SECONDS = 12 * 60 * 60;

function signClosingContext(closedByPinId, responsibleEmployeePinId, branchId, drawerNumber, secret) {
  return jwt.sign({
    type: 'cash-closing-context',
    closedByPinId: Number(closedByPinId),
    responsibleEmployeePinId: Number(responsibleEmployeePinId),
    branchId: Number(branchId),
    drawerNumber: Number(drawerNumber),
  }, secret, { expiresIn: CLOSING_CONTEXT_TTL_SECONDS });
}

function verifyClosingContext(token, { branchId, drawerNumber, responsibleEmployeePinId }, secret) {
  try {
    const proof = jwt.verify(String(token || ''), secret);
    if (proof.type !== 'cash-closing-context'
      || Number(proof.branchId) !== Number(branchId)
      || Number(proof.drawerNumber) !== Number(drawerNumber)
      || Number(proof.responsibleEmployeePinId) !== Number(responsibleEmployeePinId)
      || !Number.isInteger(Number(proof.closedByPinId))) throw new Error('invalid closing context');
    return { ...proof, closedByPinId: Number(proof.closedByPinId) };
  } catch {
    throw Object.assign(new Error('Der Abrechnungskontext ist abgelaufen. Bitte die Abrechnung über „Fortsetzen“ wieder aufnehmen.'), { statusCode: 401 });
  }
}

module.exports = { CLOSING_CONTEXT_TTL_SECONDS, signClosingContext, verifyClosingContext };
