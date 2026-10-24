function validateCashCorrection(payload = {}) {
  const reason = String(payload.reason || '').trim();
  if (reason.length < 10) throw new Error('Bitte begründe die Korrektur mit mindestens 10 Zeichen.');
  if (reason.length > 1000) throw new Error('Die Korrekturbegründung darf höchstens 1000 Zeichen lang sein.');
  const differenceCents = Number(payload.differenceCents);
  if (!Number.isInteger(differenceCents)) throw new Error('Die korrigierte Differenz ist ungültig.');
  const notificationRequired = Math.abs(differenceCents) > 500;
  if (notificationRequired && payload.notificationConfirmed !== true) {
    throw new Error('Alex und Peter müssen bei einer Einzelabweichung über 5,00 Euro informiert werden.');
  }
  return { differenceCents, notificationRequired, reason };
}

module.exports = { validateCashCorrection };
