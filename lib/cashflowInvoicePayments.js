const ALLOWED_SKONTO_RATES = new Set([0, 2, 3]);

function roundMoney(value) {
  return Math.round((Number(value) + Number.EPSILON) * 100) / 100;
}

function normalizeSkontoRate(value) {
  if (value === undefined) return undefined;
  if (value === null || value === '') return 0;

  const rate = Number(value);
  if (!ALLOWED_SKONTO_RATES.has(rate)) {
    throw new Error('Skonto muss 0, 2 oder 3 Prozent betragen.');
  }
  return rate;
}

function calculatePlannedAmount(grossAmount, skontoRate = 0) {
  const gross = Number(grossAmount);
  const rate = normalizeSkontoRate(skontoRate) ?? 0;

  if (!Number.isFinite(gross) || gross === 0) {
    throw new Error('Der Original-Rechnungsbetrag ist ungültig.');
  }

  return roundMoney(gross * (1 - rate / 100));
}

function calculatePaymentAmount({ grossAmount, plannedAmount, skontoRate = 0, useSkonto }) {
  const hasGross = grossAmount !== null && grossAmount !== undefined && grossAmount !== '';
  const gross = hasGross ? Number(grossAmount) : NaN;
  const planned = Number(plannedAmount);

  if (useSkonto && Number.isFinite(gross)) {
    return calculatePlannedAmount(gross, skontoRate);
  }
  if (!useSkonto && Number.isFinite(gross)) {
    return roundMoney(gross);
  }
  if (!Number.isFinite(planned)) {
    throw new Error('Für die Buchung ist kein gültiger Zahlbetrag vorhanden.');
  }
  return roundMoney(planned);
}

module.exports = {
  ALLOWED_SKONTO_RATES,
  calculatePaymentAmount,
  calculatePlannedAmount,
  normalizeSkontoRate,
  roundMoney,
};
