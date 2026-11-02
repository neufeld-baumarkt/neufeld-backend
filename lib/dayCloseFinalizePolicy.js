function problem(message, statusCode = 400) {
  return Object.assign(new Error(message), { statusCode });
}

function nonNegativeCents(value, label) {
  const cents = Number(value);
  if (!Number.isSafeInteger(cents) || cents < 0) throw problem(`${label} ist ungültig.`);
  return cents;
}

function normalizeBabReconciliation(input, expectedTotalCents) {
  const expected = nonNegativeCents(expectedTotalCents, 'Die aufgelaufene Gesamtsumme BaB');
  if (!input || typeof input !== 'object' || Array.isArray(input)) {
    throw problem('Der BaB-Kontrollabgleich fehlt.', 409);
  }
  const displayed = nonNegativeCents(input.displayedTotalCents, 'Die angezeigte Gesamtsumme BaB');
  if (displayed !== expected) throw problem('Die angezeigte Gesamtsumme BaB ist nicht mehr aktuell.', 409);
  const status = String(input.status || '');
  if (status === 'confirmed') {
    return { status, displayedTotalCents: expected, eurocontrolTotalCents: expected, differenceCents: 0, note: null };
  }
  if (status !== 'difference') throw problem('Der BaB-Kontrollabgleich ist ungültig.');
  const eurocontrolTotalCents = nonNegativeCents(input.eurocontrolTotalCents, 'Der BaB-Wert aus EuroControl');
  if (eurocontrolTotalCents === expected) throw problem('Bei identischen BaB-Werten bitte „Jo, passt!“ verwenden.');
  const note = String(input.note || '').trim();
  if (note.length < 3 || note.length > 500) throw problem('Bitte die BaB-Abweichung kurz beschreiben.');
  return {
    status,
    displayedTotalCents: expected,
    eurocontrolTotalCents,
    differenceCents: eurocontrolTotalCents - expected,
    note,
  };
}

function matchesCompletedFinalization(report, input = {}) {
  return report?.workflowVersion === 'v2'
    && Boolean(report?.finalSafeCount?.id)
    && String(report.finalSafeCount.id) === String(input.safeCountId || '');
}

module.exports = { matchesCompletedFinalization, normalizeBabReconciliation };
