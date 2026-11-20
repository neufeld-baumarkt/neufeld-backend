function amountProblem(message) {
  return Object.assign(new Error(message), { code: 'INVALID_SEARCH_AMOUNT' });
}

function parseAmountToCents(value) {
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || value === 0) {
      throw amountProblem('Bitte einen gültigen Betrag größer als 0 eingeben.');
    }
    return Math.abs(Math.round(value * 100));
  }

  const compact = String(value ?? '')
    .replace(/[€\s\u00a0']/g, '')
    .trim();

  if (!compact) {
    throw amountProblem('Bitte einen Betrag eingeben.');
  }

  let normalized;
  if (/^-?(?:\d{1,3}(?:\.\d{3})*|\d+)(?:,\d{1,2})?$/.test(compact)) {
    normalized = compact.replace(/\./g, '').replace(',', '.');
  } else if (/^-?\d+(?:\.\d{1,2})?$/.test(compact)) {
    normalized = compact;
  } else {
    throw amountProblem('Betrag bitte zum Beispiel als 1.234,56 eingeben.');
  }

  const amount = Number(normalized);
  const cents = Math.abs(Math.round(amount * 100));
  if (!Number.isSafeInteger(cents) || cents === 0) {
    throw amountProblem('Bitte einen gültigen Betrag größer als 0 eingeben.');
  }
  return cents;
}

function rowAmountCents(row) {
  const value = row?.zahlbetrag ?? row?.betrag;
  const amount = Number(value);
  if (!Number.isFinite(amount) || amount <= 0) return null;
  const cents = Math.round(amount * 100);
  return Number.isSafeInteger(cents) && cents > 0 ? cents : null;
}

function deriveGroupedStatus(rows) {
  const booked = rows.filter((row) => row.status === 'gebucht').length;
  if (booked === 0) return 'angekuendigt';
  if (booked === rows.length) return 'gebucht';
  return 'teilweise_gebucht';
}

function buildAmountSuggestions(rows, targetCents) {
  if (!Number.isSafeInteger(targetCents) || targetCents <= 0) {
    throw amountProblem('Ungültiger Suchbetrag.');
  }

  const usable = (Array.isArray(rows) ? rows : [])
    .map((row) => ({ row, amountCents: rowAmountCents(row) }))
    .filter((entry) => entry.amountCents !== null);

  const avisGroups = new Map();
  for (const entry of usable) {
    const avisNumber = String(entry.row.avis_nummer || '').trim();
    if (!avisNumber) continue;
    if (!avisGroups.has(avisNumber)) avisGroups.set(avisNumber, []);
    avisGroups.get(avisNumber).push(entry);
  }

  const avis = [];
  for (const [avisNumber, entries] of avisGroups) {
    const sumCents = entries.reduce((sum, entry) => sum + entry.amountCents, 0);
    if (sumCents !== targetCents) continue;
    const groupRows = entries.map((entry) => ({
      ...entry.row,
      typ: 'rechnung',
      suchbetrag: (entry.amountCents / 100).toFixed(2),
    }));
    avis.push({
      typ: 'avis',
      avis_nummer: avisNumber,
      anzahl_rechnungen: groupRows.length,
      gesamtsumme: (sumCents / 100).toFixed(2),
      status: deriveGroupedStatus(groupRows),
      rechnungen: groupRows,
    });
  }

  const rechnungen = usable
    .filter((entry) => entry.amountCents === targetCents)
    .map((entry) => ({
      ...entry.row,
      typ: 'rechnung',
      suchbetrag: (entry.amountCents / 100).toFixed(2),
    }));

  return { avis, rechnungen };
}

module.exports = {
  buildAmountSuggestions,
  parseAmountToCents,
  rowAmountCents,
};
