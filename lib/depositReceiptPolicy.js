const RECEIPT_NOTE_CENTS = Object.freeze([500, 1000, 2000, 5000, 10000, 20000]);

function digits(value, { min = 1, max = 40 } = {}) {
  const normalized = String(value ?? '').replace(/\D/g, '');
  return normalized.length >= min && normalized.length <= max ? normalized : null;
}

function germanMoneyToCents(value) {
  const normalized = String(value ?? '').trim().replace(/\s/g, '').replace(/\./g, '').replace(',', '.');
  if (!normalized) return null;
  const amount = Number(normalized);
  return Number.isFinite(amount) && amount >= 0 ? Math.round(amount * 100) : null;
}

function germanDateToIso(value) {
  const match = String(value ?? '').match(/^(\d{1,2})\.(\d{1,2})\.(\d{2}|\d{4})$/);
  if (!match) return null;
  const year = match[3].length === 2 ? 2000 + Number(match[3]) : Number(match[3]);
  const month = Number(match[2]);
  const day = Number(match[1]);
  const date = new Date(Date.UTC(year, month - 1, day));
  if (date.getUTCFullYear() !== year || date.getUTCMonth() !== month - 1 || date.getUTCDate() !== day) return null;
  return `${year.toString().padStart(4, '0')}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
}

function validIsoDate(value) {
  const match = String(value ?? '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  const date = new Date(`${value}T00:00:00.000Z`);
  return !Number.isNaN(date.valueOf()) && date.toISOString().slice(0, 10) === value ? value : null;
}

function normalizeCounts(counts = {}) {
  const normalized = {};
  for (const cents of RECEIPT_NOTE_CENTS) {
    const count = Number(counts[cents] ?? 0);
    normalized[cents] = Number.isInteger(count) && count >= 0 ? count : 0;
  }
  return normalized;
}

function banknoteTotalCents(counts = {}) {
  const normalized = normalizeCounts(counts);
  return RECEIPT_NOTE_CENTS.reduce((sum, cents) => sum + cents * normalized[cents], 0);
}

function parseDepositReceiptText(text) {
  const source = String(text ?? '').replace(/\r/g, '');
  const flattened = source.replace(/[ \t]+/g, ' ');
  const header = flattened.match(/Datum\s+Uhrzeit\s+Tx-?Nr\s+Terminal\s*\n?\s*(\d{1,2}\.\d{1,2}\.\d{2,4})\s+(\d{1,2}:\d{2})\s+(\d{4,})\s+(\d{4,})/i);
  const counts = {};
  const linePattern = /(5|10|20|50|100|200)[,.]00\s*EUR\s*[*xX]\s*(\d+)\s*=\s*([\d.]+,\d{2})\s*EUR/gi;
  let line;
  while ((line = linePattern.exec(flattened))) {
    counts[Number(line[1]) * 100] = Number(line[2]);
  }
  const totalMatch = flattened.match(/Summe\s*=\s*([\d.]+,\d{2})\s*EUR/i);
  const purposeMatch = source.match(/Verwendungszweck\s*:\s*\n?\s*["“]?([^\n"”]+)["”]?/i);
  const purpose = purposeMatch?.[1]?.trim() || null;
  return {
    depositDate: germanDateToIso(header?.[1]),
    depositTime: /^([01]\d|2[0-3]):[0-5]\d$/.test(header?.[2] || '') ? header[2] : null,
    transactionNumber: digits(header?.[3], { min: 4 }),
    terminalNumber: digits(header?.[4], { min: 4 }),
    purpose: /^eingezahlte\s+Noten\s*:?$/i.test(purpose || '') ? null : purpose,
    banknoteCounts: normalizeCounts(counts),
    receiptTotalCents: germanMoneyToCents(totalMatch?.[1]),
  };
}

function validateDepositReceipt(payload = {}) {
  const errors = [];
  const depositDate = validIsoDate(payload.depositDate);
  const depositTime = /^([01]\d|2[0-3]):[0-5]\d$/.test(payload.depositTime || '') ? payload.depositTime : null;
  const transactionNumber = digits(payload.transactionNumber, { min: 4 });
  const terminalNumber = digits(payload.terminalNumber, { min: 4 });
  const receiptTotalCents = Number(payload.receiptTotalCents);
  const banknoteCounts = normalizeCounts(payload.banknoteCounts);
  const calculatedTotalCents = banknoteTotalCents(banknoteCounts);
  if (!depositDate) errors.push('Einzahlungsdatum fehlt oder ist ungültig.');
  const berlinToday = new Intl.DateTimeFormat('sv-SE', { timeZone: 'Europe/Berlin' }).format(new Date());
  if (depositDate && depositDate > berlinToday) errors.push('Einzahlungsdatum darf nicht in der Zukunft liegen.');
  if (!depositTime) errors.push('Einzahlungszeit fehlt oder ist ungültig.');
  if (!transactionNumber) errors.push('Transaktionsnummer fehlt oder ist ungültig.');
  if (!terminalNumber) errors.push('Terminalnummer fehlt oder ist ungültig.');
  if (!Number.isInteger(receiptTotalCents) || receiptTotalCents <= 0) errors.push('Belegsumme fehlt oder ist ungültig.');
  if (calculatedTotalCents !== receiptTotalCents) errors.push('Scheinstückelung und Belegsumme stimmen nicht überein.');
  if (payload.onedriveConfirmed !== true) errors.push('Die manuelle OneDrive-Ablage muss bestätigt werden.');
  return {
    errors,
    normalized: {
      depositDate,
      depositTime,
      transactionNumber,
      terminalNumber,
      purpose: String(payload.purpose || '').trim().slice(0, 120) || null,
      banknoteCounts,
      receiptTotalCents,
      calculatedTotalCents,
      ocrConfidence: Math.max(0, Math.min(100, Number(payload.ocrConfidence) || 0)),
    },
  };
}

function matchingClosingSets(closings = [], receiptTotalCents) {
  const candidates = closings
    .map((closing) => ({ ...closing, expectedDepositCents: Number(closing.expectedDepositCents) }))
    .filter((closing) => Number.isInteger(closing.expectedDepositCents) && closing.expectedDepositCents > 0);
  const matches = [];
  const visit = (index, selected, total) => {
    if (total === receiptTotalCents && selected.length) matches.push(selected.map((item) => item.id));
    if (total >= receiptTotalCents) return;
    for (let cursor = index; cursor < candidates.length; cursor += 1) {
      visit(cursor + 1, [...selected, candidates[cursor]], total + candidates[cursor].expectedDepositCents);
    }
  };
  visit(0, [], 0);
  return matches;
}

module.exports = {
  RECEIPT_NOTE_CENTS,
  banknoteTotalCents,
  germanDateToIso,
  germanMoneyToCents,
  matchingClosingSets,
  normalizeCounts,
  parseDepositReceiptText,
  validateDepositReceipt,
};
