const DENOMINATIONS = [500, 200, 100, 50, 20, 10, 5, 2, 1, 0.5, 0.2, 0.1, 0.05, 0.02, 0.01];
const BRANCHES = new Set(['Telgte', 'Ahaus', 'Vreden', 'Münster']);

function cents(value) {
  const numberValue = Number(String(value ?? '').replace(',', '.'));
  return Number.isFinite(numberValue) ? Math.round(numberValue * 100) : null;
}

function countCash(counts = {}) {
  return DENOMINATIONS.reduce((total, denomination) => {
    const count = Number(counts[denomination]);
    return total + Math.round(denomination * 100) * (Number.isInteger(count) && count >= 0 ? count : 0);
  }, 0);
}

function validatePayload(payload = {}, { submitting = false } = {}) {
  const errors = [];
  if (!/^\d{4}-\d{2}-\d{2}$/.test(payload.businessDate || '')) errors.push('Datum ist ungültig.');
  const berlinToday = new Intl.DateTimeFormat('sv-SE', { timeZone: 'Europe/Berlin' }).format(new Date());
  if (payload.businessDate > berlinToday) errors.push('Ein zukünftiger Geschäftstag kann nicht abgeschlossen werden.');
  if (!BRANCHES.has(payload.branch)) errors.push('Filiale ist ungültig.');
  const fields = ['expectedCash', 'floatAmount', 'ecRegister', 'ecTerminal', 'cardRegister', 'cardTerminal'];
  if (submitting && fields.some((field) => cents(payload[field]) === null || cents(payload[field]) < 0)) {
    errors.push('Alle Beträge müssen vollständig und positiv sein.');
  }
  const counted = countCash(payload.counts);
  if (submitting && counted <= 0) errors.push('Bargeldzählung fehlt.');
  const totalDifference = counted - (cents(payload.expectedCash) || 0)
    + (cents(payload.ecTerminal) || 0) - (cents(payload.ecRegister) || 0)
    + (cents(payload.cardTerminal) || 0) - (cents(payload.cardRegister) || 0);
  if (submitting && Math.abs(totalDifference) > 0 && String(payload.differenceReason || '').trim().length < 10) {
    errors.push('Differenz muss mit mindestens 10 Zeichen begründet werden.');
  }
  if (submitting && payload.confirmed !== true) errors.push('Abschlusskontrolle muss bestätigt werden.');
  return { errors, countedCash: counted / 100, totalDifference: totalDifference / 100 };
}

function resolveBranch(user, requested) {
  if (user?.role === 'Filiale') return user.filiale || null;
  return BRANCHES.has(requested) ? requested : null;
}

module.exports = { BRANCHES, countCash, resolveBranch, validatePayload };
