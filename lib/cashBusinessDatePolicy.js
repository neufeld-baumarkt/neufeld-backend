const HISTORICAL_DATE_ROLES = Object.freeze(['Admin', 'Supervisor']);

function berlinBusinessDate(now = new Date()) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Europe/Berlin', year: 'numeric', month: '2-digit', day: '2-digit',
  }).formatToParts(now);
  const value = Object.fromEntries(parts.map(({ type, value: part }) => [type, part]));
  return `${value.year}-${value.month}-${value.day}`;
}

function isRealIsoDate(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const [year, month, day] = value.split('-').map(Number);
  const date = new Date(Date.UTC(year, month - 1, day));
  return date.getUTCFullYear() === year && date.getUTCMonth() === month - 1 && date.getUTCDate() === day;
}

function canSelectHistoricalBusinessDate(role) {
  return HISTORICAL_DATE_ROLES.includes(String(role || '').trim());
}

function resolveCashBusinessDate(req, now = new Date()) {
  const today = berlinBusinessDate(now);
  const requested = String(req.get?.('x-cash-business-date') || '').trim() || today;
  if (!isRealIsoDate(requested)) throw Object.assign(new Error('Das gewählte Geschäftsdatum ist ungültig.'), { statusCode: 400 });
  if (requested > today) throw Object.assign(new Error('Ein zukünftiger Geschäftstag kann nicht bearbeitet werden.'), { statusCode: 400 });
  if (requested !== today && !canSelectHistoricalBusinessDate(req.user?.role)) {
    throw Object.assign(new Error('Historische Geschäftstage dürfen nur Admin und Supervisor nacherfassen.'), { statusCode: 403 });
  }
  return requested;
}

module.exports = { HISTORICAL_DATE_ROLES, berlinBusinessDate, canSelectHistoricalBusinessDate, resolveCashBusinessDate };
