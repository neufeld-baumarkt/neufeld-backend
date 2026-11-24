const HISTORICAL_DATE_ROLES = Object.freeze(['Admin', 'Supervisor']);
const HISTORICAL_DATE_USERS = new Map([
  ['alex neufeld', 'Geschäftsführer'],
  ['gökhan genc', 'Manager-1'],
  ['jörg menkehorst', 'Manager-1'],
]);

function berlinBusinessDate(now = new Date()) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Europe/Berlin', year: 'numeric', month: '2-digit', day: '2-digit',
  }).formatToParts(now);
  const value = Object.fromEntries(parts.map(({ type, value: part }) => [type, part]));
  return `${value.year}-${value.month}-${value.day}`;
}

function normalizeCashBusinessDate(value) {
  if (typeof value === 'string') {
    const match = value.match(/^(\d{4}-\d{2}-\d{2})/);
    if (match && isRealIsoDate(match[1])) return match[1];
  }
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return berlinBusinessDate(value);
  }
  throw Object.assign(new Error('Das Geschäftsdatum ist ungültig.'), { statusCode: 400 });
}

function isRealIsoDate(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const [year, month, day] = value.split('-').map(Number);
  const date = new Date(Date.UTC(year, month - 1, day));
  return date.getUTCFullYear() === year && date.getUTCMonth() === month - 1 && date.getUTCDate() === day;
}

function normalizeUserName(value) {
  return String(value || '').trim().replace(/\s+/g, ' ').toLocaleLowerCase('de-DE');
}

function canSelectHistoricalBusinessDate(userOrRole) {
  const user = typeof userOrRole === 'object' && userOrRole !== null
    ? userOrRole
    : { role: userOrRole };
  const role = String(user.role || '').trim();
  if (HISTORICAL_DATE_ROLES.includes(role)) return true;
  return HISTORICAL_DATE_USERS.get(normalizeUserName(user.name)) === role;
}

function resolveCashBusinessDate(req, now = new Date()) {
  const today = berlinBusinessDate(now);
  const requested = String(req.get?.('x-cash-business-date') || '').trim() || today;
  if (!isRealIsoDate(requested)) throw Object.assign(new Error('Das gewählte Geschäftsdatum ist ungültig.'), { statusCode: 400 });
  if (requested > today) throw Object.assign(new Error('Ein zukünftiger Geschäftstag kann nicht bearbeitet werden.'), { statusCode: 400 });
  if (requested !== today && !canSelectHistoricalBusinessDate(req.user)) {
    throw Object.assign(new Error('Für diesen Benutzer ist keine Nacherfassung freigegeben.'), { statusCode: 403 });
  }
  return requested;
}

module.exports = {
  HISTORICAL_DATE_ROLES,
  HISTORICAL_DATE_USERS,
  berlinBusinessDate,
  canSelectHistoricalBusinessDate,
  normalizeCashBusinessDate,
  resolveCashBusinessDate,
};
