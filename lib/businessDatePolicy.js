const HISTORICAL_ENTRY_ROLES = new Set(['Admin', 'Supervisor']);
const HISTORICAL_ENTRY_USERS = new Map([
  ['alex neufeld', 'Geschäftsführer'],
  ['gökhan genc', 'Manager-1'],
  ['jörg menkehorst', 'Manager-1'],
]);

function normalizeUserName(value) {
  return String(value || '').trim().replace(/\s+/g, ' ').toLocaleLowerCase('de-DE');
}

function mayEnterHistoricalBusinessDate({ role, name } = {}) {
  const normalizedRole = String(role || '').trim();
  if (HISTORICAL_ENTRY_ROLES.has(normalizedRole)) return true;
  return HISTORICAL_ENTRY_USERS.get(normalizeUserName(name)) === normalizedRole;
}

function berlinBusinessDate(date = new Date()) {
  return new Intl.DateTimeFormat('sv-SE', { timeZone: 'Europe/Berlin' }).format(date);
}

function resolveAllowedBusinessDate({ requestedDate, role, name, today = berlinBusinessDate() } = {}) {
  const businessDate = String(requestedDate || today);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(businessDate)) {
    throw Object.assign(new Error('Der Geschäftstag ist ungültig.'), { statusCode: 400 });
  }
  if (businessDate > today) {
    throw Object.assign(new Error('Ein zukünftiger Geschäftstag ist nicht zulässig.'), { statusCode: 400 });
  }
  if (businessDate < today && !mayEnterHistoricalBusinessDate({ role, name })) {
    throw Object.assign(new Error('Für diesen Benutzer ist keine Nacherfassung freigegeben.'), { statusCode: 403 });
  }
  return businessDate;
}

module.exports = {
  HISTORICAL_ENTRY_ROLES,
  HISTORICAL_ENTRY_USERS,
  berlinBusinessDate,
  mayEnterHistoricalBusinessDate,
  resolveAllowedBusinessDate,
};
