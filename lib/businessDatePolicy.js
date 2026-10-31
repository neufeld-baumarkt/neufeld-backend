const HISTORICAL_ENTRY_ROLES = new Set(['Admin', 'Supervisor']);

function berlinBusinessDate(date = new Date()) {
  return new Intl.DateTimeFormat('sv-SE', { timeZone: 'Europe/Berlin' }).format(date);
}

function resolveAllowedBusinessDate({ requestedDate, role, today = berlinBusinessDate() } = {}) {
  const businessDate = String(requestedDate || today);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(businessDate)) {
    throw Object.assign(new Error('Der Geschäftstag ist ungültig.'), { statusCode: 400 });
  }
  if (businessDate > today) {
    throw Object.assign(new Error('Ein zukünftiger Geschäftstag ist nicht zulässig.'), { statusCode: 400 });
  }
  if (businessDate < today && !HISTORICAL_ENTRY_ROLES.has(role)) {
    throw Object.assign(new Error('Nacherfassungen sind ausschließlich für Admin und Supervisor zulässig.'), { statusCode: 403 });
  }
  return businessDate;
}

module.exports = { HISTORICAL_ENTRY_ROLES, berlinBusinessDate, resolveAllowedBusinessDate };
