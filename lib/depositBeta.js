const { banknoteTotalCents } = require('./depositReceiptPolicy');
const { sellingWeek } = require('./sellingWeek');

const schedule = { 1: [-3, -2], 3: [-2, -1], 5: [-2, -1] };
const berlin = new Intl.DateTimeFormat('sv-SE', {
  timeZone: 'Europe/Berlin', dateStyle: 'short', timeStyle: 'short',
});

function iso(date) {
  return date.toISOString().slice(0, 10);
}

function depositCycle(now, holidays) {
  const [today, time] = berlin.format(now).split(' ');
  const due = new Date(today + 'T00:00:00Z');
  while (!schedule[due.getUTCDay()]) due.setUTCDate(due.getUTCDate() - 1);
  return cycleForDate(due, today, time, holidays);
}

function cycleForDate(due, today, time, holidays) {
  const dueDate = iso(due);
  const dates = schedule[due.getUTCDay()].map(offset => {
    const date = new Date(due);
    date.setUTCDate(date.getUTCDate() + offset);
    return iso(date);
  }).filter(date => !holidays.has(date));
  return { dueDate, dates, upcoming: today < dueDate, overdue: today > dueDate || (today === dueDate && time >= '12:00') };
}

function depositWeek(now, holidays, start) {
  const [today, time] = berlin.format(now).split(' ');
  const monday = new Date(sellingWeek(today, start).start + 'T00:00:00Z');
  return [0, 2, 4].map(offset => {
    const due = new Date(monday);
    due.setUTCDate(due.getUTCDate() + offset);
    return cycleForDate(due, today, time, holidays);
  });
}

function depositBetaConfig(env) {
  if (env.DEPOSIT_BETA_ENABLED !== '1') return null;
  const ids = [env.DEPOSIT_BETA_ADMIN_ID, env.DEPOSIT_BETA_SUPERVISOR_ID, env.DEPOSIT_BETA_VREDEN_ID];
  const managingDirector = env.DEPOSIT_BETA_MANAGING_DIRECTOR_ID;
  const configuredIds = managingDirector === undefined ? ids : [...ids, managingDirector];
  if (configuredIds.some(id => !/^[1-9]\d*$/.test(id || '') || !Number.isSafeInteger(Number(id))) ||
      new Set(configuredIds).size !== configuredIds.length) {
    throw new Error('Einzahlungsbeta benötigt gültige, unterschiedliche Benutzer-IDs.');
  }
  if (typeof env.DEPOSIT_BETA_HOLIDAYS !== 'string') {
    throw new Error('Für die Einzahlungsbeta muss der Feiertagskalender explizit gesetzt sein.');
  }
  const holidays = env.DEPOSIT_BETA_HOLIDAYS.split(',').map(date => date.trim()).filter(Boolean);
  if (holidays.some(date => !/^\d{4}-\d{2}-\d{2}$/.test(date) ||
    !Number.isFinite(Date.parse(date)) || iso(new Date(date)) !== date)) {
    throw new Error('Einzahlungsbeta benötigt Feiertage als gültige ISO-Kalendertage.');
  }
  const source = env.DEPOSIT_BETA_SOURCE ?? 'controlling';
  if (!['core', 'controlling'].includes(source)) {
    throw new Error('Unbekannte Belegquelle für die Einzahlungsbeta.');
  }
  return { admin: ids[0], supervisor: ids[1], vreden: ids[2], managingDirector, source, holidays: new Set(holidays) };
}

function depositScope(config, user) {
  const id = String(user?.id);
  if ((id === config.admin && user.role === 'Admin') ||
      (id === config.supervisor && user.role === 'Supervisor') ||
      (id === config.managingDirector && user.role === 'Geschäftsführer')) return { branch: null };
  if (id === config.vreden && user.role === 'Filiale' &&
      String(user.filiale).trim().toLowerCase() === 'vreden') return { branch: 'Vreden' };
  return null;
}

function depositStatuses(rows, cycle) {
  const branches = new Map();
  for (const row of rows) {
    if (!branches.has(row.branch)) branches.set(row.branch, new Map());
    if (!row.date) continue;
    const days = branches.get(row.branch);
    const validReceipt = row.verified && row.valid_links &&
      Number(row.receipt_total_cents) === Number(row.linked_total_cents) &&
      Number(row.receipt_total_cents) === banknoteTotalCents(row.banknote_counts);
    const covered = row.final && row.bank_amount_cents != null &&
      (Number(row.bank_amount_cents) === 0 || validReceipt);
    days.set(row.date, (days.get(row.date) ?? true) && Boolean(covered));
  }
  return Array.from(branches, ([branch, days]) => {
    const missingDates = cycle.dates.filter(date => !days.get(date));
    const status = missingDates.length === 0 ? 'green'
      : cycle.upcoming ? 'planned' : cycle.overdue ? 'red' : 'yellow';
    return { branch, missingDates, status };
  });
}

module.exports = { depositBetaConfig, depositCycle, depositWeek, depositScope, depositStatuses };
