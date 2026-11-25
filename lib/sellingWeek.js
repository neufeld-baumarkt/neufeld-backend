const berlinDate = new Intl.DateTimeFormat('sv-SE', { timeZone: 'Europe/Berlin', dateStyle: 'short' });
const iso = date => date.toISOString().slice(0, 10);
const addDays = (date, days) => new Date(date.getTime() + days * 86400000);
const mondayOf = date => addDays(date, -((date.getUTCDay() + 6) % 7));

function sellingWeek(today, requested) {
  if (requested !== undefined && (typeof requested !== 'string' || !/^[1-9]\d{3}-\d{2}-\d{2}$/.test(requested))) return null;
  const current = mondayOf(new Date(`${today}T00:00:00Z`));
  const monday = requested === undefined ? current : new Date(`${requested}T00:00:00Z`);
  if (requested !== undefined && (!Number.isFinite(+monday) || iso(monday) !== requested || monday.getUTCDay() !== 1)) return null;
  const year = addDays(monday, 3).getUTCFullYear();
  const first = mondayOf(new Date(Date.UTC(year, 0, 4)));
  return {
    start: iso(monday), year, week: 1 + Math.round((monday - first) / 604800000),
    previous: iso(addDays(monday, -7)), next: iso(addDays(monday, 7)),
    days: Array.from({ length: 6 }, (_, index) => iso(addDays(monday, index))),
  };
}

module.exports = { berlinDate, sellingWeek };
