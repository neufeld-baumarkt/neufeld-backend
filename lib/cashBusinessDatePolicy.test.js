const test = require('node:test');
const assert = require('node:assert/strict');
const { berlinBusinessDate, normalizeCashBusinessDate, resolveCashBusinessDate } = require('./cashBusinessDatePolicy');

const now = new Date('2026-10-25T10:00:00.000Z');
const req = (role, date = '', name = '') => ({ user:{ role, name }, get:(header)=>header==='x-cash-business-date'?date:'' });

test('ermittelt den Berliner Geschäftstag', () => {
  assert.equal(berlinBusinessDate(new Date('2026-10-24T22:30:00.000Z')), '2026-10-25');
});

test('verwendet ohne Auswahl den aktuellen Geschäftstag', () => {
  assert.equal(resolveCashBusinessDate(req('Mitarbeiter'), now), '2026-10-25');
});

test('erlaubt historische Daten für Admin, Supervisor und die drei benannten Führungskräfte', () => {
  assert.equal(resolveCashBusinessDate(req('Admin','2026-10-24'), now), '2026-10-24');
  assert.equal(resolveCashBusinessDate(req('Supervisor','2026-05-23'), now), '2026-05-23');
  for (const [role, name] of [
    ['Geschäftsführer', 'Alex Neufeld'],
    ['Manager-1', 'Gökhan Genc'],
    ['Manager-1', 'Jörg Menkehorst'],
  ]) {
    assert.equal(resolveCashBusinessDate(req(role, '2026-10-24', name), now), '2026-10-24');
  }
});

test('blockiert nicht freigegebene Namen und falsche Rollenzuordnungen', () => {
  for (const [role, name] of [
    ['Geschäftsführer', 'Andere Geschäftsführung'],
    ['Manager-1', 'Andere Leitung'],
    ['Manager-1', 'Alex Neufeld'],
    ['Geschäftsführer', 'Gökhan Genc'],
    ['Filialleitung', 'Jörg Menkehorst'],
  ]) {
    assert.throws(()=>resolveCashBusinessDate(req(role, '2026-10-24', name),now), /keine Nacherfassung/);
  }
});

test('blockiert ungültige und zukünftige Daten', () => {
  assert.throws(()=>resolveCashBusinessDate(req('Admin','2026-02-30'),now), /ungültig/);
  assert.throws(()=>resolveCashBusinessDate(req('Admin','2026-10-26'),now), /zukünftiger/);
});

test('normalisiert PostgreSQL-Datumswerte vor dem Geschäftstagsvergleich', () => {
  const databaseDate = new Date('2026-11-04T00:00:00.000Z');

  assert.equal(normalizeCashBusinessDate(databaseDate), '2026-11-04');
  assert.equal(normalizeCashBusinessDate('2026-11-04T00:00:00.000Z'), '2026-11-04');
  assert.equal(normalizeCashBusinessDate(databaseDate), req('Admin','2026-11-04').get('x-cash-business-date'));
});
