const test = require('node:test');
const assert = require('node:assert/strict');
const { berlinBusinessDate, resolveAllowedBusinessDate } = require('./businessDatePolicy');

test('Geschäftstag folgt Europe/Berlin', () => {
  assert.equal(berlinBusinessDate(new Date('2026-10-24T22:30:00.000Z')), '2026-10-25');
});

test('Admin und Supervisor dürfen vergangene Geschäftstage nacherfassen', () => {
  assert.equal(resolveAllowedBusinessDate({ requestedDate: '2026-10-23', role: 'Admin', today: '2026-10-25' }), '2026-10-23');
  assert.equal(resolveAllowedBusinessDate({ requestedDate: '2026-10-24', role: 'Supervisor', today: '2026-10-25' }), '2026-10-24');
});

test('die drei namentlich freigegebenen Führungskräfte dürfen passend zu ihrer Rolle nacherfassen', () => {
  for (const user of [
    { name: 'Alex Neufeld', role: 'Geschäftsführer' },
    { name: 'Gökhan Genc', role: 'Manager-1' },
    { name: 'Jörg Menkehorst', role: 'Manager-1' },
  ]) {
    assert.equal(resolveAllowedBusinessDate({ requestedDate: '2026-10-24', ...user, today: '2026-10-25' }), '2026-10-24');
  }
});

test('gleiche Rollen, falsche Namen und falsche Rollenzuordnungen werden abgewiesen', () => {
  for (const user of [
    { name: 'Andere Geschäftsführung', role: 'Geschäftsführer' },
    { name: 'Andere Leitung', role: 'Manager-1' },
    { name: 'Alex Neufeld', role: 'Manager-1' },
    { name: 'Gökhan Genc', role: 'Geschäftsführer' },
    { name: 'Jörg Menkehorst', role: 'Filiale' },
  ]) {
    assert.throws(
      () => resolveAllowedBusinessDate({ requestedDate: '2026-10-24', ...user, today: '2026-10-25' }),
      (error) => error.statusCode === 403,
    );
  }
});

test('heute bleibt für alle Rollen erlaubt und Zukunft ist gesperrt', () => {
  assert.equal(resolveAllowedBusinessDate({ requestedDate: '2026-10-25', role: 'Filiale', today: '2026-10-25' }), '2026-10-25');
  assert.throws(
    () => resolveAllowedBusinessDate({ requestedDate: '2026-10-26', role: 'Admin', today: '2026-10-25' }),
    (error) => error.statusCode === 400,
  );
});
