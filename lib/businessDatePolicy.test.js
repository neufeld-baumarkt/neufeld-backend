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

test('alle anderen Rollen werden bei Nacherfassung serverseitig abgewiesen', () => {
  for (const role of ['Filiale', 'Geschäftsführer', 'Manager-1']) {
    assert.throws(
      () => resolveAllowedBusinessDate({ requestedDate: '2026-10-24', role, today: '2026-10-25' }),
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
