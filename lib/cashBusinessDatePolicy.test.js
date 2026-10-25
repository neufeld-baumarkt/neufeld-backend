const test = require('node:test');
const assert = require('node:assert/strict');
const { berlinBusinessDate, resolveCashBusinessDate } = require('./cashBusinessDatePolicy');

const now = new Date('2026-10-25T10:00:00.000Z');
const req = (role, date = '') => ({ user:{ role }, get:(name)=>name==='x-cash-business-date'?date:'' });

test('ermittelt den Berliner Geschäftstag', () => {
  assert.equal(berlinBusinessDate(new Date('2026-10-24T22:30:00.000Z')), '2026-10-25');
});

test('verwendet ohne Auswahl den aktuellen Geschäftstag', () => {
  assert.equal(resolveCashBusinessDate(req('Mitarbeiter'), now), '2026-10-25');
});

test('erlaubt historische Daten nur Admin und Supervisor', () => {
  assert.equal(resolveCashBusinessDate(req('Admin','2026-10-24'), now), '2026-10-24');
  assert.equal(resolveCashBusinessDate(req('Supervisor','2026-05-23'), now), '2026-05-23');
  for (const role of ['Geschäftsführer','Manager','Manager-1','Filialleitung','Mitarbeiter']) {
    assert.throws(()=>resolveCashBusinessDate(req(role,'2026-10-24'),now), /Admin und Supervisor/);
  }
});

test('blockiert ungültige und zukünftige Daten', () => {
  assert.throws(()=>resolveCashBusinessDate(req('Admin','2026-02-30'),now), /ungültig/);
  assert.throws(()=>resolveCashBusinessDate(req('Admin','2026-10-26'),now), /zukünftiger/);
});
