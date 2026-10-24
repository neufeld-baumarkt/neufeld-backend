const test = require('node:test');
const assert = require('node:assert/strict');
const { canUseCashModule, resolveCashBranch } = require('./cashModuleAccess');

function req(user, branch = '') {
  return { user, get: (name) => name === 'x-cash-branch' ? branch : '' };
}

function db(active = ['Vreden', 'Ahaus']) {
  return { query: async (_sql, values) => ({ rows: active.includes(values[0]) ? [{ id: active.indexOf(values[0]) + 1, name: values[0] }] : [] }) };
}

test('erlaubt während DEV nur Vreden und zentrale Rollen', () => {
  assert.equal(canUseCashModule({ role: 'Mitarbeiter', filiale: 'Vreden' }), true);
  assert.equal(canUseCashModule({ role: 'Mitarbeiter', filiale: 'Ahaus' }), false);
  assert.equal(canUseCashModule({ role: 'Manager-1', filiale: 'Alle' }), true);
});

test('zentrale Alle-Benutzer müssen eine aktive echte Filiale wählen', async () => {
  await assert.rejects(resolveCashBranch(db(), req({ role: 'Admin', filiale: 'Alle' })), /echte Filiale/);
  assert.deepEqual(await resolveCashBranch(db(), req({ role: 'Admin', filiale: 'Alle' }, 'Ahaus')), { id: 2, name: 'Ahaus' });
  await assert.rejects(resolveCashBranch(db(), req({ role: 'Admin', filiale: 'Alle' }, 'Alle')), /echte Filiale/);
});

test('filialgebundene Benutzer dürfen ihre Filiale nicht überschreiben', async () => {
  assert.deepEqual(await resolveCashBranch(db(), req({ role: 'Mitarbeiter', filiale: 'Vreden' })), { id: 1, name: 'Vreden' });
  await assert.rejects(resolveCashBranch(db(), req({ role: 'Mitarbeiter', filiale: 'Vreden' }, 'Ahaus')), /stimmt nicht/);
  await assert.rejects(resolveCashBranch(db(), req({ role: 'Mitarbeiter', filiale: 'Ahaus' })), /DEV-Phase/);
});
