const test = require('node:test');
const assert = require('node:assert/strict');
const { canCorrectCashClosing, canUseCashModule, resolveCashBranch } = require('./cashModuleAccess');

function req(user, branch = '') {
  return { user, get: (name) => name === 'x-cash-branch' ? branch : '' };
}

function db(active = ['Ahaus', 'Münster', 'Telgte', 'Vreden']) {
  return { query: async (_sql, values) => ({ rows: active.includes(values[0]) ? [{ id: active.indexOf(values[0]) + 1, name: values[0] }] : [] }) };
}

test('erlaubt alle vier aktiven Filialen und zentrale Rollen', () => {
  for (const filiale of ['Ahaus', 'Münster', 'Telgte', 'Vreden']) {
    assert.equal(canUseCashModule({ role: 'Mitarbeiter', filiale }), true);
  }
  assert.equal(canUseCashModule({ role: 'Mitarbeiter', filiale: 'Alle' }), false);
  assert.equal(canUseCashModule({ role: 'Mitarbeiter' }), false);
  assert.equal(canUseCashModule({ role: 'Manager-1', filiale: 'Alle' }), true);
});

test('beschränkt nachträgliche Korrekturen auf bestätigte Führungsrollen', () => {
  for (const role of ['Filialleitung','Admin','Supervisor','Geschäftsführer','Manager-1','Manager']) {
    assert.equal(canCorrectCashClosing(role), true);
  }
  assert.equal(canCorrectCashClosing('Mitarbeiter'), false);
});

test('zentrale Alle-Benutzer müssen eine aktive echte Filiale wählen', async () => {
  await assert.rejects(resolveCashBranch(db(), req({ role: 'Admin', filiale: 'Alle' })), /echte Filiale/);
  assert.deepEqual(await resolveCashBranch(db(), req({ role: 'Admin', filiale: 'Alle' }, 'Ahaus')), { id: 1, name: 'Ahaus' });
  await assert.rejects(resolveCashBranch(db(), req({ role: 'Admin', filiale: 'Alle' }, 'Alle')), /echte Filiale/);
});

test('filialgebundene Benutzer dürfen ihre Filiale nicht überschreiben', async () => {
  assert.deepEqual(await resolveCashBranch(db(), req({ role: 'Mitarbeiter', filiale: 'Vreden' })), { id: 4, name: 'Vreden' });
  await assert.rejects(resolveCashBranch(db(), req({ role: 'Mitarbeiter', filiale: 'Vreden' }, 'Ahaus')), /stimmt nicht/);
  assert.deepEqual(await resolveCashBranch(db(), req({ role: 'Mitarbeiter', filiale: 'Ahaus' })), { id: 1, name: 'Ahaus' });
  assert.deepEqual(await resolveCashBranch(db(), req({ role: 'Mitarbeiter', filiale: 'Münster' })), { id: 2, name: 'Münster' });
  assert.deepEqual(await resolveCashBranch(db(), req({ role: 'Mitarbeiter', filiale: 'Telgte' })), { id: 3, name: 'Telgte' });
});
