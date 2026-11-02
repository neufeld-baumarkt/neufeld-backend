const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const backendRoot = path.join(__dirname, '..');

test('canonical Cashflow route is mounted with invoice and payment workflow', () => {
  const server = fs.readFileSync(path.join(backendRoot, 'server.js'), 'utf8');
  const route = fs.readFileSync(path.join(backendRoot, 'routes', 'cashflow.js'), 'utf8');

  assert.match(server, /require\('\.\/routes\/cashflow'\)/);
  assert.doesNotMatch(server, /cashflowLegacyDiagnostic/);
  assert.match(route, /buchungsgruppe_id/);
  assert.match(route, /randomUUID/);
  assert.match(route, /router\.get\('\/buchungen'/);
  assert.match(route, /router\.post\('\/buchungen'/);
  assert.match(route, /router\.patch\('\/buchungen\/:id'/);
  assert.match(route, /router\.get\('\/buchungen\/ueberweisungen'/);
  assert.match(route, /router\.post\('\/buchungen\/:id\/bezahlen'/);
  assert.match(route, /router\.get\('\/bab-tagesberichte'/);
  assert.match(route, /bab_report\.safeSkimCents/);
});
