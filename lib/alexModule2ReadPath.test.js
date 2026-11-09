const test = require('node:test');
const assert = require('node:assert/strict');
const { readFile } = require('node:fs/promises');
const path = require('node:path');

test('Alex Modul 2 synchronisiert beim Import und nicht in GET-Lesepfaden', async () => {
  const routeSource = await readFile(path.join(__dirname, '..', 'routes', 'controlling.js'), 'utf8');
  const importSource = await readFile(path.join(__dirname, 'qlikImport.js'), 'utf8');
  const migrationSource = await readFile(path.join(__dirname, '..', 'migrations', '038_alex_module_2_read_model_indexes.sql'), 'utf8');
  const classicStart = routeSource.indexOf("router.get('/ec-cash/month'");
  const classicEnd = routeSource.indexOf("router.get('/payment-days'", classicStart);
  const overviewStart = routeSource.indexOf("router.get('/v2/overview'");
  const overviewEnd = routeSource.indexOf("router.patch('/v2/reviews'", overviewStart);

  assert.doesNotMatch(routeSource.slice(classicStart, classicEnd), /synchronizeQlikPaymentDays/);
  assert.doesNotMatch(routeSource.slice(overviewStart, overviewEnd), /synchronizeQlikPaymentDays/);
  assert.match(importSource, /synchronizeAlexModule2/);
  assert.match(migrationSource, /idx_payment_days_module2_month_read/);
  assert.match(migrationSource, /idx_payment_transfers_module2_month_read/);
});
