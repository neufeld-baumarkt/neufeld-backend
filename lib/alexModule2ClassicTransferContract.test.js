const assert = require('node:assert/strict');
const test = require('node:test');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');

const root = join(__dirname, '..');
const route = readFileSync(join(root, 'routes', 'controlling.js'), 'utf8');
const migration = readFileSync(join(root, 'migrations', '044_alex2_unified_transfer_providers.sql'), 'utf8');

test('Datenbank führt klassische Sammelüberweisungen konfliktfrei in die echten Anbieter über', () => {
  assert.match(migration, /WHEN payment_type\.code='EC_CASH' THEN 'TELECASH'/);
  assert.match(migration, /WHEN payment_type\.code='KREDITKARTE' THEN 'FIRSTDATA'/);
  assert.match(migration, /DUBLETTE_BEREINIGT/);
  assert.match(migration, /ANBIETER_NORMALISIERT/);
  assert.match(migration, /DROP INDEX IF EXISTS controlling\.ux_payment_transfers_classic_day/);
});

test('Backend schreibt in beiden Ansichten dieselben anbieterspezifischen Gesamtüberweisungen', () => {
  assert.match(route, /router\.put\('\/transfers\/classic'/);
  assert.match(route, /const provider = providerForPaymentType\(paymentType\)/);
  assert.match(route, /transfer\.provider=\$2/);
  assert.match(route, /Bitte in V2 bearbeiten/);
  assert.doesNotMatch(route, /WHERE t\.provider <> 'CLASSIC'/);
});
