const assert = require('node:assert/strict');
const test = require('node:test');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');

const root = join(__dirname, '..');
const route = readFileSync(join(root, 'routes', 'controlling.js'), 'utf8');
const migration = readFileSync(join(root, 'migrations', '035_alex_module_2_classic_transfers.sql'), 'utf8');
const baseMigration = readFileSync(join(root, 'migrations', '031_alex_module_2_v2.sql'), 'utf8');

test('Datenbank erlaubt genau eine klassische Sammelüberweisung je Zahlungsart und Tag', () => {
  assert.match(migration, /'CLASSIC'/);
  assert.match(migration, /UNIQUE INDEX IF NOT EXISTS ux_payment_transfers_classic_day/);
  assert.match(migration, /\(payment_type_id, buchungsdatum\)/);
  assert.match(migration, /WHERE provider = 'CLASSIC'/);
  assert.match(baseMigration, /'FIRSTDATA', 'TELECASH', 'UNBEKANNT', 'CLASSIC'/);
});

test('Backend schreibt klassische Gesamtüberweisungen idempotent und auditierbar', () => {
  assert.match(route, /router\.put\('\/transfers\/classic'/);
  assert.match(route, /ON CONFLICT \(payment_type_id,buchungsdatum\) WHERE provider='CLASSIC'/);
  assert.match(route, /'ERFASST' : 'ANGEPASST'/);
  assert.match(route, /WHERE t\.provider <> 'CLASSIC'/);
});
