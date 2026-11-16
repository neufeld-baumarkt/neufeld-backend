const test = require('node:test');
const assert = require('node:assert/strict');
const { existsSync, readFileSync } = require('node:fs');
const { join } = require('node:path');

const route = readFileSync(join(__dirname, 'controlling.js'), 'utf8');
const bundledMigration = join(__dirname, '..', 'migrations', '047_alex2_match_engine_ist_integrity.sql');
const saveroomMigration = join(__dirname, '..', '..', 'database', 'migrations', '047_alex2_match_engine_ist_integrity.sql');
const migration = readFileSync(existsSync(bundledMigration) ? bundledMigration : saveroomMigration, 'utf8');

test('alle Lesepfade zeigen IST nur aus dem kanonischen bestaetigten Match-Beleg', () => {
  const helperStart = route.indexOf('function canonicalIstMatchJoin');
  const helperEnd = route.indexOf('function getDaysInMonth', helperStart);
  const helper = route.slice(helperStart, helperEnd);

  assert.ok(helperStart >= 0 && helperEnd > helperStart);
  assert.match(helper, /transfer_match\.status='bestaetigt'/);
  assert.match(helper, /transfer_match\.differenz=0/);
  assert.match(helper, /event_type='EXTERN_BESTAETIGT'/);
  assert.equal((route.match(/canonicalIstMatchJoin\('pd'\)/g) || []).length, 3);
  assert.doesNotMatch(route, /match_item\.betrag\s*=\s*pd\.ist_betrag/);
});

test('alte manuelle Schreibpfade koennen IST nicht mehr setzen', () => {
  assert.match(route, /IST Match-Engine kann nicht manuell erfasst werden/);
  assert.match(route, /IST Match-Engine kann nicht manuell geaendert werden|IST Match-Engine kann nicht manuell geändert werden/);

  const createStart = route.indexOf("router.post('/payment-days'");
  const createEnd = route.indexOf("router.put('/payment-days/:id'", createStart);
  const updateEnd = route.indexOf("router.patch('/payment-days/:id/review'", createEnd);
  assert.doesNotMatch(route.slice(createStart, createEnd), /ist_betrag = EXCLUDED\.ist_betrag/);
  assert.doesNotMatch(route.slice(createEnd, updateEnd), /ist_betrag\s*=\s*\$\d/);
});

test('Migration bereinigt nur Kreditkarte 09 bis 14 November und installiert DB-Schutz', () => {
  assert.match(migration, /payment_type\.code = 'KREDITKARTE'/);
  assert.match(migration, /BETWEEN DATE '2026-11-09' AND DATE '2026-11-14'/);
  assert.match(migration, /match\.status = 'bestaetigt'/);
  assert.match(migration, /event_type[\s\S]*'RUECKGESETZT'/);
  assert.match(migration, /CREATE TRIGGER payment_days_match_engine_ist_guard/);
  assert.match(migration, /item\.betrag = NEW\.ist_betrag/);
  assert.doesNotMatch(migration, /DELETE FROM|TRUNCATE/);
});
