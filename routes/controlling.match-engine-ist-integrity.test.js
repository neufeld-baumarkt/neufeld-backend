const test = require('node:test');
const assert = require('node:assert/strict');
const { existsSync, readFileSync } = require('node:fs');
const { join } = require('node:path');

const route = readFileSync(join(__dirname, 'controlling.js'), 'utf8');
const bundledMigration = join(__dirname, '..', 'migrations', '047_alex2_match_engine_ist_integrity.sql');
const saveroomMigration = join(__dirname, '..', '..', 'database', 'migrations', '047_alex2_match_engine_ist_integrity.sql');
const migration = readFileSync(existsSync(bundledMigration) ? bundledMigration : saveroomMigration, 'utf8');

test('klassische Lesepfade zeigen IST nur aus einem bestaetigten exakten Match', () => {
  const matchGuard = /CASE WHEN EXISTS \([\s\S]*?transfer_match\.status = 'bestaetigt'[\s\S]*?match_item\.betrag = pd\.ist_betrag[\s\S]*?THEN pd\.ist_betrag ELSE NULL END AS ist_betrag/g;
  assert.equal((route.match(matchGuard) || []).length, 2);
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
