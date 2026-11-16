const test = require('node:test');
const assert = require('node:assert/strict');
const { existsSync, readFileSync } = require('node:fs');
const { join } = require('node:path');

const route = readFileSync(join(__dirname, 'controlling.js'), 'utf8');
const bundledMigration = join(__dirname, '..', 'migrations', '048_alex2_delete_orphan_transfer.sql');
const saveroomMigration = join(__dirname, '..', '..', 'database', 'migrations', '048_alex2_delete_orphan_transfer.sql');
const migration = readFileSync(existsSync(bundledMigration) ? bundledMigration : saveroomMigration, 'utf8');

test('Sammelueberweisungen werden nur revisionssicher mit Pflichtgrund geloescht', () => {
  const helper = route.slice(route.indexOf('async function deleteUnconfirmedTransfer'), route.indexOf("router.put('/transfers/classic'"));
  assert.match(helper, /Das Löschen benötigt eine Begründung/);
  assert.match(helper, /status='bestaetigt'/);
  assert.match(helper, /deleted_at=now\(\),deleted_by=\$2,deletion_reason=\$3/);
  assert.match(helper, /'GELOESCHT'/);
  assert.doesNotMatch(helper, /DELETE FROM controlling\.payment_transfers/);
  assert.match(route, /router\.delete\('\/v2\/transfers\/:id'/);
});

test('Einmalige Bereinigung ist centgenau, eindeutig und blockiert bestaetigte Matches', () => {
  assert.match(migration, /buchungsdatum = DATE '2026-11-05'/);
  assert.match(migration, /1714786/);
  assert.match(migration, /target_count <> 1/);
  assert.match(migration, /match\.status = 'bestaetigt'/);
  assert.match(migration, /deleted_at = now\(\)/);
  assert.match(migration, /'GELOESCHT'/);
  assert.doesNotMatch(migration, /DELETE FROM|TRUNCATE/);
});
