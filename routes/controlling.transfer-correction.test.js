const test = require('node:test');
const assert = require('node:assert/strict');
const { existsSync, readFileSync } = require('node:fs');
const { join } = require('node:path');

const route = readFileSync(join(__dirname, 'controlling.js'), 'utf8');
const bundledMigration = join(__dirname, '..', 'migrations', '046_alex2_transfer_amount_corrections.sql');
const saveroomMigration = join(__dirname, '..', '..', 'database', 'migrations', '046_alex2_transfer_amount_corrections.sql');
const migration = readFileSync(existsSync(bundledMigration) ? bundledMigration : saveroomMigration, 'utf8');

test('Betragskorrektur ist in Klassik und V2 derselbe auditierte Backend-Prozess', () => {
  assert.match(route, /async function correctTransferAmount/);
  assert.match(route, /router\.patch\('\/v2\/transfers\/:id'/);
  assert.match(route, /source: 'CLASSIC_VIEW'/);
  assert.match(route, /source: 'V2_VIEW'/);
  assert.match(route, /Die Betragskorrektur benötigt eine Begründung/);
  assert.match(route, /'BETRAG_KORRIGIERT'/);
});

test('alte Zuordnungen werden nicht gelöscht, sondern sicher zurückgebaut', () => {
  const correction = route.slice(route.indexOf('async function correctTransferAmount'), route.indexOf("router.put('/transfers/classic'"));
  assert.match(correction, /status='abgelehnt'/);
  assert.match(correction, /status='STORNIERT'/);
  assert.match(correction, /status='OFFEN',settlement_match_id=NULL/);
  assert.match(correction, /'TRANSFER_KORRIGIERT'/);
  assert.doesNotMatch(correction, /DELETE FROM controlling\.payment_transfer_match/);
  assert.doesNotMatch(correction, /DELETE FROM controlling\.payment_(?:transfer|review)_events/);
});

test('Migration erweitert nur Status und unveränderliche Audit-Ereignisse', () => {
  assert.match(migration, /^BEGIN;/);
  assert.match(migration, /'STORNIERT'/);
  assert.match(migration, /'TRANSFER_KORRIGIERT'/);
  assert.match(migration, /'BETRAG_KORRIGIERT'/);
  assert.match(migration, /COMMIT;\s*$/);
  assert.doesNotMatch(migration, /DELETE FROM|TRUNCATE/);
});
