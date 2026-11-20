const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const migration = fs.readFileSync(
  path.join(__dirname, '..', 'migrations', '047_alex2_match_engine_ist_integrity.sql'),
  'utf8',
);

test('IST-Einmalbereinigung wird nach installiertem Schutz-Trigger nicht wiederholt', () => {
  const repeatGuards = migration.match(/tgname = 'payment_days_match_engine_ist_guard'/g) || [];
  assert.equal(repeatGuards.length, 3);
  assert.match(migration, /AND NOT EXISTS \(\s*SELECT 1\s*FROM pg_trigger/);
  assert.match(migration, /CREATE TRIGGER payment_days_match_engine_ist_guard/);
});
