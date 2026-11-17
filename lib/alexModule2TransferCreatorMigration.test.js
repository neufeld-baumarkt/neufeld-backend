const test = require('node:test');
const assert = require('node:assert/strict');
const { existsSync } = require('node:fs');
const { readFile } = require('node:fs/promises');
const path = require('node:path');

const bundledMigrationPath = path.join(__dirname, '..', 'migrations', '052_alex2_transfer_creator_user.sql');
const saveroomMigrationPath = path.join(__dirname, '..', '..', 'database', 'migrations', '052_alex2_transfer_creator_user.sql');
const migrationPath = existsSync(bundledMigrationPath) ? bundledMigrationPath : saveroomMigrationPath;

test('Modul-2-Sammelüberweisungen erhalten eine Integer-Referenz auf public.users', async () => {
  const migration = await readFile(migrationPath, 'utf8');

  assert.match(migration, /created_by_user_id integer REFERENCES public\.users\(id\)/);
  assert.match(migration, /idx_payment_transfers_created_by_user/);
  assert.doesNotMatch(migration, /DROP COLUMN|ALTER COLUMN created_by TYPE/);
});
