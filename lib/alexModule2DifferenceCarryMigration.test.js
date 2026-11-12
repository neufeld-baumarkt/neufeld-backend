const assert = require('node:assert/strict');
const test = require('node:test');
const { readFile } = require('node:fs/promises');
const path = require('node:path');

test('Differenz-Ausgleiche sind eindeutig, positiv und vollständig auditiert', async () => {
  const migration = await readFile(
    path.join(__dirname, '..', 'migrations', '043_alex2_exact_difference_carries.sql'),
    'utf8',
  );

  assert.match(migration, /^BEGIN;/);
  assert.match(migration, /payment_transfer_difference_carries/);
  assert.match(migration, /source_match_id uuid NOT NULL UNIQUE/);
  assert.match(migration, /amount_cents bigint NOT NULL CHECK \(amount_cents > 0\)/);
  assert.match(migration, /settlement_match_id uuid UNIQUE/);
  assert.match(migration, /AUSGLEICH_AUSSTEHEND/);
  assert.match(migration, /FEHLBETRAG_GEMERKT/);
  assert.match(migration, /AUSGLEICH_BESTAETIGT/);
  assert.match(migration, /COMMIT;\s*$/);
});
