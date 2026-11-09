const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const operationPath = path.join(__dirname, '..', 'migrations', '039_alex2_credit_card_review_reset.sql');

test('setzt ausschließlich Kreditkarten-Prüfungen und ihre aktiven Matches kontrolliert zurück', () => {
  const sql = fs.readFileSync(operationPath, 'utf8');

  assert.match(sql, /^BEGIN;/);
  assert.equal((sql.match(/payment_type\.code = 'KREDITKARTE'/g) || []).length, 3);
  assert.match(sql, /module2_maintenance_operations/);
  assert.match(sql, /ON CONFLICT \(operation_key\) DO NOTHING/);
  assert.match(sql, /match\.status IN \('vorgeschlagen', 'bestaetigt', 'vertagt'\)/);
  assert.match(sql, /SET status = 'offen'/);
  assert.match(sql, /SET review_status = 'OFFEN'/);
  assert.match(sql, /reviewed_by = NULL/);
  assert.match(sql, /MATCH_ENGINE_SUBSET_CENTS_V1_CORRECTION/);
  assert.match(sql, /COMMIT;\s*$/);
  assert.doesNotMatch(sql, /payment_type\.code = 'EC_CASH'/);
});
