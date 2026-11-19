const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const route = fs.readFileSync(path.join(__dirname, 'tageskasse.js'), 'utf8');

test('Tagesberichte erhalten den Verifiziert-Status nur aus einer gespeicherten Belegzuordnung', () => {
  assert.match(route, /EXISTS \(SELECT 1 FROM core\.cash_deposit_receipt_business_days link/);
  assert.match(route, /receiptVerified:row\.receipt_verified === true/);
  assert.match(route, /report\.receiptVerified = row\.receipt_id != null/);
});

test('Detailberichte liefern den unveränderbaren Belegnachweis mit Zeitpunkt und Mitarbeiter', () => {
  assert.match(route, /receipt\.confirmed_by_pin_id/);
  assert.match(route, /receipt\.confirmed_by_name/);
  assert.match(route, /receipt\.confirmed_at/);
  assert.match(route, /verifiedAt:row\.confirmed_at/);
  assert.match(route, /verifiedBy:\{ id:Number\(row\.confirmed_by_pin_id\), name:row\.confirmed_by_name \}/);
});
