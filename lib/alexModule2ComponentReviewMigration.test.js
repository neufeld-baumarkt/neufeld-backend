const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const migration = fs.readFileSync(path.join(
  __dirname, '..', 'migrations', '045_alex2_component_reviews.sql'
), 'utf8');

test('speichert Kartenarten einzeln und protokolliert Bestätigungen unveränderlich', () => {
  assert.match(migration, /CREATE TABLE IF NOT EXISTS controlling\.payment_day_component_reviews/);
  assert.match(migration, /'EC_CASH', 'AMEX', 'MAESTRO', 'MASTERCARD', 'VISA'/);
  assert.match(migration, /payment_day_component_reviews_unique UNIQUE \(payment_day_id, component_code\)/);
  assert.match(migration, /CREATE TABLE IF NOT EXISTS controlling\.payment_day_component_review_events/);
  assert.match(migration, /payment_day_component_review_events_append_only/);
});

test('übernimmt bestehende Qlik-Komponenten ohne künstliche Summenaufteilung', () => {
  assert.match(migration, /payment_type_original=component\.original_type/);
  assert.match(migration, /COALESCE\(abs\(sum\(raw\.amount_cents\)\), 0\)/);
  assert.doesNotMatch(migration, /0\.07|0\.43|SYNTHETIC/);
});
