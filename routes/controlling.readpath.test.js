const test = require('node:test');
const assert = require('node:assert/strict');
const { existsSync } = require('node:fs');
const { readFile } = require('node:fs/promises');
const path = require('node:path');

const sourcePath = path.join(__dirname, 'controlling.js');
const bundledCanonicalReadMigrationPath = path.join(
  __dirname,
  '..',
  'migrations',
  '051_alex2_canonical_ist_read_path.sql'
);
const saveroomCanonicalReadMigrationPath = path.join(
  __dirname,
  '..',
  '..',
  'database',
  'migrations',
  '051_alex2_canonical_ist_read_path.sql'
);
const canonicalReadMigrationPath = existsSync(bundledCanonicalReadMigrationPath)
  ? bundledCanonicalReadMigrationPath
  : saveroomCanonicalReadMigrationPath;

test('Alex Modul 2 synchronisiert Qlik nicht mehr in den GET-Lesepfaden', async () => {
  const source = await readFile(sourcePath, 'utf8');
  const classicStart = source.indexOf("router.get('/ec-cash/month'");
  const classicEnd = source.indexOf("router.get('/payment-days'", classicStart);
  const overviewStart = source.indexOf("router.get('/v2/overview'");
  const overviewEnd = source.indexOf("router.patch('/v2/reviews'", overviewStart);

  assert.ok(classicStart >= 0 && classicEnd > classicStart);
  assert.ok(overviewStart >= 0 && overviewEnd > overviewStart);
  assert.doesNotMatch(source.slice(classicStart, classicEnd), /synchronizeQlikPaymentDays/);
  assert.doesNotMatch(source.slice(overviewStart, overviewEnd), /synchronizeQlikPaymentDays/);
  assert.match(source.slice(overviewStart, overviewEnd), /sync: \{ mode: 'QLIK_IMPORT' \}/);
});

test('Klassik und V2 leiten IST aus demselben bestätigten Match-Beleg ab', async () => {
  const source = await readFile(sourcePath, 'utf8');
  const classicStart = source.indexOf("router.get('/ec-cash/month'");
  const classicEnd = source.indexOf("router.get('/payment-days'", classicStart);
  const paymentDaysEnd = source.indexOf("router.patch('/payment-days/:id/review'", classicEnd);
  const overviewStart = source.indexOf("router.get('/v2/overview'");
  const overviewEnd = source.indexOf("router.patch('/v2/component-reviews/:id'", overviewStart);
  const canonicalJoinStart = source.indexOf('function canonicalIstMatchJoin');
  const canonicalJoinEnd = source.indexOf('function getDaysInMonth', canonicalJoinStart);
  const canonicalJoin = source.slice(canonicalJoinStart, canonicalJoinEnd);

  assert.ok(canonicalJoinStart >= 0 && canonicalJoinEnd > canonicalJoinStart);
  assert.match(canonicalJoin, /payment_transfer_match_items/);
  assert.match(canonicalJoin, /transfer_match\.status='bestaetigt'/);
  assert.match(canonicalJoin, /transfer_match\.differenz=0/);
  assert.match(canonicalJoin, /event_type='EXTERN_BESTAETIGT'/);

  for (const readPath of [
    source.slice(classicStart, classicEnd),
    source.slice(classicEnd, paymentDaysEnd),
    source.slice(overviewStart, overviewEnd),
  ]) {
    assert.match(readPath, /canonicalIstMatchJoin\('pd'\)/);
    assert.match(readPath, /canonical_ist\.ist_betrag/);
    assert.doesNotMatch(readPath, /match_item\.betrag\s*=\s*pd\.ist_betrag/);
  }

  assert.match(source.slice(overviewStart, overviewEnd), /canonical_ist\.ist_betrag IS NOT NULL/);
  assert.match(source.slice(overviewStart, overviewEnd), /effectiveComparisonStatus/);
  assert.match(source.slice(overviewStart, overviewEnd), /effectiveReviewStatus/);
});

test('Klassik und V2 leiten SOLL aus demselben Qlik-Centwert ab', async () => {
  const source = await readFile(sourcePath, 'utf8');
  const classicStart = source.indexOf("router.get('/ec-cash/month'");
  const classicEnd = source.indexOf("router.get('/payment-days'", classicStart);
  const paymentDaysEnd = source.indexOf("router.patch('/payment-days/:id/review'", classicEnd);
  const overviewStart = source.indexOf("router.get('/v2/overview'");
  const overviewEnd = source.indexOf("router.patch('/v2/component-reviews/:id'", overviewStart);

  for (const classicReadPath of [
    source.slice(classicStart, classicEnd),
    source.slice(classicEnd, paymentDaysEnd),
  ]) {
    assert.match(classicReadPath, /pd\.qlik_amount_cents::numeric \/ 100\.0\) AS soll_betrag/);
    assert.doesNotMatch(classicReadPath, /\n\s*pd\.soll_betrag,/);
  }

  assert.match(source.slice(overviewStart, overviewEnd), /pd\.qlik_amount_cents/);
});

test('Der kanonische IST-Lesepfad ist in der Datenbank indiziert', async () => {
  const migration = await readFile(canonicalReadMigrationPath, 'utf8');

  assert.match(migration, /payment_transfer_match_items \(payment_day_id, match_id\)/);
  assert.match(migration, /INCLUDE \(betrag\)/);
  assert.match(migration, /payment_review_events \(payment_day_id, created_at DESC\)/);
  assert.match(migration, /WHERE event_type = 'EXTERN_BESTAETIGT'/);
});
