const test = require('node:test');
const assert = require('node:assert/strict');
const { readFile } = require('node:fs/promises');
const path = require('node:path');

const sourcePath = path.join(__dirname, 'controlling.js');

test('klassische Monatsansicht liefert den menschlichen Qlik-Prüfstatus', async () => {
  const source = await readFile(sourcePath, 'utf8');
  const start = source.indexOf("router.get('/ec-cash/month'");
  const end = source.indexOf("router.get('/payment-days'", start);
  const responseBuilder = source.slice(source.indexOf('function buildEcCashMonthResponse'), start);

  assert.ok(start >= 0 && end > start);
  assert.match(source.slice(start, end), /pd\.review_status/);
  assert.match(source.slice(start, end), /reviewer\.name AS reviewed_by/);
  assert.match(responseBuilder, /reviewStatus: row\.review_status \|\| 'OFFEN'/);
});
test('Ein-Klick-Bestätigung ändert genau einen Qlik-Tageswert und schreibt ein Audit-Ereignis', async () => {
  const source = await readFile(sourcePath, 'utf8');
  const start = source.indexOf("router.patch('/payment-days/:id/review'");
  const route = source.slice(start, source.indexOf("router.get('/v2/overview'", start));

  assert.ok(start >= 0);
  assert.match(route, /review_status='BESTAETIGT'/);
  assert.match(route, /WHERE id=\$1 AND qlik_amount_cents IS NOT NULL/);
  assert.match(route, /payment_review_events/);
  assert.match(route, /'CLASSIC_ONE_CLICK'/);
});

test('IST wird ausschließlich aus menschlich bestätigten Match-Positionen befüllt', async () => {
  const source = await readFile(sourcePath, 'utf8');
  const start = source.indexOf("router.post('/v2/transfers/:id/decision'");
  const route = source.slice(start);

  assert.ok(start >= 0);
  assert.match(route, /ist_betrag=CASE WHEN \$2='EXTERN_BESTAETIGT' THEN mi\.betrag ELSE NULL END/);
  assert.match(route, /SET review_status='EXTERN_BESTAETIGT',ist_betrag=item\.betrag/);
  assert.match(route, /if \(decision\.action === 'confirm'\)/);
});
