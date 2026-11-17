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

test('Ein-Klick-Bestätigung bestätigt Tageswert und Einzelwerte atomar und schreibt Audit-Ereignisse', async () => {
  const source = await readFile(sourcePath, 'utf8');
  const start = source.indexOf("router.patch('/payment-days/:id/review'");
  const route = source.slice(start, source.indexOf("router.get('/v2/overview'", start));

  assert.ok(start >= 0);
  assert.match(route, /review_status='BESTAETIGT'/);
  assert.match(route, /WHERE id=\$1 AND qlik_amount_cents IS NOT NULL/);
  assert.match(route, /payment_review_events/);
  assert.match(route, /confirmPaymentDayComponents/);
  assert.match(source, /payment_day_component_review_events/);
  assert.match(route, /'CLASSIC_ONE_CLICK'/);
});

test('Sammelüberweisungen speichern die ganzzahlige Benutzer-ID in der korrekt typisierten Spalte', async () => {
  const source = await readFile(sourcePath, 'utf8');
  const classicStart = source.indexOf("router.put('/transfers/classic'");
  const v2Start = source.indexOf("router.post('/v2/transfers'");
  const v2End = source.indexOf("router.patch('/v2/transfers/:id'", v2Start);

  assert.match(source.slice(classicStart, v2Start), /created_by_user_id/);
  assert.match(source.slice(v2Start, v2End), /created_by_user_id/);
  assert.doesNotMatch(source.slice(classicStart, v2End), /provider,created_by\)/);
});

test('IST wird ausschließlich aus menschlich bestätigten Match-Positionen befüllt', async () => {
  const source = await readFile(sourcePath, 'utf8');
  const start = source.indexOf("router.post('/v2/transfers/:id/decision'");
  const route = source.slice(start);

  assert.ok(start >= 0);
  assert.match(route, /SET review_status=\$2::varchar\(30\)/);
  assert.match(route, /ist_betrag=CASE WHEN \$2::varchar\(30\)='EXTERN_BESTAETIGT' THEN mi\.betrag ELSE NULL END/);
  assert.match(route, /SET review_status='EXTERN_BESTAETIGT',ist_betrag=item\.betrag/);
  assert.match(route, /if \(decision\.action === 'confirm'\)/);
});

test('Klassik und V2 starten nach dem Speichern denselben automatischen Match-Vorschlag', async () => {
  const source = await readFile(sourcePath, 'utf8');
  const classicStart = source.indexOf("router.put('/transfers/classic'");
  const v2Start = source.indexOf("router.post('/v2/transfers'");
  const suggestStart = source.indexOf("router.post('/v2/transfers/:id/suggest'");

  assert.ok(classicStart >= 0 && v2Start > classicStart && suggestStart > v2Start);
  assert.match(source.slice(classicStart, v2Start), /tryAutomaticMatchSuggestion\(transfer\.id, req\.user\.id\)/);
  assert.match(source.slice(v2Start, suggestStart), /tryAutomaticMatchSuggestion\(result\.rows\[0\]\.id, req\.user\.id\)/);
  assert.match(source, /async function createAutomaticMatchSuggestion/);
  assert.match(source, /automaticTrigger',true/);
  assert.match(source, /suggestionError/);
});
