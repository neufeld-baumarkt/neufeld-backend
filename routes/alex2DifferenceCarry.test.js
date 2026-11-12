const assert = require('node:assert/strict');
const test = require('node:test');
const { readFileSync } = require('node:fs');

const source = readFileSync(__dirname + '/controlling.js', 'utf8');

test('Transferliste schlägt nur den letzten exakten offenen Fehlbetrag derselben Zahlungsstrecke vor', () => {
  const start = source.indexOf("router.get('/v2/transfers'");
  const end = source.indexOf("router.post('/v2/transfers'", start);
  const route = source.slice(start, end);

  assert.match(route, /carry\.status='OFFEN'/);
  assert.match(route, /round\(m\.differenz\*100\)::bigint=carry\.amount_cents/);
  assert.match(route, /m\.differenz>0/);
  assert.match(route, /source_transfer\.provider=t\.provider/);
  assert.match(route, /source_transfer\.payment_type_id=t\.payment_type_id/);
  assert.match(route, /source_transfer\.buchungsdatum<=t\.buchungsdatum/);
  assert.match(route, /ORDER BY source_transfer\.buchungsdatum DESC/);
  assert.match(route, /carrySuggestion/);
});

test('negative Bestätigung merkt den Fehlbetrag und hält die betroffenen Tage offen', () => {
  const start = source.indexOf("router.post('/v2/transfers/:id/decision'");
  const end = source.indexOf("router.get('/v2/mappings'", start);
  const route = source.slice(start, end);

  assert.match(route, /Number\(current\.difference_cents\) < 0/);
  assert.match(route, /AUSGLEICH_AUSSTEHEND/);
  assert.match(route, /payment_transfer_difference_carries/);
  assert.match(route, /FEHLBETRAG_GEMERKT/);
});

test('Ausgleich wird erneut gesperrt, centgenau validiert und beidseitig auditiert', () => {
  const start = source.indexOf("router.post('/v2/transfers/:id/decision'");
  const end = source.indexOf("router.get('/v2/mappings'", start);
  const route = source.slice(start, end);

  assert.match(route, /carry\.id=\$1 AND carry\.status='OFFEN'/);
  assert.match(route, /FOR UPDATE OF carry/);
  assert.match(route, /isExactDifferenceCarry/);
  assert.match(route, /Der Fehlbetrag passt nicht exakt/);
  assert.match(route, /FEHLBETRAG_AUSGEGLICHEN/);
  assert.match(route, /AUSGLEICH_BESTAETIGT/);
  assert.match(route, /carrySettled: Boolean\(carryToSettle\)/);
});
