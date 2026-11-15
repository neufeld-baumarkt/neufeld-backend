const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const source = fs.readFileSync(path.join(__dirname, 'controlling.js'), 'utf8');

test('V2 liest Komponenten und Fortschritt ausschließlich aus dem Backend', () => {
  assert.match(source, /payment_day_component_reviews component/);
  assert.match(source, /componentProgress: \{/);
  assert.match(source, /required: requiredComponents\.length/);
});

test('Einzelbestätigung wird auditiert und der Filialstatus serverseitig berechnet', () => {
  const start = source.indexOf("router.patch('/v2/component-reviews/:id'");
  const end = source.indexOf("router.patch('/v2/reviews'", start);
  const route = source.slice(start, end);
  assert.ok(start >= 0 && end > start);
  assert.match(route, /payment_day_component_review_events/);
  assert.match(route, /amount_cents>0 AND review_status='BESTAETIGT'/);
  assert.match(route, /const complete = Number\(progress\.required\) > 0/);
  assert.match(route, /Nullwerte gelten als „kein Umsatz“/);
  assert.doesNotMatch(route, /req\.body.*amount/i);
});
