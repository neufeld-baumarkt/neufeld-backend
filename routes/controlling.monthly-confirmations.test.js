const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const source = fs.readFileSync(path.join(__dirname, 'controlling.js'), 'utf8');

test('Monatsübersicht trennt menschliche Bestätigung vom kanonischen Transferbeleg', () => {
  const start = source.indexOf("router.get('/v2/overview'");
  const end = source.indexOf("router.patch('/v2/component-reviews/:id'", start);
  const route = source.slice(start, end);
  assert.ok(start >= 0 && end > start);
  assert.match(route, /review_event\.event_type='BESTAETIGT'/);
  assert.match(route, /canonical_ist\.ist_betrag IS NOT NULL/);
  assert.match(route, /canonicalIstMatchJoin\('pd'\)/);
  assert.match(route, /confirmationStatuses/);
});

test('Aggregierte Bestätigung gilt nur für alle umsatzrelevanten Filialwerte', () => {
  const start = source.indexOf('const relevantBranches = item.branches.filter');
  const end = source.indexOf('const summary =', start);
  const aggregation = source.slice(start, end);
  assert.ok(start >= 0 && end > start);
  assert.match(aggregation, /Number\(branch\.qlikAmountCents\) > 0/);
  assert.match(aggregation, /relevantBranches\.every/);
  assert.match(aggregation, /confirmationStatuses\.push\('BESTAETIGT'\)/);
  assert.match(aggregation, /confirmationStatuses\.push\('EXTERN_BESTAETIGT'\)/);
});
