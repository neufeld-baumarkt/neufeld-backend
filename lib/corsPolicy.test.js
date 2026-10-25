const test = require('node:test');
const assert = require('node:assert/strict');

const { ALLOWED_CORS_HEADERS, CASH_BRANCH_HEADER, CASH_BUSINESS_DATE_HEADER } = require('./corsPolicy');

test('erlaubt die temporäre Filialzuordnung im Browser-Preflight', () => {
  assert.equal(CASH_BRANCH_HEADER, 'x-cash-branch');
  assert.ok(ALLOWED_CORS_HEADERS.includes(CASH_BRANCH_HEADER));
});

test('erlaubt das ausgewählte Geschäftsdatum im Browser-Preflight', () => {
  assert.equal(CASH_BUSINESS_DATE_HEADER, 'x-cash-business-date');
  assert.ok(ALLOWED_CORS_HEADERS.includes(CASH_BUSINESS_DATE_HEADER));
});
