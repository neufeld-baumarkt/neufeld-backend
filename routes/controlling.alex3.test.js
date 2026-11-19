const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const route = fs.readFileSync(path.join(__dirname, 'controlling.js'), 'utf8');

test('Alex 3 reads manual, automatic cash, linked receipt and Qlik evidence separately', () => {
  assert.match(route, /router\.get\('\/alex3\/month'/);
  assert.match(route, /controlling\.alex3_manual_daily_values/);
  assert.match(route, /controlling\.cash_business_days/);
  assert.match(route, /controlling\.cash_deposit_receipt_closings/);
  assert.match(route, /controlling\.qlik_derived_daily_values/);
  assert.match(route, /buildAlex3Month/);
});

test('Alex 3 manual endpoint blocks Vreden EZ overrides and stores cents', () => {
  assert.match(route, /router\.put\('\/alex3\/manual-values'/);
  assert.match(route, /branch === 'Vreden' && movementType === 'EZ'/);
  assert.match(route, /Number\.isSafeInteger/);
  assert.match(route, /ON CONFLICT \(business_date,filiale,movement_type\)/);
});
