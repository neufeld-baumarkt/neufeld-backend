const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const route = fs.readFileSync(path.join(__dirname, 'controlling.js'), 'utf8');

test('Alex 3 reads Qlik, day-close and complete receipt coverage separately', () => {
  assert.match(route, /router\.get\('\/alex3\/month'/);
  assert.match(route, /controlling\.alex3_manual_daily_values/);
  assert.match(route, /controlling\.cash_business_days/);
  assert.match(route, /controlling\.cash_deposit_receipt_closings/);
  assert.match(route, /SUM\(closing\.bank_amount_cents\)::integer/);
  assert.match(route, /confirmed_closing_bank_cents/);
  assert.match(route, /core\.cash_business_days/);
  assert.match(route, /core\.cash_deposit_receipt_business_days/);
  assert.match(route, /final_report->'babReport'->>'bookedCents'/);
  assert.match(route, /businessDays:\s*\[\.\.\.coreBusinessDays\.rows,\s*\.\.\.businessDays\.rows\]/);
  assert.match(route, /controlling\.qlik_derived_daily_values/);
  assert.match(route, /confirmed_closing_count/);
  assert.match(route, /linked_closing_count/);
  assert.match(route, /alex3_qlik_value_confirmations/);
  assert.match(route, /buildAlex3Month/);
});

test('Alex 3 confirms only the latest concrete Qlik value and keeps EZ read-only', () => {
  assert.match(route, /router\.post\('\/alex3\/qlik-values\/:id\/confirm'/);
  assert.match(route, /metric\.metric_code='BAR_AN_BANK_QLIK'/);
  assert.match(route, /newer\.calculated_at,newer\.id/);
  assert.match(route, /ON CONFLICT \(qlik_derived_value_id\) DO NOTHING/);
  assert.match(route, /router\.put\('\/alex3\/manual-values'/);
  assert.match(route, /if \(movementType === 'EZ'\)/);
  assert.match(route, /Number\.isSafeInteger/);
  assert.match(route, /ON CONFLICT \(business_date,filiale,movement_type\)/);
});
