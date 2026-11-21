const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const route = fs.readFileSync(path.join(__dirname, 'controlling.js'), 'utf8');

test('Alex 3 reads the source-neutral canonical values, day-close and receipt coverage separately', () => {
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
  assert.match(route, /controlling\.canonical_daily_metric_values/);
  assert.match(route, /value\.selected_source,value\.source_status/);
  assert.match(route, /confirmed_closing_count/);
  assert.match(route, /linked_closing_count/);
  assert.match(route, /alex3_qlik_value_confirmations/);
  assert.match(route, /buildAlex3Month/);
});

test('Alex 3 confirms only the latest concrete Qlik value and keeps EZ read-only', () => {
  assert.match(route, /router\.post\('\/alex3\/qlik-values\/:id\/confirm'/);
  assert.match(route, /metric\.metric_code='BAR_AN_BANK_QLIK'/);
  assert.match(route, /FROM controlling\.canonical_daily_metric_values current_value/);
  assert.match(route, /ON CONFLICT \(qlik_derived_value_id\) DO NOTHING/);
  assert.match(route, /RETURNING qlik_derived_value_id,confirmed_at,confirmed_by_user_id/);
  assert.match(route, /FROM inserted\s+JOIN public\.users reviewer/);
  assert.match(route, /UNION ALL\s+SELECT confirmation\.qlik_derived_value_id/);
  assert.match(route, /WHERE NOT EXISTS \(SELECT 1 FROM inserted\)/);
  assert.match(route, /confirmed:\s*true/);
  assert.match(route, /sollState:\s*'BESTAETIGT'/);
  assert.match(route, /router\.put\('\/alex3\/manual-values'/);
  assert.match(route, /if \(movementType === 'EZ'\)/);
  assert.match(route, /Number\.isSafeInteger/);
  assert.match(route, /ON CONFLICT \(business_date,filiale,movement_type\)/);
});
