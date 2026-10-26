const test = require('node:test');
const assert = require('node:assert/strict');
const { runCashClosingMigration } = require('./runCashClosingMigration');

test('führt die idempotente und unveränderbare Abschlussmigration aus', async () => {
  const calls = [];
  await runCashClosingMigration({ query: async (sql) => calls.push(sql) });
  assert.equal(calls.length, 7);
  assert.match(calls[0], /ADD COLUMN IF NOT EXISTS business_date/);
  assert.match(calls[0], /ux_cash_drawer_closings_opening/);
  assert.match(calls[0], /cash_drawer_closings_immutable/);
  assert.match(calls[1], /closed_by_pin_id/);
  assert.match(calls[1], /corrects_closing_id/);
  assert.match(calls[1], /ux_cash_drawer_closings_corrects/);
  assert.doesNotMatch(calls[1], /UPDATE\s+core\.cash_drawer_closings/i);
  assert.match(calls[2], /CREATE TABLE IF NOT EXISTS core\.cash_safe_counts/);
  assert.match(calls[2], /cash_safe_counts_immutable/);
  assert.match(calls[3], /fixed_drawer_cents/);
  assert.match(calls[3], /previous_evening_count_id/);
  assert.match(calls[4], /lifecycle_status/);
  assert.match(calls[4], /ux_cash_drawer_opening_active_slot/);
  assert.match(calls[5], /cash_business_days_finalized_immutable/);
  assert.match(calls[5], /ux_cash_safe_counts_slot/);
  assert.match(calls[6], /CREATE TABLE IF NOT EXISTS core\.cash_deposit_receipts/);
  assert.match(calls[6], /cash_deposit_receipts_immutable/);
  assert.match(calls[6], /UNIQUE \(business_day_id\)/);
});
