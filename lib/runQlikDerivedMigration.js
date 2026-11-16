const fs = require('fs');
const path = require('path');
const { synchronizeQlikDailyRevenue } = require('./alexModule1');
const {
  backfillHistoricalQlikPaymentDays,
  deleteOctoberCreditCardTransfers,
} = require('./alexModule2');

const migrationPaths = [
  '031_alex_module_2_v2.sql',
  '032_qlik_derived_registry.sql',
  '033_qlik_core_derivation_rules.sql',
  '034_alex_module_2_qlik_adapter.sql',
  '035_alex_module_2_classic_transfers.sql',
  '036_qlik_alex1_daily_revenue.sql',
  '037_alex1_qlik_booking_adapter.sql',
  '038_alex_module_2_read_model_indexes.sql',
  '039_alex2_credit_card_review_reset.sql',
  '041_alex2_positive_sales_read_model.sql',
  '042_alex2_historical_backfill_and_transfer_cleanup.sql',
  '043_alex2_exact_difference_carries.sql',
  '044_alex2_unified_transfer_providers.sql',
  '045_alex2_component_reviews.sql',
  '046_alex2_transfer_amount_corrections.sql',
  '047_alex2_match_engine_ist_integrity.sql',
  '048_alex2_delete_orphan_transfer.sql',
].map((fileName) => path.join(__dirname, '..', 'migrations', fileName));

async function backfillAlexModule1(pool) {
  if (typeof pool.connect !== 'function') return [];
  const dates = await pool.query(`
    SELECT DISTINCT to_char(report_date, 'YYYY-MM-DD') AS business_date
    FROM controlling.qlik_import_runs
    WHERE status='IMPORTED' AND report_date >= DATE '2026-06-01'
    ORDER BY business_date
  `);
  const results = [];
  for (const row of dates.rows) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        'SELECT * FROM controlling.materialize_qlik_derived_daily_values($1::date)',
        [row.business_date],
      );
      await client.query(
        'SELECT * FROM controlling.materialize_qlik_daily_aggregate_values($1::date)',
        [row.business_date],
      );
      const result = await synchronizeQlikDailyRevenue(client, { businessDate: row.business_date });
      await client.query('COMMIT');
      results.push({ businessDate: row.business_date, status: result.status, synchronized: result.synchronized });
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }
  return results;
}

async function runQlikDerivedMigration(pool) {
  for (const migrationPath of migrationPaths) {
    await pool.query(fs.readFileSync(migrationPath, 'utf8'));
  }

  const verification = await pool.query(`
    SELECT
      to_regclass('controlling.qlik_derived_daily_values') IS NOT NULL AS derived_values_ready,
      to_regclass('controlling.qlik_metric_consumer_bindings') IS NOT NULL AS bindings_ready,
      EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'controlling'
          AND table_name = 'payment_days'
          AND column_name = 'qlik_derived_value_id'
      ) AS adapter_ready
      , NOT EXISTS (
        SELECT 1 FROM controlling.payment_transfers
        WHERE provider='CLASSIC' AND deleted_at IS NULL
      ) AS unified_transfers_ready
      , to_regclass('controlling.qlik_derived_daily_aggregates') IS NOT NULL AS aggregates_ready
      , EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='cashflow'
          AND table_name='buchungen'
          AND column_name='qlik_aggregate_value_id'
      ) AS alex1_adapter_ready
      , EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'controlling'
          AND tablename = 'payment_days'
          AND indexname = 'idx_payment_days_module2_positive_sales'
      ) AS alex2_positive_sales_ready
      , EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='controlling'
          AND table_name='payment_transfers'
          AND column_name='deleted_at'
      ) AS alex2_cleanup_ready
      , to_regclass('controlling.payment_transfer_difference_carries') IS NOT NULL
        AS alex2_difference_carries_ready
      , to_regclass('controlling.payment_day_component_reviews') IS NOT NULL
        AS alex2_component_reviews_ready
      , to_regclass('controlling.payment_day_component_review_events') IS NOT NULL
        AS alex2_component_review_events_ready
  `);

  const state = verification.rows[0] || {};
  if (!state.derived_values_ready || !state.bindings_ready || !state.adapter_ready
      || !state.unified_transfers_ready || !state.aggregates_ready || !state.alex1_adapter_ready
      || !state.alex2_positive_sales_ready || !state.alex2_cleanup_ready
      || !state.alex2_difference_carries_ready || !state.alex2_component_reviews_ready
      || !state.alex2_component_review_events_ready) {
    throw new Error('Qlik derived-value schema verification failed');
  }
  const backfill = await backfillAlexModule1(pool);
  const synchronized = backfill.reduce((sum, row) => sum + Number(row.synchronized || 0), 0);
  const alex2Backfill = await backfillHistoricalQlikPaymentDays(pool);
  const transferCleanup = await deleteOctoberCreditCardTransfers(pool);
  console.log(`✅ Veredelte Qlik-Werte sowie Alex-1/2-Adapter sind bereit (${synchronized} Alex-1-Buchungen, ${alex2Backfill.synchronized} Alex-2-Tageswerte synchronisiert; ${transferCleanup.deletedTransfers} Sammelüberweisungen bereinigt)`);
}

module.exports = { backfillAlexModule1, migrationPaths, runQlikDerivedMigration };
