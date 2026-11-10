const fs = require('fs');
const path = require('path');
const { synchronizeQlikDailyRevenue } = require('./alexModule1');

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
      , EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'controlling'
          AND tablename = 'payment_transfers'
          AND indexname = 'ux_payment_transfers_classic_day'
      ) AS classic_transfers_ready
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
  `);

  const state = verification.rows[0] || {};
  if (!state.derived_values_ready || !state.bindings_ready || !state.adapter_ready
      || !state.classic_transfers_ready || !state.aggregates_ready || !state.alex1_adapter_ready
      || !state.alex2_positive_sales_ready) {
    throw new Error('Qlik derived-value schema verification failed');
  }
  const backfill = await backfillAlexModule1(pool);
  const synchronized = backfill.reduce((sum, row) => sum + Number(row.synchronized || 0), 0);
  console.log(`✅ Veredelte Qlik-Werte sowie Alex-1/2-Adapter sind bereit (${synchronized} Alex-1-Buchungen synchronisiert)`);
}

module.exports = { backfillAlexModule1, migrationPaths, runQlikDerivedMigration };
