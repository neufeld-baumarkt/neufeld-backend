const fs = require('fs');
const path = require('path');

const migrationPaths = [
  '031_alex_module_2_v2.sql',
  '032_qlik_derived_registry.sql',
  '033_qlik_core_derivation_rules.sql',
  '034_alex_module_2_qlik_adapter.sql',
  '035_alex_module_2_classic_transfers.sql',
].map((fileName) => path.join(__dirname, '..', 'migrations', fileName));

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
  `);

  const state = verification.rows[0] || {};
  if (!state.derived_values_ready || !state.bindings_ready || !state.adapter_ready || !state.classic_transfers_ready) {
    throw new Error('Qlik derived-value schema verification failed');
  }
  console.log('✅ Veredelte Qlik-Werte und Alex-2-Adapter sind bereit');
}

module.exports = { migrationPaths, runQlikDerivedMigration };
