const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const { rebuildAlexModule2PaymentDays } = require('./alexModule2');

function migrationSql() {
  const candidates = [
    join(__dirname, '..', 'migrations', '049_alex2_tabula_rasa_pre_october.sql'),
    join(__dirname, '..', '..', 'database', 'migrations', '049_alex2_tabula_rasa_pre_october.sql'),
  ];
  for (const file of candidates) {
    try { return readFileSync(file, 'utf8'); } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
  }
  throw new Error('Migration 049 was not found.');
}

function sourceNeutralGuardSql() {
  const candidates = [
    join(__dirname, '..', 'migrations', '060_restore_source_neutral_alex2_guard.sql'),
    join(__dirname, '..', '..', 'database', 'migrations', '060_restore_source_neutral_alex2_guard.sql'),
  ];
  for (const file of candidates) {
    try { return readFileSync(file, 'utf8'); } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
  }
  throw new Error('Migration 060 was not found.');
}

async function runAlexModule2StartupMaintenance(pool, options = {}) {
  await pool.query(options.sql || migrationSql());
  await pool.query(options.sourceNeutralGuardSql || sourceNeutralGuardSql());
  const rebuild = options.rebuild || rebuildAlexModule2PaymentDays;
  const rebuilt = await rebuild(pool);
  const verification = await pool.query(
    `SELECT
       EXISTS (
         SELECT 1 FROM pg_trigger
         WHERE tgrelid='controlling.payment_days'::regclass
           AND tgname='payment_days_qlik_fact_guard' AND tgenabled <> 'D'
       ) AS payment_day_qlik_guard,
       EXISTS (
         SELECT 1 FROM pg_trigger
         WHERE tgrelid='controlling.payment_transfers'::regclass
           AND tgname='payment_transfers_provider_guard' AND tgenabled <> 'D'
       ) AS transfer_provider_guard,
       (SELECT count(*)::integer
        FROM controlling.payment_transfers transfer
        JOIN controlling.payment_types payment_type ON payment_type.id = transfer.payment_type_id
        WHERE transfer.deleted_at IS NULL
          AND ((payment_type.code = 'EC_CASH' AND transfer.provider <> 'TELECASH')
            OR (payment_type.code = 'KREDITKARTE' AND transfer.provider <> 'FIRSTDATA'))) AS provider_mismatches`
  );
  const result = verification.rows[0];
  if (result.payment_day_qlik_guard !== true
      || result.transfer_provider_guard !== true
      || Number(result.provider_mismatches) !== 0) {
    throw new Error(`Alex-2 verification failed: ${JSON.stringify(result)}`);
  }
  return { rebuilt, verification: result };
}

module.exports = { runAlexModule2StartupMaintenance };
