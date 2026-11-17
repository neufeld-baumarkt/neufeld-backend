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

async function runAlexModule2StartupMaintenance(pool, options = {}) {
  await pool.query(options.sql || migrationSql());
  const rebuild = options.rebuild || rebuildAlexModule2PaymentDays;
  const rebuilt = await rebuild(pool);
  const verification = await pool.query(
    `SELECT
       (SELECT count(*)::integer
        FROM controlling.payment_days day
        WHERE day.qlik_amount_cents IS NULL
           OR day.qlik_import_run_id IS NULL
           OR day.qlik_derived_value_id IS NULL) AS unproven_payment_days,
       (SELECT count(*)::integer
        FROM controlling.payment_days day
        WHERE day.soll_betrag IS NULL
           OR round(day.soll_betrag * 100)::bigint <> day.qlik_amount_cents) AS inconsistent_qlik_amounts,
       (SELECT count(*)::integer
        FROM controlling.payment_transfers transfer
        JOIN controlling.payment_types payment_type ON payment_type.id = transfer.payment_type_id
        WHERE transfer.deleted_at IS NULL
          AND ((payment_type.code = 'EC_CASH' AND transfer.provider <> 'TELECASH')
            OR (payment_type.code = 'KREDITKARTE' AND transfer.provider <> 'FIRSTDATA'))) AS provider_mismatches`
  );
  const result = verification.rows[0];
  if (Number(result.unproven_payment_days) !== 0
      || Number(result.inconsistent_qlik_amounts) !== 0
      || Number(result.provider_mismatches) !== 0) {
    throw new Error(`Alex-2 verification failed: ${JSON.stringify(result)}`);
  }
  return { rebuilt, verification: result };
}

module.exports = { runAlexModule2StartupMaintenance };
