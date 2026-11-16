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
       (SELECT count(*)::integer FROM controlling.payment_transfers
        WHERE buchungsdatum < DATE '2026-10-01') AS transfers_before_october,
       (SELECT count(*)::integer FROM controlling.payment_days
        WHERE datum < DATE '2026-06-01') AS days_before_qlik,
       (SELECT count(*)::integer FROM controlling.payment_days day
        WHERE day.datum BETWEEN DATE '2026-06-01' AND DATE '2026-09-30'
          AND (day.qlik_amount_cents IS NULL OR day.qlik_import_run_id IS NULL
               OR day.qlik_derived_value_id IS NULL)) AS unproven_rebuilt_days,
       (SELECT count(DISTINCT day.id)::integer
        FROM controlling.payment_days day
        JOIN controlling.payment_transfer_match_items item ON item.payment_day_id=day.id
        JOIN controlling.payment_transfer_matches match ON match.id=item.match_id
        JOIN controlling.payment_transfers transfer ON transfer.id=match.transfer_id
        WHERE day.datum < DATE '2026-10-01'
          AND transfer.buchungsdatum >= DATE '2026-10-01') AS protected_future_days`
  );
  const result = verification.rows[0];
  if (Number(result.transfers_before_october) !== 0
      || Number(result.days_before_qlik) !== 0
      || Number(result.unproven_rebuilt_days) !== 0
      || Number(result.protected_future_days) !== 12) {
    throw new Error(`Alex-2 verification failed: ${JSON.stringify(result)}`);
  }
  return { rebuilt, verification: result };
}

module.exports = { runAlexModule2StartupMaintenance };

