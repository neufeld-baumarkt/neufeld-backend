const fs = require('fs');
const path = require('path');

const migrationPath = path.join(__dirname, '..', 'migrations', '030_qlik_import_foundation.sql');

async function runQlikImportMigration(pool) {
  const sql = fs.readFileSync(migrationPath, 'utf8');
  await pool.query(sql);

  const verification = await pool.query(`
    SELECT
      to_regclass('controlling.qlik_import_runs') IS NOT NULL AS runs_ready,
      to_regclass('controlling.qlik_import_rows') IS NOT NULL AS rows_ready,
      to_regclass('controlling.qlik_import_events') IS NOT NULL AS events_ready,
      (
        SELECT count(*) = 3
        FROM pg_trigger
        WHERE NOT tgisinternal
          AND tgname IN (
            'qlik_import_runs_immutable',
            'qlik_import_rows_append_only',
            'qlik_import_events_append_only'
          )
      ) AS guards_ready
  `);

  const state = verification.rows[0] || {};
  if (!state.runs_ready || !state.rows_ready || !state.events_ready || !state.guards_ready) {
    throw new Error('Qlik import schema verification failed');
  }
  console.log('✅ Qlik-Importschema mit unveränderlicher Rohschicht ist bereit');
}

module.exports = { runQlikImportMigration };
