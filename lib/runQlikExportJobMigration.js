const { existsSync, readFileSync } = require('node:fs');
const { join } = require('node:path');

const bundledMigrationPath = join(__dirname, '..', 'migrations', '055_qlik_export_job_control.sql');
const saveroomMigrationPath = join(__dirname, '..', '..', 'database', 'migrations', '055_qlik_export_job_control.sql');
const migrationPath = existsSync(bundledMigrationPath) ? bundledMigrationPath : saveroomMigrationPath;

async function runQlikExportJobMigration(pool) {
  await pool.query(readFileSync(migrationPath, 'utf8'));
}

module.exports = { migrationPath, runQlikExportJobMigration };
