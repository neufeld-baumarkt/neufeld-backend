const { existsSync, readFileSync } = require('node:fs');
const { join } = require('node:path');

const bundledMigrationPath = join(__dirname, '..', 'migrations', '056_eurocontrol_fallback_source.sql');
const saveroomMigrationPath = join(__dirname, '..', '..', 'database', 'migrations', '056_eurocontrol_fallback_source.sql');
const migrationPath = existsSync(bundledMigrationPath) ? bundledMigrationPath : saveroomMigrationPath;

async function runEuroControlImportMigration(pool) {
  await pool.query(readFileSync(migrationPath, 'utf8'));
}

module.exports = { migrationPath, runEuroControlImportMigration };
