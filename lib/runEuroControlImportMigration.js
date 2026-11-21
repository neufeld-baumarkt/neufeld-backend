const { existsSync, readFileSync } = require('node:fs');
const { join } = require('node:path');

const migrationNames = [
  '056_eurocontrol_fallback_source.sql',
  '057_eurocontrol_canonical_fallback.sql',
  '058_alex2_canonical_payment_days.sql',
];
const migrationPaths = migrationNames.map((name) => {
  const bundled = join(__dirname, '..', 'migrations', name);
  return existsSync(bundled) ? bundled : join(__dirname, '..', '..', 'database', 'migrations', name);
});

async function runEuroControlImportMigration(pool) {
  for (const migrationPath of migrationPaths) {
    await pool.query(readFileSync(migrationPath, 'utf8'));
  }
}

module.exports = { migrationPaths, runEuroControlImportMigration };
