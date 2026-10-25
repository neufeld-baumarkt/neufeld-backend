const fs = require('fs');
const path = require('path');

const migrationPaths = [
  path.join(__dirname, '..', 'migrations', '012_cash_drawer_closings.sql'),
  path.join(__dirname, '..', 'migrations', '014_cash_closing_revisions.sql'),
  path.join(__dirname, '..', 'migrations', '015_cash_safe_counts.sql'),
];

async function runCashClosingMigration(pool) {
  for (const migrationPath of migrationPaths) {
    const sql = fs.readFileSync(migrationPath, 'utf8');
    await pool.query(sql);
  }
}

module.exports = { runCashClosingMigration };
