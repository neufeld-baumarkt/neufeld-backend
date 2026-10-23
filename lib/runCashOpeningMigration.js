const fs = require('fs');
const path = require('path');

const migrationPath = path.join(
  __dirname,
  '..',
  'migrations',
  '010_cash_drawer_openings.sql'
);

async function runCashOpeningMigration(pool) {
  const sql = fs.readFileSync(migrationPath, 'utf8');
  await pool.query(sql);
}

module.exports = { runCashOpeningMigration };
