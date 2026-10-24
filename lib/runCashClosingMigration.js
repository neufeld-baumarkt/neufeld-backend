const fs = require('fs');
const path = require('path');

const migrationPath = path.join(__dirname, '..', 'migrations', '012_cash_drawer_closings.sql');

async function runCashClosingMigration(pool) {
  const sql = fs.readFileSync(migrationPath, 'utf8');
  await pool.query(sql);
}

module.exports = { runCashClosingMigration };
