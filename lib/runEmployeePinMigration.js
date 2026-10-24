const fs = require('fs');
const path = require('path');

const migrationPath = path.join(
  __dirname,
  '..',
  'migrations',
  '011_employee_pin_first_use.sql'
);

async function runEmployeePinMigration(pool) {
  const sql = fs.readFileSync(migrationPath, 'utf8');
  await pool.query(sql);
}

module.exports = { runEmployeePinMigration };
