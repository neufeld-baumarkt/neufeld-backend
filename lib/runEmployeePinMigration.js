const fs = require('fs');
const path = require('path');

const migrationPaths = [
  '011_employee_pin_first_use.sql',
  '013_global_employee_pins.sql',
].map((name) => path.join(__dirname, '..', 'migrations', name));

async function runEmployeePinMigration(pool) {
  for (const migrationPath of migrationPaths) {
    const sql = fs.readFileSync(migrationPath, 'utf8');
    await pool.query(sql);
  }
}

module.exports = { migrationPaths, runEmployeePinMigration };
