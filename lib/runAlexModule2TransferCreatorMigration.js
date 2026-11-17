const { existsSync, readFileSync } = require('node:fs');
const { join } = require('node:path');

const bundledMigrationPath = join(__dirname, '..', 'migrations', '052_alex2_transfer_creator_user.sql');
const saveroomMigrationPath = join(__dirname, '..', '..', 'database', 'migrations', '052_alex2_transfer_creator_user.sql');
const migrationPath = existsSync(bundledMigrationPath) ? bundledMigrationPath : saveroomMigrationPath;

async function runAlexModule2TransferCreatorMigration(pool) {
  const sql = readFileSync(migrationPath, 'utf8');
  await pool.query(sql);
}

module.exports = { migrationPath, runAlexModule2TransferCreatorMigration };
