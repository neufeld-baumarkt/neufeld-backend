const { readFileSync } = require('node:fs');
const { join } = require('node:path');

const migrationPath = join(__dirname, '..', 'migrations', '050_alex2_live_sync_notifications.sql');

async function runAlexModule2LiveSyncMigration(pool) {
  const sql = readFileSync(migrationPath, 'utf8');
  await pool.query(sql);
}

module.exports = { migrationPath, runAlexModule2LiveSyncMigration };
