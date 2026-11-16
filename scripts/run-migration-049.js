const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const pool = require('../db');

async function main() {
  const sql = readFileSync(join(__dirname, '..', 'migrations', '049_alex2_tabula_rasa_pre_october.sql'), 'utf8');
  await pool.query(sql);
  const result = await pool.query(
    `SELECT details FROM controlling.module2_maintenance_operations
     WHERE operation_key='2026-11-16_ALEX2_TABULA_RASA_PRE_OCTOBER_V1'`
  );
  if (!result.rowCount) throw new Error('Tabula-rasa operation receipt is missing.');
  console.log(JSON.stringify({ ok: true, details: result.rows[0].details }));
}

main()
  .then(() => pool.end())
  .catch(async (error) => {
    console.error(error);
    await pool.end().catch(() => {});
    process.exitCode = 1;
  });
