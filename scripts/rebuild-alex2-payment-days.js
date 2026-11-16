const pool = require('../db');
const { rebuildAlexModule2PaymentDays } = require('../lib/alexModule2');

async function main() {
  const result = await rebuildAlexModule2PaymentDays(pool);
  console.log(JSON.stringify(result));
}

main()
  .then(() => pool.end())
  .catch(async (error) => {
    console.error(error);
    await pool.end().catch(() => {});
    process.exitCode = 1;
  });

