const { existsSync, readFileSync } = require('node:fs');
const { join } = require('node:path');
const { synchronizeQlikPaymentDays } = require('./alexModule2');

const migrationNames = [
  '056_eurocontrol_fallback_source.sql',
  '057_eurocontrol_canonical_fallback.sql',
  '058_alex2_canonical_payment_days.sql',
];
const migrationPaths = migrationNames.map((name) => {
  const bundled = join(__dirname, '..', 'migrations', name);
  return existsSync(bundled) ? bundled : join(__dirname, '..', '..', 'database', 'migrations', name);
});

async function reconcileImportedEuroControlPaymentDays(
  pool,
  synchronize = synchronizeQlikPaymentDays
) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const periods = await client.query(
      `SELECT DISTINCT
              EXTRACT(YEAR FROM derived.business_date)::integer AS jahr,
              EXTRACT(MONTH FROM derived.business_date)::integer AS monat
       FROM controlling.eurocontrol_derived_daily_values derived
       JOIN controlling.qlik_metric_definitions metric
         ON metric.id=derived.metric_definition_id AND metric.active=true
       WHERE derived.availability_status='VORHANDEN'
         AND metric.metric_code IN ('EC_ZAHLUNGEN','KREDITKARTEN_ZAHLUNGEN')
       ORDER BY jahr, monat`
    );
    const months = [];
    let synchronized = 0;
    for (const period of periods.rows) {
      const jahr = Number(period.jahr);
      const monat = Number(period.monat);
      const result = await synchronize(client, { jahr, monat });
      const count = Number(result?.synchronized || 0);
      synchronized += count;
      months.push({ jahr, monat, synchronized: count });
    }
    await client.query('COMMIT');
    return { months, synchronized };
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

async function runEuroControlImportMigration(pool, options = {}) {
  for (const migrationPath of migrationPaths) {
    await pool.query(readFileSync(migrationPath, 'utf8'));
  }
  const reconcile = options.reconcile || reconcileImportedEuroControlPaymentDays;
  return reconcile(pool);
}

module.exports = {
  migrationPaths,
  reconcileImportedEuroControlPaymentDays,
  runEuroControlImportMigration,
};
