const { BRANCHES } = require('./tageskassePolicy');

const CONTROLLING_ROLES = new Set(['Admin', 'Supervisor', 'Geschäftsführer']);
const RECONCILIATION_METRICS = Object.freeze([
  { key: 'dailyRevenue', metricCode: 'GESAMTUMSATZ_QLIK', appField: 'daily_revenue_cents' },
  { key: 'bankDepositTarget', metricCode: 'BAR_AN_BANK_QLIK', appField: 'eurocontrol_bank_cents' },
  { key: 'cashDifference', metricCode: 'KASSENDIFFERENZ', appField: 'cash_difference_cents' },
]);

function problem(message, statusCode = 400) {
  return Object.assign(new Error(message), { statusCode });
}

function validateBusinessDate(value) {
  const date = String(value || '');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) throw problem('Das Datum ist ungültig.');
  return date;
}

function validateBranch(value) {
  const branch = String(value || '').trim();
  if (!BRANCHES.has(branch)) throw problem('Die Filiale ist ungültig.');
  return branch;
}

function validateConsumerCode(value) {
  if (value === undefined || value === null || value === '') return null;
  const code = String(value).trim().toUpperCase();
  if (!/^[A-Z][A-Z0-9_]{2,99}$/.test(code)) throw problem('Der Verbraucher-Code ist ungültig.');
  return code;
}

function normalizeDerivedRow(row) {
  const numericValue = Number(row.numeric_value);
  if (!Number.isFinite(numericValue)) throw problem('Ein Qlik-Fachwert ist nicht numerisch.', 500);
  if (row.value_kind === 'MONEY_CENTS' && !Number.isSafeInteger(numericValue)) {
    throw problem('Ein Qlik-Geldwert enthält Bruchteile eines Cents.', 500);
  }
  return {
    id: row.id,
    metricCode: row.metric_code,
    displayName: row.display_name,
    valueKind: row.value_kind,
    value: numericValue,
    valueCents: row.value_kind === 'MONEY_CENTS' ? numericValue : null,
    availabilityStatus: row.availability_status,
    qualityStatus: row.quality_status,
    ruleVersion: Number(row.rule_version),
    calculatedAt: row.calculated_at,
    sourceCount: Number(row.source_count),
  };
}

async function loadDerivedValues(queryable, { branch, businessDate, consumerCode = null }) {
  const normalizedBranch = validateBranch(branch);
  const normalizedDate = validateBusinessDate(businessDate);
  const normalizedConsumer = validateConsumerCode(consumerCode);
  const result = await queryable.query(`
    WITH candidates AS (
      SELECT value.id, metric.metric_code, metric.display_name, metric.value_kind,
             value.numeric_value, value.availability_status, value.quality_status,
             rule.version AS rule_version, value.calculated_at,
             (SELECT count(*) FROM controlling.qlik_derived_value_sources source
              WHERE source.derived_value_id=value.id) AS source_count,
             row_number() OVER (
               PARTITION BY metric.id
               ORDER BY value.calculated_at DESC, value.id DESC
             ) AS rank
      FROM controlling.qlik_derived_daily_values value
      JOIN controlling.qlik_metric_definitions metric
        ON metric.id=value.metric_definition_id AND metric.active=true
      JOIN controlling.qlik_derivation_rule_versions rule
        ON rule.id=value.rule_version_id AND rule.status='APPROVED'
      JOIN public.filialen branch ON branch.id=value.branch_id
      WHERE lower(branch.name)=lower($1) AND value.business_date=$2::date
        AND ($3::text IS NULL OR EXISTS (
          SELECT 1
          FROM controlling.qlik_metric_consumer_bindings binding
          JOIN controlling.qlik_consumers consumer ON consumer.id=binding.consumer_id
          WHERE binding.metric_definition_id=metric.id
            AND binding.status='APPROVED' AND consumer.active=true
            AND consumer.consumer_code=$3
            AND value.business_date >= binding.valid_from
            AND (binding.valid_to IS NULL OR value.business_date <= binding.valid_to)
        ))
    )
    SELECT * FROM candidates WHERE rank=1 ORDER BY metric_code
  `, [normalizedBranch, normalizedDate, normalizedConsumer]);
  return {
    branch: normalizedBranch,
    businessDate: normalizedDate,
    consumerCode: normalizedConsumer,
    values: result.rows.map(normalizeDerivedRow),
  };
}

function compareValue({ appValue, qlikValue }) {
  if (!qlikValue) return { status: 'MISSING_QLIK', differenceCents: null, requiresMirkoReview: false };
  if (qlikValue.availabilityStatus !== 'VORHANDEN') {
    return { status: 'QLIK_INCOMPLETE', differenceCents: null, requiresMirkoReview: false };
  }
  if (appValue === null || appValue === undefined) {
    return { status: 'MISSING_APP_VALUE', differenceCents: null, requiresMirkoReview: false };
  }
  const differenceCents = Number(appValue) - Number(qlikValue.valueCents);
  return {
    status: differenceCents === 0 ? 'MATCH' : 'DEVIATION',
    differenceCents,
    requiresMirkoReview: differenceCents !== 0,
  };
}

async function loadDayCloseReconciliation(queryable, input) {
  const derived = await loadDerivedValues(queryable, input);
  const appResult = await queryable.query(`
    SELECT metrics.daily_revenue_cents, metrics.eurocontrol_bank_cents,
           (SELECT sum(closing.eurocontrol_difference_cents)
            FROM controlling.cash_drawer_closings closing
            WHERE closing.business_day_id=day.id AND closing.status='confirmed') AS cash_difference_cents,
           day.status AS business_day_status
    FROM public.filialen branch
    LEFT JOIN controlling.cash_business_days day
      ON day.filiale_id=branch.id AND day.business_date=$2::date
    LEFT JOIN controlling.branch_daily_metrics metrics ON metrics.business_day_id=day.id
    WHERE lower(branch.name)=lower($1) AND branch.aktiv=true
    LIMIT 1
  `, [derived.branch, derived.businessDate]);
  const appValues = appResult.rows[0] || {};
  const byCode = new Map(derived.values.map((value) => [value.metricCode, value]));
  const comparisons = RECONCILIATION_METRICS.map((definition) => {
    const qlikValue = byCode.get(definition.metricCode) || null;
    const rawAppValue = appValues[definition.appField];
    const appValueCents = rawAppValue === null || rawAppValue === undefined ? null : Number(rawAppValue);
    return {
      key: definition.key,
      metricCode: definition.metricCode,
      appValueCents,
      qlikValue,
      ...compareValue({ appValue: appValueCents, qlikValue }),
    };
  });
  return {
    branch: derived.branch,
    businessDate: derived.businessDate,
    businessDayStatus: appValues.business_day_status || null,
    blocking: false,
    reviewOwner: 'MIRKO',
    comparisons,
  };
}

module.exports = {
  CONTROLLING_ROLES,
  RECONCILIATION_METRICS,
  compareValue,
  loadDayCloseReconciliation,
  loadDerivedValues,
  normalizeDerivedRow,
  validateBranch,
  validateBusinessDate,
  validateConsumerCode,
};
