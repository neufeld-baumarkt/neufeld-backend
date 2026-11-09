const DAY_TAGS = Object.freeze(['So', 'Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa']);

function problem(message, code = 'ALEX1_QLIK_SYNC_FAILED') {
  return Object.assign(new Error(message), { code });
}

function validateBusinessDate(value) {
  const date = value instanceof Date && !Number.isNaN(value.getTime())
    ? [
      value.getFullYear(),
      String(value.getMonth() + 1).padStart(2, '0'),
      String(value.getDate()).padStart(2, '0'),
    ].join('-')
    : String(value || '').slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    throw problem('Das Qlik-Berichtsdatum kann nicht fuer Alex Modul 1 verwendet werden.');
  }
  return date;
}

function isoCalendarParts(value) {
  const businessDate = validateBusinessDate(value);
  const date = new Date(`${businessDate}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) throw problem('Das Qlik-Berichtsdatum ist ungueltig.');
  const day = date.getUTCDay() || 7;
  const thursday = new Date(date);
  thursday.setUTCDate(date.getUTCDate() + 4 - day);
  const yearStart = new Date(Date.UTC(thursday.getUTCFullYear(), 0, 1));
  const week = Math.ceil((((thursday - yearStart) / 86400000) + 1) / 7);
  return { businessDate, jahr: thursday.getUTCFullYear(), kw: week, tag: DAY_TAGS[date.getUTCDay()] };
}

async function synchronizeQlikDailyRevenue(queryable, { businessDate } = {}) {
  if (!queryable || typeof queryable.query !== 'function') {
    throw problem('Eine Datenbankverbindung ist erforderlich.');
  }
  const calendar = isoCalendarParts(businessDate);
  const aggregateResult = await queryable.query(
    `
    SELECT
      aggregate.id,
      aggregate.numeric_value,
      aggregate.availability_status,
      aggregate.expected_member_count,
      aggregate.actual_member_count,
      count(source.derived_daily_value_id)::integer AS source_count
    FROM controlling.qlik_derived_daily_aggregates aggregate
    JOIN controlling.qlik_metric_definitions metric
      ON metric.id=aggregate.metric_definition_id
     AND metric.metric_code='GESAMTUMSATZ_ALLE_FILIALEN_QLIK'
    JOIN controlling.qlik_derivation_rule_versions rule
      ON rule.id=aggregate.rule_version_id
     AND rule.status='APPROVED'
    JOIN controlling.qlik_metric_consumer_bindings binding
      ON binding.metric_definition_id=metric.id
     AND binding.status='APPROVED'
     AND binding.usage_type='VORBELEGUNG'
    JOIN controlling.qlik_consumers consumer
      ON consumer.id=binding.consumer_id
     AND consumer.consumer_code='ALEX_MODULE_1'
    LEFT JOIN controlling.qlik_derived_daily_aggregate_sources source
      ON source.aggregate_value_id=aggregate.id
    WHERE aggregate.business_date=$1::date
    GROUP BY aggregate.id
    ORDER BY aggregate.calculated_at DESC, aggregate.id DESC
    LIMIT 1
    `,
    [calendar.businessDate],
  );
  const aggregate = aggregateResult.rows[0];
  if (!aggregate) return { status: 'QLIK_NOT_AVAILABLE', synchronized: 0 };

  const expectedCount = Number(aggregate.expected_member_count);
  const actualCount = Number(aggregate.actual_member_count);
  const sourceCount = Number(aggregate.source_count);
  if (
    aggregate.availability_status !== 'VORHANDEN'
    || expectedCount !== 4
    || actualCount !== expectedCount
    || sourceCount !== expectedCount
  ) {
    return {
      status: 'QLIK_INCOMPLETE', synchronized: 0,
      expectedMemberCount: expectedCount, actualMemberCount: actualCount, sourceCount,
    };
  }

  const alreadyBooked = await queryable.query(
    'SELECT id FROM cashflow.buchungen WHERE qlik_aggregate_value_id=$1 LIMIT 1',
    [aggregate.id],
  );
  if (alreadyBooked.rows[0]) {
    return { status: 'ALREADY_BOOKED', synchronized: 0, bookingId: alreadyBooked.rows[0].id };
  }

  const planResult = await queryable.query(
    `
    SELECT b.id,b.betrag,b.quelle,b.erstellt_von,b.status
    FROM cashflow.buchungen b
    JOIN cashflow.kategorien k ON k.id=b.kategorie_id
    WHERE k.typ='Einnahme'
      AND b.kategorie_id=1
      AND b.filiale='Unternehmen'
      AND b.status IN ('angekuendigt','gebucht')
      AND (
        b.datum=$1::date
        OR (b.datum IS NULL AND b.jahr=$2 AND b.kw=$3 AND b.tag=$4)
      )
    ORDER BY CASE WHEN b.status='gebucht' THEN 0 ELSE 1 END,b.erstellt_am,b.id
    FOR UPDATE OF b
    `,
    [calendar.businessDate, calendar.jahr, calendar.kw, calendar.tag],
  );
  const existingBooking = planResult.rows.find((row) => row.status === 'gebucht');
  if (existingBooking) {
    return { status: 'EXISTING_BOOKING', synchronized: 0, bookingId: existingBooking.id };
  }
  const plans = planResult.rows.filter((row) => row.status === 'angekuendigt');
  if (plans.length === 0) return { status: 'PLAN_NOT_FOUND', synchronized: 0 };
  if (plans.length !== 1) return { status: 'PLAN_AMBIGUOUS', synchronized: 0 };

  const plan = plans[0];
  const rawPlanAmount = plan.betrag;
  const planAmount = Number(rawPlanAmount);
  if (rawPlanAmount == null || String(rawPlanAmount).trim() === ''
    || !Number.isFinite(planAmount) || planAmount <= 0) {
    return { status: 'PLAN_INVALID', synchronized: 0, bookingId: plan.id };
  }
  const actualCents = Number(aggregate.numeric_value);
  if (!Number.isSafeInteger(actualCents) || actualCents <= 0) {
    return { status: 'QLIK_INVALID', synchronized: 0, aggregateId: aggregate.id };
  }
  const actualAmount = actualCents / 100;
  const deviationAmount = Number((actualAmount - planAmount).toFixed(2));
  const deviationPercent = Number(((deviationAmount / planAmount) * 100).toFixed(2));
  const updated = await queryable.query(
    `
    UPDATE cashflow.buchungen
    SET datum=COALESCE(datum,$2::date),
        planbetrag=betrag,
        plan_quelle=quelle,
        plan_erstellt_von=erstellt_von,
        betrag=$3,
        status='gebucht',
        abweichung_betrag=$4,
        abweichung_prozent=$5,
        quelle='qlik_gesamtumsatz_alle_filialen',
        qlik_aggregate_value_id=$6,
        qlik_gebucht_am=NOW(),
        geaendert_am=NOW()
    WHERE id=$1 AND status='angekuendigt' AND qlik_aggregate_value_id IS NULL
    RETURNING id,planbetrag,betrag,abweichung_betrag,abweichung_prozent,status,
              plan_quelle,plan_erstellt_von,qlik_aggregate_value_id
    `,
    [plan.id, calendar.businessDate, actualAmount, deviationAmount, deviationPercent, aggregate.id],
  );
  if (!updated.rows[0]) throw problem('Die Alex-1-Planung wurde parallel veraendert.');
  return { status: 'BOOKED', synchronized: 1, booking: updated.rows[0], aggregateId: aggregate.id };
}

async function listPendingQlikDailyRevenueDates(queryable, { limit = 250 } = {}) {
  const safeLimit = Number.isInteger(limit) && limit > 0 && limit <= 1000 ? limit : 250;
  const result = await queryable.query(
    `
    WITH ranked_aggregates AS (
      SELECT aggregate.*,
             row_number() OVER (
               PARTITION BY aggregate.business_date
               ORDER BY aggregate.calculated_at DESC, aggregate.id DESC
             ) AS aggregate_rank
      FROM controlling.qlik_derived_daily_aggregates aggregate
      JOIN controlling.qlik_metric_definitions metric
        ON metric.id=aggregate.metric_definition_id
       AND metric.metric_code='GESAMTUMSATZ_ALLE_FILIALEN_QLIK'
      JOIN controlling.qlik_derivation_rule_versions rule
        ON rule.id=aggregate.rule_version_id
       AND rule.status='APPROVED'
      JOIN controlling.qlik_metric_consumer_bindings binding
        ON binding.metric_definition_id=metric.id
       AND binding.status='APPROVED'
       AND binding.usage_type='VORBELEGUNG'
      JOIN controlling.qlik_consumers consumer
        ON consumer.id=binding.consumer_id
       AND consumer.consumer_code='ALEX_MODULE_1'
    )
    SELECT aggregate.business_date
    FROM ranked_aggregates aggregate
    LEFT JOIN controlling.qlik_derived_daily_aggregate_sources source
      ON source.aggregate_value_id=aggregate.id
    WHERE aggregate.availability_status='VORHANDEN'
      AND aggregate.aggregate_rank=1
      AND aggregate.expected_member_count=4
      AND aggregate.actual_member_count=aggregate.expected_member_count
      AND NOT EXISTS (
        SELECT 1
        FROM cashflow.buchungen existing
        JOIN cashflow.kategorien category ON category.id=existing.kategorie_id
        WHERE category.typ='Einnahme'
          AND existing.kategorie_id=1
          AND existing.filiale='Unternehmen'
          AND existing.status='gebucht'
          AND (
            existing.datum=aggregate.business_date
            OR (
              existing.datum IS NULL
              AND existing.jahr=extract(isoyear FROM aggregate.business_date)::integer
              AND existing.kw=extract(week FROM aggregate.business_date)::integer
              AND existing.tag=CASE extract(isodow FROM aggregate.business_date)::integer
                WHEN 1 THEN 'Mo' WHEN 2 THEN 'Di' WHEN 3 THEN 'Mi'
                WHEN 4 THEN 'Do' WHEN 5 THEN 'Fr' WHEN 6 THEN 'Sa' ELSE 'So'
              END
            )
          )
      )
    GROUP BY aggregate.id, aggregate.business_date, aggregate.expected_member_count
    HAVING count(source.derived_daily_value_id)=aggregate.expected_member_count
    ORDER BY aggregate.business_date, aggregate.id
    LIMIT $1
    `,
    [safeLimit],
  );
  return result.rows.map((row) => validateBusinessDate(row.business_date));
}

async function retryPendingQlikDailyRevenue(pool, options = {}) {
  if (!pool || typeof pool.query !== 'function' || typeof pool.connect !== 'function') {
    throw problem('Ein Datenbank-Pool ist fuer die Alex-1-Wiederholungspruefung erforderlich.');
  }
  const dates = await listPendingQlikDailyRevenueDates(pool, options);
  const results = [];
  let synchronized = 0;

  for (const businessDate of dates) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const result = await synchronizeQlikDailyRevenue(client, { businessDate });
      await client.query('COMMIT');
      synchronized += Number(result.synchronized || 0);
      results.push({ businessDate, ...result });
    } catch (error) {
      try {
        await client.query('ROLLBACK');
      } catch (_) {
        // Der urspruengliche Tagesfehler bleibt die relevante Diagnose.
      }
      results.push({ businessDate, status: 'ERROR', synchronized: 0, error });
    } finally {
      client.release();
    }
  }

  return { checked: dates.length, synchronized, results };
}

module.exports = {
  isoCalendarParts,
  listPendingQlikDailyRevenueDates,
  retryPendingQlikDailyRevenue,
  synchronizeQlikDailyRevenue,
};
