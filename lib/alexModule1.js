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

function centsToAmount(value) {
  const cents = Number(value);
  if (!Number.isSafeInteger(cents) || cents <= 0) {
    throw problem('Der Qlik-Gesamtumsatz ist kein positiver Centbetrag.');
  }
  return cents / 100;
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
    SELECT b.id,b.betrag,b.quelle,b.erstellt_von
    FROM cashflow.buchungen b
    JOIN cashflow.kategorien k ON k.id=b.kategorie_id
    WHERE k.typ='Einnahme'
      AND b.kategorie_id=1
      AND b.filiale='Unternehmen'
      AND b.status='angekuendigt'
      AND (
        b.datum=$1::date
        OR (b.datum IS NULL AND b.jahr=$2 AND b.kw=$3 AND b.tag=$4)
      )
    ORDER BY b.erstellt_am,b.id
    FOR UPDATE OF b
    `,
    [calendar.businessDate, calendar.jahr, calendar.kw, calendar.tag],
  );
  if (planResult.rows.length === 0) return { status: 'PLAN_NOT_FOUND', synchronized: 0 };
  if (planResult.rows.length !== 1) return { status: 'PLAN_AMBIGUOUS', synchronized: 0 };

  const plan = planResult.rows[0];
  const planAmount = Number(plan.betrag);
  const actualAmount = centsToAmount(aggregate.numeric_value);
  if (!Number.isFinite(planAmount) || planAmount <= 0) {
    throw problem('Die Alex-1-Planzahl ist ungueltig.');
  }
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

module.exports = { isoCalendarParts, synchronizeQlikDailyRevenue };
