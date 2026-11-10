const PAYMENT_TYPES = new Set(['EC_CASH', 'KREDITKARTE']);
const PROVIDERS = new Set(['FIRSTDATA', 'TELECASH']);
const REVIEW_ACTIONS = Object.freeze({
  start: 'IN_PRUEFUNG',
  confirm: 'BESTAETIGT',
  correct: 'KORRIGIERT',
  defer: 'VERTAGT',
});
const TRANSFER_ACTIONS = new Set(['confirm', 'reject', 'defer']);

function problem(message, statusCode = 400) {
  return Object.assign(new Error(message), { statusCode });
}

function berlinDateOnly(value) {
  if (!value) return null;
  if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}/.test(value)) return value.slice(0, 10);
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/Berlin', year: 'numeric', month: '2-digit', day: '2-digit',
  }).formatToParts(new Date(value));
  const part = (type) => parts.find((entry) => entry.type === type)?.value;
  return `${part('year')}-${part('month')}-${part('day')}`;
}

function classicDayHasPositiveRevenue(day) {
  return Object.values(day?.filialen || {}).some((branch) =>
    Number(branch?.sollBetrag) > 0 || Number(branch?.istBetrag) > 0
  );
}

function filterPositiveRevenueItems(items) {
  return (Array.isArray(items) ? items : []).filter((item) => Number(item?.qlikTotalCents) > 0);
}

function parseMonth(value, label) {
  const number = Number(value);
  if (!Number.isInteger(number) || number < 1 || number > 12) {
    throw problem(`${label} muss zwischen 1 und 12 liegen.`);
  }
  return number;
}

function parseYear(value) {
  const number = Number(value);
  if (!Number.isInteger(number) || number < 2020 || number > 2200) {
    throw problem('jahr ist ungültig.');
  }
  return number;
}

function normalizePaymentType(value) {
  const normalized = String(value || '').trim().toUpperCase();
  if (!PAYMENT_TYPES.has(normalized)) throw problem('Zahlungsart ist ungültig.');
  return normalized;
}

function normalizeProvider(value) {
  const normalized = String(value || '').trim().toUpperCase();
  if (!PROVIDERS.has(normalized)) throw problem('Zahlungsdienstleister ist ungültig.');
  return normalized;
}

function normalizeNote(value, { required = false } = {}) {
  const note = String(value || '').trim();
  if (required && note.length < 3) throw problem('Eine kurze Begründung ist erforderlich.');
  if (note.length > 1000) throw problem('Die Begründung darf höchstens 1000 Zeichen enthalten.');
  return note || null;
}

function normalizeReviewDecision(payload = {}) {
  const action = String(payload.action || '').trim();
  const status = REVIEW_ACTIONS[action];
  if (!status) throw problem('Prüfentscheidung ist ungültig.');
  const note = normalizeNote(payload.note, { required: action === 'correct' || action === 'defer' });
  const deferredUntil = payload.deferredUntil ? String(payload.deferredUntil) : null;
  if (action === 'defer') {
    const date = new Date(`${deferredUntil}T00:00:00Z`);
    if (!deferredUntil || Number.isNaN(date.getTime())) throw problem('Ein gültiges Wiedervorlagedatum ist erforderlich.');
  }
  return { action, status, note, deferredUntil: action === 'defer' ? deferredUntil : null };
}

function normalizeTransferDecision(payload = {}) {
  const action = String(payload.action || '').trim();
  if (!TRANSFER_ACTIONS.has(action)) throw problem('Match-Entscheidung ist ungültig.');
  const note = normalizeNote(payload.note, { required: action !== 'confirm' });
  const deferredUntil = payload.deferredUntil ? String(payload.deferredUntil) : null;
  if (action === 'defer') {
    const date = new Date(`${deferredUntil}T00:00:00Z`);
    if (!deferredUntil || Number.isNaN(date.getTime())) throw problem('Ein gültiges Wiedervorlagedatum ist erforderlich.');
  }
  return { action, note, deferredUntil: action === 'defer' ? deferredUntil : null };
}

function moneyToCents(value) {
  if (value === null || value === undefined || value === '') throw problem('Betrag fehlt.');
  let normalized = value;
  if (typeof value === 'string') {
    const compact = value.replace(/[€\s]/g, '');
    normalized = compact.includes(',')
      ? compact.replace(/\./g, '').replace(',', '.')
      : compact;
  }
  const number = Number(normalized);
  if (!Number.isFinite(number) || number <= 0) throw problem('Betrag muss größer als null sein.');
  return Math.round(number * 100);
}

function matchResult(usable, sumCents, targetCents, previousSum, candidateBySum, complete = true) {
  const itemIds = [];
  let current = sumCents;
  while (current > 0) {
    const candidateIndex = candidateBySum[current];
    if (candidateIndex < 0) break;
    itemIds.push(usable[candidateIndex].id);
    current = previousSum[current];
  }
  itemIds.reverse();
  return {
    exact: sumCents === targetCents,
    complete,
    sumCents,
    differenceCents: targetCents - sumCents,
    itemIds,
  };
}

function findBestMatch(candidates, targetCents, { maxStates = 5000000 } = {}) {
  const target = Number(targetCents);
  if (!Number.isSafeInteger(target) || target <= 0) throw problem('Match-Zielbetrag ist ungültig.');
  const usable = (Array.isArray(candidates) ? candidates : [])
    .map((candidate) => ({ ...candidate, amountCents: Number(candidate.amountCents) }))
    .filter((candidate) => candidate.id && Number.isSafeInteger(candidate.amountCents) && candidate.amountCents > 0)
    .sort((left, right) => String(left.datum).localeCompare(String(right.datum)) || left.amountCents - right.amountCents);

  const allSum = usable.reduce((sum, candidate) => sum + candidate.amountCents, 0);
  if (allSum === target) {
    return { exact: true, complete: true, sumCents: target, differenceCents: 0, itemIds: usable.map((candidate) => candidate.id) };
  }

  if (!Number.isSafeInteger(maxStates) || maxStates < target + 1) {
    return { exact: false, complete: false, sumCents: 0, differenceCents: target, itemIds: [] };
  }

  const previousSum = new Int32Array(target + 1);
  const candidateBySum = new Int16Array(target + 1);
  previousSum.fill(-1);
  candidateBySum.fill(-1);
  previousSum[0] = 0;

  const reachableSums = new Int32Array(target + 1);
  let reachableCount = 1;
  let bestSum = 0;

  for (let candidateIndex = 0; candidateIndex < usable.length; candidateIndex += 1) {
    const amountCents = usable[candidateIndex].amountCents;
    const previousReachableCount = reachableCount;
    for (let index = 0; index < previousReachableCount; index += 1) {
      const sum = reachableSums[index];
      const next = sum + amountCents;
      if (next > target || previousSum[next] !== -1) continue;
      previousSum[next] = sum;
      candidateBySum[next] = candidateIndex;
      reachableSums[reachableCount] = next;
      reachableCount += 1;
      if (next > bestSum) bestSum = next;
      if (next === target) return matchResult(usable, next, target, previousSum, candidateBySum);
    }
  }

  return matchResult(usable, bestSum, target, previousSum, candidateBySum);
}

async function synchronizeQlikPaymentDays(client, { jahr, monat }) {
  const year = parseYear(jahr);
  const month = parseMonth(monat, 'monat');
  const sourceResult = await client.query(
    `
    WITH candidates AS (
      SELECT derived.id AS derived_value_id, derived.metric_definition_id,
             derived.business_date, branch.name AS branch_name,
             derived.numeric_value, derived.availability_status,
             metric.metric_code, rule.version AS rule_version,
             binding.id AS binding_id, binding.consumer_id,
             binding.binding_config ->> 'targetPaymentTypeCode' AS target_payment_type_code,
             derived.calculated_at,
             provenance.import_run_id, provenance.source_count,
             row_number() OVER (
               PARTITION BY derived.metric_definition_id, derived.branch_id, derived.business_date
               ORDER BY derived.calculated_at DESC, derived.id DESC
             ) AS rank
      FROM controlling.qlik_derived_daily_values derived
      JOIN controlling.qlik_metric_definitions metric
        ON metric.id=derived.metric_definition_id AND metric.active=true
      JOIN controlling.qlik_derivation_rule_versions rule
        ON rule.id=derived.rule_version_id AND rule.status='APPROVED'
      JOIN public.filialen branch ON branch.id=derived.branch_id AND branch.aktiv=true
      JOIN controlling.qlik_metric_consumer_bindings binding
        ON binding.metric_definition_id=metric.id
       AND binding.status='APPROVED' AND binding.usage_type='VORBELEGUNG'
       AND derived.business_date >= binding.valid_from
       AND (binding.valid_to IS NULL OR derived.business_date <= binding.valid_to)
      JOIN controlling.qlik_consumers consumer
        ON consumer.id=binding.consumer_id
       AND consumer.consumer_code='ALEX_MODULE_2' AND consumer.active=true
      JOIN LATERAL (
        SELECT min(raw.import_run_id::text)::uuid AS import_run_id,
               count(*)::integer AS source_count,
               count(DISTINCT raw.import_run_id) AS import_run_count
        FROM controlling.qlik_derived_value_sources source
        JOIN controlling.qlik_import_rows raw ON raw.id=source.qlik_import_row_id
        WHERE source.derived_value_id=derived.id
      ) provenance ON provenance.import_run_count=1
      WHERE EXTRACT(YEAR FROM derived.business_date)::integer=$1
        AND EXTRACT(MONTH FROM derived.business_date)::integer=$2
        AND derived.availability_status='VORHANDEN'
        AND metric.metric_code IN ('EC_ZAHLUNGEN','KREDITKARTEN_ZAHLUNGEN')
        AND binding.binding_config ->> 'targetPaymentTypeCode' IN ('EC_CASH','KREDITKARTE')
    )
    SELECT * FROM candidates WHERE rank=1
    ORDER BY business_date, target_payment_type_code, branch_name
    `,
    [year, month]
  );

  const values = sourceResult.rows.map((row) => {
    const amountCents = Number(row.numeric_value);
    if (!Number.isSafeInteger(amountCents) || amountCents < 0) {
      throw problem(`Veredelter Qlik-Wert ${row.metric_code} ist kein gültiger Centbetrag.`, 500);
    }
    return { ...row, amountCents };
  });

  const duplicateKeys = new Set();
  const seenKeys = new Set();
  for (const value of values) {
    const key = `${berlinDateOnly(value.business_date)}|${value.target_payment_type_code}|${value.branch_name}`;
    if (seenKeys.has(key)) duplicateKeys.add(key);
    seenKeys.add(key);
  }
  if (duplicateKeys.size) {
    throw problem('Mehrdeutige freigegebene Alex-2-Verbraucherbindungen erkannt.', 500);
  }

  const existingResult = await client.query(
    `SELECT pd.id, pd.datum, pd.filiale, pt.code AS payment_type,
            pd.qlik_amount_cents, pd.qlik_import_run_id, pd.qlik_derived_value_id
     FROM controlling.payment_days pd
     JOIN controlling.payment_types pt ON pt.id=pd.payment_type_id
     WHERE pd.jahr=$1 AND pd.monat=$2 AND pt.code=ANY($3::text[])`,
    [year, month, [...PAYMENT_TYPES]]
  );
  const existingByKey = new Map(existingResult.rows.map((row) => {
    const date = berlinDateOnly(row.datum);
    return [`${date}|${row.payment_type}|${row.filiale}`, row];
  }));

  let synchronized = 0;
  for (const value of values) {
    const date = berlinDateOnly(value.business_date);
    const existing = existingByKey.get(`${date}|${value.target_payment_type_code}|${value.branch_name}`);
    const sourceChanged = !existing
      || Number(existing.qlik_amount_cents) !== value.amountCents
      || String(existing.qlik_import_run_id) !== String(value.import_run_id)
      || String(existing.qlik_derived_value_id) !== String(value.derived_value_id);
    const upsert = await client.query(
      `
      INSERT INTO controlling.payment_days (
        payment_type_id, filiale, datum, jahr, monat, tag, soll_betrag,
        qlik_amount_cents, qlik_import_run_id, qlik_derived_value_id, availability_status,
        comparison_status, review_status, updated_at
      )
      SELECT pt.id, $2, $3::date, $4, $5, EXTRACT(DAY FROM $3::date)::integer,
             $6::bigint / 100.0, $6, $7, $8, 'VORHANDEN', 'NICHT_VERGLICHEN', 'OFFEN', now()
      FROM controlling.payment_types pt WHERE pt.code=$1
      ON CONFLICT (payment_type_id, filiale, datum) DO UPDATE SET
        soll_betrag=COALESCE(controlling.payment_days.soll_betrag, EXCLUDED.soll_betrag),
        qlik_amount_cents=EXCLUDED.qlik_amount_cents,
        qlik_import_run_id=EXCLUDED.qlik_import_run_id,
        qlik_derived_value_id=EXCLUDED.qlik_derived_value_id,
        availability_status='VORHANDEN',
        comparison_status=CASE
          WHEN controlling.payment_days.ist_betrag IS NULL THEN 'NICHT_VERGLICHEN'
          WHEN round(controlling.payment_days.ist_betrag * 100)::bigint=EXCLUDED.qlik_amount_cents THEN 'UEBEREINSTIMMEND'
          ELSE 'ABWEICHEND'
        END,
        review_status=CASE
          WHEN controlling.payment_days.qlik_amount_cents IS DISTINCT FROM EXCLUDED.qlik_amount_cents
            OR controlling.payment_days.qlik_derived_value_id IS DISTINCT FROM EXCLUDED.qlik_derived_value_id
          THEN 'OFFEN'
          ELSE controlling.payment_days.review_status
        END,
        reviewed_by=CASE WHEN controlling.payment_days.qlik_amount_cents IS DISTINCT FROM EXCLUDED.qlik_amount_cents OR controlling.payment_days.qlik_derived_value_id IS DISTINCT FROM EXCLUDED.qlik_derived_value_id THEN NULL ELSE controlling.payment_days.reviewed_by END,
        reviewed_at=CASE WHEN controlling.payment_days.qlik_amount_cents IS DISTINCT FROM EXCLUDED.qlik_amount_cents OR controlling.payment_days.qlik_derived_value_id IS DISTINCT FROM EXCLUDED.qlik_derived_value_id THEN NULL ELSE controlling.payment_days.reviewed_at END,
        updated_at=now()
      RETURNING id
      `,
      [value.target_payment_type_code, value.branch_name, date, year, month, value.amountCents, value.import_run_id, value.derived_value_id]
    );
    const paymentDayId = upsert.rows[0]?.id;
    if (!paymentDayId) continue;
    await client.query('DELETE FROM controlling.payment_day_qlik_sources WHERE payment_day_id=$1', [paymentDayId]);
    if (sourceChanged) {
      await client.query(
        `INSERT INTO controlling.payment_review_events (payment_day_id,event_type,details)
         VALUES ($1,'QLIK_SYNCHRONISIERT',jsonb_build_object(
           'amountCents',$2::bigint,'sourceCount',$3::integer,
           'derivedValueId',$4::text,'metricCode',$5::text,'ruleVersion',$6::integer
         ))`,
        [paymentDayId, value.amountCents, value.source_count, value.derived_value_id, value.metric_code, value.rule_version]
      );
      await client.query(
        `INSERT INTO controlling.qlik_consumer_value_events (
           derived_value_id,binding_id,metric_definition_id,consumer_id,event_type,details
         ) VALUES ($1,$2,$3,$4,'PRESENTED',jsonb_build_object(
           'paymentDayId',$5::text,'targetPaymentTypeCode',$6::text
         ))`,
        [value.derived_value_id, value.binding_id, value.metric_definition_id, value.consumer_id,
          paymentDayId, value.target_payment_type_code]
      );
    }
    synchronized += 1;
  }
  return { synchronized };
}

module.exports = {
  PAYMENT_TYPES,
  PROVIDERS,
  berlinDateOnly,
  classicDayHasPositiveRevenue,
  filterPositiveRevenueItems,
  parseMonth,
  parseYear,
  normalizePaymentType,
  normalizeProvider,
  normalizeReviewDecision,
  normalizeTransferDecision,
  moneyToCents,
  findBestMatch,
  synchronizeQlikPaymentDays,
};
