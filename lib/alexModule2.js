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

function providerForPaymentType(value) {
  const paymentType = normalizePaymentType(value);
  return paymentType === 'EC_CASH' ? 'TELECASH' : 'FIRSTDATA';
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

const HISTORICAL_BACKFILL_OPERATION = '2026-11-10_ALEX2_QLIK_HISTORY_20260601_20261107';

async function backfillHistoricalQlikPaymentDays(pool, options = {}) {
  const fromDate = options.fromDate || '2026-06-01';
  const throughDate = options.throughDate || '2026-11-07';
  const synchronize = options.synchronize || synchronizeQlikPaymentDays;
  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [HISTORICAL_BACKFILL_OPERATION]);
    const applied = await client.query(
      `SELECT operation_key, applied_at, details
       FROM controlling.module2_maintenance_operations
       WHERE operation_key=$1`,
      [HISTORICAL_BACKFILL_OPERATION]
    );
    if (applied.rowCount) {
      await client.query('COMMIT');
      return { status: 'ALREADY_APPLIED', months: [], synchronized: 0 };
    }

    const monthResult = await client.query(
      `SELECT DISTINCT
              EXTRACT(YEAR FROM derived.business_date)::integer AS jahr,
              EXTRACT(MONTH FROM derived.business_date)::integer AS monat
       FROM controlling.qlik_derived_daily_values derived
       JOIN controlling.qlik_metric_definitions metric
         ON metric.id=derived.metric_definition_id AND metric.active=true
       WHERE derived.business_date BETWEEN $1::date AND $2::date
         AND derived.availability_status='VORHANDEN'
         AND metric.metric_code IN ('EC_ZAHLUNGEN','KREDITKARTEN_ZAHLUNGEN')
       ORDER BY jahr, monat`,
      [fromDate, throughDate]
    );

    const months = [];
    let synchronized = 0;
    for (const row of monthResult.rows) {
      const jahr = Number(row.jahr);
      const monat = Number(row.monat);
      const result = await synchronize(client, { jahr, monat });
      const count = Number(result?.synchronized || 0);
      synchronized += count;
      months.push({ jahr, monat, synchronized: count });
    }

    await client.query(
      `INSERT INTO controlling.module2_maintenance_operations (operation_key, details)
       VALUES ($1, jsonb_build_object(
         'fromDate',$2::text,
         'throughDate',$3::text,
         'months',$4::jsonb,
         'synchronized',$5::integer
       ))`,
      [HISTORICAL_BACKFILL_OPERATION, fromDate, throughDate, JSON.stringify(months), synchronized]
    );
    await client.query('COMMIT');
    return { status: 'APPLIED', months, synchronized };
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

const OCTOBER_CREDIT_CARD_DELETION_OPERATION = '2026-11-10_DELETE_CC_TRANSFERS_20261002_20261003';

async function deleteOctoberCreditCardTransfers(pool, options = {}) {
  const bookingDates = options.bookingDates || ['2026-10-02', '2026-10-03'];
  const actorUserId = options.actorUserId || null;
  const deletionReason = normalizeNote(
    options.reason || 'Rücksetzung nach historischem Qlik-Backfill.',
    { required: true }
  );
  if (!Array.isArray(bookingDates) || bookingDates.length !== 2
      || bookingDates.some((date) => !/^\d{4}-\d{2}-\d{2}$/.test(String(date)))) {
    throw problem('Für die Bereinigung werden genau zwei gültige Buchungsdaten benötigt.');
  }
  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [OCTOBER_CREDIT_CARD_DELETION_OPERATION]);
    const applied = await client.query(
      `SELECT operation_key FROM controlling.module2_maintenance_operations
       WHERE operation_key=$1`,
      [OCTOBER_CREDIT_CARD_DELETION_OPERATION]
    );
    if (applied.rowCount) {
      await client.query('COMMIT');
      return { status: 'ALREADY_APPLIED', deletedTransfers: 0, resetPaymentDays: 0 };
    }

    const targets = await client.query(
      `SELECT transfer.id AS transfer_id, transfer.buchungsdatum,
              match.id AS match_id, match.status AS match_status
       FROM controlling.payment_transfers transfer
       JOIN controlling.payment_types payment_type ON payment_type.id=transfer.payment_type_id
       LEFT JOIN controlling.payment_transfer_matches match
         ON match.transfer_id=transfer.id
        AND match.status IN ('vorgeschlagen','bestaetigt','vertagt')
       WHERE payment_type.code='KREDITKARTE'
         AND transfer.buchungsdatum=ANY($1::date[])
         AND transfer.deleted_at IS NULL
       ORDER BY transfer.buchungsdatum, transfer.created_at
       FOR UPDATE OF transfer, match`,
      [bookingDates]
    );
    const foundDates = new Set(targets.rows.map((row) => berlinDateOnly(row.buchungsdatum)));
    if (foundDates.size !== bookingDates.length
        || bookingDates.some((date) => !foundDates.has(date))) {
      throw problem('Die Kreditkarten-Sammelüberweisungen vom 02.10. und 03.10. wurden nicht vollständig gefunden.', 409);
    }
    const transferIds = [...new Set(targets.rows.map((row) => row.transfer_id))];
    const matchIds = targets.rows.map((row) => row.match_id).filter(Boolean);

    const resetDays = await client.query(
      `UPDATE controlling.payment_days payment_day
       SET review_status='OFFEN', reviewed_by=NULL, reviewed_at=NULL,
           review_note=NULL, deferred_until=NULL, updated_at=now()
       WHERE payment_day.id IN (
         SELECT DISTINCT item.payment_day_id
         FROM controlling.payment_transfer_match_items item
         WHERE item.match_id=ANY($1::uuid[])
       )
       RETURNING payment_day.id`,
      [matchIds]
    );
    for (const day of resetDays.rows) {
      await client.query(
        `INSERT INTO controlling.payment_review_events
           (payment_day_id,event_type,actor_user_id,details)
         VALUES ($1,'RUECKGESETZT',$2,jsonb_build_object(
           'operation',$3::text,'reason',$4::text
         ))`,
        [day.id, actorUserId, OCTOBER_CREDIT_CARD_DELETION_OPERATION, deletionReason]
      );
    }

    if (matchIds.length) await client.query(
      `UPDATE controlling.payment_transfer_matches
       SET status='abgelehnt', reviewed_by=NULL, reviewed_at=NULL,
           review_note=$2, deferred_until=NULL, updated_at=now()
       WHERE id=ANY($1::uuid[])`,
      [matchIds, deletionReason]
    );
    for (const target of targets.rows.filter((row) => row.match_id)) {
      await client.query(
        `INSERT INTO controlling.payment_transfer_events
           (transfer_id,match_id,event_type,actor_user_id,details)
         VALUES ($1,$2,'RUECKGESETZT',$3,jsonb_build_object(
           'reason',$4::text,'previousMatchStatus',$5::text
         ))`,
        [target.transfer_id, target.match_id, actorUserId, deletionReason, target.match_status]
      );
    }
    const deleted = await client.query(
      `UPDATE controlling.payment_transfers
       SET status='abgelehnt', decided_by=NULL, decided_at=NULL,
           decision_note=NULL, deferred_until=NULL, deleted_at=now(),
           deleted_by=$2, deletion_reason=$3, updated_at=now()
       WHERE id=ANY($1::uuid[]) AND deleted_at IS NULL
       RETURNING id`,
      [transferIds, actorUserId, deletionReason]
    );
    await client.query(
      `INSERT INTO controlling.module2_maintenance_operations (operation_key,details)
       VALUES ($1,jsonb_build_object(
         'bookingDates',$2::jsonb,'deletedTransfers',$3::integer,
         'resetPaymentDays',$4::integer
       ))`,
      [OCTOBER_CREDIT_CARD_DELETION_OPERATION, JSON.stringify(bookingDates),
        deleted.rowCount, resetDays.rowCount]
    );
    await client.query('COMMIT');
    return {
      status: 'APPLIED',
      deletedTransfers: deleted.rowCount,
      resetPaymentDays: resetDays.rowCount,
    };
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
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
  providerForPaymentType,
  normalizeReviewDecision,
  normalizeTransferDecision,
  moneyToCents,
  findBestMatch,
  synchronizeQlikPaymentDays,
  HISTORICAL_BACKFILL_OPERATION,
  backfillHistoricalQlikPaymentDays,
  OCTOBER_CREDIT_CARD_DELETION_OPERATION,
  deleteOctoberCreditCardTransfers,
};
