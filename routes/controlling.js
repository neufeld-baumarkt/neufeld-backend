const express = require('express');
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');
const {
  parseMonth,
  parseYear,
  normalizePaymentType,
  normalizeProvider,
  normalizeReviewDecision,
  normalizeTransferDecision,
  moneyToCents,
  findBestMatch,
  synchronizeQlikPaymentDays,
  berlinDateOnly,
} = require('../lib/alexModule2');

const router = express.Router();

const ALLOWED_ROLES = new Set(['Admin', 'Supervisor', 'Geschäftsführer']);
const EC_CASH_PAYMENT_TYPES = ['EC_CASH', 'KREDITKARTE'];
const FILIALEN = ['Telgte', 'Ahaus', 'Vreden', 'Münster'];

function hasAccess(req) {
  return req.user && ALLOWED_ROLES.has(req.user.role);
}

function toNumberOrNull(value) {
  if (value === undefined || value === null || value === '') return null;
  const numberValue = Number(value);
  return Number.isFinite(numberValue) ? numberValue : null;
}

function formatDateOnly(value) {
  if (!value) return null;
  if (typeof value === 'string') return value.slice(0, 10);
  return value.toISOString().slice(0, 10);
}

function mapPaymentDay(row) {
  if (!row) return null;

  return {
    ...row,
    datum: formatDateOnly(row.datum),
  };
}

function mapPaymentTransfer(row) {
  if (!row) return null;

  return {
    ...row,
    buchungsdatum: formatDateOnly(row.buchungsdatum),
  };
}

function parsePositiveInteger(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function getDaysInMonth(jahr, monat) {
  return new Date(jahr, monat, 0).getDate();
}

function buildDateString(jahr, monat, tag) {
  return `${String(jahr).padStart(4, '0')}-${String(monat).padStart(2, '0')}-${String(tag).padStart(2, '0')}`;
}

function createEmptyFilialeData() {
  return {
    id: null,
    sollBetrag: null,
    istBetrag: null,
    status: 'offen',
    bemerkung: null,
    updatedAt: null,
  };
}

function createEmptyDay(jahr, monat, tag) {
  const datum = buildDateString(jahr, monat, tag);
  const filialen = {};

  FILIALEN.forEach((filiale) => {
    filialen[filiale] = createEmptyFilialeData();
  });

  return {
    datum,
    tag,
    filialen,
    gesamtUeberweisung: {
      betrag: null,
      transfers: [],
    },
  };
}

function createEmptyPaymentTypeBlock(paymentType, jahr, monat) {
  const daysInMonth = getDaysInMonth(jahr, monat);
  const tage = [];

  for (let tag = 1; tag <= daysInMonth; tag += 1) {
    tage.push(createEmptyDay(jahr, monat, tag));
  }

  return {
    paymentType,
    tage,
  };
}

function buildEcCashMonthResponse({ jahr, monat, paymentDays, paymentTransfers }) {
  const blocks = {};

  EC_CASH_PAYMENT_TYPES.forEach((paymentType) => {
    blocks[paymentType] = createEmptyPaymentTypeBlock(paymentType, jahr, monat);
  });

  paymentDays.forEach((row) => {
    const paymentType = row.code;
    const tag = Number(row.tag);
    const filiale = row.filiale;

    if (!blocks[paymentType]) return;
    if (!FILIALEN.includes(filiale)) return;
    if (!Number.isInteger(tag) || tag < 1) return;

    const day = blocks[paymentType].tage[tag - 1];
    if (!day) return;

    day.filialen[filiale] = {
      id: row.id,
      sollBetrag: toNumberOrNull(row.soll_betrag),
      istBetrag: toNumberOrNull(row.ist_betrag),
      status: row.status || 'offen',
      bemerkung: row.bemerkung || null,
      updatedAt: row.updated_at || null,
    };
  });

  paymentTransfers.forEach((row) => {
    const paymentType = row.code;
    const buchungsdatum = formatDateOnly(row.buchungsdatum);
    const tag = Number(row.tag);

    if (!blocks[paymentType]) return;
    if (!Number.isInteger(tag) || tag < 1) return;

    const day = blocks[paymentType].tage[tag - 1];
    if (!day) return;

    const transfer = {
      id: row.id,
      provider: row.provider || 'UNBEKANNT',
      filiale: row.filiale,
      buchungsdatum,
      betrag: toNumberOrNull(row.betrag),
      referenz: row.referenz || null,
      status: row.status || 'offen',
      createdAt: row.created_at || null,
    };

    if (transfer.provider === 'CLASSIC') {
      day.gesamtUeberweisung.betrag = transfer.betrag;
      day.gesamtUeberweisung.classicTransferId = transfer.id;
      return;
    }

    day.gesamtUeberweisung.transfers.push(transfer);

    if (!day.gesamtUeberweisung.classicTransferId) {
      const currentTotal = toNumberOrNull(day.gesamtUeberweisung.betrag) || 0;
      day.gesamtUeberweisung.betrag = currentTotal + (transfer.betrag || 0);
    }
  });

  return {
    ok: true,
    jahr,
    monat,
    filialen: FILIALEN,
    paymentTypes: EC_CASH_PAYMENT_TYPES,
    ecCash: blocks.EC_CASH,
    kreditkarte: blocks.KREDITKARTE,
  };
}

router.get('/payment-types', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) {
    return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  }

  try {
    const result = await pool.query(`
      SELECT id, code, bezeichnung, aktiv, sortierung
      FROM controlling.payment_types
      WHERE aktiv = true
      ORDER BY sortierung ASC, id ASC
    `);

    return res.json({
      ok: true,
      count: result.rowCount,
      paymentTypes: result.rows,
    });
  } catch (err) {
    console.error('[CONTROLLING_PAYMENT_TYPES_ERROR]', err);
    return res.status(500).json({
      ok: false,
      message: 'Zahlungsarten konnten nicht geladen werden.',
      error: err.message,
      code: err.code || null,
    });
  }
});

router.get('/ec-cash/month', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) {
    return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  }

  const jahr = parsePositiveInteger(req.query.jahr);
  const monat = parsePositiveInteger(req.query.monat);

  if (!jahr || !monat || monat < 1 || monat > 12) {
    return res.status(400).json({
      ok: false,
      message: 'jahr und monat sind erforderlich. monat muss zwischen 1 und 12 liegen.',
    });
  }

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await synchronizeQlikPaymentDays(client, { jahr, monat });
    const paymentDaysResult = await client.query(
      `
      SELECT
        pd.id,
        pd.datum,
        pd.jahr,
        pd.monat,
        pd.tag,
        pd.filiale,
        pt.code,
        pt.bezeichnung,
        pd.soll_betrag,
        pd.ist_betrag,
        pd.status,
        pd.bemerkung,
        pd.updated_at
      FROM controlling.payment_days pd
      INNER JOIN controlling.payment_types pt
        ON pt.id = pd.payment_type_id
      WHERE pd.jahr = $1
        AND pd.monat = $2
        AND pt.code = ANY($3::text[])
      ORDER BY pd.datum ASC, pt.sortierung ASC, pd.filiale ASC
      `,
      [jahr, monat, EC_CASH_PAYMENT_TYPES]
    );

    const paymentTransfersResult = await client.query(
      `
      SELECT
        ptf.id,
        ptf.payment_type_id,
        pt.code,
        pt.bezeichnung,
        ptf.filiale,
        ptf.buchungsdatum,
        EXTRACT(DAY FROM ptf.buchungsdatum)::integer AS tag,
        ptf.betrag,
        ptf.referenz,
        ptf.status,
        ptf.provider,
        ptf.created_at
      FROM controlling.payment_transfers ptf
      INNER JOIN controlling.payment_types pt
        ON pt.id = ptf.payment_type_id
      WHERE EXTRACT(YEAR FROM ptf.buchungsdatum)::integer = $1
        AND EXTRACT(MONTH FROM ptf.buchungsdatum)::integer = $2
        AND pt.code = ANY($3::text[])
      ORDER BY ptf.buchungsdatum ASC, pt.sortierung ASC, ptf.filiale ASC, ptf.created_at ASC
      `,
      [jahr, monat, EC_CASH_PAYMENT_TYPES]
    );

    await client.query('COMMIT');
    return res.json(
      buildEcCashMonthResponse({
        jahr,
        monat,
        paymentDays: paymentDaysResult.rows.map(mapPaymentDay),
        paymentTransfers: paymentTransfersResult.rows.map(mapPaymentTransfer),
      })
    );
  } catch (err) {
    if (client) await client.query('ROLLBACK').catch(() => {});
    console.error('[CONTROLLING_EC_CASH_MONTH_ERROR]', err);
    return res.status(500).json({
      ok: false,
      message: 'EC-Cash-Monatsdaten konnten nicht geladen werden.',
      error: err.message,
      code: err.code || null,
    });
  } finally {
    if (client) client.release();
  }
});

router.get('/payment-days', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) {
    return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  }

  const { jahr, monat, paymentType } = req.query;

  if (!jahr || !monat || !paymentType) {
    return res.status(400).json({
      ok: false,
      message: 'jahr, monat und paymentType sind erforderlich.',
    });
  }

  try {
    const result = await pool.query(
      `
      SELECT
        pd.id,
        pd.datum,
        pd.jahr,
        pd.monat,
        pd.tag,
        pd.filiale,
        pt.code,
        pt.bezeichnung,
        pd.soll_betrag,
        pd.ist_betrag,
        pd.status,
        pd.bemerkung,
        pd.updated_at
      FROM controlling.payment_days pd
      INNER JOIN controlling.payment_types pt
        ON pt.id = pd.payment_type_id
      WHERE pd.jahr = $1
        AND pd.monat = $2
        AND pt.code = $3
      ORDER BY pd.datum ASC, pt.sortierung ASC, pd.filiale ASC
      `,
      [Number(jahr), Number(monat), paymentType]
    );

    return res.json({
      ok: true,
      jahr: Number(jahr),
      monat: Number(monat),
      paymentType,
      count: result.rowCount,
      rows: result.rows.map(mapPaymentDay),
    });
  } catch (err) {
    console.error('[CONTROLLING_PAYMENT_DAYS_ERROR]', err);
    return res.status(500).json({
      ok: false,
      message: 'Tagesdaten konnten nicht geladen werden.',
      error: err.message,
      code: err.code || null,
    });
  }
});

router.post('/payment-days', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) {
    return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  }

  const {
    paymentType,
    filiale,
    datum,
    sollBetrag,
    istBetrag,
    status,
    bemerkung,
  } = req.body || {};

  if (!paymentType || !filiale || !datum) {
    return res.status(400).json({
      ok: false,
      message: 'paymentType, filiale und datum sind erforderlich.',
    });
  }

  const parsedDate = new Date(`${datum}T00:00:00`);
  if (Number.isNaN(parsedDate.getTime())) {
    return res.status(400).json({
      ok: false,
      message: 'datum ist ungültig. Erwartet wird YYYY-MM-DD.',
    });
  }

  const jahr = parsedDate.getFullYear();
  const monat = parsedDate.getMonth() + 1;
  const tag = parsedDate.getDate();

  const soll = toNumberOrNull(sollBetrag);
  const ist = toNumberOrNull(istBetrag);

  try {
    const result = await pool.query(
      `
      INSERT INTO controlling.payment_days (
        payment_type_id,
        filiale,
        datum,
        jahr,
        monat,
        tag,
        soll_betrag,
        ist_betrag,
        status,
        bemerkung,
        created_by,
        updated_at
      )
      SELECT
        pt.id,
        $2,
        $3::date,
        $4,
        $5,
        $6,
        $7,
        $8,
        COALESCE($9, 'offen'),
        $10,
        $11,
        now()
      FROM controlling.payment_types pt
      WHERE pt.code = $1
      ON CONFLICT (payment_type_id, filiale, datum)
      DO UPDATE SET
        soll_betrag = EXCLUDED.soll_betrag,
        ist_betrag = EXCLUDED.ist_betrag,
        status = EXCLUDED.status,
        bemerkung = EXCLUDED.bemerkung,
        updated_at = now()
      RETURNING *
      `,
      [
        paymentType,
        filiale,
        datum,
        jahr,
        monat,
        tag,
        soll,
        ist,
        status || 'offen',
        bemerkung || null,
        null,
      ]
    );

    if (result.rowCount === 0) {
      return res.status(400).json({
        ok: false,
        message: 'paymentType ist unbekannt.',
      });
    }

    return res.status(201).json({
      ok: true,
      paymentDay: mapPaymentDay(result.rows[0]),
    });
  } catch (err) {
    console.error('[CONTROLLING_PAYMENT_DAY_CREATE_ERROR]', err);
    return res.status(500).json({
      ok: false,
      message: 'Tageswert konnte nicht gespeichert werden.',
      error: err.message,
      code: err.code || null,
    });
  }
});

router.put('/payment-days/:id', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) {
    return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  }

  const { id } = req.params;
  const { sollBetrag, istBetrag, status, bemerkung } = req.body || {};

  try {
    const result = await pool.query(
      `
      UPDATE controlling.payment_days
      SET
        soll_betrag = $2,
        ist_betrag = $3,
        status = COALESCE($4, status),
        bemerkung = $5,
        updated_at = now()
      WHERE id = $1
      RETURNING *
      `,
      [
        id,
        toNumberOrNull(sollBetrag),
        toNumberOrNull(istBetrag),
        status || null,
        bemerkung || null,
      ]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({
        ok: false,
        message: 'Tageswert wurde nicht gefunden.',
      });
    }

    return res.json({
      ok: true,
      paymentDay: mapPaymentDay(result.rows[0]),
    });
  } catch (err) {
    console.error('[CONTROLLING_PAYMENT_DAY_UPDATE_ERROR]', err);
    return res.status(500).json({
      ok: false,
      message: 'Tageswert konnte nicht aktualisiert werden.',
      error: err.message,
      code: err.code || null,
    });
  }
});

router.delete('/payment-days/:id', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) {
    return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  }

  const { id } = req.params;

  try {
    const result = await pool.query(
      `
      DELETE FROM controlling.payment_days
      WHERE id = $1
      RETURNING *
      `,
      [id]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({
        ok: false,
        message: 'Tageswert wurde nicht gefunden.',
      });
    }

    return res.json({
      ok: true,
      deletedPaymentDay: mapPaymentDay(result.rows[0]),
    });
  } catch (err) {
    console.error('[CONTROLLING_PAYMENT_DAY_DELETE_ERROR]', err);
    return res.status(500).json({
      ok: false,
      message: 'Tageswert konnte nicht gelöscht werden.',
      error: err.message,
      code: err.code || null,
    });
  }
});

function routeError(res, error, fallback) {
  const status = Number(error?.statusCode) || 500;
  if (status >= 500) console.error('[ALEX_MODULE2_V2_ERROR]', error);
  return res.status(status).json({ ok: false, message: status >= 500 ? fallback : error.message });
}

function dateOnly(value) {
  return berlinDateOnly(value);
}

function reviewPrecedence(statuses) {
  const order = ['IN_PRUEFUNG', 'OFFEN', 'VERTAGT', 'KORRIGIERT', 'BESTAETIGT', 'EXTERN_BESTAETIGT'];
  return order.find((status) => statuses.includes(status)) || 'OFFEN';
}

function comparisonPrecedence(statuses) {
  if (statuses.includes('ABWEICHEND')) return 'ABWEICHEND';
  if (statuses.includes('ZEITVERSATZ_MOEGLICH')) return 'ZEITVERSATZ_MOEGLICH';
  if (statuses.length && statuses.every((status) => status === 'UEBEREINSTIMMEND')) return 'UEBEREINSTIMMEND';
  return 'NICHT_VERGLICHEN';
}

router.get('/v2/overview', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  let client;
  try {
    const jahr = parseYear(req.query.jahr);
    const monat = parseMonth(req.query.monat, 'monat');
    client = await pool.connect();
    await client.query('BEGIN');
    const sync = await synchronizeQlikPaymentDays(client, { jahr, monat });
    const [daysResult, unresolvedResult, importResult] = await Promise.all([
      client.query(
        `SELECT pd.id, pd.datum, pd.filiale, pt.code AS payment_type,
                pd.qlik_amount_cents, pd.ist_betrag, pd.availability_status,
                pd.comparison_status, pd.review_status, pd.review_note,
                pd.deferred_until, pd.reviewed_at, u.name AS reviewed_by
         FROM controlling.payment_days pd
         JOIN controlling.payment_types pt ON pt.id=pd.payment_type_id
         LEFT JOIN public.users u ON u.id=pd.reviewed_by
         WHERE pd.jahr=$1 AND pd.monat=$2 AND pt.code=ANY($3::text[])
         ORDER BY pd.datum DESC, pt.sortierung, pd.filiale`,
        [jahr, monat, EC_CASH_PAYMENT_TYPES]
      ),
      client.query(
        `WITH ranked AS (
           SELECT metric.metric_code, derived.availability_status,
                  row_number() OVER (
                    PARTITION BY derived.metric_definition_id, derived.branch_id, derived.business_date
                    ORDER BY derived.calculated_at DESC, derived.id DESC
                  ) AS rank
           FROM controlling.qlik_derived_daily_values derived
           JOIN controlling.qlik_metric_definitions metric
             ON metric.id=derived.metric_definition_id
           JOIN controlling.qlik_metric_consumer_bindings binding
             ON binding.metric_definition_id=metric.id
            AND binding.status='APPROVED' AND binding.usage_type='VORBELEGUNG'
           JOIN controlling.qlik_consumers consumer
             ON consumer.id=binding.consumer_id AND consumer.consumer_code='ALEX_MODULE_2'
           WHERE EXTRACT(YEAR FROM derived.business_date)::integer=$1
             AND EXTRACT(MONTH FROM derived.business_date)::integer=$2
         )
         SELECT metric_code AS payment_type_original, count(*)::integer AS row_count
         FROM ranked WHERE rank=1 AND availability_status<>'VORHANDEN'
         GROUP BY metric_code ORDER BY metric_code`,
        [jahr, monat]
      ),
      client.query(
        `SELECT max(completed_at) AS completed_at, count(DISTINCT report_date)::integer AS imported_days
         FROM controlling.qlik_import_runs
         WHERE status='IMPORTED' AND EXTRACT(YEAR FROM report_date)::integer=$1
           AND EXTRACT(MONTH FROM report_date)::integer=$2`,
        [jahr, monat]
      ),
    ]);
    await client.query('COMMIT');

    const itemsByKey = new Map();
    for (const row of daysResult.rows) {
      const datum = dateOnly(row.datum);
      const key = `${datum}|${row.payment_type}`;
      if (!itemsByKey.has(key)) itemsByKey.set(key, { datum, paymentType: row.payment_type, branches: [] });
      itemsByKey.get(key).branches.push({
        id: row.id,
        filiale: row.filiale,
        qlikAmountCents: row.qlik_amount_cents === null ? null : Number(row.qlik_amount_cents),
        comparisonAmountCents: row.ist_betrag === null ? null : Math.round(Number(row.ist_betrag) * 100),
        availabilityStatus: row.availability_status,
        comparisonStatus: row.comparison_status,
        reviewStatus: row.review_status,
        reviewNote: row.review_note,
        deferredUntil: dateOnly(row.deferred_until),
        reviewedAt: row.reviewed_at,
        reviewedBy: row.reviewed_by,
      });
    }
    const items = [...itemsByKey.values()].map((item) => ({
      ...item,
      qlikTotalCents: item.branches.reduce((sum, branch) => sum + (branch.qlikAmountCents || 0), 0),
      availabilityStatus: item.branches.every((branch) => branch.availabilityStatus === 'VORHANDEN') ? 'VORHANDEN' : 'UNVOLLSTAENDIG',
      comparisonStatus: comparisonPrecedence(item.branches.map((branch) => branch.comparisonStatus)),
      reviewStatus: reviewPrecedence(item.branches.map((branch) => branch.reviewStatus)),
    }));
    const summary = {
      newCount: items.filter((item) => item.reviewStatus === 'OFFEN').length,
      deviationCount: items.filter((item) => item.comparisonStatus === 'ABWEICHEND').length,
      deferredCount: items.filter((item) => item.reviewStatus === 'VERTAGT').length,
      confirmedCount: items.filter((item) => ['BESTAETIGT', 'EXTERN_BESTAETIGT'].includes(item.reviewStatus)).length,
      unresolvedPaymentTypeCount: unresolvedResult.rows.length,
    };
    return res.json({ ok: true, jahr, monat, sync, summary, items,
      unresolvedPaymentTypes: unresolvedResult.rows,
      importState: importResult.rows[0] || { completed_at: null, imported_days: 0 } });
  } catch (error) {
    if (client) await client.query('ROLLBACK').catch(() => {});
    return routeError(res, error, 'Modul-2-Daten konnten nicht geladen werden.');
  } finally {
    client?.release();
  }
});

router.patch('/v2/reviews', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  let client;
  try {
    const paymentType = normalizePaymentType(req.body?.paymentType);
    const datum = String(req.body?.datum || '');
    if (!/^\d{4}-\d{2}-\d{2}$/.test(datum)) throw Object.assign(new Error('Geschäftstag ist ungültig.'), { statusCode: 400 });
    const decision = normalizeReviewDecision(req.body);
    client = await pool.connect();
    await client.query('BEGIN');
    const updated = await client.query(
      `UPDATE controlling.payment_days pd
       SET review_status=$3, review_note=$4, deferred_until=$5::date,
           reviewed_by=$6, reviewed_at=now(), updated_at=now()
       FROM controlling.payment_types pt
       WHERE pd.payment_type_id=pt.id AND pt.code=$1 AND pd.datum=$2::date
       RETURNING pd.id`,
      [paymentType, datum, decision.status, decision.note, decision.deferredUntil, req.user.id]
    );
    if (!updated.rowCount) throw Object.assign(new Error('Für diesen Tag wurden keine Qlik-Werte gefunden.'), { statusCode: 404 });
    for (const row of updated.rows) {
      await client.query(
        `INSERT INTO controlling.payment_review_events (payment_day_id,event_type,actor_user_id,details)
         VALUES ($1,$2,$3,jsonb_build_object('note',$4::text,'deferredUntil',$5::text))`,
        [row.id, decision.status, req.user.id, decision.note, decision.deferredUntil]
      );
    }
    await client.query('COMMIT');
    return res.json({ ok: true, updated: updated.rowCount, reviewStatus: decision.status });
  } catch (error) {
    if (client) await client.query('ROLLBACK').catch(() => {});
    return routeError(res, error, 'Prüfentscheidung konnte nicht gespeichert werden.');
  } finally {
    client?.release();
  }
});

router.put('/transfers/classic', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) {
    return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  }

  let client;

  try {
    const paymentType = normalizePaymentType(req.body?.paymentType);
    const amountCents = moneyToCents(req.body?.amount);
    const buchungsdatum = String(req.body?.datum || '');

    if (!/^\d{4}-\d{2}-\d{2}$/.test(buchungsdatum)) {
      throw Object.assign(new Error('Buchungsdatum ist ungültig.'), { statusCode: 400 });
    }

    client = await pool.connect();
    await client.query('BEGIN');
    const result = await client.query(
      `INSERT INTO controlling.payment_transfers
        (payment_type_id,filiale,buchungsdatum,betrag,referenz,status,provider,created_by)
       SELECT id,'Unternehmen',$2::date,$3::bigint/100.0,$4,'offen','CLASSIC',NULL
       FROM controlling.payment_types WHERE code=$1
       ON CONFLICT (payment_type_id,buchungsdatum) WHERE provider='CLASSIC'
       DO UPDATE SET betrag=EXCLUDED.betrag,status='offen',updated_at=now()
       RETURNING *, (xmax=0) AS inserted`,
      [paymentType, buchungsdatum, amountCents, `CLASSIC:${paymentType}:${buchungsdatum}`]
    );

    if (!result.rowCount) {
      throw Object.assign(new Error('Zahlungsart wurde nicht gefunden.'), { statusCode: 400 });
    }

    const transfer = result.rows[0];
    await client.query(
      `INSERT INTO controlling.payment_transfer_events
        (transfer_id,event_type,actor_user_id,details)
       VALUES ($1,$2,$3,jsonb_build_object('source','CLASSIC','amountCents',$4::bigint))`,
      [transfer.id, transfer.inserted ? 'ERFASST' : 'ANGEPASST', req.user.id, amountCents]
    );

    await client.query('COMMIT');
    return res.json({ ok: true, transfer: mapPaymentTransfer(transfer) });
  } catch (error) {
    if (client) await client.query('ROLLBACK').catch(() => {});
    return routeError(res, error, 'Gesamtüberweisung konnte nicht gespeichert werden.');
  } finally {
    client?.release();
  }
});

router.get('/v2/transfers', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  try {
    const jahr = parseYear(req.query.jahr);
    const monat = parseMonth(req.query.monat, 'monat');
    const result = await pool.query(
      `SELECT t.id, t.provider, t.buchungsdatum, t.betrag, t.referenz, t.status,
              t.decision_note, t.deferred_until, t.decided_at, pt.code AS payment_type,
              m.id AS match_id, m.status AS match_status, m.differenz,
              m.automatisch, m.algorithm_version, m.review_note,
              COALESCE(jsonb_agg(jsonb_build_object(
                'id', mi.id, 'paymentDayId', pd.id, 'datum', pd.datum,
                'filiale', pd.filiale, 'amountCents', round(mi.betrag*100)::bigint,
                'partial', mi.teilbetrag
              ) ORDER BY mi.reihenfolge) FILTER (WHERE mi.id IS NOT NULL), '[]'::jsonb) AS items
       FROM controlling.payment_transfers t
       JOIN controlling.payment_types pt ON pt.id=t.payment_type_id
       LEFT JOIN controlling.payment_transfer_matches m
         ON m.transfer_id=t.id AND m.status IN ('vorgeschlagen','bestaetigt','vertagt')
       LEFT JOIN controlling.payment_transfer_match_items mi ON mi.match_id=m.id
       LEFT JOIN controlling.payment_days pd ON pd.id=mi.payment_day_id
       WHERE t.provider <> 'CLASSIC'
         AND EXTRACT(YEAR FROM t.buchungsdatum)::integer=$1
         AND EXTRACT(MONTH FROM t.buchungsdatum)::integer=$2
       GROUP BY t.id, pt.code, m.id
       ORDER BY t.provider, t.buchungsdatum DESC, t.created_at DESC`,
      [jahr, monat]
    );
    return res.json({ ok: true, transfers: result.rows.map((row) => ({
      ...row,
      buchungsdatum: dateOnly(row.buchungsdatum),
      betragCents: Math.round(Number(row.betrag) * 100),
      differenzCents: row.differenz === null ? null : Math.round(Number(row.differenz) * 100),
      deferred_until: dateOnly(row.deferred_until),
    })) });
  } catch (error) {
    return routeError(res, error, 'Sammelüberweisungen konnten nicht geladen werden.');
  }
});

router.post('/v2/transfers', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  try {
    const provider = normalizeProvider(req.body?.provider);
    const paymentType = normalizePaymentType(req.body?.paymentType);
    const amountCents = moneyToCents(req.body?.amount);
    const buchungsdatum = String(req.body?.buchungsdatum || '');
    const referenz = String(req.body?.referenz || '').trim() || null;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(buchungsdatum)) throw Object.assign(new Error('Buchungsdatum ist ungültig.'), { statusCode: 400 });
    const result = await pool.query(
      `INSERT INTO controlling.payment_transfers
        (payment_type_id,filiale,buchungsdatum,betrag,referenz,status,provider,created_by)
       SELECT id,'Unternehmen',$3::date,$4::bigint/100.0,$5,'offen',$1,NULL
       FROM controlling.payment_types WHERE code=$2 RETURNING *`,
      [provider, paymentType, buchungsdatum, amountCents, referenz]
    );
    if (!result.rowCount) throw Object.assign(new Error('Zahlungsart wurde nicht gefunden.'), { statusCode: 400 });
    await pool.query(
      `INSERT INTO controlling.payment_transfer_events (transfer_id,event_type,actor_user_id,details)
       VALUES ($1,'ERFASST',$2,'{}'::jsonb)`,
      [result.rows[0].id, req.user.id]
    );
    return res.status(201).json({ ok: true, transfer: result.rows[0] });
  } catch (error) {
    return routeError(res, error, 'Sammelüberweisung konnte nicht gespeichert werden.');
  }
});

router.post('/v2/transfers/:id/suggest', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    const transferResult = await client.query(
      `SELECT t.*, pt.code AS payment_type, round(t.betrag*100)::bigint AS amount_cents
       FROM controlling.payment_transfers t JOIN controlling.payment_types pt ON pt.id=t.payment_type_id
       WHERE t.id=$1 FOR UPDATE`,
      [req.params.id]
    );
    if (!transferResult.rowCount) throw Object.assign(new Error('Sammelüberweisung wurde nicht gefunden.'), { statusCode: 404 });
    const transfer = transferResult.rows[0];
    const candidatesResult = await client.query(
      `SELECT pd.id, pd.datum, pd.filiale, pd.qlik_amount_cents
       FROM controlling.payment_days pd
       WHERE pd.payment_type_id=$1 AND pd.qlik_amount_cents>0
         AND pd.datum BETWEEN $2::date - INTERVAL '21 days' AND $2::date
         AND NOT EXISTS (
           SELECT 1 FROM controlling.payment_transfer_match_items used
           JOIN controlling.payment_transfer_matches confirmed ON confirmed.id=used.match_id
           WHERE used.payment_day_id=pd.id AND confirmed.status='bestaetigt'
         )
       ORDER BY pd.datum, pd.filiale`,
      [transfer.payment_type_id, dateOnly(transfer.buchungsdatum)]
    );
    const suggestion = findBestMatch(candidatesResult.rows.map((row) => ({
      id: row.id, datum: dateOnly(row.datum), amountCents: Number(row.qlik_amount_cents), filiale: row.filiale,
    })), Number(transfer.amount_cents));
    if (!suggestion.itemIds.length) throw Object.assign(new Error('Keine geeigneten Tageswerte für einen Vorschlag gefunden.'), { statusCode: 409 });
    await client.query(
      `UPDATE controlling.payment_transfer_matches SET status='abgelehnt',updated_at=now(),review_note='Durch neuen Vorschlag ersetzt.'
       WHERE transfer_id=$1 AND status IN ('vorgeschlagen','vertagt')`,
      [transfer.id]
    );
    const matchResult = await client.query(
      `INSERT INTO controlling.payment_transfer_matches (transfer_id,status,differenz,automatisch,algorithm_version)
       VALUES ($1,'vorgeschlagen',$2::bigint/100.0,true,'subset-cents-v1') RETURNING *`,
      [transfer.id, suggestion.differenceCents]
    );
    const match = matchResult.rows[0];
    let order = 0;
    for (const itemId of suggestion.itemIds) {
      const candidate = candidatesResult.rows.find((row) => row.id === itemId);
      await client.query(
        `INSERT INTO controlling.payment_transfer_match_items (match_id,payment_day_id,betrag,teilbetrag,reihenfolge)
         VALUES ($1,$2,$3::bigint/100.0,false,$4)`,
        [match.id, itemId, Number(candidate.qlik_amount_cents), order]
      );
      order += 1;
    }
    await client.query("UPDATE controlling.payment_transfers SET status='vorgeschlagen',updated_at=now() WHERE id=$1", [transfer.id]);
    await client.query(
      `INSERT INTO controlling.payment_transfer_events (transfer_id,match_id,event_type,details)
       VALUES ($1,$2,'VORGESCHLAGEN',jsonb_build_object('exact',$3::boolean,'differenceCents',$4::bigint))`,
      [transfer.id, match.id, suggestion.exact, suggestion.differenceCents]
    );
    await client.query('COMMIT');
    return res.status(201).json({ ok: true, match: { ...match, suggestion } });
  } catch (error) {
    if (client) await client.query('ROLLBACK').catch(() => {});
    return routeError(res, error, 'Match-Vorschlag konnte nicht erzeugt werden.');
  } finally {
    client?.release();
  }
});

router.get('/v2/transfers/:id/candidates', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  try {
    const result = await pool.query(
      `SELECT pd.id, pd.datum, pd.filiale, pd.qlik_amount_cents,
              EXISTS (
                SELECT 1 FROM controlling.payment_transfer_match_items mi
                JOIN controlling.payment_transfer_matches m ON m.id=mi.match_id
                WHERE m.transfer_id=t.id AND m.status IN ('vorgeschlagen','vertagt') AND mi.payment_day_id=pd.id
              ) AS selected
       FROM controlling.payment_transfers t
       JOIN controlling.payment_days pd ON pd.payment_type_id=t.payment_type_id
         AND pd.datum BETWEEN t.buchungsdatum - INTERVAL '21 days' AND t.buchungsdatum
         AND pd.qlik_amount_cents>0
       WHERE t.id=$1 AND NOT EXISTS (
         SELECT 1 FROM controlling.payment_transfer_match_items used
         JOIN controlling.payment_transfer_matches confirmed ON confirmed.id=used.match_id
         WHERE used.payment_day_id=pd.id AND confirmed.status='bestaetigt' AND confirmed.transfer_id<>t.id
       )
       ORDER BY pd.datum,pd.filiale`,
      [req.params.id]
    );
    return res.json({ ok: true, candidates: result.rows.map((row) => ({
      id: row.id,
      datum: dateOnly(row.datum),
      filiale: row.filiale,
      amountCents: Number(row.qlik_amount_cents),
      selected: row.selected,
    })) });
  } catch (error) {
    return routeError(res, error, 'Match-Kandidaten konnten nicht geladen werden.');
  }
});

router.put('/v2/transfers/:id/match', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  let client;
  try {
    const requestedItems = Array.isArray(req.body?.items) ? req.body.items : [];
    if (!requestedItems.length || requestedItems.length > 60) throw Object.assign(new Error('Mindestens ein Tageswert ist erforderlich.'), { statusCode: 400 });
    client = await pool.connect();
    await client.query('BEGIN');
    const transferResult = await client.query(
      `SELECT t.*, round(t.betrag*100)::bigint AS amount_cents
       FROM controlling.payment_transfers t WHERE id=$1 FOR UPDATE`,
      [req.params.id]
    );
    if (!transferResult.rowCount) throw Object.assign(new Error('Sammelüberweisung wurde nicht gefunden.'), { statusCode: 404 });
    const matchResult = await client.query(
      `SELECT * FROM controlling.payment_transfer_matches
       WHERE transfer_id=$1 AND status IN ('vorgeschlagen','vertagt') FOR UPDATE`,
      [req.params.id]
    );
    if (!matchResult.rowCount) throw Object.assign(new Error('Es gibt keinen anpassbaren Vorschlag.'), { statusCode: 409 });
    const match = matchResult.rows[0];
    const ids = requestedItems.map((item) => String(item.paymentDayId || ''));
    const days = await client.query(
      `SELECT id, qlik_amount_cents FROM controlling.payment_days
       WHERE id=ANY($1::uuid[]) AND payment_type_id=$2`,
      [ids, transferResult.rows[0].payment_type_id]
    );
    if (days.rowCount !== ids.length) throw Object.assign(new Error('Mindestens ein Tageswert ist ungültig.'), { statusCode: 400 });
    await client.query('DELETE FROM controlling.payment_transfer_match_items WHERE match_id=$1', [match.id]);
    let totalCents = 0;
    for (let index = 0; index < requestedItems.length; index += 1) {
      const item = requestedItems[index];
      const day = days.rows.find((row) => row.id === item.paymentDayId);
      const amountCents = item.amountCents === undefined ? Number(day.qlik_amount_cents) : Number(item.amountCents);
      if (!Number.isSafeInteger(amountCents) || amountCents <= 0 || amountCents > Number(day.qlik_amount_cents)) {
        throw Object.assign(new Error('Teilbetrag ist ungültig.'), { statusCode: 400 });
      }
      totalCents += amountCents;
      await client.query(
        `INSERT INTO controlling.payment_transfer_match_items (match_id,payment_day_id,betrag,teilbetrag,reihenfolge)
         VALUES ($1,$2,$3::bigint/100.0,$4,$5)`,
        [match.id, item.paymentDayId, amountCents, amountCents !== Number(day.qlik_amount_cents), index]
      );
    }
    const differenceCents = Number(transferResult.rows[0].amount_cents) - totalCents;
    await client.query(
      `UPDATE controlling.payment_transfer_matches SET differenz=$2::bigint/100.0,automatisch=false,updated_at=now() WHERE id=$1`,
      [match.id, differenceCents]
    );
    await client.query(
      `INSERT INTO controlling.payment_transfer_events (transfer_id,match_id,event_type,actor_user_id,details)
       VALUES ($1,$2,'ANGEPASST',$3,jsonb_build_object('differenceCents',$4::bigint))`,
      [req.params.id, match.id, req.user.id, differenceCents]
    );
    await client.query('COMMIT');
    return res.json({ ok: true, differenceCents, automatic: false });
  } catch (error) {
    if (client) await client.query('ROLLBACK').catch(() => {});
    return routeError(res, error, 'Match-Vorschlag konnte nicht angepasst werden.');
  } finally {
    client?.release();
  }
});

router.post('/v2/transfers/:id/decision', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  let client;
  try {
    const decision = normalizeTransferDecision(req.body);
    client = await pool.connect();
    await client.query('BEGIN');
    const result = await client.query(
      `SELECT t.id AS transfer_id, m.id AS match_id, m.differenz
       FROM controlling.payment_transfers t
       JOIN controlling.payment_transfer_matches m ON m.transfer_id=t.id
       WHERE t.id=$1 AND m.status IN ('vorgeschlagen','vertagt') FOR UPDATE OF t,m`,
      [req.params.id]
    );
    if (!result.rowCount) throw Object.assign(new Error('Offener Match-Vorschlag wurde nicht gefunden.'), { statusCode: 404 });
    const current = result.rows[0];
    if (decision.action === 'confirm' && Number(current.differenz) !== 0 && !String(req.body?.note || '').trim()) {
      throw Object.assign(new Error('Eine bestätigte Differenz benötigt eine Begründung.'), { statusCode: 400 });
    }
    const matchStatus = decision.action === 'confirm' ? 'bestaetigt' : decision.action === 'reject' ? 'abgelehnt' : 'vertagt';
    const eventType = decision.action === 'confirm' ? 'BESTAETIGT' : decision.action === 'reject' ? 'ABGELEHNT' : 'VERTAGT';
    await client.query(
      `UPDATE controlling.payment_transfer_matches
       SET status=$2,reviewed_by=$3,reviewed_at=now(),review_note=$4,deferred_until=$5::date,updated_at=now()
       WHERE id=$1`,
      [current.match_id, matchStatus, req.user.id, decision.note || String(req.body?.note || '').trim() || null, decision.deferredUntil]
    );
    await client.query(
      `UPDATE controlling.payment_transfers
       SET status=$2,decided_by=$3,decided_at=now(),decision_note=$4,deferred_until=$5::date,updated_at=now()
       WHERE id=$1`,
      [current.transfer_id, matchStatus, req.user.id, decision.note || String(req.body?.note || '').trim() || null, decision.deferredUntil]
    );
    if (decision.action === 'confirm') {
      const days = await client.query(
        `UPDATE controlling.payment_days pd SET review_status='EXTERN_BESTAETIGT',reviewed_by=$2,reviewed_at=now(),updated_at=now()
         FROM controlling.payment_transfer_match_items mi
         WHERE mi.match_id=$1 AND mi.payment_day_id=pd.id RETURNING pd.id`,
        [current.match_id, req.user.id]
      );
      for (const day of days.rows) {
        await client.query(
          `INSERT INTO controlling.payment_review_events (payment_day_id,event_type,actor_user_id,details)
           VALUES ($1,'EXTERN_BESTAETIGT',$2,jsonb_build_object('matchId',$3::uuid))`,
          [day.id, req.user.id, current.match_id]
        );
      }
    }
    await client.query(
      `INSERT INTO controlling.payment_transfer_events (transfer_id,match_id,event_type,actor_user_id,details)
       VALUES ($1,$2,$3,$4,jsonb_build_object('note',$5::text,'deferredUntil',$6::text))`,
      [current.transfer_id, current.match_id, eventType, req.user.id, decision.note, decision.deferredUntil]
    );
    await client.query('COMMIT');
    return res.json({ ok: true, status: matchStatus });
  } catch (error) {
    if (client) await client.query('ROLLBACK').catch(() => {});
    return routeError(res, error, 'Match-Entscheidung konnte nicht gespeichert werden.');
  } finally {
    client?.release();
  }
});

router.get('/v2/mappings', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  try {
    const result = await pool.query(
      `SELECT m.*, u.name AS approved_by_name
       FROM controlling.qlik_payment_type_mappings m
       JOIN public.users u ON u.id=m.approved_by
       ORDER BY m.payment_type_original,m.version DESC`
    );
    return res.json({ ok: true, mappings: result.rows });
  } catch (error) {
    return routeError(res, error, 'Qlik-Zuordnungen konnten nicht geladen werden.');
  }
});

router.post('/v2/mappings', verifyToken(), async (req, res) => {
  if (!hasAccess(req) || req.user.role !== 'Admin') return res.status(403).json({ ok: false, message: 'Nur Admin darf Qlik-Zuordnungen freigeben.' });
  let client;
  try {
    const original = String(req.body?.paymentTypeOriginal || '').trim();
    const target = normalizePaymentType(req.body?.targetPaymentType);
    const multiplier = Number(req.body?.amountMultiplier);
    const evidence = String(req.body?.evidenceNote || '').trim();
    if (!original || original.length > 200) throw Object.assign(new Error('Original-Zahlart ist ungültig.'), { statusCode: 400 });
    if (![1, -1].includes(multiplier)) throw Object.assign(new Error('Vorzeichenregel ist ungültig.'), { statusCode: 400 });
    if (evidence.length < 8 || evidence.length > 1000) throw Object.assign(new Error('Ein belastbarer Freigabenachweis ist erforderlich.'), { statusCode: 400 });
    client = await pool.connect();
    await client.query('BEGIN');
    const versionResult = await client.query(
      `SELECT COALESCE(max(version),0)+1 AS version
       FROM controlling.qlik_payment_type_mappings WHERE lower(payment_type_original)=lower($1)`,
      [original]
    );
    await client.query(
      `UPDATE controlling.qlik_payment_type_mappings SET active=false
       WHERE lower(payment_type_original)=lower($1) AND active=true`,
      [original]
    );
    const inserted = await client.query(
      `INSERT INTO controlling.qlik_payment_type_mappings
        (payment_type_original,target_payment_type_code,amount_multiplier,version,evidence_note,approved_by)
       VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
      [original, target, multiplier, Number(versionResult.rows[0].version), evidence, req.user.id]
    );
    await client.query('COMMIT');
    return res.status(201).json({ ok: true, mapping: inserted.rows[0] });
  } catch (error) {
    if (client) await client.query('ROLLBACK').catch(() => {});
    return routeError(res, error, 'Qlik-Zuordnung konnte nicht gespeichert werden.');
  } finally {
    client?.release();
  }
});

module.exports = router;
