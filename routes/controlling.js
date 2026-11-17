const express = require('express');
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');
const { streamAlexModule2Changes } = require('../lib/alexModule2LiveSync');
const {
  parseMonth,
  parseYear,
  normalizePaymentType,
  providerForPaymentType,
  normalizeReviewDecision,
  normalizeTransferDecision,
  moneyToCents,
  MAX_AUTOMATIC_MATCH_AMOUNT_CENTS,
  findBestMatch,
  isExactDifferenceCarry,
  berlinDateOnly,
  classicDayHasPositiveRevenue,
  filterPositiveRevenueItems,
} = require('../lib/alexModule2');

const router = express.Router();

const ALLOWED_ROLES = new Set(['Admin', 'Supervisor', 'Geschäftsführer']);
const EC_CASH_PAYMENT_TYPES = ['EC_CASH', 'KREDITKARTE'];
const FILIALEN = ['Telgte', 'Ahaus', 'Vreden', 'Münster'];

router.get('/sync/events', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  try {
    await streamAlexModule2Changes({ pool, req, res });
  } catch (error) {
    console.error('[ALEX_MODULE2_SYNC_ERROR]', error);
    if (!res.headersSent) return res.status(503).json({ ok: false, message: 'Live-Synchronisierung ist vorübergehend nicht verfügbar.' });
  }
});

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

function canonicalIstMatchJoin(paymentDayAlias = 'pd', resultAlias = 'canonical_ist') {
  if (!/^[a-z_][a-z0-9_]*$/i.test(paymentDayAlias) || !/^[a-z_][a-z0-9_]*$/i.test(resultAlias)) {
    throw new Error('Ungültiger SQL-Alias für den kanonischen Modul-2-Lesepfad.');
  }

  return `LEFT JOIN LATERAL (
    SELECT match_item.betrag AS ist_betrag, transfer_match.id AS match_id
    FROM controlling.payment_transfer_match_items match_item
    JOIN controlling.payment_transfer_matches transfer_match
      ON transfer_match.id=match_item.match_id
    WHERE match_item.payment_day_id=${paymentDayAlias}.id
      AND transfer_match.status='bestaetigt'
      AND (
        transfer_match.differenz=0
        OR EXISTS (
          SELECT 1
          FROM controlling.payment_review_events transfer_confirmation
          WHERE transfer_confirmation.payment_day_id=${paymentDayAlias}.id
            AND transfer_confirmation.event_type='EXTERN_BESTAETIGT'
        )
      )
    ORDER BY transfer_match.reviewed_at DESC NULLS LAST,
             transfer_match.updated_at DESC,
             transfer_match.id DESC
    LIMIT 1
  ) ${resultAlias} ON TRUE`;
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
      originalBetrag: null,
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
      reviewStatus: row.review_status || 'OFFEN',
      reviewedBy: row.reviewed_by || null,
      reviewedAt: row.reviewed_at || null,
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
      day.gesamtUeberweisung.originalBetrag = transfer.betrag;
      day.gesamtUeberweisung.classicTransferId = transfer.id;
      return;
    }

    day.gesamtUeberweisung.transfers.push(transfer);

    if (!day.gesamtUeberweisung.classicTransferId) {
      const currentTotal = toNumberOrNull(day.gesamtUeberweisung.betrag) || 0;
      day.gesamtUeberweisung.betrag = currentTotal + (transfer.betrag || 0);
      if (day.gesamtUeberweisung.transfers.length === 1) {
        day.gesamtUeberweisung.classicTransferId = transfer.id;
        day.gesamtUeberweisung.originalBetrag = transfer.betrag;
      } else {
        day.gesamtUeberweisung.classicTransferId = null;
        day.gesamtUeberweisung.originalBetrag = null;
      }
    }
  });

  Object.values(blocks).forEach((block) => {
    block.tage = block.tage.filter(classicDayHasPositiveRevenue);
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
        (pd.qlik_amount_cents::numeric / 100.0) AS soll_betrag,
        canonical_ist.ist_betrag,
        pd.review_status,
        pd.reviewed_at,
        reviewer.name AS reviewed_by,
        pd.status,
        pd.bemerkung,
        pd.updated_at
      FROM controlling.payment_days pd
      INNER JOIN controlling.payment_types pt
        ON pt.id = pd.payment_type_id
      ${canonicalIstMatchJoin('pd')}
      LEFT JOIN public.users reviewer
        ON reviewer.id = pd.reviewed_by
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
        AND ptf.deleted_at IS NULL
      ORDER BY ptf.buchungsdatum ASC, pt.sortierung ASC, ptf.filiale ASC, ptf.created_at ASC
      `,
      [jahr, monat, EC_CASH_PAYMENT_TYPES]
    );

    return res.json(
      buildEcCashMonthResponse({
        jahr,
        monat,
        paymentDays: paymentDaysResult.rows.map(mapPaymentDay),
        paymentTransfers: paymentTransfersResult.rows.map(mapPaymentTransfer),
      })
    );
  } catch (err) {
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
        (pd.qlik_amount_cents::numeric / 100.0) AS soll_betrag,
        canonical_ist.ist_betrag,
        pd.status,
        pd.bemerkung,
        pd.updated_at
      FROM controlling.payment_days pd
      INNER JOIN controlling.payment_types pt
        ON pt.id = pd.payment_type_id
      ${canonicalIstMatchJoin('pd')}
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

  return res.status(405).json({
    ok: false,
    message: 'Tageswerte werden ausschließlich aus Qlik synchronisiert.',
  });

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

  if (istBetrag !== undefined && istBetrag !== null && istBetrag !== '') {
    return res.status(400).json({
      ok: false,
      message: 'IST Match-Engine kann nicht manuell erfasst werden.',
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
        NULL,
        COALESCE($8, 'offen'),
        $9,
        $10,
        now()
      FROM controlling.payment_types pt
      WHERE pt.code = $1
      ON CONFLICT (payment_type_id, filiale, datum)
      DO UPDATE SET
        soll_betrag = EXCLUDED.soll_betrag,
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

  return res.status(405).json({
    ok: false,
    message: 'SOLL Qlik kann nicht manuell verändert werden.',
  });

  const { id } = req.params;
  const { sollBetrag, istBetrag, status, bemerkung } = req.body || {};

  if (istBetrag !== undefined && istBetrag !== null && istBetrag !== '') {
    return res.status(400).json({
      ok: false,
      message: 'IST Match-Engine kann nicht manuell geändert werden.',
    });
  }

  try {
    const result = await pool.query(
      `
      UPDATE controlling.payment_days
      SET
        soll_betrag = $2,
        status = COALESCE($3, status),
        bemerkung = $4,
        updated_at = now()
      WHERE id = $1
      RETURNING *
      `,
      [
        id,
        toNumberOrNull(sollBetrag),
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

async function confirmPaymentDayComponents(client, paymentDayIds, actorUserId, source) {
  const pending = await client.query(
    `UPDATE controlling.payment_day_component_reviews
     SET review_status='BESTAETIGT', reviewed_by=$2, reviewed_at=now(), updated_at=now()
     WHERE payment_day_id=ANY($1::uuid[])
       AND amount_cents>0
       AND review_status<>'BESTAETIGT'
     RETURNING id, payment_day_id, component_code, component_label, amount_cents`,
    [paymentDayIds, actorUserId]
  );

  for (const component of pending.rows) {
    await client.query(
      `INSERT INTO controlling.payment_day_component_review_events (
         component_review_id, event_type, actor_user_id, details
       ) VALUES ($1,'BESTAETIGT',$2,jsonb_build_object(
         'source',$3::text,'componentCode',$4::text,
         'componentLabel',$5::text,'amountCents',$6::bigint
       ))`,
      [component.id, actorUserId, source, component.component_code,
        component.component_label, component.amount_cents]
    );
  }

  return pending.rows;
}

router.patch('/payment-days/:id/review', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) {
    return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  }

  const { id } = req.params;
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(id)) {
    return res.status(400).json({ ok: false, message: 'Tageswert-ID ist ungültig.' });
  }

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    const result = await client.query(
      `UPDATE controlling.payment_days
       SET review_status='BESTAETIGT', review_note=NULL, deferred_until=NULL,
           reviewed_by=$2, reviewed_at=now(), updated_at=now()
       WHERE id=$1 AND qlik_amount_cents IS NOT NULL
       RETURNING id, review_status, reviewed_at`,
      [id, req.user.id]
    );

    if (!result.rowCount) {
      throw Object.assign(new Error('Qlik-Tageswert wurde nicht gefunden.'), { statusCode: 404 });
    }

    const confirmedComponents = await confirmPaymentDayComponents(
      client,
      [id],
      req.user.id,
      'CLASSIC_ONE_CLICK'
    );

    await client.query(
      `INSERT INTO controlling.payment_review_events
         (payment_day_id,event_type,actor_user_id,details)
       VALUES ($1,'BESTAETIGT',$2,jsonb_build_object(
         'source','CLASSIC_ONE_CLICK','confirmedComponents',$3::integer
       ))`,
      [id, req.user.id, confirmedComponents.length]
    );
    await client.query('COMMIT');

    return res.json({
      ok: true,
      paymentDayId: result.rows[0].id,
      reviewStatus: result.rows[0].review_status,
      reviewedAt: result.rows[0].reviewed_at,
    });
  } catch (error) {
    if (client) await client.query('ROLLBACK').catch(() => {});
    return routeError(res, error, 'Qlik-Tageswert konnte nicht bestätigt werden.');
  } finally {
    client?.release();
  }
});

router.delete('/payment-days/:id', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) {
    return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  }

  return res.status(405).json({
    ok: false,
    message: 'Qlik-Tageswerte können nicht manuell gelöscht werden.',
  });

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
    const [daysResult, componentResult, unresolvedResult, importResult] = await Promise.all([
      client.query(
        `SELECT pd.id, pd.datum, pd.filiale, pt.code AS payment_type,
                pd.qlik_amount_cents, canonical_ist.ist_betrag, pd.availability_status,
                pd.comparison_status, pd.review_status, pd.review_note,
                pd.deferred_until, pd.reviewed_at, u.name AS reviewed_by,
                (
                  pd.review_status='BESTAETIGT'
                  OR EXISTS (
                    SELECT 1 FROM controlling.payment_review_events review_event
                    WHERE review_event.payment_day_id=pd.id
                      AND review_event.event_type='BESTAETIGT'
                  )
                ) AS manually_confirmed,
                (canonical_ist.ist_betrag IS NOT NULL) AS transfer_confirmed
         FROM controlling.payment_days pd
         JOIN controlling.payment_types pt ON pt.id=pd.payment_type_id
         ${canonicalIstMatchJoin('pd')}
         LEFT JOIN public.users u ON u.id=pd.reviewed_by
         WHERE pd.jahr=$1 AND pd.monat=$2 AND pt.code=ANY($3::text[])
           AND EXISTS (
             SELECT 1
             FROM controlling.payment_days positive_day
             WHERE positive_day.payment_type_id=pd.payment_type_id
               AND positive_day.datum=pd.datum
               AND positive_day.qlik_amount_cents>0
           )
         ORDER BY pd.datum DESC, pt.sortierung, pd.filiale`,
        [jahr, monat, EC_CASH_PAYMENT_TYPES]
      ),
      client.query(
        `SELECT component.id, component.payment_day_id, component.component_code,
                component.component_label, component.amount_cents, component.review_status,
                component.reviewed_at, reviewer.name AS reviewed_by
         FROM controlling.payment_day_component_reviews component
         JOIN controlling.payment_days pd ON pd.id=component.payment_day_id
         LEFT JOIN public.users reviewer ON reviewer.id=component.reviewed_by
         WHERE pd.jahr=$1 AND pd.monat=$2
         ORDER BY component.payment_day_id, component.component_code`,
        [jahr, monat]
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
    const componentsByDay = new Map();
    for (const row of componentResult.rows) {
      if (!componentsByDay.has(row.payment_day_id)) componentsByDay.set(row.payment_day_id, []);
      componentsByDay.get(row.payment_day_id).push({
        id: row.id,
        code: row.component_code,
        label: row.component_label,
        amountCents: Number(row.amount_cents),
        reviewStatus: row.review_status,
        reviewedAt: row.reviewed_at,
        reviewedBy: row.reviewed_by,
        requiresReview: Number(row.amount_cents) > 0,
      });
    }
    const itemsByKey = new Map();
    for (const row of daysResult.rows) {
      const datum = dateOnly(row.datum);
      const key = `${datum}|${row.payment_type}`;
      if (!itemsByKey.has(key)) itemsByKey.set(key, { datum, paymentType: row.payment_type, branches: [] });
      const components = componentsByDay.get(row.id) || [];
      const requiredComponents = components.filter((component) => component.requiresReview);
      const confirmedComponents = requiredComponents.filter((component) => component.reviewStatus === 'BESTAETIGT');
      const comparisonAmountCents = row.ist_betrag === null ? null : Math.round(Number(row.ist_betrag) * 100);
      const effectiveReviewStatus = row.transfer_confirmed
        ? 'EXTERN_BESTAETIGT'
        : row.review_status === 'EXTERN_BESTAETIGT'
          ? (row.manually_confirmed ? 'BESTAETIGT' : 'OFFEN')
          : row.review_status;
      const effectiveComparisonStatus = comparisonAmountCents === null
        ? 'NICHT_VERGLICHEN'
        : comparisonAmountCents === Number(row.qlik_amount_cents)
          ? 'UEBEREINSTIMMEND'
          : 'ABWEICHEND';
      itemsByKey.get(key).branches.push({
        id: row.id,
        filiale: row.filiale,
        qlikAmountCents: row.qlik_amount_cents === null ? null : Number(row.qlik_amount_cents),
        comparisonAmountCents,
        availabilityStatus: row.availability_status,
        comparisonStatus: effectiveComparisonStatus,
        reviewStatus: effectiveReviewStatus,
        reviewNote: row.review_note,
        deferredUntil: dateOnly(row.deferred_until),
        reviewedAt: row.reviewed_at,
        reviewedBy: row.reviewed_by,
        confirmationStatuses: [
          ...(row.manually_confirmed ? ['BESTAETIGT'] : []),
          ...(row.transfer_confirmed ? ['EXTERN_BESTAETIGT'] : []),
        ],
        components,
        componentProgress: {
          confirmed: confirmedComponents.length,
          required: requiredComponents.length,
          complete: requiredComponents.length > 0 && confirmedComponents.length === requiredComponents.length,
        },
      });
    }
    const items = filterPositiveRevenueItems([...itemsByKey.values()].map((item) => {
      const relevantBranches = item.branches.filter((branch) => Number(branch.qlikAmountCents) > 0);
      const confirmationStatuses = [];
      if (relevantBranches.length > 0
        && relevantBranches.every((branch) => branch.confirmationStatuses.includes('BESTAETIGT'))) {
        confirmationStatuses.push('BESTAETIGT');
      }
      if (relevantBranches.length > 0
        && relevantBranches.every((branch) => branch.confirmationStatuses.includes('EXTERN_BESTAETIGT'))) {
        confirmationStatuses.push('EXTERN_BESTAETIGT');
      }
      return {
        ...item,
        qlikTotalCents: item.branches.reduce((sum, branch) => sum + (branch.qlikAmountCents || 0), 0),
        availabilityStatus: item.branches.every((branch) => branch.availabilityStatus === 'VORHANDEN') ? 'VORHANDEN' : 'UNVOLLSTAENDIG',
        comparisonStatus: comparisonPrecedence(item.branches.map((branch) => branch.comparisonStatus)),
        reviewStatus: reviewPrecedence(item.branches.map((branch) => branch.reviewStatus)),
        confirmationStatuses,
      };
    }));
    const summary = {
      newCount: items.filter((item) => item.reviewStatus === 'OFFEN').length,
      deviationCount: items.filter((item) => item.comparisonStatus === 'ABWEICHEND').length,
      deferredCount: items.filter((item) => item.reviewStatus === 'VERTAGT').length,
      confirmedCount: items.filter((item) => ['BESTAETIGT', 'EXTERN_BESTAETIGT'].includes(item.reviewStatus)).length,
      unresolvedPaymentTypeCount: unresolvedResult.rows.length,
    };
    return res.json({ ok: true, jahr, monat, sync: { mode: 'QLIK_IMPORT' }, summary, items,
      unresolvedPaymentTypes: unresolvedResult.rows,
      importState: importResult.rows[0] || { completed_at: null, imported_days: 0 } });
  } catch (error) {
    return routeError(res, error, 'Modul-2-Daten konnten nicht geladen werden.');
  } finally {
    client?.release();
  }
});

router.patch('/v2/component-reviews', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });

  let client;
  try {
    const paymentType = normalizePaymentType(req.body?.paymentType);
    const datum = String(req.body?.datum || '');
    if (!/^\d{4}-\d{2}-\d{2}$/.test(datum)) {
      throw Object.assign(new Error('Geschäftstag ist ungültig.'), { statusCode: 400 });
    }

    client = await pool.connect();
    await client.query('BEGIN');
    const selectedDays = await client.query(
      `SELECT pd.id
       FROM controlling.payment_days pd
       JOIN controlling.payment_types pt ON pt.id=pd.payment_type_id
       WHERE pt.code=$1 AND pd.datum=$2::date AND pd.qlik_amount_cents IS NOT NULL
       ORDER BY pd.id
       FOR UPDATE OF pd`,
      [paymentType, datum]
    );
    if (!selectedDays.rowCount) {
      throw Object.assign(new Error('Für diesen Tag wurden keine Qlik-Werte gefunden.'), { statusCode: 404 });
    }

    const paymentDayIds = selectedDays.rows.map((row) => row.id);
    const confirmedComponents = await confirmPaymentDayComponents(
      client,
      paymentDayIds,
      req.user.id,
      'V2_DAY_BULK_CONFIRM'
    );
    await client.query(
      `UPDATE controlling.payment_days
       SET review_status=CASE WHEN review_status='EXTERN_BESTAETIGT' THEN review_status ELSE 'BESTAETIGT' END,
           reviewed_by=CASE WHEN review_status='EXTERN_BESTAETIGT' THEN reviewed_by ELSE $2 END,
           reviewed_at=CASE WHEN review_status='EXTERN_BESTAETIGT' THEN reviewed_at ELSE now() END,
           review_note=NULL, deferred_until=NULL, updated_at=now()
       WHERE id=ANY($1::uuid[])`,
      [paymentDayIds, req.user.id]
    );
    for (const paymentDayId of paymentDayIds) {
      await client.query(
        `INSERT INTO controlling.payment_review_events (payment_day_id,event_type,actor_user_id,details)
         VALUES ($1,'BESTAETIGT',$2,jsonb_build_object(
           'source','V2_DAY_BULK_CONFIRM','paymentType',$3::text,'datum',$4::text
         ))`,
        [paymentDayId, req.user.id, paymentType, datum]
      );
    }
    await client.query('COMMIT');
    return res.json({
      ok: true,
      paymentType,
      datum,
      confirmedPaymentDays: paymentDayIds.length,
      confirmedComponents: confirmedComponents.length,
    });
  } catch (error) {
    if (client) await client.query('ROLLBACK').catch(() => {});
    return routeError(res, error, 'Einzelwerte konnten nicht gemeinsam bestätigt werden.');
  } finally {
    client?.release();
  }
});

router.patch('/v2/component-reviews/:id', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  const componentId = String(req.params.id || '');
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(componentId)) {
    return res.status(400).json({ ok: false, message: 'Einzelwert ist ungültig.' });
  }

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    const selected = await client.query(
      `SELECT component.id, component.payment_day_id, component.amount_cents,
              component.component_code, component.component_label
       FROM controlling.payment_day_component_reviews component
       WHERE component.id=$1
       FOR UPDATE`,
      [componentId]
    );
    if (!selected.rowCount) throw Object.assign(new Error('Einzelwert wurde nicht gefunden.'), { statusCode: 404 });
    const component = selected.rows[0];
    if (Number(component.amount_cents) <= 0) {
      throw Object.assign(new Error('Nullwerte gelten als „kein Umsatz“ und benötigen keine Bestätigung.'), { statusCode: 409 });
    }

    await client.query(
      `UPDATE controlling.payment_day_component_reviews
       SET review_status='BESTAETIGT', reviewed_by=$2, reviewed_at=now(), updated_at=now()
       WHERE id=$1`,
      [componentId, req.user.id]
    );
    await client.query(
      `INSERT INTO controlling.payment_day_component_review_events (
         component_review_id, event_type, actor_user_id, details
       ) VALUES ($1,'BESTAETIGT',$2,jsonb_build_object(
         'componentCode',$3::text,'componentLabel',$4::text,'amountCents',$5::bigint
       ))`,
      [componentId, req.user.id, component.component_code, component.component_label, component.amount_cents]
    );

    const progressResult = await client.query(
      `SELECT count(*) FILTER (WHERE amount_cents>0)::integer AS required,
              count(*) FILTER (WHERE amount_cents>0 AND review_status='BESTAETIGT')::integer AS confirmed
       FROM controlling.payment_day_component_reviews
       WHERE payment_day_id=$1`,
      [component.payment_day_id]
    );
    const progress = progressResult.rows[0];
    const complete = Number(progress.required) > 0 && Number(progress.required) === Number(progress.confirmed);
    const nextStatus = complete ? 'BESTAETIGT' : 'IN_PRUEFUNG';
    const parentUpdate = await client.query(
      `UPDATE controlling.payment_days
       SET review_status=CASE WHEN review_status='EXTERN_BESTAETIGT' THEN review_status ELSE $2 END,
           reviewed_by=CASE WHEN review_status='EXTERN_BESTAETIGT' THEN reviewed_by ELSE $3 END,
           reviewed_at=CASE WHEN review_status='EXTERN_BESTAETIGT' THEN reviewed_at ELSE now() END,
           updated_at=now()
       WHERE id=$1
       RETURNING review_status`,
      [component.payment_day_id, nextStatus, req.user.id]
    );
    await client.query(
      `INSERT INTO controlling.payment_review_events (payment_day_id,event_type,actor_user_id,details)
       VALUES ($1,$2,$3,jsonb_build_object(
         'source','COMPONENT_REVIEW','componentReviewId',$4::text,
         'confirmed',$5::integer,'required',$6::integer
       ))`,
      [component.payment_day_id, nextStatus, req.user.id, componentId,
        progress.confirmed, progress.required]
    );
    await client.query('COMMIT');
    return res.json({
      ok: true,
      componentId,
      reviewStatus: 'BESTAETIGT',
      branchReviewStatus: parentUpdate.rows[0]?.review_status || nextStatus,
      progress: { confirmed: Number(progress.confirmed), required: Number(progress.required), complete },
    });
  } catch (error) {
    if (client) await client.query('ROLLBACK').catch(() => {});
    return routeError(res, error, 'Einzelwert konnte nicht bestätigt werden.');
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

async function createAutomaticMatchSuggestion(transferId, actorUserId = null) {
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    const transferResult = await client.query(
      `SELECT t.*, pt.code AS payment_type, round(t.betrag*100)::bigint AS amount_cents
       FROM controlling.payment_transfers t JOIN controlling.payment_types pt ON pt.id=t.payment_type_id
       WHERE t.id=$1 AND t.deleted_at IS NULL FOR UPDATE`,
      [transferId]
    );
    if (!transferResult.rowCount) throw Object.assign(new Error('Sammelüberweisung wurde nicht gefunden.'), { statusCode: 404 });
    const transfer = transferResult.rows[0];
    if (Number(transfer.amount_cents) > MAX_AUTOMATIC_MATCH_AMOUNT_CENTS) {
      throw Object.assign(new Error('Der Betrag überschreitet den automatischen Suchbereich von 100.000 €.'), {
        statusCode: 409,
        code: 'MATCH_LIMIT_EXCEEDED',
      });
    }
    const confirmed = await client.query(
      `SELECT 1 FROM controlling.payment_transfer_matches
       WHERE transfer_id=$1 AND status='bestaetigt'`,
      [transfer.id]
    );
    if (confirmed.rowCount) {
      throw Object.assign(new Error('Eine bereits bestätigte Zuordnung wird nicht neu berechnet.'), { statusCode: 409 });
    }
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
      id: row.id,
      datum: dateOnly(row.datum),
      amountCents: Number(row.qlik_amount_cents),
      filiale: row.filiale,
    })), Number(transfer.amount_cents));
    if (!suggestion.complete) {
      throw Object.assign(new Error('Der Suchraum ist für einen verlässlichen automatischen Vorschlag zu groß.'), { statusCode: 409 });
    }
    if (!suggestion.itemIds.length) {
      throw Object.assign(new Error('Keine geeigneten Tageswerte für einen Vorschlag gefunden.'), { statusCode: 409 });
    }
    await client.query(
      `UPDATE controlling.payment_transfer_matches
       SET status='abgelehnt',updated_at=now(),review_note='Durch neuen Vorschlag ersetzt.'
       WHERE transfer_id=$1 AND status IN ('vorgeschlagen','vertagt')`,
      [transfer.id]
    );
    const matchResult = await client.query(
      `INSERT INTO controlling.payment_transfer_matches (transfer_id,status,differenz,automatisch,algorithm_version)
       VALUES ($1,'vorgeschlagen',$2::bigint/100.0,true,'subset-cents-v2') RETURNING *`,
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
      `INSERT INTO controlling.payment_transfer_events (transfer_id,match_id,event_type,actor_user_id,details)
       VALUES ($1,$2,'VORGESCHLAGEN',$3,jsonb_build_object('exact',$4::boolean,'differenceCents',$5::bigint,'automaticTrigger',true))`,
      [transfer.id, match.id, actorUserId, suggestion.exact, suggestion.differenceCents]
    );
    await client.query('COMMIT');
    return { ...match, suggestion };
  } catch (error) {
    if (client) await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client?.release();
  }
}

async function tryAutomaticMatchSuggestion(transferId, actorUserId) {
  try {
    return { match: await createAutomaticMatchSuggestion(transferId, actorUserId), suggestionError: null };
  } catch (error) {
    return { match: null, suggestionError: error.message || 'Match-Vorschlag konnte nicht erzeugt werden.' };
  }
}

function transferCorrectionReason(value) {
  const reason = String(value || '').trim();
  if (!reason) throw Object.assign(new Error('Die Betragskorrektur benötigt eine Begründung.'), { statusCode: 400 });
  if (reason.length > 500) throw Object.assign(new Error('Die Begründung darf höchstens 500 Zeichen lang sein.'), { statusCode: 400 });
  return reason;
}

async function correctTransferAmount(client, { transferId, amountCents, reason, actorUserId, source }) {
  const transferResult = await client.query(
    `SELECT transfer.*, round(transfer.betrag*100)::bigint AS old_amount_cents
     FROM controlling.payment_transfers transfer
     WHERE transfer.id=$1 AND transfer.deleted_at IS NULL
     FOR UPDATE`,
    [transferId]
  );
  if (!transferResult.rowCount) throw Object.assign(new Error('Sammelüberweisung wurde nicht gefunden.'), { statusCode: 404 });
  const transfer = transferResult.rows[0];
  const oldAmountCents = Number(transfer.old_amount_cents);
  if (oldAmountCents === amountCents) return { transfer, changed: false, supersededMatchIds: [] };

  const correctionReason = transferCorrectionReason(reason);
  const matches = await client.query(
    `SELECT id,status FROM controlling.payment_transfer_matches
     WHERE transfer_id=$1 AND status IN ('vorgeschlagen','vertagt','bestaetigt')
     ORDER BY created_at,id FOR UPDATE`,
    [transferId]
  );
  const matchIds = matches.rows.map((row) => row.id);

  if (matchIds.length) {
    const settledCarries = await client.query(
      `UPDATE controlling.payment_transfer_difference_carries carry
       SET status='OFFEN',settlement_match_id=NULL,settled_by=NULL,settled_at=NULL
       WHERE carry.settlement_match_id=ANY($1::uuid[]) AND carry.status='AUSGEGLICHEN'
       RETURNING carry.id,carry.source_match_id`,
      [matchIds]
    );
    for (const carry of settledCarries.rows) {
      const sourceDays = await client.query(
        `UPDATE controlling.payment_days day
         SET review_status='AUSGLEICH_AUSSTEHEND',ist_betrag=NULL,
             reviewed_by=$2,reviewed_at=now(),updated_at=now()
         FROM controlling.payment_transfer_match_items item
         WHERE item.match_id=$1 AND item.payment_day_id=day.id
         RETURNING day.id`,
        [carry.source_match_id, actorUserId]
      );
      for (const day of sourceDays.rows) {
        await client.query(
          `INSERT INTO controlling.payment_review_events
             (payment_day_id,event_type,actor_user_id,details)
           VALUES ($1,'TRANSFER_KORRIGIERT',$2,jsonb_build_object(
             'source',$3::text,'reason',$4::text,'carryId',$5::uuid,
             'effect','DIFFERENCE_CARRY_REOPENED'
           ))`,
          [day.id, actorUserId, source, correctionReason, carry.id]
        );
      }
    }

    await client.query(
      `UPDATE controlling.payment_transfer_difference_carries
       SET status='STORNIERT'
       WHERE source_match_id=ANY($1::uuid[]) AND status IN ('OFFEN','AUSGEGLICHEN')`,
      [matchIds]
    );

    const restoredDays = await client.query(
      `UPDATE controlling.payment_days day
       SET review_status=COALESCE(human.event_type,'OFFEN'),ist_betrag=NULL,
           reviewed_by=human.actor_user_id,reviewed_at=human.created_at,updated_at=now()
       FROM controlling.payment_transfer_match_items item
       LEFT JOIN LATERAL (
         SELECT event.event_type,event.actor_user_id,event.created_at
         FROM controlling.payment_review_events event
         WHERE event.payment_day_id=item.payment_day_id
           AND event.event_type IN ('IN_PRUEFUNG','BESTAETIGT','KORRIGIERT','VERTAGT')
         ORDER BY event.created_at DESC,event.id DESC LIMIT 1
       ) human ON true
       WHERE item.match_id=ANY($1::uuid[]) AND item.payment_day_id=day.id
       RETURNING day.id,item.match_id`,
      [matchIds]
    );
    for (const day of restoredDays.rows) {
      await client.query(
        `INSERT INTO controlling.payment_review_events
           (payment_day_id,event_type,actor_user_id,details)
         VALUES ($1,'TRANSFER_KORRIGIERT',$2,jsonb_build_object(
           'source',$3::text,'reason',$4::text,'supersededMatchId',$5::uuid,
           'oldAmountCents',$6::bigint,'newAmountCents',$7::bigint
         ))`,
        [day.id, actorUserId, source, correctionReason, day.match_id, oldAmountCents, amountCents]
      );
    }

    await client.query(
      `UPDATE controlling.payment_transfer_matches
       SET status='abgelehnt',reviewed_by=$2,reviewed_at=now(),
           review_note=$3,updated_at=now()
       WHERE id=ANY($1::uuid[])`,
      [matchIds, actorUserId, `Durch Betragskorrektur ersetzt: ${correctionReason}`]
    );
  }

  const updated = await client.query(
    `UPDATE controlling.payment_transfers
     SET betrag=$2::bigint/100.0,status='offen',decided_by=NULL,decided_at=NULL,
         decision_note=NULL,deferred_until=NULL,updated_at=now()
     WHERE id=$1 RETURNING *`,
    [transferId, amountCents]
  );
  await client.query(
    `INSERT INTO controlling.payment_transfer_events
       (transfer_id,event_type,actor_user_id,details)
     VALUES ($1,'BETRAG_KORRIGIERT',$2,jsonb_build_object(
       'source',$3::text,'reason',$4::text,'oldAmountCents',$5::bigint,
       'newAmountCents',$6::bigint,'supersededMatchIds',$7::jsonb
     ))`,
    [transferId, actorUserId, source, correctionReason, oldAmountCents, amountCents, JSON.stringify(matchIds)]
  );
  return { transfer: updated.rows[0], changed: true, supersededMatchIds: matchIds };
}

async function deleteUnconfirmedTransfer(client, { transferId, reason, actorUserId, source }) {
  const deletionReason = String(reason || '').trim();
  if (!deletionReason) {
    throw Object.assign(new Error('Das Löschen benötigt eine Begründung.'), { statusCode: 400 });
  }

  const selected = await client.query(
    `SELECT transfer.*
     FROM controlling.payment_transfers transfer
     WHERE transfer.id=$1 AND transfer.deleted_at IS NULL
     FOR UPDATE`,
    [transferId]
  );
  if (!selected.rowCount) {
    throw Object.assign(new Error('Sammelüberweisung wurde nicht gefunden.'), { statusCode: 404 });
  }

  const confirmed = await client.query(
    `SELECT id FROM controlling.payment_transfer_matches
     WHERE transfer_id=$1 AND status='bestaetigt'
     LIMIT 1`,
    [transferId]
  );
  if (confirmed.rowCount) {
    throw Object.assign(new Error('Eine bestätigte Zuordnung kann nicht gelöscht werden. Bitte zuerst den Match nachvollziehbar zurücksetzen.'), { statusCode: 409 });
  }

  const rejected = await client.query(
    `UPDATE controlling.payment_transfer_matches
     SET status='abgelehnt',reviewed_by=$2,reviewed_at=now(),
         review_note=$3,updated_at=now()
     WHERE transfer_id=$1 AND status IN ('vorgeschlagen','vertagt')
     RETURNING id`,
    [transferId, actorUserId, `Sammelüberweisung gelöscht: ${deletionReason}`]
  );

  const deleted = await client.query(
    `UPDATE controlling.payment_transfers
     SET deleted_at=now(),deleted_by=$2,deletion_reason=$3,
         status='abgelehnt',decided_by=$2,decided_at=now(),
         decision_note=$3,updated_at=now()
     WHERE id=$1 AND deleted_at IS NULL
     RETURNING *`,
    [transferId, actorUserId, deletionReason]
  );
  await client.query(
    `INSERT INTO controlling.payment_transfer_events
       (transfer_id,event_type,actor_user_id,details)
     VALUES ($1,'GELOESCHT',$2,jsonb_build_object(
       'source',$3::text,'reason',$4::text,'rejectedMatchIds',$5::jsonb
     ))`,
    [transferId, actorUserId, source, deletionReason,
      JSON.stringify(rejected.rows.map((row) => row.id))]
  );

  return deleted.rows[0];
}

router.put('/transfers/classic', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) {
    return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  }

  let client;

  try {
    const paymentType = normalizePaymentType(req.body?.paymentType);
    const provider = providerForPaymentType(paymentType);
    const amountCents = moneyToCents(req.body?.amount);
    const buchungsdatum = String(req.body?.datum || '');

    if (!/^\d{4}-\d{2}-\d{2}$/.test(buchungsdatum)) {
      throw Object.assign(new Error('Buchungsdatum ist ungültig.'), { statusCode: 400 });
    }

    client = await pool.connect();
    await client.query('BEGIN');
    await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`ALEX2:${paymentType}:${buchungsdatum}`]);
    const existing = await client.query(
      `SELECT transfer.*, match.status AS match_status
       FROM controlling.payment_transfers transfer
       JOIN controlling.payment_types payment_type ON payment_type.id=transfer.payment_type_id
       LEFT JOIN controlling.payment_transfer_matches match
         ON match.transfer_id=transfer.id
        AND match.status IN ('vorgeschlagen','vertagt','bestaetigt')
       WHERE payment_type.code=$1 AND transfer.provider=$2
         AND transfer.buchungsdatum=$3::date AND transfer.deleted_at IS NULL
       ORDER BY transfer.created_at, transfer.id
       FOR UPDATE OF transfer`,
      [paymentType, provider, buchungsdatum]
    );
    if (existing.rowCount > 1) {
      throw Object.assign(new Error('Für diesen Tag bestehen mehrere Sammelüberweisungen. Bitte in V2 bearbeiten.'), { statusCode: 409 });
    }
    let transfer;
    let shouldSuggest = true;
    let eventType;
    if (existing.rowCount === 1) {
      const current = existing.rows[0];
      const currentAmountCents = Math.round(Number(current.betrag) * 100);
      if (currentAmountCents !== amountCents) {
        const correction = await correctTransferAmount(client, {
          transferId: current.id,
          amountCents,
          reason: req.body?.reason,
          actorUserId: req.user.id,
          source: 'CLASSIC_VIEW',
        });
        transfer = correction.transfer;
        eventType = null;
        shouldSuggest = true;
      } else {
        transfer = current;
        eventType = null;
        shouldSuggest = !current.match_status;
      }
    } else {
      const inserted = await client.query(
        `INSERT INTO controlling.payment_transfers
          (payment_type_id,filiale,buchungsdatum,betrag,referenz,status,provider,created_by_user_id)
         SELECT id,'Unternehmen',$2::date,$3::bigint/100.0,$4,'offen',$5,$6
         FROM controlling.payment_types WHERE code=$1
         RETURNING *`,
        [paymentType, buchungsdatum, amountCents, `CLASSIC:${paymentType}:${buchungsdatum}`, provider, req.user.id]
      );
      if (!inserted.rowCount) throw Object.assign(new Error('Zahlungsart wurde nicht gefunden.'), { statusCode: 400 });
      transfer = inserted.rows[0];
      eventType = 'ERFASST';
    }
    if (eventType) await client.query(
      `INSERT INTO controlling.payment_transfer_events
        (transfer_id,event_type,actor_user_id,details)
       VALUES ($1,$2,$3,jsonb_build_object('source','CLASSIC_VIEW','provider',$4::text,'amountCents',$5::bigint))`,
      [transfer.id, eventType, req.user.id, provider, amountCents]
    );

    await client.query('COMMIT');
    client.release();
    client = null;
    const automatic = shouldSuggest
      ? await tryAutomaticMatchSuggestion(transfer.id, req.user.id)
      : { match: null, suggestionError: null };
    return res.json({ ok: true, transfer: mapPaymentTransfer(transfer), ...automatic });
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
              source_carry.id AS source_carry_id, source_carry.status AS source_carry_status,
              source_carry.amount_cents AS source_carry_amount_cents,
              source_settlement_transfer.id AS source_settlement_transfer_id,
              source_settlement_transfer.buchungsdatum AS source_settlement_date,
              settlement_carry.id AS settlement_carry_id,
              settlement_carry.status AS settlement_carry_status,
              settlement_carry.amount_cents AS settlement_carry_amount_cents,
              settlement_source_transfer.id AS settlement_source_transfer_id,
              settlement_source_transfer.buchungsdatum AS settlement_source_date,
              carry_candidate.id AS carry_suggestion_id,
              carry_candidate.amount_cents AS carry_suggestion_amount_cents,
              carry_candidate.source_transfer_id AS carry_suggestion_source_transfer_id,
              carry_candidate.source_booking_date AS carry_suggestion_source_date,
              COALESCE(jsonb_agg(jsonb_build_object(
                'id', mi.id, 'paymentDayId', pd.id, 'datum', pd.datum,
                'filiale', pd.filiale, 'amountCents', round(mi.betrag*100)::bigint,
                'partial', mi.teilbetrag
              ) ORDER BY mi.reihenfolge) FILTER (WHERE mi.id IS NOT NULL), '[]'::jsonb) AS items
       FROM controlling.payment_transfers t
       JOIN controlling.payment_types pt ON pt.id=t.payment_type_id
       LEFT JOIN controlling.payment_transfer_matches m
         ON m.transfer_id=t.id AND m.status IN ('vorgeschlagen','bestaetigt','vertagt')
       LEFT JOIN controlling.payment_transfer_difference_carries source_carry
         ON source_carry.source_match_id=m.id
       LEFT JOIN controlling.payment_transfer_matches source_settlement_match
         ON source_settlement_match.id=source_carry.settlement_match_id
       LEFT JOIN controlling.payment_transfers source_settlement_transfer
         ON source_settlement_transfer.id=source_settlement_match.transfer_id
       LEFT JOIN controlling.payment_transfer_difference_carries settlement_carry
         ON settlement_carry.settlement_match_id=m.id
       LEFT JOIN controlling.payment_transfer_matches settlement_source_match
         ON settlement_source_match.id=settlement_carry.source_match_id
       LEFT JOIN controlling.payment_transfers settlement_source_transfer
         ON settlement_source_transfer.id=settlement_source_match.transfer_id
       LEFT JOIN LATERAL (
         SELECT carry.id, carry.amount_cents,
                source_transfer.id AS source_transfer_id,
                source_transfer.buchungsdatum AS source_booking_date
         FROM controlling.payment_transfer_difference_carries carry
         JOIN controlling.payment_transfer_matches source_match ON source_match.id=carry.source_match_id
         JOIN controlling.payment_transfers source_transfer ON source_transfer.id=source_match.transfer_id
         WHERE carry.status='OFFEN'
           AND m.status IN ('vorgeschlagen','vertagt')
           AND round(m.differenz*100)::bigint=carry.amount_cents
           AND m.differenz>0
           AND source_transfer.provider=t.provider
           AND source_transfer.payment_type_id=t.payment_type_id
           AND source_transfer.buchungsdatum<=t.buchungsdatum
           AND source_transfer.deleted_at IS NULL
         ORDER BY source_transfer.buchungsdatum DESC, carry.created_at DESC
         LIMIT 1
       ) carry_candidate ON true
       LEFT JOIN controlling.payment_transfer_match_items mi ON mi.match_id=m.id
       LEFT JOIN controlling.payment_days pd ON pd.id=mi.payment_day_id
       WHERE EXTRACT(YEAR FROM t.buchungsdatum)::integer=$1
         AND EXTRACT(MONTH FROM t.buchungsdatum)::integer=$2
         AND t.deleted_at IS NULL
       GROUP BY t.id, pt.code, m.id, source_carry.id, source_settlement_transfer.id,
                settlement_carry.id, settlement_source_transfer.id,
                carry_candidate.id, carry_candidate.amount_cents,
                carry_candidate.source_transfer_id, carry_candidate.source_booking_date
       ORDER BY t.provider, t.buchungsdatum DESC, t.created_at DESC`,
      [jahr, monat]
    );
    return res.json({ ok: true, transfers: result.rows.map((row) => {
      const rawDifferenceCents = row.differenz === null ? null : Math.round(Number(row.differenz) * 100);
      const carrySettled = row.source_carry_status === 'AUSGEGLICHEN' || row.settlement_carry_status === 'AUSGEGLICHEN';
      return {
        ...row,
        buchungsdatum: dateOnly(row.buchungsdatum),
        betragCents: Math.round(Number(row.betrag) * 100),
        rawDifferenzCents: rawDifferenceCents,
        differenzCents: carrySettled ? 0 : rawDifferenceCents,
        deferred_until: dateOnly(row.deferred_until),
        differenceCarry: row.source_carry_id ? {
          id: row.source_carry_id,
          role: 'SOURCE',
          status: row.source_carry_status,
          amountCents: Number(row.source_carry_amount_cents),
          counterpartTransferId: row.source_settlement_transfer_id || null,
          counterpartDate: dateOnly(row.source_settlement_date),
        } : row.settlement_carry_id ? {
          id: row.settlement_carry_id,
          role: 'SETTLEMENT',
          status: row.settlement_carry_status,
          amountCents: Number(row.settlement_carry_amount_cents),
          counterpartTransferId: row.settlement_source_transfer_id,
          counterpartDate: dateOnly(row.settlement_source_date),
        } : null,
        carrySuggestion: row.carry_suggestion_id ? {
          id: row.carry_suggestion_id,
          amountCents: Number(row.carry_suggestion_amount_cents),
          sourceTransferId: row.carry_suggestion_source_transfer_id,
          sourceDate: dateOnly(row.carry_suggestion_source_date),
        } : null,
      };
    }) });
  } catch (error) {
    return routeError(res, error, 'Sammelüberweisungen konnten nicht geladen werden.');
  }
});

router.post('/v2/transfers', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  try {
    const paymentType = normalizePaymentType(req.body?.paymentType);
    const provider = providerForPaymentType(paymentType);
    const amountCents = moneyToCents(req.body?.amount);
    const buchungsdatum = String(req.body?.buchungsdatum || '');
    const referenz = String(req.body?.referenz || '').trim() || null;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(buchungsdatum)) throw Object.assign(new Error('Buchungsdatum ist ungültig.'), { statusCode: 400 });
    const result = await pool.query(
      `INSERT INTO controlling.payment_transfers
        (payment_type_id,filiale,buchungsdatum,betrag,referenz,status,provider,created_by_user_id)
       SELECT id,'Unternehmen',$3::date,$4::bigint/100.0,$5,'offen',$1,$6
       FROM controlling.payment_types WHERE code=$2 RETURNING *`,
      [provider, paymentType, buchungsdatum, amountCents, referenz, req.user.id]
    );
    if (!result.rowCount) throw Object.assign(new Error('Zahlungsart wurde nicht gefunden.'), { statusCode: 400 });
    await pool.query(
      `INSERT INTO controlling.payment_transfer_events (transfer_id,event_type,actor_user_id,details)
       VALUES ($1,'ERFASST',$2,'{}'::jsonb)`,
      [result.rows[0].id, req.user.id]
    );
    const automatic = await tryAutomaticMatchSuggestion(result.rows[0].id, req.user.id);
    return res.status(201).json({ ok: true, transfer: result.rows[0], ...automatic });
  } catch (error) {
    return routeError(res, error, 'Sammelüberweisung konnte nicht gespeichert werden.');
  }
});

router.patch('/v2/transfers/:id', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  let client;
  try {
    const amountCents = moneyToCents(req.body?.amount);
    client = await pool.connect();
    await client.query('BEGIN');
    const correction = await correctTransferAmount(client, {
      transferId: req.params.id,
      amountCents,
      reason: req.body?.reason,
      actorUserId: req.user.id,
      source: 'V2_VIEW',
    });
    await client.query('COMMIT');
    client.release();
    client = null;
    const automatic = correction.changed
      ? await tryAutomaticMatchSuggestion(req.params.id, req.user.id)
      : { match: null, suggestionError: null };
    return res.json({ ok: true, transfer: mapPaymentTransfer(correction.transfer), ...automatic });
  } catch (error) {
    if (client) await client.query('ROLLBACK').catch(() => {});
    return routeError(res, error, 'Betrag der Sammelüberweisung konnte nicht korrigiert werden.');
  } finally {
    client?.release();
  }
});

router.delete('/v2/transfers/:id', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    const transfer = await deleteUnconfirmedTransfer(client, {
      transferId: req.params.id,
      reason: req.body?.reason,
      actorUserId: req.user.id,
      source: String(req.body?.source || 'V2_VIEW'),
    });
    await client.query('COMMIT');
    return res.json({ ok: true, transfer: mapPaymentTransfer(transfer) });
  } catch (error) {
    if (client) await client.query('ROLLBACK').catch(() => {});
    return routeError(res, error, 'Sammelüberweisung konnte nicht gelöscht werden.');
  } finally {
    client?.release();
  }
});

router.post('/v2/transfers/:id/suggest', verifyToken(), async (req, res) => {
  if (!hasAccess(req)) return res.status(403).json({ ok: false, message: 'Zugriff verweigert.' });
  try {
    const match = await createAutomaticMatchSuggestion(req.params.id, req.user.id);
    const suggestion = match.suggestion;
    return res.status(201).json({ ok: true, match: { ...match, suggestion } });
  } catch (error) {
    return routeError(res, error, 'Match-Vorschlag konnte nicht erzeugt werden.');
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
       WHERE t.id=$1 AND t.deleted_at IS NULL AND NOT EXISTS (
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
       FROM controlling.payment_transfers t WHERE id=$1 AND deleted_at IS NULL FOR UPDATE`,
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
      `SELECT t.id AS transfer_id, t.provider, t.payment_type_id,
              t.buchungsdatum, pt.code AS payment_type,
              m.id AS match_id, m.differenz,
              round(m.differenz*100)::bigint AS difference_cents
       FROM controlling.payment_transfers t
       JOIN controlling.payment_transfer_matches m ON m.transfer_id=t.id
       JOIN controlling.payment_types pt ON pt.id=t.payment_type_id
       WHERE t.id=$1 AND t.deleted_at IS NULL
         AND m.status IN ('vorgeschlagen','vertagt') FOR UPDATE OF t,m`,
      [req.params.id]
    );
    if (!result.rowCount) throw Object.assign(new Error('Offener Match-Vorschlag wurde nicht gefunden.'), { statusCode: 404 });
    const current = result.rows[0];
    const settleCarryId = String(req.body?.settleCarryId || '').trim() || null;
    let carryToSettle = null;
    if (decision.action === 'confirm' && settleCarryId) {
      const carryResult = await client.query(
        `SELECT carry.id, carry.amount_cents, carry.source_match_id,
                source_transfer.id AS source_transfer_id,
                source_transfer.provider AS source_provider,
                source_transfer.payment_type_id AS source_payment_type_id,
                source_transfer.buchungsdatum AS source_date,
                round(source_match.differenz*100)::bigint AS source_difference_cents
         FROM controlling.payment_transfer_difference_carries carry
         JOIN controlling.payment_transfer_matches source_match ON source_match.id=carry.source_match_id
         JOIN controlling.payment_transfers source_transfer ON source_transfer.id=source_match.transfer_id
         WHERE carry.id=$1 AND carry.status='OFFEN'
         FOR UPDATE OF carry`,
        [settleCarryId]
      );
      if (!carryResult.rowCount) throw Object.assign(new Error('Der vorgeschlagene Fehlbetrag ist nicht mehr offen.'), { statusCode: 409 });
      carryToSettle = carryResult.rows[0];
      const exact = isExactDifferenceCarry({
        sourceDifferenceCents: Number(carryToSettle.source_difference_cents),
        settlementDifferenceCents: Number(current.difference_cents),
        sourceProvider: carryToSettle.source_provider,
        settlementProvider: current.provider,
        sourcePaymentType: String(carryToSettle.source_payment_type_id),
        settlementPaymentType: String(current.payment_type_id),
        sourceDate: dateOnly(carryToSettle.source_date),
        settlementDate: dateOnly(current.buchungsdatum),
      });
      if (!exact || Number(carryToSettle.amount_cents) !== Number(current.difference_cents)) {
        throw Object.assign(new Error('Der Fehlbetrag passt nicht exakt zu dieser positiven Differenz.'), { statusCode: 409 });
      }
    }
    if (decision.action === 'confirm' && Number(current.differenz) !== 0
      && !String(req.body?.note || '').trim() && !carryToSettle) {
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
      const dayStatus = Number(current.difference_cents) < 0 ? 'AUSGLEICH_AUSSTEHEND' : 'EXTERN_BESTAETIGT';
      const days = await client.query(
        `UPDATE controlling.payment_days pd
         SET review_status=$2::varchar(30),
             ist_betrag=CASE WHEN $2::varchar(30)='EXTERN_BESTAETIGT' THEN mi.betrag ELSE NULL END,
             reviewed_by=$3,reviewed_at=now(),updated_at=now()
         FROM controlling.payment_transfer_match_items mi
         WHERE mi.match_id=$1 AND mi.payment_day_id=pd.id RETURNING pd.id`,
        [current.match_id, dayStatus, req.user.id]
      );
      for (const day of days.rows) {
        await client.query(
          `INSERT INTO controlling.payment_review_events (payment_day_id,event_type,actor_user_id,details)
           VALUES ($1,$2,$3,jsonb_build_object('matchId',$4::uuid))`,
          [day.id, dayStatus, req.user.id, current.match_id]
        );
      }
      if (Number(current.difference_cents) < 0) {
        const carry = await client.query(
          `INSERT INTO controlling.payment_transfer_difference_carries
             (source_match_id,amount_cents,created_by)
           VALUES ($1,$2,$3)
           RETURNING id`,
          [current.match_id, Math.abs(Number(current.difference_cents)), req.user.id]
        );
        await client.query(
          `INSERT INTO controlling.payment_transfer_events
             (transfer_id,match_id,event_type,actor_user_id,details)
           VALUES ($1,$2,'FEHLBETRAG_GEMERKT',$3,jsonb_build_object(
             'carryId',$4::uuid,'amountCents',$5::bigint
           ))`,
          [current.transfer_id, current.match_id, req.user.id, carry.rows[0].id,
            Math.abs(Number(current.difference_cents))]
        );
      }
      if (carryToSettle) {
        await client.query(
          `UPDATE controlling.payment_transfer_difference_carries
           SET status='AUSGEGLICHEN',settlement_match_id=$2,settled_by=$3,settled_at=now()
           WHERE id=$1`,
          [carryToSettle.id, current.match_id, req.user.id]
        );
        const sourceDays = await client.query(
          `UPDATE controlling.payment_days pd
           SET review_status='EXTERN_BESTAETIGT',ist_betrag=item.betrag,
               reviewed_by=$2,reviewed_at=now(),updated_at=now()
           FROM controlling.payment_transfer_match_items item
           WHERE item.match_id=$1 AND item.payment_day_id=pd.id
           RETURNING pd.id`,
          [carryToSettle.source_match_id, req.user.id]
        );
        for (const day of sourceDays.rows) {
          await client.query(
            `INSERT INTO controlling.payment_review_events (payment_day_id,event_type,actor_user_id,details)
             VALUES ($1,'EXTERN_BESTAETIGT',$2,jsonb_build_object(
               'sourceMatchId',$3::uuid,'settlementMatchId',$4::uuid,'carryId',$5::uuid
             ))`,
            [day.id, req.user.id, carryToSettle.source_match_id, current.match_id, carryToSettle.id]
          );
        }
        await client.query(
          `INSERT INTO controlling.payment_transfer_events
             (transfer_id,match_id,event_type,actor_user_id,details)
           VALUES
             ($1,$2,'FEHLBETRAG_AUSGEGLICHEN',$5,jsonb_build_object(
               'carryId',$4::uuid,'settlementTransferId',$3::uuid,'amountCents',$6::bigint
             )),
             ($3,$7,'AUSGLEICH_BESTAETIGT',$5,jsonb_build_object(
               'carryId',$4::uuid,'sourceTransferId',$1::uuid,'amountCents',$6::bigint
             ))`,
          [carryToSettle.source_transfer_id, carryToSettle.source_match_id,
            current.transfer_id, carryToSettle.id, req.user.id,
            Number(carryToSettle.amount_cents), current.match_id]
        );
      }
    }
    await client.query(
      `INSERT INTO controlling.payment_transfer_events (transfer_id,match_id,event_type,actor_user_id,details)
       VALUES ($1,$2,$3,$4,jsonb_build_object('note',$5::text,'deferredUntil',$6::text))`,
      [current.transfer_id, current.match_id, eventType, req.user.id, decision.note, decision.deferredUntil]
    );
    await client.query('COMMIT');
    return res.json({ ok: true, status: matchStatus, carrySettled: Boolean(carryToSettle) });
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
