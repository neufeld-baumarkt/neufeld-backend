const express = require('express');
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

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
      filiale: row.filiale,
      buchungsdatum,
      betrag: toNumberOrNull(row.betrag),
      referenz: row.referenz || null,
      status: row.status || 'offen',
      createdAt: row.created_at || null,
    };

    day.gesamtUeberweisung.transfers.push(transfer);

    const currentTotal = toNumberOrNull(day.gesamtUeberweisung.betrag) || 0;
    const transferAmount = toNumberOrNull(row.betrag) || 0;
    day.gesamtUeberweisung.betrag = currentTotal + transferAmount;
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

  try {
    const paymentDaysResult = await pool.query(
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

    const paymentTransfersResult = await pool.query(
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

module.exports = router;