const express = require('express');
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

const router = express.Router();

const ALLOWED_ROLES = new Set(['Admin', 'Supervisor', 'Geschäftsführer']);

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