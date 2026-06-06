// routes/cashflow.js – Cashflow Read-Only Endpoints
// Zweck:
// - Jahresübersicht für Cashflow-Dashboard bereitstellen
// - Kategorieauswertung für Cashflow-Dashboard bereitstellen
// - KPI-Auswertung für Cashflow-Dashboard bereitstellen
// - Optionaler bisKw-Filter für Zeitraumvergleiche
// - Zugriff nur für Admin, Supervisor und Geschäftsführer
// - Keine Schreiboperationen
// - Saldo wird serverseitig über cashflow.kategorien.typ berechnet

const express = require('express');
const router = express.Router();

const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

const ALLOWED_ROLES = new Set(['Admin', 'Supervisor', 'Geschäftsführer']);

function requireCashflowAccess(req, res, next) {
  const role = req.user?.role;

  if (!ALLOWED_ROLES.has(role)) {
    return res.status(403).json({
      message: 'Zugriff verweigert. Erforderliche Rolle: Admin, Supervisor oder Geschäftsführer.',
    });
  }

  next();
}

function parseJahrParam(req, res) {
  const jahr = Number(req.query?.jahr);

  if (!Number.isInteger(jahr) || jahr < 2000 || jahr > 2100) {
    res.status(400).json({
      message: 'Ungültiges Jahr. Erwartet wird z. B. ?jahr=2024',
    });
    return null;
  }

  return jahr;
}

function parseBisKwParam(req, res) {
  if (req.query?.bisKw === undefined || req.query?.bisKw === null || req.query?.bisKw === '') {
    return null;
  }

  const bisKw = Number(req.query.bisKw);

  if (!Number.isInteger(bisKw) || bisKw < 1 || bisKw > 53) {
    res.status(400).json({
      message: 'Ungültige bisKw. Erwartet wird z. B. &bisKw=18',
    });
    return false;
  }

  return bisKw;
}

function buildBisKwFilter(bisKw, params) {
  if (bisKw === null) {
    return '';
  }

  params.push(bisKw);
  return `AND b.kw <= $${params.length}`;
}

// GET /api/cashflow/jahresuebersicht?jahr=2024
// GET /api/cashflow/jahresuebersicht?jahr=2024&bisKw=18
router.get('/jahresuebersicht', verifyToken(), requireCashflowAccess, async (req, res) => {
  const jahr = parseJahrParam(req, res);
  if (jahr === null) return;

  const bisKw = parseBisKwParam(req, res);
  if (bisKw === false) return;

  try {
    const params = [jahr];
    const bisKwFilter = buildBisKwFilter(bisKw, params);

    const result = await pool.query(
      `
      SELECT
        b.kw,
        COALESCE(SUM(CASE WHEN k.typ = 'Einnahme' THEN b.betrag ELSE 0 END), 0)::numeric(12,2) AS einnahmen,
        COALESCE(SUM(CASE WHEN k.typ = 'Ausgabe' THEN b.betrag ELSE 0 END), 0)::numeric(12,2) AS ausgaben,
        COALESCE(SUM(
          CASE
            WHEN k.typ = 'Einnahme' THEN b.betrag
            WHEN k.typ = 'Ausgabe' THEN -b.betrag
            ELSE 0
          END
        ), 0)::numeric(12,2) AS saldo
      FROM cashflow.buchungen b
      JOIN cashflow.kategorien k
        ON k.id = b.kategorie_id
      WHERE b.jahr = $1
        AND k.aktiv = true
        ${bisKwFilter}
      GROUP BY b.kw
      ORDER BY b.kw
      `,
      params
    );

    return res.json({
      jahr,
      bisKw,
      weeks: result.rows,
    });
  } catch (err) {
    console.error('Fehler GET /api/cashflow/jahresuebersicht:', err);
    return res.status(500).json({
      message: 'Serverfehler bei Cashflow-Jahresübersicht.',
    });
  }
});

// GET /api/cashflow/kategorien?jahr=2024
// GET /api/cashflow/kategorien?jahr=2024&bisKw=18
router.get('/kategorien', verifyToken(), requireCashflowAccess, async (req, res) => {
  const jahr = parseJahrParam(req, res);
  if (jahr === null) return;

  const bisKw = parseBisKwParam(req, res);
  if (bisKw === false) return;

  try {
    const params = [jahr];
    const bisKwFilter = buildBisKwFilter(bisKw, params);

    const result = await pool.query(
      `
      WITH kategorien_summen AS (
        SELECT
          k.id,
          k.name AS kategorie,
          k.typ,
          k.sortierung,
          COALESCE(SUM(b.betrag), 0)::numeric(12,2) AS gesamt
        FROM cashflow.buchungen b
        JOIN cashflow.kategorien k
          ON k.id = b.kategorie_id
        WHERE b.jahr = $1
          AND k.aktiv = true
          ${bisKwFilter}
        GROUP BY
          k.id,
          k.name,
          k.typ,
          k.sortierung
      ),
      typ_summen AS (
        SELECT
          typ,
          COALESCE(SUM(gesamt), 0)::numeric(12,2) AS typ_gesamt
        FROM kategorien_summen
        GROUP BY typ
      )
      SELECT
        ks.id,
        ks.kategorie,
        ks.typ,
        ks.gesamt,
        CASE
          WHEN ts.typ_gesamt = 0 THEN 0::numeric(8,2)
          ELSE ROUND((ks.gesamt / ts.typ_gesamt) * 100, 2)::numeric(8,2)
        END AS anteil_prozent
      FROM kategorien_summen ks
      JOIN typ_summen ts
        ON ts.typ = ks.typ
      ORDER BY
        ks.sortierung,
        ks.id
      `,
      params
    );

    return res.json({
      jahr,
      bisKw,
      kategorien: result.rows,
    });
  } catch (err) {
    console.error('Fehler GET /api/cashflow/kategorien:', err);
    return res.status(500).json({
      message: 'Serverfehler bei Cashflow-Kategorienauswertung.',
    });
  }
});

// GET /api/cashflow/kpis?jahr=2024
// GET /api/cashflow/kpis?jahr=2024&bisKw=18
router.get('/kpis', verifyToken(), requireCashflowAccess, async (req, res) => {
  const jahr = parseJahrParam(req, res);
  if (jahr === null) return;

  const bisKw = parseBisKwParam(req, res);
  if (bisKw === false) return;

  try {
    const params = [jahr];
    const bisKwFilter = buildBisKwFilter(bisKw, params);

    const result = await pool.query(
      `
      WITH wochen AS (
        SELECT
          b.kw,
          COALESCE(SUM(CASE WHEN k.typ = 'Einnahme' THEN b.betrag ELSE 0 END), 0)::numeric(12,2) AS einnahmen,
          COALESCE(SUM(CASE WHEN k.typ = 'Ausgabe' THEN b.betrag ELSE 0 END), 0)::numeric(12,2) AS ausgaben,
          COALESCE(SUM(
            CASE
              WHEN k.typ = 'Einnahme' THEN b.betrag
              WHEN k.typ = 'Ausgabe' THEN -b.betrag
              ELSE 0
            END
          ), 0)::numeric(12,2) AS saldo
        FROM cashflow.buchungen b
        JOIN cashflow.kategorien k
          ON k.id = b.kategorie_id
        WHERE b.jahr = $1
          AND k.aktiv = true
          ${bisKwFilter}
        GROUP BY b.kw
      ),
      aggregate AS (
        SELECT
          COALESCE(SUM(einnahmen), 0)::numeric(12,2) AS einnahmen,
          COALESCE(SUM(ausgaben), 0)::numeric(12,2) AS ausgaben,
          COALESCE(SUM(saldo), 0)::numeric(12,2) AS saldo,
          COALESCE(AVG(saldo), 0)::numeric(12,2) AS durchschnitt_saldo,
          COUNT(*)::int AS anzahl_wochen
        FROM wochen
      ),
      beste AS (
        SELECT
          kw AS beste_kw,
          saldo AS bester_saldo
        FROM wochen
        ORDER BY saldo DESC, kw ASC
        LIMIT 1
      ),
      schlechteste AS (
        SELECT
          kw AS schlechteste_kw,
          saldo AS schlechtester_saldo
        FROM wochen
        ORDER BY saldo ASC, kw ASC
        LIMIT 1
      )
      SELECT
        a.einnahmen,
        a.ausgaben,
        a.saldo,
        b.beste_kw,
        b.bester_saldo,
        s.schlechteste_kw,
        s.schlechtester_saldo,
        a.durchschnitt_saldo,
        a.anzahl_wochen
      FROM aggregate a
      LEFT JOIN beste b ON true
      LEFT JOIN schlechteste s ON true
      `,
      params
    );

    const row = result.rows?.[0] || {};

    return res.json({
      jahr,
      bisKw,
      kpis: {
        einnahmen: row.einnahmen || '0.00',
        ausgaben: row.ausgaben || '0.00',
        saldo: row.saldo || '0.00',
        beste_kw: row.beste_kw || null,
        bester_saldo: row.bester_saldo || null,
        schlechteste_kw: row.schlechteste_kw || null,
        schlechtester_saldo: row.schlechtester_saldo || null,
        durchschnitt_saldo: row.durchschnitt_saldo || '0.00',
        anzahl_wochen: row.anzahl_wochen || 0,
      },
    });
  } catch (err) {
    console.error('Fehler GET /api/cashflow/kpis:', err);
    return res.status(500).json({
      message: 'Serverfehler bei Cashflow-KPI-Auswertung.',
    });
  }
});

module.exports = router;