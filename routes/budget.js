// routes/budget.js – Budget API V1 (SSoT: 18.01.2026)
// READ:  budget.v_week_summary (read-only)
// WRITE: budget.week_budgets (umsatz_vorwoche_brutto per UPSERT)
// Regeln:
// - Backend rechnet NICHT selbst, liefert View 1:1
// - View wird NICHT beschrieben
// - GET: Standard filiale aus JWT; Admin/Supervisor optional ?filiale=XYZ
// - PUT: nur Admin/Supervisor; UPSERT (filiale,jahr,kw); Snapshot aus budget.week_rules (bei INSERT)

const express = require('express');
const router = express.Router();

const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

const ROLLEN_GLOBAL_FILIALE = ['Admin', 'Supervisor'];

function isGlobalRole(role) {
  return ROLLEN_GLOBAL_FILIALE.includes(role);
}

function parseIntStrict(value) {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  if (!Number.isInteger(n)) return null;
  return n;
}

function parseNumericNonNegative(value) {
  if (value === null || value === undefined) return null;

  // akzeptiert Zahl oder String "11900.00"
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  if (n < 0) return null;

  // wir lassen DB numeric(14,2) runden/validieren – hier nur Plausibilität
  return n;
}

function resolveFiliale(req) {
  // Standard: filiale aus JWT
  // Admin/Supervisor: optional ?filiale=XYZ
  const { role, filiale: tokenFiliale } = req.user || {};
  const queryFiliale = typeof req.query.filiale === 'string' ? req.query.filiale.trim() : '';

  if (isGlobalRole(role) && queryFiliale) return queryFiliale;
  return tokenFiliale || null;
}

// =====================================================
// GET /api/budget/:jahr/:kw
// =====================================================
router.get('/:jahr/:kw', verifyToken(), async (req, res) => {
  const jahr = parseIntStrict(req.params.jahr);
  const kw = parseIntStrict(req.params.kw);
  const filiale = resolveFiliale(req);

  if (!jahr || !kw) {
    return res.status(400).json({ message: 'Ungültige Parameter: jahr und kw müssen Integer sein.' });
  }
  if (kw < 1 || kw > 53) {
    return res.status(400).json({ message: 'Ungültige Kalenderwoche (kw muss 1..53 sein).' });
  }
  if (!filiale) {
    return res.status(400).json({ message: 'Filiale fehlt im Token (oder Query) – Zugriff nicht möglich.' });
  }

  try {
    const result = await pool.query(
      `
        SELECT *
        FROM budget.v_week_summary
        WHERE filiale = $1 AND jahr = $2 AND kw = $3
        LIMIT 1
      `,
      [filiale, jahr, kw]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({
        message: 'Keine Budgetdaten gefunden (v_week_summary).',
        filiale,
        jahr,
        kw
      });
    }

    // View 1:1 zurückgeben – keine Zusatzberechnungen
    return res.json(result.rows[0]);
  } catch (err) {
    console.error('Fehler GET /api/budget/:jahr/:kw:', err.message);
    return res.status(500).json({ message: 'Serverfehler (Budget READ).' });
  }
});

// =====================================================
// PUT /api/budget/:jahr/:kw/umsatz
// Body: { umsatz_vorwoche_brutto: number|string }
// Rechte: Admin, Supervisor
// Verhalten:
// - UPSERT auf budget.week_budgets (filiale,jahr,kw)
// - Bei INSERT: prozentsatz_snapshot + mwst_faktor_snapshot aus budget.week_rules
// - Bei UPDATE: nur umsatz_vorwoche_brutto + updated_at, Snapshots bleiben wie sie waren
// =====================================================
router.put('/:jahr/:kw/umsatz', verifyToken(), async (req, res) => {
  const { role } = req.user || {};
  if (!isGlobalRole(role)) {
    return res.status(403).json({ message: 'Zugriff verweigert. Erforderliche Rolle: Admin oder Supervisor.' });
  }

  const jahr = parseIntStrict(req.params.jahr);
  const kw = parseIntStrict(req.params.kw);
  const filiale = resolveFiliale(req);

  if (!jahr || !kw) {
    return res.status(400).json({ message: 'Ungültige Parameter: jahr und kw müssen Integer sein.' });
  }
  if (kw < 1 || kw > 53) {
    return res.status(400).json({ message: 'Ungültige Kalenderwoche (kw muss 1..53 sein).' });
  }
  if (!filiale) {
    return res.status(400).json({ message: 'Filiale fehlt (Token oder Query ?filiale=XYZ).' });
  }

  const umsatz = parseNumericNonNegative(req.body?.umsatz_vorwoche_brutto);
  if (umsatz === null) {
    return res.status(400).json({
      message: 'Ungültiger Body: umsatz_vorwoche_brutto muss eine Zahl >= 0 sein.'
    });
  }

  try {
    // 1) prozentsatz + mwst_faktor aus week_rules lesen (SSoT/DB-SSoT)
    const ruleRes = await pool.query(
      `
        SELECT prozentsatz, mwst_faktor
        FROM budget.week_rules
        WHERE jahr = $1 AND kw = $2
        LIMIT 1
      `,
      [jahr, kw]
    );

    if (ruleRes.rows.length === 0) {
      return res.status(404).json({
        message: 'Keine week_rules gefunden für jahr/kw – UPSERT nicht möglich.',
        jahr,
        kw
      });
    }

    const { prozentsatz, mwst_faktor } = ruleRes.rows[0];

    // 2) UPSERT (filiale,jahr,kw)
    // - INSERT setzt Snapshots aus week_rules
    // - UPDATE setzt nur umsatz + updated_at (Snapshots bleiben unverändert)
    const upsertRes = await pool.query(
      `
        INSERT INTO budget.week_budgets
          (filiale, jahr, kw, umsatz_vorwoche_brutto, prozentsatz_snapshot, mwst_faktor_snapshot, updated_at)
        VALUES
          ($1, $2, $3, $4, $5, $6, NOW())
        ON CONFLICT (filiale, jahr, kw)
        DO UPDATE SET
          umsatz_vorwoche_brutto = EXCLUDED.umsatz_vorwoche_brutto,
          updated_at = NOW()
        RETURNING id
      `,
      [filiale, jahr, kw, umsatz, prozentsatz, mwst_faktor]
    );

    const weekBudgetId = upsertRes.rows?.[0]?.id;

    // 3) Antwort: View-Datensatz zurückgeben (Wahrheit für Anzeige)
    //    (über filiale/jahr/kw – id ist in View ebenfalls vorhanden, aber Filter ist eindeutig)
    const viewRes = await pool.query(
      `
        SELECT *
        FROM budget.v_week_summary
        WHERE filiale = $1 AND jahr = $2 AND kw = $3
        LIMIT 1
      `,
      [filiale, jahr, kw]
    );

    if (viewRes.rows.length === 0) {
      // sollte praktisch nicht passieren, aber sauber bleiben
      return res.status(200).json({
        message: 'Umsatz gespeichert, aber View lieferte keinen Datensatz.',
        id: weekBudgetId || null,
        filiale,
        jahr,
        kw
      });
    }

    return res.json(viewRes.rows[0]);
  } catch (err) {
    console.error('Fehler PUT /api/budget/:jahr/:kw/umsatz:', err.message);
    return res.status(500).json({ message: 'Serverfehler (Budget WRITE).' });
  }
});

module.exports = router;
