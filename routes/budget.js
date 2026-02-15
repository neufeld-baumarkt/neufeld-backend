// routes/budget.js – Budget API V1 + MS4 (Bookings) + Split-Bookings (Modell A) + Rule-Update + Response-Redaction
// Stand: 31.01.2026 (Backend: Split-Bookings via DB-Funktionen, Child-Schutz im CRUD)
//
// READ:  budget.v_week_summary_global_ytd (read-only)   ✅ NEU
// WRITE: budget.week_budgets (*)                        ✅
// WRITE: budget.bookings                                ✅
// WRITE: budget.booking_splits                           ✅
// WRITE: budget.budget_rules                             ✅
//
// FIXES / HOTPATHS:
// - enforceFilialeForCentral(): Zentralrollen müssen Filiale übergeben
// - Split-Parent darf nicht über /bookings/:id gelöscht werden → 400 + Hinweis
// - Split-Delete läuft über DB-Funktion delete_split_booking(parentId)
//
// NOTE:
// - Responses werden typ-basiert redacted (canReadBookingType/canWriteBookingType)
// - Für Week-Summary wird nach Mutationen neu berechnet

const express = require('express');
const router = express.Router();
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

// ------------------------------
// Helpers
// ------------------------------
const parseIntSafe = (v) => {
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : null;
};

const normalizeFiliale = (f) => {
  if (!f) return null;
  const s = String(f).trim();
  if (!s) return null;
  return s;
};

const resolveFiliale = (req) => {
  // Priorität:
  // 1) query.filiale
  // 2) header x-filiale
  // 3) user.filiale (falls nicht "Alle")
  const q = normalizeFiliale(req.query?.filiale);
  if (q) return q;

  const h = normalizeFiliale(req.headers['x-filiale']);
  if (h) return h;

  const uf = normalizeFiliale(req.user?.filiale);
  if (uf && uf !== 'Alle') return uf;

  return null;
};

const isCentralRole = (role) => {
  return ['Admin', 'Supervisor', 'Manager-1', 'Manager-2', 'Geschäftsführer'].includes(role);
};

const enforceFilialeForCentral = (req, res) => {
  // Zentralrollen müssen Filiale explizit setzen (query/header), sonst ist Kontext unklar.
  const role = req.user?.role;
  if (isCentralRole(role)) {
    const f = normalizeFiliale(req.query?.filiale) || normalizeFiliale(req.headers['x-filiale']);
    if (!f) {
      res.status(400).json({
        message:
          'Für Zentralrollen ist filiale erforderlich (Query ?filiale=... oder Header x-filiale).',
      });
      return false;
    }
  }
  return true;
};

const canReadBookingType = (role, typ) => {
  // Placeholder – ggf. erweitert in Rules
  // Aktuell: alles lesbar für alle Rollen, aber Redaction kann typabhängig erfolgen.
  return true;
};

const canWriteBookingType = (role, typ) => {
  // Minimalregel: Filiale darf nur eigenes (oder bei Zentral alles).
  // Feineres Regelwerk über /rules.
  return true;
};

const fetchWeekSummary = async ({ filiale, jahr, kw }) => {
  const r = await pool.query(
    `
      SELECT *
      FROM budget.v_week_summary
      WHERE filiale = $1 AND jahr = $2 AND kw = $3
      LIMIT 1
    `,
    [filiale, jahr, kw]
  );
  return r.rows?.[0] || null;
};

const fetchWeekSummaryGlobalYTD = async ({ jahr, kw }) => {
  const r = await pool.query(
    `
      SELECT *
      FROM budget.v_week_summary_global_ytd
      WHERE jahr = $1 AND kw = $2
      LIMIT 1
    `,
    [jahr, kw]
  );
  return r.rows?.[0] || null;
};

// ------------------------------
// RULES
// ------------------------------

// GET /api/budget/rules?jahr=2026&kw=2[&filiale=Münster]
router.get('/rules', verifyToken(), async (req, res) => {
  if (!enforceFilialeForCentral(req, res)) return;

  const filiale = resolveFiliale(req);
  const jahr = parseIntSafe(req.query?.jahr);
  const kw = parseIntSafe(req.query?.kw);

  if (!jahr || !kw) return res.status(400).json({ message: 'jahr und kw sind erforderlich.' });

  try {
    const result = await pool.query(
      `
        SELECT *
        FROM budget.budget_rules
        WHERE jahr = $1 AND kw = $2
        ORDER BY id ASC
      `,
      [jahr, kw]
    );

    return res.json(result.rows || []);
  } catch (e) {
    console.error('Fehler GET /api/budget/rules:', e.message);
    return res.status(500).json({ message: 'Serverfehler (Rules GET).' });
  }
});

// POST /api/budget/rules
router.post('/rules', verifyToken(), async (req, res) => {
  if (!enforceFilialeForCentral(req, res)) return;

  const { jahr, kw, rules } = req.body || {};
  const _jahr = parseIntSafe(jahr);
  const _kw = parseIntSafe(kw);

  if (!_jahr || !_kw) return res.status(400).json({ message: 'jahr und kw sind erforderlich.' });
  if (!Array.isArray(rules)) return res.status(400).json({ message: 'rules muss ein Array sein.' });

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // alte Regeln entfernen
    await client.query(`DELETE FROM budget.budget_rules WHERE jahr = $1 AND kw = $2`, [_jahr, _kw]);

    // neue Regeln einfügen
    for (const r of rules) {
      await client.query(
        `
          INSERT INTO budget.budget_rules (
            jahr, kw, typ, key, value, created_at
          ) VALUES ($1, $2, $3, $4, $5, NOW())
        `,
        [_jahr, _kw, r.typ || null, r.key || null, r.value || null]
      );
    }

    await client.query('COMMIT');
    return res.json({ ok: true });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('Fehler POST /api/budget/rules:', e.message);
    return res.status(500).json({ message: 'Serverfehler (Rules POST).' });
  } finally {
    client.release();
  }
});

// ------------------------------
// WEEK SUMMARY
// ------------------------------

// GET /api/budget/week-summary?jahr=2026&kw=2&filiale=Münster
router.get('/week-summary', verifyToken(), async (req, res) => {
  if (!enforceFilialeForCentral(req, res)) return;

  const filiale = resolveFiliale(req);
  const jahr = parseIntSafe(req.query?.jahr);
  const kw = parseIntSafe(req.query?.kw);

  if (!filiale) return res.status(400).json({ message: 'Filiale konnte nicht ermittelt werden.' });
  if (!jahr || !kw) return res.status(400).json({ message: 'jahr und kw sind erforderlich.' });

  try {
    const summary = await fetchWeekSummary({ filiale, jahr, kw });
    return res.json(summary || {});
  } catch (e) {
    console.error('Fehler GET /api/budget/week-summary:', e.message);
    return res.status(500).json({ message: 'Serverfehler (Week Summary GET).' });
  }
});

// GET /api/budget/week-summary-global-ytd?jahr=2026&kw=2
router.get('/week-summary-global-ytd', verifyToken(), async (req, res) => {
  const jahr = parseIntSafe(req.query?.jahr);
  const kw = parseIntSafe(req.query?.kw);
  if (!jahr || !kw) return res.status(400).json({ message: 'jahr und kw sind erforderlich.' });

  try {
    const summary = await fetchWeekSummaryGlobalYTD({ jahr, kw });
    return res.json(summary || {});
  } catch (e) {
    console.error('Fehler GET /api/budget/week-summary-global-ytd:', e.message);
    return res.status(500).json({ message: 'Serverfehler (Global YTD Summary GET).' });
  }
});

// ------------------------------
// BOOKINGS
// ------------------------------

// GET /api/budget/bookings?jahr=2026&kw=2[&filiale=Münster]
router.get('/bookings', verifyToken(), async (req, res) => {
  const { role } = req.user || {};

  if (!enforceFilialeForCentral(req, res)) return;

  const filiale = resolveFiliale(req);
  const jahr = parseIntSafe(req.query?.jahr);
  const kw = parseIntSafe(req.query?.kw);

  if (!filiale) return res.status(400).json({ message: 'Filiale konnte nicht ermittelt werden.' });
  if (!jahr || !kw) return res.status(400).json({ message: 'jahr und kw sind erforderlich.' });

  try {
    const result = await pool.query(
      `
        SELECT
          b.*,
          EXISTS (
            SELECT 1
            FROM budget.booking_splits bs
            WHERE bs.parent_booking_id = b.id
          ) AS has_splits
        FROM budget.bookings b
        JOIN budget.week_budgets wb ON wb.id = b.week_budget_id
        WHERE wb.filiale = $1 AND wb.jahr = $2 AND wb.kw = $3
        ORDER BY
          b.datum DESC,
          b.created_at DESC,
          b.id DESC
      `,
      [filiale, jahr, kw]
    );

    const safe = result.rows.filter((r) => canReadBookingType(role, r.typ));
    return res.json(safe);
  } catch (e) {
    console.error('Fehler GET /api/budget/bookings:', e.message);
    return res.status(500).json({ message: 'Serverfehler (Bookings GET).' });
  }
});

// POST /api/budget/bookings
router.post('/bookings', verifyToken(), async (req, res) => {
  const { role } = req.user || {};

  if (!enforceFilialeForCentral(req, res)) return;

  const filiale = resolveFiliale(req);
  const jahr = parseIntSafe(req.query?.jahr);
  const kw = parseIntSafe(req.query?.kw);

  if (!filiale) return res.status(400).json({ message: 'Filiale konnte nicht ermittelt werden.' });
  if (!jahr || !kw) return res.status(400).json({ message: 'jahr und kw sind erforderlich.' });

  const {
    datum,
    typ,
    betrag,
    gesamtbetrag,
    lieferant,
    aktion_nr,
    beschreibung,
    von_filiale,
    an_filiale,
    status,
    source,
  } = req.body || {};

  if (!canWriteBookingType(role, typ)) return res.status(403).json({ message: 'Keine Rechte.' });
  if (!typ) return res.status(400).json({ message: 'typ ist erforderlich.' });

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // WeekBudget holen/erstellen
    let wb = await client.query(
      `
        SELECT id
        FROM budget.week_budgets
        WHERE filiale = $1 AND jahr = $2 AND kw = $3
        LIMIT 1
      `,
      [filiale, jahr, kw]
    );

    let week_budget_id = wb.rows?.[0]?.id;

    if (!week_budget_id) {
      const ins = await client.query(
        `
          INSERT INTO budget.week_budgets (filiale, jahr, kw, created_at)
          VALUES ($1, $2, $3, NOW())
          RETURNING id
        `,
        [filiale, jahr, kw]
      );
      week_budget_id = ins.rows?.[0]?.id;
    }

    const insBooking = await client.query(
      `
        INSERT INTO budget.bookings (
          week_budget_id,
          datum,
          typ,
          betrag,
          gesamtbetrag,
          lieferant,
          aktion_nr,
          beschreibung,
          von_filiale,
          an_filiale,
          status,
          created_by,
          created_at,
          source
        ) VALUES (
          $1, $2, $3, $4, $5,
          $6, $7, $8, $9, $10,
          $11, $12, NOW(), $13
        )
        RETURNING *
      `,
      [
        week_budget_id,
        datum || null,
        typ,
        betrag || 0,
        gesamtbetrag || null,
        lieferant || null,
        aktion_nr || null,
        beschreibung || null,
        von_filiale || null,
        an_filiale || null,
        status || 'offen',
        req.user?.name || null,
        source || null,
      ]
    );

    const created = insBooking.rows?.[0];
    const summary = await fetchWeekSummary({ filiale, jahr, kw });

    await client.query('COMMIT');
    return res.json({ booking: created, week_summary: summary });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('Fehler POST /api/budget/bookings:', e.message);
    return res.status(500).json({ message: 'Serverfehler (Bookings POST).' });
  } finally {
    client.release();
  }
});

// PUT /api/budget/bookings/:id
router.put('/bookings/:id', verifyToken(), async (req, res) => {
  const { role } = req.user || {};

  if (!enforceFilialeForCentral(req, res)) return;

  const filiale = resolveFiliale(req);
  const jahr = parseIntSafe(req.query?.jahr);
  const kw = parseIntSafe(req.query?.kw);
  const id = req.params?.id;

  if (!filiale) return res.status(400).json({ message: 'Filiale konnte nicht ermittelt werden.' });
  if (!jahr || !kw) return res.status(400).json({ message: 'jahr und kw sind erforderlich.' });
  if (!id) return res.status(400).json({ message: 'id ist erforderlich.' });

  const {
    datum,
    typ,
    betrag,
    gesamtbetrag,
    lieferant,
    aktion_nr,
    beschreibung,
    von_filiale,
    an_filiale,
    status,
    source,
  } = req.body || {};

  if (!canWriteBookingType(role, typ)) return res.status(403).json({ message: 'Keine Rechte.' });

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Booking laden + Schutzinfo
    const cur = await client.query(
      `
        SELECT
          b.*,
          EXISTS (
            SELECT 1
            FROM budget.booking_splits bs
            WHERE bs.parent_booking_id = b.id
          ) AS has_splits
        FROM budget.bookings b
        JOIN budget.week_budgets wb ON wb.id = b.week_budget_id
        WHERE b.id = $1 AND wb.filiale = $2 AND wb.jahr = $3 AND wb.kw = $4
        LIMIT 1
      `,
      [id, filiale, jahr, kw]
    );

    const booking = cur.rows?.[0];
    if (!booking) {
      await client.query('ROLLBACK');
      return res.status(404).json({ message: 'Buchung nicht gefunden.' });
    }

    if (booking.parent_booking_id) {
      await client.query('ROLLBACK');
      return res.status(400).json({ message: 'Diese Buchung ist ein Split-Child und darf hier nicht geändert werden.' });
    }

    if (booking.has_splits) {
      await client.query('ROLLBACK');
      return res.status(400).json({ message: 'Diese Buchung ist ein Split-Parent (hat Splits) und darf hier nicht geändert werden. Nutze Split-Update.' });
    }

    const upd = await client.query(
      `
        UPDATE budget.bookings
        SET
          datum = $2,
          typ = $3,
          betrag = $4,
          gesamtbetrag = $5,
          lieferant = $6,
          aktion_nr = $7,
          beschreibung = $8,
          von_filiale = $9,
          an_filiale = $10,
          status = $11,
          source = $12
        WHERE id = $1
        RETURNING *
      `,
      [
        id,
        datum || null,
        typ || booking.typ,
        betrag ?? booking.betrag,
        gesamtbetrag ?? booking.gesamtbetrag,
        lieferant ?? booking.lieferant,
        aktion_nr ?? booking.aktion_nr,
        beschreibung ?? booking.beschreibung,
        von_filiale ?? booking.von_filiale,
        an_filiale ?? booking.an_filiale,
        status ?? booking.status,
        source ?? booking.source,
      ]
    );

    const updated = upd.rows?.[0];
    const summary = await fetchWeekSummary({ filiale, jahr, kw });

    await client.query('COMMIT');
    return res.json({ booking: updated, week_summary: summary });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('Fehler PUT /api/budget/bookings/:id:', e.message);
    return res.status(500).json({ message: 'Serverfehler (Bookings PUT).' });
  } finally {
    client.release();
  }
});

// DELETE /api/budget/bookings/:id
router.delete('/bookings/:id', verifyToken(), async (req, res) => {
  const { role } = req.user || {};

  if (!enforceFilialeForCentral(req, res)) return;

  const filiale = resolveFiliale(req);
  const jahr = parseIntSafe(req.query?.jahr);
  const kw = parseIntSafe(req.query?.kw);
  const id = req.params?.id;

  if (!filiale) return res.status(400).json({ message: 'Filiale konnte nicht ermittelt werden.' });
  if (!jahr || !kw) return res.status(400).json({ message: 'jahr und kw sind erforderlich.' });
  if (!id) return res.status(400).json({ message: 'id ist erforderlich.' });

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const cur = await client.query(
      `
        SELECT
          b.*,
          EXISTS (
            SELECT 1
            FROM budget.booking_splits bs
            WHERE bs.parent_booking_id = b.id
          ) AS has_splits
        FROM budget.bookings b
        JOIN budget.week_budgets wb ON wb.id = b.week_budget_id
        WHERE b.id = $1 AND wb.filiale = $2 AND wb.jahr = $3 AND wb.kw = $4
        LIMIT 1
      `,
      [id, filiale, jahr, kw]
    );

    const booking = cur.rows?.[0];
    if (!booking) {
      await client.query('ROLLBACK');
      return res.status(404).json({ message: 'Buchung nicht gefunden.' });
    }

    if (booking.parent_booking_id) {
      await client.query('ROLLBACK');
      return res.status(400).json({ message: 'Diese Buchung ist ein Split-Child und darf hier nicht gelöscht werden.' });
    }

    if (booking.has_splits) {
      await client.query('ROLLBACK');
      return res.status(400).json({
        message:
          'Diese Buchung ist ein Split-Parent (hat Splits) und darf hier nicht gelöscht werden. Nutze /bookings/split.',
      });
    }

    if (!canWriteBookingType(role, booking.typ)) {
      await client.query('ROLLBACK');
      return res.status(403).json({ message: 'Keine Rechte.' });
    }

    await client.query(`DELETE FROM budget.bookings WHERE id = $1`, [id]);

    const summary = await fetchWeekSummary({ filiale, jahr, kw });

    await client.query('COMMIT');
    return res.json({ ok: true, week_summary: summary });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('Fehler DELETE /api/budget/bookings/:id:', e.message);
    return res.status(500).json({ message: 'Serverfehler (Bookings DELETE).' });
  } finally {
    client.release();
  }
});

// DELETE /api/budget/bookings/split/:parentId
router.delete('/bookings/split/:parentId', verifyToken(), async (req, res) => {
  const { role } = req.user || {};

  if (!enforceFilialeForCentral(req, res)) return;

  const filiale = resolveFiliale(req);
  const jahr = parseIntSafe(req.query?.jahr);
  const kw = parseIntSafe(req.query?.kw);
  const parentId = req.params?.parentId;

  if (!filiale) return res.status(400).json({ message: 'Filiale konnte nicht ermittelt werden.' });
  if (!jahr || !kw) return res.status(400).json({ message: 'jahr und kw sind erforderlich.' });
  if (!parentId) return res.status(400).json({ message: 'parentId ist erforderlich.' });

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Parent validieren + Quellfiliale prüfen
    const parentRes = await client.query(
      `
        SELECT b.id, b.parent_booking_id, wb.filiale
        FROM budget.bookings b
        JOIN budget.week_budgets wb ON wb.id = b.week_budget_id
        WHERE b.id = $1
        LIMIT 1
      `,
      [parentId]
    );

    const parent = parentRes.rows?.[0];
    if (!parent) {
      await client.query('ROLLBACK');
      return res.status(404).json({ message: 'Split-Parent nicht gefunden.' });
    }

    if (parent.parent_booking_id) {
      await client.query('ROLLBACK');
      return res.status(400).json({ message: 'Diese ID gehört zu einem Split-Child und darf hier nicht gelöscht werden.' });
    }

    // Filial-User darf nur eigenen Parent löschen
    if (!isCentralRole(role) && parent.filiale !== filiale) {
      await client.query('ROLLBACK');
      return res.status(403).json({ message: 'Keine Rechte für diese Filiale.' });
    }

    // Betroffene Ziel-Filialen für Summary ermitteln
    const targetsRes = await client.query(
      `
        SELECT DISTINCT twb.filiale
        FROM budget.booking_splits bs
        JOIN budget.week_budgets twb ON twb.id = bs.target_week_budget_id
        WHERE bs.parent_booking_id = $1
      `,
      [parentId]
    );

    const targetFilialen = (targetsRes.rows || []).map((r) => r.filiale).filter(Boolean);

    // Zentrale Löschlogik in DB (Parent löschen, Cascade für Children + Splits)
    await client.query(`SELECT budget.delete_split_booking($1::uuid)`, [parentId]);

    // Week Summary: Quelle + Ziele
    const summaries = {};
    summaries[filiale] = await fetchWeekSummary({ filiale, jahr, kw });

    for (const tf of targetFilialen) {
      if (tf && tf !== filiale) {
        summaries[tf] = await fetchWeekSummary({ filiale: tf, jahr, kw });
      }
    }

    await client.query('COMMIT');
    return res.json({ ok: true, week_summaries: summaries });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('Fehler DELETE /api/budget/bookings/split/:parentId:', e.message);
    return res.status(500).json({ message: 'Serverfehler (Split Bookings DELETE).' });
  } finally {
    client.release();
  }
});

module.exports = router;
