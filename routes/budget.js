// routes/budget.js – Budget API V1 + MS4 (Bookings)  (SSoT: 18.–20.01.2026)
// READ:  budget.v_week_summary (read-only)
// WRITE: budget.week_budgets (umsatz_vorwoche_brutto per UPSERT)
// WRITE: budget.bookings     (MS4 CRUD)
// Regeln:
// - Backend rechnet NICHT selbst, liefert View 1:1
// - View wird NICHT beschrieben
// - Filialzugriff: Filiale-User nur eigene Filiale; Zentrale-Rollen optional ?filiale=XYZ
// - Umsatz-PUT: nur Admin/Supervisor
// - Bookings-Rechte (MS4):
//   - typ='bestellung'   : Filiale (eigene) + Supervisor + Admin dürfen schreiben; Filiale darf auch DELETE (Storno/Vertipper)
//   - typ='aktionsvorab' : Schreiben/Lesen: Manager-1, Geschäftsführer, Supervisor, Admin; Filialen nur lesen
//   - typ='abgabe'/'korrektur': Schreiben/Lesen: Supervisor, Admin; Filialen nur lesen

const express = require('express');
const router = express.Router();

const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

// =====================================================
// Rollen / Rechte
// =====================================================

const ROLE_ADMIN = 'Admin';
const ROLE_SUPERVISOR = 'Supervisor';
const ROLE_MANAGER_1 = 'Manager-1';
const ROLE_GF = 'Geschäftsführer';

const ROLES_GLOBAL_FILIALE = [ROLE_ADMIN, ROLE_SUPERVISOR, ROLE_MANAGER_1, ROLE_GF]; // dürfen ?filiale=XYZ nutzen (read); write je Typ separat
const ROLES_UMSATZ_WRITE = [ROLE_ADMIN, ROLE_SUPERVISOR];

const BOOKING_TYPES = ['bestellung', 'aktionsvorab', 'abgabe', 'korrektur'];

function isGlobalFilialeRole(role) {
  return ROLES_GLOBAL_FILIALE.includes(role);
}

function canWriteBookingType(role, bookingType) {
  if (!BOOKING_TYPES.includes(bookingType)) return false;

  if (bookingType === 'bestellung') {
    // Filiale darf (eigene Filiale). Supervisor/Admin dürfen.
    return role === ROLE_ADMIN || role === ROLE_SUPERVISOR || !isGlobalFilialeRole(role); // "nicht global" = Filialrolle
  }

  if (bookingType === 'aktionsvorab') {
    return role === ROLE_ADMIN || role === ROLE_SUPERVISOR || role === ROLE_MANAGER_1 || role === ROLE_GF;
  }

  // abgabe / korrektur
  return role === ROLE_ADMIN || role === ROLE_SUPERVISOR;
}

function canReadBookingType(role, bookingType) {
  if (!BOOKING_TYPES.includes(bookingType)) return false;

  // Lesen: grundsätzlich filialbezogen. Filialen dürfen Aktionen lesen, aber nicht schreiben.
  // Zentrale dürfen immer lesen (filialbezogen via token oder ?filiale=XYZ).
  if (isGlobalFilialeRole(role)) return true;

  // Filialrolle:
  // - bestellung: lesen ja
  // - aktionsvorab: lesen ja
  // - abgabe/korrektur: lesen ja (wenn du das später einschränken willst: hier anpassen)
  return true;
}

function isFilialeRole(role) {
  // In eurem System sind "Zentrale"-Rollen bekannt; alles andere behandeln wir als Filiale-User.
  return !isGlobalFilialeRole(role);
}

// =====================================================
// Parser / Helper
// =====================================================

function parseIntStrict(value) {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  if (!Number.isInteger(n)) return null;
  return n;
}

function parseNumericNonNegative(value) {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  if (n < 0) return null;
  return n;
}

function parseNumericNonZero(value) {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  if (n === 0) return null;
  return n;
}

function parseTextRequired(value) {
  if (typeof value !== 'string') return null;
  const t = value.trim();
  if (!t) return null;
  return t;
}

function parseTextOptional(value) {
  if (value === null || value === undefined) return null;
  if (typeof value !== 'string') return null;
  const t = value.trim();
  return t ? t : null;
}

function parseDateOptional(value) {
  if (value === null || value === undefined) return null;
  if (typeof value !== 'string') return null;
  const t = value.trim();
  if (!t) return null;

  // simple YYYY-MM-DD check (DB validiert endgültig)
  // akzeptiert auch ISO mit Zeit, aber wir nehmen nur YYYY-MM-DD
  const m = t.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return null;
  return t;
}

function resolveFiliale(req) {
  // Standard: filiale aus JWT
  // Zentrale-Rollen: optional ?filiale=XYZ
  const { role, filiale: tokenFiliale } = req.user || {};
  const queryFiliale = typeof req.query.filiale === 'string' ? req.query.filiale.trim() : '';

  if (isGlobalFilialeRole(role) && queryFiliale) return queryFiliale;
  return tokenFiliale || null;
}

function getActor(req) {
  const u = req.user || {};
  return (typeof u.name === 'string' && u.name.trim()) ? u.name.trim() : (u.id ? String(u.id) : 'unknown');
}

// =====================================================
// WeekBudget sicherstellen (für Bookings)
// - Bookings referenzieren week_budgets.id (FK)
// - Falls week_budgets für filiale/jahr/kw noch nicht existiert:
//   -> anlegen mit Snapshots aus week_rules, umsatz bleibt NULL
// =====================================================
async function ensureWeekBudgetId(client, filiale, jahr, kw) {
  // 1) exists?
  const existsRes = await client.query(
    `
      SELECT id
      FROM budget.week_budgets
      WHERE filiale = $1 AND jahr = $2 AND kw = $3
      LIMIT 1
    `,
    [filiale, jahr, kw]
  );

  if (existsRes.rows.length > 0) return existsRes.rows[0].id;

  // 2) week_rules lesen
  const ruleRes = await client.query(
    `
      SELECT prozentsatz, mwst_faktor
      FROM budget.week_rules
      WHERE jahr = $1 AND kw = $2
      LIMIT 1
    `,
    [jahr, kw]
  );

  if (ruleRes.rows.length === 0) {
    const err = new Error('NO_WEEK_RULES');
    err.meta = { jahr, kw };
    throw err;
  }

  const { prozentsatz, mwst_faktor } = ruleRes.rows[0];

  // 3) insert (umsatz NULL), conflict-safe
  const insRes = await client.query(
    `
      INSERT INTO budget.week_budgets
        (filiale, jahr, kw, umsatz_vorwoche_brutto, prozentsatz_snapshot, mwst_faktor_snapshot, updated_at)
      VALUES
        ($1, $2, $3, NULL, $4, $5, NOW())
      ON CONFLICT (filiale, jahr, kw)
      DO NOTHING
      RETURNING id
    `,
    [filiale, jahr, kw, prozentsatz, mwst_faktor]
  );

  if (insRes.rows.length > 0) return insRes.rows[0].id;

  // 4) falls parallel angelegt wurde: nochmal select
  const againRes = await client.query(
    `
      SELECT id
      FROM budget.week_budgets
      WHERE filiale = $1 AND jahr = $2 AND kw = $3
      LIMIT 1
    `,
    [filiale, jahr, kw]
  );

  if (againRes.rows.length === 0) {
    const err = new Error('WEEK_BUDGET_CREATE_FAILED');
    err.meta = { filiale, jahr, kw };
    throw err;
  }

  return againRes.rows[0].id;
}

async function fetchWeekSummary(client, filiale, jahr, kw) {
  const viewRes = await client.query(
    `
      SELECT *
      FROM budget.v_week_summary
      WHERE filiale = $1 AND jahr = $2 AND kw = $3
      LIMIT 1
    `,
    [filiale, jahr, kw]
  );
  return viewRes.rows.length ? viewRes.rows[0] : null;
}

// =====================================================
// GET /api/budget/:jahr/:kw  (V1)
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

    return res.json(result.rows[0]);
  } catch (err) {
    console.error('Fehler GET /api/budget/:jahr/:kw:', err.message);
    return res.status(500).json({ message: 'Serverfehler (Budget READ).' });
  }
});

// =====================================================
// PUT /api/budget/:jahr/:kw/umsatz  (V1)
// Body: { umsatz_vorwoche_brutto: number|string }
// Rechte: Admin, Supervisor
// =====================================================
router.put('/:jahr/:kw/umsatz', verifyToken(), async (req, res) => {
  const { role } = req.user || {};
  if (!ROLES_UMSATZ_WRITE.includes(role)) {
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

// =====================================================
// MS4 – BOOKINGS
// =====================================================

// -----------------------------------------------------
// GET /api/budget/:jahr/:kw/bookings
// - Filiale: nur eigene Filiale
// - Zentrale: optional ?filiale=XYZ
// - Liefert alle Bookings der Woche (week_budget) für diese Filiale
// Optional: ?typ=bestellung|aktionsvorab|abgabe|korrektur
// -----------------------------------------------------
router.get('/:jahr/:kw/bookings', verifyToken(), async (req, res) => {
  const { role } = req.user || {};
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

  const typFilterRaw = typeof req.query.typ === 'string' ? req.query.typ.trim() : '';
  const typFilter = typFilterRaw ? typFilterRaw : null;
  if (typFilter && !BOOKING_TYPES.includes(typFilter)) {
    return res.status(400).json({ message: `Ungültiger typ-Filter. Erlaubt: ${BOOKING_TYPES.join(', ')}` });
  }

  try {
    const client = await pool.connect();
    try {
      const weekBudgetIdRes = await client.query(
        `
          SELECT id
          FROM budget.week_budgets
          WHERE filiale = $1 AND jahr = $2 AND kw = $3
          LIMIT 1
        `,
        [filiale, jahr, kw]
      );

      if (weekBudgetIdRes.rows.length === 0) {
        // Noch keine WeekBudget-Zeile angelegt -> dann gibt es auch keine Bookings
        return res.json({
          filiale,
          jahr,
          kw,
          bookings: [],
          week_summary: await fetchWeekSummary(client, filiale, jahr, kw)
        });
      }

      const weekBudgetId = weekBudgetIdRes.rows[0].id;

      const bookingsRes = await client.query(
        `
          SELECT
            b.id,
            b.week_budget_id,
            b.datum,
            b.typ,
            b.betrag,
            b.lieferant,
            b.aktion_nr,
            b.beschreibung,
            b.von_filiale,
            b.an_filiale,
            b.status,
            b.bestaetigt_von,
            b.bestaetigt_am,
            b.created_by,
            b.created_at
          FROM budget.bookings b
          WHERE b.week_budget_id = $1
            AND ($2::text IS NULL OR b.typ = $2::text)
          ORDER BY b.created_at DESC
        `,
        [weekBudgetId, typFilter]
      );

      // Typ-spezifische Sicht: Filiale darf alles lesen (laut Vorgabe). Falls du abgabe/korrektur später verstecken willst:
      // -> hier nach role/typ filtern.
      const filtered = bookingsRes.rows.filter((row) => canReadBookingType(role, row.typ));

      const weekSummary = await fetchWeekSummary(client, filiale, jahr, kw);

      return res.json({
        filiale,
        jahr,
        kw,
        bookings: filtered,
        week_summary: weekSummary
      });
    } finally {
      client.release();
    }
  } catch (err) {
    console.error('Fehler GET /api/budget/:jahr/:kw/bookings:', err.message);
    return res.status(500).json({ message: 'Serverfehler (Bookings READ).' });
  }
});

// -----------------------------------------------------
// POST /api/budget/:jahr/:kw/bookings
// Body (minimal):
//  - typ (required)
//  - betrag (required, != 0)  (negativ belastet Budget; positiv = Gutschrift/Korrektur)
//  - beschreibung (required)
// Optional:
//  - datum (YYYY-MM-DD)
//  - lieferant, aktion_nr, von_filiale, an_filiale
// Rechte: typ-spezifisch (siehe oben)
// -----------------------------------------------------
router.post('/:jahr/:kw/bookings', verifyToken(), async (req, res) => {
  const { role, filiale: tokenFiliale } = req.user || {};
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

  const typ = parseTextRequired(req.body?.typ);
  if (!typ || !BOOKING_TYPES.includes(typ)) {
    return res.status(400).json({ message: `Ungültiger Body: typ muss einer von ${BOOKING_TYPES.join(', ')} sein.` });
  }

  // Filiale darf nur eigene Filiale schreiben (bei bestellung)
  if (isFilialeRole(role) && filiale !== tokenFiliale) {
    return res.status(403).json({ message: 'Zugriff verweigert: Filialuser darf nur in eigener Filiale schreiben.' });
  }

  if (!canWriteBookingType(role, typ)) {
    return res.status(403).json({ message: `Zugriff verweigert: Rolle darf typ='${typ}' nicht anlegen.` });
  }

  const betrag = parseNumericNonZero(req.body?.betrag);
  if (betrag === null) {
    return res.status(400).json({ message: 'Ungültiger Body: betrag muss eine Zahl sein und darf nicht 0 sein.' });
  }

  const beschreibung = parseTextRequired(req.body?.beschreibung);
  if (!beschreibung) {
    return res.status(400).json({ message: 'Ungültiger Body: beschreibung ist Pflicht.' });
  }

  const datum = parseDateOptional(req.body?.datum);
  if (req.body?.datum !== undefined && req.body?.datum !== null && datum === null) {
    return res.status(400).json({ message: 'Ungültiger Body: datum muss YYYY-MM-DD sein (oder weglassen).' });
  }

  const lieferant = parseTextOptional(req.body?.lieferant);
  const aktion_nr = parseTextOptional(req.body?.aktion_nr);
  const von_filiale = parseTextOptional(req.body?.von_filiale);
  const an_filiale = parseTextOptional(req.body?.an_filiale);

  const createdBy = getActor(req);

  try {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      const weekBudgetId = await ensureWeekBudgetId(client, filiale, jahr, kw);

      const insertRes = await client.query(
        `
          INSERT INTO budget.bookings
            (week_budget_id, datum, typ, betrag, lieferant, aktion_nr, beschreibung, von_filiale, an_filiale, created_by)
          VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
          RETURNING
            id, week_budget_id, datum, typ, betrag, lieferant, aktion_nr, beschreibung, von_filiale, an_filiale,
            status, bestaetigt_von, bestaetigt_am, created_by, created_at
        `,
        [weekBudgetId, datum, typ, betrag, lieferant, aktion_nr, beschreibung, von_filiale, an_filiale, createdBy]
      );

      const weekSummary = await fetchWeekSummary(client, filiale, jahr, kw);

      await client.query('COMMIT');
      return res.status(201).json({
        booking: insertRes.rows[0],
        week_summary: weekSummary
      });
    } catch (e) {
      await client.query('ROLLBACK');

      if (e.message === 'NO_WEEK_RULES') {
        return res.status(404).json({
          message: 'Keine week_rules gefunden für jahr/kw – Booking nicht möglich.',
          jahr,
          kw
        });
      }

      console.error('Fehler POST /api/budget/:jahr/:kw/bookings:', e.message);
      return res.status(500).json({ message: 'Serverfehler (Bookings CREATE).' });
    } finally {
      client.release();
    }
  } catch (err) {
    console.error('Fehler POST /api/budget/:jahr/:kw/bookings (connect):', err.message);
    return res.status(500).json({ message: 'Serverfehler (Bookings CREATE).' });
  }
});

// -----------------------------------------------------
// PUT /api/budget/bookings/:id
// - typ ist NICHT änderbar (Rechte hängen daran)
// - Update-Felder:
//   betrag (!=0), beschreibung (pflicht), datum (optional), lieferant/aktion_nr/von_filiale/an_filiale (optional)
// Rechte: typ-spezifisch + filialbezogen (bei bestellung darf Filiale nur eigene)
// -----------------------------------------------------
router.put('/bookings/:id', verifyToken(), async (req, res) => {
  const { role, filiale: tokenFiliale } = req.user || {};
  const bookingId = typeof req.params.id === 'string' ? req.params.id.trim() : '';

  if (!bookingId) {
    return res.status(400).json({ message: 'Ungültige Parameter: id fehlt.' });
  }

  const betrag = parseNumericNonZero(req.body?.betrag);
  if (betrag === null) {
    return res.status(400).json({ message: 'Ungültiger Body: betrag muss eine Zahl sein und darf nicht 0 sein.' });
  }

  const beschreibung = parseTextRequired(req.body?.beschreibung);
  if (!beschreibung) {
    return res.status(400).json({ message: 'Ungültiger Body: beschreibung ist Pflicht.' });
  }

  const datum = parseDateOptional(req.body?.datum);
  if (req.body?.datum !== undefined && req.body?.datum !== null && datum === null) {
    return res.status(400).json({ message: 'Ungültiger Body: datum muss YYYY-MM-DD sein (oder weglassen).' });
  }

  const lieferant = parseTextOptional(req.body?.lieferant);
  const aktion_nr = parseTextOptional(req.body?.aktion_nr);
  const von_filiale = parseTextOptional(req.body?.von_filiale);
  const an_filiale = parseTextOptional(req.body?.an_filiale);

  try {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      // Booking + Filiale/Jahr/KW auflösen
      const curRes = await client.query(
        `
          SELECT
            b.id,
            b.week_budget_id,
            b.typ,
            wb.filiale,
            wb.jahr,
            wb.kw
          FROM budget.bookings b
          JOIN budget.week_budgets wb ON wb.id = b.week_budget_id
          WHERE b.id = $1
          LIMIT 1
        `,
        [bookingId]
      );

      if (curRes.rows.length === 0) {
        await client.query('ROLLBACK');
        return res.status(404).json({ message: 'Booking nicht gefunden.' });
      }

      const current = curRes.rows[0];

      if (!canWriteBookingType(role, current.typ)) {
        await client.query('ROLLBACK');
        return res.status(403).json({ message: `Zugriff verweigert: Rolle darf typ='${current.typ}' nicht bearbeiten.` });
      }

      // Filialbegrenzung bei bestellung für Filialrolle
      if (current.typ === 'bestellung' && isFilialeRole(role) && current.filiale !== tokenFiliale) {
        await client.query('ROLLBACK');
        return res.status(403).json({ message: 'Zugriff verweigert: Filialuser darf nur eigene Bestellungen bearbeiten.' });
      }

      const updRes = await client.query(
        `
          UPDATE budget.bookings
          SET
            betrag = $2,
            beschreibung = $3,
            datum = $4,
            lieferant = $5,
            aktion_nr = $6,
            von_filiale = $7,
            an_filiale = $8
          WHERE id = $1
          RETURNING
            id, week_budget_id, datum, typ, betrag, lieferant, aktion_nr, beschreibung, von_filiale, an_filiale,
            status, bestaetigt_von, bestaetigt_am, created_by, created_at
        `,
        [bookingId, betrag, beschreibung, datum, lieferant, aktion_nr, von_filiale, an_filiale]
      );

      const weekSummary = await fetchWeekSummary(client, current.filiale, current.jahr, current.kw);

      await client.query('COMMIT');
      return res.json({
        booking: updRes.rows[0],
        week_summary: weekSummary
      });
    } catch (e) {
      await client.query('ROLLBACK');
      console.error('Fehler PUT /api/budget/bookings/:id:', e.message);
      return res.status(500).json({ message: 'Serverfehler (Bookings UPDATE).' });
    } finally {
      client.release();
    }
  } catch (err) {
    console.error('Fehler PUT /api/budget/bookings/:id (connect):', err.message);
    return res.status(500).json({ message: 'Serverfehler (Bookings UPDATE).' });
  }
});

// -----------------------------------------------------
// DELETE /api/budget/bookings/:id
// Rechte: typ-spezifisch + filialbezogen (bei bestellung darf Filiale nur eigene)
// -----------------------------------------------------
router.delete('/bookings/:id', verifyToken(), async (req, res) => {
  const { role, filiale: tokenFiliale } = req.user || {};
  const bookingId = typeof req.params.id === 'string' ? req.params.id.trim() : '';

  if (!bookingId) {
    return res.status(400).json({ message: 'Ungültige Parameter: id fehlt.' });
  }

  try {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      const curRes = await client.query(
        `
          SELECT
            b.id,
            b.week_budget_id,
            b.typ,
            wb.filiale,
            wb.jahr,
            wb.kw
          FROM budget.bookings b
          JOIN budget.week_budgets wb ON wb.id = b.week_budget_id
          WHERE b.id = $1
          LIMIT 1
        `,
        [bookingId]
      );

      if (curRes.rows.length === 0) {
        await client.query('ROLLBACK');
        return res.status(404).json({ message: 'Booking nicht gefunden.' });
      }

      const current = curRes.rows[0];

      if (!canWriteBookingType(role, current.typ)) {
        await client.query('ROLLBACK');
        return res.status(403).json({ message: `Zugriff verweigert: Rolle darf typ='${current.typ}' nicht löschen.` });
      }

      // Filialbegrenzung bei bestellung für Filialrolle
      if (current.typ === 'bestellung' && isFilialeRole(role) && current.filiale !== tokenFiliale) {
        await client.query('ROLLBACK');
        return res.status(403).json({ message: 'Zugriff verweigert: Filialuser darf nur eigene Bestellungen löschen.' });
      }

      await client.query(`DELETE FROM budget.bookings WHERE id = $1`, [bookingId]);

      const weekSummary = await fetchWeekSummary(client, current.filiale, current.jahr, current.kw);

      await client.query('COMMIT');
      return res.json({
        message: 'Booking gelöscht.',
        week_summary: weekSummary
      });
    } catch (e) {
      await client.query('ROLLBACK');
      console.error('Fehler DELETE /api/budget/bookings/:id:', e.message);
      return res.status(500).json({ message: 'Serverfehler (Bookings DELETE).' });
    } finally {
      client.release();
    }
  } catch (err) {
    console.error('Fehler DELETE /api/budget/bookings/:id (connect):', err.message);
    return res.status(500).json({ message: 'Serverfehler (Bookings DELETE).' });
  }
});

module.exports = router;
