// routes/budget.js – Budget API V1 + MS4 (Bookings) + Rule-Update + Response-Redaction
// Stand: 27.01.2026 (Backend-Ebene: Option 1 umgesetzt)
//
// READ:  budget.v_week_summary_global_ytd (read-only)   ✅ NEU
// WRITE: budget.week_budgets (umsatz_vorwoche_brutto per UPSERT)
// WRITE: budget.bookings     (MS4 CRUD)
// WRITE: budget.week_rules   (Admin/Supervisor dürfen prozentsatz/mwst_faktor pflegen)
//
// Grundregeln:
// - Backend rechnet NICHT selbst, liefert View 1:1 (aber mit Redaction je Rolle)
// - View wird NICHT beschrieben
//
// Sonderregel (verbindlich):
// - Wenn role !== "Filiale": dann MUSS eine Filiale explizit gesetzt sein.
//   (Damit verhindern wir, dass Zentral-User versehentlich in "Alle" schreiben/lesen.)
//
// Sicherheitsregel (verbindlich):
// - Filiale darf den Wochen-Prozentsatz NICHT sehen.
// - Das bedeutet: prozentsatz_* und mwst_faktor_* sowie budget_satz_ytd_prozent
//   werden serverseitig aus Responses entfernt.

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
const ROLE_FILIALE = 'Filiale';

const BOOKING_TYPES = ['bestellung', 'aktionsvorab', 'abgabe', 'korrektur'];

function isFilialeRole(role) {
  return role === ROLE_FILIALE;
}

function isCentralRole(role) {
  return !isFilialeRole(role);
}

function isAdminOrSupervisor(role) {
  return role === ROLE_ADMIN || role === ROLE_SUPERVISOR;
}

function normalizeFiliale(value) {
  if (typeof value !== 'string') return null;
  const t = value.trim();
  if (!t) return null;
  return t;
}

// Zentral-User können die Filiale übergeben via:
// - Query:   ?filiale=XYZ
// - Header:  x-filiale: XYZ
// - Body:    { filiale: "XYZ" }
function getRequestedFiliale(req) {
  const q = normalizeFiliale(req.query?.filiale);
  if (q) return q;

  const h = normalizeFiliale(req.headers?.['x-filiale']);
  if (h) return h;

  const b = normalizeFiliale(req.body?.filiale);
  if (b) return b;

  return null;
}

function resolveFiliale(req) {
  // Sonderregel:
  // - Filiale-User: Filiale aus JWT
  // - Zentral-User: Filiale MUSS explizit übergeben werden (Query/Header/Body)
  const { role, filiale: tokenFiliale } = req.user || {};

  if (isFilialeRole(role)) {
    return normalizeFiliale(tokenFiliale);
  }

  return getRequestedFiliale(req);
}

function enforceFilialeForCentral(req, res) {
  const { role } = req.user || {};
  if (isCentralRole(role)) {
    const f = getRequestedFiliale(req);
    if (!f) {
      res.status(400).json({
        message:
          'Filiale fehlt: Für zentrale Rollen muss eine Filiale explizit gesetzt sein (?filiale=XYZ oder Header x-filiale oder Body filiale).'
      });
      return false;
    }
    if (f.toLowerCase() === 'alle') {
      res.status(400).json({
        message: 'Ungültige Filiale: "Alle" ist im Budget-Kontext nicht erlaubt. Bitte eine echte Filiale setzen.'
      });
      return false;
    }
  }
  return true;
}

function canWriteBookingType(role, bookingType) {
  if (!BOOKING_TYPES.includes(bookingType)) return false;

  if (bookingType === 'bestellung') {
    // Filiale (eigene), Supervisor, Admin dürfen schreiben
    return role === ROLE_FILIALE || role === ROLE_SUPERVISOR || role === ROLE_ADMIN;
  }

  if (bookingType === 'aktionsvorab') {
    // Schreiben: BZL (Manager-1), GL, Supervisor, Admin
    return role === ROLE_ADMIN || role === ROLE_SUPERVISOR || role === ROLE_MANAGER_1 || role === ROLE_GF;
  }

  // abgabe / korrektur: Schreiben: Supervisor, Admin
  return role === ROLE_ADMIN || role === ROLE_SUPERVISOR;
}

function canReadBookingType(role, bookingType) {
  if (!BOOKING_TYPES.includes(bookingType)) return false;
  // Lesen ist grundsätzlich erlaubt (filialbezogen wird über resolveFiliale erzwungen)
  return true;
}

// =====================================================
// Redaction (Sicherheit): Filiale darf Prozentsätze/MwSt nicht sehen
// =====================================================

function redactWeekSummaryForRole(row, role) {
  if (!row) return row;
  if (!isFilialeRole(role)) return row;

  const clean = { ...row };

  // View-Felder (effektiv / snapshot / global)
  delete clean.prozentsatz_effektiv;
  delete clean.prozentsatz_snapshot;
  delete clean.prozentsatz;
  delete clean.mwst_faktor_effektiv;
  delete clean.mwst_faktor_snapshot;
  delete clean.mwst_faktor;

  // YTD-Felder, die indirekt den Satz verraten würden
  delete clean.budget_satz_ytd_prozent;

  return clean;
}

// =====================================================
// Helpers
// =====================================================

async function fetchWeekSummary(client, filiale, jahr, kw) {
  const viewRes = await client.query(
    `
      SELECT *
      FROM budget.v_week_summary_global_ytd
      WHERE filiale = $1 AND jahr = $2 AND kw = $3
      LIMIT 1
    `,
    [filiale, jahr, kw]
  );
  return viewRes.rows.length ? viewRes.rows[0] : null;
}

function parseIntSafe(v) {
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : null;
}

function parseNumericSafe(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

// =====================================================
// RULES (GLOBAL/WOCHE): Admin/Supervisor dürfen ändern
// =====================================================

// GET /api/budget/rules?jahr=2026&kw=2
router.get('/rules', verifyToken(), async (req, res) => {
  const { role } = req.user || {};
  if (!isAdminOrSupervisor(role)) {
    return res.status(403).json({ message: 'Zugriff verweigert: Nur Admin/Supervisor dürfen Rules sehen.' });
  }

  const jahr = parseIntSafe(req.query?.jahr);
  const kw = parseIntSafe(req.query?.kw);

  if (!jahr || !kw) {
    return res.status(400).json({ message: 'jahr und kw sind erforderlich (Query-Parameter).' });
  }

  try {
    const r = await pool.query(
      `
        SELECT jahr, kw, prozentsatz, mwst_faktor, gueltig_ab, gueltig_bis, updated_at
        FROM budget.week_rules
        WHERE jahr = $1 AND kw = $2
        LIMIT 1
      `,
      [jahr, kw]
    );

    if (r.rows.length === 0) {
      return res.status(404).json({ message: 'Keine Rule gefunden für diese Woche.' });
    }

    return res.json(r.rows[0]);
  } catch (e) {
    console.error('Fehler GET /api/budget/rules:', e.message);
    return res.status(500).json({ message: 'Serverfehler (Rules GET).' });
  }
});

// PUT /api/budget/rules  Body: { jahr, kw, prozentsatz, mwst_faktor? }
router.put('/rules', verifyToken(), async (req, res) => {
  const { role } = req.user || {};
  if (!isAdminOrSupervisor(role)) {
    return res.status(403).json({ message: 'Zugriff verweigert: Nur Admin/Supervisor dürfen Rules ändern.' });
  }

  const jahr = parseIntSafe(req.body?.jahr);
  const kw = parseIntSafe(req.body?.kw);
  const prozentsatz = parseNumericSafe(req.body?.prozentsatz);
  const mwst_faktor = parseNumericSafe(req.body?.mwst_faktor);

  if (!jahr || !kw) {
    return res.status(400).json({ message: 'jahr und kw sind erforderlich.' });
  }
  if (prozentsatz === null || prozentsatz === undefined) {
    return res.status(400).json({ message: 'prozentsatz ist erforderlich.' });
  }
  if (prozentsatz < 0 || prozentsatz > 1) {
    return res.status(400).json({ message: 'prozentsatz muss zwischen 0 und 1 liegen (z.B. 0.48).' });
  }
  if (mwst_faktor !== null && (mwst_faktor <= 0 || mwst_faktor > 5)) {
    return res.status(400).json({ message: 'mwst_faktor ist ungültig.' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const upd = await client.query(
      `
        UPDATE budget.week_rules
        SET prozentsatz = $3,
            mwst_faktor = COALESCE($4, mwst_faktor),
            updated_at = NOW()
        WHERE jahr = $1 AND kw = $2
        RETURNING jahr, kw, prozentsatz, mwst_faktor, gueltig_ab, gueltig_bis, updated_at
      `,
      [jahr, kw, prozentsatz, mwst_faktor]
    );

    let ruleRow = null;
    let message = 'Rule aktualisiert.';

    if (upd.rows.length > 0) {
      ruleRow = upd.rows[0];
    } else {
      const ins = await client.query(
        `
          INSERT INTO budget.week_rules (jahr, kw, prozentsatz, mwst_faktor, gueltig_ab, gueltig_bis, updated_at)
          VALUES ($1, $2, $3, COALESCE($4, 1.19), NULL, NULL, NOW())
          RETURNING jahr, kw, prozentsatz, mwst_faktor, gueltig_ab, gueltig_bis, updated_at
        `,
        [jahr, kw, prozentsatz, mwst_faktor]
      );
      ruleRow = ins.rows[0];
      message = 'Rule angelegt.';
    }

    // V1: Snapshot nachziehen, falls nur Platzhalter (0) vorhanden ist und noch nicht freigegeben
    await client.query(
      `
        UPDATE budget.week_budgets
        SET prozentsatz_snapshot = $3,
            updated_at = NOW()
        WHERE jahr = $1
          AND kw = $2
          AND freigegeben = false
          AND prozentsatz_snapshot = 0
      `,
      [jahr, kw, prozentsatz]
    );

    await client.query('COMMIT');
    return res.json({
      message,
      rule: ruleRow
    });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('Fehler PUT /api/budget/rules:', e.message);
    return res.status(500).json({ message: 'Serverfehler (Rules PUT).' });
  } finally {
    client.release();
  }
});

// =====================================================
// Week Summary (Budget-Kopfwerte)
// =====================================================

// GET /api/budget/week-summary?jahr=2026&kw=2[&filiale=Münster]
router.get('/week-summary', verifyToken(), async (req, res) => {
  const { role } = req.user || {};

  if (!enforceFilialeForCentral(req, res)) return;

  const filiale = resolveFiliale(req);
  const jahr = parseIntSafe(req.query?.jahr);
  const kw = parseIntSafe(req.query?.kw);

  if (!filiale) return res.status(400).json({ message: 'Filiale konnte nicht ermittelt werden.' });
  if (!jahr || !kw) return res.status(400).json({ message: 'jahr und kw sind erforderlich.' });

  try {
    const client = await pool.connect();
    try {
      const raw = await fetchWeekSummary(client, filiale, jahr, kw);
      const weekSummary = redactWeekSummaryForRole(raw, role);
      return res.json(weekSummary);
    } finally {
      client.release();
    }
  } catch (e) {
    console.error('Fehler GET /api/budget/week-summary:', e.message);
    return res.status(500).json({ message: 'Serverfehler (Week Summary).' });
  }
});

// =====================================================
// Umsatz Vorwoche speichern / upsert week_budgets
// =====================================================

// PUT /api/budget/umsatz-vorwoche
// Body: { jahr, kw, umsatz_vorwoche_brutto }
router.put('/umsatz-vorwoche', verifyToken(), async (req, res) => {
  const { role } = req.user || {};

  if (!isAdminOrSupervisor(role)) {
    return res.status(403).json({ message: 'Zugriff verweigert: Nur Admin/Supervisor dürfen Umsatz Vorwoche setzen.' });
  }

  if (!enforceFilialeForCentral(req, res)) return;

  const filiale = resolveFiliale(req);
  const jahr = parseIntSafe(req.body?.jahr);
  const kw = parseIntSafe(req.body?.kw);
  const umsatz = parseNumericSafe(req.body?.umsatz_vorwoche_brutto);

  if (!filiale) return res.status(400).json({ message: 'Filiale konnte nicht ermittelt werden.' });
  if (!jahr || !kw) return res.status(400).json({ message: 'jahr und kw sind erforderlich.' });
  if (umsatz === null) return res.status(400).json({ message: 'umsatz_vorwoche_brutto ist erforderlich.' });

  try {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      const upsert = await client.query(
        `
          INSERT INTO budget.week_budgets (filiale, jahr, kw, umsatz_vorwoche_brutto, prozentsatz_snapshot, freigegeben, created_at, updated_at)
          VALUES (
            $1, $2, $3, $4,
            COALESCE((SELECT prozentsatz FROM budget.week_rules WHERE jahr = $2 AND kw = $3 LIMIT 1), 0),
            COALESCE((SELECT freigegeben FROM budget.week_budgets WHERE filiale=$1 AND jahr=$2 AND kw=$3 LIMIT 1), false),
            NOW(), NOW()
          )
          ON CONFLICT (filiale, jahr, kw)
          DO UPDATE SET
            umsatz_vorwoche_brutto = EXCLUDED.umsatz_vorwoche_brutto,
            prozentsatz_snapshot = CASE
              WHEN budget.week_budgets.prozentsatz_snapshot = 0 THEN EXCLUDED.prozentsatz_snapshot
              ELSE budget.week_budgets.prozentsatz_snapshot
            END,
            updated_at = NOW()
          RETURNING id, filiale, jahr, kw
        `,
        [filiale, jahr, kw, umsatz]
      );

      const wb = upsert.rows[0];

      const weekSummaryRaw = await fetchWeekSummary(client, filiale, jahr, kw);
      const weekSummary = redactWeekSummaryForRole(weekSummaryRaw, role);

      await client.query('COMMIT');

      return res.json({
        message: 'Umsatz Vorwoche gespeichert.',
        week_budget: wb,
        week_summary: weekSummary
      });
    } catch (e) {
      await client.query('ROLLBACK');
      console.error('Fehler PUT /api/budget/umsatz-vorwoche:', e.message);
      return res.status(500).json({ message: 'Serverfehler (Umsatz Vorwoche).' });
    } finally {
      client.release();
    }
  } catch (e) {
    console.error('Fehler PUT /api/budget/umsatz-vorwoche (connect):', e.message);
    return res.status(500).json({ message: 'Serverfehler (Umsatz Vorwoche connect).' });
  }
});

// =====================================================
// Bookings (MS4)
// =====================================================

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
        SELECT b.*
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
  const { role, filiale: tokenFiliale } = req.user || {};

  if (!enforceFilialeForCentral(req, res)) return;

  const filiale = resolveFiliale(req);
  const jahr = parseIntSafe(req.body?.jahr);
  const kw = parseIntSafe(req.body?.kw);

  const datum = req.body?.datum;
  const typ = req.body?.typ;
  const betrag = parseNumericSafe(req.body?.betrag);
  const lieferant = req.body?.lieferant || null;
  const aktion_nr = req.body?.aktion_nr || null;
  const beschreibung = req.body?.beschreibung || null;
  const von_filiale = req.body?.von_filiale || null;
  const an_filiale = req.body?.an_filiale || null;
  const status = req.body?.status || 'offen';

  if (!filiale) return res.status(400).json({ message: 'Filiale konnte nicht ermittelt werden.' });
  if (!jahr || !kw) return res.status(400).json({ message: 'jahr und kw sind erforderlich.' });
  if (!typ || !BOOKING_TYPES.includes(typ)) return res.status(400).json({ message: 'typ ist ungültig.' });
  if (betrag === null) return res.status(400).json({ message: 'betrag ist erforderlich.' });
  if (!datum) return res.status(400).json({ message: 'datum ist erforderlich.' });

  if (!canWriteBookingType(role, typ)) {
    return res.status(403).json({ message: `Zugriff verweigert: Rolle darf typ='${typ}' nicht anlegen.` });
  }

  if (typ === 'bestellung' && isFilialeRole(role)) {
    const tf = normalizeFiliale(tokenFiliale);
    if (tf && filiale !== tf) {
      return res.status(403).json({ message: 'Zugriff verweigert: Filialuser darf nur eigene Bestellungen anlegen.' });
    }
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const wbRes = await client.query(
      `
        INSERT INTO budget.week_budgets (filiale, jahr, kw, prozentsatz_snapshot, freigegeben, created_at, updated_at)
        VALUES (
          $1, $2, $3,
          COALESCE((SELECT prozentsatz FROM budget.week_rules WHERE jahr = $2 AND kw = $3 LIMIT 1), 0),
          false, NOW(), NOW()
        )
        ON CONFLICT (filiale, jahr, kw)
        DO UPDATE SET
          prozentsatz_snapshot = CASE
            WHEN budget.week_budgets.prozentsatz_snapshot = 0 THEN EXCLUDED.prozentsatz_snapshot
            ELSE budget.week_budgets.prozentsatz_snapshot
          END,
          updated_at = NOW()
        RETURNING id
      `,
      [filiale, jahr, kw]
    );
    const week_budget_id = wbRes.rows[0].id;

    const ins = await client.query(
      `
        INSERT INTO budget.bookings
          (week_budget_id, datum, typ, betrag, lieferant, aktion_nr, beschreibung, von_filiale, an_filiale, status, created_by, created_at)
        VALUES
          ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
        RETURNING *
      `,
      [
        week_budget_id,
        datum,
        typ,
        betrag,
        lieferant,
        aktion_nr,
        beschreibung,
        von_filiale,
        an_filiale,
        status,
        req.user?.name || 'unknown'
      ]
    );

    const weekSummaryRaw = await fetchWeekSummary(client, filiale, jahr, kw);
    const weekSummary = redactWeekSummaryForRole(weekSummaryRaw, role);

    await client.query('COMMIT');

    return res.json({
      message: 'Booking angelegt.',
      booking: ins.rows[0],
      week_summary: weekSummary
    });
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
  const { role, filiale: tokenFiliale } = req.user || {};
  const bookingId = req.params.id;

  if (!enforceFilialeForCentral(req, res)) return;

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
      return res.status(403).json({ message: `Zugriff verweigert: Rolle darf typ='${current.typ}' nicht ändern.` });
    }

    if (
      current.typ === 'bestellung' &&
      isFilialeRole(role) &&
      normalizeFiliale(tokenFiliale) &&
      current.filiale !== normalizeFiliale(tokenFiliale)
    ) {
      await client.query('ROLLBACK');
      return res.status(403).json({ message: 'Zugriff verweigert: Filialuser darf nur eigene Bestellungen ändern.' });
    }

    const fields = ['datum', 'betrag', 'lieferant', 'aktion_nr', 'beschreibung', 'von_filiale', 'an_filiale', 'status'];
    const updates = [];
    const values = [];
    let idx = 1;

    for (const f of fields) {
      if (req.body[f] !== undefined) {
        updates.push(`${f} = $${idx++}`);
        values.push(req.body[f]);
      }
    }

    if (updates.length === 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({ message: 'Keine Felder zum Aktualisieren übergeben.' });
    }

    values.push(bookingId);

    const upd = await client.query(
      `
        UPDATE budget.bookings
        SET ${updates.join(', ')}
        WHERE id = $${idx}
        RETURNING *
      `,
      values
    );

    const weekSummaryRaw = await fetchWeekSummary(client, current.filiale, current.jahr, current.kw);
    const weekSummary = redactWeekSummaryForRole(weekSummaryRaw, role);

    await client.query('COMMIT');

    return res.json({
      message: 'Booking aktualisiert.',
      booking: upd.rows[0],
      week_summary: weekSummary
    });
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
  const { role, filiale: tokenFiliale } = req.user || {};
  const bookingId = req.params.id;

  if (!enforceFilialeForCentral(req, res)) return;

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

    if (
      current.typ === 'bestellung' &&
      isFilialeRole(role) &&
      normalizeFiliale(tokenFiliale) &&
      current.filiale !== normalizeFiliale(tokenFiliale)
    ) {
      await client.query('ROLLBACK');
      return res.status(403).json({ message: 'Zugriff verweigert: Filialuser darf nur eigene Bestellungen löschen.' });
    }

    await client.query(`DELETE FROM budget.bookings WHERE id = $1`, [bookingId]);

    const weekSummaryRaw = await fetchWeekSummary(client, current.filiale, current.jahr, current.kw);
    const weekSummary = redactWeekSummaryForRole(weekSummaryRaw, role);

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
});

module.exports = router;
