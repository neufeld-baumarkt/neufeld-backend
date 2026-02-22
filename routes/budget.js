// routes/budget.js – Budget API V1 + MS4 (Bookings) + Split-Bookings (Modell A) + Rule-Update + Response-Redaction
// Stand: 31.01.2026 (Backend: Split-Bookings via DB-Funktionen, Child-Schutz im CRUD)
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

const SOURCE_BESTELLUNG = 'BESTELLUNG';
const SOURCE_AKTION = 'AKTION';

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

function normalizeTextOrNull(value) {
  if (value === null || value === undefined) return null;
  if (typeof value !== 'string') return null;
  const t = value.trim();
  return t ? t : null;
}

// source wird strikt aus aktion_nr abgeleitet (DB-Constraint ist aktiv)
function deriveSourceFromAktionNr(aktionNr) {
  const a = normalizeTextOrNull(aktionNr);
  return a ? SOURCE_AKTION : SOURCE_BESTELLUNG;
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

  // ✅ NEU: Ist-Verbrauchsquote YTD ist intern (nicht für Filiale)
  delete clean.ist_verbrauch_satz_ytd_prozent;

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
// Week-Budget Resolver (Filiale/Jahr/KW -> week_budget_id)
// =====================================================

async function resolveWeekBudgetId(client, filiale, jahr, kw) {
  const r = await client.query(
    `
      SELECT id
      FROM budget.week_budgets
      WHERE filiale = $1 AND jahr = $2 AND kw = $3
      LIMIT 1
    `,
    [filiale, jahr, kw]
  );
  return r.rows?.[0]?.id || null;
}

async function resolveTargetWeekBudgetId(client, jahr, kw, splitItem) {
  if (splitItem?.target_week_budget_id) return splitItem.target_week_budget_id;
  const tf = normalizeFiliale(splitItem?.target_filiale || splitItem?.filiale);
  if (!tf) return null;
  const id = await resolveWeekBudgetId(client, tf, jahr, kw);
  return id;
}

function parseSplitsArray(input) {
  if (!Array.isArray(input)) return [];
  return input
    .map((s) => ({
      target_week_budget_id: s?.target_week_budget_id || null,
      target_filiale: s?.target_filiale || s?.filiale || null,
      betrag: parseNumericSafe(s?.betrag)
    }))
    .filter((s) => s.betrag !== null);
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
// Split-Bookings (Modell A / Mitbestellung)
// =====================================================
//
// Prinzip (verbindlich):
// - Alle Split-Operationen laufen über DB-Funktionen (SSoT in der DB).
// - Normaler CRUD (/bookings/:id) darf KEINE Child-Bookings anfassen.
// - Parent mit Splits darf nicht über normalen CRUD verändert/gelöscht werden.
//
// DB-Funktionen:
// - budget.create_split_booking(jsonb, text) -> jsonb
// - budget.update_split_booking(uuid, jsonb, text) -> jsonb
// - budget.delete_split_booking(uuid) -> jsonb
//

function guardNoAlleFiliale(filiale, res) {
  if (normalizeFiliale(filiale) === 'Alle') {
    res.status(400).json({ message: 'Filiale "Alle" ist für Split/Bookings unzulässig.' });
    return false;
  }
  return true;
}

async function fetchAffectedWeekSummaries(client, role, jahr, kw, filialen) {
  const uniq = Array.from(new Set((filialen || []).filter(Boolean)));
  const out = {};
  for (const f of uniq) {
    const raw = await fetchWeekSummary(client, f, jahr, kw);
    out[f] = redactWeekSummaryForRole(raw, role);
  }
  return out;
}

// POST /api/budget/bookings/split
router.post('/bookings/split', verifyToken(), async (req, res) => {
  const { role, name, filiale: tokenFiliale } = req.user || {};

  if (!enforceFilialeForCentral(req, res)) return;

  const jahr = parseIntSafe(req.body?.jahr);
  const kw = parseIntSafe(req.body?.kw);
  if (!jahr || !kw) return res.status(400).json({ message: 'jahr und kw sind erforderlich.' });

  // Zentral: filiale muss im Body stehen; Filiale: kommt aus Token
  const filiale = resolveFiliale({ ...req, query: req.body });
  if (!filiale) return res.status(400).json({ message: 'Filiale konnte nicht ermittelt werden.' });
  if (!guardNoAlleFiliale(filiale, res)) return;

  const datum = req.body?.datum;
  const typ = req.body?.typ || 'bestellung';
  const gesamtbetrag = parseNumericSafe(req.body?.gesamtbetrag);
  const lieferant = req.body?.lieferant || null;
  const aktion_nr = req.body?.aktion_nr || null;
  const beschreibung = req.body?.beschreibung || null;
  const status = req.body?.status || 'offen';

  // ✅ FIX: Im Split-Endpoint ist "gesamtbetrag" Pflicht und DARF NICHT geblockt werden.
  //        Nur serverseitige Split-Systemfelder bleiben unzulässig.
  if (req.body?.parent_booking_id !== undefined || req.body?.split_group_id !== undefined) {
    return res.status(400).json({
      message: 'Felder (parent_booking_id/split_group_id) sind hier unzulässig. Diese werden serverseitig gesetzt.'
    });
  }

  if (!datum) return res.status(400).json({ message: 'datum ist erforderlich.' });
  if (!typ || !BOOKING_TYPES.includes(typ)) return res.status(400).json({ message: 'typ ist ungültig.' });
  if (gesamtbetrag === null) return res.status(400).json({ message: 'gesamtbetrag ist erforderlich.' });

  if (!canWriteBookingType(role, typ)) {
    return res.status(403).json({ message: `Zugriff verweigert: Rolle darf typ='${typ}' nicht anlegen.` });
  }

  // Typ-Regeln
  if (typ === 'aktionsvorab' && !normalizeTextOrNull(aktion_nr)) {
    return res.status(400).json({ message: "aktion_nr ist erforderlich für typ='aktionsvorab'." });
  }
  if (typ === 'bestellung' && normalizeTextOrNull(aktion_nr)) {
    return res.status(400).json({ message: "aktion_nr ist unzulässig für typ='bestellung'." });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const sourceWeekBudgetId = await resolveWeekBudgetId(client, filiale, jahr, kw);
    if (!sourceWeekBudgetId) {
      await client.query('ROLLBACK');
      return res.status(404).json({ message: `week_budget nicht gefunden für ${filiale} (${jahr}/KW${kw}).` });
    }

    const splitsIn = parseSplitsArray(req.body?.splits);
    if (splitsIn.length === 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({ message: 'splits ist erforderlich (mindestens 1 Eintrag).' });
    }

    const splitsResolved = [];
    for (const s of splitsIn) {
      const targetId = await resolveTargetWeekBudgetId(client, jahr, kw, s);
      if (!targetId) {
        await client.query('ROLLBACK');
        return res.status(400).json({ message: 'Split-Ziel ungültig: target_week_budget_id oder target_filiale fehlt/unkorrekt.' });
      }

      const meta = await client.query(`SELECT filiale FROM budget.week_budgets WHERE id = $1 LIMIT 1`, [targetId]);
      const targetFiliale = meta.rows?.[0]?.filiale;

      if (!targetFiliale) {
        await client.query('ROLLBACK');
        return res.status(400).json({ message: 'Split-Ziel ungültig (week_budget nicht gefunden).' });
      }
      if (normalizeFiliale(targetFiliale) === 'Alle') {
        await client.query('ROLLBACK');
        return res.status(400).json({ message: 'Split-Ziel "Alle" ist unzulässig.' });
      }
      if (normalizeFiliale(targetFiliale) === normalizeFiliale(filiale)) {
        await client.query('ROLLBACK');
        return res.status(400).json({ message: 'Split-Ziel darf nicht die gleiche Filiale wie die Quelle sein.' });
      }

      splitsResolved.push({
        target_week_budget_id: targetId,
        betrag: s.betrag
      });
    }

    const sourceFinal = deriveSourceFromAktionNr(aktion_nr);

    const payload = {
      source_week_budget_id: sourceWeekBudgetId,
      gesamtbetrag,
      typ,
      source: sourceFinal,
      status,
      datum,
      lieferant,
      aktion_nr,
      beschreibung,
      splits: splitsResolved
    };

    const r = await client.query(
      `SELECT budget.create_split_booking($1::jsonb, $2::text) AS result`,
      [JSON.stringify(payload), name || null]
    );

    const result = r.rows?.[0]?.result || null;
    const parentId = result?.parent_id;

    const targets = await client.query(
      `
        SELECT DISTINCT wb.filiale
        FROM budget.booking_splits bs
        JOIN budget.week_budgets wb ON wb.id = bs.target_week_budget_id
        WHERE bs.parent_booking_id = $1
      `,
      [parentId]
    );

    const affectedFilialen = [filiale, ...targets.rows.map((x) => x.filiale)];
    const weekSummaries = await fetchAffectedWeekSummaries(client, role, jahr, kw, affectedFilialen);

    await client.query('COMMIT');

    return res.status(201).json({
      message: 'Split-Booking angelegt.',
      result,
      week_summaries: weekSummaries
    });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('Fehler POST /api/budget/bookings/split:', e.message);
    return res.status(500).json({ message: 'Serverfehler (Split Bookings POST).' });
  } finally {
    client.release();
  }
});

// PUT /api/budget/bookings/split/:parentId (replace-all)
router.put('/bookings/split/:parentId', verifyToken(), async (req, res) => {
  const { role, name, filiale: tokenFiliale } = req.user || {};
  const parentId = req.params.parentId;

  if (!enforceFilialeForCentral(req, res)) return;

  const jahr = parseIntSafe(req.body?.jahr);
  const kw = parseIntSafe(req.body?.kw);
  if (!jahr || !kw) return res.status(400).json({ message: 'jahr und kw sind erforderlich.' });

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const cur = await client.query(
      `
        SELECT
          b.id,
          b.parent_booking_id,
          b.week_budget_id,
          wb.filiale,
          wb.jahr,
          wb.kw
        FROM budget.bookings b
        JOIN budget.week_budgets wb ON wb.id = b.week_budget_id
        WHERE b.id = $1
        LIMIT 1
      `,
      [parentId]
    );

    if (cur.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ message: 'Split-Parent nicht gefunden.' });
    }

    const parent = cur.rows[0];
    if (parent.parent_booking_id) {
      await client.query('ROLLBACK');
      return res.status(400).json({ message: 'Ungültig: ID gehört zu einer Child-Buchung.' });
    }

    if (!guardNoAlleFiliale(parent.filiale, res)) {
      await client.query('ROLLBACK');
      return;
    }

    if (
      isFilialeRole(role) &&
      normalizeFiliale(tokenFiliale) &&
      normalizeFiliale(parent.filiale) !== normalizeFiliale(tokenFiliale)
    ) {
      await client.query('ROLLBACK');
      return res.status(403).json({ message: 'Zugriff verweigert: Filialuser darf nur eigene Split-Parents ändern.' });
    }

    const splitsIn = parseSplitsArray(req.body?.splits);
    if (splitsIn.length === 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({ message: 'splits ist erforderlich (mindestens 1 Eintrag).' });
    }

    const splitsResolved = [];
    for (const s of splitsIn) {
      const targetId = await resolveTargetWeekBudgetId(client, jahr, kw, s);
      if (!targetId) {
        await client.query('ROLLBACK');
        return res.status(400).json({ message: 'Split-Ziel ungültig: target_week_budget_id oder target_filiale fehlt/unkorrekt.' });
      }

      const meta = await client.query(`SELECT filiale FROM budget.week_budgets WHERE id = $1 LIMIT 1`, [targetId]);
      const targetFiliale = meta.rows?.[0]?.filiale;

      if (!targetFiliale) {
        await client.query('ROLLBACK');
        return res.status(400).json({ message: 'Split-Ziel ungültig (week_budget nicht gefunden).' });
      }
      if (normalizeFiliale(targetFiliale) === 'Alle') {
        await client.query('ROLLBACK');
        return res.status(400).json({ message: 'Split-Ziel "Alle" ist unzulässig.' });
      }
      if (normalizeFiliale(targetFiliale) === normalizeFiliale(parent.filiale)) {
        await client.query('ROLLBACK');
        return res.status(400).json({ message: 'Split-Ziel darf nicht die gleiche Filiale wie die Quelle sein.' });
      }

      splitsResolved.push({ target_week_budget_id: targetId, betrag: s.betrag });
    }

    const payload = {
      gesamtbetrag: req.body?.gesamtbetrag,
      datum: req.body?.datum,
      lieferant: req.body?.lieferant,
      aktion_nr: req.body?.aktion_nr,
      beschreibung: req.body?.beschreibung,
      status: req.body?.status,
      splits: splitsResolved
    };

    const r = await client.query(
      `SELECT budget.update_split_booking($1::uuid, $2::jsonb, $3::text) AS result`,
      [parentId, JSON.stringify(payload), name || null]
    );

    const result = r.rows?.[0]?.result || null;

    const targets = await client.query(
      `
        SELECT DISTINCT wb.filiale
        FROM budget.booking_splits bs
        JOIN budget.week_budgets wb ON wb.id = bs.target_week_budget_id
        WHERE bs.parent_booking_id = $1
      `,
      [parentId]
    );

    const affectedFilialen = [parent.filiale, ...targets.rows.map((x) => x.filiale)];
    const weekSummaries = await fetchAffectedWeekSummaries(client, role, jahr, kw, affectedFilialen);

    await client.query('COMMIT');

    return res.json({
      message: 'Split-Booking aktualisiert.',
      result,
      week_summaries: weekSummaries
    });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('Fehler PUT /api/budget/bookings/split/:parentId:', e.message);
    return res.status(500).json({ message: 'Serverfehler (Split Bookings PUT).' });
  } finally {
    client.release();
  }
});

// DELETE /api/budget/bookings/split/:parentId
router.delete('/bookings/split/:parentId', verifyToken(), async (req, res) => {
  const { role, filiale: tokenFiliale } = req.user || {};
  const parentId = req.params.parentId;

  if (!enforceFilialeForCentral(req, res)) return;

  const jahr = parseIntSafe(req.query?.jahr);
  const kw = parseIntSafe(req.query?.kw);
  if (!jahr || !kw) return res.status(400).json({ message: 'jahr und kw sind erforderlich (query).' });

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const cur = await client.query(
      `
        SELECT
          b.id,
          b.parent_booking_id,
          wb.filiale,
          wb.jahr,
          wb.kw
        FROM budget.bookings b
        JOIN budget.week_budgets wb ON wb.id = b.week_budget_id
        WHERE b.id = $1
        LIMIT 1
      `,
      [parentId]
    );

    if (cur.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ message: 'Split-Parent nicht gefunden.' });
    }

    const parent = cur.rows[0];
    if (parent.parent_booking_id) {
      await client.query('ROLLBACK');
      return res.status(400).json({ message: 'Ungültig: ID gehört zu einer Child-Buchung.' });
    }

    if (!guardNoAlleFiliale(parent.filiale, res)) {
      await client.query('ROLLBACK');
      return;
    }

    if (
      isFilialeRole(role) &&
      normalizeFiliale(tokenFiliale) &&
      normalizeFiliale(parent.filiale) !== normalizeFiliale(tokenFiliale)
    ) {
      await client.query('ROLLBACK');
      return res.status(403).json({ message: 'Zugriff verweigert: Filialuser darf nur eigene Split-Parents löschen.' });
    }

    const targets = await client.query(
      `
        SELECT DISTINCT wb.filiale
        FROM budget.booking_splits bs
        JOIN budget.week_budgets wb ON wb.id = bs.target_week_budget_id
        WHERE bs.parent_booking_id = $1
      `,
      [parentId]
    );

    const affectedFilialen = [parent.filiale, ...targets.rows.map((x) => x.filiale)];

    const r = await client.query(
      `SELECT budget.delete_split_booking($1::uuid) AS result`,
      [parentId]
    );

    const result = r.rows?.[0]?.result || null;
    const weekSummaries = await fetchAffectedWeekSummaries(client, role, jahr, kw, affectedFilialen);

    await client.query('COMMIT');

    return res.json({
      message: 'Split-Booking gelöscht.',
      result,
      week_summaries: weekSummaries
    });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('Fehler DELETE /api/budget/bookings/split/:parentId:', e.message);
    return res.status(500).json({ message: 'Serverfehler (Split Bookings DELETE).' });
  } finally {
    client.release();
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

  // Split-Felder sind im normalen CRUD verboten
  if (req.body?.gesamtbetrag !== undefined || req.body?.parent_booking_id !== undefined || req.body?.split_group_id !== undefined) {
    return res.status(400).json({ message: 'Split-Felder (gesamtbetrag/parent_booking_id/split_group_id) sind hier unzulässig. Nutze /bookings/split.' });
  }

  if (!filiale) return res.status(400).json({ message: 'Filiale konnte nicht ermittelt werden.' });
  if (!jahr || !kw) return res.status(400).json({ message: 'jahr und kw sind erforderlich.' });
  if (!typ || !BOOKING_TYPES.includes(typ)) return res.status(400).json({ message: 'typ ist ungültig.' });
  if (betrag === null) return res.status(400).json({ message: 'betrag ist erforderlich.' });
  if (!datum) return res.status(400).json({ message: 'datum ist erforderlich.' });

  if (!canWriteBookingType(role, typ)) {
    return res.status(403).json({ message: `Zugriff verweigert: Rolle darf typ='${typ}' nicht anlegen.` });
  }

  // Fachlogik: Aktionsvorab MUSS eine aktion_nr haben, Bestellung DARF keine haben
  if (typ === 'aktionsvorab' && !normalizeTextOrNull(aktion_nr)) {
    return res.status(400).json({ message: "aktion_nr ist erforderlich für typ='aktionsvorab'." });
  }
  if (typ === 'bestellung' && normalizeTextOrNull(aktion_nr)) {
    return res.status(400).json({ message: "aktion_nr ist unzulässig für typ='bestellung'." });
  }

  if (typ === 'bestellung' && isFilialeRole(role)) {
    const tf = normalizeFiliale(tokenFiliale);
    if (tf && filiale !== tf) {
      return res.status(403).json({ message: 'Zugriff verweigert: Filialuser darf nur eigene Bestellungen anlegen.' });
    }
  }

  const source = deriveSourceFromAktionNr(aktion_nr);

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
          (week_budget_id, datum, typ, betrag, lieferant, aktion_nr, beschreibung, von_filiale, an_filiale, status, created_by, created_at, source)
        VALUES
          ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW(), $12)
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
        req.user?.name || 'unknown',
        source
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
          b.aktion_nr,
          b.parent_booking_id,
          EXISTS (SELECT 1 FROM budget.booking_splits bs WHERE bs.parent_booking_id = b.id) AS has_splits,
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


    // Split-Schutz: Childs/Parents mit Splits dürfen NICHT über normalen CRUD geändert werden
    if (current.parent_booking_id) {
      await client.query('ROLLBACK');
      return res.status(400).json({ message: 'Diese Buchung ist ein Split-Child und darf hier nicht geändert werden. Nutze /bookings/split.' });
    }
    if (current.has_splits) {
      await client.query('ROLLBACK');
      return res.status(400).json({ message: 'Diese Buchung ist ein Split-Parent (hat Splits) und darf hier nicht geändert werden. Nutze /bookings/split.' });
    }

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

    // "source" ist nicht direkt editierbar (wird aus aktion_nr abgeleitet)
    if (req.body?.source !== undefined) {
      await client.query('ROLLBACK');
      return res.status(400).json({ message: "source kann nicht direkt gesetzt werden (wird aus aktion_nr abgeleitet)." });
    }

    const fields = ['datum', 'betrag', 'lieferant', 'beschreibung', 'von_filiale', 'an_filiale', 'status'];
    const updates = [];
    const values = [];
    let idx = 1;

    for (const f of fields) {
      if (req.body[f] !== undefined) {
        updates.push(`${f} = $${idx++}`);
        values.push(req.body[f]);
      }
    }

    // aktion_nr ist special: wenn gesetzt/geändert → source automatisch nachziehen + typ-Regeln prüfen
    let aktionNrFinal = current.aktion_nr;
    let aktionNrTouched = false;

    if (req.body.aktion_nr !== undefined) {
      aktionNrTouched = true;
      aktionNrFinal = req.body.aktion_nr || null;

      // typ-Regeln
      if (current.typ === 'aktionsvorab' && !normalizeTextOrNull(aktionNrFinal)) {
        await client.query('ROLLBACK');
        return res.status(400).json({ message: "aktion_nr ist erforderlich für typ='aktionsvorab'." });
      }
      if (current.typ === 'bestellung' && normalizeTextOrNull(aktionNrFinal)) {
        await client.query('ROLLBACK');
        return res.status(400).json({ message: "aktion_nr ist unzulässig für typ='bestellung'." });
      }

      updates.push(`aktion_nr = $${idx++}`);
      values.push(aktionNrFinal);

      const sourceFinal = deriveSourceFromAktionNr(aktionNrFinal);
      updates.push(`source = $${idx++}`);
      values.push(sourceFinal);
    }

    if (updates.length === 0 && !aktionNrTouched) {
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
          b.parent_booking_id,
          EXISTS (SELECT 1 FROM budget.booking_splits bs WHERE bs.parent_booking_id = b.id) AS has_splits,
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

    // Split-Schutz: Childs/Parents mit Splits dürfen NICHT über normalen CRUD gelöscht werden
    if (current.parent_booking_id) {
      await client.query('ROLLBACK');
      return res.status(400).json({ message: 'Diese Buchung ist ein Split-Child und darf hier nicht gelöscht werden. Nutze /bookings/split.' });
    }
    if (current.has_splits) {
      await client.query('ROLLBACK');
      return res.status(400).json({ message: 'Diese Buchung ist ein Split-Parent (hat Splits) und darf hier nicht gelöscht werden. Nutze /bookings/split.' });
    }

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
