// routes/reklamationen.js – CRUD + lfd_nr Counter + Notiz-PATCH (produktiver Stand)
const express = require('express');
const router = express.Router();
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

// ------------------------------------------------------
// Helpers
// ------------------------------------------------------
function isPrivileged(role) {
  return role === 'Admin' || role === 'Supervisor';
}

function canSeeAll({ role, filiale }) {
  // bewusst konservativ: nur Admin/Supervisor sehen alles.
  // (Falls ihr weitere Zentralrollen habt, erweitern – aber jetzt nicht raten.)
  return isPrivileged(role) || filiale === 'alle';
}

function toIntYear(dateStr) {
  // erwartet "YYYY-MM-DD" oder Date-String
  const d = new Date(dateStr);
  if (Number.isNaN(d.getTime())) return null;
  return d.getFullYear();
}

async function allocateLfdNrRange(client, filiale, jahr, amount) {
  // Erwartet eine Tabelle lfd_nr_counter mit Unique (filiale, jahr).
  // Spalten: filiale TEXT, jahr INT, last_nr INT
  // Diese Funktion reserviert "amount" neue Nummern atomar und gibt ein Array zurück.
  if (!filiale || !jahr || !amount || amount < 1) return [];

  const q = `
    INSERT INTO lfd_nr_counter (filiale, jahr, last_nr)
    VALUES ($1, $2, $3)
    ON CONFLICT (filiale, jahr)
    DO UPDATE SET last_nr = lfd_nr_counter.last_nr + $3
    RETURNING last_nr
  `;

  const r = await client.query(q, [filiale, jahr, amount]);
  const lastNr = Number(r.rows[0]?.last_nr);
  if (!Number.isFinite(lastNr)) throw new Error('lfd_nr_counter RETURNING last_nr fehlgeschlagen');

  const start = lastNr - amount + 1;
  const numbers = [];
  for (let n = start; n <= lastNr; n += 1) numbers.push(n);
  return numbers;
}

function normalizeNotiz(raw) {
  if (raw === undefined) return { hasNotiz: false, value: undefined };
  const trimmed = String(raw).trim();
  return { hasNotiz: true, value: trimmed.length === 0 ? null : trimmed };
}

// ------------------------------------------------------
// GET /api/reklamationen – Liste (rollen-/filialabhängig) + Aggregatfelder
// ------------------------------------------------------
router.get('/', verifyToken(), async (req, res) => {
  const { role, filiale } = req.user;

  try {
    const baseSelect = `
      SELECT
        r.*,
        COALESCE(COUNT(p.id), 0)::int AS position_count,
        MIN(p.lfd_nr)::int AS min_lfd_nr
      FROM reklamationen r
      LEFT JOIN reklamation_positionen p ON p.reklamation_id = r.id
    `;

    const groupBy = ` GROUP BY r.id `;
    const orderBy = ` ORDER BY r.datum DESC NULLS LAST, MIN(p.lfd_nr) ASC NULLS LAST `;

    let query = baseSelect;
    const params = [];

    if (!canSeeAll({ role, filiale })) {
      query += ` WHERE r.filiale = $1 `;
      params.push(filiale);
    }

    query += groupBy + orderBy;

    const result = await pool.query(query, params);
    res.json(result.rows);
  } catch (error) {
    console.error('Fehler beim Abrufen der Reklamationen:', error);
    res.status(500).json({ message: 'Serverfehler beim Abrufen der Reklamationen' });
  }
});

// ------------------------------------------------------
// GET /api/reklamationen/:id – Detail + Positionen
// ------------------------------------------------------
router.get('/:id', verifyToken(), async (req, res) => {
  const { role, filiale } = req.user;
  const { id } = req.params;

  try {
    const rRes = await pool.query('SELECT * FROM reklamationen WHERE id = $1', [id]);
    if (rRes.rows.length === 0) {
      return res.status(404).json({ message: 'Reklamation nicht gefunden' });
    }

    const reklamation = rRes.rows[0];

    if (!canSeeAll({ role, filiale }) && reklamation.filiale !== filiale) {
      return res.status(403).json({ message: 'Kein Zugriff auf diese Reklamation' });
    }

    const pRes = await pool.query(
      `
      SELECT *
      FROM reklamation_positionen
      WHERE reklamation_id = $1
      ORDER BY lfd_nr ASC NULLS LAST, pos_id ASC NULLS LAST
      `,
      [id]
    );

    res.json({ ...reklamation, positionen: pRes.rows });
  } catch (error) {
    console.error('Fehler beim Abrufen der Reklamationsdetails:', error);
    res.status(500).json({ message: 'Serverfehler beim Abrufen der Reklamationsdetails' });
  }
});

// ------------------------------------------------------
// POST /api/reklamationen – Neu anlegen (lfd_nr Vergabe backendseitig)
// ------------------------------------------------------
router.post('/', verifyToken(), async (req, res) => {
  const { role, filiale: userFiliale } = req.user;

  const {
    filiale,
    art,
    datum,
    rekla_nr,
    lieferant,
    ls_nummer_grund,
    status,
    letzte_aenderung,
    versand,
    tracking_id,
    positionen,
  } = req.body || {};

  // Basic Checks
  if (!filiale || !art || !datum || !rekla_nr || !lieferant || !ls_nummer_grund || !status) {
    return res.status(400).json({ message: 'Pflichtfelder fehlen (Kopf)' });
  }
  if (!Array.isArray(positionen) || positionen.length < 1) {
    return res.status(400).json({ message: 'Mindestens eine Position ist erforderlich' });
  }

  // Filiale-User dürfen nur eigene Filiale anlegen
  if (!canSeeAll({ role, filiale: userFiliale }) && filiale !== userFiliale) {
    return res.status(403).json({ message: 'Du darfst nur für deine eigene Filiale anlegen' });
  }

  const year = toIntYear(datum);
  if (!year) return res.status(400).json({ message: 'Ungültiges Datum' });

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Insert Kopf
    const insertRekl = await client.query(
      `
      INSERT INTO reklamationen
        (filiale, art, datum, rekla_nr, lieferant, ls_nummer_grund, status, letzte_aenderung, versand, tracking_id)
      VALUES
        ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      RETURNING *
      `,
      [
        filiale,
        art,
        datum,
        rekla_nr,
        lieferant,
        ls_nummer_grund,
        status,
        letzte_aenderung || datum,
        !!versand,
        tracking_id || null,
      ]
    );

    const reklamation = insertRekl.rows[0];

    // lfd_nr reservieren (pro Position eine)
    const lfdNumbers = await allocateLfdNrRange(client, filiale, year, positionen.length);

    // Insert Positionen
    for (let i = 0; i < positionen.length; i += 1) {
      const p = positionen[i] || {};
      const lfdNr = lfdNumbers[i];

      await client.query(
        `
        INSERT INTO reklamation_positionen
          (reklamation_id, artikelnummer, ean, bestell_menge, bestell_einheit, rekla_menge, rekla_einheit, lfd_nr)
        VALUES
          ($1,$2,$3,$4,$5,$6,$7,$8)
        `,
        [
          reklamation.id,
          p.artikelnummer || null,
          p.ean || null,
          p.bestell_menge ?? null,
          p.bestell_einheit || null,
          p.rekla_menge ?? null,
          p.rekla_einheit || null,
          lfdNr,
        ]
      );
    }

    await client.query('COMMIT');

    // Detail zurückgeben
    const pRes = await pool.query(
      `
      SELECT *
      FROM reklamation_positionen
      WHERE reklamation_id = $1
      ORDER BY lfd_nr ASC NULLS LAST, pos_id ASC NULLS LAST
      `,
      [reklamation.id]
    );

    res.status(201).json({ ...reklamation, positionen: pRes.rows });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Fehler beim Anlegen der Reklamation:', error);
    res.status(500).json({ message: 'Serverfehler beim Anlegen der Reklamation' });
  } finally {
    client.release();
  }
});

// ------------------------------------------------------
// PUT /api/reklamationen/:id – Komplett bearbeiten (nur Admin/Supervisor)
// lfd_nr stabil: vorhandene lfd_nr werden beibehalten, neue bekommen neue Nummern
// ------------------------------------------------------
router.put('/:id', verifyToken(), async (req, res) => {
  const { role } = req.user;
  const { id } = req.params;

  if (!isPrivileged(role)) {
    return res.status(403).json({ message: 'Keine Berechtigung' });
  }

  const {
    filiale,
    art,
    datum,
    rekla_nr,
    lieferant,
    ls_nummer_grund,
    status,
    letzte_aenderung,
    versand,
    tracking_id,
    positionen,
    // Notiz wird NICHT über PUT gepflegt (dafür PATCH), aber wenn sie im Payload ist, ignorieren wir sie bewusst.
  } = req.body || {};

  if (!filiale || !art || !datum || !rekla_nr || !lieferant || !ls_nummer_grund || !status) {
    return res.status(400).json({ message: 'Pflichtfelder fehlen (Kopf)' });
  }
  if (!Array.isArray(positionen) || positionen.length < 1) {
    return res.status(400).json({ message: 'Mindestens eine Position ist erforderlich' });
  }

  const year = toIntYear(datum);
  if (!year) return res.status(400).json({ message: 'Ungültiges Datum' });

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Existenz check
    const existing = await client.query('SELECT id FROM reklamationen WHERE id = $1', [id]);
    if (existing.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ message: 'Reklamation nicht gefunden' });
    }

    // Update Kopf
    const upRes = await client.query(
      `
      UPDATE reklamationen
      SET
        filiale = $1,
        art = $2,
        datum = $3,
        rekla_nr = $4,
        lieferant = $5,
        ls_nummer_grund = $6,
        status = $7,
        letzte_aenderung = $8,
        versand = $9,
        tracking_id = $10
      WHERE id = $11
      RETURNING *
      `,
      [
        filiale,
        art,
        datum,
        rekla_nr,
        lieferant,
        ls_nummer_grund,
        status,
        letzte_aenderung || datum,
        !!versand,
        tracking_id || null,
        id,
      ]
    );

    const reklamation = upRes.rows[0];

    // Positionen: vorhandene lfd_nr behalten, neue ohne lfd_nr bekommen neue Nummern
    const newOnes = positionen.filter((p) => !p || p.lfd_nr === undefined || p.lfd_nr === null);
    const newNumbers = await allocateLfdNrRange(client, filiale, year, newOnes.length);

    // Delete + Insert (komplett ersetzen)
    await client.query('DELETE FROM reklamation_positionen WHERE reklamation_id = $1', [id]);

    let newIdx = 0;
    for (const p of positionen) {
      const hasLfd = p && p.lfd_nr !== undefined && p.lfd_nr !== null;
      const lfdNr = hasLfd ? Number(p.lfd_nr) : newNumbers[newIdx++];

      await client.query(
        `
        INSERT INTO reklamation_positionen
          (reklamation_id, artikelnummer, ean, bestell_menge, bestell_einheit, rekla_menge, rekla_einheit, lfd_nr)
        VALUES
          ($1,$2,$3,$4,$5,$6,$7,$8)
        `,
        [
          id,
          p?.artikelnummer || null,
          p?.ean || null,
          p?.bestell_menge ?? null,
          p?.bestell_einheit || null,
          p?.rekla_menge ?? null,
          p?.rekla_einheit || null,
          lfdNr,
        ]
      );
    }

    await client.query('COMMIT');

    const pRes = await pool.query(
      `
      SELECT *
      FROM reklamation_positionen
      WHERE reklamation_id = $1
      ORDER BY lfd_nr ASC NULLS LAST, pos_id ASC NULLS LAST
      `,
      [id]
    );

    res.json({ ...reklamation, positionen: pRes.rows });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Fehler beim Bearbeiten der Reklamation:', error);
    res.status(500).json({ message: 'Serverfehler beim Bearbeiten der Reklamation' });
  } finally {
    client.release();
  }
});

// ------------------------------------------------------
// PATCH /api/reklamationen/:id – Teilupdate (Notiz + optional Felder)
// Notiz schreiben nur Admin/Supervisor
// ------------------------------------------------------
router.patch('/:id', verifyToken(), async (req, res) => {
  const { role, name } = req.user;
  const { id } = req.params;

  if (!isPrivileged(role)) {
    return res.status(403).json({ message: 'Keine Berechtigung' });
  }

  const { notiz, status, versand, tracking_id, ls_nummer_grund, letzte_aenderung } = req.body || {};

  const sets = [];
  const params = [];
  let idx = 1;

  // Notiz handling (trim + leer => NULL)
  const n = normalizeNotiz(notiz);
  if (n.hasNotiz) {
    sets.push(`notiz = $${idx++}`);
    params.push(n.value);
    sets.push(`notiz_von = $${idx++}`);
    params.push(name);
    sets.push(`notiz_am = NOW()`);
  }

  // optionale Felder (falls ihr diese PATCH-Variante nutzt)
  if (status !== undefined) {
    sets.push(`status = $${idx++}`);
    params.push(status);
  }
  if (versand !== undefined) {
    sets.push(`versand = $${idx++}`);
    params.push(!!versand);
  }
  if (tracking_id !== undefined) {
    sets.push(`tracking_id = $${idx++}`);
    params.push(tracking_id ? String(tracking_id) : null);
  }
  if (ls_nummer_grund !== undefined) {
    sets.push(`ls_nummer_grund = $${idx++}`);
    params.push(ls_nummer_grund ? String(ls_nummer_grund) : null);
  }
  if (letzte_aenderung !== undefined) {
    sets.push(`letzte_aenderung = $${idx++}`);
    params.push(letzte_aenderung);
  }

  if (sets.length === 0) {
    return res.status(400).json({ message: 'Kein gültiges Feld im PATCH-Payload' });
  }

  params.push(id);

  try {
    const q = `
      UPDATE reklamationen
      SET ${sets.join(', ')}
      WHERE id = $${idx}
      RETURNING *
    `;

    const rRes = await pool.query(q, params);
    if (rRes.rows.length === 0) {
      return res.status(404).json({ message: 'Reklamation nicht gefunden' });
    }

    res.json(rRes.rows[0]);
  } catch (error) {
    console.error('Fehler beim PATCH der Reklamation:', error);
    res.status(500).json({ message: 'Serverfehler beim PATCH der Reklamation' });
  }
});

// ------------------------------------------------------
// DELETE /api/reklamationen/:id – Löschen (nur Admin/Supervisor)
// ------------------------------------------------------
router.delete('/:id', verifyToken(), async (req, res) => {
  const { role } = req.user;
  const { id } = req.params;

  if (!isPrivileged(role)) {
    return res.status(403).json({ message: 'Keine Berechtigung' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    await client.query('DELETE FROM reklamation_positionen WHERE reklamation_id = $1', [id]);
    const del = await client.query('DELETE FROM reklamationen WHERE id = $1 RETURNING id', [id]);

    if (del.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ message: 'Reklamation nicht gefunden' });
    }

    await client.query('COMMIT');
    res.json({ message: 'Reklamation gelöscht' });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Fehler beim Löschen der Reklamation:', error);
    res.status(500).json({ message: 'Serverfehler beim Löschen der Reklamation' });
  } finally {
    client.release();
  }
});

module.exports = router;
