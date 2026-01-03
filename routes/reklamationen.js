// routes/reklamationen.js – V1.1.0 (LFD_NR-Auto pro Filiale + Jahr, Edit-Fall B = Nummern bleiben stabil)
const express = require('express');
const router = express.Router();
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

// Rollen mit globaler Sicht (für GET-Anzeige)
const rollenMitGlobalzugriff = ['Admin', 'Supervisor', 'Manager-1', 'Geschäftsführer'];

// Rollen mit Bearbeitungs- und Löschrecht – NUR diese beiden!
const rollenMitBearbeitungsrecht = ['Admin', 'Supervisor'];

/**
 * Vergibt lfd_nr blockweise, transaktionssicher (Row-Lock) pro Filiale + Jahr.
 * - Nutzt Tabelle: lfd_nr_counter (filiale, jahr, last_value, updated_at)
 * - Garantiert: einmalig, fortlaufend, kein Reuse (auch nicht bei Delete)
 */
async function allocateLfdNumbers(client, filiale, jahr, count) {
  if (!filiale || typeof filiale !== 'string' || filiale.trim() === '') {
    throw new Error('LFD_NR: filiale fehlt/ungültig');
  }
  if (!Number.isInteger(jahr) || jahr < 2000 || jahr > 3000) {
    throw new Error('LFD_NR: jahr fehlt/ungültig');
  }
  if (!Number.isInteger(count) || count <= 0) {
    return { start: null, end: null };
  }

  // ensure row exists
  await client.query(
    `
    INSERT INTO lfd_nr_counter (filiale, jahr, last_value)
    VALUES ($1, $2, 0)
    ON CONFLICT (filiale, jahr) DO NOTHING;
    `,
    [filiale, jahr]
  );

  // lock + increment atomar
  const result = await client.query(
    `
    WITH locked AS (
      SELECT last_value
      FROM lfd_nr_counter
      WHERE filiale = $1 AND jahr = $2
      FOR UPDATE
    ),
    upd AS (
      UPDATE lfd_nr_counter c
      SET last_value = locked.last_value + $3,
          updated_at = now()
      FROM locked
      WHERE c.filiale = $1 AND c.jahr = $2
      RETURNING locked.last_value AS old_last, c.last_value AS new_last
    )
    SELECT old_last, new_last FROM upd;
    `,
    [filiale, jahr, count]
  );

  if (!result.rows || result.rows.length === 0) {
    throw new Error('LFD_NR: Counter-Update fehlgeschlagen');
  }

  const oldLast = Number(result.rows[0].old_last);
  const newLast = Number(result.rows[0].new_last);

  if (!Number.isFinite(oldLast) || !Number.isFinite(newLast) || newLast - oldLast !== count) {
    throw new Error('LFD_NR: Counter-Werte inkonsistent');
  }

  return { start: oldLast + 1, end: newLast };
}

function getCurrentYear() {
  return new Date().getFullYear();
}

/**
 * GET /api/reklamationen
 * Liefert Reklamationen gefiltert nach Rolle/Filiale.
 * Zusätzlich:
 * - position_count = Anzahl Positionen
 * - min_lfd_nr     = kleinste lfd_nr der Positionen (für Anzeige "30+3")
 */
router.get('/', verifyToken(), async (req, res) => {
  const { role, filiale } = req.user;

  try {
    let query;
    let params = [];

    const baseSelect = `
      SELECT
        r.*,
        COUNT(p.id)    AS position_count,
        MIN(p.lfd_nr)  AS min_lfd_nr
      FROM reklamationen r
      LEFT JOIN reklamation_positionen p ON r.id = p.reklamation_id
    `;

    if (
      rollenMitGlobalzugriff.includes(role) ||
      filiale === 'alle' ||
      !filiale ||
      filiale === ''
    ) {
      query = `
        ${baseSelect}
        GROUP BY r.id
        ORDER BY r.datum DESC
      `;
    } else {
      query = `
        ${baseSelect}
        WHERE r.filiale = $1
        GROUP BY r.id
        ORDER BY r.datum DESC
      `;
      params = [filiale];
    }

    const result = await pool.query(query, params);
    res.json(result.rows);
  } catch (error) {
    console.error('Fehler beim Abrufen der Reklamationen:', error);
    res.status(500).json({ message: 'Fehler beim Abrufen der Reklamationen' });
  }
});

/**
 * GET /api/reklamationen/:id
 * Detail + Positionen, gleiche Berechtigungsprüfung wie Liste
 */
router.get('/:id', verifyToken(), async (req, res) => {
  const { id } = req.params;
  const { role, filiale } = req.user;

  try {
    const reklamationResult = await pool.query(
      'SELECT * FROM reklamationen WHERE id = $1',
      [id]
    );

    if (reklamationResult.rows.length === 0) {
      return res.status(404).json({ message: 'Reklamation nicht gefunden' });
    }

    const reklamation = reklamationResult.rows[0];

    const istErlaubt =
      rollenMitGlobalzugriff.includes(role) ||
      filiale === 'alle' ||
      !filiale ||
      filiale === '' ||
      filiale === reklamation.filiale;

    if (!istErlaubt) {
      return res.status(403).json({ message: 'Zugriff verweigert' });
    }

    const positionenResult = await pool.query(
      `
      SELECT *
      FROM reklamation_positionen
      WHERE reklamation_id = $1
      ORDER BY lfd_nr NULLS LAST, pos_id;
      `,
      [id]
    );

    res.json({
      reklamation,
      positionen: positionenResult.rows,
    });
  } catch (error) {
    console.error('Fehler beim Abrufen der Detailreklamation:', error);
    res.status(500).json({ message: 'Fehler beim Abrufen der Detailreklamation' });
  }
});

/**
 * POST /api/reklamationen
 * Neue Reklamation anlegen (alle User, Filialuser nur eigene Filiale)
 * Neu: lfd_nr wird automatisch vergeben pro Filiale + Jahr, einmalig/fortlaufend, transaktionssicher.
 */
router.post('/', verifyToken(), async (req, res) => {
  const user = req.user;
  const data = req.body;

  if (user.role === 'Filiale' && data.filiale && data.filiale !== user.filiale) {
    return res.status(403).json({ message: 'Nur eigene Filiale anlegbar' });
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const filialeFinal = (data.filiale || user.filiale || '').toString();

    const reklaQuery = `
      INSERT INTO reklamationen (
        datum, letzte_aenderung, art, rekla_nr, lieferant, filiale, status,
        ls_nummer_grund, versand, tracking_id
      )
      VALUES (
        $1, CURRENT_DATE, $2, $3, $4, $5, $6,
        $7, $8, $9
      )
      RETURNING id;
    `;

    const reklaValues = [
      data.datum || null,
      data.art || null,
      data.rekla_nr || null,
      data.lieferant || null,
      filialeFinal || null,
      data.status || 'Angelegt',
      data.ls_nummer_grund || null,
      data.versand || false,
      data.tracking_id || null,
    ];

    const reklaResult = await client.query(reklaQuery, reklaValues);
    const reklamationId = reklaResult.rows[0].id;

    const positionen = Array.isArray(data.positionen) ? data.positionen : [];
    if (positionen.length > 0) {
      const jahr = getCurrentYear();
      const { start } = await allocateLfdNumbers(client, filialeFinal, jahr, positionen.length);

      const posQuery = `
        INSERT INTO reklamation_positionen (
          reklamation_id, artikelnummer, ean, bestell_menge, bestell_einheit,
          rekla_menge, rekla_einheit, lfd_nr
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
      `;

      let nextNr = start;
      for (const pos of positionen) {
        const posValues = [
          reklamationId,
          pos.artikelnummer || null,
          pos.ean || null,
          pos.bestell_menge || null,
          pos.bestell_einheit || null,
          pos.rekla_menge || null,
          pos.rekla_einheit || null,
          nextNr,
        ];
        await client.query(posQuery, posValues);
        nextNr++;
      }
    }

    await client.query('COMMIT');

    console.log(
      `Reklamation angelegt – ID: ${reklamationId} von ${user.name} (${user.role})`
    );

    res.status(201).json({ message: 'Reklamation erfolgreich angelegt', id: reklamationId });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Fehler beim Anlegen:', err.message);
    res.status(500).json({ message: 'Serverfehler beim Speichern' });
  } finally {
    client.release();
  }
});

/**
 * PUT /api/reklamationen/:id
 * Komplett bearbeiten (Reklamation + Positionen ersetzen)
 * Nur Admin/Supervisor
 *
 * Fall B (gewünscht): lfd_nr bleibt stabil.
 * - Vorhandene lfd_nr werden als "zulässig" aus DB gelesen.
 * - Beim Speichern werden nur lfd_nr akzeptiert, die vorher zu dieser Reklamation gehörten.
 * - Neue Positionen (ohne gültige lfd_nr) bekommen neue Nummern aus dem Counter (pro Filiale+Jahr).
 */
router.put('/:id', verifyToken(), async (req, res) => {
  const { id } = req.params;
  const user = req.user;
  const data = req.body;

  if (!rollenMitBearbeitungsrecht.includes(user.role)) {
    return res.status(403).json({
      message: 'Zugriff verweigert: Nur Admin oder Supervisor dürfen bearbeiten',
    });
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const updateQuery = `
      UPDATE reklamationen SET
        datum = $1,
        letzte_aenderung = CURRENT_DATE,
        art = $2,
        rekla_nr = $3,
        lieferant = $4,
        filiale = $5,
        status = $6,
        ls_nummer_grund = $7,
        versand = $8,
        tracking_id = $9
      WHERE id = $10
      RETURNING filiale;
    `;

    const updateValues = [
      data.datum || null,
      data.art || null,
      data.rekla_nr || null,
      data.lieferant || null,
      data.filiale || null,
      data.status || null,
      data.ls_nummer_grund || null,
      data.versand ?? false,
      data.tracking_id || null,
      id,
    ];

    const result = await client.query(updateQuery, updateValues);

    if (result.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ message: 'Reklamation nicht gefunden' });
    }

    const filialeFinal = (result.rows[0].filiale || '').toString();

    // bestehende lfd_nr dieser Reklamation "merken" (Schutz gegen Manipulation)
    const existingLfdRes = await client.query(
      `
      SELECT lfd_nr
      FROM reklamation_positionen
      WHERE reklamation_id = $1 AND lfd_nr IS NOT NULL;
      `,
      [id]
    );
    const existingLfdSet = new Set(existingLfdRes.rows.map((r) => Number(r.lfd_nr)).filter(Number.isFinite));

    // alte Positionen löschen (wir setzen sie neu ein, aber mit stabilen lfd_nr)
    await client.query('DELETE FROM reklamation_positionen WHERE reklamation_id = $1', [id]);

    const positionen = Array.isArray(data.positionen) ? data.positionen : [];
    if (positionen.length > 0) {
      // ermitteln, welche Positionen neue Nummern brauchen
      const needsNew = [];
      const keepNr = [];

      for (const pos of positionen) {
        const incoming = pos && pos.lfd_nr !== undefined && pos.lfd_nr !== null ? Number(pos.lfd_nr) : null;
        if (Number.isFinite(incoming) && existingLfdSet.has(incoming)) {
          keepNr.push({ pos, lfd_nr: incoming });
        } else {
          needsNew.push(pos);
        }
      }

      // neue Nummern blockweise ziehen
      let nextNew = null;
      if (needsNew.length > 0) {
        const jahr = getCurrentYear();
        const alloc = await allocateLfdNumbers(client, filialeFinal, jahr, needsNew.length);
        nextNew = alloc.start;
      }

      const posQuery = `
        INSERT INTO reklamation_positionen (
          reklamation_id, artikelnummer, ean, bestell_menge, bestell_einheit,
          rekla_menge, rekla_einheit, lfd_nr
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
      `;

      // Wichtig: Reihenfolge beibehalten, wie sie aus dem Frontend kommt.
      // Wir setzen lfd_nr je Position stabil (falls gültig) oder neu.
      for (const pos of positionen) {
        let lfdNrToUse = null;

        const incoming = pos && pos.lfd_nr !== undefined && pos.lfd_nr !== null ? Number(pos.lfd_nr) : null;
        if (Number.isFinite(incoming) && existingLfdSet.has(incoming)) {
          lfdNrToUse = incoming;
        } else {
          lfdNrToUse = nextNew;
          nextNew++;
        }

        const posValues = [
          id,
          pos.artikelnummer || null,
          pos.ean || null,
          pos.bestell_menge || null,
          pos.bestell_einheit || null,
          pos.rekla_menge || null,
          pos.rekla_einheit || null,
          lfdNrToUse,
        ];

        await client.query(posQuery, posValues);
      }
    }

    await client.query('COMMIT');

    console.log(
      `Reklamation vollständig bearbeitet – ID: ${id} von ${user.name} (${user.role})`
    );
    res.json({ message: 'Reklamation erfolgreich aktualisiert' });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Fehler beim Bearbeiten:', err.message);
    res.status(500).json({ message: 'Serverfehler beim Aktualisieren' });
  } finally {
    client.release();
  }
});

/**
 * PATCH /api/reklamationen/:id
 * Teil-Update: status, versand, tracking_id, ls_nummer_grund
 * Nur Admin/Supervisor
 */
router.patch('/:id', verifyToken(), async (req, res) => {
  const { id } = req.params;
  const user = req.user;
  const updates = req.body;

  if (!rollenMitBearbeitungsrecht.includes(user.role)) {
    return res.status(403).json({
      message: 'Zugriff verweigert: Nur Admin oder Supervisor dürfen Änderungen vornehmen',
    });
  }

  if (Object.keys(updates).length === 0) {
    return res.status(400).json({ message: 'Keine Änderungen übermittelt' });
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const checkResult = await client.query('SELECT id FROM reklamationen WHERE id = $1', [id]);
    if (checkResult.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ message: 'Reklamation nicht gefunden' });
    }

    const allowedFields = ['status', 'versand', 'tracking_id', 'ls_nummer_grund'];
    const setClauses = [];
    const values = [];
    let paramIndex = 1;

    for (const field of allowedFields) {
      if (updates[field] !== undefined) {
        setClauses.push(`${field} = $${paramIndex}`);
        values.push(updates[field]);
        paramIndex++;
      }
    }

    if (setClauses.length === 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({ message: 'Keine gültigen Felder zum Updaten' });
    }

    setClauses.push('letzte_aenderung = CURRENT_DATE');

    const query = `UPDATE reklamationen SET ${setClauses.join(', ')} WHERE id = $${paramIndex}`;
    values.push(id);

    await client.query(query, values);

    await client.query('COMMIT');

    console.log(
      `Reklamation Teil-Update – ID: ${id} von ${user.name} (${user.role}): ${JSON.stringify(
        updates
      )}`
    );

    res.json({ message: 'Reklamation erfolgreich teilweise aktualisiert' });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Fehler beim PATCH:', err.message);
    res.status(500).json({ message: 'Serverfehler beim Aktualisieren' });
  } finally {
    client.release();
  }
});

/**
 * DELETE /api/reklamationen/:id
 * Nur Admin/Supervisor
 */
router.delete('/:id', verifyToken(), async (req, res) => {
  const { id } = req.params;
  const user = req.user;

  if (!rollenMitBearbeitungsrecht.includes(user.role)) {
    return res.status(403).json({
      message: 'Zugriff verweigert: Nur Admin oder Supervisor dürfen löschen',
    });
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    await client.query('DELETE FROM reklamation_positionen WHERE reklamation_id = $1', [id]);
    const result = await client.query(
      'DELETE FROM reklamationen WHERE id = $1 RETURNING rekla_nr',
      [id]
    );

    if (result.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ message: 'Reklamation nicht gefunden' });
    }

    await client.query('COMMIT');

    console.log(
      `Reklamation gelöscht – ID: ${id}, Nr: ${result.rows[0].rekla_nr} von ${user.name} (${user.role})`
    );

    res.json({ message: 'Reklamation erfolgreich gelöscht' });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Fehler beim Löschen:', err.message);
    res.status(500).json({ message: 'Serverfehler beim Löschen' });
  } finally {
    client.release();
  }
});

module.exports = router;
