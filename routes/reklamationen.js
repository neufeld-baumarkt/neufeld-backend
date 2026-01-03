// routes/reklamationen.js – V1.0.1 (FIX) mit LFD_NR-Aggregaten für die Listenansicht
const express = require('express');
const router = express.Router();
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

// Rollen mit globaler Sicht (für GET-Anzeige)
const rollenMitGlobalzugriff = ['Admin', 'Supervisor', 'Manager-1', 'Geschäftsführer'];

// Rollen mit Bearbeitungs- und Löschrecht – NUR diese beiden!
const rollenMitBearbeitungsrecht = ['Admin', 'Supervisor'];

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
      'SELECT * FROM reklamation_positionen WHERE reklamation_id = $1 ORDER BY pos_id',
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
 * Hinweis: lfd_nr-Vergabe ist hier noch NICHT enthalten (kommt als nächster Schritt).
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
      data.filiale || user.filiale || null,
      data.status || 'Angelegt',
      data.ls_nummer_grund || null,
      data.versand || false,
      data.tracking_id || null,
    ];

    const reklaResult = await client.query(reklaQuery, reklaValues);
    const reklamationId = reklaResult.rows[0].id;

    if (data.positionen && Array.isArray(data.positionen) && data.positionen.length > 0) {
      const posQuery = `
        INSERT INTO reklamation_positionen (
          reklamation_id, artikelnummer, ean, bestell_menge, bestell_einheit,
          rekla_menge, rekla_einheit
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7);
      `;

      for (const pos of data.positionen) {
        const posValues = [
          reklamationId,
          pos.artikelnummer || null,
          pos.ean || null,
          pos.bestell_menge || null,
          pos.bestell_einheit || null,
          pos.rekla_menge || null,
          pos.rekla_einheit || null,
        ];

        await client.query(posQuery, posValues);
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
      WHERE id = $10;
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

    await client.query('DELETE FROM reklamation_positionen WHERE reklamation_id = $1', [id]);

    if (data.positionen && Array.isArray(data.positionen) && data.positionen.length > 0) {
      const posQuery = `
        INSERT INTO reklamation_positionen (
          reklamation_id, artikelnummer, ean, bestell_menge, bestell_einheit,
          rekla_menge, rekla_einheit
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7);
      `;

      for (const pos of data.positionen) {
        const posValues = [
          id,
          pos.artikelnummer || null,
          pos.ean || null,
          pos.bestell_menge || null,
          pos.bestell_einheit || null,
          pos.rekla_menge || null,
          pos.rekla_einheit || null,
        ];

        await client.query(posQuery, posValues);
      }
    }

    await client.query('COMMIT');

    console.log(`Reklamation vollständig bearbeitet – ID: ${id} von ${user.name} (${user.role})`);
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
      `Reklamation Teil-Update – ID: ${id} von ${user.name} (${user.role}): ${JSON.stringify(updates)}`
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
