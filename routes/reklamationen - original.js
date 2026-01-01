// /routes/reklamationen.js – finale Version, id automatisch mit uuid_generate_v4()
const express = require('express');
const router = express.Router();
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

// GET /api/reklamationen – Liste nach Rolle/Filiale
router.get('/', verifyToken(), async (req, res) => {
  const { role, filiale } = req.user;

  try {
    let query;
    let params = [];

    // Globale Sicht für Admin, Supervisor & Co. ODER wenn keine eigene Filiale zugewiesen ist (Zentrale)
    const rollenMitGlobalzugriff = ['Admin', 'Supervisor', 'Manager-1', 'Geschäftsführer'];

    if (
      rollenMitGlobalzugriff.includes(role) ||
      filiale === 'alle' ||
      !filiale ||                    // NEU: Wenn keine Filiale im Token → Zentrale-User → alles sehen
      filiale === ''                 // Falls leerer String
    ) {
      query = 'SELECT * FROM reklamationen ORDER BY datum DESC';
    } else {
      query = 'SELECT * FROM reklamationen WHERE filiale = $1 ORDER BY datum DESC';
      params = [filiale];
    }

    const result = await pool.query(query, params);
    res.json(result.rows);
  } catch (error) {
    console.error('Fehler beim Abrufen der Reklamationen:', error);
    res.status(500).json({ message: 'Fehler beim Abrufen der Reklamationen' });
  }
});

// GET /api/reklamationen/:id – Detailansicht mit Positionen
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

    const rollenMitGlobalzugriff = ['Admin', 'Supervisor', 'Manager-1', 'Geschäftsführer'];
    const istErlaubt =
      rollenMitGlobalzugriff.includes(role) ||
      filiale === 'alle' ||
      !filiale ||                                // Auch hier für Konsistenz
      filiale === '' ||
      filiale === reklamation.filiale;

    if (!istErlaubt) {
      return res.status(403).json({ message: 'Zugriff verweigert' });
    }

    const positionenResult = await pool.query(
      'SELECT * FROM reklamation_positionen WHERE reklamation_id = $1',
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

// POST /api/reklamationen – Neue Reklamation + Position anlegen (id automatisch)
router.post('/', verifyToken(), async (req, res) => {
  const user = req.user;
  const data = req.body;

  if (user.role === 'Filiale' && data.filiale && data.filiale !== user.filiale) {
    return res.status(403).json({ message: 'Nur eigene Filiale anlegbar' });
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // Reklamation anlegen (id wird automatisch durch uuid_generate_v4() generiert)
    const reklaQuery = `
      INSERT INTO reklamationen (
        datum, letzte_aenderung, art, rekla_nr, lieferant, filiale, status,
        ls_nummer_grund, versand, tracking_id
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
      ) RETURNING id;
    `;

    const reklaValues = [
      data.datum || null,
      data.letzte_aenderung || null,
      data.art || null,
      data.rekla_nr || null,
      data.lieferant || null,
      data.filiale || null,
      data.status || 'Angelegt',
      data.ls_nummer_grund || null,
      data.versand || false,
      data.tracking_id || null
    ];

    const reklaResult = await client.query(reklaQuery, reklaValues);
    const reklamationId = reklaResult.rows[0].id;

    // Position anlegen
    const posQuery = `
      INSERT INTO reklamation_positionen (
        reklamation_id, artikelnummer, ean, bestell_menge, bestell_einheit,
        rekla_menge, rekla_einheit
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7
      );
    `;

    const posValues = [
      reklamationId,
      data.artikelnummer || null,
      data.ean || null,
      data.bestell_menge || null,
      data.bestell_einheit || null,
      data.rekla_menge || null,
      data.rekla_einheit || null
    ];

    await client.query(posQuery, posValues);

    await client.query('COMMIT');

    console.log(`Reklamation angelegt – ID: ${reklamationId}, Rekla-Nr: ${data.rekla_nr}`);

    res.status(201).json({ message: 'Reklamation erfolgreich angelegt', id: reklamationId });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Fehler beim Anlegen:', err.message);
    res.status(500).json({ message: 'Serverfehler beim Speichern', error: err.message });
  } finally {
    client.release();
  }
});

module.exports = router;