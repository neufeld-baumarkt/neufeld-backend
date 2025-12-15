// /routes/reklamationen.js – komplette Datei, GET wie bei dir + POST passend zu deinem Modal
const express = require('express');
const router = express.Router();
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

// GET /api/reklamationen – Liste nach Rolle/Filiale (genau wie bei dir)
router.get('/', verifyToken(), async (req, res) => {
  const { role, filiale } = req.user;

  try {
    let query;
    let params = [];

    if (role === 'Admin' || filiale === 'alle') {
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

// GET /api/reklamationen/:id – Detailansicht mit Positionen (genau wie bei dir)
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

// POST /api/reklamationen – Neue Reklamation anlegen (passend zu deinem flachen formData)
router.post('/', verifyToken(), async (req, res) => {
  const user = req.user;
  const data = req.body;  // flaches Objekt aus deinem Modal

  // Filial-Nutzer dürfen nur eigene Filiale anlegen
  if (user.role === 'Filiale' && data.filiale && data.filiale !== user.filiale) {
    return res.status(403).json({ message: 'Nur eigene Filiale anlegbar' });
  }

  try {
    // Duplikatsprüfung für rekla_nr
    const dupeCheck = await pool.query('SELECT id FROM reklamationen WHERE rekla_nr = $1', [data.rekla_nr]);
    if (dupeCheck.rows.length > 0) {
      return res.status(409).json({ message: 'Reklamationsnummer bereits vergeben' });
    }

    // INSERT – alle Felder aus deinem formData
    const query = `
      INSERT INTO reklamationen (
        filiale, art, datum, rekla_nr, lieferant, ls_nummer_grund,
        versand, tracking_id, artikelnummer, ean,
        bestell_menge, bestell_einheit, rekla_menge, rekla_einheit,
        status, letzte_aenderung
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        $11, $12, $13, $14, $15, $16
      ) RETURNING id;
    `;

    const values = [
      data.filiale || null,
      data.art || null,
      data.datum || null,
      data.rekla_nr || null,
      data.lieferant || null,
      data.ls_nummer_grund || null,
      data.versand || false,
      data.tracking_id || null,
      data.artikelnummer || null,
      data.ean || null,
      data.bestell_menge || null,
      data.bestell_einheit || null,
      data.rekla_menge || null,
      data.rekla_einheit || null,
      data.status || 'Angelegt',
      data.letzte_aenderung || null
    ];

    const result = await pool.query(query, values);
    const newId = result.rows[0].id;

    console.log(`Neue Reklamation angelegt – ID: ${newId}, Rekla-Nr: ${data.rekla_nr}`);

    res.status(201).json({ message: 'Reklamation erfolgreich angelegt', id: newId });
  } catch (err) {
    console.error('Fehler beim Anlegen der Reklamation:', err.message);
    res.status(500).json({ message: 'Serverfehler beim Speichern', error: err.message });
  }
});

module.exports = router;