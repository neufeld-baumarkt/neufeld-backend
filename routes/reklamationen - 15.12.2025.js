// /routes/reklamationen.js
const express = require('express');
const router = express.Router();
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

/*
────────────────────────────────────────────────────────
GET /api/reklamationen – Liste nach Rolle/Filiale
────────────────────────────────────────────────────────
*/
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

/*
────────────────────────────────────────────────────────
GET /api/reklamationen/:id – Detailansicht mit Positionen
────────────────────────────────────────────────────────
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

/*
────────────────────────────────────────────────────────
POST /api/reklamationen – Neue Reklamation anlegen
────────────────────────────────────────────────────────
*/
router.post('/', verifyToken(), async (req, res) => {
  const client = await pool.connect();
  const { reklamation, positionen } = req.body;
  const { filiale } = req.user;

  try {
    await client.query('BEGIN');

    // 1. Reklamation anlegen
    const insertReklamationQuery = `
      INSERT INTO reklamationen
        (rekla_nr, datum, lieferant_id, art_id, status_id, filiale)
      VALUES
        ($1, $2, $3, $4, $5, $6)
      RETURNING id
    `;

    const reklamationValues = [
      reklamation.rekla_nr,
      reklamation.datum,
      reklamation.lieferant_id,
      reklamation.art_id,
      reklamation.status_id,
      filiale
    ];

    const reklamationResult = await client.query(
      insertReklamationQuery,
      reklamationValues
    );

    const reklamationId = reklamationResult.rows[0].id;

    // 2. Positionen anlegen
    for (const pos of positionen) {
      const insertPositionQuery = `
        INSERT INTO reklamation_positionen
          (reklamation_id, artikelnummer, ean, rekla_menge, einheit_id, bestell_menge, bestell_einheit_id)
        VALUES
          ($1, $2, $3, $4, $5, $6, $7)
      `;

      const positionValues = [
        reklamationId,
        pos.artikelnummer,
        pos.ean,
        pos.rekla_menge,
        pos.einheit_id,
        pos.bestell_menge,
        pos.bestell_einheit_id
      ];

      await client.query(insertPositionQuery, positionValues);
    }

    await client.query('COMMIT');

    res.status(201).json({
      message: 'Reklamation erfolgreich angelegt',
      reklamation_id: reklamationId
    });

  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Fehler beim Anlegen der Reklamation:', error);
    res.status(500).json({ message: 'Fehler beim Anlegen der Reklamation' });
  } finally {
    client.release();
  }
});

module.exports = router;
