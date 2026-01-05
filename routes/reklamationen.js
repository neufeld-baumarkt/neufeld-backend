// routes/reklamationen.js
const express = require('express');
const router = express.Router();
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

/**
 * Rollen-Helfer
 */
function canEditNote(user) {
  const role = (user?.role || '').toLowerCase();
  return role === 'admin' || role === 'supervisor';
}

/**
 * GET /api/reklamationen
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
  } catch (err) {
    console.error('Fehler beim Abrufen der Reklamationen:', err);
    res.status(500).json({ error: 'Fehler beim Laden der Reklamationen' });
  }
});

/**
 * GET /api/reklamationen/:id
 */
router.get('/:id', verifyToken(), async (req, res) => {
  const { id } = req.params;

  try {
    const reklamation = await pool.query(
      'SELECT * FROM reklamationen WHERE id = $1',
      [id]
    );

    const positionen = await pool.query(
      'SELECT * FROM reklamation_positionen WHERE reklamation_id = $1 ORDER BY lfd_nr ASC',
      [id]
    );

    if (reklamation.rows.length === 0) {
      return res.status(404).json({ error: 'Reklamation nicht gefunden' });
    }

    res.json({
      reklamation: reklamation.rows[0],
      positionen: positionen.rows
    });
  } catch (err) {
    console.error('Fehler beim Laden der Reklamationsdetails:', err);
    res.status(500).json({ error: 'Fehler beim Laden der Details' });
  }
});

/**
 * PATCH /api/reklamationen/:id
 * → erweitert um Notiz
 */
router.patch('/:id', verifyToken(), async (req, res) => {
  const { id } = req.params;
  const { notiz } = req.body;
  const user = req.user;

  if (!canEditNote(user)) {
    return res.status(403).json({ error: 'Keine Berechtigung für Notizen' });
  }

  try {
    const cleanNote =
      typeof notiz === 'string' && notiz.trim().length > 0
        ? notiz.trim()
        : null;

    await pool.query(
      `
      UPDATE reklamationen
      SET
        notiz = $1,
        notiz_von = $2,
        notiz_am = NOW()
      WHERE id = $3
      `,
      [cleanNote, user.name || null, id]
    );

    res.json({ success: true });
  } catch (err) {
    console.error('Fehler beim Speichern der Notizen:', err);
    res.status(500).json({ error: 'Notiz konnte nicht gespeichert werden' });
  }
});

module.exports = router;
