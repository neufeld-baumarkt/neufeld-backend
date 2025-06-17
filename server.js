// /routes/reklamationen.js
const express = require('express');
const router = express.Router();
const verifyToken = require('../middleware/verifyToken');
const pool = require('../db'); // zentraler Pool auslagern

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

module.exports = router;
