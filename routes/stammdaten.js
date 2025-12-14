// routes/stammdaten.js
const express = require('express');
const router = express.Router();

// Pool aus db.js holen
const pool = require('../db');

// Auth-Middleware
const verifyToken = require('../middleware/verifyToken');
router.use(verifyToken);

// Alle Routen ohne zusätzlichen Prefix (weil /api schon in server.js gesetzt ist)

// Filialen – angepasst an deine Tabellenstruktur (Spalte "name" statt "bezeichnung")
router.get('/filialen', async (req, res) => {
  try {
    const result = await pool.query('SELECT id, name FROM filialen WHERE aktiv = true ORDER BY name ASC');
    res.json(result.rows);
  } catch (err) {
    console.error('Fehler /api/filialen:', err);
    res.status(500).json({ error: 'Datenbankfehler' });
  }
});

// Art der Reklamation
router.get('/art_der_reklamation', async (req, res) => {
  try {
    const result = await pool.query('SELECT id, bezeichnung AS name FROM art_der_reklamation ORDER BY bezeichnung ASC');
    res.json(result.rows);
  } catch (err) {
    console.error('Fehler /api/art_der_reklamation:', err);
    res.status(500).json({ error: 'Datenbankfehler' });
  }
});

// Lieferanten
router.get('/lieferanten', async (req, res) => {
  try {
    const result = await pool.query('SELECT id, bezeichnung AS name FROM lieferanten ORDER BY bezeichnung ASC');
    res.json(result.rows);
  } catch (err) {
    console.error('Fehler /api/lieferanten:', err);
    res.status(500).json({ error: 'Datenbankfehler' });
  }
});

// Einheit
router.get('/einheit', async (req, res) => {
  try {
    const result = await pool.query('SELECT id, bezeichnung AS name FROM einheit ORDER BY bezeichnung ASC');
    res.json(result.rows);
  } catch (err) {
    console.error('Fehler /api/einheit:', err);
    res.status(500).json({ error: 'Datenbankfehler' });
  }
});

// Status
router.get('/status', async (req, res) => {
  try {
    const result = await pool.query('SELECT id, bezeichnung AS name FROM status ORDER BY bezeichnung ASC');
    res.json(result.rows);
  } catch (err) {
    console.error('Fehler /api/status:', err);
    res.status(500).json({ error: 'Datenbankfehler' });
  }
});

module.exports = router;