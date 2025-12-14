// routes/stammdaten.js – angepasste Version

const express = require('express');
const router = express.Router();
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');
router.use(verifyToken); // Falls Auth wirklich nötig ist – siehe unten!

// Filialen
router.get('/filialen', async (req, res) => {
  try {
    const result = await pool.query('SELECT name FROM filialen WHERE aktiv = true ORDER BY name ASC');
    const data = result.rows.map(row => row.name);
    console.log('/api/filialen aufgerufen –', data.length, 'Einträge');
    res.json(data);
  } catch (err) {
    console.error('Fehler /api/filialen:', err.message);
    res.status(500).json({ error: 'Datenbankfehler' });
  }
});

// Reklamationsarten
router.get('/reklamationsarten', async (req, res) => {
  try {
    const result = await pool.query('SELECT bezeichnung FROM art_der_reklamation ORDER BY bezeichnung ASC');
    const data = result.rows.map(row => row.bezeichnung);
    console.log('/api/reklamationsarten aufgerufen –', data.length, 'Einträge');
    res.json(data);
  } catch (err) {
    console.error('Fehler /api/reklamationsarten:', err.message);
    res.status(500).json({ error: 'Datenbankfehler' });
  }
});

// Lieferanten
router.get('/lieferanten', async (req, res) => {
  try {
    const result = await pool.query('SELECT bezeichnung FROM lieferanten ORDER BY bezeichnung ASC');
    const data = result.rows.map(row => row.bezeichnung);
    console.log('/api/lieferanten aufgerufen –', data.length, 'Einträge');
    res.json(data);
  } catch (err) {
    console.error('Fehler /api/lieferanten:', err.message);
    res.status(500).json({ error: 'Datenbankfehler' });
  }
});

// Einheiten
router.get('/einheiten', async (req, res) => {
  try {
    const result = await pool.query('SELECT bezeichnung FROM einheit ORDER BY bezeichnung ASC');
    const data = result.rows.map(row => row.bezeichnung);
    console.log('/api/einheiten aufgerufen –', data.length, 'Einträge');
    res.json(data);
  } catch (err) {
    console.error('Fehler /api/einheiten:', err.message);
    res.status(500).json({ error: 'Datenbankfehler' });
  }
});

// Status
router.get('/status', async (req, res) => {
  try {
    const result = await pool.query('SELECT bezeichnung FROM status ORDER BY id ASC');
    const data = result.rows.map(row => row.bezeichnung);
    console.log('/api/status aufgerufen –', data.length, 'Einträge');
    res.json(data);
  } catch (err) {
    console.error('Fehler /api/status:', err.message);
    res.status(500).json({ error: 'Datenbankfehler' });
  }
});

module.exports = router;