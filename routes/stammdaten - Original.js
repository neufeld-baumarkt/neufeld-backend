// routes/stammdaten.js
const express = require('express');
const router = express.Router();
const pool = require('../db');

// Auth temporär deaktiviert – für Testzwecke!
// Sobald alles läuft, kannst du die Zeile wieder aktivieren und Token im Frontend mitsenden
// const verifyToken = require('../middleware/verifyToken');
// router.use(verifyToken);

// Filialen – nur die Namen als Strings
router.get('/filialen', async (req, res) => {
  try {
    const result = await pool.query('SELECT name FROM filialen WHERE aktiv = true ORDER BY name ASC');
    const data = result.rows.map(row => row.name);
    console.log('/api/filialen aufgerufen –', data.length, 'Einträge geladen');
    res.json(data);
  } catch (err) {
    console.error('Fehler bei /api/filialen:', err.message);
    res.status(500).json({ error: 'Datenbankfehler' });
  }
});

// Reklamationsarten
router.get('/reklamationsarten', async (req, res) => {
  try {
    const result = await pool.query('SELECT bezeichnung FROM art_der_reklamation ORDER BY bezeichnung ASC');
    const data = result.rows.map(row => row.bezeichnung);
    console.log('/api/reklamationsarten aufgerufen –', data.length, 'Einträge geladen');
    res.json(data);
  } catch (err) {
    console.error('Fehler bei /api/reklamationsarten:', err.message);
    res.status(500).json({ error: 'Datenbankfehler' });
  }
});

// Lieferanten
router.get('/lieferanten', async (req, res) => {
  try {
    const result = await pool.query('SELECT bezeichnung FROM lieferanten ORDER BY bezeichnung ASC');
    const data = result.rows.map(row => row.bezeichnung);
    console.log('/api/lieferanten aufgerufen –', data.length, 'Einträge geladen');
    res.json(data);
  } catch (err) {
    console.error('Fehler bei /api/lieferanten:', err.message);
    res.status(500).json({ error: 'Datenbankfehler' });
  }
});

// Einheiten
router.get('/einheiten', async (req, res) => {
  try {
    const result = await pool.query('SELECT bezeichnung FROM einheit ORDER BY bezeichnung ASC');
    const data = result.rows.map(row => row.bezeichnung);
    console.log('/api/einheiten aufgerufen –', data.length, 'Einträge geladen');
    res.json(data);
  } catch (err) {
    console.error('Fehler bei /api/einheiten:', err.message);
    res.status(500).json({ error: 'Datenbankfehler' });
  }
});

// Status
router.get('/status', async (req, res) => {
  try {
    const result = await pool.query('SELECT bezeichnung FROM status ORDER BY id ASC');
    const data = result.rows.map(row => row.bezeichnung);
    console.log('/api/status aufgerufen –', data.length, 'Einträge geladen');
    res.json(data);
  } catch (err) {
    console.error('Fehler bei /api/status:', err.message);
    res.status(500).json({ error: 'Datenbankfehler' });
  }
});

module.exports = router;