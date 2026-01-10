// routes/stammdaten.js – ALLE ROUTEN WIEDER GESCHÜTZT!
const express = require('express');
const router = express.Router();
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

// ✅ AUTH AKTIVIERT: Alle Stammdaten erfordern gültiges Token (keine spezielle Rolle nötig)
router.use(verifyToken());  // ← DAS WAR DER FEHLER! Jetzt sicher!

// Filialen – nur aktive Namen als Strings
router.get('/filialen', async (req, res) => {
  try {
    const result = await pool.query('SELECT name FROM filialen WHERE aktiv = true ORDER BY name ASC');
    const data = result.rows.map(row => row.name);
    console.log(`📋 /api/filialen – ${req.user.name} (${req.user.role}): ${data.length} aktive Filialen`);
    res.json(data);
  } catch (err) {
    console.error('Fehler /api/filialen:', err.message);
    res.status(500).json({ error: 'Datenbankfehler bei Filialen' });
  }
});

// Reklamationsarten
router.get('/reklamationsarten', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT bezeichnung
      FROM art_der_reklamation
      ORDER BY
        CASE WHEN bezeichnung = 'Kundenreklamation MDE' THEN 0 ELSE 1 END,
        bezeichnung ASC
    `);
    const data = result.rows.map(row => row.bezeichnung);
    console.log(`📋 /api/reklamationsarten – ${req.user.name}: ${data.length} Arten`);
    res.json(data);
  } catch (err) {
    console.error('Fehler /api/reklamationsarten:', err.message);
    res.status(500).json({ error: 'Datenbankfehler bei Reklamationsarten' });
  }
});

// Lieferanten
router.get('/lieferanten', async (req, res) => {
  try {
    const result = await pool.query('SELECT bezeichnung FROM lieferanten ORDER BY bezeichnung ASC');
    const data = result.rows.map(row => row.bezeichnung);
    console.log(`📋 /api/lieferanten – ${req.user.name}: ${data.length} Lieferanten`);
    res.json(data);
  } catch (err) {
    console.error('Fehler /api/lieferanten:', err.message);
    res.status(500).json({ error: 'Datenbankfehler bei Lieferanten' });
  }
});

// Einheiten
router.get('/einheiten', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT bezeichnung
      FROM einheit
      ORDER BY
        CASE WHEN bezeichnung = 'Stück' THEN 0 ELSE 1 END,
        bezeichnung ASC
    `);
    const data = result.rows.map(row => row.bezeichnung);
    console.log(`📋 /api/einheiten – ${req.user.name}: ${data.length} Einheiten`);
    res.json(data);
  } catch (err) {
    console.error('Fehler /api/einheiten:', err.message);
    res.status(500).json({ error: 'Datenbankfehler bei Einheiten' });
  }
});

// Status
router.get('/status', async (req, res) => {
  try {
    const result = await pool.query('SELECT bezeichnung FROM status ORDER BY id ASC');
    const data = result.rows.map(row => row.bezeichnung);
    console.log(`📋 /api/status – ${req.user.name}: ${data.length} Status`);
    res.json(data);
  } catch (err) {
    console.error('Fehler /api/status:', err.message);
    res.status(500).json({ error: 'Datenbankfehler bei Status' });
  }
});

module.exports = router;
