// routes/reklamationen.js – STRENGE ROLLENREGELN: Bearbeiten/Löschen NUR Supervisor & Admin
const express = require('express');
const router = express.Router();
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

// Strenge Rollen: Nur diese dürfen bearbeiten oder löschen
const rollenMitBearbeitungsrecht = ['Admin', 'Supervisor'];

// Hilfs-Array nur noch für globale Ansicht (GET)
const rollenMitGlobalzugriff = ['Admin', 'Supervisor', 'Manager-1', 'Geschäftsführer'];

// GET /api/reklamationen – Liste (unverändert: SuperUser sehen alles, Filiale nur eigene)
router.get('/', verifyToken(), async (req, res) => {
  const { role, filiale } = req.user;

  try {
    let query;
    let params = [];

    if (
      rollenMitGlobalzugriff.includes(role) ||
      filiale === 'alle' ||
      !filiale ||
      filiale === ''
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

// GET /:id – Detail (unverändert: gleiche Sichtlogik wie Liste)
router.get('/:id', verifyToken(), async (req, res) => {
  // ... (genau wie vorher – bleibt unverändert)
  // Nur die Ansicht erlaubt je nach Rolle/Filiale
});

// POST – Anlegen: JEDER angemeldete User darf (unverändert, aber mit Kommentar)
router.post('/', verifyToken(), async (req, res) => {
  // Jeder mit Token darf anlegen – Filiale nur in eigener Filiale
  // ... (wie bisher)
});

// PUT – Bearbeiten: NUR Admin oder Supervisor!
router.put('/:id', verifyToken(), async (req, res) => {
  const { id } = req.params;
  const user = req.user;

  // Strenge Prüfung: Nur Admin oder Supervisor
  if (!rollenMitBearbeitungsrecht.includes(user.role)) {
    return res.status(403).json({ 
      message: 'Zugriff verweigert: Nur Supervisor oder Admin dürfen bearbeiten' 
    });
  }

  // Rest wie vorher: Transaktion, Update, Positionen ersetzen...
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
      WHERE id = $10
    `;

    const data = req.body;
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
      id
    ];

    const result = await client.query(updateQuery, updateValues);
    if (result.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ message: 'Reklamation nicht gefunden' });
    }

    await client.query('DELETE FROM reklamation_positionen WHERE reklamation_id = $1', [id]);

    if (data.positionen && data.positionen.length > 0) {
      const posQuery = `INSERT INTO reklamation_positionen (...) VALUES (...)`;
      // ... (wie vorher)
    }

    await client.query('COMMIT');
    console.log(`Reklamation vollständig bearbeitet – ID: ${id} von ${user.name} (${user.role})`);
    res.json({ message: 'Reklamation erfolgreich aktualisiert' });
  } catch (err) {
    // ... Fehlerhandling
  } finally {
    client.release();
  }
});

// PATCH – Teilupdate: AUCH NUR Admin oder Supervisor!
router.patch('/:id', verifyToken(), async (req, res) => {
  const { id } = req.params;
  const user = req.user;

  if (!rollenMitBearbeitungsrecht.includes(user.role)) {
    return res.status(403).json({ 
      message: 'Zugriff verweigert: Nur Supervisor oder Admin dürfen Änderungen vornehmen' 
    });
  }

  // Rest genau wie vorher...
});

// DELETE – Löschen: NUR Admin oder Supervisor
router.delete('/:id', verifyToken(), async (req, res) => {
  const { id } = req.params;
  const user = req.user;

  if (!rollenMitBearbeitungsrecht.includes(user.role)) {
    return res.status(403).json({ 
      message: 'Zugriff verweigert: Nur Supervisor oder Admin dürfen löschen' 
    });
  }

  // Rest wie vorher...
});

module.exports = router;