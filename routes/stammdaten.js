// routes/stammdaten.js – ALLE ROUTEN WIEDER X GESCHÜTZT!
const express = require('express');
const router = express.Router();
const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

// ✅ AUTH AKTIVIERT: Alle Stammdaten erfordern gültiges Token (keine spezielle Rolle nötig)
router.use(verifyToken()); // ← DAS WAR DER FEHLER! Jetzt sicher!

// Helper: Nur Admin + Supervisor dürfen Stammdaten verwalten
function isStammdatenAdmin(req) {
  const role = String(req?.user?.role || '');
  return role === 'Admin' || role === 'Supervisor';
}

function requireStammdatenAdmin(req, res) {
  if (!isStammdatenAdmin(req)) {
    res.status(403).json({ error: 'Keine Berechtigung (nur Admin/Supervisor).' });
    return false;
  }
  return true;
}

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

// Lieferanten (Dropdown) – NUR aktive als Strings
router.get('/lieferanten', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT bezeichnung
      FROM lieferanten
      WHERE aktiv = true
      ORDER BY bezeichnung ASC
    `);
    const data = result.rows.map(row => row.bezeichnung);
    console.log(`📋 /api/lieferanten – ${req.user.name}: ${data.length} aktive Lieferanten`);
    res.json(data);
  } catch (err) {
    console.error('Fehler /api/lieferanten:', err.message);
    res.status(500).json({ error: 'Datenbankfehler bei Lieferanten' });
  }
});

// Lieferanten (Verwaltung) – Admin/Supervisor: id + bezeichnung + aktiv
router.get('/lieferanten/manage', async (req, res) => {
  if (!requireStammdatenAdmin(req, res)) return;

  try {
    const result = await pool.query(`
      SELECT id, bezeichnung, aktiv
      FROM lieferanten
      ORDER BY bezeichnung ASC
    `);
    console.log(`🛠️ /api/lieferanten/manage – ${req.user.name} (${req.user.role}): ${result.rows.length} Lieferanten`);
    res.json(result.rows);
  } catch (err) {
    console.error('Fehler /api/lieferanten/manage:', err.message);
    res.status(500).json({ error: 'Datenbankfehler bei Lieferanten (Manage)' });
  }
});

// Lieferant anlegen – Admin/Supervisor
router.post('/lieferanten', async (req, res) => {
  if (!requireStammdatenAdmin(req, res)) return;

  try {
    // Frontend soll "bezeichnung" senden; "name" akzeptieren wir als Fallback, damit nichts bricht.
    const raw = (req.body?.bezeichnung ?? req.body?.name ?? '');
    const bezeichnung = String(raw).trim();

    if (!bezeichnung) {
      return res.status(400).json({ error: 'Pflichtfeld fehlt: bezeichnung' });
    }

    const result = await pool.query(
      `
        INSERT INTO lieferanten (bezeichnung, aktiv)
        VALUES ($1, true)
        RETURNING id, bezeichnung, aktiv
      `,
      [bezeichnung]
    );

    console.log(`✅ Lieferant angelegt – ${req.user.name} (${req.user.role}): "${bezeichnung}"`);
    return res.status(201).json(result.rows[0]);
  } catch (err) {
    // Unique-Index auf lower(trim(bezeichnung)) -> 23505 bei Duplikat
    if (err.code === '23505') {
      return res.status(409).json({ error: 'Lieferant existiert bereits (Name ist eindeutig).' });
    }
    console.error('Fehler POST /api/lieferanten:', err.message);
    return res.status(500).json({ error: 'Datenbankfehler beim Anlegen des Lieferanten' });
  }
});

// Lieferant ändern (Name und/oder aktiv) – Admin/Supervisor
router.patch('/lieferanten/:id', async (req, res) => {
  if (!requireStammdatenAdmin(req, res)) return;

  try {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ error: 'Ungültige ID' });
    }

    const updates = [];
    const values = [];
    let idx = 1;

    // bezeichnung optional
    if (req.body?.bezeichnung !== undefined || req.body?.name !== undefined) {
      const raw = (req.body?.bezeichnung ?? req.body?.name ?? '');
      const bezeichnung = String(raw).trim();
      if (!bezeichnung) {
        return res.status(400).json({ error: 'bezeichnung darf nicht leer sein' });
      }
      updates.push(`bezeichnung = $${idx++}`);
      values.push(bezeichnung);
    }

    // aktiv optional
    if (req.body?.aktiv !== undefined) {
      const aktiv = req.body.aktiv;
      if (typeof aktiv !== 'boolean') {
        return res.status(400).json({ error: 'aktiv muss boolean sein (true/false)' });
      }
      updates.push(`aktiv = $${idx++}`);
      values.push(aktiv);
    }

    if (updates.length === 0) {
      return res.status(400).json({ error: 'Keine Änderungen übergeben (bezeichnung und/oder aktiv).' });
    }

    values.push(id);

    const result = await pool.query(
      `
        UPDATE lieferanten
        SET ${updates.join(', ')}
        WHERE id = $${idx}
        RETURNING id, bezeichnung, aktiv
      `,
      values
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Lieferant nicht gefunden' });
    }

    console.log(`🛠️ Lieferant geändert – ${req.user.name} (${req.user.role}): id=${id}`);
    return res.json(result.rows[0]);
  } catch (err) {
    if (err.code === '23505') {
      return res.status(409).json({ error: 'Lieferant existiert bereits (Name ist eindeutig).' });
    }
    console.error('Fehler PATCH /api/lieferanten/:id:', err.message);
    return res.status(500).json({ error: 'Datenbankfehler beim Aktualisieren des Lieferanten' });
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
