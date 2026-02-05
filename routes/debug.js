// routes/debug.js – Admin-only Debug-Endpoints (DB-Metadaten, Viewdefs)
// Stand: 29.01.2026
//
// Zweck:
// - Admin kann (bei aktivierter DEBUG_DB_METADATA-Flag) definierte View-Definitionen auslesen,
//   um unterwegs ohne pgAdmin die DB-Views verifizieren zu können.
// - Sicherheitsprinzipien:
//   - NUR Admin (verifyToken('Admin')).
//   - Whitelist: keine freien Viewnamen, nur feste Keys.
//   - Feature-Flag: DEBUG_DB_METADATA muss explizit "true" sein, sonst 404.

const express = require('express');
const router = express.Router();

const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

const ROLE_ADMIN = 'Admin';

// Whitelist: Key -> voll qualifizierter Viewname
const VIEW_WHITELIST = Object.freeze({
  global: 'budget.v_week_summary_global',
  ytd: 'budget.v_week_summary_global_ytd',
});

function isDebugEnabled() {
  return String(process.env.DEBUG_DB_METADATA || '').toLowerCase() === 'true';
}

// GET /api/debug/viewdef/:key
router.get('/viewdef/:key', verifyToken(ROLE_ADMIN), async (req, res) => {
  // Feature-Flag: im Normalbetrieb soll es diesen Endpoint praktisch nicht geben
  if (!isDebugEnabled()) {
    return res.status(404).json({ message: 'Not found' });
  }

  const key = String(req.params?.key || '').trim().toLowerCase();
  const viewName = VIEW_WHITELIST[key];

  if (!viewName) {
    return res.status(400).json({
      message: 'Ungültiger key. Erlaubt sind nur: ' + Object.keys(VIEW_WHITELIST).join(', ')
    });
  }

  try {
    // pg_get_viewdef akzeptiert regclass – wir casten den parametrierten Namen explizit
    const r = await pool.query(
      `SELECT pg_get_viewdef($1::regclass, true) AS definition`,
      [viewName]
    );

    const definition = r.rows?.[0]?.definition || null;

    if (!definition) {
      return res.status(404).json({ message: 'Viewdef nicht gefunden.' });
    }

    return res.json({
      key,
      view: viewName,
      definition,
    });
  } catch (e) {
    // 42P01: undefined_table (gilt auch für regclass lookup)
    if (e && e.code === '42P01') {
      return res.status(404).json({ message: 'View nicht gefunden.' });
    }

    console.error('Fehler GET /api/debug/viewdef:', e.message);
    return res.status(500).json({ message: 'Serverfehler (Debug viewdef).' });
  }
});

module.exports = router;
