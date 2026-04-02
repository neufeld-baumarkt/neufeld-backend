const express = require('express');

const router = express.Router();

const verifyToken = require('../middleware/verifyToken');

/**
 * GET /api/bestellungen
 * Zweck:
 * - Erstes Test-Endpoint für Bestellungen
 * - Verifiziert:
 *   - Routing
 *   - Auth
 *   - Backend-Anbindung
 */
router.get('/', verifyToken(), async (req, res) => {
  try {
    const { id, name, role, filiale } = req.user || {};

    return res.json({
      status: 'ok',
      message: 'Bestellungen API erreichbar',
      user: {
        id,
        name,
        role,
        filiale,
      },
      data: [],
    });
  } catch (err) {
    console.error('GET /api/bestellungen Fehler:', err);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

module.exports = router;