const express = require('express');

const router = express.Router();

const verifyToken = require('../middleware/verifyToken');

/**
 * GET /api/bestellungen
 * Zweck:
 * - Erster echter Read-Endpunkt für das Bestellmodul
 * - Liefert eine stabile Grundstruktur für das Frontend
 * - Noch bewusst ohne Datenbankzugriff
 */
router.get('/', verifyToken(), async (req, res) => {
  try {
    const { id, name, role, filiale } = req.user || {};

    return res.json({
      status: 'ok',
      module: 'bestellungen',
      message: 'Bestellungen API erreichbar',
      stage: 'phase-2-read-base',
      user: {
        id: id ?? null,
        name: name ?? null,
        role: role ?? null,
        filiale: filiale ?? null,
      },
      permissions: {
        authenticated: true,
        canRead: true,
        canWrite: false,
      },
      filters: {
        jahr: null,
        kw: null,
        filiale: filiale ?? null,
      },
      form: {
        kopf: {
          bestellung_id: null,
          filiale: filiale ?? null,
          jahr: null,
          kw: null,
          lieferant: null,
          bestelldatum: null,
          bemerkung: null,
          status: 'neu',
        },
        positionen: [],
      },
      meta: {
        dbConnected: false,
        source: 'static-read-base',
        nextStep: 'read-from-db',
      },
    });
  } catch (err) {
    console.error('GET /api/bestellungen Fehler:', err);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

module.exports = router;