const express = require('express');
const bcrypt = require('bcrypt');

const router = express.Router();

const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

function isCentralRole(role) {
  return ['Admin', 'Supervisor', 'Geschäftsführer', 'Manager-1'].includes(role);
}

function isFourDigitPin(pin) {
  return typeof pin === 'string' && /^[0-9]{4}$/.test(pin);
}

/**
 * Private: PIN vergeben/ändern
 * POST /api/filiale-pins
 */
router.post('/', verifyToken(), async (req, res) => {
  try {
    const { role, id: actorUserId } = req.user || {};

    if (!isCentralRole(role)) {
      return res.status(403).json({ message: 'Zugriff verweigert (PIN-Vergabe nur Zentrale-Rollen).' });
    }

    const filiale_id = Number(req.body?.filiale_id);
    const display_name = String(req.body?.display_name || '').trim();
    const pin = String(req.body?.pin || '').trim();

    if (!Number.isInteger(filiale_id) || filiale_id <= 0) {
      return res.status(400).json({ message: 'filiale_id muss eine gültige Filial-ID (int) sein.' });
    }
    if (!display_name) {
      return res.status(400).json({ message: 'display_name ist Pflicht.' });
    }
    if (!isFourDigitPin(pin)) {
      return res.status(400).json({ message: 'pin muss exakt 4-stellig numerisch sein.' });
    }

    const fRes = await pool.query('SELECT id FROM public.filialen WHERE id = $1 LIMIT 1', [filiale_id]);
    if (fRes.rows.length === 0) {
      return res.status(404).json({ message: `Filiale mit id=${filiale_id} nicht gefunden.` });
    }

    const pin_hash = await bcrypt.hash(pin, 10);

    const upQ = `
      INSERT INTO core.filiale_pins (
        filiale_id, display_name, pin_hash,
        is_active, failed_attempts, locked_until, last_failed_at, last_used_at
      )
      VALUES ($1,$2,$3,true,0,NULL,NULL,NULL)
      ON CONFLICT (filiale_id, display_name)
      DO UPDATE SET
        pin_hash = EXCLUDED.pin_hash,
        is_active = true,
        failed_attempts = 0,
        locked_until = NULL,
        last_failed_at = NULL,
        last_used_at = NULL
      RETURNING id, filiale_id, display_name, is_active, failed_attempts, locked_until, last_used_at, created_at
    `;
    const upRes = await pool.query(upQ, [filiale_id, display_name, pin_hash]);

    return res.status(201).json({
      message: 'PIN gespeichert.',
      actor_user_id: actorUserId,
      pin: upRes.rows[0],
    });
  } catch (err) {
    console.error('POST /api/filiale-pins Fehler:', err);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

/**
 * Private: PIN-Liste (ohne Hash)
 * GET /api/filiale-pins?filiale_id=4
 */
router.get('/', verifyToken(), async (req, res) => {
  try {
    const { role } = req.user || {};
    if (!isCentralRole(role)) {
      return res.status(403).json({ message: 'Zugriff verweigert.' });
    }

    const filialeId = req.query?.filiale_id != null ? Number(req.query.filiale_id) : null;
    if (filialeId != null && (!Number.isInteger(filialeId) || filialeId <= 0)) {
      return res.status(400).json({ message: 'filiale_id muss eine gültige int sein.' });
    }

    const q =
      filialeId == null
        ? `
        SELECT id, filiale_id, display_name, is_active, failed_attempts, locked_until, last_failed_at, last_used_at, created_at
        FROM core.filiale_pins
        ORDER BY filiale_id, display_name
      `
        : `
        SELECT id, filiale_id, display_name, is_active, failed_attempts, locked_until, last_failed_at, last_used_at, created_at
        FROM core.filiale_pins
        WHERE filiale_id = $1
        ORDER BY display_name
      `;

    const r = filialeId == null ? await pool.query(q) : await pool.query(q, [filialeId]);
    return res.json({ pins: r.rows });
  } catch (err) {
    console.error('GET /api/filiale-pins Fehler:', err);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

module.exports = router;