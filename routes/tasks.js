const express = require('express');
const bcrypt = require('bcrypt');

const router = express.Router();

const pool = require('../db');
const verifyToken = require('../middleware/verifyToken');

// --- Helpers ---
function isCentralRole(role) {
  return ['Admin', 'Supervisor', 'Geschäftsführer', 'Manager-1'].includes(role);
}

function isFourDigitPin(pin) {
  return typeof pin === 'string' && /^[0-9]{4}$/.test(pin);
}

// --- Lockout Policy ---
const PIN_MAX_FAILS = 5;
const PIN_LOCK_MINUTES = 15;

/**
 * Private: Tasks (Read-only) – STEP 2.1
 * GET /api/tasks
 * - Filiale: nur eigene Tasks (owner=me)
 * - Andere Rollen: aktuell 403 (bewusst minimal)
 */
router.get('/', verifyToken(), async (req, res) => {
  try {
    const { role, filiale } = req.user || {};

    if (role !== 'Filiale') {
      return res.status(403).json({ message: 'Zugriff verweigert (nur Filiale in STEP 2.1).' });
    }

    if (!filiale) {
      return res.status(400).json({ message: 'Filiale im Token fehlt. Bitte erneut anmelden.' });
    }

    const fRes = await pool.query(
      'SELECT id, name FROM public.filialen WHERE name = $1 LIMIT 1',
      [filiale]
    );

    if (fRes.rows.length === 0) {
      return res.status(404).json({ message: `Filiale '${filiale}' nicht in public.filialen gefunden.` });
    }

    const filialeId = fRes.rows[0].id;

    const q = `
      SELECT
        t.id,
        t.owner_type,
        t.owner_id,
        t.title,
        t.body,
        t.status,
        t.created_by_user_id,
        t.created_at,
        t.updated_at,
        t.ack_at,
        t.admin_closed_at,
        t.admin_closed_by_user_id,
        t.admin_note,
        t.executed_at,
        t.executed_by_user_id,
        t.due_at,
        t.source_type,
        t.source_id,
        le.event_type AS last_event_type,
        le.event_at   AS last_event_at
      FROM core.tasks t
      LEFT JOIN LATERAL (
        SELECT event_type, event_at
        FROM core.task_events
        WHERE task_id = t.id
        ORDER BY event_at DESC
        LIMIT 1
      ) le ON true
      WHERE t.owner_type = 'filiale'
        AND t.owner_id = $1
        AND t.status = ANY($2::text[])
      ORDER BY t.created_at DESC
      LIMIT 200
    `;

    const statuses = ['open', 'ack', 'admin_closed', 'executed', 'canceled'];
    const tRes = await pool.query(q, [filialeId, statuses]);

    return res.json({
      owner: { owner_type: 'filiale', owner_id: filialeId, filiale_name: filiale },
      tasks: tRes.rows,
    });
  } catch (err) {
    console.error('GET /api/tasks Fehler:', err);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

/**
 * Private: Tasks Create – STEP 2.2
 * POST /api/tasks
 */
router.post('/', verifyToken(), async (req, res) => {
  try {
    const { id: actorUserId, role } = req.user || {};

    if (!isCentralRole(role)) {
      return res.status(403).json({ message: 'Zugriff verweigert (Task erstellen nur Zentrale-Rollen).' });
    }

    const owner_type = String(req.body?.owner_type || '').trim();
    const owner_id_raw = req.body?.owner_id;
    const title = String(req.body?.title || '').trim();
    const body = String(req.body?.body || '').trim();

    const due_at_raw = req.body?.due_at ?? null;
    const source_type = req.body?.source_type != null ? String(req.body.source_type).trim() : null;
    const source_id = req.body?.source_id != null ? String(req.body.source_id).trim() : null;

    if (owner_type !== 'filiale') {
      return res.status(400).json({ message: "owner_type muss 'filiale' sein (Startphase)." });
    }

    const owner_id = Number(owner_id_raw);
    if (!Number.isInteger(owner_id) || owner_id <= 0) {
      return res.status(400).json({ message: 'owner_id muss eine gültige Filial-ID (int) sein.' });
    }

    if (!title) return res.status(400).json({ message: 'title ist Pflicht.' });
    if (!body) return res.status(400).json({ message: 'body ist Pflicht.' });

    let due_at = null;
    if (due_at_raw !== null && due_at_raw !== '') {
      const d = new Date(due_at_raw);
      if (Number.isNaN(d.getTime())) {
        return res.status(400).json({ message: 'due_at ist kein gültiges Datum.' });
      }
      due_at = d.toISOString();
    }

    const fRes = await pool.query('SELECT id FROM public.filialen WHERE id = $1 LIMIT 1', [owner_id]);
    if (fRes.rows.length === 0) {
      return res.status(404).json({ message: `Filiale mit id=${owner_id} nicht gefunden.` });
    }

    await pool.query('BEGIN');

    const insertTaskQ = `
      INSERT INTO core.tasks (
        owner_type, owner_id, title, body, status, created_by_user_id,
        due_at, source_type, source_id
      )
      VALUES ($1,$2,$3,$4,'open',$5,$6,$7,$8)
      RETURNING
        id, owner_type, owner_id, title, body, status,
        created_by_user_id, created_at, updated_at,
        ack_at, admin_closed_at, admin_closed_by_user_id, admin_note,
        executed_at, executed_by_user_id,
        due_at, source_type, source_id
    `;
    const tIns = await pool.query(insertTaskQ, [
      owner_type,
      owner_id,
      title,
      body,
      actorUserId,
      due_at,
      source_type,
      source_id,
    ]);

    const task = tIns.rows[0];

    const insertEventQ = `
      INSERT INTO core.task_events (task_id, event_type, actor_user_id, meta)
      VALUES ($1,'created',$2,$3)
      RETURNING event_type, event_at
    `;
    const meta = { source: 'api', role };
    const eIns = await pool.query(insertEventQ, [task.id, actorUserId, meta]);

    await pool.query('COMMIT');

    return res.status(201).json({
      task: {
        ...task,
        last_event_type: eIns.rows[0]?.event_type || 'created',
        last_event_at: eIns.rows[0]?.event_at || null,
      },
    });
  } catch (err) {
    try {
      await pool.query('ROLLBACK');
    } catch (_) {}
    console.error('POST /api/tasks Fehler:', err);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

/**
 * Private: Task Ack (ohne PIN) – STEP 2.3
 * POST /api/tasks/:id/ack
 */
router.post('/:id/ack', verifyToken(), async (req, res) => {
  const taskId = String(req.params?.id || '').trim();

  try {
    const { role, filiale, id: actorUserId } = req.user || {};

    if (role !== 'Filiale') {
      return res.status(403).json({ message: 'Zugriff verweigert (ack nur Filiale).' });
    }
    if (!filiale) {
      return res.status(400).json({ message: 'Filiale im Token fehlt. Bitte erneut anmelden.' });
    }
    if (!taskId) {
      return res.status(400).json({ message: 'Task-ID fehlt.' });
    }

    const fRes = await pool.query('SELECT id FROM public.filialen WHERE name = $1 LIMIT 1', [filiale]);
    if (fRes.rows.length === 0) {
      return res.status(404).json({ message: `Filiale '${filiale}' nicht in public.filialen gefunden.` });
    }
    const filialeId = fRes.rows[0].id;

    await pool.query('BEGIN');

    const updQ = `
      UPDATE core.tasks
      SET status = 'ack',
          ack_at = now(),
          updated_at = now()
      WHERE id = $1
        AND owner_type = 'filiale'
        AND owner_id = $2
        AND status = 'open'
      RETURNING
        id, owner_type, owner_id, title, body, status,
        created_by_user_id, created_at, updated_at,
        ack_at, admin_closed_at, admin_closed_by_user_id, admin_note,
        executed_at, executed_by_user_id,
        due_at, source_type, source_id
    `;
    const updRes = await pool.query(updQ, [taskId, filialeId]);

    if (updRes.rows.length === 1) {
      const task = updRes.rows[0];

      const evQ = `
        INSERT INTO core.task_events (task_id, event_type, actor_user_id, meta)
        VALUES ($1,'ack',$2,$3)
        RETURNING event_type, event_at
      `;
      const meta = { source: 'api', owner_type: 'filiale', owner_id: filialeId };
      const evRes = await pool.query(evQ, [task.id, actorUserId, meta]);

      await pool.query('COMMIT');

      return res.json({
        task: {
          ...task,
          last_event_type: evRes.rows[0]?.event_type || 'ack',
          last_event_at: evRes.rows[0]?.event_at || null,
        },
      });
    }

    await pool.query('ROLLBACK');

    const getQ = `
      SELECT
        t.*,
        le.event_type AS last_event_type,
        le.event_at   AS last_event_at
      FROM core.tasks t
      LEFT JOIN LATERAL (
        SELECT event_type, event_at
        FROM core.task_events
        WHERE task_id = t.id
        ORDER BY event_at DESC
        LIMIT 1
      ) le ON true
      WHERE t.id = $1
        AND t.owner_type = 'filiale'
        AND t.owner_id = $2
      LIMIT 1
    `;
    const getRes = await pool.query(getQ, [taskId, filialeId]);

    if (getRes.rows.length === 0) {
      return res.status(404).json({ message: 'Task nicht gefunden.' });
    }

    return res.json({ task: getRes.rows[0] });
  } catch (err) {
    try {
      await pool.query('ROLLBACK');
    } catch (_) {}
    console.error('POST /api/tasks/:id/ack Fehler:', err);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

/**
 * Private: Task Execute (mit PIN) – STEP 2.4
 * POST /api/tasks/:id/execute
 * Body: { display_name: "Julien", pin: "4831" }
 */
router.post('/:id/execute', verifyToken(), async (req, res) => {
  const taskId = String(req.params?.id || '').trim();

  try {
    const { role, filiale, id: actorUserId } = req.user || {};

    if (role !== 'Filiale') {
      return res.status(403).json({ message: 'Zugriff verweigert (execute nur Filiale).' });
    }
    if (!filiale) {
      return res.status(400).json({ message: 'Filiale im Token fehlt. Bitte erneut anmelden.' });
    }
    if (!taskId) {
      return res.status(400).json({ message: 'Task-ID fehlt.' });
    }

    const display_name = String(req.body?.display_name || '').trim();
    const pin = String(req.body?.pin || '').trim();

    if (!display_name) return res.status(400).json({ message: 'display_name ist Pflicht.' });
    if (!isFourDigitPin(pin)) return res.status(400).json({ message: 'pin muss exakt 4-stellig numerisch sein.' });

    const fRes = await pool.query('SELECT id FROM public.filialen WHERE name = $1 LIMIT 1', [filiale]);
    if (fRes.rows.length === 0) {
      return res.status(404).json({ message: `Filiale '${filiale}' nicht in public.filialen gefunden.` });
    }
    const filialeId = fRes.rows[0].id;

    const t0 = await pool.query(
      `SELECT id, status, owner_id
       FROM core.tasks
       WHERE id = $1 AND owner_type='filiale' AND owner_id = $2
       LIMIT 1`,
      [taskId, filialeId]
    );
    if (t0.rows.length === 0) {
      return res.status(404).json({ message: 'Task nicht gefunden.' });
    }

    const currentStatus = t0.rows[0].status;

    if (['executed', 'admin_closed', 'canceled'].includes(currentStatus)) {
      const getQ = `
        SELECT
          t.*,
          le.event_type AS last_event_type,
          le.event_at   AS last_event_at
        FROM core.tasks t
        LEFT JOIN LATERAL (
          SELECT event_type, event_at
          FROM core.task_events
          WHERE task_id = t.id
          ORDER BY event_at DESC
          LIMIT 1
        ) le ON true
        WHERE t.id = $1
          AND t.owner_type = 'filiale'
          AND t.owner_id = $2
        LIMIT 1
      `;
      const getRes = await pool.query(getQ, [taskId, filialeId]);
      return res.json({ task: getRes.rows[0] });
    }

    const pRes = await pool.query(
      `SELECT id, pin_hash, is_active, failed_attempts, locked_until
       FROM core.filiale_pins
       WHERE filiale_id = $1 AND display_name = $2
       LIMIT 1`,
      [filialeId, display_name]
    );

    if (pRes.rows.length === 0) {
      await pool.query(
        `INSERT INTO core.task_events (task_id, event_type, actor_user_id, meta)
         VALUES ($1,'pin_failed',$2,$3)`,
        [taskId, actorUserId, { display_name, reason: 'not_found_or_mismatch' }]
      );
      return res.status(401).json({ message: 'PIN ungültig.' });
    }

    const pinRow = pRes.rows[0];

    if (!pinRow.is_active) {
      return res.status(403).json({ message: 'PIN deaktiviert.' });
    }

    if (pinRow.locked_until && new Date(pinRow.locked_until).getTime() > Date.now()) {
      return res.status(423).json({ message: 'PIN gesperrt. Bitte später erneut versuchen.' });
    }

    const ok = await bcrypt.compare(pin, pinRow.pin_hash);

    if (!ok) {
      const nextFails = Number(pinRow.failed_attempts || 0) + 1;
      const lock =
        nextFails >= PIN_MAX_FAILS
          ? new Date(Date.now() + PIN_LOCK_MINUTES * 60 * 1000).toISOString()
          : null;

      await pool.query(
        `UPDATE core.filiale_pins
         SET failed_attempts = $1,
             last_failed_at = now(),
             locked_until = $2
         WHERE id = $3`,
        [nextFails, lock, pinRow.id]
      );

      await pool.query(
        `INSERT INTO core.task_events (task_id, event_type, actor_user_id, meta)
         VALUES ($1,'pin_failed',$2,$3)`,
        [taskId, actorUserId, { display_name, failed_attempts: nextFails, locked_until: lock }]
      );

      return res.status(401).json({ message: 'PIN ungültig.' });
    }

    await pool.query('BEGIN');

    await pool.query(
      `UPDATE core.filiale_pins
       SET failed_attempts = 0,
           locked_until = NULL,
           last_failed_at = NULL,
           last_used_at = now()
       WHERE id = $1`,
      [pinRow.id]
    );

    const updQ = `
      UPDATE core.tasks
      SET status = 'executed',
          executed_at = now(),
          executed_by_user_id = $3,
          updated_at = now()
      WHERE id = $1
        AND owner_type = 'filiale'
        AND owner_id = $2
        AND status <> 'executed'
      RETURNING
        id, owner_type, owner_id, title, body, status,
        created_by_user_id, created_at, updated_at,
        ack_at, admin_closed_at, admin_closed_by_user_id, admin_note,
        executed_at, executed_by_user_id,
        due_at, source_type, source_id
    `;
    const updRes = await pool.query(updQ, [taskId, filialeId, actorUserId]);

    if (updRes.rows.length === 0) {
      await pool.query('ROLLBACK');

      const getRes = await pool.query(
        `SELECT t.*,
                le.event_type AS last_event_type,
                le.event_at AS last_event_at
         FROM core.tasks t
         LEFT JOIN LATERAL (
           SELECT event_type, event_at
           FROM core.task_events
           WHERE task_id = t.id
           ORDER BY event_at DESC
           LIMIT 1
         ) le ON true
         WHERE t.id=$1 AND t.owner_type='filiale' AND t.owner_id=$2
         LIMIT 1`,
        [taskId, filialeId]
      );

      return res.json({ task: getRes.rows[0] });
    }

    const task = updRes.rows[0];

    const evRes = await pool.query(
      `INSERT INTO core.task_events (task_id, event_type, actor_user_id, meta)
       VALUES ($1,'executed',$2,$3)
       RETURNING event_type, event_at`,
      [task.id, actorUserId, { display_name }]
    );

    await pool.query('COMMIT');

    return res.json({
      task: {
        ...task,
        last_event_type: evRes.rows[0]?.event_type || 'executed',
        last_event_at: evRes.rows[0]?.event_at || null,
      },
    });
  } catch (err) {
    try {
      await pool.query('ROLLBACK');
    } catch (_) {}
    console.error('POST /api/tasks/:id/execute Fehler:', err);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

/**
 * Private: Admin Close (Zentrale) – STEP 2.5
 * POST /api/tasks/:id/admin-close
 * Body: { note: "..." }
 */
router.post('/:id/admin-close', verifyToken(), async (req, res) => {
  const taskId = String(req.params?.id || '').trim();

  try {
    const { role, id: actorUserId } = req.user || {};

    if (!isCentralRole(role)) {
      return res.status(403).json({ message: 'Zugriff verweigert (admin-close nur Zentrale-Rollen).' });
    }
    if (!taskId) {
      return res.status(400).json({ message: 'Task-ID fehlt.' });
    }

    const note = String(req.body?.note || '').trim();
    if (!note) {
      return res.status(400).json({ message: 'note ist Pflicht.' });
    }

    const t0 = await pool.query(
      `SELECT id, status
       FROM core.tasks
       WHERE id = $1
       LIMIT 1`,
      [taskId]
    );

    if (t0.rows.length === 0) {
      return res.status(404).json({ message: 'Task nicht gefunden.' });
    }

    if (t0.rows[0].status === 'admin_closed') {
      const getRes = await pool.query(
        `SELECT
           t.*,
           le.event_type AS last_event_type,
           le.event_at   AS last_event_at
         FROM core.tasks t
         LEFT JOIN LATERAL (
           SELECT event_type, event_at
           FROM core.task_events
           WHERE task_id = t.id
           ORDER BY event_at DESC
           LIMIT 1
         ) le ON true
         WHERE t.id = $1
         LIMIT 1`,
        [taskId]
      );
      return res.json({ task: getRes.rows[0] });
    }

    await pool.query('BEGIN');

    const updQ = `
      UPDATE core.tasks
      SET status = 'admin_closed',
          admin_closed_at = now(),
          admin_closed_by_user_id = $2,
          admin_note = $3,
          updated_at = now()
      WHERE id = $1
        AND status <> 'admin_closed'
      RETURNING
        id, owner_type, owner_id, title, body, status,
        created_by_user_id, created_at, updated_at,
        ack_at, admin_closed_at, admin_closed_by_user_id, admin_note,
        executed_at, executed_by_user_id,
        due_at, source_type, source_id
    `;
    const updRes = await pool.query(updQ, [taskId, actorUserId, note]);

    if (updRes.rows.length === 0) {
      await pool.query('ROLLBACK');

      const getRes = await pool.query(
        `SELECT
           t.*,
           le.event_type AS last_event_type,
           le.event_at   AS last_event_at
         FROM core.tasks t
         LEFT JOIN LATERAL (
           SELECT event_type, event_at
           FROM core.task_events
           WHERE task_id = t.id
           ORDER BY event_at DESC
           LIMIT 1
         ) le ON true
         WHERE t.id = $1
         LIMIT 1`,
        [taskId]
      );

      if (getRes.rows.length === 0) {
        return res.status(404).json({ message: 'Task nicht gefunden.' });
      }
      return res.json({ task: getRes.rows[0] });
    }

    const task = updRes.rows[0];

    const evRes = await pool.query(
      `INSERT INTO core.task_events (task_id, event_type, actor_user_id, meta)
       VALUES ($1,'admin_closed',$2,$3)
       RETURNING event_type, event_at`,
      [task.id, actorUserId, { source: 'api', role, note }]
    );

    await pool.query('COMMIT');

    return res.json({
      task: {
        ...task,
        last_event_type: evRes.rows[0]?.event_type || 'admin_closed',
        last_event_at: evRes.rows[0]?.event_at || null,
      },
    });
  } catch (err) {
    try {
      await pool.query('ROLLBACK');
    } catch (_) {}
    console.error('POST /api/tasks/:id/admin-close Fehler:', err);
    return res.status(500).json({ message: 'Serverfehler' });
  }
});

module.exports = router;