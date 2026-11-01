const express = require('express');
const verifyToken = require('../middleware/verifyToken');
const {
  countActiveCashProcesses, publicRuntimeStatus, readRuntimeControl,
} = require('../lib/maintenanceControl');

function problem(message, statusCode = 400) {
  return Object.assign(new Error(message), { statusCode });
}

async function transaction(pool, work) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await work(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    try { await client.query('ROLLBACK'); } catch { /* preserve original error */ }
    throw error;
  } finally { client.release(); }
}

async function lockedControl(client) {
  const result = await client.query(`SELECT * FROM core.app_runtime_control
    WHERE control_key='global' FOR UPDATE`);
  if (!result.rows[0]) throw problem('Globaler Betriebsstatus fehlt.', 500);
  return result.rows[0];
}

async function event(client, previous, next, user, details = {}) {
  await client.query(`INSERT INTO core.app_runtime_control_events
    (control_key,previous_mode,next_mode,actor_user_id,actor_name,details)
    VALUES ('global',$1,$2,$3,$4,$5::jsonb)`,
  [previous, next, user.id, user.name, JSON.stringify(details)]);
}

function createMaintenanceRouter({ pool }) {
  if (!pool) throw new TypeError('Maintenance router requires pool.');
  const router = express.Router();
  router.use((_req, res, next) => { res.setHeader('Cache-Control', 'no-store'); next(); });

  router.get('/status', async (_req, res) => {
    try {
      const [control, processes] = await Promise.all([
        readRuntimeControl(pool), countActiveCashProcesses(pool),
      ]);
      return res.json(publicRuntimeStatus(control, processes));
    } catch (error) {
      console.error('[MAINTENANCE_STATUS_ERROR]', error?.code || error?.message);
      return res.status(503).json({ code: 'RUNTIME_CONTROL_UNAVAILABLE', message: 'Betriebsstatus nicht verfügbar.' });
    }
  });

  router.get('/readiness', verifyToken({ requiredRole: 'Admin', allowMaintenanceAdmin: true }), async (_req, res) => {
    try {
      const [control, processes, details] = await Promise.all([
        readRuntimeControl(pool), countActiveCashProcesses(pool),
        pool.query(`SELECT * FROM (
            SELECT 'draft' AS process_kind,d.workflow_type,d.business_date,
              f.name AS branch,NULL::smallint AS drawer_number,d.updated_at
            FROM controlling.cash_workflow_drafts d
            JOIN public.filialen f ON f.id=d.filiale_id
            WHERE d.status='active'
            UNION ALL
            SELECT 'open_drawer' AS process_kind,'drawer_opening' AS workflow_type,
              o.business_date,f.name AS branch,o.drawer_number,o.completed_at AS updated_at
            FROM core.cash_drawer_opening_counts o
            JOIN public.filialen f ON f.id=o.branch_id
            WHERE o.status='completed' AND o.lifecycle_status='active' AND NOT EXISTS (
              SELECT 1 FROM core.cash_drawer_closings c
              WHERE c.opening_count_id=o.id
            )
          ) processes ORDER BY updated_at DESC LIMIT 50`),
      ]);
      return res.json({ ...publicRuntimeStatus(control, processes), processes: details.rows });
    } catch (error) { return res.status(500).json({ message: 'Wartungsbereitschaft konnte nicht geprüft werden.' }); }
  });

  router.post('/drain', verifyToken({ requiredRole: 'Admin', allowMaintenanceAdmin: true }), async (req, res) => {
    try {
      const message = String(req.body?.message || 'Wartung wird vorbereitet. Bitte laufenden Vorgang abschließen.').trim().slice(0, 500);
      const updated = await transaction(pool, async (client) => {
        const current = await lockedControl(client);
        if (current.mode === 'maintenance') throw problem('Wartungsmodus ist bereits aktiv.', 409);
        if (current.mode === 'draining') return current;
        const result = await client.query(`UPDATE core.app_runtime_control SET
          mode='draining',message=$1,requested_by_user_id=$2,drain_started_at=NOW(),
          maintenance_started_at=NULL,revision=revision+1,updated_at=NOW()
          WHERE control_key='global' RETURNING *`, [message, req.user.id]);
        await event(client, 'open', 'draining', req.user, { message });
        return result.rows[0];
      });
      const processes = await countActiveCashProcesses(pool);
      return res.json(publicRuntimeStatus(updated, processes));
    } catch (error) { return res.status(Number(error.statusCode) || 500).json({ message: error.statusCode ? error.message : 'Wartung konnte nicht vorbereitet werden.' }); }
  });

  router.post('/activate', verifyToken({ requiredRole: 'Admin', allowMaintenanceAdmin: true }), async (req, res) => {
    try {
      const updated = await transaction(pool, async (client) => {
        const current = await lockedControl(client);
        if (current.mode !== 'draining') throw problem('Zuerst muss der Zulauf gesperrt werden.', 409);
        const processes = await countActiveCashProcesses(client);
        if (processes.activeCount > 0) throw problem(`Noch ${processes.activeCount} Kassenprozess(e) aktiv.`, 409);
        const result = await client.query(`UPDATE core.app_runtime_control SET
          mode='maintenance',maintenance_started_at=NOW(),session_epoch=session_epoch+1,
          revision=revision+1,updated_at=NOW() WHERE control_key='global' RETURNING *`);
        await event(client, 'draining', 'maintenance', req.user, { invalidatedSessions: true });
        return result.rows[0];
      });
      return res.json(publicRuntimeStatus(updated, { activeCount: 0, lastActivityAt: null }));
    } catch (error) { return res.status(Number(error.statusCode) || 500).json({ message: error.statusCode ? error.message : 'Wartungsmodus konnte nicht aktiviert werden.' }); }
  });

  router.post('/open', verifyToken({ requiredRole: 'Admin', allowMaintenanceAdmin: true }), async (req, res) => {
    try {
      const updated = await transaction(pool, async (client) => {
        const current = await lockedControl(client);
        if (current.mode === 'open') return current;
        const result = await client.query(`UPDATE core.app_runtime_control SET
          mode='open',message=NULL,requested_by_user_id=$1,drain_started_at=NULL,
          maintenance_started_at=NULL,revision=revision+1,updated_at=NOW()
          WHERE control_key='global' RETURNING *`, [req.user.id]);
        await event(client, current.mode, 'open', req.user, { reopened: true });
        return result.rows[0];
      });
      const processes = await countActiveCashProcesses(pool);
      return res.json(publicRuntimeStatus(updated, processes));
    } catch (error) { return res.status(500).json({ message: 'Anwendung konnte nicht freigegeben werden.' }); }
  });

  return router;
}

module.exports = { createMaintenanceRouter };
