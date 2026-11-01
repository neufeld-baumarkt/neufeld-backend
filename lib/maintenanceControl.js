const MODES = new Set(['open', 'draining', 'maintenance']);

async function readRuntimeControl(queryable) {
  const result = await queryable.query(`SELECT control_key,mode,message,session_epoch,revision,
    requested_by_user_id,drain_started_at,maintenance_started_at,updated_at
    FROM core.app_runtime_control WHERE control_key='global' LIMIT 1`);
  if (!result.rows[0]) throw new Error('Global runtime control is missing.');
  return result.rows[0];
}

async function countActiveCashProcesses(queryable) {
  const result = await queryable.query(`WITH active_drafts AS (
      SELECT COUNT(*)::integer AS active_count,MAX(updated_at) AS last_activity_at
      FROM controlling.cash_workflow_drafts WHERE status='active'
    ), open_drawers AS (
      SELECT COUNT(*)::integer AS active_count,MAX(o.updated_at) AS last_activity_at
      FROM core.cash_drawer_opening_counts o
      WHERE o.status='completed' AND o.lifecycle_status='active'
        AND NOT EXISTS (
          SELECT 1 FROM core.cash_drawer_closings c
          WHERE c.opening_count_id=o.id
        )
    )
    SELECT active_drafts.active_count + open_drawers.active_count AS active_count,
      active_drafts.active_count AS active_draft_count,
      open_drawers.active_count AS open_drawer_count,
      GREATEST(active_drafts.last_activity_at,open_drawers.last_activity_at) AS last_activity_at
    FROM active_drafts CROSS JOIN open_drawers`);
  return {
    activeCount: Number(result.rows[0]?.active_count || 0),
    activeDraftCount: Number(result.rows[0]?.active_draft_count || 0),
    openDrawerCount: Number(result.rows[0]?.open_drawer_count || 0),
    lastActivityAt: result.rows[0]?.last_activity_at || null,
  };
}

function publicRuntimeStatus(control, processes) {
  return {
    mode: MODES.has(control.mode) ? control.mode : 'maintenance',
    message: control.message || null,
    revision: Number(control.revision),
    activeCashProcesses: processes.activeCount,
    activeCashDrafts: Number(processes.activeDraftCount || 0),
    openCashDrawers: Number(processes.openDrawerCount || 0),
    lastCashActivityAt: processes.lastActivityAt,
    updatedAt: control.updated_at,
  };
}

function isMutation(method) {
  return !['GET', 'HEAD', 'OPTIONS'].includes(String(method || '').toUpperCase());
}

function mayContinueDuringDrain(req) {
  if (!isMutation(req.method)) return true;
  const path = String(req.originalUrl || req.url || '').split('?')[0];
  return path === '/api/login'
    || path.startsWith('/api/maintenance/')
    || path.startsWith('/api/tageskasse/');
}

function createMaintenanceGate({ pool }) {
  if (!pool) throw new TypeError('Maintenance gate requires pool.');
  return async (req, res, next) => {
    try {
      const control = await readRuntimeControl(pool);
      req.runtimeControl = control;
      if (control.mode === 'open') return next();
      if (control.mode === 'draining' && mayContinueDuringDrain(req)) return next();
      if (control.mode === 'maintenance'
          && String(req.originalUrl || req.url || '').split('?')[0] === '/api/login') return next();
      return res.status(503).json({
        code: control.mode === 'draining' ? 'MAINTENANCE_DRAINING' : 'MAINTENANCE_ACTIVE',
        message: control.message || (control.mode === 'draining'
          ? 'Wartung wird vorbereitet. Neue Vorgänge sind vorübergehend gesperrt.'
          : 'Die Anwendung befindet sich vorübergehend im Wartungsmodus.'),
      });
    } catch (error) {
      console.error('[MAINTENANCE_GATE_ERROR]', error?.code || error?.message);
      return res.status(503).json({ code: 'RUNTIME_CONTROL_UNAVAILABLE', message: 'Betriebsstatus nicht verfügbar.' });
    }
  };
}

module.exports = {
  MODES, countActiveCashProcesses, createMaintenanceGate, isMutation,
  mayContinueDuringDrain, publicRuntimeStatus, readRuntimeControl,
};
