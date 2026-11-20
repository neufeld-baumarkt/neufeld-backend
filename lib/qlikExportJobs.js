const ACTIVE_STATUSES = new Set(['REQUESTED', 'EXPORT_RUNNING', 'IMPORT_RUNNING']);
const TERMINAL_STATUSES = new Set(['SUCCEEDED', 'NO_DATA', 'FAILED']);
const WORKER_STATUSES = new Set([...ACTIVE_STATUSES, ...TERMINAL_STATUSES]);

function berlinIsoDate(now = new Date()) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Europe/Berlin', year: 'numeric', month: '2-digit', day: '2-digit',
  }).format(now);
}

function previousIsoDate(now = new Date()) {
  const parts = berlinIsoDate(now).split('-').map(Number);
  const utc = new Date(Date.UTC(parts[0], parts[1] - 1, parts[2] - 1));
  return utc.toISOString().slice(0, 10);
}

function validateReportDate(value, now = new Date()) {
  const date = String(value || previousIsoDate(now));
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || Number.isNaN(Date.parse(`${date}T00:00:00Z`))) {
    const error = new Error('Das Qlik-Berichtsdatum ist ungültig.');
    error.statusCode = 400;
    throw error;
  }
  if (date >= berlinIsoDate(now)) {
    const error = new Error('Qlik kann nur für abgeschlossene Tage gestartet werden.');
    error.statusCode = 400;
    throw error;
  }
  return date;
}

function mapJob(row) {
  if (!row) return null;
  return {
    id: row.id,
    reportDate: String(row.report_date).slice(0, 10),
    triggerType: row.trigger_type,
    status: row.status,
    requestedBy: row.requested_by_name || null,
    workerId: row.worker_id || null,
    sourceFileName: row.source_file_name || null,
    importRunId: row.import_run_id || null,
    message: row.message || null,
    errorCode: row.error_code || null,
    requestedAt: row.requested_at,
    startedAt: row.started_at,
    updatedAt: row.updated_at,
    completedAt: row.completed_at,
  };
}

async function loadStatus(pool) {
  const [jobResult, importResult] = await Promise.all([
    pool.query(`SELECT * FROM controlling.qlik_export_jobs ORDER BY requested_at DESC LIMIT 1`),
    pool.query(`SELECT id,report_date,status,source_file_name,completed_at
      FROM controlling.qlik_import_runs WHERE status='IMPORTED'
      ORDER BY report_date DESC,completed_at DESC LIMIT 1`),
  ]);
  const imported = importResult.rows[0];
  return {
    job: mapJob(jobResult.rows[0]),
    lastImport: imported ? {
      id: imported.id,
      reportDate: String(imported.report_date).slice(0, 10),
      status: imported.status,
      sourceFileName: imported.source_file_name,
      completedAt: imported.completed_at,
    } : null,
  };
}

async function requestJob(pool, { reportDate, userId, userName, now = new Date() }) {
  const date = validateReportDate(reportDate, now);
  try {
    const result = await pool.query(
      `INSERT INTO controlling.qlik_export_jobs
         (report_date,trigger_type,status,requested_by_user_id,requested_by_name)
       VALUES ($1,'MANUAL','REQUESTED',$2,$3)
       RETURNING *`,
      [date, userId || null, String(userName || '').trim() || null],
    );
    return { created: true, job: mapJob(result.rows[0]) };
  } catch (error) {
    if (error?.code !== '23505') throw error;
    const existing = await pool.query(
      `SELECT * FROM controlling.qlik_export_jobs
       WHERE report_date=$1 AND status IN ('REQUESTED','EXPORT_RUNNING','IMPORT_RUNNING')
       ORDER BY requested_at DESC LIMIT 1`, [date],
    );
    return { created: false, job: mapJob(existing.rows[0]) };
  }
}

async function claimJob(pool, workerId) {
  const result = await pool.query(
    `WITH next_job AS (
       SELECT id FROM controlling.qlik_export_jobs
       WHERE status='REQUESTED' ORDER BY requested_at ASC
       FOR UPDATE SKIP LOCKED LIMIT 1
     )
     UPDATE controlling.qlik_export_jobs job
     SET status='EXPORT_RUNNING', worker_id=$1, started_at=COALESCE(started_at,now()),
         updated_at=now(), message='Qlik-Export wurde vom NAS-Worker übernommen.'
     FROM next_job WHERE job.id=next_job.id RETURNING job.*`,
    [workerId],
  );
  return mapJob(result.rows[0]);
}

async function updateJob(pool, jobId, update) {
  if (!WORKER_STATUSES.has(update.status) || update.status === 'REQUESTED') {
    const error = new Error('Ungültiger Qlik-Jobstatus.');
    error.statusCode = 400;
    throw error;
  }
  const terminal = TERMINAL_STATUSES.has(update.status);
  if (update.status === 'FAILED' && !String(update.errorCode || '').trim()) {
    const error = new Error('Ein fehlgeschlagener Qlik-Job benötigt einen Fehlercode.');
    error.statusCode = 400;
    throw error;
  }
  const result = await pool.query(
    `UPDATE controlling.qlik_export_jobs
     SET status=$2, source_file_name=COALESCE($3,source_file_name),
         import_run_id=COALESCE($4,import_run_id), message=$5, error_code=$6,
         updated_at=now(), completed_at=CASE WHEN $7 THEN now() ELSE NULL END
     WHERE id=$1 AND status IN ('EXPORT_RUNNING','IMPORT_RUNNING')
     RETURNING *`,
    [jobId, update.status, update.sourceFileName || null, update.importRunId || null,
      String(update.message || '').slice(0, 500) || null,
      String(update.errorCode || '').slice(0, 100) || null, terminal],
  );
  return mapJob(result.rows[0]);
}

module.exports = {
  ACTIVE_STATUSES, WORKER_STATUSES, berlinIsoDate, previousIsoDate, validateReportDate,
  mapJob, loadStatus, requestJob, claimJob, updateJob,
};
