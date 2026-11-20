-- DB-gestuetzte Steuerung und Beobachtung manueller Qlik-Exporte.
-- Die App stellt nur Auftraege ein; ein signierter NAS-Worker fuehrt sie aus.

CREATE TABLE IF NOT EXISTS controlling.qlik_export_jobs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  report_date date NOT NULL,
  trigger_type varchar(20) NOT NULL DEFAULT 'MANUAL'
    CHECK (trigger_type IN ('MANUAL', 'SCHEDULED')),
  status varchar(30) NOT NULL DEFAULT 'REQUESTED'
    CHECK (status IN (
      'REQUESTED', 'EXPORT_RUNNING', 'IMPORT_RUNNING',
      'SUCCEEDED', 'NO_DATA', 'FAILED'
    )),
  requested_by_user_id integer REFERENCES public.users(id) ON DELETE SET NULL,
  requested_by_name varchar(160),
  worker_id varchar(120),
  source_file_name varchar(255),
  import_run_id uuid REFERENCES controlling.qlik_import_runs(id),
  message varchar(500),
  error_code varchar(100),
  requested_at timestamptz NOT NULL DEFAULT now(),
  started_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  CHECK (
    (status IN ('REQUESTED', 'EXPORT_RUNNING', 'IMPORT_RUNNING') AND completed_at IS NULL)
    OR (status IN ('SUCCEEDED', 'NO_DATA', 'FAILED') AND completed_at IS NOT NULL)
  ),
  CHECK (status <> 'FAILED' OR error_code IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_qlik_export_jobs_active_date
  ON controlling.qlik_export_jobs (report_date)
  WHERE status IN ('REQUESTED', 'EXPORT_RUNNING', 'IMPORT_RUNNING');

CREATE INDEX IF NOT EXISTS idx_qlik_export_jobs_recent
  ON controlling.qlik_export_jobs (requested_at DESC);

COMMENT ON TABLE controlling.qlik_export_jobs IS
  'Zentrale, von App und NAS-Worker gemeinsam genutzte Auftrags- und Statusquelle fuer Qlik-Exporte.';

DO $runtime_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sandbox_app') THEN
    REVOKE DELETE, TRUNCATE ON controlling.qlik_export_jobs FROM sandbox_app;
    GRANT SELECT, INSERT, UPDATE ON controlling.qlik_export_jobs TO sandbox_app;
  END IF;
END
$runtime_grants$;
