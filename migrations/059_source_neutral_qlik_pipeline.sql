-- EuroControl wird in denselben Rohzeilen- und Regelpfad wie Qlik projiziert.
-- Die historischen Tabellennamen bleiben aus Kompatibilitaetsgruenden bestehen;
-- source_system ist die verbindliche Herkunft. Verbraucher lesen weiterhin
-- dieselben abgeleiteten Fachwerte und muessen die Quelle nicht unterscheiden.

BEGIN;

ALTER TABLE controlling.qlik_import_runs
  ADD COLUMN IF NOT EXISTS source_system varchar(20) NOT NULL DEFAULT 'QLIK',
  ADD COLUMN IF NOT EXISTS upstream_import_id uuid,
  ADD COLUMN IF NOT EXISTS upstream_source_sha256 char(64);

ALTER TABLE controlling.qlik_import_runs
  DROP CONSTRAINT IF EXISTS qlik_import_runs_source_system_check,
  DROP CONSTRAINT IF EXISTS qlik_import_runs_upstream_check,
  DROP CONSTRAINT IF EXISTS qlik_import_runs_upstream_import_fkey;

ALTER TABLE controlling.qlik_import_runs
  ADD CONSTRAINT qlik_import_runs_source_system_check
    CHECK (source_system IN ('QLIK', 'EUROCONTROL')),
  ADD CONSTRAINT qlik_import_runs_upstream_check CHECK (
    (source_system='QLIK' AND upstream_import_id IS NULL AND upstream_source_sha256 IS NULL)
    OR
    (source_system='EUROCONTROL' AND upstream_import_id IS NOT NULL
      AND upstream_source_sha256 ~ '^[0-9a-f]{64}$')
  ),
  ADD CONSTRAINT qlik_import_runs_upstream_import_fkey
    FOREIGN KEY (upstream_import_id)
    REFERENCES controlling.eurocontrol_imports(id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_qlik_import_runs_eurocontrol_upstream
  ON controlling.qlik_import_runs (upstream_import_id)
  WHERE source_system='EUROCONTROL';

-- Zwei eigenstaendige PDFs duerfen denselben fachlichen CSV-Inhalt ergeben.
-- Echte Qlik-Dateien bleiben weiterhin am Datei-Hash idempotent.
ALTER TABLE controlling.qlik_import_runs
  DROP CONSTRAINT IF EXISTS qlik_import_runs_source_sha256_key;

CREATE UNIQUE INDEX IF NOT EXISTS uq_qlik_import_runs_qlik_source_sha256
  ON controlling.qlik_import_runs (source_sha256)
  WHERE source_system='QLIK';

CREATE INDEX IF NOT EXISTS idx_qlik_import_runs_source_date
  ON controlling.qlik_import_runs (source_system, report_date DESC, completed_at DESC);

ALTER TABLE controlling.payment_review_events
  DROP CONSTRAINT IF EXISTS payment_review_events_event_type_check;

ALTER TABLE controlling.payment_review_events
  ADD CONSTRAINT payment_review_events_event_type_check
    CHECK (event_type IN (
      'QLIK_SYNCHRONISIERT', 'EUROCONTROL_SYNCHRONISIERT',
      'IN_PRUEFUNG', 'BESTAETIGT', 'KORRIGIERT', 'VERTAGT',
      'AUSGLEICH_AUSSTEHEND', 'EXTERN_BESTAETIGT', 'RUECKGESETZT',
      'TRANSFER_KORRIGIERT'
    )) NOT VALID;

CREATE OR REPLACE FUNCTION controlling.guard_qlik_import_run()
RETURNS trigger
LANGUAGE plpgsql
AS $qlik_run_guard$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'Importlaeufe duerfen nicht geloescht werden.';
  END IF;
  IF NEW.id IS DISTINCT FROM OLD.id
     OR NEW.report_date IS DISTINCT FROM OLD.report_date
     OR NEW.source_file_name IS DISTINCT FROM OLD.source_file_name
     OR NEW.source_sha256 IS DISTINCT FROM OLD.source_sha256
     OR NEW.source_size_bytes IS DISTINCT FROM OLD.source_size_bytes
     OR NEW.source_payload IS DISTINCT FROM OLD.source_payload
     OR NEW.source_system IS DISTINCT FROM OLD.source_system
     OR NEW.upstream_import_id IS DISTINCT FROM OLD.upstream_import_id
     OR NEW.upstream_source_sha256 IS DISTINCT FROM OLD.upstream_source_sha256
     OR NEW.started_at IS DISTINCT FROM OLD.started_at
     OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
    RAISE EXCEPTION 'Quelldaten des gemeinsamen Importpfads sind unveraenderlich.';
  END IF;
  IF NOT (
    NEW.status = OLD.status
    OR (OLD.status = 'RECEIVED' AND NEW.status IN ('VALIDATED', 'FAILED'))
    OR (OLD.status = 'VALIDATED' AND NEW.status IN ('IMPORTED', 'FAILED'))
  ) THEN
    RAISE EXCEPTION 'Ungueltiger Importstatus: % -> %', OLD.status, NEW.status;
  END IF;
  RETURN NEW;
END
$qlik_run_guard$;

COMMENT ON COLUMN controlling.qlik_import_runs.source_system IS
  'Herkunft des Qlik-kompatiblen Rohdatensatzes; Verbraucher verwenden unabhaengig davon denselben Regelpfad.';
COMMENT ON COLUMN controlling.qlik_import_runs.upstream_import_id IS
  'EuroControl-PDF-Import, aus dem der Qlik-kompatible Rohdatensatz erzeugt wurde.';

CREATE OR REPLACE VIEW controlling.canonical_daily_metric_values AS
WITH unified_origin AS (
  SELECT derived.id,
         CASE WHEN bool_and(run.source_system='EUROCONTROL')
              THEN 'EUROCONTROL' ELSE 'QLIK' END AS source_system
  FROM controlling.qlik_derived_daily_values derived
  JOIN controlling.qlik_derived_value_sources source
    ON source.derived_value_id=derived.id
  JOIN controlling.qlik_import_rows raw
    ON raw.id=source.qlik_import_row_id
  JOIN controlling.qlik_import_runs run
    ON run.id=raw.import_run_id
  GROUP BY derived.id
), unified_ranked AS (
  SELECT derived.id, derived.branch_id, derived.customer_number,
         derived.business_date, metric.metric_code, derived.numeric_value,
         derived.availability_status, derived.quality_status,
         derived.source_fingerprint, derived.calculated_at AS source_time,
         origin.source_system,
         row_number() OVER (
           PARTITION BY origin.source_system, derived.branch_id,
                        derived.business_date, metric.metric_code
           ORDER BY derived.calculated_at DESC, derived.id DESC
         ) AS source_rank
  FROM controlling.qlik_derived_daily_values derived
  JOIN unified_origin origin ON origin.id=derived.id
  JOIN controlling.qlik_metric_definitions metric
    ON metric.id=derived.metric_definition_id
  WHERE derived.availability_status='VORHANDEN'
    AND derived.quality_status<>'ABGELEHNT'
), legacy_eurocontrol AS (
  SELECT derived.id, derived.branch_id, derived.customer_number,
         derived.business_date, derived.metric_code, derived.numeric_value,
         derived.availability_status, derived.quality_status,
         derived.source_fingerprint, derived.created_at AS source_time,
         'EUROCONTROL'::text AS source_system
  FROM controlling.eurocontrol_derived_daily_values derived
  WHERE derived.availability_status='VORHANDEN'
    AND derived.quality_status='VALIDATED'
), eurocontrol_ranked AS (
  SELECT candidates.*,
         row_number() OVER (
           PARTITION BY candidates.branch_id, candidates.business_date,
                        candidates.metric_code
           ORDER BY candidates.source_time DESC, candidates.id DESC
         ) AS source_rank
  FROM (
    SELECT id, branch_id, customer_number, business_date, metric_code,
           numeric_value, availability_status, quality_status,
           source_fingerprint, source_time, source_system
    FROM unified_ranked
    WHERE source_system='EUROCONTROL' AND source_rank=1
    UNION ALL
    SELECT * FROM legacy_eurocontrol
  ) candidates
), qlik AS (
  SELECT * FROM unified_ranked
  WHERE source_system='QLIK' AND source_rank=1
), eurocontrol AS (
  SELECT * FROM eurocontrol_ranked WHERE source_rank=1
)
SELECT COALESCE(qlik.branch_id, eurocontrol.branch_id) AS branch_id,
       COALESCE(qlik.customer_number, eurocontrol.customer_number) AS customer_number,
       COALESCE(qlik.business_date, eurocontrol.business_date) AS business_date,
       COALESCE(qlik.metric_code, eurocontrol.metric_code) AS metric_code,
       COALESCE(qlik.numeric_value, eurocontrol.numeric_value) AS numeric_value,
       CASE WHEN qlik.id IS NOT NULL THEN 'QLIK' ELSE 'EUROCONTROL' END AS selected_source,
       CASE
         WHEN qlik.id IS NOT NULL AND eurocontrol.id IS NOT NULL
              AND qlik.numeric_value=eurocontrol.numeric_value THEN 'BOTH_EQUAL'
         WHEN qlik.id IS NOT NULL AND eurocontrol.id IS NOT NULL THEN 'BOTH_CONFLICT'
         WHEN qlik.id IS NOT NULL THEN 'QLIK_PRIMARY'
         ELSE 'EUROCONTROL_FALLBACK'
       END AS source_status,
       qlik.numeric_value AS qlik_numeric_value,
       eurocontrol.numeric_value AS eurocontrol_numeric_value,
       qlik.id AS qlik_value_id,
       eurocontrol.id AS eurocontrol_value_id,
       COALESCE(qlik.source_fingerprint, eurocontrol.source_fingerprint) AS selected_fingerprint,
       COALESCE(qlik.id, eurocontrol.id) AS id
FROM qlik
FULL OUTER JOIN eurocontrol
  ON eurocontrol.branch_id=qlik.branch_id
 AND eurocontrol.business_date=qlik.business_date
 AND eurocontrol.metric_code=qlik.metric_code;

DO $runtime_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sandbox_app') THEN
    GRANT SELECT ON controlling.canonical_daily_metric_values TO sandbox_app;
  END IF;
END
$runtime_grants$;

COMMIT;
