-- Revisionssichere Rohschicht fuer Qlik-Filialberichte.
-- Fachliche Kennzahlen und Verifikationen werden erst spaeter aus diesen
-- unveraenderten Quelldaten abgeleitet.

CREATE TABLE IF NOT EXISTS controlling.qlik_import_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  report_date date NOT NULL,
  source_file_name varchar(255) NOT NULL
    CHECK (length(trim(source_file_name)) BETWEEN 1 AND 255),
  source_sha256 char(64) NOT NULL UNIQUE
    CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
  source_size_bytes integer NOT NULL CHECK (source_size_bytes > 0),
  source_payload bytea NOT NULL,
  status varchar(20) NOT NULL DEFAULT 'RECEIVED'
    CHECK (status IN ('RECEIVED', 'VALIDATED', 'IMPORTED', 'FAILED')),
  row_count integer CHECK (row_count >= 0),
  customer_count integer CHECK (customer_count >= 0),
  payment_type_count integer CHECK (payment_type_count >= 0),
  error_code varchar(100),
  error_message text,
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (id, report_date),
  CHECK (octet_length(source_payload) = source_size_bytes),
  CHECK (
    (status IN ('RECEIVED', 'VALIDATED') AND completed_at IS NULL)
    OR (status IN ('IMPORTED', 'FAILED') AND completed_at IS NOT NULL)
  ),
  CHECK (
    (status <> 'FAILED' AND error_code IS NULL AND error_message IS NULL)
    OR (status = 'FAILED' AND error_code IS NOT NULL)
  ),
  CHECK (
    status <> 'IMPORTED'
    OR (row_count IS NOT NULL AND customer_count IS NOT NULL AND payment_type_count IS NOT NULL)
  )
);

CREATE TABLE IF NOT EXISTS controlling.qlik_import_rows (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  import_run_id uuid NOT NULL,
  source_row_number integer NOT NULL CHECK (source_row_number >= 2),
  report_date date NOT NULL,
  customer_number char(6) NOT NULL CHECK (customer_number ~ '^\d{6}$'),
  branch_name varchar(50),
  payment_type_original text NOT NULL CHECK (length(trim(payment_type_original)) > 0),
  amount_cents bigint NOT NULL,
  raw_fields jsonb NOT NULL
    CHECK (jsonb_typeof(raw_fields) = 'array' AND jsonb_array_length(raw_fields) = 4),
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT qlik_import_rows_run_date_fkey
    FOREIGN KEY (import_run_id, report_date)
    REFERENCES controlling.qlik_import_runs(id, report_date),
  CONSTRAINT qlik_import_rows_source_row_unique
    UNIQUE (import_run_id, source_row_number),
  CONSTRAINT qlik_import_rows_business_key_unique
    UNIQUE (import_run_id, report_date, customer_number, payment_type_original)
);

CREATE TABLE IF NOT EXISTS controlling.qlik_import_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  import_run_id uuid NOT NULL
    REFERENCES controlling.qlik_import_runs(id),
  event_type varchar(80) NOT NULL CHECK (length(trim(event_type)) > 0),
  details jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(details) = 'object'),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_qlik_import_runs_report_date
  ON controlling.qlik_import_runs (report_date DESC, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_qlik_import_rows_date_customer
  ON controlling.qlik_import_rows (report_date DESC, customer_number);
CREATE INDEX IF NOT EXISTS idx_qlik_import_rows_payment_type
  ON controlling.qlik_import_rows (payment_type_original, report_date DESC);
CREATE INDEX IF NOT EXISTS idx_qlik_import_events_run_time
  ON controlling.qlik_import_events (import_run_id, created_at);

CREATE OR REPLACE FUNCTION controlling.guard_qlik_import_run()
RETURNS trigger
LANGUAGE plpgsql
AS $qlik_run_guard$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'Qlik-Importlaeufe duerfen nicht geloescht werden.';
  END IF;
  IF NEW.id IS DISTINCT FROM OLD.id
     OR NEW.report_date IS DISTINCT FROM OLD.report_date
     OR NEW.source_file_name IS DISTINCT FROM OLD.source_file_name
     OR NEW.source_sha256 IS DISTINCT FROM OLD.source_sha256
     OR NEW.source_size_bytes IS DISTINCT FROM OLD.source_size_bytes
     OR NEW.source_payload IS DISTINCT FROM OLD.source_payload
     OR NEW.started_at IS DISTINCT FROM OLD.started_at
     OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
    RAISE EXCEPTION 'Qlik-Quelldaten sind unveraenderlich.';
  END IF;
  IF NOT (
    NEW.status = OLD.status
    OR (OLD.status = 'RECEIVED' AND NEW.status IN ('VALIDATED', 'FAILED'))
    OR (OLD.status = 'VALIDATED' AND NEW.status IN ('IMPORTED', 'FAILED'))
  ) THEN
    RAISE EXCEPTION 'Ungueltiger Qlik-Importstatus: % -> %', OLD.status, NEW.status;
  END IF;
  RETURN NEW;
END
$qlik_run_guard$;

CREATE OR REPLACE FUNCTION controlling.reject_qlik_raw_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $qlik_raw_guard$
BEGIN
  RAISE EXCEPTION 'Qlik-Rohdaten und Importereignisse sind append-only.';
END
$qlik_raw_guard$;

DROP TRIGGER IF EXISTS qlik_import_runs_immutable ON controlling.qlik_import_runs;
CREATE TRIGGER qlik_import_runs_immutable
BEFORE UPDATE OR DELETE ON controlling.qlik_import_runs
FOR EACH ROW EXECUTE FUNCTION controlling.guard_qlik_import_run();

DROP TRIGGER IF EXISTS qlik_import_rows_append_only ON controlling.qlik_import_rows;
CREATE TRIGGER qlik_import_rows_append_only
BEFORE UPDATE OR DELETE ON controlling.qlik_import_rows
FOR EACH ROW EXECUTE FUNCTION controlling.reject_qlik_raw_mutation();

DROP TRIGGER IF EXISTS qlik_import_events_append_only ON controlling.qlik_import_events;
CREATE TRIGGER qlik_import_events_append_only
BEFORE UPDATE OR DELETE ON controlling.qlik_import_events
FOR EACH ROW EXECUTE FUNCTION controlling.reject_qlik_raw_mutation();

COMMENT ON TABLE controlling.qlik_import_runs IS
  'Qlik-Importkopf mit unveraendertem CSV-Original und duplikatsicherem SHA-256.';
COMMENT ON TABLE controlling.qlik_import_rows IS
  'Unveraenderliche, centgenau geparste Originalzeilen eines Qlik-Filialberichts.';
COMMENT ON TABLE controlling.qlik_import_events IS
  'Append-only Ereignishistorie fuer Qlik-Importlaeufe.';
