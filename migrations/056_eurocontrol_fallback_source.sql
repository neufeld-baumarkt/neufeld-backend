-- Revisionssichere EuroControl-Ersatzquelle fuer Alex Modul 2.
-- Roh-PDF, extrahierte Zahlungsarten und veredelte Tageswerte bleiben getrennt.

BEGIN;

CREATE TABLE IF NOT EXISTS controlling.branch_source_identifiers (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  branch_id integer NOT NULL REFERENCES public.filialen(id),
  source_system varchar(30) NOT NULL
    CHECK (source_system IN ('QLIK', 'EUROCONTROL')),
  external_identifier varchar(30) NOT NULL
    CHECK (external_identifier ~ '^[0-9]{6}$'),
  external_label varchar(160),
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_system, external_identifier),
  UNIQUE (source_system, branch_id)
);

WITH requested(branch_name, external_identifier) AS (
  VALUES
    ('Telgte', '041117'),
    ('Ahaus', '041129'),
    ('Vreden', '041135'),
    ('Münster', '041140')
), sources(source_system) AS (
  VALUES ('QLIK'), ('EUROCONTROL')
)
INSERT INTO controlling.branch_source_identifiers (
  branch_id, source_system, external_identifier, external_label
)
SELECT branch.id, sources.source_system, requested.external_identifier, branch.name
FROM requested
JOIN public.filialen branch ON lower(branch.name)=lower(requested.branch_name)
CROSS JOIN sources
ON CONFLICT (source_system, external_identifier) DO NOTHING;

CREATE TABLE IF NOT EXISTS controlling.eurocontrol_payment_mappings (
  external_label varchar(100) PRIMARY KEY,
  mapping_role varchar(30) NOT NULL
    CHECK (mapping_role IN ('EC_CASH', 'CREDIT_PROVIDER', 'IGNORE')),
  provider_code varchar(50),
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (mapping_role='CREDIT_PROVIDER' AND provider_code IS NOT NULL)
    OR (mapping_role<>'CREDIT_PROVIDER' AND provider_code IS NULL)
  )
);

INSERT INTO controlling.eurocontrol_payment_mappings (
  external_label, mapping_role, provider_code
) VALUES
  ('Kartenzahlung', 'EC_CASH', NULL),
  ('Maestro', 'CREDIT_PROVIDER', 'MAESTRO'),
  ('Mastercard', 'CREDIT_PROVIDER', 'MASTERCARD'),
  ('Visa', 'CREDIT_PROVIDER', 'VISA'),
  ('Amex', 'CREDIT_PROVIDER', 'AMEX'),
  ('American Express', 'CREDIT_PROVIDER', 'AMEX'),
  ('Diners', 'CREDIT_PROVIDER', 'DINERS'),
  ('JCB', 'CREDIT_PROVIDER', 'JCB'),
  ('BAR', 'IGNORE', NULL),
  ('Gutschein', 'IGNORE', NULL),
  ('Rechnungskauf-Karte', 'IGNORE', NULL)
ON CONFLICT (external_label) DO NOTHING;

CREATE TABLE IF NOT EXISTS controlling.eurocontrol_imports (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  branch_id integer NOT NULL REFERENCES public.filialen(id),
  report_date date NOT NULL,
  source_file_name varchar(255) NOT NULL
    CHECK (length(trim(source_file_name)) BETWEEN 1 AND 255),
  source_sha256 char(64) NOT NULL UNIQUE
    CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
  source_size_bytes integer NOT NULL CHECK (source_size_bytes > 0),
  source_payload bytea NOT NULL,
  external_identifier varchar(30) NOT NULL,
  external_label varchar(160) NOT NULL,
  page_count integer NOT NULL CHECK (page_count > 0),
  report_version varchar(100),
  report_created_at timestamp without time zone,
  parsed_payload jsonb NOT NULL CHECK (jsonb_typeof(parsed_payload)='object'),
  status varchar(20) NOT NULL DEFAULT 'IMPORTED'
    CHECK (status IN ('IMPORTED', 'CONFLICT')),
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (octet_length(source_payload)=source_size_bytes)
);

CREATE INDEX IF NOT EXISTS idx_eurocontrol_imports_branch_date
  ON controlling.eurocontrol_imports (branch_id, report_date DESC, created_at DESC);

CREATE TABLE IF NOT EXISTS controlling.eurocontrol_import_payment_rows (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  import_id uuid NOT NULL REFERENCES controlling.eurocontrol_imports(id),
  row_number integer NOT NULL CHECK (row_number > 0),
  external_label varchar(100) NOT NULL,
  mapping_role varchar(30) NOT NULL
    CHECK (mapping_role IN ('EC_CASH', 'CREDIT_PROVIDER', 'IGNORE')),
  provider_code varchar(50),
  soll_cents bigint NOT NULL,
  ist_cents bigint NOT NULL,
  difference_cents bigint NOT NULL,
  withdrawal_cents bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (import_id, row_number),
  UNIQUE (import_id, external_label),
  CHECK (
    (mapping_role='CREDIT_PROVIDER' AND provider_code IS NOT NULL)
    OR (mapping_role<>'CREDIT_PROVIDER' AND provider_code IS NULL)
  )
);

CREATE TABLE IF NOT EXISTS controlling.eurocontrol_daily_values (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  import_id uuid NOT NULL REFERENCES controlling.eurocontrol_imports(id),
  branch_id integer NOT NULL REFERENCES public.filialen(id),
  business_date date NOT NULL,
  payment_type_code varchar(30) NOT NULL
    CHECK (payment_type_code IN ('EC_CASH', 'KREDITKARTE')),
  amount_cents bigint NOT NULL CHECK (amount_cents >= 0),
  provider_breakdown jsonb NOT NULL DEFAULT '{}'::jsonb
    CHECK (jsonb_typeof(provider_breakdown)='object'),
  quality_status varchar(20) NOT NULL
    CHECK (quality_status IN ('VALIDATED', 'CONFLICT')),
  source_system varchar(30) NOT NULL DEFAULT 'EUROCONTROL'
    CHECK (source_system='EUROCONTROL'),
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (import_id, payment_type_code)
);

CREATE INDEX IF NOT EXISTS idx_eurocontrol_daily_values_lookup
  ON controlling.eurocontrol_daily_values
    (business_date DESC, branch_id, payment_type_code, created_at DESC);

CREATE TABLE IF NOT EXISTS controlling.eurocontrol_import_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  import_id uuid NOT NULL REFERENCES controlling.eurocontrol_imports(id),
  event_type varchar(50) NOT NULL,
  details jsonb NOT NULL DEFAULT '{}'::jsonb
    CHECK (jsonb_typeof(details)='object'),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION controlling.reject_eurocontrol_fact_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $eurocontrol_guard$
BEGIN
  RAISE EXCEPTION 'EuroControl-Rohdaten, Fachwerte und Ereignisse sind append-only.';
END
$eurocontrol_guard$;

DO $triggers$
DECLARE
  table_name text;
  trigger_name text;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'eurocontrol_imports',
    'eurocontrol_import_payment_rows',
    'eurocontrol_daily_values',
    'eurocontrol_import_events'
  ]
  LOOP
    trigger_name := table_name || '_append_only';
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON controlling.%I', trigger_name, table_name);
    EXECUTE format(
      'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON controlling.%I FOR EACH ROW EXECUTE FUNCTION controlling.reject_eurocontrol_fact_mutation()',
      trigger_name, table_name
    );
  END LOOP;
END
$triggers$;

DO $runtime_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sandbox_app') THEN
    GRANT SELECT ON
      controlling.branch_source_identifiers,
      controlling.eurocontrol_payment_mappings
    TO sandbox_app;
    GRANT SELECT, INSERT ON
      controlling.eurocontrol_imports,
      controlling.eurocontrol_import_payment_rows,
      controlling.eurocontrol_daily_values,
      controlling.eurocontrol_import_events
    TO sandbox_app;
    REVOKE UPDATE, DELETE, TRUNCATE ON
      controlling.eurocontrol_imports,
      controlling.eurocontrol_import_payment_rows,
      controlling.eurocontrol_daily_values,
      controlling.eurocontrol_import_events
    FROM sandbox_app;
  END IF;
END
$runtime_grants$;

COMMIT;
