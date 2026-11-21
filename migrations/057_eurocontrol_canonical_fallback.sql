Attempting to perform the InitializeDefaultDrives operation on the 'FileSystem' provider failed.
-- Vollstaendiger EuroControl-Fallback fuer die app-relevanten Qlik-Fachwerte.
-- Das PDF wird vor dem Import kryptografisch und technisch geprueft, aber nicht
-- dauerhaft als Blob gespeichert. Strukturierte Rohfelder und Ableitungen sind
-- append-only; die kanonische Sicht bevorzugt Qlik je Fachwert.

BEGIN;

ALTER TABLE controlling.eurocontrol_imports
  ALTER COLUMN source_payload DROP NOT NULL;
ALTER TABLE controlling.eurocontrol_imports
  DROP CONSTRAINT IF EXISTS eurocontrol_imports_payload_size_check;
ALTER TABLE controlling.eurocontrol_imports
  ADD CONSTRAINT eurocontrol_imports_payload_size_check
  CHECK (source_payload IS NULL OR octet_length(source_payload)=source_size_bytes);
ALTER TABLE controlling.eurocontrol_imports
  ADD COLUMN IF NOT EXISTS parser_version varchar(30);

CREATE TABLE IF NOT EXISTS controlling.eurocontrol_import_fields (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  import_id uuid NOT NULL REFERENCES controlling.eurocontrol_imports(id),
  field_code varchar(60) NOT NULL CHECK (field_code IN (
    'REPORT_REVENUE_CENTS', 'PAYOUTS_CENTS', 'CUSTOMER_COUNT',
    'AVERAGE_RECEIPT_CENTS', 'BANK_DEPOSIT_CENTS',
    'VAT_7_GROSS_CENTS', 'VAT_19_GROSS_CENTS'
  )),
  value_kind varchar(20) NOT NULL CHECK (value_kind IN ('MONEY_CENTS', 'COUNT')),
  amount_cents bigint,
  count_value bigint,
  page_number integer NOT NULL CHECK (page_number > 0),
  explicitly_reported boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (import_id, field_code),
  CHECK (
    (value_kind='MONEY_CENTS' AND amount_cents IS NOT NULL AND count_value IS NULL)
    OR (value_kind='COUNT' AND count_value IS NOT NULL AND amount_cents IS NULL)
  )
);

CREATE TABLE IF NOT EXISTS controlling.eurocontrol_derived_daily_values (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  import_id uuid NOT NULL REFERENCES controlling.eurocontrol_imports(id),
  branch_id integer NOT NULL REFERENCES public.filialen(id),
  customer_number char(6) NOT NULL CHECK (customer_number ~ '^\d{6}$'),
  business_date date NOT NULL,
  metric_code varchar(80) NOT NULL CHECK (metric_code IN (
    'EC_ZAHLUNGEN', 'KREDITKARTEN_ZAHLUNGEN', 'KASSENDIFFERENZ',
    'BAR_AN_BANK_QLIK', 'KASSENAUSZAHLUNGEN', 'UMSATZ_7_PROZENT',
    'UMSATZ_19_PROZENT', 'GESAMTUMSATZ_QLIK'
  )),
  numeric_value numeric(20,4) NOT NULL,
  provider_breakdown jsonb NOT NULL DEFAULT '{}'::jsonb
    CHECK (jsonb_typeof(provider_breakdown)='object'),
  source_fields jsonb NOT NULL CHECK (jsonb_typeof(source_fields)='array'),
  availability_status varchar(30) NOT NULL DEFAULT 'VORHANDEN'
    CHECK (availability_status IN ('VORHANDEN', 'UNVOLLSTAENDIG')),
  quality_status varchar(30) NOT NULL
    CHECK (quality_status IN ('VALIDATED', 'CONFLICT')),
  source_fingerprint char(64) NOT NULL
    CHECK (source_fingerprint ~ '^[0-9a-f]{64}$'),
  parser_version varchar(30) NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (import_id, metric_code)
);

CREATE INDEX IF NOT EXISTS idx_eurocontrol_derived_daily_values_lookup
  ON controlling.eurocontrol_derived_daily_values
    (business_date DESC, branch_id, metric_code, created_at DESC);

DROP TRIGGER IF EXISTS eurocontrol_import_fields_append_only
  ON controlling.eurocontrol_import_fields;
CREATE TRIGGER eurocontrol_import_fields_append_only
BEFORE UPDATE OR DELETE ON controlling.eurocontrol_import_fields
FOR EACH ROW EXECUTE FUNCTION controlling.reject_eurocontrol_fact_mutation();

DROP TRIGGER IF EXISTS eurocontrol_derived_daily_values_append_only
  ON controlling.eurocontrol_derived_daily_values;
CREATE TRIGGER eurocontrol_derived_daily_values_append_only
BEFORE UPDATE OR DELETE ON controlling.eurocontrol_derived_daily_values
FOR EACH ROW EXECUTE FUNCTION controlling.reject_eurocontrol_fact_mutation();

CREATE OR REPLACE VIEW controlling.canonical_daily_metric_values AS
WITH qlik_ranked AS (
  SELECT derived.id, derived.branch_id, derived.customer_number,
         derived.business_date, metric.metric_code, derived.numeric_value,
         derived.availability_status, derived.quality_status,
         derived.source_fingerprint, derived.calculated_at,
         row_number() OVER (
           PARTITION BY derived.branch_id, derived.business_date, metric.metric_code
           ORDER BY derived.calculated_at DESC, derived.id DESC
         ) AS source_rank
  FROM controlling.qlik_derived_daily_values derived
  JOIN controlling.qlik_metric_definitions metric
    ON metric.id=derived.metric_definition_id
  WHERE derived.availability_status='VORHANDEN'
    AND derived.quality_status<>'ABGELEHNT'
), eurocontrol_ranked AS (
  SELECT derived.id, derived.branch_id, derived.customer_number,
         derived.business_date, derived.metric_code, derived.numeric_value,
         derived.availability_status, derived.quality_status,
         derived.source_fingerprint, derived.created_at,
         row_number() OVER (
           PARTITION BY derived.branch_id, derived.business_date, derived.metric_code
           ORDER BY derived.created_at DESC, derived.id DESC
         ) AS source_rank
  FROM controlling.eurocontrol_derived_daily_values derived
  WHERE derived.availability_status='VORHANDEN'
    AND derived.quality_status='VALIDATED'
), qlik AS (
  SELECT * FROM qlik_ranked WHERE source_rank=1
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
       COALESCE(qlik.source_fingerprint, eurocontrol.source_fingerprint) AS selected_fingerprint
FROM qlik
FULL OUTER JOIN eurocontrol
  ON eurocontrol.branch_id=qlik.branch_id
 AND eurocontrol.business_date=qlik.business_date
 AND eurocontrol.metric_code=qlik.metric_code;

CREATE OR REPLACE VIEW controlling.canonical_daily_metric_aggregates AS
WITH expected AS (
  SELECT count(*)::integer AS expected_count
  FROM controlling.branch_source_identifiers
  WHERE source_system='EUROCONTROL' AND active=true
), components AS (
  SELECT value.business_date, value.numeric_value, value.selected_source,
         value.source_status
  FROM controlling.canonical_daily_metric_values value
  JOIN public.filialen branch ON branch.id=value.branch_id AND branch.aktiv=true
  WHERE value.metric_code='GESAMTUMSATZ_QLIK'
)
SELECT components.business_date,
       'GESAMTUMSATZ_ALLE_FILIALEN_QLIK'::varchar(80) AS metric_code,
       CASE WHEN count(*)=expected.expected_count THEN sum(components.numeric_value) END AS numeric_value,
       expected.expected_count AS expected_member_count,
       count(*)::integer AS actual_member_count,
       CASE WHEN count(*)=expected.expected_count THEN 'VORHANDEN' ELSE 'UNVOLLSTAENDIG' END AS availability_status,
       CASE WHEN count(DISTINCT components.selected_source)=1 THEN min(components.selected_source)
            ELSE 'MIXED' END AS selected_source,
       CASE WHEN bool_or(components.source_status='BOTH_CONFLICT') THEN 'SOURCE_CONFLICT'
            WHEN count(*)<>expected.expected_count THEN 'INCOMPLETE'
            WHEN count(DISTINCT components.selected_source)>1 THEN 'MIXED_SOURCES'
            ELSE 'COMPLETE' END AS source_status
FROM components
CROSS JOIN expected
GROUP BY components.business_date, expected.expected_count;

DO $runtime_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sandbox_app') THEN
    GRANT SELECT, INSERT ON
      controlling.eurocontrol_import_fields,
      controlling.eurocontrol_derived_daily_values
    TO sandbox_app;
    REVOKE UPDATE, DELETE, TRUNCATE ON
      controlling.eurocontrol_import_fields,
      controlling.eurocontrol_derived_daily_values
    FROM sandbox_app;
    GRANT SELECT ON
      controlling.canonical_daily_metric_values,
      controlling.canonical_daily_metric_aggregates
    TO sandbox_app;
  END IF;
END
$runtime_grants$;

COMMIT;


