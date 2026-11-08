-- Modulneutrale, versionierte Qlik-Ableitungen und frei erweiterbare
-- Verbraucherbindungen. Kein Fachwert gehoert einem einzelnen Modul.

BEGIN;

CREATE TABLE IF NOT EXISTS controlling.qlik_metric_definitions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  metric_code varchar(80) NOT NULL UNIQUE
    CHECK (metric_code ~ '^[A-Z][A-Z0-9_]{2,79}$'),
  display_name varchar(160) NOT NULL CHECK (length(trim(display_name)) > 0),
  value_kind varchar(30) NOT NULL
    CHECK (value_kind IN ('MONEY_CENTS', 'COUNT', 'DECIMAL', 'RATIO_BPS')),
  aggregation_method varchar(20) NOT NULL
    CHECK (aggregation_method IN ('SUM', 'LAST', 'AVERAGE', 'NONE')),
  description text NOT NULL CHECK (length(trim(description)) >= 8),
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS controlling.qlik_derivation_rule_versions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  metric_definition_id uuid NOT NULL
    REFERENCES controlling.qlik_metric_definitions(id),
  version integer NOT NULL CHECK (version > 0),
  status varchar(20) NOT NULL DEFAULT 'DRAFT'
    CHECK (status IN ('DRAFT', 'APPROVED', 'RETIRED')),
  rule_kind varchar(20) NOT NULL
    CHECK (rule_kind IN ('DIRECT', 'SUM', 'FORMULA')),
  rule_config jsonb NOT NULL CHECK (jsonb_typeof(rule_config) = 'object'),
  valid_from date NOT NULL,
  valid_to date,
  evidence_note text NOT NULL CHECK (length(trim(evidence_note)) >= 8),
  approved_by integer REFERENCES public.users(id),
  approved_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (metric_definition_id, version),
  UNIQUE (id, metric_definition_id),
  CHECK (valid_to IS NULL OR valid_to >= valid_from),
  CHECK (
    (status = 'DRAFT' AND approved_by IS NULL AND approved_at IS NULL)
    OR
    (status IN ('APPROVED', 'RETIRED') AND approved_by IS NOT NULL AND approved_at IS NOT NULL)
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_qlik_derivation_rule_current
  ON controlling.qlik_derivation_rule_versions (metric_definition_id)
  WHERE status = 'APPROVED' AND valid_to IS NULL;

CREATE TABLE IF NOT EXISTS controlling.qlik_derived_daily_values (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  metric_definition_id uuid NOT NULL
    REFERENCES controlling.qlik_metric_definitions(id),
  rule_version_id uuid NOT NULL,
  branch_id integer NOT NULL REFERENCES public.filialen(id),
  customer_number char(6) NOT NULL CHECK (customer_number ~ '^\d{6}$'),
  business_date date NOT NULL,
  numeric_value numeric(20,4) NOT NULL,
  availability_status varchar(30) NOT NULL
    CHECK (availability_status IN ('VORHANDEN', 'UNVOLLSTAENDIG', 'UNGEKLAERTE_ZAHLART')),
  quality_status varchar(30) NOT NULL DEFAULT 'NICHT_GEPRUEFT'
    CHECK (quality_status IN ('NICHT_GEPRUEFT', 'PLAUSIBEL', 'AUFFAELLIG', 'ABGELEHNT')),
  source_fingerprint char(64) NOT NULL
    CHECK (source_fingerprint ~ '^[0-9a-f]{64}$'),
  calculated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT qlik_derived_daily_values_rule_metric_fkey
    FOREIGN KEY (rule_version_id, metric_definition_id)
    REFERENCES controlling.qlik_derivation_rule_versions(id, metric_definition_id),
  UNIQUE (id, metric_definition_id),
  UNIQUE (
    metric_definition_id, rule_version_id, branch_id, business_date,
    source_fingerprint
  )
);

CREATE INDEX IF NOT EXISTS idx_qlik_derived_daily_values_lookup
  ON controlling.qlik_derived_daily_values
    (metric_definition_id, business_date DESC, branch_id, calculated_at DESC);

CREATE TABLE IF NOT EXISTS controlling.qlik_derived_value_sources (
  derived_value_id uuid NOT NULL
    REFERENCES controlling.qlik_derived_daily_values(id),
  qlik_import_row_id uuid NOT NULL
    REFERENCES controlling.qlik_import_rows(id),
  source_role varchar(30) NOT NULL DEFAULT 'INPUT'
    CHECK (source_role IN ('INPUT', 'CONTROL', 'EXCLUSION')),
  normalized_numeric_value numeric(20,4) NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (derived_value_id, qlik_import_row_id)
);

CREATE INDEX IF NOT EXISTS idx_qlik_derived_value_sources_row
  ON controlling.qlik_derived_value_sources
    (qlik_import_row_id, derived_value_id);

CREATE TABLE IF NOT EXISTS controlling.qlik_consumers (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  consumer_code varchar(100) NOT NULL UNIQUE
    CHECK (consumer_code ~ '^[A-Z][A-Z0-9_]{2,99}$'),
  consumer_type varchar(30) NOT NULL
    CHECK (consumer_type IN ('MODULE', 'FUNCTION', 'ENGINE', 'EXPORT', 'DASHBOARD')),
  display_name varchar(160) NOT NULL CHECK (length(trim(display_name)) > 0),
  description text NOT NULL CHECK (length(trim(description)) >= 8),
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS controlling.qlik_metric_consumer_bindings (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  metric_definition_id uuid NOT NULL
    REFERENCES controlling.qlik_metric_definitions(id),
  consumer_id uuid NOT NULL REFERENCES controlling.qlik_consumers(id),
  usage_type varchar(30) NOT NULL
    CHECK (usage_type IN ('VORBELEGUNG', 'VERIFIZIERUNG', 'ANZEIGE', 'MATCH_BASIS', 'EXPORT')),
  version integer NOT NULL CHECK (version > 0),
  status varchar(20) NOT NULL DEFAULT 'DRAFT'
    CHECK (status IN ('DRAFT', 'APPROVED', 'RETIRED')),
  valid_from date NOT NULL,
  valid_to date,
  tolerance_numeric numeric(20,4) NOT NULL DEFAULT 0
    CHECK (tolerance_numeric >= 0),
  binding_config jsonb NOT NULL DEFAULT '{}'::jsonb
    CHECK (jsonb_typeof(binding_config) = 'object'),
  requires_human_review boolean NOT NULL DEFAULT true,
  escalation_role varchar(80),
  evidence_note text NOT NULL CHECK (length(trim(evidence_note)) >= 8),
  approved_by integer REFERENCES public.users(id),
  approved_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (metric_definition_id, consumer_id, usage_type, version),
  UNIQUE (id, metric_definition_id, consumer_id),
  CHECK (valid_to IS NULL OR valid_to >= valid_from),
  CHECK (escalation_role IS NULL OR length(trim(escalation_role)) > 0),
  CHECK (
    (status = 'DRAFT' AND approved_by IS NULL AND approved_at IS NULL)
    OR
    (status IN ('APPROVED', 'RETIRED') AND approved_by IS NOT NULL AND approved_at IS NOT NULL)
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_qlik_metric_consumer_binding_current
  ON controlling.qlik_metric_consumer_bindings
    (metric_definition_id, consumer_id, usage_type)
  WHERE status = 'APPROVED' AND valid_to IS NULL;

CREATE TABLE IF NOT EXISTS controlling.qlik_consumer_value_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  derived_value_id uuid NOT NULL
    REFERENCES controlling.qlik_derived_daily_values(id),
  binding_id uuid NOT NULL,
  metric_definition_id uuid NOT NULL,
  consumer_id uuid NOT NULL,
  event_type varchar(40) NOT NULL
    CHECK (event_type IN (
      'PRESENTED', 'COMPARED', 'CONFIRMED', 'CORRECTED', 'DEFERRED',
      'EXTERNALLY_CONFIRMED', 'REJECTED'
    )),
  actor_user_id integer REFERENCES public.users(id),
  details jsonb NOT NULL DEFAULT '{}'::jsonb
    CHECK (jsonb_typeof(details) = 'object'),
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT qlik_consumer_value_events_binding_fkey
    FOREIGN KEY (binding_id, metric_definition_id, consumer_id)
    REFERENCES controlling.qlik_metric_consumer_bindings(
      id, metric_definition_id, consumer_id
    ),
  CONSTRAINT qlik_consumer_value_events_value_metric_fkey
    FOREIGN KEY (derived_value_id, metric_definition_id)
    REFERENCES controlling.qlik_derived_daily_values(id, metric_definition_id)
);

CREATE INDEX IF NOT EXISTS idx_qlik_consumer_value_events_context
  ON controlling.qlik_consumer_value_events
    (consumer_id, derived_value_id, created_at DESC);

CREATE OR REPLACE FUNCTION controlling.reject_qlik_derived_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $qlik_derived_guard$
BEGIN
  RAISE EXCEPTION 'Qlik-Ableitungen, Herkunftsnachweise und Verbraucherereignisse sind append-only.';
END
$qlik_derived_guard$;

CREATE OR REPLACE FUNCTION controlling.guard_qlik_versioned_registry()
RETURNS trigger
LANGUAGE plpgsql
AS $qlik_registry_guard$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'Versionierte Qlik-Regeln und Verbraucherbindungen duerfen nicht geloescht werden.';
  END IF;
  IF OLD.status = 'RETIRED' THEN
    RAISE EXCEPTION 'Stillgelegte Qlik-Registry-Versionen sind unveraenderlich.';
  END IF;
  IF OLD.status = 'APPROVED' THEN
    IF NEW.status <> 'RETIRED'
       OR (to_jsonb(NEW) - ARRAY['status', 'valid_to'])
          IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['status', 'valid_to']) THEN
      RAISE EXCEPTION 'Freigegebene Qlik-Registry-Versionen duerfen nur stillgelegt werden.';
    END IF;
  END IF;
  RETURN NEW;
END
$qlik_registry_guard$;

DROP TRIGGER IF EXISTS qlik_derived_daily_values_append_only
  ON controlling.qlik_derived_daily_values;
CREATE TRIGGER qlik_derived_daily_values_append_only
BEFORE UPDATE OR DELETE ON controlling.qlik_derived_daily_values
FOR EACH ROW EXECUTE FUNCTION controlling.reject_qlik_derived_mutation();

DROP TRIGGER IF EXISTS qlik_derived_value_sources_append_only
  ON controlling.qlik_derived_value_sources;
CREATE TRIGGER qlik_derived_value_sources_append_only
BEFORE UPDATE OR DELETE ON controlling.qlik_derived_value_sources
FOR EACH ROW EXECUTE FUNCTION controlling.reject_qlik_derived_mutation();

DROP TRIGGER IF EXISTS qlik_consumer_value_events_append_only
  ON controlling.qlik_consumer_value_events;
CREATE TRIGGER qlik_consumer_value_events_append_only
BEFORE UPDATE OR DELETE ON controlling.qlik_consumer_value_events
FOR EACH ROW EXECUTE FUNCTION controlling.reject_qlik_derived_mutation();

DROP TRIGGER IF EXISTS qlik_derivation_rule_versions_guard
  ON controlling.qlik_derivation_rule_versions;
CREATE TRIGGER qlik_derivation_rule_versions_guard
BEFORE UPDATE OR DELETE ON controlling.qlik_derivation_rule_versions
FOR EACH ROW EXECUTE FUNCTION controlling.guard_qlik_versioned_registry();

DROP TRIGGER IF EXISTS qlik_metric_consumer_bindings_guard
  ON controlling.qlik_metric_consumer_bindings;
CREATE TRIGGER qlik_metric_consumer_bindings_guard
BEFORE UPDATE OR DELETE ON controlling.qlik_metric_consumer_bindings
FOR EACH ROW EXECUTE FUNCTION controlling.guard_qlik_versioned_registry();

COMMENT ON TABLE controlling.qlik_metric_definitions IS
  'Modulneutrale Definitionen veredelter Qlik-Fachwerte.';
COMMENT ON TABLE controlling.qlik_derivation_rule_versions IS
  'Versionierte, freizugebende Berechnungsregeln ohne ausfuehrbares SQL.';
COMMENT ON TABLE controlling.qlik_derived_daily_values IS
  'Append-only Fachwerte je Kennzahl, Regel, Filiale und Geschaeftstag.';
COMMENT ON TABLE controlling.qlik_derived_value_sources IS
  'Konkrete Qlik-Rohzeilen als Herkunftsnachweis eines Fachwerts.';
COMMENT ON TABLE controlling.qlik_consumers IS
  'Erweiterbares Register fuer Module, Funktionen, Engines und Exporte.';
COMMENT ON TABLE controlling.qlik_metric_consumer_bindings IS
  'Versionierte n:m-Bindungen zwischen Fachwerten und Verbrauchern.';
COMMENT ON TABLE controlling.qlik_consumer_value_events IS
  'Append-only Pruef- und Nutzungsereignisse je Verbraucherbindung.';

DO $runtime_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sandbox_app') THEN
    REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON
      controlling.qlik_metric_definitions,
      controlling.qlik_derivation_rule_versions,
      controlling.qlik_consumers,
      controlling.qlik_metric_consumer_bindings
    FROM sandbox_app;
    GRANT SELECT ON
      controlling.qlik_metric_definitions,
      controlling.qlik_derivation_rule_versions,
      controlling.qlik_consumers,
      controlling.qlik_metric_consumer_bindings
    TO sandbox_app;

    REVOKE UPDATE, DELETE, TRUNCATE ON
      controlling.qlik_derived_daily_values,
      controlling.qlik_derived_value_sources,
      controlling.qlik_consumer_value_events
    FROM sandbox_app;
    GRANT SELECT, INSERT ON
      controlling.qlik_derived_daily_values,
      controlling.qlik_derived_value_sources,
      controlling.qlik_consumer_value_events
    TO sandbox_app;
  END IF;
END
$runtime_grants$;

COMMIT;
