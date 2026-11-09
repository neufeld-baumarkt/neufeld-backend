-- Filialuebergreifender Qlik-Tagesumsatz fuer Alex Modul 1.
-- Die vier Filialwerte bleiben kanonisch; das Aggregat speichert nur die
-- nachvollziehbare Summe und ihre Vollstaendigkeit.

BEGIN;

CREATE TABLE IF NOT EXISTS controlling.qlik_derived_daily_aggregates (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  metric_definition_id uuid NOT NULL REFERENCES controlling.qlik_metric_definitions(id),
  rule_version_id uuid NOT NULL,
  business_date date NOT NULL,
  numeric_value numeric(20,4) NOT NULL,
  expected_member_count integer NOT NULL CHECK (expected_member_count > 0),
  actual_member_count integer NOT NULL CHECK (
    actual_member_count >= 0 AND actual_member_count <= expected_member_count
  ),
  availability_status varchar(30) NOT NULL
    CHECK (availability_status IN ('VORHANDEN', 'UNVOLLSTAENDIG')),
  quality_status varchar(30) NOT NULL DEFAULT 'NICHT_GEPRUEFT'
    CHECK (quality_status IN ('NICHT_GEPRUEFT', 'PLAUSIBEL', 'AUFFAELLIG', 'ABGELEHNT')),
  source_fingerprint char(64) NOT NULL CHECK (source_fingerprint ~ '^[0-9a-f]{64}$'),
  calculated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT qlik_derived_daily_aggregates_rule_metric_fkey
    FOREIGN KEY (rule_version_id, metric_definition_id)
    REFERENCES controlling.qlik_derivation_rule_versions(id, metric_definition_id),
  UNIQUE (id, metric_definition_id),
  UNIQUE (metric_definition_id, rule_version_id, business_date, source_fingerprint)
);

CREATE INDEX IF NOT EXISTS idx_qlik_derived_daily_aggregates_lookup
  ON controlling.qlik_derived_daily_aggregates
    (metric_definition_id, business_date DESC, calculated_at DESC);

CREATE TABLE IF NOT EXISTS controlling.qlik_derived_daily_aggregate_sources (
  aggregate_value_id uuid NOT NULL REFERENCES controlling.qlik_derived_daily_aggregates(id),
  derived_daily_value_id uuid NOT NULL REFERENCES controlling.qlik_derived_daily_values(id),
  customer_number char(6) NOT NULL CHECK (customer_number ~ '^\d{6}$'),
  normalized_numeric_value numeric(20,4) NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (aggregate_value_id, derived_daily_value_id),
  UNIQUE (aggregate_value_id, customer_number)
);

INSERT INTO controlling.qlik_metric_definitions (
  metric_code, display_name, value_kind, aggregation_method, description
) VALUES (
  'GESAMTUMSATZ_ALLE_FILIALEN_QLIK',
  'Gesamtumsatz aller Filialen laut Qlik',
  'MONEY_CENTS',
  'SUM',
  'Vollstaendige Summe der vier veredelten Qlik-Filialtagesumsaetze.'
)
ON CONFLICT (metric_code) DO NOTHING;

WITH metric AS (
  SELECT id FROM controlling.qlik_metric_definitions
  WHERE metric_code='GESAMTUMSATZ_ALLE_FILIALEN_QLIK'
)
INSERT INTO controlling.qlik_derivation_rule_versions (
  metric_definition_id, version, status, rule_kind, rule_config,
  valid_from, evidence_note
)
SELECT id, 1, 'DRAFT', 'SUM',
       '{"input_metric_codes":["GESAMTUMSATZ_QLIK"],"expected_customer_numbers":["041117","041129","041135","041140"],"require_all_members":true}'::jsonb,
       DATE '2026-06-01',
       'Peter: Alex 1 erhaelt als gebuchte Einnahme die Summe der vollstaendigen Qlik-Tagesumsaetze aller vier Filialen.'
FROM metric
ON CONFLICT (metric_definition_id, version) DO NOTHING;

INSERT INTO controlling.qlik_consumers (
  consumer_code, consumer_type, display_name, description
) VALUES (
  'ALEX_MODULE_1', 'MODULE', 'Alex Modul 1',
  'Einnahmen- und Ausgabenplanung mit Qlik-Ist-Umsatz nach vollstaendigem Filialimport.'
)
ON CONFLICT (consumer_code) DO NOTHING;

WITH requested AS (
  SELECT metric.id AS metric_definition_id, consumer.id AS consumer_id
  FROM controlling.qlik_metric_definitions metric
  CROSS JOIN controlling.qlik_consumers consumer
  WHERE metric.metric_code='GESAMTUMSATZ_ALLE_FILIALEN_QLIK'
    AND consumer.consumer_code='ALEX_MODULE_1'
)
INSERT INTO controlling.qlik_metric_consumer_bindings (
  metric_definition_id, consumer_id, usage_type, version, status,
  valid_from, tolerance_numeric, binding_config, requires_human_review,
  escalation_role, evidence_note
)
SELECT metric_definition_id, consumer_id, 'VORBELEGUNG', 1, 'DRAFT',
       DATE '2026-06-01', 0,
       '{"semanticRole":"IST_EINNAHMEN","targetTable":"cashflow.buchungen","targetCategoryId":1,"targetStatus":"gebucht","targetAmountField":"betrag","preservePlanInField":"planbetrag","sourceLabel":"qlik_gesamtumsatz_alle_filialen","writeMode":"ONLY_WHEN_COMPLETE","onIncomplete":"NO_WRITE","onDeviation":"CALCULATE_EXISTING_BACKEND_FIELDS"}'::jsonb,
       false, 'Mirko',
       'Peter: Qlik ersetzt das manuelle Summieren des Ist-Umsatzes; die Planzahl bleibt bestehen und fehlende Filialen verhindern die Buchung.'
FROM requested
ON CONFLICT (metric_definition_id, consumer_id, usage_type, version) DO NOTHING;

-- Fuchsbau-Freigabe von Peter am 09.11.2026: Nur die Umsatzregel,
-- das 4/4-Aggregat und dessen Alex-1-Verbraucherbindung werden freigegeben.
DO $approve_alex_module_1$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM public.users
    WHERE id = 5 AND role IN ('Admin', 'Supervisor', 'Geschäftsführer')
  ) THEN
    RAISE EXCEPTION 'Freigabebenutzer 5 fuer Alex Modul 1 ist nicht verfuegbar.';
  END IF;

  UPDATE controlling.qlik_derivation_rule_versions rule
  SET status='APPROVED', approved_by=5, approved_at=now()
  FROM controlling.qlik_metric_definitions metric
  WHERE rule.metric_definition_id=metric.id
    AND metric.metric_code IN ('GESAMTUMSATZ_QLIK', 'GESAMTUMSATZ_ALLE_FILIALEN_QLIK')
    AND rule.version=1
    AND rule.status='DRAFT';

  UPDATE controlling.qlik_metric_consumer_bindings binding
  SET status='APPROVED', approved_by=5, approved_at=now()
  FROM controlling.qlik_consumers consumer,
       controlling.qlik_metric_definitions metric
  WHERE binding.consumer_id=consumer.id
    AND binding.metric_definition_id=metric.id
    AND consumer.consumer_code='ALEX_MODULE_1'
    AND metric.metric_code='GESAMTUMSATZ_ALLE_FILIALEN_QLIK'
    AND binding.usage_type='VORBELEGUNG'
    AND binding.version=1
    AND binding.status='DRAFT';
END
$approve_alex_module_1$;

CREATE OR REPLACE FUNCTION controlling.materialize_qlik_daily_aggregate_values(
  p_business_date date
)
RETURNS TABLE (inserted_aggregates integer, inserted_sources integer)
LANGUAGE plpgsql
SECURITY INVOKER
AS $qlik_aggregate$
DECLARE
  aggregate_count integer := 0;
  source_count integer := 0;
BEGIN
  IF p_business_date IS NULL THEN
    RAISE EXCEPTION 'Geschaeftstag darf nicht NULL sein.';
  END IF;

  WITH aggregate_rules AS (
    SELECT rule.id AS rule_version_id, rule.metric_definition_id,
           rule.rule_config,
           jsonb_array_length(rule.rule_config -> 'expected_customer_numbers') AS expected_count
    FROM controlling.qlik_derivation_rule_versions rule
    WHERE rule.status='APPROVED'
      AND p_business_date >= rule.valid_from
      AND (rule.valid_to IS NULL OR p_business_date <= rule.valid_to)
      AND jsonb_typeof(rule.rule_config -> 'input_metric_codes')='array'
      AND jsonb_typeof(rule.rule_config -> 'expected_customer_numbers')='array'
      AND COALESCE((rule.rule_config ->> 'require_all_members')::boolean, false)=true
  ), latest_components AS (
    SELECT aggregate_rules.rule_version_id,
           aggregate_rules.metric_definition_id AS aggregate_metric_id,
           aggregate_rules.expected_count,
           expected.customer_number,
           component.id AS component_id,
           component.numeric_value,
           component.availability_status,
           row_number() OVER (
             PARTITION BY aggregate_rules.rule_version_id, expected.customer_number
             ORDER BY component.calculated_at DESC, component.id DESC
           ) AS component_rank
    FROM aggregate_rules
    CROSS JOIN LATERAL jsonb_array_elements_text(
      aggregate_rules.rule_config -> 'expected_customer_numbers'
    ) expected(customer_number)
    LEFT JOIN controlling.qlik_derived_daily_values component
      ON component.business_date=p_business_date
     AND component.customer_number=expected.customer_number
     AND component.metric_definition_id IN (
       SELECT input_metric.id
       FROM controlling.qlik_metric_definitions input_metric
       WHERE EXISTS (
         SELECT 1
         FROM jsonb_array_elements_text(
           aggregate_rules.rule_config -> 'input_metric_codes'
         ) input_code(metric_code)
         WHERE input_code.metric_code=input_metric.metric_code
       )
     )
  ), selected AS (
    SELECT * FROM latest_components
    WHERE component_rank=1 OR component_id IS NULL
  ), evaluated AS (
    SELECT rule_version_id, aggregate_metric_id, expected_count,
           count(component_id) FILTER (
             WHERE availability_status='VORHANDEN'
           )::integer AS actual_count,
           COALESCE(sum(numeric_value) FILTER (
             WHERE availability_status='VORHANDEN'
           ), 0)::numeric AS numeric_value,
           CASE WHEN count(component_id) FILTER (
                      WHERE availability_status='VORHANDEN'
                    )=expected_count
                THEN 'VORHANDEN' ELSE 'UNVOLLSTAENDIG' END AS availability_status,
           encode(digest(
             rule_version_id::text || '|' || p_business_date::text || '|' ||
             string_agg(
               customer_number || ':' || COALESCE(component_id::text, 'MISSING'),
               ',' ORDER BY customer_number
             ),
             'sha256'
           ), 'hex') AS source_fingerprint
    FROM selected
    GROUP BY rule_version_id, aggregate_metric_id, expected_count
  )
  INSERT INTO controlling.qlik_derived_daily_aggregates (
    metric_definition_id, rule_version_id, business_date, numeric_value,
    expected_member_count, actual_member_count, availability_status,
    source_fingerprint
  )
  SELECT aggregate_metric_id, rule_version_id, p_business_date, numeric_value,
         expected_count, actual_count, availability_status, source_fingerprint
  FROM evaluated
  ON CONFLICT (
    metric_definition_id, rule_version_id, business_date, source_fingerprint
  ) DO NOTHING;
  GET DIAGNOSTICS aggregate_count = ROW_COUNT;

  WITH latest_aggregates AS (
    SELECT aggregate.id, aggregate.rule_version_id, rule.rule_config,
           row_number() OVER (
             PARTITION BY aggregate.rule_version_id
             ORDER BY aggregate.calculated_at DESC, aggregate.id DESC
           ) AS aggregate_rank
    FROM controlling.qlik_derived_daily_aggregates aggregate
    JOIN controlling.qlik_derivation_rule_versions rule
      ON rule.id=aggregate.rule_version_id
    WHERE aggregate.business_date=p_business_date
      AND rule.status='APPROVED'
  ), components AS (
    SELECT aggregate.id AS aggregate_id,
           component.id AS component_id,
           component.customer_number,
           component.numeric_value,
           row_number() OVER (
             PARTITION BY aggregate.id, component.customer_number
             ORDER BY component.calculated_at DESC, component.id DESC
           ) AS component_rank
    FROM latest_aggregates aggregate
    JOIN controlling.qlik_derived_daily_values component
      ON component.business_date=p_business_date
     AND component.availability_status='VORHANDEN'
     AND EXISTS (
       SELECT 1 FROM jsonb_array_elements_text(
         aggregate.rule_config -> 'expected_customer_numbers'
       ) expected(customer_number)
       WHERE expected.customer_number=component.customer_number
     )
    JOIN controlling.qlik_metric_definitions input_metric
      ON input_metric.id=component.metric_definition_id
     AND EXISTS (
       SELECT 1 FROM jsonb_array_elements_text(
         aggregate.rule_config -> 'input_metric_codes'
       ) input_code(metric_code)
       WHERE input_code.metric_code=input_metric.metric_code
     )
    WHERE aggregate.aggregate_rank=1
  )
  INSERT INTO controlling.qlik_derived_daily_aggregate_sources (
    aggregate_value_id, derived_daily_value_id, customer_number,
    normalized_numeric_value
  )
  SELECT aggregate_id, component_id, customer_number, numeric_value
  FROM components
  WHERE component_rank=1
  ON CONFLICT (aggregate_value_id, derived_daily_value_id) DO NOTHING;
  GET DIAGNOSTICS source_count = ROW_COUNT;

  RETURN QUERY SELECT aggregate_count, source_count;
END
$qlik_aggregate$;

DROP TRIGGER IF EXISTS qlik_derived_daily_aggregates_append_only
  ON controlling.qlik_derived_daily_aggregates;
CREATE TRIGGER qlik_derived_daily_aggregates_append_only
BEFORE UPDATE OR DELETE ON controlling.qlik_derived_daily_aggregates
FOR EACH ROW EXECUTE FUNCTION controlling.reject_qlik_derived_mutation();

DROP TRIGGER IF EXISTS qlik_derived_daily_aggregate_sources_append_only
  ON controlling.qlik_derived_daily_aggregate_sources;
CREATE TRIGGER qlik_derived_daily_aggregate_sources_append_only
BEFORE UPDATE OR DELETE ON controlling.qlik_derived_daily_aggregate_sources
FOR EACH ROW EXECUTE FUNCTION controlling.reject_qlik_derived_mutation();

COMMENT ON TABLE controlling.qlik_derived_daily_aggregates IS
  'Append-only Qlik-Fachwerte ueber mehrere Filialen mit expliziter Vollstaendigkeit.';
COMMENT ON TABLE controlling.qlik_derived_daily_aggregate_sources IS
  'Konkrete veredelte Filialwerte als Herkunft eines filialuebergreifenden Aggregats.';

DO $runtime_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sandbox_app') THEN
    REVOKE UPDATE, DELETE, TRUNCATE ON
      controlling.qlik_derived_daily_aggregates,
      controlling.qlik_derived_daily_aggregate_sources
    FROM sandbox_app;
    GRANT SELECT, INSERT ON
      controlling.qlik_derived_daily_aggregates,
      controlling.qlik_derived_daily_aggregate_sources
    TO sandbox_app;
    REVOKE ALL ON FUNCTION
      controlling.materialize_qlik_daily_aggregate_values(date)
    FROM PUBLIC;
    GRANT EXECUTE ON FUNCTION
      controlling.materialize_qlik_daily_aggregate_values(date)
    TO sandbox_app;
  END IF;
END
$runtime_grants$;

COMMIT;
