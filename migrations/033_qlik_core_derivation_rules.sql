-- Erstes fachlich bestaetigtes Qlik-Kernpaket. Regeln werden als DRAFT
-- angelegt; nur explizit APPROVED Regeln duerfen Tageswerte materialisieren.

BEGIN;

INSERT INTO controlling.qlik_metric_definitions (
  metric_code, display_name, value_kind, aggregation_method, description
) VALUES
  ('EC_ZAHLUNGEN', 'EC-Karten-Zahlungen', 'MONEY_CENTS', 'SUM',
   'Positiver EC-Tageswert aus Kartenzahlung IST.'),
  ('KREDITKARTEN_ZAHLUNGEN', 'Kreditkarten-Zahlungen', 'MONEY_CENTS', 'SUM',
   'Summe aus American Express, Maestro, Mastercard und Visa.'),
  ('KASSENDIFFERENZ', 'Kassendifferenz', 'MONEY_CENTS', 'SUM',
   'Vorzeichengetreue BAR Differenz des Filialtags.'),
  ('BAR_AN_BANK_QLIK', 'Bar an Bank laut Qlik', 'MONEY_CENTS', 'SUM',
   'Positiver Qlik-Kontrollwert aus dem negativen Barumsatz.'),
  ('KASSENAUSZAHLUNGEN', 'Auszahlungen an der Kasse', 'MONEY_CENTS', 'SUM',
   'Positiver Kontrollwert fuer belegpflichtige Kassenauszahlungen.'),
  ('UMSATZ_7_PROZENT', 'Umsatz 7 Prozent', 'MONEY_CENTS', 'SUM',
   'Positiver Tagesumsatz mit sieben Prozent Umsatzsteuer.'),
  ('UMSATZ_19_PROZENT', 'Umsatz 19 Prozent', 'MONEY_CENTS', 'SUM',
   'Positiver Tagesumsatz mit neunzehn Prozent Umsatzsteuer.'),
  ('GESAMTUMSATZ_QLIK', 'Gesamtumsatz laut Qlik', 'MONEY_CENTS', 'SUM',
   'Vorlaeufige freigegebene Summe aus Umsatz 7 und 19 Prozent.')
ON CONFLICT (metric_code) DO NOTHING;

WITH rules(metric_code, rule_kind, rule_config, evidence_note) AS (
  VALUES
    ('EC_ZAHLUNGEN', 'DIRECT',
     '{"payment_types":["Kartenzahlung IST"],"multiplier":-1}'::jsonb,
     'Peter: Kartenzahlung IST entspricht EC; positiver Wert mit Faktor minus eins.'),
    ('KREDITKARTEN_ZAHLUNGEN', 'SUM',
     '{"payment_types":["American Express IST","Maestro IST","Mastercard IST","Visa IST"],"multiplier":-1,"missing_components_as_zero":true}'::jsonb,
     'Peter: AmEX, Maestro, Mastercard und Visa ergeben gemeinsam Kreditkarte.'),
    ('KASSENDIFFERENZ', 'DIRECT',
     '{"payment_types":["BAR Differenz"],"multiplier":1}'::jsonb,
     'Peter: BAR Differenz ist die Kassendifferenz; Vorzeichen bleibt erhalten.'),
    ('BAR_AN_BANK_QLIK', 'DIRECT',
     '{"payment_types":["Barumsatz"],"multiplier":-1}'::jsonb,
     'Peter und Gegenrechnung Vreden: Barumsatz ist positiver BaB-Kontrollwert.'),
    ('KASSENAUSZAHLUNGEN', 'DIRECT',
     '{"payment_types":["Auszahlung"],"multiplier":-1}'::jsonb,
     'Peter: Auszahlung ist Kassenausgabe; positiver Kontrollwert, Beleg bleibt Chefsache.'),
    ('UMSATZ_7_PROZENT', 'DIRECT',
     '{"payment_types":["Umsatz 7%"],"multiplier":1}'::jsonb,
     'Peter: Umsatz 7 Prozent ist positiver Umsatzbestandteil.'),
    ('UMSATZ_19_PROZENT', 'DIRECT',
     '{"payment_types":["Umsatz 19%"],"multiplier":1}'::jsonb,
     'Peter: Umsatz 19 Prozent ist positiver Umsatzbestandteil.'),
    ('GESAMTUMSATZ_QLIK', 'SUM',
     '{"payment_types":["Umsatz 7%","Umsatz 19%"],"multiplier":1}'::jsonb,
     'Bestaetigte Gegenrechnung Vreden 07.11.2026; Sonderpositionen bleiben ausgeschlossen.')
)
INSERT INTO controlling.qlik_derivation_rule_versions (
  metric_definition_id, version, status, rule_kind, rule_config,
  valid_from, evidence_note
)
SELECT metric.id, 1, 'DRAFT', rules.rule_kind, rules.rule_config,
       DATE '2026-06-01', rules.evidence_note
FROM rules
JOIN controlling.qlik_metric_definitions metric
  ON metric.metric_code = rules.metric_code
ON CONFLICT (metric_definition_id, version) DO NOTHING;

CREATE OR REPLACE FUNCTION controlling.materialize_qlik_derived_daily_values(
  p_business_date date
)
RETURNS TABLE (inserted_values integer, inserted_sources integer)
LANGUAGE plpgsql
SECURITY INVOKER
AS $qlik_materialize$
DECLARE
  value_count integer := 0;
  source_count integer := 0;
BEGIN
  IF p_business_date IS NULL THEN
    RAISE EXCEPTION 'Geschaeftstag darf nicht NULL sein.';
  END IF;

  IF EXISTS (
    SELECT row_data.branch_name
    FROM controlling.qlik_import_rows row_data
    JOIN controlling.qlik_import_runs run ON run.id = row_data.import_run_id
    LEFT JOIN public.filialen branch
      ON lower(trim(branch.name)) = lower(trim(row_data.branch_name))
    WHERE run.status = 'IMPORTED'
      AND row_data.report_date = p_business_date
    GROUP BY row_data.branch_name
    HAVING count(DISTINCT branch.id) <> 1
  ) THEN
    RAISE EXCEPTION 'Mindestens eine Qlik-Filiale ist nicht eindeutig zugeordnet.';
  END IF;

  WITH active_rules AS (
    SELECT rule.id AS rule_version_id, rule.metric_definition_id,
           rule.rule_config,
           jsonb_array_length(rule.rule_config -> 'payment_types') AS expected_sources,
           COALESCE((rule.rule_config ->> 'missing_components_as_zero')::boolean, false)
             AS missing_components_as_zero,
           (rule.rule_config ->> 'multiplier')::numeric AS multiplier
    FROM controlling.qlik_derivation_rule_versions rule
    WHERE rule.status = 'APPROVED'
      AND p_business_date >= rule.valid_from
      AND (rule.valid_to IS NULL OR p_business_date <= rule.valid_to)
      AND jsonb_typeof(rule.rule_config -> 'payment_types') = 'array'
      AND jsonb_array_length(rule.rule_config -> 'payment_types') > 0
      AND rule.rule_config ? 'multiplier'
  ), branch_runs AS (
    SELECT DISTINCT run.id AS import_run_id, run.source_sha256,
           row_data.report_date, row_data.customer_number, row_data.branch_name,
           branch.id AS branch_id
    FROM controlling.qlik_import_runs run
    JOIN controlling.qlik_import_rows row_data ON row_data.import_run_id = run.id
    JOIN public.filialen branch
      ON lower(trim(branch.name)) = lower(trim(row_data.branch_name))
    WHERE run.status = 'IMPORTED' AND row_data.report_date = p_business_date
  ), evaluated AS (
    SELECT rules.rule_version_id, rules.metric_definition_id,
           branch_runs.branch_id, branch_runs.customer_number,
           branch_runs.report_date,
           COALESCE(sum(source.amount_cents), 0)::numeric * rules.multiplier AS numeric_value,
           CASE WHEN count(source.id) = rules.expected_sources
                     OR (rules.missing_components_as_zero AND count(source.id) > 0)
                THEN 'VORHANDEN' ELSE 'UNVOLLSTAENDIG' END AS availability_status,
           encode(digest(
             rules.rule_version_id::text || '|' || branch_runs.import_run_id::text || '|' ||
             branch_runs.customer_number || '|' ||
             COALESCE(string_agg(source.id::text, ',' ORDER BY source.id::text), 'MISSING'),
             'sha256'
           ), 'hex') AS source_fingerprint
    FROM branch_runs
    CROSS JOIN active_rules rules
    LEFT JOIN controlling.qlik_import_rows source
      ON source.import_run_id = branch_runs.import_run_id
     AND source.customer_number = branch_runs.customer_number
     AND EXISTS (
       SELECT 1 FROM jsonb_array_elements_text(rules.rule_config -> 'payment_types') item(value)
       WHERE item.value = source.payment_type_original
     )
    GROUP BY rules.rule_version_id, rules.metric_definition_id,
             rules.expected_sources, rules.missing_components_as_zero, rules.multiplier,
             branch_runs.branch_id, branch_runs.customer_number,
             branch_runs.report_date, branch_runs.import_run_id
  )
  INSERT INTO controlling.qlik_derived_daily_values (
    metric_definition_id, rule_version_id, branch_id, customer_number,
    business_date, numeric_value, availability_status, quality_status,
    source_fingerprint
  )
  SELECT metric_definition_id, rule_version_id, branch_id, customer_number,
         report_date, numeric_value, availability_status, 'NICHT_GEPRUEFT',
         source_fingerprint
  FROM evaluated
  ON CONFLICT (
    metric_definition_id, rule_version_id, branch_id, business_date,
    source_fingerprint
  ) DO NOTHING;
  GET DIAGNOSTICS value_count = ROW_COUNT;

  WITH active_rules AS (
    SELECT rule.id AS rule_version_id, rule.metric_definition_id, rule.rule_config
    FROM controlling.qlik_derivation_rule_versions rule
    WHERE rule.status = 'APPROVED'
      AND p_business_date >= rule.valid_from
      AND (rule.valid_to IS NULL OR p_business_date <= rule.valid_to)
  ), candidates AS (
    SELECT derived.id AS derived_value_id, source.id AS source_row_id,
           source.amount_cents::numeric AS source_value
    FROM controlling.qlik_derived_daily_values derived
    JOIN active_rules rules
      ON rules.rule_version_id = derived.rule_version_id
     AND rules.metric_definition_id = derived.metric_definition_id
    JOIN controlling.qlik_import_rows source
      ON source.report_date = derived.business_date
     AND source.customer_number = derived.customer_number
     AND EXISTS (
       SELECT 1 FROM jsonb_array_elements_text(rules.rule_config -> 'payment_types') item(value)
       WHERE item.value = source.payment_type_original
     )
    JOIN controlling.qlik_import_runs run ON run.id = source.import_run_id
    WHERE derived.business_date = p_business_date
      AND run.status = 'IMPORTED'
      AND derived.source_fingerprint = encode(digest(
        rules.rule_version_id::text || '|' || run.id::text || '|' ||
        source.customer_number || '|' ||
        (SELECT string_agg(all_source.id::text, ',' ORDER BY all_source.id::text)
         FROM controlling.qlik_import_rows all_source
         WHERE all_source.import_run_id = run.id
           AND all_source.customer_number = source.customer_number
           AND EXISTS (
             SELECT 1 FROM jsonb_array_elements_text(rules.rule_config -> 'payment_types') item2(value)
             WHERE item2.value = all_source.payment_type_original
           )),
        'sha256'
      ), 'hex')
  )
  INSERT INTO controlling.qlik_derived_value_sources (
    derived_value_id, qlik_import_row_id, source_role, normalized_numeric_value
  )
  SELECT derived_value_id, source_row_id, 'INPUT', source_value
  FROM candidates
  ON CONFLICT (derived_value_id, qlik_import_row_id) DO NOTHING;
  GET DIAGNOSTICS source_count = ROW_COUNT;

  RETURN QUERY SELECT value_count, source_count;
END
$qlik_materialize$;

COMMENT ON FUNCTION controlling.materialize_qlik_derived_daily_values(date) IS
  'Materialisiert ausschliesslich APPROVED Qlik-Regeln mit Rohzeilen-Provenienz; wiederholbar ohne Umschreiben.';

DO $runtime_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sandbox_app') THEN
    REVOKE ALL ON FUNCTION controlling.materialize_qlik_derived_daily_values(date) FROM PUBLIC;
    GRANT EXECUTE ON FUNCTION controlling.materialize_qlik_derived_daily_values(date) TO sandbox_app;
  END IF;
END
$runtime_grants$;

COMMIT;
