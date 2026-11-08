-- Bindet Alex Modul 2 lose an die zentralen, veredelten Qlik-Fachwerte.
-- Die n:m-Verbraucherbindung bleibt versioniert und fuer weitere Module offen.

BEGIN;

ALTER TABLE controlling.payment_days
  ADD COLUMN IF NOT EXISTS qlik_derived_value_id uuid
    REFERENCES controlling.qlik_derived_daily_values(id);

ALTER TABLE controlling.payment_days
  DROP CONSTRAINT IF EXISTS payment_days_qlik_derived_value_check;

ALTER TABLE controlling.payment_days
  ADD CONSTRAINT payment_days_qlik_derived_value_check
    CHECK (qlik_derived_value_id IS NULL OR qlik_amount_cents IS NOT NULL);

CREATE INDEX IF NOT EXISTS idx_payment_days_qlik_derived_value
  ON controlling.payment_days (qlik_derived_value_id)
  WHERE qlik_derived_value_id IS NOT NULL;

INSERT INTO controlling.qlik_consumers (
  consumer_code, consumer_type, display_name, description
) VALUES (
  'ALEX_MODULE_2', 'MODULE', 'Alex Modul 2',
  'Kontrollworkflow und Match-Engine fuer EC- und Kreditkartenzahlungen.'
)
ON CONFLICT (consumer_code) DO NOTHING;

WITH requested(metric_code, target_payment_type_code, usage_type, evidence_note) AS (
  VALUES
    ('EC_ZAHLUNGEN', 'EC_CASH', 'VORBELEGUNG',
     'Peter: Veredelter EC-Tageswert soll Alex Modul 2 automatisch vorbelegen.'),
    ('KREDITKARTEN_ZAHLUNGEN', 'KREDITKARTE', 'VORBELEGUNG',
     'Peter: Veredelter Kreditkarten-Tageswert soll Alex Modul 2 automatisch vorbelegen.'),
    ('EC_ZAHLUNGEN', 'EC_CASH', 'MATCH_BASIS',
     'Peter: Veredelter EC-Tageswert ist Basis der FirstData- und TeleCash-Match-Engine.'),
    ('KREDITKARTEN_ZAHLUNGEN', 'KREDITKARTE', 'MATCH_BASIS',
     'Peter: Veredelter Kreditkarten-Tageswert ist Basis der FirstData- und TeleCash-Match-Engine.')
)
INSERT INTO controlling.qlik_metric_consumer_bindings (
  metric_definition_id, consumer_id, usage_type, version, status,
  valid_from, tolerance_numeric, binding_config, requires_human_review,
  escalation_role, evidence_note
)
SELECT metric.id, consumer.id, requested.usage_type, 1, 'DRAFT',
       DATE '2026-06-01', 0,
       jsonb_build_object('targetPaymentTypeCode', requested.target_payment_type_code),
       true, 'Mirko', requested.evidence_note
FROM requested
JOIN controlling.qlik_metric_definitions metric
  ON metric.metric_code=requested.metric_code
JOIN controlling.qlik_consumers consumer
  ON consumer.consumer_code='ALEX_MODULE_2'
ON CONFLICT (metric_definition_id, consumer_id, usage_type, version) DO NOTHING;

DO $approve_alex_module_2$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM public.users WHERE id = 5 AND role IN ('Admin', 'Supervisor', 'Geschäftsführer')
  ) THEN
    RAISE EXCEPTION 'Freigabebenutzer 5 fuer Alex Modul 2 ist nicht verfuegbar.';
  END IF;

  UPDATE controlling.qlik_metric_consumer_bindings binding
  SET status = 'APPROVED', approved_by = 5, approved_at = now()
  FROM controlling.qlik_consumers consumer,
       controlling.qlik_metric_definitions metric
  WHERE binding.consumer_id = consumer.id
    AND binding.metric_definition_id = metric.id
    AND consumer.consumer_code = 'ALEX_MODULE_2'
    AND metric.metric_code IN ('EC_ZAHLUNGEN', 'KREDITKARTEN_ZAHLUNGEN')
    AND binding.usage_type IN ('VORBELEGUNG', 'MATCH_BASIS')
    AND binding.version = 1
    AND binding.status = 'DRAFT';
END
$approve_alex_module_2$;

COMMENT ON COLUMN controlling.payment_days.qlik_derived_value_id IS
  'Verwendeter zentraler Qlik-Fachwert; Rohdatenherkunft folgt ueber qlik_derived_value_sources.';

COMMIT;
